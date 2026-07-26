"""Tests for the graph-owned invoke and streaming execution lifecycle."""

from __future__ import annotations

import inspect
from datetime import date
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pytest

from cli import main as cli_main
from cli.models import AnalystType
from tradingagents.agents.utils.agent_utils import build_instrument_context
from tradingagents.dataflows.temporal import get_analysis_context
from tradingagents.graph.checkpointer import thread_id
from tradingagents.graph.trading_graph import TradingAgentsGraph

FINAL_STATE = {
    "company_of_interest": "NVDA",
    "trade_date": "2026-01-10",
    "final_trade_decision": "Buy",
}


def make_lifecycle_graph(*, two_chunks=False, stream_error=None, empty_stream=False):
    graph = object.__new__(TradingAgentsGraph)
    graph.config = {
        "checkpoint_enabled": False,
        "data_cache_dir": "/unused",
        "max_debate_rounds": 1,
        "max_risk_discuss_rounds": 1,
    }
    graph.selected_analysts = ("market",)
    graph.debug = False
    graph.curr_state = None
    graph.ticker = None
    graph.memory_log = MagicMock()
    graph.memory_log.get_past_context.return_value = "chronological lesson"
    graph.resolve_instrument_context = MagicMock(return_value="NVDA identity")
    graph._resolve_pending_entries = MagicMock()

    def resolve_pending_entries(company_name, *, as_of_date):
        context = get_analysis_context()
        assert context is not None
        assert context.as_of_date.isoformat() == as_of_date

    graph._resolve_pending_entries.side_effect = resolve_pending_entries
    graph._log_state = MagicMock()
    graph.process_signal = MagicMock(return_value="Buy")
    graph.propagator = MagicMock()

    def create_initial_state(
        company_name,
        trade_date,
        *,
        asset_type,
        past_context,
        instrument_context,
    ):
        return {
            "company_of_interest": company_name,
            "trade_date": str(trade_date),
            "asset_type": asset_type,
            "past_context": past_context,
            "instrument_context": instrument_context,
        }

    graph.propagator.create_initial_state.side_effect = create_initial_state
    graph.propagator.get_graph_args.return_value = {"stream_mode": "values", "config": {}}
    graph.graph = MagicMock()
    graph.workflow = MagicMock()

    first_chunk = {**FINAL_STATE, "final_trade_decision": "Hold"}

    def stream(initial_state, **kwargs):
        assert get_analysis_context() is not None
        if stream_error is not None:
            raise stream_error
        if empty_stream:
            return
        if two_chunks:
            yield first_chunk
        yield FINAL_STATE

    graph.graph.stream.side_effect = stream
    graph.graph.invoke.return_value = FINAL_STATE
    return graph


def test_stream_injects_memory_and_instrument_context():
    graph = make_lifecycle_graph()
    callback = object()

    chunks = list(graph.stream("NVDA", "2026-01-10", callbacks=[callback]))

    assert chunks[-1]["final_trade_decision"] == "Buy"
    graph._resolve_pending_entries.assert_called_once_with("NVDA", as_of_date="2026-01-10")
    graph.memory_log.get_past_context.assert_called_once_with("NVDA", as_of_date="2026-01-10")
    graph.resolve_instrument_context.assert_called_once_with("NVDA", "stock")
    initial = graph.graph.stream.call_args.args[0]
    assert initial["past_context"] == "chronological lesson"
    assert initial["instrument_context"] == "NVDA identity"
    graph.propagator.get_graph_args.assert_called_once_with(callbacks=[callback])


def test_historical_execution_uses_ticker_only_context_without_identity_lookup():
    graph = make_lifecycle_graph()
    graph.resolve_instrument_context = TradingAgentsGraph.resolve_instrument_context.__get__(
        graph, TradingAgentsGraph
    )

    with patch(
        "tradingagents.graph.trading_graph.resolve_instrument_identity",
        side_effect=AssertionError("historical execution must not access Yahoo .info"),
    ) as identity_lookup:
        list(graph.stream("NVDA", "2020-01-02"))

    identity_lookup.assert_not_called()
    initial = graph.graph.stream.call_args.args[0]
    assert initial["instrument_context"] == build_instrument_context(
        "NVDA", "stock", identity=None
    )


def test_live_execution_preserves_identity_lookup():
    graph = make_lifecycle_graph()
    graph.resolve_instrument_context = TradingAgentsGraph.resolve_instrument_context.__get__(
        graph, TradingAgentsGraph
    )

    with patch(
        "tradingagents.graph.trading_graph.resolve_instrument_identity",
        return_value={"company_name": "NVIDIA Corporation"},
    ) as identity_lookup:
        list(graph.stream("NVDA", date.today().isoformat()))

    identity_lookup.assert_called_once_with("NVDA")
    initial = graph.graph.stream.call_args.args[0]
    assert "Company: NVIDIA Corporation" in initial["instrument_context"]


def test_completed_stream_logs_and_stores_decision():
    graph = make_lifecycle_graph()

    list(graph.stream("NVDA", "2026-01-10"))

    graph._log_state.assert_called_once_with("2026-01-10", FINAL_STATE)
    graph.memory_log.store_decision.assert_called_once_with(
        ticker="NVDA",
        trade_date="2026-01-10",
        final_trade_decision="Buy",
    )
    assert graph.curr_state == FINAL_STATE
    assert get_analysis_context() is None


def test_interrupted_stream_does_not_store_decision():
    graph = make_lifecycle_graph(two_chunks=True)
    iterator = graph.stream("NVDA", "2026-01-10")

    assert next(iterator)["final_trade_decision"] == "Hold"
    iterator.close()

    graph.memory_log.store_decision.assert_not_called()
    graph._log_state.assert_not_called()
    assert graph.curr_state is None
    assert get_analysis_context() is None


def test_temporal_context_is_cleared_after_stream_exception():
    graph = make_lifecycle_graph(stream_error=RuntimeError("boom"))

    with pytest.raises(RuntimeError, match="boom"):
        list(graph.stream("NVDA", "2026-01-10"))

    graph.memory_log.store_decision.assert_not_called()
    assert get_analysis_context() is None


def test_empty_stream_raises_without_completing_or_clearing_checkpoint():
    graph = make_lifecycle_graph(empty_stream=True)
    graph.config["checkpoint_enabled"] = True
    checkpoint_graph = graph.graph
    plain_graph = MagicMock()
    graph.workflow.compile.side_effect = [checkpoint_graph, plain_graph]
    context = _RecordingCheckpointContext()

    with (
        patch(
            "tradingagents.graph.trading_graph.get_checkpointer",
            return_value=context,
        ),
        patch("tradingagents.graph.trading_graph.checkpoint_step", return_value=None),
        patch("tradingagents.graph.trading_graph.clear_checkpoint") as clear,
        pytest.raises(
            RuntimeError,
            match="Graph execution produced no value chunks for NVDA on 2026-01-10",
        ),
    ):
        list(graph.stream("NVDA", "2026-01-10"))

    graph._log_state.assert_not_called()
    graph.memory_log.store_decision.assert_not_called()
    clear.assert_not_called()
    assert graph.curr_state is None
    assert context.entered and context.exited
    assert graph.graph is plain_graph
    assert get_analysis_context() is None


def test_propagate_uses_same_scope_and_preserves_return_shape():
    graph = make_lifecycle_graph()

    final_state, decision = graph.propagate("NVDA", "2026-01-10", asset_type="stock")

    assert final_state == FINAL_STATE
    assert decision == "Buy"
    graph.graph.invoke.assert_called_once()
    graph.memory_log.store_decision.assert_called_once()
    assert get_analysis_context() is None


def test_debug_propagate_streams_deduplicates_messages_and_completes_once():
    graph = make_lifecycle_graph()
    graph.debug = True
    first_message = MagicMock()
    first_message.content = "first"
    repeated_message = MagicMock()
    repeated_message.content = "first"
    final_message = MagicMock()
    final_message.content = "final"
    chunks = [
        {"messages": [first_message], "market_report": "Market"},
        {"messages": [repeated_message], "news_report": "News"},
        {
            "messages": [final_message],
            "final_trade_decision": "Buy",
            "company_of_interest": "NVDA",
            "trade_date": "2026-01-10",
        },
    ]
    graph.graph.stream.side_effect = None
    graph.graph.stream.return_value = iter(chunks)
    complete_run = TradingAgentsGraph._complete_run
    with patch.object(
        TradingAgentsGraph, "_complete_run", autospec=True
    ) as complete:
        complete.side_effect = complete_run
        final_state, decision = graph.propagate("NVDA", "2026-01-10")

    assert final_state == {
        "messages": [final_message],
        "market_report": "Market",
        "news_report": "News",
        "final_trade_decision": "Buy",
        "company_of_interest": "NVDA",
        "trade_date": "2026-01-10",
    }
    assert decision == "Buy"
    first_message.pretty_print.assert_called_once_with()
    repeated_message.pretty_print.assert_not_called()
    final_message.pretty_print.assert_called_once_with()
    graph.graph.invoke.assert_not_called()
    complete.assert_called_once_with(
        graph, "NVDA", "2026-01-10", "stock", final_state
    )
    graph._log_state.assert_called_once_with("2026-01-10", final_state)
    graph.memory_log.store_decision.assert_called_once()


def test_temporal_context_is_cleared_after_invoke_exception():
    graph = make_lifecycle_graph()
    graph.graph.invoke.side_effect = RuntimeError("invoke boom")

    with pytest.raises(RuntimeError, match="invoke boom"):
        graph.propagate("NVDA", "2026-01-10")

    graph.memory_log.store_decision.assert_not_called()
    assert get_analysis_context() is None


class _RecordingCheckpointContext:
    def __init__(self):
        self.saver = object()
        self.entered = False
        self.exited = False

    def __enter__(self):
        self.entered = True
        return self.saver

    def __exit__(self, exc_type, exc, traceback):
        self.exited = True


def test_checkpoint_scope_sets_signature_clears_success_and_restores_graph():
    graph = make_lifecycle_graph()
    graph.config["checkpoint_enabled"] = True
    checkpoint_graph = graph.graph
    plain_graph = MagicMock()
    graph.workflow.compile.side_effect = [checkpoint_graph, plain_graph]
    context = _RecordingCheckpointContext()

    with (
        patch(
            "tradingagents.graph.trading_graph.get_checkpointer",
            return_value=context,
        ) as checkpointer_factory,
        patch(
            "tradingagents.graph.trading_graph.checkpoint_step", return_value=4
        ) as step_lookup,
        patch("tradingagents.graph.trading_graph.clear_checkpoint") as clear,
    ):
        list(graph.stream("NVDA", "2026-01-10"))

    signature = graph._run_signature("stock")
    args = checkpoint_graph.stream.call_args.kwargs
    assert args["config"]["configurable"]["thread_id"] == thread_id(
        "NVDA", "2026-01-10", signature
    )
    checkpointer_factory.assert_called_once_with("/unused", "NVDA")
    step_lookup.assert_called_once_with("/unused", "NVDA", "2026-01-10", signature)
    clear.assert_called_once_with("/unused", "NVDA", "2026-01-10", signature)
    assert context.entered and context.exited
    assert graph.graph is plain_graph


@pytest.mark.parametrize("termination", ["close", "exception"])
def test_checkpoint_scope_closes_without_storing_partial_run(termination):
    graph = make_lifecycle_graph(two_chunks=True)
    graph.config["checkpoint_enabled"] = True
    checkpoint_graph = graph.graph
    plain_graph = MagicMock()
    graph.workflow.compile.side_effect = [checkpoint_graph, plain_graph]
    context = _RecordingCheckpointContext()
    if termination == "exception":
        checkpoint_graph.stream.side_effect = RuntimeError("checkpoint boom")

    with (
        patch(
            "tradingagents.graph.trading_graph.get_checkpointer",
            return_value=context,
        ),
        patch("tradingagents.graph.trading_graph.checkpoint_step", return_value=None),
        patch("tradingagents.graph.trading_graph.clear_checkpoint") as clear,
    ):
        iterator = graph.stream("NVDA", "2026-01-10")
        if termination == "close":
            next(iterator)
            iterator.close()
        else:
            with pytest.raises(RuntimeError, match="checkpoint boom"):
                list(iterator)

    graph.memory_log.store_decision.assert_not_called()
    clear.assert_not_called()
    assert context.entered and context.exited
    assert graph.graph is plain_graph
    assert get_analysis_context() is None


def test_cli_uses_graph_owned_stream_lifecycle():
    source = inspect.getsource(cli_main.run_single_analysis)

    assert "graph.stream(" in source
    assert "callbacks=[stats_handler]" in source
    assert "_stream_graph_in_analysis_context" not in source
    assert not hasattr(cli_main, "_stream_graph_in_analysis_context")


def test_cli_surfaces_empty_graph_execution_instead_of_hold_decision():
    class EmptyUnderlyingStreamGraph(TradingAgentsGraph):
        instance = None

        def __init__(self, selected_analysts, *, config, debug, callbacks):
            self.__class__.instance = self
            self.selected_analysts = tuple(selected_analysts)
            self.config = config
            self.debug = debug
            self.callbacks = callbacks
            self.curr_state = None
            self.ticker = None
            self.memory_log = MagicMock()
            self.memory_log.get_past_context.return_value = ""
            self.resolve_instrument_context = MagicMock(return_value="NVDA identity")
            self._resolve_pending_entries = MagicMock()
            self._log_state = MagicMock()
            self.process_signal = MagicMock(return_value="Hold")
            self.propagator = MagicMock()
            self.propagator.create_initial_state.return_value = {}
            self.propagator.get_graph_args.return_value = {
                "stream_mode": "values",
                "config": {},
            }
            self.graph = MagicMock()
            self.graph.stream.return_value = iter(())
            self.workflow = MagicMock()
            self.quick_thinking_llm = object()

    with TemporaryDirectory() as tmpdir:
        config = {
            "checkpoint_enabled": False,
            "data_cache_dir": tmpdir,
            "results_dir": tmpdir,
            "max_debate_rounds": 1,
            "max_risk_discuss_rounds": 1,
        }
        selections = {
            "analysts": [AnalystType.MARKET],
            "analysis_date": "2026-01-10",
        }
        with (
            patch.object(cli_main, "TradingAgentsGraph", EmptyUnderlyingStreamGraph),
            patch.object(cli_main, "_build_run_config", return_value=config),
            patch.object(cli_main, "message_buffer", cli_main.MessageBuffer()),
            patch.object(cli_main, "create_layout", return_value=MagicMock()),
            patch.object(cli_main, "update_display"),
            patch.object(cli_main, "Live"),
            patch.object(cli_main.console, "print"),
            patch.object(cli_main, "estimate_target_profile", return_value={}),
            pytest.raises(
                RuntimeError,
                match="Graph execution produced no value chunks for NVDA on 2026-01-10",
            ),
        ):
            cli_main.run_single_analysis(selections, "NVDA")

    graph = EmptyUnderlyingStreamGraph.instance
    assert graph is not None
    graph.process_signal.assert_not_called()
    graph._log_state.assert_not_called()
    graph.memory_log.store_decision.assert_not_called()
