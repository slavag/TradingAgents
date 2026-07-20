"""Tests for the graph-owned invoke and streaming execution lifecycle."""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest

from cli import main as cli_main
from tradingagents.dataflows.temporal import get_analysis_context
from tradingagents.graph.checkpointer import thread_id
from tradingagents.graph.trading_graph import TradingAgentsGraph

FINAL_STATE = {
    "company_of_interest": "NVDA",
    "trade_date": "2026-01-10",
    "final_trade_decision": "Buy",
}


def make_lifecycle_graph(*, two_chunks=False, stream_error=None):
    graph = object.__new__(TradingAgentsGraph)
    graph.config = {
        "checkpoint_enabled": False,
        "data_cache_dir": "/unused",
        "max_debate_rounds": 1,
        "max_risk_discuss_rounds": 1,
    }
    graph.selected_analysts = ("market",)
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


def test_propagate_uses_same_scope_and_preserves_return_shape():
    graph = make_lifecycle_graph()

    final_state, decision = graph.propagate("NVDA", "2026-01-10", asset_type="stock")

    assert final_state == FINAL_STATE
    assert decision == "Buy"
    graph.graph.invoke.assert_called_once()
    graph.memory_log.store_decision.assert_called_once()
    assert get_analysis_context() is None


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
