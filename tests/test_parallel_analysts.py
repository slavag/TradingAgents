"""Optional analyst fan-out and bounded tool use preserve shared evidence."""

from threading import Barrier
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables import RunnableLambda
from langchain_core.tools import tool
from langgraph.prebuilt import ToolNode

import tradingagents.graph.setup as graph_setup_module
from tradingagents.agents.analysts.turn import WRAP_UP, take_turn
from tradingagents.dataflows.config import get_config, run_config
from tradingagents.default_config import DEFAULT_CONFIG, _apply_env_overrides
from tradingagents.graph.analyst_execution import ANALYST_NODE_SPECS
from tradingagents.graph.setup import GraphSetup
from tradingagents.graph.trading_graph import Settlement, TradingAgentsGraph


@tool
def echo(value: str) -> str:
    """Echo a fixture value."""
    return value


def _dummy_team(monkeypatch, bull):
    monkeypatch.setattr(graph_setup_module, "create_bull_researcher", lambda *a, **kw: bull)
    for name in (
        "create_bear_researcher", "create_research_manager", "create_trader",
        "create_aggressive_debator", "create_neutral_debator",
        "create_conservative_debator", "create_portfolio_manager",
    ):
        monkeypatch.setattr(graph_setup_module, name, lambda *a, **kw: lambda state: {})
    return SimpleNamespace(
        max_debate_rounds=1,
        max_risk_discuss_rounds=1,
        should_continue_debate=lambda state: "Research Manager",
        should_continue_risk_analysis=lambda state: "Portfolio Manager",
    )


def test_optional_parallel_and_settlement_defaults_and_env(monkeypatch):
    assert DEFAULT_CONFIG["parallel_analysts"] is False
    assert DEFAULT_CONFIG["background_settlement"] is False
    assert DEFAULT_CONFIG["max_tool_rounds"] > 0
    monkeypatch.setenv("TRADINGAGENTS_PARALLEL_ANALYSTS", "true")
    monkeypatch.setenv("TRADINGAGENTS_BACKGROUND_SETTLEMENT", "true")
    monkeypatch.setenv("TRADINGAGENTS_MAX_TOOL_ROUNDS", "2")
    config = _apply_env_overrides(DEFAULT_CONFIG.copy())
    assert config["parallel_analysts"] is True
    assert config["background_settlement"] is True
    assert config["max_tool_rounds"] == 2


@pytest.mark.parametrize("with_memory", [False, True])
def test_parallel_analysts_join_reports_evidence_and_memory(monkeypatch, with_memory):
    rendezvous = Barrier(3 if with_memory else 2)
    base = {"source": "base", "title": "Base", "content": "Base evidence"}
    monkeypatch.setattr(
        graph_setup_module, "collect_shared_evidence",
        lambda state: {"shared_evidence": [base], "evidence_collected": True},
    )

    def analyst(key, report):
        def node(state):
            assert state["evidence_collected"] is True
            assert get_config()["data_vendors"]["news_data"] == "alpha_vantage"
            rendezvous.wait(timeout=5)
            return {
                "messages": [AIMessage(content=report)],
                key: report,
                "shared_evidence": [
                    *state["shared_evidence"],
                    {"source": report, "title": report, "content": f"{report} evidence"},
                ],
            }
        return node

    monkeypatch.setattr(graph_setup_module, "create_market_analyst", lambda llm: analyst("market_report", "market"))
    monkeypatch.setattr(graph_setup_module, "create_news_analyst", lambda llm: analyst("news_report", "news"))

    def bull(state):
        assert state["past_context"] == ("settled lesson" if with_memory else "")
        assert {e["source"] for e in state["shared_evidence"]} == {"base", "market", "news"}
        return {}

    logic = _dummy_team(monkeypatch, bull)
    setup = GraphSetup(None, None, {"market": ToolNode([echo]), "news": ToolNode([echo])}, logic)

    def memory_node(state):
        rendezvous.wait(timeout=5)
        return {"past_context": "settled lesson"}

    graph = setup.setup_graph(
        ("market", "news"), parallel_analysts=True,
        background_settlement=with_memory,
        memory_node=memory_node if with_memory else None,
    ).compile()
    with run_config({"data_vendors": {"news_data": "alpha_vantage"}}):
        result = graph.invoke({
            "company_of_interest": "AAPL", "trade_date": "2026-01-15",
            "messages": [], "shared_evidence": [], "past_context": "",
        })
    assert result["market_report"] == "market"
    assert result["news_report"] == "news"
    assert result["messages"] == []


def test_sequential_analyst_is_forced_to_report_at_tool_round_cap(monkeypatch):
    tool_calls = []

    @tool
    def recorded_echo(value: str) -> str:
        """Echo a recorded fixture value."""
        tool_calls.append(value)
        return value

    def market(state):
        last = state["messages"][-1]
        if isinstance(last, HumanMessage) and last.content == WRAP_UP:
            return {"messages": [AIMessage(content="Final report")], "market_report": "Final report"}
        return {
            "messages": [AIMessage(content="", tool_calls=[
                {"name": "recorded_echo", "args": {"value": "quote"}, "id": "call-1"},
            ])],
            "market_report": "",
        }

    monkeypatch.setattr(graph_setup_module, "collect_shared_evidence", lambda state: {"evidence_collected": True})
    monkeypatch.setattr(graph_setup_module, "create_market_analyst", lambda llm: market)
    logic = _dummy_team(monkeypatch, lambda state: {})
    logic.should_continue_market = lambda state: (
        "tools_market" if state["messages"][-1].tool_calls else "Msg Clear Market"
    )
    setup = GraphSetup(None, None, {"market": ToolNode([recorded_echo])}, logic)
    result = setup.setup_graph(("market",), max_tool_rounds=1).compile().invoke({
        "company_of_interest": "AAPL", "trade_date": "2026-01-15",
        "messages": [HumanMessage(content="AAPL")],
    })
    assert result["market_report"] == "Final report"
    assert tool_calls == ["quote"]


def test_parallel_handoff_preserves_executed_tool_evidence():
    def market(state):
        if any(isinstance(message, ToolMessage) for message in state["messages"]):
            return {
                "messages": [AIMessage(content="Filed report")],
                "market_report": "Filed report",
            }
        return {
            "messages": [AIMessage(content="", tool_calls=[
                {"name": "echo", "args": {"value": "verified quote"}, "id": "call-1"},
            ])],
            "market_report": "",
        }

    graph = graph_setup_module._parallel_analyst_graph(
        ANALYST_NODE_SPECS["market"], market, ToolNode([echo]),
        max_tool_rounds=2,
    )
    result = graph.invoke({
        "messages": [HumanMessage(content="AAPL")],
        "company_of_interest": "AAPL", "trade_date": "2026-01-15",
        "shared_evidence": [],
    })
    assert result["market_report"] == "Filed report"
    assert any(
        row["source"] == "echo" and "verified quote" in row["content"]
        for row in result["market_evidence"]
    )


@pytest.mark.parametrize("rounds", [0, 50])
def test_tool_round_budget_must_fit_recursion_limit(tmp_path, rounds):
    config = {
        **DEFAULT_CONFIG,
        "parallel_analysts": True,
        "max_tool_rounds": rounds,
        "max_recur_limit": 100,
        "data_cache_dir": str(tmp_path / "cache"),
        "results_dir": str(tmp_path / "results"),
        "memory_log_path": str(tmp_path / "memory.md"),
    }
    with (
        patch.object(TradingAgentsGraph, "_create_role_llm", return_value=object()),
        pytest.raises(ValueError, match="max_tool_rounds"),
    ):
        TradingAgentsGraph(config=config)


def test_background_settlement_requires_parallel_analysts(tmp_path):
    config = {
        **DEFAULT_CONFIG,
        "background_settlement": True,
        "data_cache_dir": str(tmp_path / "cache"),
        "results_dir": str(tmp_path / "results"),
    }
    with (
        patch.object(TradingAgentsGraph, "_create_role_llm", return_value=object()),
        pytest.raises(ValueError, match="background_settlement"),
    ):
        TradingAgentsGraph(config=config)


def test_memory_node_loads_lessons_after_all_ticker_settlement():
    graph = MagicMock(spec=TradingAgentsGraph)
    graph.settle_all_pending.return_value = Settlement(
        failed=[("AAPL", "2026-01-05", "reflection unavailable")]
    )
    graph.memory_log = MagicMock()
    graph.memory_log.get_past_context.return_value = "settled lesson"
    result = TradingAgentsGraph._memory_node(graph, {
        "trade_date": "2026-01-31", "company_of_interest": "NVDA",
    })
    graph.settle_all_pending.assert_called_once_with(
        as_of_date="2026-01-31", wait=False
    )
    assert result["past_context"] == "settled lesson"
    assert "1 past decision" in result["memory_note"]


def test_wrap_up_turn_uses_unbound_model_and_textual_tool_history():
    seen = []
    model = RunnableLambda(
        lambda prompt: seen.append(prompt) or AIMessage(content="Final report")
    )
    prompt = ChatPromptTemplate.from_messages([
        ("system", "Tools: {tool_names}"), MessagesPlaceholder("messages"),
    ])
    messages = [
        AIMessage(content="", tool_calls=[
            {"name": "echo", "args": {"value": "x"}, "id": "1"},
        ]),
        ToolMessage(content="x", tool_call_id="1", name="echo"),
        HumanMessage(content=WRAP_UP),
    ]
    result = take_turn(prompt, model, [], messages)
    assert result.content == "Final report"
    assert seen
    assert all(
        not getattr(message, "tool_calls", None)
        for message in seen[0].messages
    )
