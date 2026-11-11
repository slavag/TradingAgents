# TradingAgents/graph/setup.py

import logging
from collections import Counter
from dataclasses import replace
from typing import Any, TypedDict

from langchain_core.messages import HumanMessage
from langgraph.graph import END, START, StateGraph
from langgraph.prebuilt import ToolNode

from tradingagents.agents import (
    create_aggressive_debator,
    create_bear_researcher,
    create_bull_researcher,
    create_conservative_debator,
    create_fundamentals_analyst,
    create_market_analyst,
    create_msg_delete,
    create_neutral_debator,
    create_news_analyst,
    create_portfolio_manager,
    create_research_manager,
    create_sentiment_analyst,
    create_trader,
)
from tradingagents.agents.analysts.turn import WRAP_UP
from tradingagents.agents.utils.agent_states import AgentState
from tradingagents.agents.utils.evidence import (
    collect_shared_evidence,
    merge_evidence,
    tool_message_evidence,
)
from tradingagents.llm_clients.prompt_cache import PromptCachePolicy

from .analyst_execution import build_analyst_execution_plan
from .conditional_logic import ConditionalLogic

logger = logging.getLogger(__name__)

def _next_after_tools(agent_node: str, wrap_node: str, max_tool_rounds: int):
    def route(state):
        rounds = sum(
            bool(getattr(message, "tool_calls", None))
            for message in state["messages"]
        )
        return wrap_node if rounds >= max_tool_rounds else agent_node

    return route


def _wrap_up_node(spec, agent, max_tool_rounds: int):
    def wrap_up(state):
        calls = Counter(
            call["name"]
            for message in state["messages"]
            for call in (getattr(message, "tool_calls", None) or [])
        )
        logger.warning(
            "%s reached %d tool rounds (%s)", spec.agent_node,
            max_tool_rounds,
            ", ".join(f"{name} x{count}" for name, count in calls.most_common()),
        )
        try:
            result = agent({
                **state,
                "messages": [*state["messages"], HumanMessage(WRAP_UP)],
            })
        except Exception as exc:
            logger.warning(
                "%s final report turn failed (%s)",
                spec.agent_node, type(exc).__name__,
            )
            result = {}
        if not result.get(spec.report_key):
            result[spec.report_key] = (
                f"DATA_UNAVAILABLE: {spec.agent_node} could not produce a "
                f"report after {max_tool_rounds} tool rounds."
            )
        return result

    return wrap_up


def _parallel_analyst_graph(spec, agent, tool_node: ToolNode, max_tool_rounds: int):
    evidence_key = f"{spec.key}_evidence"
    fields = {spec.report_key: str, evidence_key: list}
    if spec.key == "market":
        fields["verified_market_snapshot"] = dict
    output = TypedDict(f"{spec.key.capitalize()}Output", fields, total=False)
    graph = StateGraph(AgentState, output_schema=output)
    graph.add_node("agent", agent)
    graph.add_edge(START, "agent")

    def handoff(state):
        run_date = str(state.get("trade_date") or "")
        result = {
            spec.report_key: state.get(spec.report_key, ""),
            evidence_key: merge_evidence(
                state.get("shared_evidence", []), tool_message_evidence(state),
                run_date,
            ),
        }
        if spec.key == "market" and state.get("verified_market_snapshot"):
            result["verified_market_snapshot"] = state["verified_market_snapshot"]
        return result

    graph.add_node("handoff", handoff)
    if spec.key == "social":
        graph.add_edge("agent", "handoff")
    else:
        graph.add_node("tools", tool_node)
        graph.add_node("wrap_up", _wrap_up_node(spec, agent, max_tool_rounds))
        graph.add_conditional_edges(
            "agent",
            lambda state: "tools" if state["messages"][-1].tool_calls else "handoff",
            ["tools", "handoff"],
        )
        graph.add_conditional_edges(
            "tools", _next_after_tools("agent", "wrap_up", max_tool_rounds),
            ["agent", "wrap_up"],
        )
        graph.add_edge("wrap_up", "handoff")
    graph.add_edge("handoff", END)
    return graph.compile()

# Every target a shared conditional router can return. Each edge driven by the
# router maps all of them, so a fall-through return (e.g. under prompt/i18n/
# refactor drift in the speaker labels) can never hit a missing path_map entry
# and crash LangGraph mid-run (#1088).
DEBATE_PATH_MAP = {
    "Bull Researcher": "Bull Researcher",
    "Bear Researcher": "Bear Researcher",
    "Research Manager": "Research Manager",
}
RISK_ANALYSIS_PATH_MAP = {
    "Aggressive Analyst": "Aggressive Analyst",
    "Conservative Analyst": "Conservative Analyst",
    "Neutral Analyst": "Neutral Analyst",
    "Portfolio Manager": "Portfolio Manager",
}


class GraphSetup:
    """Handles the setup and configuration of the agent graph."""

    def __init__(
        self,
        quick_thinking_llm: Any,
        deep_thinking_llm: Any,
        tool_nodes: dict[str, ToolNode],
        conditional_logic: ConditionalLogic,
        quick_cache_policy: PromptCachePolicy | None = None,
    ):
        """Initialize with required components."""
        self.quick_thinking_llm = quick_thinking_llm
        self.deep_thinking_llm = deep_thinking_llm
        self.tool_nodes = tool_nodes
        self.conditional_logic = conditional_logic
        self.quick_cache_policy = quick_cache_policy or PromptCachePolicy("", "")

    def _policy_for_rounds(self, rounds: int) -> PromptCachePolicy:
        return replace(
            self.quick_cache_policy,
            expected_reuse=self.quick_cache_policy.enabled and rounds > 1,
        )

    def setup_graph(
        self, selected_analysts=("market", "social", "news", "fundamentals"),
        *, parallel_analysts: bool = False, max_tool_rounds: int = 8,
        background_settlement: bool = False, memory_node=None,
    ):
        """Set up and compile the agent workflow graph.

        Args:
            selected_analysts (list): List of analyst types to include. Options are:
                - "market": Market analyst
                - "social": Social media analyst
                - "news": News analyst
                - "fundamentals": Fundamentals analyst
        """
        plan = build_analyst_execution_plan(selected_analysts)
        if background_settlement and (not parallel_analysts or memory_node is None):
            raise ValueError("background settlement needs parallel analysts and a memory node")

        analyst_factories = {
            "market": lambda: create_market_analyst(self.quick_thinking_llm),
            "social": lambda: create_sentiment_analyst(self.quick_thinking_llm),
            "news": lambda: create_news_analyst(self.quick_thinking_llm),
            "fundamentals": lambda: create_fundamentals_analyst(self.quick_thinking_llm),
        }

        # Create researcher and manager nodes
        research_cache = self._policy_for_rounds(self.conditional_logic.max_debate_rounds)
        bull_researcher_node = create_bull_researcher(
            self.quick_thinking_llm, cache_policy=research_cache,
        )
        bear_researcher_node = create_bear_researcher(
            self.quick_thinking_llm, cache_policy=research_cache,
        )
        research_manager_node = create_research_manager(self.deep_thinking_llm)
        trader_node = create_trader(self.quick_thinking_llm)

        # Create risk analysis nodes
        risk_cache = self._policy_for_rounds(self.conditional_logic.max_risk_discuss_rounds)
        aggressive_analyst = create_aggressive_debator(
            self.quick_thinking_llm, cache_policy=risk_cache,
        )
        neutral_analyst = create_neutral_debator(
            self.quick_thinking_llm, cache_policy=risk_cache,
        )
        conservative_analyst = create_conservative_debator(
            self.quick_thinking_llm, cache_policy=risk_cache,
        )
        portfolio_manager_node = create_portfolio_manager(self.deep_thinking_llm)

        # Create workflow
        workflow = StateGraph(AgentState)

        # Add analyst nodes to the graph
        for spec in plan.specs:
            agent = analyst_factories[spec.key]()
            if parallel_analysts:
                workflow.add_node(
                    spec.agent_node,
                    _parallel_analyst_graph(
                        spec, agent, self.tool_nodes[spec.key], max_tool_rounds,
                    ),
                )
            else:
                workflow.add_node(spec.agent_node, agent)
                workflow.add_node(spec.clear_node, create_msg_delete())
                workflow.add_node(spec.tool_node, self.tool_nodes[spec.key])
                workflow.add_node(
                    f"wrap_up_{spec.key}",
                    _wrap_up_node(spec, agent, max_tool_rounds),
                )

        # Add other nodes
        workflow.add_node("Evidence Collector", collect_shared_evidence)
        workflow.add_node("Bull Researcher", bull_researcher_node)
        workflow.add_node("Bear Researcher", bear_researcher_node)
        workflow.add_node("Research Manager", research_manager_node)
        workflow.add_node("Trader", trader_node)
        workflow.add_node("Aggressive Analyst", aggressive_analyst)
        workflow.add_node("Neutral Analyst", neutral_analyst)
        workflow.add_node("Conservative Analyst", conservative_analyst)
        workflow.add_node("Portfolio Manager", portfolio_manager_node)
        if background_settlement:
            workflow.add_node("Memory Log", memory_node)

        # Define edges
        workflow.add_edge(START, "Evidence Collector")
        if parallel_analysts:
            analysts = [spec.agent_node for spec in plan.specs]
            for node in analysts:
                workflow.add_edge("Evidence Collector", node)
            if background_settlement:
                workflow.add_edge("Evidence Collector", "Memory Log")

            def combine_analyst_evidence(state):
                combined = state.get("shared_evidence", [])
                run_date = str(state.get("trade_date") or "")
                for spec in plan.specs:
                    combined = merge_evidence(
                        combined, state.get(f"{spec.key}_evidence", []), run_date
                    )
                return {"shared_evidence": combined}

            workflow.add_node("Analyst Evidence", combine_analyst_evidence)
            workflow.add_edge(
                [*analysts, "Memory Log"] if background_settlement else analysts,
                "Analyst Evidence",
            )
            workflow.add_edge("Analyst Evidence", "Bull Researcher")
        else:
            workflow.add_edge("Evidence Collector", plan.specs[0].agent_node)
            for i, spec in enumerate(plan.specs):
                current_analyst = spec.agent_node
                current_tools = spec.tool_node
                current_clear = spec.clear_node
                workflow.add_conditional_edges(
                    current_analyst,
                    getattr(self.conditional_logic, f"should_continue_{spec.key}"),
                    [current_tools, current_clear],
                )
                wrap_node = f"wrap_up_{spec.key}"
                workflow.add_conditional_edges(
                    current_tools,
                    _next_after_tools(current_analyst, wrap_node, max_tool_rounds),
                    [current_analyst, wrap_node],
                )
                workflow.add_edge(wrap_node, current_clear)
                if i < len(plan.specs) - 1:
                    workflow.add_edge(current_clear, plan.specs[i + 1].agent_node)
                else:
                    workflow.add_edge(current_clear, "Bull Researcher")

        # Both research-debate edges share the complete DEBATE_PATH_MAP (#1088).
        for debate_node in ("Bull Researcher", "Bear Researcher"):
            workflow.add_conditional_edges(
                debate_node,
                self.conditional_logic.should_continue_debate,
                DEBATE_PATH_MAP,
            )
        workflow.add_edge("Research Manager", "Trader")
        workflow.add_edge("Trader", "Aggressive Analyst")
        # All three risk edges share the complete RISK_ANALYSIS_PATH_MAP (#1088).
        for risk_node in ("Aggressive Analyst", "Conservative Analyst", "Neutral Analyst"):
            workflow.add_conditional_edges(
                risk_node,
                self.conditional_logic.should_continue_risk_analysis,
                RISK_ANALYSIS_PATH_MAP,
            )

        workflow.add_edge("Portfolio Manager", END)

        return workflow
