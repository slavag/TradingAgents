"""Trader: turns the Research Manager's investment plan into a concrete transaction proposal."""

from __future__ import annotations

import functools

from langchain_core.messages import AIMessage

from tradingagents.agents.schemas import (
    TraderProposal,
    parse_trader_proposal_markdown,
    render_trader_proposal,
)
from tradingagents.agents.utils.agent_utils import (
    get_instrument_context_from_state,
    get_language_instruction,
)
from tradingagents.agents.utils.structured import (
    NO_EXTERNAL_TOOLS,
    bind_structured,
    invoke_structured_or_validated_freetext,
    render_stage_unavailable,
    stage_is_unavailable,
)


def create_trader(llm):
    structured_llm = bind_structured(llm, TraderProposal, "Trader")

    def trader_node(state, name):
        company_name = state["company_of_interest"]
        instrument_context = get_instrument_context_from_state(state)
        investment_plan = state["investment_plan"]

        if stage_is_unavailable(investment_plan, "Research Plan"):
            trader_plan = render_stage_unavailable(
                "Trader Proposal",
                "upstream_research_unavailable",
            )
            return {
                "messages": [AIMessage(content=trader_plan)],
                "trader_investment_plan": trader_plan,
                "sender": name,
            }

        messages = [
            {
                "role": "system",
                "content": (
                    "You are a trading agent analyzing market data to make investment decisions. "
                    "Translate the Research Manager's rating into a transaction direction using this "
                    "fixed mapping: Buy or Overweight -> Buy; Hold -> Hold; Underweight or Sell -> Sell. "
                    "Do not re-adjudicate or reverse the directional rating; use the analysts' reports "
                    "to set execution details, sizing, and risk controls. "
                    + NO_EXTERNAL_TOOLS
                    + get_language_instruction()
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Based on a comprehensive analysis by a team of analysts, here is an investment "
                    f"plan tailored for {company_name}. {instrument_context} This plan incorporates "
                    f"insights from current technical market trends, macroeconomic indicators, and "
                    f"social media sentiment. Use this plan as a foundation for evaluating your next "
                    f"trading decision.\n\nProposed Investment Plan: {investment_plan}\n\n"
                    f"Leverage these insights to make an informed and strategic decision."
                ),
            },
        ]

        trader_plan = invoke_structured_or_validated_freetext(
            structured_llm,
            llm,
            messages,
            render_trader_proposal,
            parse_trader_proposal_markdown,
            "Trader Proposal",
            "Trader",
        )

        return {
            "messages": [AIMessage(content=trader_plan)],
            "trader_investment_plan": trader_plan,
            "sender": name,
        }

    return functools.partial(trader_node, name="Trader")
