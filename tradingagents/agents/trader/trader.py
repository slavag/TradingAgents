"""Trader: turns the Research Manager's investment plan into a concrete transaction proposal."""

from __future__ import annotations

import functools

from langchain_core.messages import AIMessage

from tradingagents.agents.schemas import (
    ExistingPositionAction,
    NewPositionAction,
    PortfolioRating,
    TraderAction,
    TraderProposal,
    parse_research_plan_markdown,
    parse_trader_proposal_markdown,
    render_trader_proposal,
)
from tradingagents.agents.utils.agent_utils import (
    get_agent_context_from_state,
    get_language_instruction,
)
from tradingagents.agents.utils.decision_integrity import has_numeric_portfolio_weight
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
        instrument_context = get_agent_context_from_state(
            state, "trader", include_reports=True
        )
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
                    "Sell is only the trade side: Underweight means trim an existing position, "
                    "while Sell means exit it; neither implies opening a short. "
                    "Do not re-adjudicate or reverse the directional rating; use the analysts' reports "
                    "to set execution details, sizing, and risk controls. "
                    "Do not invent a numeric portfolio weight or NAV percentage when no holdings, "
                    "portfolio value, or risk budget were provided; use conditional sizing guidance. "
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

        if not state.get("portfolio_context") and has_numeric_portfolio_weight(trader_plan):
            trader_plan = render_stage_unavailable(
                "Trader Proposal", "unsupported_portfolio_sizing"
            )

        if not stage_is_unavailable(trader_plan, "Trader Proposal"):
            try:
                rating = parse_research_plan_markdown(investment_plan).recommendation
                proposal = parse_trader_proposal_markdown(trader_plan)
            except ValueError:
                pass  # Legacy research prose may not have the structured rating labels.
            else:
                intents = {
                    PortfolioRating.BUY: (
                        TraderAction.BUY, ExistingPositionAction.ADD, NewPositionAction.BUY
                    ),
                    PortfolioRating.OVERWEIGHT: (
                        TraderAction.BUY, ExistingPositionAction.ADD,
                        NewPositionAction.CONDITIONAL_BUY,
                    ),
                    PortfolioRating.HOLD: (
                        TraderAction.HOLD, ExistingPositionAction.HOLD, NewPositionAction.WAIT
                    ),
                    PortfolioRating.UNDERWEIGHT: (
                        TraderAction.SELL, ExistingPositionAction.TRIM, NewPositionAction.AVOID
                    ),
                    PortfolioRating.SELL: (
                        TraderAction.SELL, ExistingPositionAction.EXIT, NewPositionAction.AVOID
                    ),
                }
                expected_action, existing_intent, new_intent = intents[rating]
                if proposal.action is not expected_action:
                    trader_plan = render_stage_unavailable(
                        "Trader Proposal", "rating_action_conflict"
                    )
                else:
                    trader_plan = render_trader_proposal(proposal.model_copy(update={
                        "research_rating": rating,
                        "existing_position_intent": existing_intent,
                        "new_position_intent": new_intent,
                    }))

        return {
            "messages": [AIMessage(content=trader_plan)],
            "trader_investment_plan": trader_plan,
            "sender": name,
        }

    return functools.partial(trader_node, name="Trader")
