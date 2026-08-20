"""Integration tests for the strict Portfolio Manager decision boundary."""

from unittest.mock import MagicMock

import pytest

from tradingagents.agents.managers.portfolio_manager import create_portfolio_manager
from tradingagents.agents.schemas import (
    ConditionalActionPlan,
    DecisionStatus,
    ExistingPositionAction,
    NewPositionAction,
    PortfolioDecisionDraft,
    PortfolioRating,
    ThesisRating,
)
from tradingagents.agents.utils.structured import render_stage_unavailable


def make_pm_state(market_report: str = "Verified close: 100. Resistance: 120."):
    return {
        "company_of_interest": "TEST",
        "market_report": market_report,
        "sentiment_report": "Sentiment is mixed.",
        "news_report": "No material news.",
        "fundamentals_report": "Cash flow is stable.",
        "investment_plan": "Research plan supports Buy.",
        "trader_investment_plan": "Trader proposes Buy.",
        "past_context": "",
        "risk_debate_state": {
            "history": "Risk debate supports gradual entry.",
            "aggressive_history": "",
            "conservative_history": "",
            "neutral_history": "",
            "judge_decision": "",
            "current_aggressive_response": "",
            "current_conservative_response": "",
            "current_neutral_response": "",
            "latest_speaker": "Neutral",
            "count": 1,
        },
    }


def actionable_draft(**overrides):
    values = {
        "status": DecisionStatus.ACTIONABLE,
        "rating": PortfolioRating.BUY,
        "executive_summary": "Build gradually.",
        "investment_thesis": "Evidence supports the thesis.",
        "thesis": ThesisRating.BULLISH,
        "existing_position_action": ExistingPositionAction.HOLD,
        "existing_position_summary": "Keep a medium position.",
        "new_position_action": NewPositionAction.CONDITIONAL_BUY,
        "new_position_summary": "Wait for confirmation or a controlled pullback.",
        "recommendation_confidence_score": 72,
        "conditional_plan": ConditionalActionPlan(
            confirmation="Buy after a sustained move above 110-120.",
            alternative="Accumulate near support at 100.",
            invalidation="A sustained break below 90 weakens the setup.",
        ),
    }
    values.update(overrides)
    return PortfolioDecisionDraft(**values)


def configured_llm(*, result=None, invoke_error: Exception | None = None):
    structured = MagicMock()
    if invoke_error is not None:
        structured.invoke.side_effect = invoke_error
    else:
        structured.invoke.return_value = result
    llm = MagicMock()
    llm.with_structured_output.return_value = structured
    llm.invoke.return_value = MagicMock(content="**Rating**: Hold")
    return llm


def test_unsupported_structured_binding_returns_unavailable_without_plain_retry():
    """Restoring a plain fallback would make this return Hold instead."""
    llm = MagicMock()
    llm.with_structured_output.side_effect = NotImplementedError("unsupported")
    llm.invoke.return_value = MagicMock(content="**Rating**: Hold")

    result = create_portfolio_manager(llm)(make_pm_state())["final_trade_decision"]

    assert "**Decision Status**: Unavailable" in result
    assert "structured_binding_unsupported" in result
    assert "**Rating**" not in result
    llm.invoke.assert_not_called()


def test_unexpected_structured_binding_failure_is_sanitized():
    """A binding-time provider error must not abort graph construction or leak details."""
    llm = MagicMock()
    llm.with_structured_output.side_effect = RuntimeError("secret binding detail")

    result = create_portfolio_manager(llm)(make_pm_state())["final_trade_decision"]

    assert "**Decision Status**: Unavailable" in result
    assert "structured_binding_unsupported" in result
    assert "secret binding detail" not in result


@pytest.mark.parametrize(
    ("llm", "expected_code"),
    [
        (configured_llm(result=None), "structured_response_missing"),
        (configured_llm(result={"rating": "Buy"}), "structured_response_invalid"),
        (
            configured_llm(invoke_error=RuntimeError("secret provider detail")),
            "structured_invocation_failed",
        ),
    ],
)
def test_structured_failure_returns_sanitized_unavailable(llm, expected_code):
    result = create_portfolio_manager(llm)(make_pm_state())["final_trade_decision"]

    assert "**Decision Status**: Unavailable" in result
    assert expected_code in result
    assert "secret provider detail" not in result
    assert "**Rating**" not in result
    llm.invoke.assert_not_called()


def test_unavailable_trader_proposal_failcloses_final_decision_without_model_call():
    llm = configured_llm(result=actionable_draft())
    state = make_pm_state()
    state["trader_investment_plan"] = render_stage_unavailable(
        "Trader Proposal",
        "freetext_response_invalid",
    )

    result = create_portfolio_manager(llm)(state)["final_trade_decision"]

    assert "**Decision Status**: Unavailable" in result
    assert "upstream_trader_unavailable" in result
    assert "**Rating**" not in result
    llm.with_structured_output.return_value.invoke.assert_not_called()


def test_primary_target_without_verbatim_quote_is_removed_but_rating_survives():
    draft = actionable_draft(
        price_target=999.0,
        time_horizon="12 months",
        confidence_score=90,
        target_summary="Large upside.",
        supporting_quote="Price target: 999.",
    )

    result = create_portfolio_manager(configured_llm(result=draft))(
        make_pm_state()
    )["final_trade_decision"]

    assert "**Decision Status**: Actionable" in result
    assert "**Rating**: Buy" in result
    assert "**Price Target**" not in result
    assert "supporting_quote_not_in_evidence" in result


def test_primary_target_with_verbatim_quote_is_rendered():
    quote = "Verified close: 100. Resistance: 120."
    draft = actionable_draft(
        price_target=120.0,
        time_horizon="3 months",
        confidence_score=78,
        target_summary="Resistance supports the central case.",
        supporting_quote=quote,
    )

    result = create_portfolio_manager(configured_llm(result=draft))(
        make_pm_state(market_report=quote)
    )["final_trade_decision"]

    assert "**Price Target**: 120.0" in result
    assert f"**Target Supporting Quote**: {quote}" in result
    assert "Target Validation" not in result


def test_prompt_defines_independent_thesis_and_symmetric_conditional_actions():
    llm = configured_llm(result=actionable_draft())

    create_portfolio_manager(llm)(make_pm_state())

    structured = llm.with_structured_output.return_value
    prompt = structured.invoke.call_args.args[0]
    assert "Determine the thesis from evidence before choosing position actions" in prompt
    assert "Treat upstream recommendations as arguments, not independent votes" in prompt
    assert "Conditional Buy requires" in prompt
    assert "Conditional Sell requires" in prompt
    assert "Wait requires" in prompt
    assert "Recommendation Confidence" in prompt
    assert "Never invent a price level" in prompt
