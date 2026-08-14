"""Deterministic contracts for finalized Portfolio Manager decisions."""

import pytest
from pydantic import ValidationError

from tradingagents.agents.schemas import (
    ConditionalActionPlan,
    DecisionStatus,
    ExistingPositionAction,
    NewPositionAction,
    PortfolioDecision,
    PortfolioDecisionDraft,
    PortfolioRating,
    TargetValidationStatus,
    ThesisRating,
    render_pm_decision,
)
from tradingagents.agents.utils.decision_integrity import (
    build_decision_evidence,
    extract_verified_reference_price,
    finalize_portfolio_decision,
)


def actionable_draft(**overrides):
    values = {
        "status": DecisionStatus.ACTIONABLE,
        "rating": PortfolioRating.BUY,
        "executive_summary": "Build gradually.",
        "investment_thesis": "Verified evidence supports the thesis.",
        "thesis": ThesisRating.BULLISH,
        "existing_position_action": ExistingPositionAction.HOLD,
        "existing_position_summary": "Keep a medium position.",
        "new_position_action": NewPositionAction.CONDITIONAL_BUY,
        "new_position_summary": "Wait for confirmation or a controlled pullback.",
        "recommendation_confidence_score": 72,
        "conditional_plan": ConditionalActionPlan(
            confirmation="Buy after a sustained move above 248-253.",
            alternative="Accumulate near 222, with 211 as deeper support.",
            invalidation="A sustained break below 210 weakens the setup.",
        ),
    }
    values.update(overrides)
    return PortfolioDecisionDraft(**values)


def test_actionable_draft_requires_rating():
    """Removing the actionable/rating guard must make this test fail."""
    with pytest.raises(ValidationError, match="actionable decisions require a rating"):
        PortfolioDecisionDraft(
            status=DecisionStatus.ACTIONABLE,
            rating=None,
            executive_summary="Evidence supports a position.",
            investment_thesis="Supported thesis.",
        )


@pytest.mark.parametrize(
    "status",
    [DecisionStatus.ABSTAIN, DecisionStatus.UNAVAILABLE],
)
def test_non_actionable_draft_rejects_rating(status):
    """A non-actionable technical state must never masquerade as Hold."""
    with pytest.raises(ValidationError, match="non-actionable decisions cannot carry a rating"):
        PortfolioDecisionDraft(
            status=status,
            rating=PortfolioRating.HOLD,
            executive_summary="No actionable decision.",
            investment_thesis="Evidence is insufficient.",
        )


def test_actionable_draft_requires_position_bundle():
    with pytest.raises(ValidationError, match="actionable decisions require position guidance"):
        PortfolioDecisionDraft(
            status=DecisionStatus.ACTIONABLE,
            rating=PortfolioRating.HOLD,
            executive_summary="Maintain exposure.",
            investment_thesis="Evidence is balanced.",
            recommendation_confidence_score=50,
        )


@pytest.mark.parametrize(
    "new_action",
    [NewPositionAction.CONDITIONAL_BUY, NewPositionAction.CONDITIONAL_SELL],
)
def test_conditional_new_position_action_requires_plan(new_action):
    with pytest.raises(ValidationError, match="conditional actions require a conditional plan"):
        actionable_draft(
            new_position_action=new_action,
            conditional_plan=None,
        )


def test_bullish_wait_with_plan_normalizes_to_conditional_buy():
    draft = actionable_draft(new_position_action=NewPositionAction.WAIT)

    assert draft.new_position_action is NewPositionAction.CONDITIONAL_BUY


def test_bearish_wait_with_plan_normalizes_to_conditional_sell():
    draft = actionable_draft(
        thesis=ThesisRating.BEARISH,
        new_position_action=NewPositionAction.WAIT,
    )

    assert draft.new_position_action is NewPositionAction.CONDITIONAL_SELL


def test_wait_without_explicit_watch_plan_is_rejected():
    with pytest.raises(ValidationError, match="wait actions require a conditional plan"):
        actionable_draft(
            thesis=ThesisRating.NEUTRAL,
            new_position_action=NewPositionAction.WAIT,
            conditional_plan=None,
        )


def test_non_conditional_new_position_action_rejects_plan():
    with pytest.raises(ValidationError, match="only conditional actions may carry a conditional plan"):
        actionable_draft(new_position_action=NewPositionAction.AVOID)


def test_actionable_draft_requires_recommendation_confidence():
    with pytest.raises(
        ValidationError,
        match="actionable decisions require recommendation confidence",
    ):
        actionable_draft(recommendation_confidence_score=None)


def test_non_actionable_draft_rejects_position_guidance():
    with pytest.raises(ValidationError, match="non-actionable decisions cannot carry position guidance"):
        PortfolioDecisionDraft(
            status=DecisionStatus.ABSTAIN,
            rating=None,
            executive_summary="Evidence is insufficient.",
            investment_thesis="No directional thesis is supported.",
            thesis=ThesisRating.NEUTRAL,
        )


def test_finalize_preserves_and_renders_position_bundle():
    result = finalize_portfolio_decision(
        actionable_draft(),
        evidence_text="Verified close: 236.22.",
        reference_price=236.22,
    )

    rendered = render_pm_decision(result)

    assert result.thesis is ThesisRating.BULLISH
    assert result.existing_position_action is ExistingPositionAction.HOLD
    assert result.new_position_action is NewPositionAction.CONDITIONAL_BUY
    assert result.conditional_plan is not None
    assert "**Thesis**: Bullish" in rendered
    assert "**Existing Position**: Hold" in rendered
    assert "**New Position**: Conditional Buy" in rendered
    assert "**Conditional Confirmation**: Buy after a sustained move above 248-253." in rendered
    assert "**Conditional Alternative**: Accumulate near 222, with 211 as deeper support." in rendered
    assert "**Conditional Invalidation**: A sustained break below 210 weakens the setup." in rendered


def test_final_decision_rejects_partial_target_bundle():
    """Removing any accepted target field must invalidate the finalized object."""
    with pytest.raises(
        ValidationError,
        match="finalized target bundle must be complete or absent",
    ):
        PortfolioDecision(
            status=DecisionStatus.ACTIONABLE,
            rating=PortfolioRating.BUY,
            executive_summary="Build gradually.",
            investment_thesis="Supported thesis.",
            thesis=ThesisRating.BULLISH,
            existing_position_action=ExistingPositionAction.HOLD,
            existing_position_summary="Keep a medium position.",
            new_position_action=NewPositionAction.CONDITIONAL_BUY,
            new_position_summary="Wait for confirmation.",
            conditional_plan=ConditionalActionPlan(
                confirmation="Buy after a sustained breakout.",
                alternative="Accumulate on a controlled pullback.",
                invalidation="Exit if support fails.",
            ),
            recommendation_confidence_score=72,
            price_target=120.0,
            target_validation_status=TargetValidationStatus.ACCEPTED,
        )


def test_unavailable_factory_renders_explicit_status_without_rating():
    """A structured failure must remain visibly unavailable, never Hold."""
    decision = PortfolioDecision.unavailable("structured_response_invalid")

    rendered = render_pm_decision(decision)

    assert "**Decision Status**: Unavailable" in rendered
    assert "**Rating**" not in rendered
    assert "structured_response_invalid" in rendered


def test_finalize_accepts_complete_verbatim_price_quote():
    """Removing verbatim price provenance must reject this target."""
    draft = actionable_draft(
        price_target=120.0,
        time_horizon="3 months",
        confidence_score=78,
        target_summary="Resistance supports the central case.",
        supporting_quote="Verified close: 100. Resistance: 120.",
    )

    result = finalize_portfolio_decision(
        draft,
        evidence_text="Verified close: 100. Resistance: 120.",
        reference_price=100.0,
    )

    assert result.target_validation_status is TargetValidationStatus.ACCEPTED
    assert result.price_target == 120.0
    assert result.supporting_quote == "Verified close: 100. Resistance: 120."


def test_target_rejection_preserves_recommendation_confidence():
    draft = actionable_draft(
        price_target=130.0,
        time_horizon="3 months",
        confidence_score=78,
        target_summary="Resistance supports the central case.",
        supporting_quote="This quote was not in the evidence: 130.",
    )

    result = finalize_portfolio_decision(
        draft,
        evidence_text="Verified close: 100. Resistance: 120.",
        reference_price=100.0,
    )

    assert result.target_validation_status is TargetValidationStatus.REJECTED
    assert result.price_target is None
    assert result.confidence_score is None
    assert result.recommendation_confidence_score == 72


def test_finalize_repairs_paraphrased_quote_from_exact_price_evidence():
    """A supported target must survive harmless model paraphrasing of its quote."""
    evidence = (
        "Near-term support: 221.94.\n"
        "Near-term resistance: 50 SMA at 248.39.\n"
        "Upper Bollinger band: 253.19."
    )
    draft = actionable_draft(
        price_target=248.39,
        time_horizon="3 months",
        confidence_score=68,
        target_summary="The medium-term resistance is the central case.",
        supporting_quote="The 50-day SMA is near 248.39.",
    )

    result = finalize_portfolio_decision(
        draft,
        evidence_text=evidence,
        reference_price=236.22,
    )

    assert result.target_validation_status is TargetValidationStatus.ACCEPTED
    assert result.price_target == 248.39
    assert result.supporting_quote == "Near-term resistance: 50 SMA at 248.39."


def test_finalize_repairs_quote_from_markdown_price_context():
    """Markdown emphasis around a labelled price must not hide valid evidence."""
    evidence = "- Goldman reportedly raised its price target to **$509**"
    draft = actionable_draft(
        price_target=509.0,
        time_horizon="12 months",
        confidence_score=54,
        target_summary="The published analyst target supplies an upside case.",
        supporting_quote="Goldman's price target was raised to $509.",
    )

    result = finalize_portfolio_decision(
        draft,
        evidence_text=evidence,
        reference_price=400.0,
    )

    assert result.target_validation_status is TargetValidationStatus.ACCEPTED
    assert result.supporting_quote == evidence


@pytest.mark.parametrize(
    ("overrides", "evidence", "reference_price", "expected_reason"),
    [
        (
            {"price_target": 120.0},
            "Resistance: 120.",
            100.0,
            "target_bundle_incomplete",
        ),
        (
            {
                "price_target": -10.0,
                "time_horizon": "3 months",
                "confidence_score": 50,
                "target_summary": "Invalid downside.",
                "supporting_quote": "Support: -10.",
            },
            "Support: -10.",
            100.0,
            "target_not_positive_finite",
        ),
        (
            {
                "price_target": 120.0,
                "time_horizon": "3 months",
                "confidence_score": 70,
                "target_summary": "Missing evidence.",
                "supporting_quote": "",
            },
            "Resistance: 120.",
            100.0,
            "supporting_quote_missing",
        ),
        (
            {
                "price_target": 999.0,
                "time_horizon": "12 months",
                "confidence_score": 90,
                "target_summary": "Fabricated upside.",
                "supporting_quote": "Price target: 999.",
            },
            "Verified close: 100. Resistance: 120.",
            100.0,
            "supporting_quote_not_in_evidence",
        ),
        (
            {
                "price_target": 120.0,
                "time_horizon": "3 months",
                "confidence_score": 70,
                "target_summary": "Wrong cited number.",
                "supporting_quote": "Resistance: 119.",
            },
            "Resistance: 119.",
            100.0,
            "supporting_quote_number_mismatch",
        ),
        (
            {
                "price_target": 120.0,
                "time_horizon": "3 months",
                "confidence_score": 70,
                "target_summary": "A percentage is not a target.",
                "supporting_quote": "Revenue increased 120%.",
            },
            "Revenue increased 120%.",
            100.0,
            "supporting_quote_not_price_context",
        ),
        (
            {
                "price_target": 120.0,
                "time_horizon": "3 months",
                "confidence_score": 70,
                "target_summary": "Share volume is not a target.",
                "supporting_quote": "Price target: 120 million shares.",
            },
            "Price target: 120 million shares.",
            100.0,
            "supporting_quote_not_price_context",
        ),
        (
            {
                "price_target": 90.0,
                "time_horizon": "3 months",
                "confidence_score": 70,
                "target_summary": "Contradicts a Buy rating.",
                "supporting_quote": "Price target: 90.",
            },
            "Verified close: 100. Price target: 90.",
            100.0,
            "target_direction_conflict",
        ),
    ],
)
def test_finalize_rejects_invalid_target_but_preserves_rating(
    overrides,
    evidence,
    reference_price,
    expected_reason,
):
    """Each invalid optional bundle must be removed without erasing the rating."""
    result = finalize_portfolio_decision(
        actionable_draft(**overrides),
        evidence_text=evidence,
        reference_price=reference_price,
    )

    assert result.status is DecisionStatus.ACTIONABLE
    assert result.rating is PortfolioRating.BUY
    assert result.target_validation_status is TargetValidationStatus.REJECTED
    assert result.target_validation_reason == expected_reason
    assert result.price_target is None
    assert result.time_horizon is None
    assert result.confidence_score is None
    assert result.target_summary is None
    assert result.supporting_quote is None


def test_build_decision_evidence_uses_completed_state_sections_only():
    state = {
        "market_report": "Verified close: 100.",
        "sentiment_report": "",
        "investment_plan": "Research plan.",
        "risk_debate_state": {"history": "Risk debate."},
        "unrelated": "must not be included",
    }

    evidence = build_decision_evidence(state)

    assert "[market_report]\nVerified close: 100." in evidence
    assert "[investment_plan]\nResearch plan." in evidence
    assert "[risk_debate]\nRisk debate." in evidence
    assert "must not be included" not in evidence


def test_extract_verified_reference_price_rejects_ambiguous_numbers():
    assert extract_verified_reference_price("Revenue 100; resistance 120.") is None
    assert extract_verified_reference_price("Verified close: $1,234.50") == 1234.5
