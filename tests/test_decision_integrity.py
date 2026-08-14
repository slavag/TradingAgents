"""Deterministic contracts for finalized Portfolio Manager decisions."""

import pytest
from pydantic import ValidationError

from tradingagents.agents.schemas import (
    DecisionStatus,
    PortfolioDecision,
    PortfolioDecisionDraft,
    PortfolioRating,
    TargetValidationStatus,
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
