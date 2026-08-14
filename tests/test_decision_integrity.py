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
