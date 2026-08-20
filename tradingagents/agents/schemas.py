"""Pydantic schemas used by agents that produce structured output.

The framework's primary artifact is still prose: each agent's natural-language
reasoning is what users read in the saved markdown reports and what the
downstream agents read as context.  Structured output is layered onto the
three decision-making agents (Research Manager, Trader, Portfolio Manager)
so that:

- Their outputs follow consistent section headers across runs and providers
- Each provider's native structured-output mode is used (json_schema for
  OpenAI/xAI, response_schema for Gemini, tool-use for Anthropic)
- Schema field descriptions become the model's output instructions, freeing
  the prompt body to focus on context and the rating-scale guidance
- A render helper turns the parsed Pydantic instance back into the same
  markdown shape the rest of the system already consumes, so display,
  memory log, and saved reports keep working unchanged
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# LLMs sometimes write a placeholder string ("None", "N/A", ...) into an optional
# numeric field instead of omitting it. Coerce those to None so the structured
# call validates instead of erroring (#1058). Pydantic still parses real numeric
# strings ("189.5") to float.
_NULLISH_FLOAT = {"", "none", "n/a", "na", "null", "nil", "-", "tbd", "unknown"}


def _coerce_optional_float(value):
    if isinstance(value, str) and value.strip().lower() in _NULLISH_FLOAT:
        return None
    return value


# ---------------------------------------------------------------------------
# Shared rating types
# ---------------------------------------------------------------------------


class PortfolioRating(str, Enum):
    """5-tier rating used by the Research Manager and Portfolio Manager."""

    BUY = "Buy"
    OVERWEIGHT = "Overweight"
    HOLD = "Hold"
    UNDERWEIGHT = "Underweight"
    SELL = "Sell"


class DecisionStatus(str, Enum):
    """Whether a final Portfolio Manager decision can drive a position signal."""

    ACTIONABLE = "Actionable"
    ABSTAIN = "Abstain"
    UNAVAILABLE = "Unavailable"


class TargetValidationStatus(str, Enum):
    """Deterministic validation state for the optional target bundle."""

    NOT_PROPOSED = "Not Proposed"
    ACCEPTED = "Accepted"
    REJECTED = "Rejected"


class TargetValidationReason(str, Enum):
    """Stable reasons an optional target bundle can be rejected."""

    BUNDLE_INCOMPLETE = "target_bundle_incomplete"
    TARGET_NOT_POSITIVE_FINITE = "target_not_positive_finite"
    SUPPORTING_QUOTE_MISSING = "supporting_quote_missing"
    SUPPORTING_QUOTE_NOT_IN_EVIDENCE = "supporting_quote_not_in_evidence"
    SUPPORTING_QUOTE_NUMBER_MISMATCH = "supporting_quote_number_mismatch"
    SUPPORTING_QUOTE_NOT_PRICE_CONTEXT = "supporting_quote_not_price_context"
    TARGET_DIRECTION_CONFLICT = "target_direction_conflict"


class ThesisRating(str, Enum):
    """Evidence-led directional thesis, separate from portfolio action."""

    STRONGLY_BULLISH = "Strongly Bullish"
    BULLISH = "Bullish"
    NEUTRAL = "Neutral"
    BEARISH = "Bearish"
    STRONGLY_BEARISH = "Strongly Bearish"


class ExistingPositionAction(str, Enum):
    """Action for a reader who already owns the instrument."""

    ADD = "Add"
    HOLD = "Hold"
    TRIM = "Trim"
    EXIT = "Exit"


class NewPositionAction(str, Enum):
    """Action for a reader considering a new long or short position."""

    BUY = "Buy"
    CONDITIONAL_BUY = "Conditional Buy"
    WAIT = "Wait"
    AVOID = "Avoid"
    CONDITIONAL_SELL = "Conditional Sell"
    SELL = "Sell"


class ConditionalActionPlan(BaseModel):
    """Evidence-led conditions for a prospective conditional action."""

    confirmation: str = Field(
        min_length=1,
        description="Primary confirmation required before taking the action.",
    )
    alternative: str | None = Field(
        default=None,
        description="Optional pullback, failed-rally, or alternate entry/exit setup.",
    )
    invalidation: str = Field(
        min_length=1,
        description="Condition that weakens or cancels the prospective setup.",
    )


def _validate_position_guidance(
    *,
    status: DecisionStatus,
    thesis: ThesisRating | None,
    existing_action: ExistingPositionAction | None,
    existing_summary: str | None,
    new_action: NewPositionAction | None,
    new_summary: str | None,
    conditional_plan: ConditionalActionPlan | None,
) -> None:
    required_bundle = (
        thesis,
        existing_action,
        existing_summary,
        new_action,
        new_summary,
    )
    has_guidance = any(value is not None for value in (*required_bundle, conditional_plan))
    if status is not DecisionStatus.ACTIONABLE:
        if has_guidance:
            raise ValueError("non-actionable decisions cannot carry position guidance")
        return

    if not all(value is not None for value in required_bundle):
        raise ValueError("actionable decisions require position guidance")

    requires_plan = new_action in {
        NewPositionAction.CONDITIONAL_BUY,
        NewPositionAction.WAIT,
        NewPositionAction.CONDITIONAL_SELL,
    }
    if new_action is NewPositionAction.WAIT and conditional_plan is None:
        raise ValueError("wait actions require a conditional plan")
    if requires_plan and conditional_plan is None:
        raise ValueError("conditional actions require a conditional plan")
    if not requires_plan and conditional_plan is not None:
        raise ValueError("only conditional actions may carry a conditional plan")


def _normalize_directional_wait(
    thesis: ThesisRating | None,
    new_action: NewPositionAction | None,
) -> NewPositionAction | None:
    """Turn a directional wait into the actionable condition it represents."""
    if new_action is not NewPositionAction.WAIT:
        return new_action
    if thesis in {ThesisRating.BULLISH, ThesisRating.STRONGLY_BULLISH}:
        return NewPositionAction.CONDITIONAL_BUY
    if thesis in {ThesisRating.BEARISH, ThesisRating.STRONGLY_BEARISH}:
        return NewPositionAction.CONDITIONAL_SELL
    return new_action


class TraderAction(str, Enum):
    """3-tier transaction direction used by the Trader.

    The Trader's job is to translate the Research Manager's investment plan
    into a concrete transaction proposal: should the desk execute a Buy, a
    Sell, or sit on Hold this round.  Position sizing and the nuanced
    Overweight / Underweight calls happen later at the Portfolio Manager.
    """

    BUY = "Buy"
    HOLD = "Hold"
    SELL = "Sell"


# ---------------------------------------------------------------------------
# Research Manager
# ---------------------------------------------------------------------------


class ResearchPlan(BaseModel):
    """Structured investment plan produced by the Research Manager.

    Hand-off to the Trader: the recommendation pins the directional view,
    the rationale captures which side of the bull/bear debate carried the
    argument, and the strategic actions translate that into concrete
    instructions the trader can execute against.
    """

    recommendation: PortfolioRating = Field(
        description=(
            "The investment recommendation. Exactly one of Buy / Overweight / "
            "Hold / Underweight / Sell. Reserve Hold for situations where the "
            "evidence on both sides is genuinely balanced; otherwise commit to "
            "the side with the stronger arguments."
        ),
    )
    rationale: str = Field(
        description=(
            "Conversational summary of the key points from both sides of the "
            "debate, ending with which arguments led to the recommendation. "
            "Speak naturally, as if to a teammate."
        ),
    )
    strategic_actions: str = Field(
        description=(
            "Concrete steps for the trader to implement the recommendation, "
            "including position sizing guidance consistent with the rating."
        ),
    )


def render_research_plan(plan: ResearchPlan) -> str:
    """Render a ResearchPlan to markdown for storage and the trader's prompt context."""
    return "\n".join([
        f"**Recommendation**: {plan.recommendation.value}",
        "",
        f"**Rationale**: {plan.rationale}",
        "",
        f"**Strategic Actions**: {plan.strategic_actions}",
    ])


def _markdown_field(text: str, label: str, next_labels: tuple[str, ...]) -> str:
    boundaries = "|".join(re.escape(item) for item in next_labels)
    next_section = (
        rf"^\s*\*\*(?:{boundaries})\*\*\s*:"
        if boundaries
        else r"\Z"
    )
    match = re.search(
        rf"(?ims)^\s*\*\*{re.escape(label)}\*\*\s*:\s*(.*?)"
        rf"(?={next_section}|^\s*FINAL TRANSACTION PROPOSAL:|\Z)",
        text,
    )
    if not match or not match.group(1).strip():
        raise ValueError(f"missing markdown field: {label}")
    return match.group(1).strip()


def _enum_value(enum_type, value: str):
    normalized = value.strip().casefold()
    for member in enum_type:
        if member.value.casefold() == normalized:
            return member
    raise ValueError(f"invalid {enum_type.__name__}: {value}")


def parse_research_plan_markdown(text: str) -> ResearchPlan:
    """Validate compatibility prose and return a typed Research Plan."""
    labels = ("Recommendation", "Rationale", "Strategic Actions")
    return ResearchPlan(
        recommendation=_enum_value(
            PortfolioRating,
            _markdown_field(text, "Recommendation", labels[1:]),
        ),
        rationale=_markdown_field(text, "Rationale", labels[2:]),
        strategic_actions=_markdown_field(text, "Strategic Actions", ()),
    )


# ---------------------------------------------------------------------------
# Trader
# ---------------------------------------------------------------------------


class TraderProposal(BaseModel):
    """Structured transaction proposal produced by the Trader.

    The trader reads the Research Manager's investment plan and the analyst
    reports, then turns them into a concrete transaction: what action to
    take, the reasoning that justifies it, and the practical levels for
    entry, stop-loss, and sizing.
    """

    action: TraderAction = Field(
        description="The transaction direction. Exactly one of Buy / Hold / Sell.",
    )
    reasoning: str = Field(
        description=(
            "The case for this action, anchored in the analysts' reports and "
            "the research plan. Two to four sentences."
        ),
    )
    entry_price: float | None = Field(
        default=None,
        description="Optional entry price target in the instrument's quote currency.",
    )
    stop_loss: float | None = Field(
        default=None,
        description="Optional stop-loss price in the instrument's quote currency.",
    )
    position_sizing: str | None = Field(
        default=None,
        description="Optional sizing guidance, e.g. '5% of portfolio'.",
    )

    @field_validator("entry_price", "stop_loss", mode="before")
    @classmethod
    def _nullish_float_to_none(cls, v):
        return _coerce_optional_float(v)


def render_trader_proposal(proposal: TraderProposal) -> str:
    """Render a TraderProposal to markdown.

    The trailing ``FINAL TRANSACTION PROPOSAL: **BUY/HOLD/SELL**`` line is
    preserved for backward compatibility with the analyst stop-signal text
    and any external code that greps for it.
    """
    parts = [
        f"**Action**: {proposal.action.value}",
        "",
        f"**Reasoning**: {proposal.reasoning}",
    ]
    if proposal.entry_price is not None:
        parts.extend(["", f"**Entry Price**: {proposal.entry_price}"])
    if proposal.stop_loss is not None:
        parts.extend(["", f"**Stop Loss**: {proposal.stop_loss}"])
    if proposal.position_sizing:
        parts.extend(["", f"**Position Sizing**: {proposal.position_sizing}"])
    parts.extend([
        "",
        f"FINAL TRANSACTION PROPOSAL: **{proposal.action.value.upper()}**",
    ])
    return "\n".join(parts)


def parse_trader_proposal_markdown(text: str) -> TraderProposal:
    """Validate compatibility prose and return a typed Trader Proposal."""
    labels = ("Action", "Reasoning", "Entry Price", "Stop Loss", "Position Sizing")
    action = _enum_value(
        TraderAction,
        _markdown_field(text, "Action", labels[1:]),
    )
    final_match = re.search(
        r"(?im)^\s*FINAL TRANSACTION PROPOSAL:\s*\*\*(BUY|HOLD|SELL)\*\*\s*$",
        text,
    )
    if final_match and final_match.group(1).casefold() != action.value.casefold():
        raise ValueError("final transaction action conflicts with Action")

    def optional_field(label: str, following: tuple[str, ...]) -> str | None:
        try:
            return _markdown_field(text, label, following)
        except ValueError:
            return None

    return TraderProposal(
        action=action,
        reasoning=_markdown_field(text, "Reasoning", labels[2:]),
        entry_price=optional_field("Entry Price", labels[3:]),
        stop_loss=optional_field("Stop Loss", labels[4:]),
        position_sizing=optional_field("Position Sizing", ()),
    )


# ---------------------------------------------------------------------------
# Portfolio Manager
# ---------------------------------------------------------------------------


class PortfolioDecisionDraft(BaseModel):
    """Provider-facing Portfolio Manager response before deterministic validation."""

    status: DecisionStatus = DecisionStatus.ACTIONABLE
    rating: PortfolioRating | None = Field(
        default=None,
        description=(
            "A five-tier rating for actionable decisions. Use null for Abstain "
            "or Unavailable."
        ),
    )
    executive_summary: str = Field(
        description="Concise action or non-action summary. Two to four sentences.",
    )
    investment_thesis: str = Field(
        description="Detailed reasoning grounded in the supplied evidence.",
    )
    thesis: ThesisRating | None = Field(
        default=None,
        description="Evidence-led directional thesis, independent of position ownership.",
    )
    existing_position_action: ExistingPositionAction | None = Field(
        default=None,
        description="Action for a reader who already owns the instrument.",
    )
    existing_position_summary: str | None = Field(
        default=None,
        min_length=1,
        description="Sizing and execution guidance for an existing holder.",
    )
    new_position_action: NewPositionAction | None = Field(
        default=None,
        description="Action for a reader considering a new long or short position.",
    )
    new_position_summary: str | None = Field(
        default=None,
        min_length=1,
        description="Concise guidance for a prospective position.",
    )
    conditional_plan: ConditionalActionPlan | None = Field(
        default=None,
        description="Required for Conditional Buy, Conditional Sell, or Wait.",
    )
    recommendation_confidence_score: int | None = Field(
        default=None,
        ge=0,
        le=100,
        description=(
            "Uncalibrated evidence-strength score for the recommendation, "
            "independent of whether a numeric target is validated."
        ),
    )
    price_target: float | None = Field(default=None)
    time_horizon: str | None = Field(default=None)
    confidence_score: int | None = Field(
        default=None,
        ge=0,
        le=100,
        description="Uncalibrated evidence-strength score for the numeric target.",
    )
    target_summary: str | None = Field(default=None)
    supporting_quote: str | None = Field(default=None)

    @field_validator(
        "price_target",
        "confidence_score",
        "recommendation_confidence_score",
        mode="before",
    )
    @classmethod
    def _nullish_numeric_to_none(cls, v):
        return _coerce_optional_float(v)

    @model_validator(mode="after")
    def _validate_status_rating(self):
        if self.status is DecisionStatus.ACTIONABLE and self.rating is None:
            raise ValueError("actionable decisions require a rating")
        if self.status is not DecisionStatus.ACTIONABLE and self.rating is not None:
            raise ValueError("non-actionable decisions cannot carry a rating")
        if (
            self.status is DecisionStatus.ACTIONABLE
            and self.recommendation_confidence_score is None
        ):
            raise ValueError("actionable decisions require recommendation confidence")
        if (
            self.status is not DecisionStatus.ACTIONABLE
            and self.recommendation_confidence_score is not None
        ):
            raise ValueError("non-actionable decisions cannot carry recommendation confidence")
        self.new_position_action = _normalize_directional_wait(
            self.thesis,
            self.new_position_action,
        )
        _validate_position_guidance(
            status=self.status,
            thesis=self.thesis,
            existing_action=self.existing_position_action,
            existing_summary=self.existing_position_summary,
            new_action=self.new_position_action,
            new_summary=self.new_position_summary,
            conditional_plan=self.conditional_plan,
        )
        return self


class PortfolioDecision(BaseModel):
    """Final Portfolio Manager decision after deterministic validation."""

    status: DecisionStatus = DecisionStatus.ACTIONABLE
    rating: PortfolioRating | None = Field(
        default=None,
        description=(
            "The final position rating. Exactly one of Buy / Overweight / Hold / "
            "Underweight / Sell for actionable decisions; null otherwise."
        ),
    )
    executive_summary: str = Field(
        description=(
            "A concise action plan covering entry strategy, position sizing, "
            "key risk levels, and time horizon. Two to four sentences."
        ),
    )
    investment_thesis: str = Field(
        description=(
            "Detailed reasoning anchored in specific evidence from the analysts' "
            "debate. If prior lessons are referenced in the prompt context, "
            "incorporate them; otherwise rely solely on the current analysis."
        ),
    )
    thesis: ThesisRating | None = None
    existing_position_action: ExistingPositionAction | None = None
    existing_position_summary: str | None = Field(default=None, min_length=1)
    new_position_action: NewPositionAction | None = None
    new_position_summary: str | None = Field(default=None, min_length=1)
    conditional_plan: ConditionalActionPlan | None = None
    recommendation_confidence_score: int | None = Field(
        default=None,
        ge=0,
        le=100,
        description=(
            "Uncalibrated evidence-strength score for the recommendation, "
            "independent of target validation."
        ),
    )
    price_target: float | None = Field(
        default=None,
        description=(
            "Central-case price target in the instrument's quote currency at the "
            "stated time horizon. Provide it whenever the debate contains a "
            "verified reference price and evidence-backed valuation, support, or "
            "resistance levels; use null only when the available evidence cannot "
            "support a numeric target."
        ),
    )
    time_horizon: str | None = Field(
        default=None,
        description=(
            "Time horizon for the central-case price target, e.g. '3-6 months'. "
            "Provide it whenever a price target is provided."
        ),
    )
    confidence_score: int | None = Field(
        default=None,
        ge=0,
        le=100,
        description=(
            "Model-rated evidence strength for the numeric target, from 0 to 100. "
            "This is an uncalibrated conviction score, not a statistical "
            "probability. Provide it whenever a target is provided."
        ),
    )
    target_summary: str | None = Field(
        default=None,
        description=(
            "One or two concise sentences identifying the evidence and quoted "
            "levels that support the price target and confidence score."
        ),
    )
    supporting_quote: str | None = Field(
        default=None,
        description=(
            "Verbatim quote from the supplied evidence containing the exact "
            "target and nearby price-level context."
        ),
    )
    target_validation_status: TargetValidationStatus = (
        TargetValidationStatus.NOT_PROPOSED
    )
    target_validation_reason: str | None = None
    status_reason: str | None = None

    @field_validator(
        "price_target",
        "confidence_score",
        "recommendation_confidence_score",
        mode="before",
    )
    @classmethod
    def _nullish_numeric_to_none(cls, v):
        return _coerce_optional_float(v)

    @model_validator(mode="after")
    def _validate_final_contract(self):
        if self.status is DecisionStatus.ACTIONABLE and self.rating is None:
            raise ValueError("actionable decisions require a rating")
        if self.status is not DecisionStatus.ACTIONABLE and self.rating is not None:
            raise ValueError("non-actionable decisions cannot carry a rating")
        if (
            self.status is DecisionStatus.ACTIONABLE
            and self.recommendation_confidence_score is None
        ):
            raise ValueError("actionable decisions require recommendation confidence")
        if (
            self.status is not DecisionStatus.ACTIONABLE
            and self.recommendation_confidence_score is not None
        ):
            raise ValueError("non-actionable decisions cannot carry recommendation confidence")
        self.new_position_action = _normalize_directional_wait(
            self.thesis,
            self.new_position_action,
        )
        _validate_position_guidance(
            status=self.status,
            thesis=self.thesis,
            existing_action=self.existing_position_action,
            existing_summary=self.existing_position_summary,
            new_action=self.new_position_action,
            new_summary=self.new_position_summary,
            conditional_plan=self.conditional_plan,
        )

        bundle = (
            self.price_target,
            self.time_horizon,
            self.confidence_score,
            self.target_summary,
            self.supporting_quote,
        )
        has_any = any(value is not None for value in bundle)
        has_all = all(value is not None for value in bundle)
        if has_any and not has_all:
            raise ValueError("finalized target bundle must be complete or absent")
        if (
            self.target_validation_status is TargetValidationStatus.ACCEPTED
            and not has_all
        ):
            raise ValueError("accepted target validation requires a complete bundle")
        if (
            self.target_validation_status is not TargetValidationStatus.ACCEPTED
            and has_any
        ):
            raise ValueError("only accepted target validation may retain target fields")
        if (
            self.target_validation_status is TargetValidationStatus.REJECTED
            and not self.target_validation_reason
        ):
            raise ValueError("rejected target validation requires a reason")
        if self.status is not DecisionStatus.ACTIONABLE and has_any:
            raise ValueError("non-actionable decisions cannot carry a target bundle")
        return self

    @classmethod
    def unavailable(cls, reason_code: str) -> PortfolioDecision:
        """Create a sanitized non-actionable decision for a technical failure."""
        return cls(
            status=DecisionStatus.UNAVAILABLE,
            rating=None,
            executive_summary="Final decision unavailable.",
            investment_thesis=(
                "The Portfolio Manager could not produce a validated structured decision."
            ),
            status_reason=reason_code,
        )


def render_pm_decision(decision: PortfolioDecision) -> str:
    """Render a PortfolioDecision back to the markdown shape the rest of the system expects.

    Memory log, CLI display, and saved report files all read this markdown,
    so the rendered output preserves the exact section headers (``**Rating**``,
    ``**Executive Summary**``, ``**Investment Thesis**``) that downstream
    parsers and the report writers already handle.
    """
    parts = [f"**Decision Status**: {decision.status.value}"]
    if decision.rating is not None:
        parts.extend(["", f"**Rating**: {decision.rating.value}"])
    parts.extend([
        "",
        f"**Executive Summary**: {decision.executive_summary}",
        "",
        f"**Investment Thesis**: {decision.investment_thesis}",
    ])
    if decision.thesis is not None:
        parts.extend(["", f"**Thesis**: {decision.thesis.value}"])
    if decision.existing_position_action is not None:
        parts.extend([
            "",
            f"**Existing Position**: {decision.existing_position_action.value}",
            "",
            f"**Existing Position Guidance**: {decision.existing_position_summary}",
        ])
    if decision.new_position_action is not None:
        parts.extend([
            "",
            f"**New Position**: {decision.new_position_action.value}",
            "",
            f"**New Position Guidance**: {decision.new_position_summary}",
        ])
    if decision.conditional_plan is not None:
        parts.extend([
            "",
            f"**Conditional Confirmation**: {decision.conditional_plan.confirmation}",
        ])
        if decision.conditional_plan.alternative:
            parts.extend([
                "",
                f"**Conditional Alternative**: {decision.conditional_plan.alternative}",
            ])
        parts.extend([
            "",
            f"**Conditional Invalidation**: {decision.conditional_plan.invalidation}",
        ])
    if decision.recommendation_confidence_score is not None:
        parts.extend([
            "",
            f"**Recommendation Confidence**: {decision.recommendation_confidence_score}/100",
        ])
    if decision.status_reason:
        parts.extend(["", f"**Decision Reason**: {decision.status_reason}"])
    if decision.price_target is not None:
        parts.extend(["", f"**Price Target**: {decision.price_target}"])
    if decision.time_horizon:
        parts.extend(["", f"**Time Horizon**: {decision.time_horizon}"])
    if decision.confidence_score is not None:
        parts.extend(["", f"**Target Confidence**: {decision.confidence_score}/100"])
    if decision.target_summary:
        parts.extend(["", f"**Target Rationale**: {decision.target_summary}"])
    if decision.supporting_quote:
        parts.extend(["", f"**Target Supporting Quote**: {decision.supporting_quote}"])
    if decision.target_validation_status is TargetValidationStatus.REJECTED:
        parts.extend([
            "",
            f"**Target Validation**: Rejected ({decision.target_validation_reason})",
        ])
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Sentiment Analyst
# ---------------------------------------------------------------------------


class SentimentBand(str, Enum):
    """Discrete sentiment direction produced by the Sentiment Analyst.

    Six tiers keep the signal granular enough to be actionable while remaining
    small enough for every provider to map reliably from its JSON output.
    """

    BULLISH = "Bullish"
    MILDLY_BULLISH = "Mildly Bullish"
    NEUTRAL = "Neutral"
    MIXED = "Mixed"
    MILDLY_BEARISH = "Mildly Bearish"
    BEARISH = "Bearish"


class SentimentReport(BaseModel):
    """Structured sentiment report produced by the Sentiment Analyst.

    Replaces the previous free-form prose output so downstream consumers
    (dashboards, audit logs, PDF renderers, other agents) can read
    ``overall_band`` and ``overall_score`` without maintaining fragile regex
    fallbacks that drift with every model release. ``narrative`` preserves the
    rich source-by-source analysis; ``render_sentiment_report`` prepends a
    deterministic header so the saved report stays human-readable.
    """

    overall_band: SentimentBand = Field(
        description=(
            "Overall sentiment direction. Exactly one of: "
            "Bullish / Mildly Bullish / Neutral / Mixed / Mildly Bearish / Bearish. "
            "Use Mixed when sources point in clearly different directions. "
            "Use Neutral only when all sources are genuinely silent or non-committal."
        ),
    )
    overall_score: float = Field(
        ge=0.0,
        le=10.0,
        description=(
            "Numeric sentiment intensity on a 0–10 scale. "
            "0 = maximally bearish, 5 = neutral, 10 = maximally bullish. "
            "Guideline for consistency with overall_band: "
            "Bullish ~6.5–10, Mildly Bullish ~5.5–6.4, Neutral/Mixed ~4.5–5.5, "
            "Mildly Bearish ~3.5–4.4, Bearish ~0–3.4. "
            "Only the 0–10 bounds are enforced."
        ),
    )
    confidence: Literal["low", "medium", "high"] = Field(
        description=(
            "Confidence in the assessment based on data quality and sample size. "
            "Use 'low' when one or more sources returned a placeholder or fewer "
            "than 5 data points; 'medium' when data is present but sparse; "
            "'high' when all three sources returned substantive data."
        ),
    )
    narrative: str = Field(
        description=(
            "Full sentiment report covering, in order: "
            "(1) source-by-source breakdown with specific evidence (cite message "
            "counts, ratios, notable posts); "
            "(2) cross-source divergences and alignments; "
            "(3) dominant narrative themes; "
            "(4) catalysts and risks surfaced by the data; "
            "(5) a markdown table summarising key sentiment signals, their "
            "direction, source, and supporting evidence. "
            "Keep it informative and substantive: develop each section thoroughly "
            "with concrete evidence so every point adds new signal for the trader."
        ),
    )


def render_sentiment_report(report: SentimentReport) -> str:
    """Render a SentimentReport to the markdown shape the rest of the system expects.

    The structured header (band + score + confidence) is prepended to the
    narrative so the saved report is both human-readable and machine-parseable
    without regex.
    """
    return "\n".join([
        f"**Overall Sentiment:** **{report.overall_band.value}** "
        f"(Score: {report.overall_score:.1f}/10)",
        f"**Confidence:** {report.confidence.capitalize()}",
        "",
        report.narrative,
    ])
