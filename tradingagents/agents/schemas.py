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
    price_target: float | None = Field(default=None)
    time_horizon: str | None = Field(default=None)
    confidence_score: int | None = Field(default=None, ge=0, le=100)
    target_summary: str | None = Field(default=None)
    supporting_quote: str | None = Field(default=None)

    @field_validator("price_target", "confidence_score", mode="before")
    @classmethod
    def _nullish_numeric_to_none(cls, v):
        return _coerce_optional_float(v)

    @model_validator(mode="after")
    def _validate_status_rating(self):
        if self.status is DecisionStatus.ACTIONABLE and self.rating is None:
            raise ValueError("actionable decisions require a rating")
        if self.status is not DecisionStatus.ACTIONABLE and self.rating is not None:
            raise ValueError("non-actionable decisions cannot carry a rating")
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
            "Model-rated evidence strength for the recommendation and target, "
            "from 0 to 100. This is an uncalibrated conviction score, not a "
            "statistical probability. Provide it whenever a target is provided."
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

    @field_validator("price_target", "confidence_score", mode="before")
    @classmethod
    def _nullish_numeric_to_none(cls, v):
        return _coerce_optional_float(v)

    @model_validator(mode="after")
    def _validate_final_contract(self):
        if self.status is DecisionStatus.ACTIONABLE and self.rating is None:
            raise ValueError("actionable decisions require a rating")
        if self.status is not DecisionStatus.ACTIONABLE and self.rating is not None:
            raise ValueError("non-actionable decisions cannot carry a rating")

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
    if decision.status_reason:
        parts.extend(["", f"**Decision Reason**: {decision.status_reason}"])
    if decision.price_target is not None:
        parts.extend(["", f"**Price Target**: {decision.price_target}"])
    if decision.time_horizon:
        parts.extend(["", f"**Time Horizon**: {decision.time_horizon}"])
    if decision.confidence_score is not None:
        parts.extend(["", f"**Decision Confidence**: {decision.confidence_score}/100"])
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
