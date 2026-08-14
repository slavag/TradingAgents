"""Deterministic validation for final Portfolio Manager decisions."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from typing import Any

from tradingagents.agents.schemas import (
    PortfolioDecision,
    PortfolioDecisionDraft,
    PortfolioRating,
    TargetValidationReason,
    TargetValidationStatus,
)

_EVIDENCE_FIELDS = (
    "market_report",
    "sentiment_report",
    "news_report",
    "fundamentals_report",
    "investment_plan",
    "trader_investment_plan",
    "past_context",
)

_PRICE_LABEL = (
    r"(?:price(?:\s+target)?|target(?:\s+price)?|reference\s+price|"
    r"close|open|high|low|support|resistance|sma|ema|vwma|"
    r"bollinger(?:\s+(?:middle|upper|lower))?(?:\s+band)?|"
    r"fair\s+value|valuation|price\s+level|level)"
)
_CURRENCY_MARKER = r"(?:[$€£¥]|\b(?:USD|EUR|GBP|JPY|CAD|AUD|CHF|CNY|HKD)\b)"
_CONNECTOR = (
    r"(?:\s|[:=~≈@()\[\],-]|"
    r"\b(?:is|at|near|around|of|about|approximately|approx|roughly|"
    r"estimate|estimated|to)\b)*"
)
_NON_PRICE_SUFFIX = re.compile(
    r"\s*(?:%|percent\b|x\b|shares?\b|units?\b|volume\b|"
    r"thousand\b|million\b|billion\b|trillion\b|[KMBT]\b)",
    re.IGNORECASE,
)


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def build_decision_evidence(state: Mapping[str, Any]) -> str:
    """Build the deterministic evidence corpus used to validate a final target."""
    sections: list[str] = []
    for key in _EVIDENCE_FIELDS:
        value = state.get(key)
        if isinstance(value, str) and value.strip():
            sections.append(f"[{key}]\n{value.strip()}")

    risk_state = state.get("risk_debate_state")
    if isinstance(risk_state, Mapping):
        history = risk_state.get("history")
        if isinstance(history, str) and history.strip():
            sections.append(f"[risk_debate]\n{history.strip()}")
    return "\n\n".join(sections)


def extract_verified_reference_price(text: str) -> float | None:
    """Extract only an explicitly labelled verified close from completed evidence."""
    match = re.search(
        r"(?i)\bverified\s+close\s*[:=]\s*(?:[$€£¥]|[A-Z]{3}\s*)?"
        r"(-?\d[\d,]*(?:\.\d+)?)",
        text,
    )
    if not match:
        return None
    try:
        value = float(match.group(1).replace(",", ""))
    except ValueError:
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return value


def _target_fields(draft: PortfolioDecisionDraft) -> tuple[Any, ...]:
    return (
        draft.price_target,
        draft.time_horizon,
        draft.confidence_score,
        draft.target_summary,
        draft.supporting_quote,
    )


def _base_fields(draft: PortfolioDecisionDraft) -> dict[str, Any]:
    return {
        "status": draft.status,
        "rating": draft.rating,
        "executive_summary": draft.executive_summary,
        "investment_thesis": draft.investment_thesis,
    }


def _without_target(
    draft: PortfolioDecisionDraft,
    status: TargetValidationStatus,
    reason: TargetValidationReason | None,
) -> PortfolioDecision:
    return PortfolioDecision(
        **_base_fields(draft),
        target_validation_status=status,
        target_validation_reason=reason.value if reason else None,
    )


def _quote_rejection_reason(
    price_target: float,
    supporting_quote: str,
    evidence_text: str,
) -> TargetValidationReason | None:
    quote = _normalize_whitespace(supporting_quote)
    evidence = _normalize_whitespace(evidence_text)
    if not quote:
        return TargetValidationReason.SUPPORTING_QUOTE_MISSING
    if len(quote) > 300 or quote.casefold() not in evidence.casefold():
        return TargetValidationReason.SUPPORTING_QUOTE_NOT_IN_EVIDENCE

    matched_target_number = False
    for cited_match in re.finditer(
        r"(?<![\w.])-?\d[\d,]*(?:\.\d+)?(?!\w|\.\d)",
        quote,
    ):
        try:
            cited_value = float(cited_match.group(0).replace(",", ""))
        except ValueError:
            continue
        if not math.isclose(cited_value, price_target, rel_tol=1e-9, abs_tol=0.005):
            continue

        matched_target_number = True
        following = quote[cited_match.end() : cited_match.end() + 24]
        if _NON_PRICE_SUFFIX.match(following):
            continue

        preceding = quote[max(0, cited_match.start() - 80) : cited_match.start()]
        follows_price_label = re.search(
            rf"(?:{_PRICE_LABEL}|{_CURRENCY_MARKER}){_CONNECTOR}$",
            preceding,
            re.IGNORECASE,
        )
        precedes_price_label = re.match(
            rf"{_CONNECTOR}(?:{_PRICE_LABEL}|{_CURRENCY_MARKER})",
            following,
            re.IGNORECASE,
        )
        if follows_price_label or precedes_price_label:
            return None

    if not matched_target_number:
        return TargetValidationReason.SUPPORTING_QUOTE_NUMBER_MISMATCH
    return TargetValidationReason.SUPPORTING_QUOTE_NOT_PRICE_CONTEXT


def _target_rejection_reason(
    draft: PortfolioDecisionDraft,
    evidence_text: str,
    reference_price: float | None,
) -> TargetValidationReason | None:
    target = draft.price_target
    if target is None or not math.isfinite(target) or target <= 0:
        return TargetValidationReason.TARGET_NOT_POSITIVE_FINITE
    if not isinstance(draft.supporting_quote, str) or not draft.supporting_quote.strip():
        return TargetValidationReason.SUPPORTING_QUOTE_MISSING

    quote_reason = _quote_rejection_reason(
        target,
        draft.supporting_quote,
        evidence_text,
    )
    if quote_reason is not None:
        return quote_reason

    if reference_price is not None and math.isfinite(reference_price):
        bullish_conflict = (
            draft.rating in {PortfolioRating.BUY, PortfolioRating.OVERWEIGHT}
            and target <= reference_price
        )
        bearish_conflict = (
            draft.rating in {PortfolioRating.SELL, PortfolioRating.UNDERWEIGHT}
            and target >= reference_price
        )
        if bullish_conflict or bearish_conflict:
            return TargetValidationReason.TARGET_DIRECTION_CONFLICT
    return None


def finalize_portfolio_decision(
    draft: PortfolioDecisionDraft,
    evidence_text: str,
    reference_price: float | None = None,
) -> PortfolioDecision:
    """Finalize a provider draft through deterministic target validation."""
    proposed = _target_fields(draft)
    if not any(value is not None for value in proposed):
        return _without_target(draft, TargetValidationStatus.NOT_PROPOSED, None)
    if not all(value is not None for value in proposed):
        return _without_target(
            draft,
            TargetValidationStatus.REJECTED,
            TargetValidationReason.BUNDLE_INCOMPLETE,
        )

    reason = _target_rejection_reason(draft, evidence_text, reference_price)
    if reason is not None:
        return _without_target(draft, TargetValidationStatus.REJECTED, reason)

    return PortfolioDecision(
        **_base_fields(draft),
        price_target=draft.price_target,
        time_horizon=draft.time_horizon,
        confidence_score=draft.confidence_score,
        target_summary=draft.target_summary,
        supporting_quote=draft.supporting_quote,
        target_validation_status=TargetValidationStatus.ACCEPTED,
    )
