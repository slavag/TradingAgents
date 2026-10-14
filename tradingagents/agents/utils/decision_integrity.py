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
    r"(?:\s|[:=~≈@()\[\],|_-]|"
    r"\b(?:is|at|near|around|of|about|approximately|approx|roughly|"
    r"estimate|estimated|to)\b)*"
)
_NON_PRICE_SUFFIX = re.compile(
    r"\s*(?:%|percent\b|x\b|shares?\b|units?\b|volume\b|"
    r"thousand\b|million\b|billion\b|trillion\b|[KMBT]\b)",
    re.IGNORECASE,
)
_PORTFOLIO_WEIGHT = re.compile(
    r"(?i)\b\d+(?:\.\d+)?\s*%\s*(?:of\s+)?(?:portfolio|NAV|account|capital)\b"
)


def has_numeric_portfolio_weight(text: str) -> bool:
    """Detect an allocation percentage that requires missing account context."""
    return bool(_PORTFOLIO_WEIGHT.search(text))


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
    from tradingagents.agents.utils.evidence import render_admissible_evidence

    source_evidence = render_admissible_evidence(state, max_chars=60_000)
    if source_evidence:
        sections.append(f"[source_evidence]\n{source_evidence.strip()}")
    return "\n\n".join(sections)


def verified_target_candidates(state: Mapping[str, Any], limit: int = 12) -> list[str]:
    """Offer verbatim, dated price levels from the verified market snapshot."""
    from tradingagents.agents.utils.evidence import evidence_is_admissible

    run_date = str(state.get("trade_date") or state.get("as_of_date") or "")
    candidates = []
    for record in state.get("shared_evidence", []):
        if record.get("source") != "get_verified_market_snapshot" or not run_date:
            continue
        if not evidence_is_admissible(record, run_date):
            continue
        content = str(record.get("content") or "").split("### Recent verified closes", 1)[0]
        for raw_line in content.splitlines():
            quote = raw_line.strip()
            cells = [cell.strip() for cell in quote.strip("|").split("|")]
            if not quote.startswith("|") or len(cells) != 2 or len(quote) > 300:
                continue
            label, value = cells
            normalized = label.casefold()
            price_label = normalized in {"open", "high", "low", "close"} or any(
                part in normalized for part in ("sma", "ema", "boll", "support", "resistance")
            )
            if not price_label or not re.fullmatch(r"\$?\s*\d[\d,]*(?:\.\d+)?", value):
                continue
            if quote not in candidates:
                candidates.append(quote)
                if len(candidates) >= limit:
                    return candidates
    return candidates


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
        draft.target_low,
        draft.target_high,
        draft.time_horizon,
        draft.confidence_score,
        draft.target_summary,
        draft.supporting_quote,
        draft.supporting_quote_low,
        draft.supporting_quote_high,
        draft.target_method,
        draft.target_risks,
    )


def _base_fields(draft: PortfolioDecisionDraft) -> dict[str, Any]:
    return {
        "status": draft.status,
        "rating": draft.rating,
        "executive_summary": draft.executive_summary,
        "investment_thesis": draft.investment_thesis,
        "thesis": draft.thesis,
        "existing_position_action": draft.existing_position_action,
        "existing_position_summary": draft.existing_position_summary,
        "new_position_action": draft.new_position_action,
        "new_position_summary": draft.new_position_summary,
        "conditional_plan": draft.conditional_plan,
        "recommendation_confidence_score": draft.recommendation_confidence_score,
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


def _find_exact_price_quote(
    price_target: float,
    evidence_text: str,
    eligible_quotes: list[str] | None = None,
) -> str | None:
    for raw_line in eligible_quotes if eligible_quotes is not None else evidence_text.splitlines():
        candidate = _normalize_whitespace(raw_line)
        if not candidate or len(candidate) > 300:
            continue
        if _quote_rejection_reason(price_target, candidate, evidence_text) is None:
            return candidate
    return None


def finalize_portfolio_decision(
    draft: PortfolioDecisionDraft,
    evidence_text: str,
    reference_price: float | None = None,
    *,
    eligible_quotes: list[str] | None = None,
) -> PortfolioDecision:
    """Finalize a provider draft through deterministic price-level validation."""
    proposed = _target_fields(draft)
    if not any(value is not None for value in proposed):
        return _without_target(draft, TargetValidationStatus.NOT_PROPOSED, None)
    point_proposed = draft.price_target is not None or draft.supporting_quote is not None
    range_proposed = any(value is not None for value in (
        draft.target_low, draft.target_high,
        draft.supporting_quote_low, draft.supporting_quote_high,
    ))
    if point_proposed and range_proposed:
        return _without_target(
            draft, TargetValidationStatus.REJECTED, TargetValidationReason.TARGET_SHAPE_MIXED,
        )
    common = (draft.time_horizon, draft.confidence_score, draft.target_summary)
    shape_fields = (
        (draft.target_low, draft.target_high, draft.supporting_quote_low,
         draft.supporting_quote_high) if range_proposed else
        (draft.price_target, draft.supporting_quote) if point_proposed else ()
    )
    if not shape_fields or not all(value is not None for value in (*common, *shape_fields)):
        return _without_target(
            draft,
            TargetValidationStatus.REJECTED,
            TargetValidationReason.BUNDLE_INCOMPLETE,
        )

    if not isinstance(draft.time_horizon, str) or not draft.time_horizon.strip() or re.search(
        r"(?i)\b(?:ago|past|historical|yesterday)\b|^\s*-", draft.time_horizon,
    ):
        return _without_target(
            draft, TargetValidationStatus.REJECTED, TargetValidationReason.TARGET_HORIZON_INVALID,
        )
    if not isinstance(draft.target_summary, str) or not draft.target_summary.strip():
        return _without_target(
            draft, TargetValidationStatus.REJECTED,
            TargetValidationReason.TARGET_RATIONALE_MISSING,
        )

    if range_proposed:
        if not isinstance(draft.target_method, str) or not draft.target_method.strip():
            return _without_target(
                draft, TargetValidationStatus.REJECTED,
                TargetValidationReason.TARGET_METHOD_MISSING,
            )
        if not isinstance(draft.target_risks, str) or not draft.target_risks.strip():
            return _without_target(
                draft, TargetValidationStatus.REJECTED,
                TargetValidationReason.TARGET_RISKS_MISSING,
            )
        low, high = draft.target_low, draft.target_high
        if not all(math.isfinite(value) and value > 0 for value in (low, high)):
            return _without_target(
                draft, TargetValidationStatus.REJECTED,
                TargetValidationReason.TARGET_NOT_POSITIVE_FINITE,
            )
        if low > high:
            return _without_target(
                draft, TargetValidationStatus.REJECTED,
                TargetValidationReason.TARGET_RANGE_REVERSED,
            )
        if (
            reference_price is not None
            and math.isfinite(reference_price)
            and (
                (draft.rating in {PortfolioRating.BUY, PortfolioRating.OVERWEIGHT}
                 and low <= reference_price) or (
                draft.rating in {PortfolioRating.SELL, PortfolioRating.UNDERWEIGHT}
                and high >= reference_price
                )
            )
        ):
            return _without_target(
                draft, TargetValidationStatus.REJECTED,
                TargetValidationReason.TARGET_DIRECTION_CONFLICT,
            )

        quotes = [draft.supporting_quote_low, draft.supporting_quote_high]
        for index, (value, quote) in enumerate(zip((low, high), quotes, strict=True)):
            reason = _quote_rejection_reason(value, quote, evidence_text)
            if reason is TargetValidationReason.SUPPORTING_QUOTE_NOT_IN_EVIDENCE:
                replacement = _find_exact_price_quote(value, evidence_text, eligible_quotes)
                if replacement is not None:
                    quote = replacement
                    reason = None
            if reason is not None:
                return _without_target(draft, TargetValidationStatus.REJECTED, reason)
            if eligible_quotes is not None and _normalize_whitespace(quote) not in {
                _normalize_whitespace(row) for row in eligible_quotes
            }:
                return _without_target(
                    draft, TargetValidationStatus.REJECTED,
                    TargetValidationReason.SUPPORTING_QUOTE_NOT_ELIGIBLE,
                )
            quotes[index] = quote
        return PortfolioDecision(
            **_base_fields(draft),
            target_low=low,
            target_high=high,
            supporting_quote_low=quotes[0],
            supporting_quote_high=quotes[1],
            time_horizon=draft.time_horizon,
            confidence_score=draft.confidence_score,
            target_summary=draft.target_summary,
            target_method=draft.target_method,
            target_risks=draft.target_risks,
            target_validation_status=TargetValidationStatus.ACCEPTED,
        )

    validated_draft = draft
    quote_reason = _quote_rejection_reason(
        draft.price_target,
        draft.supporting_quote,
        evidence_text,
    )
    if quote_reason is TargetValidationReason.SUPPORTING_QUOTE_NOT_IN_EVIDENCE:
        exact_quote = _find_exact_price_quote(draft.price_target, evidence_text, eligible_quotes)
        if exact_quote is not None:
            validated_draft = draft.model_copy(
                update={"supporting_quote": exact_quote},
            )

    reason = _target_rejection_reason(
        validated_draft,
        evidence_text,
        reference_price,
    )
    if reason is not None:
        return _without_target(draft, TargetValidationStatus.REJECTED, reason)
    if eligible_quotes is not None and _normalize_whitespace(validated_draft.supporting_quote) not in {
        _normalize_whitespace(row) for row in eligible_quotes
    }:
        return _without_target(
            draft, TargetValidationStatus.REJECTED,
            TargetValidationReason.SUPPORTING_QUOTE_NOT_ELIGIBLE,
        )

    return PortfolioDecision(
        **_base_fields(validated_draft),
        price_target=validated_draft.price_target,
        time_horizon=validated_draft.time_horizon,
        confidence_score=validated_draft.confidence_score,
        target_summary=validated_draft.target_summary,
        supporting_quote=validated_draft.supporting_quote,
        target_method=validated_draft.target_method,
        target_risks=validated_draft.target_risks,
        target_validation_status=TargetValidationStatus.ACCEPTED,
    )
