"""Calendar-specific outlook validation and presentation shared by all reports."""

from __future__ import annotations

import calendar
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field

from tradingagents.agents.schemas import (
    PortfolioDecisionDraft,
    PortfolioOutlookDraft,
    PortfolioRating,
    PortfolioTargetRepair,
    TargetValidationStatus,
)
from tradingagents.agents.utils.decision_integrity import (
    build_decision_evidence,
    finalize_portfolio_decision,
    sanitize_decision_sizing,
    verified_price_context,
    verified_target_candidates,
)
from tradingagents.agents.utils.evidence import evidence_is_admissible


class PortfolioOutlook(BaseModel):
    months: Literal[6, 12]
    end_date: str
    analysis_date: str
    reference_price: float | None = None
    reference_observed_on: str | None = None
    quote_currency: str | None = None
    data_warnings: list[str] = Field(default_factory=list)
    status: Literal["available", "target_unavailable", "unavailable"] = "unavailable"
    reason: str | None = None
    rating: PortfolioRating | None = None
    recommendation_confidence_score: int | None = Field(default=None, ge=0, le=100)
    summary: str = ""
    drivers: list[str] = Field(default_factory=list)
    upside_case: str = ""
    downside_case: str = ""
    invalidation: str = ""
    source_ids: list[str] = Field(default_factory=list)
    target: PortfolioTargetRepair | None = None
    expected_return_pct: float | None = None
    expected_return_low_pct: float | None = None
    expected_return_high_pct: float | None = None


class PortfolioOutlookBundle(BaseModel):
    outlooks: list[PortfolioOutlook]


def add_months(start: date, months: int) -> date:
    year, month = divmod(start.year * 12 + start.month - 1 + months, 12)
    month += 1
    return date(year, month, min(start.day, calendar.monthrange(year, month)[1]))


def validate_outlooks(
    rows: list[PortfolioOutlookDraft], state: dict[str, Any]
) -> list[PortfolioOutlook]:
    """Publish each horizon independently, preserving valid views after target failure."""
    as_of = str(state.get("trade_date") or state.get("as_of_date") or "")
    start = date.fromisoformat(as_of)
    evidence = build_decision_evidence(state)
    price_context = verified_price_context(state)
    reference = price_context["close"]
    candidates = verified_target_candidates(state)
    admitted = {
        str(record.get("evidence_id"))
        for record in state.get("shared_evidence") or []
        if isinstance(record, dict)
        and record.get("evidence_id")
        and record.get("coverage") != "metadata_only"
        and evidence_is_admissible(record, as_of)
    }
    from tradingagents.related_companies import admitted_related_sources
    admitted.update(record["evidence_id"] for record in admitted_related_sources(state, as_of)
                    if record.get("evidence_id") and record.get("coverage") != "metadata_only")
    market = price_context
    result = []
    for months in (6, 12):
        item = PortfolioOutlook(
            months=months,
            end_date=add_months(start, months).isoformat(),
            analysis_date=as_of,
            reference_price=reference,
            reference_observed_on=market.get("observed_on"),
            quote_currency=market.get("quote_currency"),
        )
        if item.reference_price is None:
            item.data_warnings.append("reference_price_missing")
        if not item.quote_currency:
            item.data_warnings.append("quote_currency_missing")
        matches = [row for row in rows if row.months == months]
        if len(matches) != 1:
            item.reason = "duplicate_horizon" if matches else "missing_horizon"
            result.append(item)
            continue
        row = matches[0]
        if not set(row.source_ids) <= admitted:
            item.reason = "outlook_sources_invalid"
            result.append(item)
            continue
        # Internal adapter reuses the target validator. Its execution guidance
        # stays internal: a horizon view does not invent a current trading action.
        draft = sanitize_decision_sizing(
            PortfolioDecisionDraft(
                **row.model_dump(
                    exclude={
                        "months",
                        "rating",
                        "recommendation_confidence_score",
                        "summary",
                        "drivers",
                        "upside_case",
                        "downside_case",
                        "invalidation",
                        "source_ids",
                        "time_horizon",
                    }
                ),
                rating=row.rating,
                recommendation_confidence_score=row.recommendation_confidence_score,
                executive_summary=row.summary,
                investment_thesis=row.summary,
                thesis="Neutral",
                existing_position_action="Hold",
                existing_position_summary="Horizon view only.",
                new_position_action="Avoid",
                new_position_summary="Use the tactical plan for execution.",
                time_horizon=f"{months} months (to {item.end_date})",
            )
        )
        decision = finalize_portfolio_decision(
            draft, evidence, reference, eligible_quotes=candidates
        )
        item.rating = row.rating
        item.recommendation_confidence_score = row.recommendation_confidence_score
        item.summary = decision.executive_summary
        item.drivers = list(row.drivers)
        item.upside_case, item.downside_case, item.invalidation = (
            row.upside_case,
            row.downside_case,
            row.invalidation,
        )
        item.source_ids = list(row.source_ids)
        completeness = (
            "target_method_missing"
            if not row.target_method or not row.target_method.strip()
            else "target_risks_missing"
            if not row.target_risks or not row.target_risks.strip()
            else None
        )
        if (
            decision.target_validation_status is TargetValidationStatus.ACCEPTED
            and completeness is None
        ):
            item.status = "available"
            item.target = PortfolioTargetRepair.model_validate(
                {key: getattr(decision, key) for key in PortfolioTargetRepair.model_fields}
            )
            if reference is not None and reference > 0:
                for price_field, return_field in (
                    ("price_target", "expected_return_pct"),
                    ("target_low", "expected_return_low_pct"),
                    ("target_high", "expected_return_high_pct"),
                ):
                    value = getattr(item.target, price_field)
                    if value is not None:
                        setattr(item, return_field, round((value / reference - 1) * 100, 2))
        else:
            item.status = "target_unavailable"
            item.reason = completeness or decision.target_validation_reason or "target_not_proposed"
        result.append(item)
    return result


def render_outlooks(rows: list[dict]) -> str:
    """Full report section; legacy runs without outlooks receive no invented data."""
    parts = []
    for raw in rows:
        row = PortfolioOutlook.model_validate(raw)
        parts.extend(
            [
                f"## {row.months}-month outlook — {row.end_date}",
                f"Analysis date: {row.analysis_date}. Reference close: {row.reference_price} "
                f"{row.quote_currency or 'quote currency unavailable'} ({row.reference_observed_on or 'observation date unavailable'}). "
                "Daily session only; extended-hours prices are separate information.",
            ]
        )
        if row.rating is None:
            parts.append(f"Outlook unavailable: {row.reason}.")
            continue
        parts.extend(
            [
                f"Recommendation: {row.rating.value}",
                f"Recommendation confidence (uncalibrated): {row.recommendation_confidence_score}/100",
                row.summary,
            ]
        )
        if row.target:
            target = row.target
            value = (
                str(target.price_target)
                if target.price_target is not None
                else f"{target.target_low}–{target.target_high}"
            )
            parts.extend(
                [
                    f"Target: {value} {row.quote_currency or 'quote currency unavailable'} ({target.target_basis})",
                    f"Target confidence (uncalibrated): {target.confidence_score}/100",
                    f"Method: {target.target_method}",
                    f"Assumptions: {target.target_assumptions or 'Source-level scenario.'}",
                    f"Rationale: {target.target_summary}",
                    f"Risks: {target.target_risks}",
                ]
            )
            changes = row.expected_return_pct
            if changes is not None:
                parts.append(f"Expected change from reference: {changes:+.2f}%")
            elif row.expected_return_low_pct is not None:
                parts.append(
                    f"Expected change from reference: {row.expected_return_low_pct:+.2f}% to {row.expected_return_high_pct:+.2f}%"
                )
        else:
            parts.append(f"Target unavailable: {row.reason}.")
        parts.extend(
            [
                "Drivers: " + "; ".join(row.drivers),
                f"Upside: {row.upside_case}",
                f"Downside: {row.downside_case}",
                f"What changes the view: {row.invalidation}",
                "Evidence records: " + ", ".join(row.source_ids),
            ]
        )
    return "\n\n".join(parts)


def render_outlooks_html(rows: list[dict]) -> str:
    """Escape model text while giving consolidated HTML readable headings."""
    from html import escape

    return "".join(
        f"<h3>{escape(part[3:])}</h3>" if part.startswith("## ") else f"<p>{escape(part)}</p>"
        for part in render_outlooks(rows).split("\n\n")
        if part
    )
