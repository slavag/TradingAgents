"""Canonical construction helpers for immutable forecast records."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from pydantic import ValidationError

from tradingagents.agents.schemas import DecisionStatus, PortfolioDecision
from tradingagents.forecasting.schemas import (
    DataQuality,
    ForecastDecisionStatus,
    ForecastProvenance,
    ForecastRecord,
    ForecastRecordPayload,
    ModelIdentity,
    ReferencePriceSnapshot,
)

_HORIZON_PATTERN = re.compile(
    r"^\s*([1-9][0-9]*)\s+"
    r"(day|days|week|weeks|month|months|year|years|session|sessions)\s*$",
    re.IGNORECASE,
)
_SESSION_MULTIPLIERS = {
    "day": 1,
    "days": 1,
    "week": 5,
    "weeks": 5,
    "month": 21,
    "months": 21,
    "year": 252,
    "years": 252,
    "session": 1,
    "sessions": 1,
}
_EVIDENCE_FIELDS = (
    "market_report",
    "sentiment_report",
    "news_report",
    "fundamentals_report",
    "investment_plan",
    "trader_investment_plan",
    "past_context",
)
_HASH_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


def canonical_payload_json(payload: ForecastRecordPayload) -> str:
    """Serialize a validated payload into its stable content-addressing form."""
    return json.dumps(
        payload.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def forecast_record_id(payload: ForecastRecordPayload) -> str:
    """Return the SHA-256 content ID for a payload."""
    digest = hashlib.sha256(canonical_payload_json(payload).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def create_forecast_record(payload: ForecastRecordPayload) -> ForecastRecord:
    """Embed a payload's deterministic ID in an immutable record."""
    return ForecastRecord(
        record_id=forecast_record_id(payload),
        **payload.model_dump(),
    )


def normalize_horizon_sessions(text: str | None) -> int | None:
    """Normalize only exact single-value horizons into trading sessions."""
    if not isinstance(text, str):
        return None
    match = _HORIZON_PATTERN.fullmatch(text)
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2).casefold()
    return amount * _SESSION_MULTIPLIERS[unit]


def _hash_json(value: Any) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def _markdown_field(text: str, label: str) -> str | None:
    match = re.search(
        rf"(?im)^\s*\*\*{re.escape(label)}\*\*\s*:\s*(.+?)\s*$",
        text,
    )
    return match.group(1).strip() if match else None


def _legacy_decision_snapshot(text: str) -> dict[str, Any]:
    rating = _markdown_field(text, "Rating")
    status = _markdown_field(text, "Decision Status")
    if status is None:
        status = "Actionable" if rating else "Unavailable"
    target_status = _markdown_field(text, "Target Validation") or "Not Proposed"
    return {
        "status": status,
        "rating": rating,
        "status_reason": _markdown_field(text, "Decision Reason"),
        "target_validation_status": target_status.split(" (", 1)[0],
        "target_validation_reason": None,
        "price_target": None,
        "time_horizon": None,
        "confidence_score": None,
        "recommendation_confidence_score": None,
        "conditional_plan": None,
    }


def _decision_snapshot(final_state: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    raw = final_state.get("portfolio_decision")
    if isinstance(raw, dict):
        decision = PortfolioDecision.model_validate(raw)
        return decision.model_dump(mode="json"), True
    return _legacy_decision_snapshot(str(final_state.get("final_trade_decision") or "")), False


def _reference_snapshot(run_metadata: dict[str, Any]) -> ReferencePriceSnapshot | None:
    raw = run_metadata.get("reference_price")
    if not isinstance(raw, dict):
        return None
    try:
        return ReferencePriceSnapshot.model_validate(raw)
    except ValidationError:
        return None


def _model_identities(run_metadata: dict[str, Any]) -> tuple[ModelIdentity, ...]:
    raw_models = run_metadata.get("models")
    if not isinstance(raw_models, dict):
        return ()
    identities = []
    for role, raw in sorted(raw_models.items()):
        if not isinstance(raw, dict):
            continue
        provider = raw.get("provider")
        model = raw.get("model")
        if provider and model:
            identities.append(
                ModelIdentity(role=str(role), provider=str(provider), model=str(model))
            )
    return tuple(identities)


def _evidence_ids(final_state: dict[str, Any]) -> tuple[str, ...]:
    evidence = []
    for field in _EVIDENCE_FIELDS:
        value = final_state.get(field)
        if isinstance(value, str) and value.strip():
            evidence.append(_hash_json({"field": field, "content": value.strip()}))
    risk_state = final_state.get("risk_debate_state")
    if isinstance(risk_state, dict):
        history = risk_state.get("history")
        if isinstance(history, str) and history.strip():
            evidence.append(_hash_json({"field": "risk_debate", "content": history.strip()}))
    return tuple(sorted(evidence))


def _data_cutoff(final_state: dict[str, Any], generated_at: datetime) -> tuple[date, bool]:
    raw = final_state.get("as_of_date") or final_state.get("trade_date")
    try:
        return date.fromisoformat(str(raw)), True
    except (TypeError, ValueError):
        return generated_at.date(), False


def _forecast_status(value: str) -> ForecastDecisionStatus:
    mapping = {
        DecisionStatus.ACTIONABLE.value: ForecastDecisionStatus.ACTIONABLE,
        DecisionStatus.ABSTAIN.value: ForecastDecisionStatus.ABSTAIN,
        DecisionStatus.UNAVAILABLE.value: ForecastDecisionStatus.UNAVAILABLE,
    }
    return mapping.get(value, ForecastDecisionStatus.UNAVAILABLE)


def forecast_record_from_state(
    final_state: dict[str, Any],
    ticker: str,
    run_metadata: dict[str, Any] | None,
    *,
    generated_at: datetime | None = None,
) -> ForecastRecord:
    """Build a conservative immutable forecast record from completed graph state."""
    metadata = dict(run_metadata or {})
    generated_at = generated_at or datetime.now(timezone.utc)
    if generated_at.tzinfo is None or generated_at.utcoffset() is None:
        raise ValueError("generated_at must be timezone-aware")

    decision, typed_decision = _decision_snapshot(final_state)
    status = _forecast_status(str(decision.get("status") or ""))
    target_status = str(decision.get("target_validation_status") or "Not Proposed")
    central_target = (
        Decimal(str(decision["price_target"]))
        if target_status == "Accepted" and decision.get("price_target") is not None
        else None
    )
    horizon_text = decision.get("time_horizon")
    horizon_sessions = normalize_horizon_sessions(horizon_text)
    reference_price = _reference_snapshot(metadata)
    expected_return = None
    if central_target is not None and reference_price is not None:
        expected_return = central_target / reference_price.value - Decimal("1")

    invalidations = ()
    conditional_plan = decision.get("conditional_plan")
    if isinstance(conditional_plan, dict) and conditional_plan.get("invalidation"):
        invalidations = (str(conditional_plan["invalidation"]),)

    models = _model_identities(metadata)
    evidence_ids = _evidence_ids(final_state)
    source_snapshot_hash = metadata.get("evidence_fingerprint")
    if not isinstance(source_snapshot_hash, str) or not _HASH_PATTERN.fullmatch(
        source_snapshot_hash
    ):
        source_snapshot_hash = _hash_json(evidence_ids)
    prompt_hash = metadata.get("prompt_hash")
    if not isinstance(prompt_hash, str) or not _HASH_PATTERN.fullmatch(prompt_hash):
        prompt_hash = None
    config_hash = _hash_json(
        {
            "models": metadata.get("models"),
            "temperature": metadata.get("temperature"),
        }
    )

    data_cutoff, has_exact_cutoff = _data_cutoff(final_state, generated_at)
    quote_currency = metadata.get("quote_currency")
    missing_fields = {
        "expected_excess_return",
        "distribution",
        "direction_probabilities",
        "target_range",
    }
    if not typed_decision:
        missing_fields.add("typed_portfolio_decision")
    if reference_price is None:
        missing_fields.add("reference_price")
    if not quote_currency:
        missing_fields.add("quote_currency")
    if horizon_sessions is None:
        missing_fields.add("horizon_sessions")
    if expected_return is None:
        missing_fields.add("expected_return")
    if prompt_hash is None:
        missing_fields.add("prompt_hash")
    if not models:
        missing_fields.add("model_identity")
    if not evidence_ids:
        missing_fields.add("evidence")
    if not has_exact_cutoff:
        missing_fields.add("data_cutoff")

    quality = DataQuality.PARTIAL if typed_decision and evidence_ids else DataQuality.INSUFFICIENT
    payload = ForecastRecordPayload(
        canonical_symbol=ticker,
        quote_currency=str(quote_currency) if quote_currency else None,
        asset_type=str(final_state.get("asset_type") or "unknown"),
        as_of=generated_at,
        data_cutoff=data_cutoff,
        reference_price=reference_price,
        horizon_text=str(horizon_text) if horizon_text else None,
        horizon_sessions=horizon_sessions,
        expected_return=expected_return,
        expected_excess_return=None,
        distribution=None,
        direction_probabilities=None,
        central_target=central_target,
        target_low=None,
        target_high=None,
        invalidation_conditions=invalidations,
        evidence_ids=evidence_ids,
        missing_fields=tuple(sorted(missing_fields)),
        data_quality=quality,
        data_quality_notes=(
            "Uncalibrated decision snapshot; probability distribution unavailable.",
        ),
        provenance=ForecastProvenance(
            models=models,
            prompt_hash=prompt_hash,
            config_hash=config_hash,
            source_snapshot_hash=source_snapshot_hash,
        ),
        decision_status=status,
        rating=str(decision["rating"]) if decision.get("rating") else None,
        status_reason=(
            str(decision["status_reason"]) if decision.get("status_reason") else None
        ),
        target_validation_status=target_status,
        target_validation_reason=(
            str(decision["target_validation_reason"])
            if decision.get("target_validation_reason")
            else None
        ),
        recommendation_confidence_score=decision.get(
            "recommendation_confidence_score"
        ),
        target_confidence_score=decision.get("confidence_score"),
    )
    return create_forecast_record(payload)
