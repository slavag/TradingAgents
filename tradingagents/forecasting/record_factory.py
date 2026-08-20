"""Canonical construction helpers for immutable forecast records."""

from __future__ import annotations

import hashlib
import json
import re

from tradingagents.forecasting.schemas import ForecastRecord, ForecastRecordPayload

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
