"""Contracts for immutable, content-addressed forecast records."""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tradingagents.forecasting.record_factory import (
    canonical_payload_json,
    create_forecast_record,
    forecast_record_id,
    normalize_horizon_sessions,
)
from tradingagents.forecasting.schemas import (
    AdjustmentBasis,
    DataQuality,
    DirectionProbabilities,
    ForecastDecisionStatus,
    ForecastDistribution,
    ForecastProvenance,
    ForecastRecord,
    ForecastRecordPayload,
    ModelIdentity,
    ReferencePriceSnapshot,
)


def forecast_payload(**overrides) -> ForecastRecordPayload:
    values = {
        "canonical_symbol": "BE",
        "quote_currency": "USD",
        "asset_type": "stock",
        "as_of": datetime(2026, 8, 20, 18, 30, tzinfo=timezone.utc),
        "data_cutoff": date(2026, 8, 20),
        "reference_price": ReferencePriceSnapshot(
            value=Decimal("236.22"),
            observed_at=datetime(2026, 8, 20, 18, 0, tzinfo=timezone.utc),
            adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
            vendor="yfinance",
        ),
        "horizon_text": "3 months",
        "horizon_sessions": 63,
        "expected_return": Decimal("0.05833545"),
        "expected_excess_return": None,
        "distribution": None,
        "direction_probabilities": None,
        "central_target": Decimal("250"),
        "target_low": None,
        "target_high": None,
        "invalidation_conditions": ("Break below 210",),
        "evidence_ids": ("sha256:" + "a" * 64,),
        "missing_fields": (
            "expected_excess_return",
            "distribution",
            "direction_probabilities",
            "target_range",
        ),
        "data_quality": DataQuality.PARTIAL,
        "data_quality_notes": ("No calibrated probability distribution.",),
        "provenance": ForecastProvenance(
            models=(
                ModelIdentity(
                    role="portfolio_manager",
                    provider="openai",
                    model="gpt-5.6",
                ),
            ),
            prompt_hash=None,
            config_hash="sha256:" + "b" * 64,
            source_snapshot_hash="sha256:" + "c" * 64,
        ),
        "decision_status": ForecastDecisionStatus.ACTIONABLE,
        "rating": "Buy",
        "status_reason": None,
        "target_validation_status": "Accepted",
        "target_validation_reason": None,
        "recommendation_confidence_score": 78,
        "target_confidence_score": 72,
    }
    values.update(overrides)
    return ForecastRecordPayload(**values)


def test_immutable_schema_rejects_field_mutation():
    payload = forecast_payload()

    with pytest.raises(ValidationError, match="frozen"):
        payload.rating = "Sell"


def test_schema_rejects_naive_as_of_timestamp():
    with pytest.raises(ValidationError, match="timezone-aware"):
        forecast_payload(as_of=datetime(2026, 8, 20, 18, 30))


def test_schema_rejects_reference_price_observed_after_as_of():
    reference = ReferencePriceSnapshot(
        value=Decimal("236.22"),
        observed_at=datetime(2026, 8, 20, 19, 0, tzinfo=timezone.utc),
        adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
        vendor="yfinance",
    )

    with pytest.raises(ValidationError, match="reference price cannot postdate"):
        forecast_payload(reference_price=reference)


def test_schema_rejects_probabilities_that_do_not_sum_to_one():
    with pytest.raises(ValidationError, match="sum to one"):
        DirectionProbabilities(
            down=Decimal("0.20"),
            flat=Decimal("0.20"),
            up=Decimal("0.20"),
        )


def test_schema_rejects_unordered_distribution_quantiles():
    with pytest.raises(ValidationError, match="p10 <= p50 <= p90"):
        ForecastDistribution(
            p10=Decimal("250"),
            p50=Decimal("240"),
            p90=Decimal("260"),
        )


def test_schema_rejects_partial_target_range():
    with pytest.raises(ValidationError, match="target range must be complete"):
        forecast_payload(target_low=Decimal("220"), target_high=None)


def test_schema_rejects_non_actionable_rating():
    with pytest.raises(ValidationError, match="non-actionable forecast cannot carry a rating"):
        forecast_payload(
            decision_status=ForecastDecisionStatus.ABSTAIN,
            rating="Hold",
            central_target=None,
            expected_return=None,
            target_validation_status="Not Proposed",
            recommendation_confidence_score=None,
            target_confidence_score=None,
        )


def test_schema_rejects_malformed_record_id():
    with pytest.raises(ValidationError, match="String should match pattern"):
        ForecastRecord(record_id="not-a-hash", **forecast_payload().model_dump())


def test_canonical_payload_json_is_stable_and_compact():
    canonical = canonical_payload_json(forecast_payload())

    assert canonical.startswith(
        '{"as_of":"2026-08-20T18:30:00Z","asset_type":"stock",'
        '"canonical_symbol":"BE","central_target":"250"'
    )
    assert canonical.endswith(
        '"target_low":null,"target_validation_reason":null,'
        '"target_validation_status":"Accepted"}'
    )
    assert "\n" not in canonical
    assert ": " not in canonical


def test_equal_payloads_have_identical_content_hashes():
    first = forecast_payload()
    second = forecast_payload()

    assert forecast_record_id(first) == forecast_record_id(second)
    assert forecast_record_id(first).startswith("sha256:")
    assert len(forecast_record_id(first)) == 71


def test_evidence_change_produces_a_different_content_hash():
    first = forecast_payload(evidence_ids=("sha256:" + "a" * 64,))
    second = forecast_payload(evidence_ids=("sha256:" + "d" * 64,))

    assert forecast_record_id(first) != forecast_record_id(second)


def test_create_forecast_record_embeds_payload_hash():
    payload = forecast_payload()

    record = create_forecast_record(payload)

    assert record.record_id == forecast_record_id(payload)
    assert record.canonical_symbol == "BE"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1 day", 1),
        ("2 weeks", 10),
        ("3 months", 63),
        ("1 year", 252),
        ("63 sessions", 63),
        ("3-6 months", None),
        ("approximately 3 months", None),
        (None, None),
    ],
)
def test_horizon_normalization_accepts_only_unambiguous_values(text, expected):
    assert normalize_horizon_sessions(text) == expected
