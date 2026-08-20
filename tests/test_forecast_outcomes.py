"""Deterministic common-session resolution for immutable forecasts."""

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from tradingagents.evaluation.outcomes import (
    OutcomeResolutionStatus,
    PriceObservation,
    resolve_forecast_outcome,
)
from tradingagents.forecasting.record_factory import create_forecast_record
from tradingagents.forecasting.schemas import (
    AdjustmentBasis,
    DataQuality,
    ForecastDecisionStatus,
    ForecastProvenance,
    ForecastRecordPayload,
    ReferencePriceSnapshot,
)


def record(*, horizon_sessions=2, reference_price=True):
    reference = (
        ReferencePriceSnapshot(
            value=Decimal("100"),
            observed_at=datetime(2026, 8, 21, 20, 0, tzinfo=timezone.utc),
            adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
            vendor="test-feed",
        )
        if reference_price
        else None
    )
    return create_forecast_record(
        ForecastRecordPayload(
            canonical_symbol="TEST",
            quote_currency="USD",
            asset_type="stock",
            as_of=datetime(2026, 8, 21, 21, 0, tzinfo=timezone.utc),
            data_cutoff=date(2026, 8, 21),
            reference_price=reference,
            horizon_text="2 sessions" if horizon_sessions else None,
            horizon_sessions=horizon_sessions,
            expected_return=None,
            expected_excess_return=None,
            distribution=None,
            direction_probabilities=None,
            central_target=None,
            target_low=None,
            target_high=None,
            invalidation_conditions=(),
            evidence_ids=(),
            missing_fields=(),
            data_quality=DataQuality.PARTIAL,
            data_quality_notes=(),
            provenance=ForecastProvenance(
                models=(),
                prompt_hash=None,
                config_hash="sha256:" + "a" * 64,
                source_snapshot_hash="sha256:" + "b" * 64,
            ),
            decision_status=ForecastDecisionStatus.ACTIONABLE,
            rating="Hold",
            status_reason=None,
            target_validation_status="Not Proposed",
            target_validation_reason=None,
            recommendation_confidence_score=50,
            target_confidence_score=None,
        )
    )


def observations():
    return (
        PriceObservation(session=date(2026, 8, 24), close=Decimal("102")),
        PriceObservation(session=date(2026, 8, 25), close=Decimal("105")),
        PriceObservation(session=date(2026, 8, 26), close=Decimal("103")),
    )


def test_resolves_nth_common_session_after_cutoff_across_weekend():
    outcome = resolve_forecast_outcome(
        record(),
        observations(),
        adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
    )

    assert outcome.status is OutcomeResolutionStatus.RESOLVED
    assert outcome.resolved_date == date(2026, 8, 25)
    assert outcome.end_price == Decimal("105")
    assert outcome.realized_return == Decimal("0.05")
    assert outcome.evaluated_path == observations()[:2]


def test_one_session_horizon_uses_first_session_after_cutoff():
    outcome = resolve_forecast_outcome(
        record(horizon_sessions=1),
        observations(),
        adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
    )

    assert outcome.resolved_date == date(2026, 8, 24)
    assert outcome.realized_return == Decimal("0.02")


def test_basis_mismatch_is_explicitly_unresolved():
    outcome = resolve_forecast_outcome(
        record(),
        observations(),
        adjustment_basis=AdjustmentBasis.RAW,
    )

    assert outcome.status is OutcomeResolutionStatus.PRICE_BASIS_MISMATCH
    assert outcome.realized_return is None


def test_missing_reference_price_is_explicitly_unresolved():
    outcome = resolve_forecast_outcome(
        record(reference_price=False),
        observations(),
        adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
    )

    assert outcome.status is OutcomeResolutionStatus.MISSING_REFERENCE_PRICE


def test_missing_horizon_is_explicitly_unresolved():
    outcome = resolve_forecast_outcome(
        record(horizon_sessions=None),
        observations(),
        adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
    )

    assert outcome.status is OutcomeResolutionStatus.MISSING_HORIZON


def test_insufficient_future_sessions_is_explicitly_unresolved():
    outcome = resolve_forecast_outcome(
        record(horizon_sessions=3),
        observations()[:2],
        adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
    )

    assert outcome.status is OutcomeResolutionStatus.INSUFFICIENT_SESSIONS
    assert outcome.resolved_date is None


def test_non_monotonic_observations_are_rejected():
    with pytest.raises(ValueError, match="strictly increasing"):
        resolve_forecast_outcome(
            record(),
            tuple(reversed(observations())),
            adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
        )


def test_benchmark_return_aligns_to_instrument_resolved_date():
    benchmark = (
        PriceObservation(session=date(2026, 8, 24), close=Decimal("201")),
        PriceObservation(session=date(2026, 8, 25), close=Decimal("204")),
        PriceObservation(session=date(2026, 8, 26), close=Decimal("210")),
    )

    outcome = resolve_forecast_outcome(
        record(),
        observations(),
        adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
        benchmark_reference_price=Decimal("200"),
        benchmark_observations=benchmark,
    )

    assert outcome.benchmark_return == Decimal("0.02")
    assert outcome.excess_return == Decimal("0.03")
    assert "benchmark_return" not in outcome.missing_fields
