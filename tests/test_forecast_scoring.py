"""Deterministic scoring for resolved immutable forecasts."""

from datetime import date
from decimal import Decimal

from tests.test_forecast_outcomes import observations, record
from tradingagents.evaluation.calibration import CalibrationBin, summarize_calibration
from tradingagents.evaluation.outcomes import (
    OutcomeResolutionStatus,
    PriceObservation,
    resolve_forecast_outcome,
)
from tradingagents.evaluation.scoring import RealizedDirection, score_forecast
from tradingagents.forecasting.record_factory import create_forecast_record
from tradingagents.forecasting.schemas import (
    AdjustmentBasis,
    DirectionProbabilities,
    ForecastRecordPayload,
)


def changed_record(**updates):
    original = record()
    payload = original.model_dump(exclude={"record_id"})
    payload.update(updates)
    return create_forecast_record(ForecastRecordPayload.model_validate(payload))


def resolved(record_value=None, path=None):
    return resolve_forecast_outcome(
        record_value or record(),
        path or observations(),
        adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
    )


def test_scores_buy_direction_and_deterministic_transaction_cost():
    forecast = changed_record(rating="Buy")

    score = score_forecast(forecast, resolved(forecast), transaction_cost_bps=10)

    assert score.realized_direction is RealizedDirection.UP
    assert score.direction_correct is True
    assert score.gross_return == Decimal("0.05")
    assert score.transaction_cost == Decimal("0.001")
    assert score.net_return == Decimal("0.049")


def test_scores_sell_and_hold_against_explicit_flat_band():
    sell = changed_record(rating="Sell")
    falling = (
        PriceObservation(session=date(2026, 8, 24), close=Decimal("99")),
        PriceObservation(session=date(2026, 8, 25), close=Decimal("95")),
    )
    hold = changed_record(rating="Hold")
    flat = (
        PriceObservation(session=date(2026, 8, 24), close=Decimal("101")),
        PriceObservation(session=date(2026, 8, 25), close=Decimal("101")),
    )

    assert score_forecast(sell, resolved(sell, falling)).direction_correct is True
    hold_score = score_forecast(hold, resolved(hold, flat))
    assert hold_score.realized_direction is RealizedDirection.FLAT
    assert hold_score.direction_correct is True


def test_scores_target_error_interval_coverage_and_brier():
    forecast = changed_record(
        rating="Buy",
        central_target=Decimal("110"),
        target_low=Decimal("100"),
        target_high=Decimal("110"),
        target_validation_status="Accepted",
        target_confidence_score=70,
        direction_probabilities=DirectionProbabilities(
            down=Decimal("0.1"),
            flat=Decimal("0.2"),
            up=Decimal("0.7"),
        ),
    )

    score = score_forecast(forecast, resolved(forecast))

    assert score.target_mae == Decimal("5")
    assert score.target_mape == Decimal("5") / Decimal("105")
    assert score.interval_covered is True
    assert score.brier_score == Decimal("0.14") / Decimal("3")
    assert "brier_score" not in score.missing_metrics


def test_scores_path_maximum_drawdown():
    forecast = changed_record(horizon_sessions=3, horizon_text="3 sessions")
    score = score_forecast(forecast, resolved(forecast))

    assert score.maximum_drawdown == Decimal("103") / Decimal("105") - 1


def test_unresolved_outcome_keeps_metrics_missing_instead_of_zero():
    forecast = changed_record(horizon_sessions=10)
    outcome = resolve_forecast_outcome(
        forecast,
        observations(),
        adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
    )

    score = score_forecast(forecast, outcome)

    assert outcome.status is OutcomeResolutionStatus.INSUFFICIENT_SESSIONS
    assert score.gross_return is None
    assert score.direction_correct is None
    assert "gross_return" in score.missing_metrics
    assert "direction_correct" in score.missing_metrics


def test_calibration_summary_matches_hand_calculated_brier_and_ece():
    first_record = changed_record(
        rating="Buy",
        direction_probabilities=DirectionProbabilities(
            down=Decimal("0.1"),
            flat=Decimal("0.1"),
            up=Decimal("0.8"),
        ),
    )
    second_record = changed_record(
        rating="Sell",
        direction_probabilities=DirectionProbabilities(
            down=Decimal("0.2"),
            flat=Decimal("0.2"),
            up=Decimal("0.6"),
        ),
    )
    falling = (
        PriceObservation(session=date(2026, 8, 24), close=Decimal("99")),
        PriceObservation(session=date(2026, 8, 25), close=Decimal("95")),
    )
    scores = (
        score_forecast(first_record, resolved(first_record)),
        score_forecast(second_record, resolved(second_record, falling)),
    )

    summary = summarize_calibration(scores, bins=2)

    assert summary.eligible_count == 2
    assert summary.excluded_count == 0
    assert summary.mean_brier_score == (
        Decimal("0.02") + Decimal("1.04") / Decimal("3")
    ) / 2
    assert summary.expected_calibration_error == Decimal("0.2")
    assert summary.bins == (
        CalibrationBin(
            lower_bound=Decimal("0.5"),
            upper_bound=Decimal("1"),
            count=2,
            mean_confidence=Decimal("0.7"),
            observed_accuracy=Decimal("0.5"),
        ),
    )


def test_calibration_summary_excludes_ineligible_scores():
    eligible_record = changed_record(
        rating="Buy",
        direction_probabilities=DirectionProbabilities(
            down=Decimal("0.1"),
            flat=Decimal("0.2"),
            up=Decimal("0.7"),
        ),
    )
    ineligible = score_forecast(record(), resolved())
    eligible = score_forecast(eligible_record, resolved(eligible_record))

    summary = summarize_calibration((eligible, ineligible), bins=5)

    assert summary.total_count == 2
    assert summary.eligible_count == 1
    assert summary.excluded_count == 1


def test_calibration_summary_empty_input_has_no_numeric_metrics():
    summary = summarize_calibration((), bins=10)

    assert summary.total_count == 0
    assert summary.eligible_count == 0
    assert summary.mean_brier_score is None
    assert summary.expected_calibration_error is None
    assert summary.bins == ()
