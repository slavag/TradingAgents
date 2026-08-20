"""Deterministic per-record scoring for resolved forecast outcomes."""

from __future__ import annotations

from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

from tradingagents.evaluation.outcomes import (
    OutcomeResolutionStatus,
    ResolvedOutcome,
)
from tradingagents.forecasting.schemas import DirectionProbabilities, ForecastRecord


class RealizedDirection(str, Enum):
    DOWN = "down"
    FLAT = "flat"
    UP = "up"


class ForecastScore(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    record_id: str
    outcome_status: OutcomeResolutionStatus
    predicted_direction: RealizedDirection | None
    realized_direction: RealizedDirection | None
    direction_correct: bool | None
    gross_return: Decimal | None
    net_return: Decimal | None
    excess_return: Decimal | None
    transaction_cost: Decimal = Field(ge=0)
    target_mae: Decimal | None
    target_mape: Decimal | None
    interval_covered: bool | None
    brier_score: Decimal | None
    maximum_drawdown: Decimal | None
    direction_probabilities: DirectionProbabilities | None
    missing_metrics: tuple[str, ...]


def _predicted_direction(rating: str | None) -> RealizedDirection | None:
    if rating in {"Buy", "Overweight"}:
        return RealizedDirection.UP
    if rating in {"Sell", "Underweight"}:
        return RealizedDirection.DOWN
    if rating == "Hold":
        return RealizedDirection.FLAT
    return None


def _realized_direction(
    realized_return: Decimal,
    flat_return_band: Decimal,
) -> RealizedDirection:
    if realized_return > flat_return_band:
        return RealizedDirection.UP
    if realized_return < -flat_return_band:
        return RealizedDirection.DOWN
    return RealizedDirection.FLAT


def _maximum_drawdown(outcome: ResolvedOutcome) -> Decimal | None:
    if not outcome.evaluated_path:
        return None
    peak = outcome.evaluated_path[0].close
    maximum_drawdown = Decimal("0")
    for observation in outcome.evaluated_path:
        if observation.close > peak:
            peak = observation.close
        drawdown = observation.close / peak - Decimal("1")
        if drawdown < maximum_drawdown:
            maximum_drawdown = drawdown
    return maximum_drawdown


def _brier_score(
    probabilities: DirectionProbabilities | None,
    realized: RealizedDirection | None,
) -> Decimal | None:
    if probabilities is None or realized is None:
        return None
    actual = {
        RealizedDirection.DOWN: (Decimal("1"), Decimal("0"), Decimal("0")),
        RealizedDirection.FLAT: (Decimal("0"), Decimal("1"), Decimal("0")),
        RealizedDirection.UP: (Decimal("0"), Decimal("0"), Decimal("1")),
    }[realized]
    predicted = (probabilities.down, probabilities.flat, probabilities.up)
    return sum(
        (probability - label) ** 2
        for probability, label in zip(predicted, actual, strict=True)
    ) / Decimal("3")


def score_forecast(
    record: ForecastRecord,
    outcome: ResolvedOutcome,
    *,
    transaction_cost_bps: Decimal | int = 0,
    flat_return_band: Decimal = Decimal("0.02"),
) -> ForecastScore:
    """Score only metrics supported by a resolved outcome and record fields."""
    transaction_cost = Decimal(str(transaction_cost_bps)) / Decimal("10000")
    if transaction_cost < 0:
        raise ValueError("transaction cost cannot be negative")
    if flat_return_band < 0:
        raise ValueError("flat return band cannot be negative")

    predicted = _predicted_direction(record.rating)
    resolved = outcome.status is OutcomeResolutionStatus.RESOLVED
    gross_return = outcome.realized_return if resolved else None
    realized = (
        _realized_direction(gross_return, flat_return_band)
        if gross_return is not None
        else None
    )
    direction_correct = (
        predicted is realized
        if predicted is not None and realized is not None
        else None
    )
    net_return = gross_return - transaction_cost if gross_return is not None else None

    target_mae = None
    target_mape = None
    if resolved and record.central_target is not None and outcome.end_price is not None:
        target_mae = abs(record.central_target - outcome.end_price)
        target_mape = target_mae / outcome.end_price

    interval_covered = None
    if resolved and record.target_low is not None and outcome.end_price is not None:
        interval_covered = record.target_low <= outcome.end_price <= record.target_high

    brier_score = _brier_score(record.direction_probabilities, realized)
    maximum_drawdown = _maximum_drawdown(outcome) if resolved else None
    values = {
        "direction_correct": direction_correct,
        "gross_return": gross_return,
        "net_return": net_return,
        "excess_return": outcome.excess_return if resolved else None,
        "target_mae": target_mae,
        "target_mape": target_mape,
        "interval_covered": interval_covered,
        "brier_score": brier_score,
        "maximum_drawdown": maximum_drawdown,
    }
    return ForecastScore(
        record_id=record.record_id,
        outcome_status=outcome.status,
        predicted_direction=predicted,
        realized_direction=realized,
        transaction_cost=transaction_cost,
        direction_probabilities=record.direction_probabilities,
        missing_metrics=tuple(sorted(key for key, value in values.items() if value is None)),
        **values,
    )
