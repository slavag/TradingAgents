"""Aggregate deterministic calibration metrics from per-record scores."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from tradingagents.evaluation.scoring import ForecastScore, RealizedDirection


class CalibrationBin(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    lower_bound: Decimal = Field(ge=0, le=1)
    upper_bound: Decimal = Field(ge=0, le=1)
    count: int = Field(gt=0)
    mean_confidence: Decimal = Field(ge=0, le=1)
    observed_accuracy: Decimal = Field(ge=0, le=1)


class CalibrationSummary(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    total_count: int = Field(ge=0)
    eligible_count: int = Field(ge=0)
    excluded_count: int = Field(ge=0)
    mean_brier_score: Decimal | None
    expected_calibration_error: Decimal | None
    bins: tuple[CalibrationBin, ...]


def _prediction(score: ForecastScore) -> tuple[RealizedDirection, Decimal] | None:
    probabilities = score.direction_probabilities
    if probabilities is None:
        return None
    candidates = (
        (RealizedDirection.DOWN, probabilities.down),
        (RealizedDirection.FLAT, probabilities.flat),
        (RealizedDirection.UP, probabilities.up),
    )
    return max(candidates, key=lambda item: item[1])


def summarize_calibration(
    scored_forecasts: tuple[ForecastScore, ...],
    *,
    bins: int = 10,
) -> CalibrationSummary:
    """Summarize confidence calibration for eligible resolved probability forecasts."""
    if bins < 1 or bins > 100:
        raise ValueError("bins must be between 1 and 100")

    eligible = []
    for score in scored_forecasts:
        prediction = _prediction(score)
        if (
            prediction is not None
            and score.realized_direction is not None
            and score.brier_score is not None
        ):
            predicted_direction, confidence = prediction
            eligible.append(
                (
                    confidence,
                    Decimal("1")
                    if predicted_direction is score.realized_direction
                    else Decimal("0"),
                    score.brier_score,
                )
            )

    eligible_count = len(eligible)
    total_count = len(scored_forecasts)
    if not eligible:
        return CalibrationSummary(
            total_count=total_count,
            eligible_count=0,
            excluded_count=total_count,
            mean_brier_score=None,
            expected_calibration_error=None,
            bins=(),
        )

    width = Decimal("1") / Decimal(bins)
    grouped: dict[int, list[tuple[Decimal, Decimal]]] = {}
    for confidence, correct, _ in eligible:
        index = min(int(confidence * bins), bins - 1)
        grouped.setdefault(index, []).append((confidence, correct))

    calibration_bins = []
    weighted_gap = Decimal("0")
    for index, values in sorted(grouped.items()):
        count = len(values)
        mean_confidence = sum(value[0] for value in values) / count
        observed_accuracy = sum(value[1] for value in values) / count
        weighted_gap += abs(mean_confidence - observed_accuracy) * count
        calibration_bins.append(
            CalibrationBin(
                lower_bound=width * index,
                upper_bound=width * (index + 1),
                count=count,
                mean_confidence=mean_confidence,
                observed_accuracy=observed_accuracy,
            )
        )

    return CalibrationSummary(
        total_count=total_count,
        eligible_count=eligible_count,
        excluded_count=total_count - eligible_count,
        mean_brier_score=sum(value[2] for value in eligible) / eligible_count,
        expected_calibration_error=weighted_gap / eligible_count,
        bins=tuple(calibration_bins),
    )
