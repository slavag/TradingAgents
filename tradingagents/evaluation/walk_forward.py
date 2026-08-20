"""Point-in-time walk-forward folds and deterministic promotion gates."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from tradingagents.evaluation.scoring import ForecastScore


class EvaluationSample(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    record_id: str
    cutoff_date: date
    role: str
    configuration_id: str
    regime: str
    source_availability: tuple[str, ...]
    score: ForecastScore

    @field_validator("record_id", "role", "configuration_id", "regime")
    @classmethod
    def _nonempty_identity(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("sample identity fields cannot be empty")
        return value.strip()

    @model_validator(mode="after")
    def _score_matches_record(self):
        if self.score.record_id != self.record_id:
            raise ValueError("score record ID must match sample record ID")
        return self


class WalkForwardFold(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    fold_index: int
    train_start: date
    train_end: date
    promotion_start: date
    promotion_end: date
    evaluation_start: date
    evaluation_end: date
    train_samples: tuple[EvaluationSample, ...]
    promotion_samples: tuple[EvaluationSample, ...]
    evaluation_samples: tuple[EvaluationSample, ...]

    @model_validator(mode="after")
    def _validate_fold_boundaries(self):
        if not (
            self.train_start
            <= self.train_end
            < self.promotion_start
            <= self.promotion_end
            < self.evaluation_start
            <= self.evaluation_end
        ):
            raise ValueError("fold windows must be chronological and non-overlapping")

        windows = (
            (self.train_samples, self.train_start, self.train_end),
            (self.promotion_samples, self.promotion_start, self.promotion_end),
            (self.evaluation_samples, self.evaluation_start, self.evaluation_end),
        )
        record_sets = []
        for samples, start, end in windows:
            if any(not start <= sample.cutoff_date <= end for sample in samples):
                raise ValueError("sample cutoff must lie inside its fold window")
            record_sets.append({sample.record_id for sample in samples})
        if any(
            first & second
            for index, first in enumerate(record_sets)
            for second in record_sets[index + 1 :]
        ):
            raise ValueError("record IDs cannot cross fold windows")
        return self


def _sorted_samples(samples: tuple[EvaluationSample, ...]) -> tuple[EvaluationSample, ...]:
    return tuple(
        sorted(
            samples,
            key=lambda sample: (
                sample.cutoff_date,
                sample.record_id,
                sample.configuration_id,
            ),
        )
    )


def build_walk_forward_folds(
    samples: tuple[EvaluationSample, ...],
    *,
    train_days: int,
    promotion_days: int,
    evaluation_days: int,
    step_days: int,
) -> tuple[WalkForwardFold, ...]:
    """Build complete calendar-based walk-forward folds in stable order."""
    durations = (train_days, promotion_days, evaluation_days, step_days)
    if any(value <= 0 for value in durations):
        raise ValueError("walk-forward window sizes must be positive")
    if not samples:
        return ()

    seen_pairs = set()
    for sample in samples:
        pair = (sample.record_id, sample.configuration_id)
        if pair in seen_pairs:
            raise ValueError("duplicate record/configuration pair")
        seen_pairs.add(pair)

    ordered = _sorted_samples(samples)
    first_cutoff = ordered[0].cutoff_date
    last_cutoff = ordered[-1].cutoff_date
    folds = []
    fold_start = first_cutoff
    fold_index = 0
    while True:
        train_start = fold_start
        train_end = train_start + timedelta(days=train_days - 1)
        promotion_start = train_end + timedelta(days=1)
        promotion_end = promotion_start + timedelta(days=promotion_days - 1)
        evaluation_start = promotion_end + timedelta(days=1)
        evaluation_end = evaluation_start + timedelta(days=evaluation_days - 1)
        if evaluation_end > last_cutoff:
            break

        def select(start: date, end: date) -> tuple[EvaluationSample, ...]:
            return tuple(
                sample for sample in ordered if start <= sample.cutoff_date <= end
            )

        folds.append(
            WalkForwardFold(
                fold_index=fold_index,
                train_start=train_start,
                train_end=train_end,
                promotion_start=promotion_start,
                promotion_end=promotion_end,
                evaluation_start=evaluation_start,
                evaluation_end=evaluation_end,
                train_samples=select(train_start, train_end),
                promotion_samples=select(promotion_start, promotion_end),
                evaluation_samples=select(evaluation_start, evaluation_end),
            )
        )
        fold_index += 1
        fold_start += timedelta(days=step_days)

    return tuple(folds)


class PromotionThresholds(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    minimum_paired_samples: int
    minimum_challenger_coverage: Decimal
    minimum_direction_accuracy_delta: Decimal
    minimum_mean_excess_return_delta: Decimal
    maximum_brier_regression: Decimal
    maximum_drawdown_regression: Decimal

    @model_validator(mode="after")
    def _validate_thresholds(self):
        if self.minimum_paired_samples <= 0:
            raise ValueError("minimum paired samples must be positive")
        if not Decimal("0") <= self.minimum_challenger_coverage <= Decimal("1"):
            raise ValueError("minimum challenger coverage must be between zero and one")
        if self.maximum_brier_regression < 0 or self.maximum_drawdown_regression < 0:
            raise ValueError("maximum regression thresholds cannot be negative")
        return self


class MetricAggregates(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    direction_accuracy: Decimal
    mean_excess_return: Decimal
    mean_brier_score: Decimal
    mean_maximum_drawdown: Decimal


class PromotionDecision(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    promoted: bool
    rejection_reasons: tuple[str, ...]


class PairedComparison(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    role: str
    incumbent_configuration_id: str
    challenger_configuration_id: str
    incumbent_count: int
    challenger_count: int
    paired_count: int
    challenger_coverage: Decimal
    incumbent_metrics: MetricAggregates | None
    challenger_metrics: MetricAggregates | None
    direction_accuracy_delta: Decimal | None
    mean_excess_return_delta: Decimal | None
    brier_regression: Decimal | None
    drawdown_regression: Decimal | None
    decision: PromotionDecision


def _configuration(samples: tuple[EvaluationSample, ...], label: str) -> tuple[str, str]:
    configurations = {sample.configuration_id for sample in samples}
    roles = {sample.role for sample in samples}
    if len(configurations) != 1 or len(roles) != 1:
        raise ValueError(f"{label} samples must share one role and configuration")
    return next(iter(roles)), next(iter(configurations))


def _paired_aggregates(samples: tuple[EvaluationSample, ...]) -> MetricAggregates | None:
    if not samples:
        return None
    scores = [sample.score for sample in samples]
    required = (
        "direction_correct",
        "excess_return",
        "brier_score",
        "maximum_drawdown",
    )
    if any(getattr(score, field) is None for score in scores for field in required):
        return None
    count = Decimal(len(scores))
    return MetricAggregates(
        direction_accuracy=sum(
            Decimal("1") if score.direction_correct else Decimal("0")
            for score in scores
        )
        / count,
        mean_excess_return=sum(score.excess_return for score in scores) / count,
        mean_brier_score=sum(score.brier_score for score in scores) / count,
        mean_maximum_drawdown=sum(score.maximum_drawdown for score in scores) / count,
    )


def compare_paired_configurations(
    incumbent: tuple[EvaluationSample, ...],
    challenger: tuple[EvaluationSample, ...],
    thresholds: PromotionThresholds,
) -> PairedComparison:
    """Compare configurations only on shared record IDs and apply every gate."""
    incumbent_role, incumbent_config = _configuration(incumbent, "incumbent")
    challenger_role, challenger_config = _configuration(challenger, "challenger")
    if incumbent_role != challenger_role:
        raise ValueError("incumbent and challenger roles must match")
    incumbent_by_id = {sample.record_id: sample for sample in incumbent}
    challenger_by_id = {sample.record_id: sample for sample in challenger}
    if len(incumbent_by_id) != len(incumbent) or len(challenger_by_id) != len(challenger):
        raise ValueError("configuration samples must have unique record IDs")

    shared_ids = sorted(incumbent_by_id.keys() & challenger_by_id.keys())
    incumbent_pairs = tuple(incumbent_by_id[record_id] for record_id in shared_ids)
    challenger_pairs = tuple(challenger_by_id[record_id] for record_id in shared_ids)
    paired_count = len(shared_ids)
    coverage = Decimal(paired_count) / Decimal(len(incumbent)) if incumbent else Decimal("0")
    incumbent_metrics = _paired_aggregates(incumbent_pairs)
    challenger_metrics = _paired_aggregates(challenger_pairs)

    direction_delta = None
    excess_delta = None
    brier_regression = None
    drawdown_regression = None
    reasons = []
    if paired_count < thresholds.minimum_paired_samples:
        reasons.append("insufficient_paired_samples")
    if coverage < thresholds.minimum_challenger_coverage:
        reasons.append("insufficient_challenger_coverage")
    if incumbent_metrics is None or challenger_metrics is None:
        reasons.append("required_metric_missing")
    else:
        direction_delta = (
            challenger_metrics.direction_accuracy - incumbent_metrics.direction_accuracy
        )
        excess_delta = (
            challenger_metrics.mean_excess_return - incumbent_metrics.mean_excess_return
        )
        brier_regression = (
            challenger_metrics.mean_brier_score - incumbent_metrics.mean_brier_score
        )
        drawdown_regression = (
            incumbent_metrics.mean_maximum_drawdown
            - challenger_metrics.mean_maximum_drawdown
        )
        if direction_delta < thresholds.minimum_direction_accuracy_delta:
            reasons.append("direction_accuracy_delta_below_threshold")
        if excess_delta < thresholds.minimum_mean_excess_return_delta:
            reasons.append("mean_excess_return_delta_below_threshold")
        if brier_regression > thresholds.maximum_brier_regression:
            reasons.append("brier_regression_exceeded")
        if drawdown_regression > thresholds.maximum_drawdown_regression:
            reasons.append("drawdown_regression_exceeded")

    sorted_reasons = tuple(sorted(set(reasons)))
    return PairedComparison(
        role=incumbent_role,
        incumbent_configuration_id=incumbent_config,
        challenger_configuration_id=challenger_config,
        incumbent_count=len(incumbent),
        challenger_count=len(challenger),
        paired_count=paired_count,
        challenger_coverage=coverage,
        incumbent_metrics=incumbent_metrics,
        challenger_metrics=challenger_metrics,
        direction_accuracy_delta=direction_delta,
        mean_excess_return_delta=excess_delta,
        brier_regression=brier_regression,
        drawdown_regression=drawdown_regression,
        decision=PromotionDecision(
            promoted=not sorted_reasons,
            rejection_reasons=sorted_reasons,
        ),
    )
