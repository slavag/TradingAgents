"""Point-in-time walk-forward folds and deterministic promotion gates."""

from __future__ import annotations

from datetime import date, timedelta

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
