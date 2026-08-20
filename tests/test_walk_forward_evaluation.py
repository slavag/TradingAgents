"""Point-in-time folds, paired promotion gates, and role leaderboards."""

from datetime import date, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tradingagents.evaluation.outcomes import OutcomeResolutionStatus
from tradingagents.evaluation.scoring import ForecastScore
from tradingagents.evaluation.walk_forward import (
    EvaluationSample,
    WalkForwardFold,
    build_walk_forward_folds,
)


def score(record_id: str) -> ForecastScore:
    return ForecastScore(
        record_id=record_id,
        outcome_status=OutcomeResolutionStatus.RESOLVED,
        predicted_direction=None,
        realized_direction=None,
        direction_correct=True,
        gross_return=Decimal("0.01"),
        net_return=Decimal("0.01"),
        excess_return=Decimal("0.005"),
        transaction_cost=Decimal("0"),
        target_mae=None,
        target_mape=None,
        interval_covered=None,
        brier_score=Decimal("0.2"),
        maximum_drawdown=Decimal("-0.01"),
        direction_probabilities=None,
        missing_metrics=("target_mae", "target_mape", "interval_covered"),
    )


def sample(day: int, *, record_id: str | None = None, config="cfg-a"):
    record_id = record_id or f"record-{day:02d}"
    return EvaluationSample(
        record_id=record_id,
        cutoff_date=date(2026, 1, 1) + timedelta(days=day - 1),
        role="deep",
        configuration_id=config,
        regime="neutral",
        source_availability=("market", "fundamentals"),
        score=score(record_id),
    )


def test_fold_boundaries_are_exact_and_chronological():
    samples = tuple(sample(day) for day in range(1, 13))

    folds = build_walk_forward_folds(
        samples,
        train_days=3,
        promotion_days=2,
        evaluation_days=2,
        step_days=7,
    )

    assert len(folds) == 1
    fold = folds[0]
    assert (fold.train_start, fold.train_end) == (date(2026, 1, 1), date(2026, 1, 3))
    assert (fold.promotion_start, fold.promotion_end) == (
        date(2026, 1, 4),
        date(2026, 1, 5),
    )
    assert (fold.evaluation_start, fold.evaluation_end) == (
        date(2026, 1, 6),
        date(2026, 1, 7),
    )
    assert tuple(item.record_id for item in fold.train_samples) == (
        "record-01",
        "record-02",
        "record-03",
    )
    assert tuple(item.record_id for item in fold.promotion_samples) == (
        "record-04",
        "record-05",
    )
    assert tuple(item.record_id for item in fold.evaluation_samples) == (
        "record-06",
        "record-07",
    )


def test_fold_builder_drops_incomplete_trailing_window():
    folds = build_walk_forward_folds(
        tuple(sample(day) for day in range(1, 7)),
        train_days=3,
        promotion_days=2,
        evaluation_days=2,
        step_days=7,
    )

    assert folds == ()


def test_fold_rejects_nonpositive_window_sizes():
    with pytest.raises(ValueError, match="positive"):
        build_walk_forward_folds(
            (sample(1),),
            train_days=0,
            promotion_days=1,
            evaluation_days=1,
            step_days=1,
        )


def test_fold_rejects_duplicate_record_configuration_pair():
    with pytest.raises(ValueError, match="duplicate record/configuration"):
        build_walk_forward_folds(
            (sample(1, record_id="same"), sample(2, record_id="same")),
            train_days=1,
            promotion_days=1,
            evaluation_days=1,
            step_days=1,
        )


def test_fold_output_is_independent_of_input_order():
    ordered = tuple(sample(day) for day in range(1, 8))

    first = build_walk_forward_folds(
        ordered,
        train_days=3,
        promotion_days=2,
        evaluation_days=2,
        step_days=7,
    )
    second = build_walk_forward_folds(
        tuple(reversed(ordered)),
        train_days=3,
        promotion_days=2,
        evaluation_days=2,
        step_days=7,
    )

    assert first == second


def test_fold_schema_rejects_internal_record_leakage():
    leaked = sample(1, record_id="leaked")

    with pytest.raises(ValidationError, match="record IDs cannot cross fold windows"):
        WalkForwardFold(
            fold_index=0,
            train_start=date(2026, 1, 1),
            train_end=date(2026, 1, 1),
            promotion_start=date(2026, 1, 2),
            promotion_end=date(2026, 1, 2),
            evaluation_start=date(2026, 1, 3),
            evaluation_end=date(2026, 1, 3),
            train_samples=(leaked,),
            promotion_samples=(leaked.model_copy(update={"cutoff_date": date(2026, 1, 2)}),),
            evaluation_samples=(),
        )
