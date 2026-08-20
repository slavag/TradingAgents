"""Append-only evaluation storage and operational forecast evaluation."""

from decimal import Decimal
from pathlib import Path

import pytest

from tests.test_forecast_outcomes import observations, record
from tests.test_forecast_scoring import resolved
from tradingagents.evaluation.registry import EvaluationRegistry
from tradingagents.evaluation.runtime import (
    EvaluationRunStatus,
    PriceHistoryBundle,
    evaluate_forecast,
    evaluate_report_tree,
)
from tradingagents.evaluation.scoring import score_forecast
from tradingagents.forecasting.schemas import AdjustmentBasis


def test_registry_writes_and_reads_typed_outcome_and_score(tmp_path):
    forecast = record()
    outcome = resolved(forecast)
    score = score_forecast(forecast, outcome)
    registry = EvaluationRegistry(tmp_path)

    outcome_artifact = registry.write_outcome(outcome)
    score_artifact = registry.write_score(score)

    assert outcome_artifact.path == tmp_path / "evaluation" / "outcome.json"
    assert score_artifact.path == tmp_path / "evaluation" / "score.json"
    assert registry.read_outcome() == outcome
    assert registry.read_score() == score


def test_registry_repeated_identical_write_is_idempotent(tmp_path):
    outcome = resolved(record())
    registry = EvaluationRegistry(tmp_path)

    first = registry.write_outcome(outcome)
    original = first.path.read_text()
    second = registry.write_outcome(outcome)

    assert second == first
    assert first.path.read_text() == original


def test_registry_rejects_conflicting_immutable_outcome(tmp_path):
    forecast = record()
    registry = EvaluationRegistry(tmp_path)
    registry.write_outcome(resolved(forecast))
    changed = resolved(
        forecast,
        (
            observations()[0].model_copy(update={"close": Decimal("103")}),
            observations()[1],
            observations()[2],
        ),
    )

    with pytest.raises(FileExistsError, match="immutable evaluation artifact"):
        registry.write_outcome(changed)


def test_registry_refuses_to_replace_corrupt_existing_artifact(tmp_path):
    path = tmp_path / "evaluation" / "outcome.json"
    path.parent.mkdir(parents=True)
    path.write_text("not-json")

    with pytest.raises(FileExistsError, match="invalid immutable evaluation artifact"):
        EvaluationRegistry(tmp_path).write_outcome(resolved(record()))


def test_registry_returns_none_when_artifact_is_absent(tmp_path):
    registry = EvaluationRegistry(Path(tmp_path))

    assert registry.read_outcome() is None
    assert registry.read_score() is None


class StaticProvider:
    def __init__(self, bundle=None, error=None):
        self.bundle = bundle
        self.error = error
        self.calls = 0

    def load(self, forecast):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.bundle


def price_bundle(*, rows=None, benchmark=False):
    return PriceHistoryBundle(
        adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
        observations=rows if rows is not None else observations(),
        benchmark_reference_price=Decimal("200") if benchmark else None,
        benchmark_observations=(
            tuple(
                item.model_copy(update={"close": item.close * 2})
                for item in observations()
            )
            if benchmark
            else ()
        ),
    )


def test_evaluate_forecast_scores_matured_record_and_persists_artifacts(tmp_path):
    forecast = record()

    result = evaluate_forecast(forecast, tmp_path, StaticProvider(price_bundle()))

    assert result.status is EvaluationRunStatus.SCORED
    assert result.outcome_artifact is not None
    assert result.score_artifact is not None
    assert EvaluationRegistry(tmp_path).read_score().gross_return == Decimal("0.05")


def test_evaluate_forecast_keeps_immature_record_retryable(tmp_path):
    result = evaluate_forecast(
        record(horizon_sessions=3),
        tmp_path,
        StaticProvider(price_bundle(rows=observations()[:2])),
    )

    assert result.status is EvaluationRunStatus.NOT_MATURE
    assert result.score_artifact is None
    assert EvaluationRegistry(tmp_path).read_outcome() is None


def test_evaluate_forecast_contains_provider_failure_for_retry(tmp_path):
    result = evaluate_forecast(
        record(),
        tmp_path,
        StaticProvider(error=RuntimeError("secret vendor detail")),
    )

    assert result.status is EvaluationRunStatus.RETRYABLE_PROVIDER_ERROR
    assert result.reason == "price_provider_failed"
    assert "secret vendor detail" not in str(result)


def test_evaluate_forecast_skips_provider_when_score_already_exists(tmp_path):
    forecast = record()
    provider = StaticProvider(price_bundle())
    evaluate_forecast(forecast, tmp_path, provider)

    second = evaluate_forecast(forecast, tmp_path, provider)

    assert second.status is EvaluationRunStatus.ALREADY_SCORED
    assert provider.calls == 1


def test_evaluate_report_tree_rejects_invalid_forecast_record(tmp_path):
    (tmp_path / "forecast_record.json").write_text("not-json")

    result = evaluate_report_tree(tmp_path, StaticProvider(price_bundle()))

    assert result.status is EvaluationRunStatus.INVALID_RECORD
    assert result.reason == "forecast_record_invalid"


def test_evaluate_forecast_carries_benchmark_return(tmp_path):
    result = evaluate_forecast(
        record(),
        tmp_path,
        StaticProvider(price_bundle(benchmark=True)),
    )

    assert result.status is EvaluationRunStatus.SCORED
    outcome = EvaluationRegistry(tmp_path).read_outcome()
    assert outcome.benchmark_return == Decimal("0.05")
    assert outcome.excess_return == Decimal("0")
