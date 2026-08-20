"""Append-only evaluation storage and operational forecast evaluation."""

from decimal import Decimal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cli.main import app as cli_app
from tests.test_forecast_outcomes import observations, record
from tests.test_forecast_scoring import resolved
from tradingagents.evaluation.registry import EvaluationRegistry
from tradingagents.evaluation.runtime import (
    EvaluationRunStatus,
    EvaluationScope,
    PriceHistoryBundle,
    evaluate_forecast,
    evaluate_report_tree,
    evaluate_report_trees,
)
from tradingagents.evaluation.scoring import score_forecast
from tradingagents.forecasting.record_factory import create_forecast_record
from tradingagents.forecasting.schemas import (
    AdjustmentBasis,
    ForecastRecordPayload,
)
from tradingagents.web.service import evaluate_saved_forecasts


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


def test_batch_evaluation_scans_report_trees_and_summarizes_statuses(tmp_path):
    mature_dir = tmp_path / "A" / "run-1"
    pending_dir = tmp_path / "B" / "run-2"
    mature_dir.mkdir(parents=True)
    pending_dir.mkdir(parents=True)
    mature_dir.joinpath("forecast_record.json").write_text(
        record().model_dump_json(indent=2)
    )
    pending_dir.joinpath("forecast_record.json").write_text(
        record(horizon_sessions=10).model_dump_json(indent=2)
    )

    summary = evaluate_report_trees(tmp_path, StaticProvider(price_bundle()))

    assert summary.total == 2
    assert summary.scored == 1
    assert summary.not_mature == 1
    assert summary.retryable_errors == 0
    assert summary.invalid == 0
    assert tuple(result.status for result in summary.results) == (
        EvaluationRunStatus.SCORED,
        EvaluationRunStatus.NOT_MATURE,
    )


def scoped_record(symbol: str, cutoff: str, *, horizon_sessions=2):
    original = record(horizon_sessions=horizon_sessions)
    payload = original.model_dump(exclude={"record_id"})
    payload.update({"canonical_symbol": symbol, "data_cutoff": cutoff})
    return create_forecast_record(ForecastRecordPayload.model_validate(payload))


def write_forecast(root, ticker, run_name, forecast):
    report = root / ticker / run_name
    report.mkdir(parents=True)
    report.joinpath("forecast_record.json").write_text(forecast.model_dump_json(indent=2))
    return report


def test_batch_scope_filters_tickers_and_inclusive_dates(tmp_path):
    write_forecast(tmp_path, "AAA", "one", scoped_record("AAA", "2026-08-10"))
    write_forecast(tmp_path, "AAA", "two", scoped_record("AAA", "2026-08-20"))
    write_forecast(tmp_path, "BBB", "one", scoped_record("BBB", "2026-08-15"))

    summary = evaluate_report_trees(
        tmp_path,
        StaticProvider(price_bundle()),
        scope=EvaluationScope(
            tickers=("aaa",),
            date_from="2026-08-15",
            date_to="2026-08-20",
        ),
    )

    assert summary.total == 1
    assert summary.results[0].record_id == scoped_record("AAA", "2026-08-20").record_id


def test_batch_scope_pending_only_skips_existing_score(tmp_path):
    scored_dir = write_forecast(tmp_path, "AAA", "scored", scoped_record("AAA", "2026-08-10"))
    pending_forecast = scoped_record("BBB", "2026-08-10", horizon_sessions=10)
    write_forecast(
        tmp_path,
        "BBB",
        "pending",
        pending_forecast,
    )
    evaluate_report_tree(scored_dir, StaticProvider(price_bundle()))

    summary = evaluate_report_trees(
        tmp_path,
        StaticProvider(price_bundle()),
        scope=EvaluationScope(pending_only=True),
    )

    assert summary.total == 1
    assert summary.not_mature == 1
    assert summary.results[0].record_id == pending_forecast.record_id


def test_batch_scope_current_report_roots_select_exact_batch(tmp_path):
    chosen = write_forecast(tmp_path, "AAA", "chosen", scoped_record("AAA", "2026-08-10"))
    write_forecast(tmp_path, "BBB", "other", scoped_record("BBB", "2026-08-10"))

    summary = evaluate_report_trees(
        tmp_path,
        StaticProvider(price_bundle()),
        scope=EvaluationScope(report_roots=(chosen,)),
    )

    assert summary.total == 1
    assert summary.results[0].record_id == scoped_record("AAA", "2026-08-10").record_id


def test_batch_scope_rejects_report_root_outside_results_root(tmp_path):
    outside = tmp_path.parent / "outside-report"
    with pytest.raises(ValueError, match="must stay within results root"):
        evaluate_report_trees(
            tmp_path,
            StaticProvider(price_bundle()),
            scope=EvaluationScope(report_roots=(outside,)),
        )


def test_web_service_evaluation_summary_serializes_empty_root(tmp_path):
    summary = evaluate_saved_forecasts(
        results_root=tmp_path,
        provider=StaticProvider(price_bundle()),
    )

    assert summary == {
        "total": 0,
        "scored": 0,
        "not_mature": 0,
        "retryable_errors": 0,
        "already_scored": 0,
        "invalid": 0,
        "results": [],
    }


def test_web_service_applies_ticker_date_and_pending_scope(tmp_path):
    scored = write_forecast(tmp_path, "AAA", "one", scoped_record("AAA", "2026-08-10"))
    write_forecast(tmp_path, "AAA", "two", scoped_record("AAA", "2026-08-20", horizon_sessions=10))
    write_forecast(tmp_path, "BBB", "one", scoped_record("BBB", "2026-08-20"))
    evaluate_report_tree(scored, StaticProvider(price_bundle()))

    summary = evaluate_saved_forecasts(
        results_root=tmp_path,
        provider=StaticProvider(price_bundle()),
        tickers=("AAA",),
        date_from="2026-08-15",
        date_to="2026-08-20",
        pending_only=True,
    )

    assert summary["total"] == 1
    assert summary["not_mature"] == 1


def test_web_service_current_batch_paths_are_confined_to_results_root(tmp_path):
    with pytest.raises(ValueError, match="must stay within results root"):
        evaluate_saved_forecasts(
            results_root=tmp_path,
            provider=StaticProvider(price_bundle()),
            report_roots=(tmp_path.parent / "outside",),
        )


def test_cli_evaluate_forecasts_reports_empty_root(tmp_path):
    result = CliRunner().invoke(
        cli_app,
        ["evaluate-forecasts", "--results-root", str(tmp_path)],
    )

    assert result.exit_code == 0
    assert "Forecast evaluation: 0 total" in result.stdout
