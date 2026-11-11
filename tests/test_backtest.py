"""Offline contract tests for independent ticker/date backtest sweeps."""

from decimal import Decimal
from pathlib import Path
from shutil import rmtree
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import tradingagents.backtest as backtest
from cli.main import app as cli_app
from tests.test_forecast_outcomes import observations, record
from tradingagents.evaluation.outcomes import resolve_forecast_outcome
from tradingagents.evaluation.registry import EvaluationRegistry
from tradingagents.evaluation.runtime import (
    EvaluationBatchSummary,
    EvaluationRunResult,
    EvaluationRunStatus,
)
from tradingagents.evaluation.scoring import score_forecast
from tradingagents.forecasting.record_factory import forecast_record_from_state
from tradingagents.forecasting.schemas import AdjustmentBasis, ForecastDecisionStatus
from tradingagents.graph.trading_graph import Settlement


class FakeGraph:
    calls = []
    fail_on = set()
    settle_failures = []

    def __init__(self, selected_analysts, *, config):
        self.config = config
        self.analysts = selected_analysts

    def propagate(self, ticker, trade_date, asset_type="stock", portfolio=None):
        self.calls.append((ticker, trade_date, asset_type, portfolio))
        if (ticker, trade_date) in self.fail_on:
            raise RuntimeError("fixture vendor outage")
        return {
            "company_of_interest": ticker,
            "trade_date": trade_date,
            "final_trade_decision": "**Rating**: Review",
        }, "Review"

    def settle_all_pending(self):
        return Settlement(failed=list(self.settle_failures))


@pytest.fixture(autouse=True)
def offline_graph(monkeypatch):
    FakeGraph.calls = []
    FakeGraph.fail_on = set()
    FakeGraph.settle_failures = []
    monkeypatch.setattr(backtest, "TradingAgentsGraph", FakeGraph)

    def write_marker(_state, _ticker, path, **_kwargs):
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        (path / "forecast_record.json").write_text("{}", encoding="utf-8")
        return path / "complete_report.md"

    monkeypatch.setattr(backtest, "write_report_tree", write_marker)
    monkeypatch.setattr(backtest, "evaluate_report_trees", lambda *a, **kw: None)


def config(tmp_path):
    return {
        "results_dir": str(tmp_path / "results"),
        "memory_log_path": str(tmp_path / "live-memory.md"),
    }


def test_generated_run_ids_claim_unique_directories_even_on_collision(tmp_path, monkeypatch):
    ids = iter(("same", "same", "next"))
    monkeypatch.setattr(backtest, "_new_run_id", lambda: next(ids))

    first = backtest.run_backtest([], [], config(tmp_path))
    second = backtest.run_backtest([], [], config(tmp_path))

    assert first.run_id == "same"
    assert second.run_id == "next"
    assert first.run_dir != second.run_dir


def test_duplicate_cells_and_resume_only_run_missing_cells(tmp_path):
    first = backtest.run_backtest(
        ["NVDA", "NVDA"], ["2026-01-05", "2026-01-05", "2026-01-12"],
        config(tmp_path), selected_analysts=("market", "news"),
        run_id="sweep-1",
    )
    assert first.cells_run == 2
    assert FakeGraph.calls == [
        ("NVDA", "2026-01-05", "stock", None),
        ("NVDA", "2026-01-12", "stock", None),
    ]
    assert not (tmp_path / "live-memory.md").exists()

    FakeGraph.calls = []
    resumed = backtest.run_backtest(
        ["NVDA"], ["2026-01-05", "2026-01-12"], config(tmp_path),
        selected_analysts=("market", "news"), run_id="sweep-1",
    )
    assert resumed.cells_run == 0
    assert resumed.skipped == 2
    assert FakeGraph.calls == []


@pytest.mark.parametrize("changed", [
    {"deep_backend_url": "https://other.example/v1"},
    {"google_thinking_level": "high"},
    {"global_news_queries": ["different market query"]},
    {"benchmark_map": {"": "QQQ"}},
])
def test_resume_rejects_any_decision_affecting_config_change(tmp_path, changed):
    baseline = config(tmp_path)
    backtest.run_backtest(
        ["NVDA"], ["2026-01-05"], baseline, run_id="pinned-config",
    )
    FakeGraph.calls = []
    with pytest.raises(ValueError, match="resume inputs differ"):
        backtest.run_backtest(
            ["NVDA"], ["2026-01-05"], {**baseline, **changed},
            run_id="pinned-config",
        )
    assert FakeGraph.calls == []


def test_backtest_refuses_inline_secret_config_before_writing_manifest(tmp_path):
    with pytest.raises(ValueError, match="secret"):
        backtest.run_backtest(
            ["NVDA"], ["2026-01-05"],
            {**config(tmp_path), "api_key": "do-not-persist"},
            run_id="secret-rejected",
        )
    assert not (tmp_path / "results" / "backtest" / "secret-rejected").exists()


def test_interrupted_cell_resumes_and_settlement_failure_is_reported(tmp_path):
    FakeGraph.fail_on = {("NVDA", "2026-01-05")}
    FakeGraph.settle_failures = [
        ("AAPL", "2026-01-01", "reflection unavailable"),
    ]
    first = backtest.run_backtest(
        ["NVDA", "AAPL"], ["2026-01-05"], config(tmp_path), run_id="resume",
    )
    assert first.cells_run == 1
    assert first.failures == [("NVDA", "2026-01-05", "RuntimeError")]
    assert first.settlement_failures == FakeGraph.settle_failures

    FakeGraph.fail_on = set()
    FakeGraph.calls = []
    resumed = backtest.run_backtest(
        ["NVDA", "AAPL"], ["2026-01-05"], config(tmp_path), run_id="resume",
    )
    assert resumed.cells_run == 1
    assert resumed.skipped == 1
    assert FakeGraph.calls == [("NVDA", "2026-01-05", "stock", None)]


def test_run_id_cannot_escape_results_root(tmp_path):
    with pytest.raises(ValueError):
        backtest.run_backtest(["NVDA"], ["2026-01-05"], config(tmp_path), run_id="../escape")


def test_symlinked_run_id_cannot_escape_results_root(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "results" / "backtest"
    root.mkdir(parents=True)
    (root / "escape").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        backtest.run_backtest([], [], config(tmp_path), run_id="escape")
    assert list(outside.iterdir()) == []


@pytest.mark.parametrize("component", [".staging", "cells/2026-01-05"])
def test_symlinked_backtest_child_cannot_write_outside_run(tmp_path, component):
    FakeGraph.fail_on = {("NVDA", "2026-01-05")}
    first = backtest.run_backtest(
        ["NVDA"], ["2026-01-05"], config(tmp_path), run_id="child-escape",
    )
    assert first.cells_run == 0
    target = first.run_dir / component
    rmtree(target)
    outside = tmp_path / "outside"
    outside.mkdir()
    target.symlink_to(outside, target_is_directory=True)
    FakeGraph.fail_on = set()

    with pytest.raises(ValueError, match="symlink"):
        backtest.run_backtest(
            ["NVDA"], ["2026-01-05"], config(tmp_path), run_id="child-escape",
        )
    assert list(outside.iterdir()) == []


def test_date_grid_rejects_reversed_and_noncanonical_bounds():
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        backtest.iter_grid("2026-1-5", "2026-01-20")
    with pytest.raises(ValueError, match="before"):
        backtest.iter_grid("2026-02-01", "2026-01-01")


def test_summary_uses_registry_scores_and_keeps_review_unscored(tmp_path):
    run_dir = tmp_path / "sweep"
    scored_dir = run_dir / "cells" / "2026-08-21" / "TEST"
    scored_dir.mkdir(parents=True)
    forecast = record()
    (scored_dir / "forecast_record.json").write_text(
        forecast.model_dump_json(), encoding="utf-8"
    )
    outcome = resolve_forecast_outcome(
        forecast, observations(), adjustment_basis=AdjustmentBasis.SPLIT_ADJUSTED,
    )
    EvaluationRegistry(scored_dir).write_score(score_forecast(forecast, outcome))

    review_dir = run_dir / "cells" / "2026-08-21" / "REVIEW"
    review_dir.mkdir(parents=True)
    review = forecast.model_copy(update={
        "canonical_symbol": "REVIEW",
        "decision_status": ForecastDecisionStatus.ABSTAIN,
        "rating": None,
    })
    (review_dir / "forecast_record.json").write_text(
        review.model_dump_json(), encoding="utf-8"
    )

    summary = backtest.summarize(run_dir)
    assert summary.resolved == 1
    assert summary.unscored == 1
    assert summary.pending == 0
    assert summary.by_rating["Hold"].count == 1
    assert summary.by_rating["Hold"].mean_gross_return == Decimal("0.05")
    assert "recorded reference" in summary.render().lower()
    assert "first subsequent close" in summary.render().lower()


def test_evaluation_provider_failure_is_not_reported_as_immature(tmp_path, monkeypatch):
    def write_actionable(_state, _ticker, path, **_kwargs):
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        (path / "forecast_record.json").write_text(
            record().model_dump_json(), encoding="utf-8"
        )

    monkeypatch.setattr(backtest, "write_report_tree", write_actionable)
    monkeypatch.setattr(
        backtest, "evaluate_report_trees", lambda *a, **kw: EvaluationBatchSummary(
            total=1, scored=0, not_mature=0, retryable_errors=1,
            already_scored=0, invalid=0,
            results=(EvaluationRunResult(
                status=EvaluationRunStatus.RETRYABLE_PROVIDER_ERROR,
                record_id=record().record_id, reason="price_provider_failed",
            ),),
        ),
    )
    result = backtest.run_backtest(
        ["TEST"], ["2026-08-21"], config(tmp_path), run_id="provider-outage",
        price_provider=object(),
    )
    summary = backtest.summarize(result)
    assert summary.retryable_errors == 1
    assert summary.pending == 0
    assert "provider error" in summary.render().lower()


def test_review_cell_is_saved_but_never_sent_to_price_provider(tmp_path, monkeypatch):
    evaluated = []

    def write_review(state, ticker, path, **_kwargs):
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        record = forecast_record_from_state(state, ticker, {})
        (path / "forecast_record.json").write_text(
            record.model_dump_json(), encoding="utf-8"
        )
        return path / "complete_report.md"

    monkeypatch.setattr(backtest, "write_report_tree", write_review)
    monkeypatch.setattr(
        backtest, "evaluate_report_trees",
        lambda *a, **kw: evaluated.append((a, kw)),
    )
    result = backtest.run_backtest(
        ["NVDA"], ["2026-01-05"], config(tmp_path), run_id="review",
    )
    assert result.cells_run == 1
    assert evaluated == []
    assert backtest.summarize(result).unscored == 1


def test_partial_report_write_is_retried_on_resume(tmp_path, monkeypatch):
    attempts = []

    def interrupted_write(_state, _ticker, path, **_kwargs):
        attempts.append(Path(path))
        Path(path).mkdir(parents=True, exist_ok=True)
        (Path(path) / "forecast_record.json").write_text("{}", encoding="utf-8")
        if len(attempts) == 1:
            raise OSError("interrupted write")

    monkeypatch.setattr(backtest, "write_report_tree", interrupted_write)
    first = backtest.run_backtest(
        ["NVDA"], ["2026-01-05"], config(tmp_path), run_id="interrupted",
    )
    assert first.cells_run == 0
    assert first.failures == [("NVDA", "2026-01-05", "OSError")]

    second = backtest.run_backtest(
        ["NVDA"], ["2026-01-05"], config(tmp_path), run_id="interrupted",
    )
    assert second.cells_run == 1
    assert second.skipped == 0
    assert len(attempts) == 2


def test_only_identity_matched_forecast_cells_are_evaluated(tmp_path, monkeypatch):
    evaluated = []

    def write_record(_state, _ticker, path, **_kwargs):
        path = Path(path)
        path.mkdir(parents=True, exist_ok=True)
        (path / "forecast_record.json").write_text(
            record().model_dump_json(), encoding="utf-8"
        )

    monkeypatch.setattr(backtest, "write_report_tree", write_record)
    monkeypatch.setattr(
        backtest, "evaluate_report_trees",
        lambda *args, **kwargs: evaluated.append((args, kwargs)),
    )
    provider = object()
    matched = backtest.run_backtest(
        ["TEST"], ["2026-08-21"], config(tmp_path),
        run_id="matched", price_provider=provider,
    )
    assert matched.cells_run == 1
    assert len(evaluated) == 1
    assert evaluated[0][0][1] is provider

    evaluated.clear()
    mismatched = backtest.run_backtest(
        ["NVDA"], ["2026-08-21"], config(tmp_path),
        run_id="mismatched", price_provider=provider,
    )
    assert mismatched.cells_run == 1
    assert evaluated == []
    assert backtest.summarize(mismatched).unscored == 1


def test_cli_backtest_runs_explicit_grid_without_interactive_prompts(monkeypatch):
    captured = {}

    def fake_run(tickers, dates, config, **kwargs):
        captured.update(tickers=tickers, dates=dates, config=config, kwargs=kwargs)
        return SimpleNamespace(run_id="sample", run_dir=Path("/tmp/sample"),
                               cells_run=2, skipped=0, failures=[], settlement_failures=[])

    monkeypatch.setattr(backtest, "run_backtest", fake_run)
    monkeypatch.setattr(
        backtest, "summarize", lambda source: SimpleNamespace(render=lambda: "2 scored")
    )
    result = CliRunner().invoke(cli_app, [
        "backtest", "--tickers", "NVDA,AAPL", "--start", "2026-01-01",
        "--end", "2026-01-03", "--every", "2", "--analysts", "market,news",
        "--run-id", "sample",
    ])
    assert result.exit_code == 0, result.output
    assert captured["tickers"] == ["NVDA", "AAPL"]
    assert captured["dates"] == ["2026-01-01", "2026-01-03"]
    assert captured["kwargs"]["selected_analysts"] == ("market", "news")
    assert "2 scored" in result.output
