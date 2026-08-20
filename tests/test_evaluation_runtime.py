"""Append-only evaluation storage and operational forecast evaluation."""

from decimal import Decimal
from pathlib import Path

import pytest

from tests.test_forecast_outcomes import observations, record
from tests.test_forecast_scoring import resolved
from tradingagents.evaluation.registry import EvaluationRegistry
from tradingagents.evaluation.scoring import score_forecast


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
