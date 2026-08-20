"""Operational runner for resolving and scoring matured forecast records."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Protocol

import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from tradingagents.dataflows.stockstats_utils import load_ohlcv
from tradingagents.evaluation.outcomes import (
    OutcomeResolutionStatus,
    PriceObservation,
    resolve_forecast_outcome,
)
from tradingagents.evaluation.registry import EvaluationArtifact, EvaluationRegistry
from tradingagents.evaluation.scoring import score_forecast
from tradingagents.forecasting.schemas import AdjustmentBasis, ForecastRecord


class PriceHistoryBundle(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    adjustment_basis: AdjustmentBasis
    observations: tuple[PriceObservation, ...]
    benchmark_reference_price: Decimal | None = None
    benchmark_observations: tuple[PriceObservation, ...] = ()


class OutcomePriceProvider(Protocol):
    def load(self, forecast: ForecastRecord) -> PriceHistoryBundle: ...


class EvaluationRunStatus(str, Enum):
    SCORED = "scored"
    NOT_MATURE = "not_mature"
    RETRYABLE_PROVIDER_ERROR = "retryable_provider_error"
    ALREADY_SCORED = "already_scored"
    INVALID_RECORD = "invalid_record"


class EvaluationRunResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    status: EvaluationRunStatus
    record_id: str | None
    reason: str | None
    outcome_artifact: EvaluationArtifact | None = None
    score_artifact: EvaluationArtifact | None = None


class EvaluationBatchSummary(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    total: int
    scored: int
    not_mature: int
    retryable_errors: int
    already_scored: int
    invalid: int
    results: tuple[EvaluationRunResult, ...]


class EvaluationScope(BaseModel):
    """Optional intersection filters for saved forecast evaluation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    tickers: tuple[str, ...] = ()
    date_from: date | None = None
    date_to: date | None = None
    pending_only: bool = False
    report_roots: tuple[Path, ...] = ()

    @field_validator("tickers")
    @classmethod
    def _canonical_tickers(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(sorted({value.strip().upper() for value in values if value.strip()}))

    @model_validator(mode="after")
    def _date_order(self):
        if self.date_from and self.date_to and self.date_from > self.date_to:
            raise ValueError("evaluation date_from cannot exceed date_to")
        return self


def evaluate_forecast(
    forecast: ForecastRecord,
    report_tree: Path,
    provider: OutcomePriceProvider,
    *,
    transaction_cost_bps: Decimal | int = 0,
) -> EvaluationRunResult:
    """Resolve and persist one matured forecast without leaking provider details."""
    registry = EvaluationRegistry(Path(report_tree))
    if registry.read_score() is not None:
        return EvaluationRunResult(
            status=EvaluationRunStatus.ALREADY_SCORED,
            record_id=forecast.record_id,
            reason=None,
        )
    try:
        prices = provider.load(forecast)
    except Exception:
        return EvaluationRunResult(
            status=EvaluationRunStatus.RETRYABLE_PROVIDER_ERROR,
            record_id=forecast.record_id,
            reason="price_provider_failed",
        )

    outcome = resolve_forecast_outcome(
        forecast,
        prices.observations,
        adjustment_basis=prices.adjustment_basis,
        benchmark_reference_price=prices.benchmark_reference_price,
        benchmark_observations=prices.benchmark_observations,
    )
    if outcome.status is OutcomeResolutionStatus.INSUFFICIENT_SESSIONS:
        return EvaluationRunResult(
            status=EvaluationRunStatus.NOT_MATURE,
            record_id=forecast.record_id,
            reason=outcome.reason,
        )
    if outcome.status is not OutcomeResolutionStatus.RESOLVED:
        return EvaluationRunResult(
            status=EvaluationRunStatus.INVALID_RECORD,
            record_id=forecast.record_id,
            reason=outcome.reason,
        )

    outcome_artifact = registry.write_outcome(outcome)
    score = score_forecast(
        forecast,
        outcome,
        transaction_cost_bps=transaction_cost_bps,
    )
    score_artifact = registry.write_score(score)
    return EvaluationRunResult(
        status=EvaluationRunStatus.SCORED,
        record_id=forecast.record_id,
        reason=None,
        outcome_artifact=outcome_artifact,
        score_artifact=score_artifact,
    )


def evaluate_report_tree(
    report_tree: Path,
    provider: OutcomePriceProvider,
    *,
    transaction_cost_bps: Decimal | int = 0,
) -> EvaluationRunResult:
    """Load and evaluate the forecast record stored in one report tree."""
    path = Path(report_tree) / "forecast_record.json"
    try:
        forecast = ForecastRecord.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception:
        return EvaluationRunResult(
            status=EvaluationRunStatus.INVALID_RECORD,
            record_id=None,
            reason="forecast_record_invalid",
        )
    return evaluate_forecast(
        forecast,
        Path(report_tree),
        provider,
        transaction_cost_bps=transaction_cost_bps,
    )


class YFinanceOutcomePriceProvider:
    """Load total-return-adjusted daily sessions through the existing cache."""

    def __init__(self, available_through: date | None = None):
        self.available_through = available_through or date.today()

    def load(self, forecast: ForecastRecord) -> PriceHistoryBundle:
        frame = load_ohlcv(forecast.canonical_symbol, self.available_through.isoformat())
        observations = tuple(
            PriceObservation(
                session=pd.Timestamp(row["Date"]).date(),
                close=Decimal(str(row["Close"])),
            )
            for _, row in frame.iterrows()
            if pd.Timestamp(row["Date"]).date() > forecast.data_cutoff
        )
        return PriceHistoryBundle(
            adjustment_basis=AdjustmentBasis.TOTAL_RETURN_ADJUSTED,
            observations=observations,
        )


def evaluate_report_trees(
    root: Path,
    provider: OutcomePriceProvider,
    *,
    transaction_cost_bps: Decimal | int = 0,
    scope: EvaluationScope | None = None,
) -> EvaluationBatchSummary:
    """Evaluate every saved forecast record below a root in stable path order."""
    root = Path(root).resolve()
    scope = scope or EvaluationScope()
    if scope.report_roots:
        candidates = set()
        for requested_root in scope.report_roots:
            requested = Path(requested_root).resolve()
            try:
                requested.relative_to(root)
            except ValueError as exc:
                raise ValueError("evaluation report roots must stay within results root") from exc
            direct = requested / "forecast_record.json"
            if direct.exists():
                candidates.add(direct)
            else:
                candidates.update(requested.rglob("forecast_record.json"))
        paths = tuple(sorted(candidates))
    else:
        paths = tuple(sorted(root.rglob("forecast_record.json")))

    selected_paths = []
    for path in paths:
        try:
            forecast = ForecastRecord.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception:
            if scope.tickers or scope.date_from or scope.date_to or scope.pending_only:
                continue
            selected_paths.append(path)
            continue
        if scope.tickers and forecast.canonical_symbol not in scope.tickers:
            continue
        if scope.date_from and forecast.data_cutoff < scope.date_from:
            continue
        if scope.date_to and forecast.data_cutoff > scope.date_to:
            continue
        if scope.pending_only and (path.parent / "evaluation" / "score.json").exists():
            continue
        selected_paths.append(path)

    results = tuple(
        evaluate_report_tree(
            path.parent,
            provider,
            transaction_cost_bps=transaction_cost_bps,
        )
        for path in selected_paths
    )
    return EvaluationBatchSummary(
        total=len(results),
        scored=sum(result.status is EvaluationRunStatus.SCORED for result in results),
        not_mature=sum(
            result.status is EvaluationRunStatus.NOT_MATURE for result in results
        ),
        retryable_errors=sum(
            result.status is EvaluationRunStatus.RETRYABLE_PROVIDER_ERROR
            for result in results
        ),
        already_scored=sum(
            result.status is EvaluationRunStatus.ALREADY_SCORED for result in results
        ),
        invalid=sum(
            result.status is EvaluationRunStatus.INVALID_RECORD for result in results
        ),
        results=results,
    )
