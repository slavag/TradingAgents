"""Resolve forecast records against caller-supplied trading sessions."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from tradingagents.forecasting.schemas import AdjustmentBasis, ForecastRecord


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class PriceObservation(_FrozenModel):
    session: date
    close: Decimal = Field(gt=0)

    @field_validator("close")
    @classmethod
    def _close_is_finite(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("close must be finite")
        return value


class OutcomeResolutionStatus(str, Enum):
    RESOLVED = "resolved"
    MISSING_REFERENCE_PRICE = "missing_reference_price"
    MISSING_HORIZON = "missing_horizon"
    PRICE_BASIS_MISMATCH = "price_basis_mismatch"
    INSUFFICIENT_SESSIONS = "insufficient_sessions"


class ResolvedOutcome(_FrozenModel):
    record_id: str
    status: OutcomeResolutionStatus
    reason: str | None
    data_cutoff: date
    resolved_date: date | None
    horizon_sessions: int | None
    reference_price: Decimal | None
    end_price: Decimal | None
    realized_return: Decimal | None
    benchmark_return: Decimal | None
    excess_return: Decimal | None
    evaluated_path: tuple[PriceObservation, ...]
    adjustment_basis: AdjustmentBasis
    missing_fields: tuple[str, ...]


def _validate_observations(
    observations: tuple[PriceObservation, ...],
    *,
    label: str,
) -> None:
    sessions = [item.session for item in observations]
    if any(
        current <= previous
        for previous, current in zip(sessions, sessions[1:], strict=False)
    ):
        raise ValueError(f"{label} observations must be strictly increasing and unique")


def _unresolved(
    record: ForecastRecord,
    status: OutcomeResolutionStatus,
    adjustment_basis: AdjustmentBasis,
) -> ResolvedOutcome:
    reference = record.reference_price.value if record.reference_price else None
    return ResolvedOutcome(
        record_id=record.record_id,
        status=status,
        reason=status.value,
        data_cutoff=record.data_cutoff,
        resolved_date=None,
        horizon_sessions=record.horizon_sessions,
        reference_price=reference,
        end_price=None,
        realized_return=None,
        benchmark_return=None,
        excess_return=None,
        evaluated_path=(),
        adjustment_basis=adjustment_basis,
        missing_fields=(
            "resolved_date",
            "end_price",
            "realized_return",
            "benchmark_return",
            "excess_return",
        ),
    )


def resolve_forecast_outcome(
    record: ForecastRecord,
    observations: tuple[PriceObservation, ...],
    *,
    adjustment_basis: AdjustmentBasis,
    benchmark_reference_price: Decimal | None = None,
    benchmark_observations: tuple[PriceObservation, ...] = (),
) -> ResolvedOutcome:
    """Resolve a record at its exact future trading-session horizon."""
    _validate_observations(observations, label="instrument")
    if benchmark_observations:
        _validate_observations(benchmark_observations, label="benchmark")

    if record.reference_price is None:
        return _unresolved(
            record,
            OutcomeResolutionStatus.MISSING_REFERENCE_PRICE,
            adjustment_basis,
        )
    if record.horizon_sessions is None:
        return _unresolved(
            record,
            OutcomeResolutionStatus.MISSING_HORIZON,
            adjustment_basis,
        )
    if record.reference_price.adjustment_basis is not adjustment_basis:
        return _unresolved(
            record,
            OutcomeResolutionStatus.PRICE_BASIS_MISMATCH,
            adjustment_basis,
        )

    future = tuple(item for item in observations if item.session > record.data_cutoff)
    if len(future) < record.horizon_sessions:
        return _unresolved(
            record,
            OutcomeResolutionStatus.INSUFFICIENT_SESSIONS,
            adjustment_basis,
        )

    evaluated_path = future[: record.horizon_sessions]
    endpoint = evaluated_path[-1]
    reference_price = record.reference_price.value
    realized_return = endpoint.close / reference_price - Decimal("1")

    benchmark_return = None
    excess_return = None
    missing_fields = []
    if benchmark_reference_price is not None:
        if not benchmark_reference_price.is_finite() or benchmark_reference_price <= 0:
            raise ValueError("benchmark reference price must be positive and finite")
        benchmark_endpoint = next(
            (
                item
                for item in benchmark_observations
                if item.session == endpoint.session
            ),
            None,
        )
        if benchmark_endpoint is not None:
            benchmark_return = (
                benchmark_endpoint.close / benchmark_reference_price - Decimal("1")
            )
            excess_return = realized_return - benchmark_return
        else:
            missing_fields.extend(("benchmark_return", "excess_return"))
    else:
        missing_fields.extend(("benchmark_return", "excess_return"))

    return ResolvedOutcome(
        record_id=record.record_id,
        status=OutcomeResolutionStatus.RESOLVED,
        reason=None,
        data_cutoff=record.data_cutoff,
        resolved_date=endpoint.session,
        horizon_sessions=record.horizon_sessions,
        reference_price=reference_price,
        end_price=endpoint.close,
        realized_return=realized_return,
        benchmark_return=benchmark_return,
        excess_return=excess_return,
        evaluated_path=evaluated_path,
        adjustment_basis=adjustment_basis,
        missing_fields=tuple(missing_fields),
    )
