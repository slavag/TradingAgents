"""Frozen domain schemas for point-in-time forecast records."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_HASH_PATTERN = r"^sha256:[0-9a-f]{64}$"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ForecastDecisionStatus(str, Enum):
    ACTIONABLE = "Actionable"
    ABSTAIN = "Abstain"
    UNAVAILABLE = "Unavailable"


class AdjustmentBasis(str, Enum):
    RAW = "raw"
    SPLIT_ADJUSTED = "split_adjusted"
    TOTAL_RETURN_ADJUSTED = "total_return_adjusted"
    UNKNOWN = "unknown"


class DataQuality(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    INSUFFICIENT = "insufficient"


class ReferencePriceSnapshot(_FrozenModel):
    value: Decimal = Field(gt=0)
    observed_at: datetime
    adjustment_basis: AdjustmentBasis
    vendor: str = Field(min_length=1)

    @field_validator("observed_at")
    @classmethod
    def _observed_at_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        return value

    @field_validator("value")
    @classmethod
    def _value_is_finite(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("reference price must be finite")
        return value


class DirectionProbabilities(_FrozenModel):
    down: Decimal = Field(ge=0, le=1)
    flat: Decimal = Field(ge=0, le=1)
    up: Decimal = Field(ge=0, le=1)

    @model_validator(mode="after")
    def _sum_to_one(self):
        values = (self.down, self.flat, self.up)
        if not all(value.is_finite() for value in values):
            raise ValueError("direction probabilities must be finite")
        if abs(sum(values) - Decimal("1")) > Decimal("0.000001"):
            raise ValueError("direction probabilities must sum to one")
        return self


class ForecastDistribution(_FrozenModel):
    p10: Decimal = Field(gt=0)
    p50: Decimal = Field(gt=0)
    p90: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def _ordered_quantiles(self):
        values = (self.p10, self.p50, self.p90)
        if not all(value.is_finite() for value in values):
            raise ValueError("forecast distribution must be finite")
        if not self.p10 <= self.p50 <= self.p90:
            raise ValueError("forecast distribution must satisfy p10 <= p50 <= p90")
        return self


class ModelIdentity(_FrozenModel):
    role: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    model: str = Field(min_length=1)


class ForecastProvenance(_FrozenModel):
    models: tuple[ModelIdentity, ...]
    prompt_hash: str | None = Field(default=None, pattern=_HASH_PATTERN)
    config_hash: str = Field(pattern=_HASH_PATTERN)
    source_snapshot_hash: str = Field(pattern=_HASH_PATTERN)


class ForecastRecordPayload(_FrozenModel):
    schema_version: Literal[1] = 1
    canonical_symbol: str = Field(min_length=1)
    quote_currency: str | None = Field(default=None, min_length=3, max_length=12)
    asset_type: str = Field(min_length=1)
    as_of: datetime
    data_cutoff: date
    reference_price: ReferencePriceSnapshot | None = None
    horizon_text: str | None = None
    horizon_sessions: int | None = Field(default=None, gt=0)
    expected_return: Decimal | None = None
    expected_excess_return: Decimal | None = None
    distribution: ForecastDistribution | None = None
    direction_probabilities: DirectionProbabilities | None = None
    central_target: Decimal | None = Field(default=None, gt=0)
    target_low: Decimal | None = Field(default=None, gt=0)
    target_high: Decimal | None = Field(default=None, gt=0)
    invalidation_conditions: tuple[str, ...] = ()
    evidence_ids: tuple[str, ...] = ()
    missing_fields: tuple[str, ...] = ()
    data_quality: DataQuality
    data_quality_notes: tuple[str, ...] = ()
    provenance: ForecastProvenance
    decision_status: ForecastDecisionStatus
    rating: str | None = None
    status_reason: str | None = None
    target_validation_status: str = Field(min_length=1)
    target_validation_reason: str | None = None
    recommendation_confidence_score: int | None = Field(default=None, ge=0, le=100)
    target_confidence_score: int | None = Field(default=None, ge=0, le=100)

    @field_validator("canonical_symbol")
    @classmethod
    def _canonicalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("quote_currency")
    @classmethod
    def _canonicalize_currency(cls, value: str | None) -> str | None:
        return value.strip().upper() if value is not None else None

    @field_validator("as_of")
    @classmethod
    def _as_of_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        return value

    @field_validator(
        "expected_return",
        "expected_excess_return",
        "central_target",
        "target_low",
        "target_high",
    )
    @classmethod
    def _optional_decimal_is_finite(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and not value.is_finite():
            raise ValueError("forecast numeric fields must be finite")
        return value

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids_are_hashes(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        import re

        if any(re.fullmatch(_HASH_PATTERN, value) is None for value in values):
            raise ValueError("evidence IDs must be SHA-256 hashes")
        return values

    @model_validator(mode="after")
    def _validate_record_contract(self):
        if self.data_cutoff > self.as_of.date():
            raise ValueError("data cutoff cannot postdate as_of")
        if (
            self.reference_price is not None
            and self.reference_price.observed_at > self.as_of
        ):
            raise ValueError("reference price cannot postdate as_of")

        has_low = self.target_low is not None
        has_high = self.target_high is not None
        if has_low != has_high:
            raise ValueError("target range must be complete or absent")
        if has_low and self.target_low > self.target_high:
            raise ValueError("target range must satisfy low <= high")

        if self.decision_status is ForecastDecisionStatus.ACTIONABLE:
            if not self.rating:
                raise ValueError("actionable forecast requires a rating")
        elif self.rating is not None:
            raise ValueError("non-actionable forecast cannot carry a rating")

        if self.target_validation_status == "Accepted":
            if self.central_target is None:
                raise ValueError("accepted target validation requires a central target")
        elif self.central_target is not None:
            raise ValueError("only accepted target validation may carry a central target")

        if (
            self.target_validation_status == "Rejected"
            and not self.target_validation_reason
        ):
            raise ValueError("rejected target validation requires a reason")

        if self.decision_status is not ForecastDecisionStatus.ACTIONABLE:
            forecast_values = (
                self.expected_return,
                self.expected_excess_return,
                self.distribution,
                self.direction_probabilities,
                self.central_target,
                self.target_low,
                self.target_high,
                self.recommendation_confidence_score,
                self.target_confidence_score,
            )
            if any(value is not None for value in forecast_values):
                raise ValueError("non-actionable forecast cannot carry forecast metrics")
        return self


class ForecastRecord(ForecastRecordPayload):
    record_id: str = Field(pattern=_HASH_PATTERN)
