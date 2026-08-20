"""Frozen domain contracts for deterministic portfolio optimization."""

from __future__ import annotations

from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Holding(_FrozenModel):
    symbol: str = Field(min_length=1)
    quantity: Decimal = Field(ge=0)
    price: Decimal = Field(gt=0)
    sector: str = Field(min_length=1)
    country: str = Field(min_length=1)

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return value.strip().upper()

    @property
    def market_value(self) -> Decimal:
        return self.quantity * self.price


class PortfolioState(_FrozenModel):
    cash: Decimal = Field(ge=0)
    holdings: tuple[Holding, ...]

    @model_validator(mode="after")
    def _unique_symbols_and_value(self):
        symbols = [holding.symbol for holding in self.holdings]
        if len(symbols) != len(set(symbols)):
            raise ValueError("holding symbols must be unique")
        if self.total_value <= 0:
            raise ValueError("portfolio total value must be positive")
        return self

    @property
    def total_value(self) -> Decimal:
        return self.cash + sum(
            (holding.market_value for holding in self.holdings),
            Decimal("0"),
        )

    @property
    def cash_weight(self) -> Decimal:
        return self.cash / self.total_value

    def current_weight(self, symbol: str) -> Decimal:
        canonical = symbol.strip().upper()
        value = sum(
            (
                holding.market_value
                for holding in self.holdings
                if holding.symbol == canonical
            ),
            Decimal("0"),
        )
        return value / self.total_value


class InstrumentForecast(_FrozenModel):
    symbol: str = Field(min_length=1)
    expected_return: Decimal
    uncertainty: Decimal = Field(ge=0)
    reference_price: Decimal = Field(gt=0)
    forecast_record_id: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return value.strip().upper()


class InstrumentConstraint(_FrozenModel):
    symbol: str = Field(min_length=1)
    maximum_weight: Decimal = Field(ge=0, le=1)
    average_daily_value: Decimal = Field(gt=0)
    maximum_participation: Decimal = Field(gt=0, le=1)
    sector: str = Field(min_length=1)
    country: str = Field(min_length=1)

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return value.strip().upper()


class GroupLimit(_FrozenModel):
    name: str = Field(min_length=1)
    maximum_weight: Decimal = Field(ge=0, le=1)

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        return value.strip().title()


class PortfolioConstraints(_FrozenModel):
    maximum_gross_weight: Decimal = Field(gt=0, le=1)
    minimum_cash_weight: Decimal = Field(ge=0, le=1)
    maximum_turnover: Decimal = Field(ge=0, le=2)
    maximum_volatility: Decimal = Field(gt=0)
    uncertainty_penalty: Decimal = Field(ge=0)
    fee_bps: Decimal = Field(ge=0)
    spread_bps: Decimal = Field(ge=0)
    sector_limits: tuple[GroupLimit, ...]
    country_limits: tuple[GroupLimit, ...]

    @model_validator(mode="after")
    def _coherent_limits(self):
        if self.maximum_gross_weight + self.minimum_cash_weight > Decimal("1"):
            raise ValueError("cash and gross exposure limits conflict")
        for label, limits in (
            ("sector", self.sector_limits),
            ("country", self.country_limits),
        ):
            names = [limit.name for limit in limits]
            if len(names) != len(set(names)):
                raise ValueError(f"{label} limits must be unique")
        return self


class OptimizationStatus(str, Enum):
    OPTIMIZED = "optimized"
    NO_TRADE = "no_trade"


class ConstraintDiagnostic(_FrozenModel):
    code: str = Field(min_length=1)
    passed: bool
    detail: str = Field(min_length=1)


class TargetWeight(_FrozenModel):
    symbol: str = Field(min_length=1)
    current_weight: Decimal = Field(ge=0, le=1)
    target_weight: Decimal = Field(ge=0, le=1)
    trade_value: Decimal
    net_edge: Decimal
    risk_contribution: Decimal | None

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return value.strip().upper()


class PortfolioOptimizationResult(_FrozenModel):
    status: OptimizationStatus
    target_weights: tuple[TargetWeight, ...]
    cash_weight: Decimal = Field(ge=0, le=1)
    turnover: Decimal = Field(ge=0, le=2)
    expected_net_edge: Decimal
    expected_volatility: Decimal = Field(ge=0)
    diagnostics: tuple[ConstraintDiagnostic, ...]

    @model_validator(mode="after")
    def _unique_symbols_and_total_weight(self):
        symbols = [target.symbol for target in self.target_weights]
        if len(symbols) != len(set(symbols)):
            raise ValueError("target symbols must be unique")
        total = self.cash_weight + sum(
            (target.target_weight for target in self.target_weights),
            Decimal("0"),
        )
        if abs(total - Decimal("1")) > Decimal("0.000001"):
            raise ValueError("target and cash weights must sum to one")
        return self
