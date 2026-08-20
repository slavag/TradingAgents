"""Deterministic shrinkage covariance and portfolio-risk calculations."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, model_validator


class RiskModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    symbols: tuple[str, ...]
    covariance: tuple[tuple[Decimal, ...], ...]
    observations: int = Field(ge=2)
    shrinkage: Decimal = Field(ge=0, le=1)
    minimum_eigenvalue: Decimal

    @model_validator(mode="after")
    def _matrix_shape(self):
        size = len(self.symbols)
        if size == 0 or len(self.covariance) != size:
            raise ValueError("covariance shape must match symbols")
        if any(len(row) != size for row in self.covariance):
            raise ValueError("covariance matrix must be square")
        if len(set(self.symbols)) != size:
            raise ValueError("risk model symbols must be unique")
        return self


def estimate_shrinkage_covariance(
    symbols: tuple[str, ...],
    returns: tuple[tuple[Decimal, ...], ...],
    *,
    shrinkage: Decimal = Decimal("0.25"),
) -> RiskModel:
    """Estimate sample covariance shrunk toward its diagonal."""
    if not Decimal("0") <= shrinkage <= Decimal("1"):
        raise ValueError("shrinkage must be between zero and one")
    if len(symbols) == 0 or len(set(symbols)) != len(symbols):
        raise ValueError("symbols must be nonempty and unique")
    if len(returns) < 2:
        raise ValueError("at least two return observations are required")
    if any(len(row) != len(symbols) for row in returns):
        raise ValueError("return rows must match symbol count")
    if any(not value.is_finite() for row in returns for value in row):
        raise ValueError("returns must be finite")

    canonical = tuple(symbol.strip().upper() for symbol in symbols)
    order = tuple(sorted(range(len(canonical)), key=lambda index: canonical[index]))
    ordered_symbols = tuple(canonical[index] for index in order)
    ordered_returns = tuple(tuple(row[index] for index in order) for row in returns)
    count = Decimal(len(ordered_returns))
    means = tuple(
        sum((row[index] for row in ordered_returns), Decimal("0")) / count
        for index in range(len(ordered_symbols))
    )
    sample = []
    denominator = Decimal(len(ordered_returns) - 1)
    for left in range(len(ordered_symbols)):
        row_values = []
        for right in range(len(ordered_symbols)):
            covariance = sum(
                (
                    (row[left] - means[left]) * (row[right] - means[right])
                    for row in ordered_returns
                ),
                Decimal("0"),
            ) / denominator
            if left != right:
                covariance *= Decimal("1") - shrinkage
            row_values.append(covariance)
        sample.append(tuple(row_values))
    covariance_matrix = tuple(sample)
    eigenvalues = np.linalg.eigvalsh(
        np.array([[float(value) for value in row] for row in covariance_matrix])
    )
    minimum_eigenvalue = Decimal(str(float(eigenvalues.min())))
    return RiskModel(
        symbols=ordered_symbols,
        covariance=covariance_matrix,
        observations=len(ordered_returns),
        shrinkage=shrinkage,
        minimum_eigenvalue=minimum_eigenvalue,
    )


def portfolio_volatility(weights: Mapping[str, Decimal], risk_model: RiskModel) -> Decimal:
    """Return square-root variance for weights aligned to a risk model."""
    vector = tuple(weights.get(symbol, Decimal("0")) for symbol in risk_model.symbols)
    variance = sum(
        (
            vector[left] * risk_model.covariance[left][right] * vector[right]
            for left in range(len(vector))
            for right in range(len(vector))
        ),
        Decimal("0"),
    )
    if variance < Decimal("-1e-18"):
        raise ValueError("portfolio variance cannot be negative")
    return max(variance, Decimal("0")).sqrt()
