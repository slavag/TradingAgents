"""Deterministic constrained portfolio state, risk, and allocation."""

from decimal import Decimal

import pytest
from pydantic import ValidationError

from tradingagents.portfolio.state import (
    ConstraintDiagnostic,
    GroupLimit,
    Holding,
    InstrumentConstraint,
    InstrumentForecast,
    OptimizationStatus,
    PortfolioConstraints,
    PortfolioOptimizationResult,
    PortfolioState,
    TargetWeight,
)


def test_portfolio_state_computes_total_value_and_current_weights():
    state = PortfolioState(
        cash=Decimal("1000"),
        holdings=(
            Holding(symbol="AAA", quantity=Decimal("10"), price=Decimal("100"), sector="Tech", country="US"),
            Holding(symbol="BBB", quantity=Decimal("20"), price=Decimal("50"), sector="Energy", country="US"),
        ),
    )

    assert state.total_value == Decimal("3000")
    assert state.current_weight("AAA") == Decimal("1") / Decimal("3")
    assert state.cash_weight == Decimal("1") / Decimal("3")


def test_portfolio_state_rejects_duplicate_symbols():
    holding = Holding(symbol="AAA", quantity=1, price=100, sector="Tech", country="US")
    with pytest.raises(ValidationError, match="holding symbols must be unique"):
        PortfolioState(cash=0, holdings=(holding, holding))


def test_portfolio_state_rejects_negative_cash_or_quantity():
    with pytest.raises(ValidationError):
        PortfolioState(cash=-1, holdings=())
    with pytest.raises(ValidationError):
        Holding(symbol="AAA", quantity=-1, price=100, sector="Tech", country="US")


def test_constraints_reject_incoherent_limits():
    with pytest.raises(ValidationError, match="cash and gross exposure limits conflict"):
        PortfolioConstraints(
            maximum_gross_weight=Decimal("0.95"),
            minimum_cash_weight=Decimal("0.10"),
            maximum_turnover=Decimal("0.20"),
            maximum_volatility=Decimal("0.25"),
            uncertainty_penalty=Decimal("1"),
            fee_bps=Decimal("2"),
            spread_bps=Decimal("5"),
            sector_limits=(),
            country_limits=(),
        )


def test_instrument_constraints_require_matching_positive_capacity():
    with pytest.raises(ValidationError):
        InstrumentConstraint(
            symbol="AAA",
            maximum_weight=Decimal("1.2"),
            average_daily_value=Decimal("1000000"),
            maximum_participation=Decimal("0.1"),
            sector="Tech",
            country="US",
        )


def test_optimization_result_is_frozen():
    result = PortfolioOptimizationResult(
        status=OptimizationStatus.NO_TRADE,
        target_weights=(
            TargetWeight(
                symbol="AAA",
                current_weight=Decimal("0"),
                target_weight=Decimal("0"),
                trade_value=Decimal("0"),
                net_edge=Decimal("0"),
                risk_contribution=None,
            ),
        ),
        cash_weight=Decimal("1"),
        turnover=Decimal("0"),
        expected_net_edge=Decimal("0"),
        expected_volatility=Decimal("0"),
        diagnostics=(
            ConstraintDiagnostic(code="no_positive_edge", passed=True, detail="No trade."),
        ),
    )

    with pytest.raises(ValidationError, match="frozen"):
        result.status = OptimizationStatus.OPTIMIZED


def test_forecast_and_group_limit_normalize_symbols_and_names():
    forecast = InstrumentForecast(
        symbol=" aaa ",
        expected_return=Decimal("0.12"),
        uncertainty=Decimal("0.03"),
        reference_price=Decimal("100"),
        forecast_record_id="sha256:" + "a" * 64,
    )
    limit = GroupLimit(name=" tech ", maximum_weight=Decimal("0.4"))

    assert forecast.symbol == "AAA"
    assert limit.name == "Tech"
