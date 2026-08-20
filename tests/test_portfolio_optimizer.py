"""Deterministic constrained portfolio state, risk, and allocation."""

import json
from decimal import Decimal

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from cli.main import app as cli_app
from tradingagents.portfolio.optimizer import optimize_portfolio, validate_target_weights
from tradingagents.portfolio.risk_model import (
    RiskModel,
    estimate_shrinkage_covariance,
    portfolio_volatility,
)
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
from tradingagents.web.service import optimize_portfolio_payload


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


def test_risk_model_sorts_symbols_and_reorders_covariance_columns():
    model = estimate_shrinkage_covariance(
        ("BBB", "AAA"),
        (
            (Decimal("0.02"), Decimal("0.01")),
            (Decimal("-0.02"), Decimal("-0.01")),
        ),
        shrinkage=Decimal("0.5"),
    )

    assert model.symbols == ("AAA", "BBB")
    assert model.covariance == (
        (Decimal("0.0002"), Decimal("0.0002")),
        (Decimal("0.0002"), Decimal("0.0008")),
    )


def test_risk_model_rejects_missing_or_nonfinite_returns():
    with pytest.raises(ValueError, match="finite"):
        estimate_shrinkage_covariance(
            ("AAA",),
            ((Decimal("NaN"),), (Decimal("0.01"),)),
        )
    with pytest.raises(ValueError, match="at least two"):
        estimate_shrinkage_covariance(("AAA",), ((Decimal("0.01"),),))


def test_shrinkage_covariance_is_positive_semidefinite():
    model = estimate_shrinkage_covariance(
        ("AAA", "BBB"),
        (
            (Decimal("0.01"), Decimal("-0.01")),
            (Decimal("-0.02"), Decimal("0.02")),
            (Decimal("0.015"), Decimal("-0.015")),
        ),
        shrinkage=Decimal("0.25"),
    )

    assert model.minimum_eigenvalue >= Decimal("-1e-12")


def test_portfolio_volatility_matches_hand_calculation():
    model = estimate_shrinkage_covariance(
        ("AAA", "BBB"),
        (
            (Decimal("0.01"), Decimal("0.02")),
            (Decimal("-0.01"), Decimal("-0.02")),
        ),
        shrinkage=Decimal("0.5"),
    )

    volatility = portfolio_volatility(
        {"AAA": Decimal("0.5"), "BBB": Decimal("0.5")},
        model,
    )

    assert abs(volatility - Decimal("0.00035").sqrt()) < Decimal("1e-20")


def optimizer_state():
    return PortfolioState(cash=Decimal("1000"), holdings=())


def optimizer_constraints(**updates):
    values = {
        "maximum_gross_weight": Decimal("0.8"),
        "minimum_cash_weight": Decimal("0.2"),
        "maximum_turnover": Decimal("1"),
        "maximum_volatility": Decimal("0.5"),
        "uncertainty_penalty": Decimal("1"),
        "fee_bps": Decimal("2"),
        "spread_bps": Decimal("5"),
        "sector_limits": (GroupLimit(name="Tech", maximum_weight=Decimal("0.8")),),
        "country_limits": (GroupLimit(name="US", maximum_weight=Decimal("0.8")),),
    }
    values.update(updates)
    return PortfolioConstraints(**values)


def instrument_constraint(symbol="AAA", **updates):
    values = {
        "symbol": symbol,
        "maximum_weight": Decimal("0.5"),
        "average_daily_value": Decimal("1000000"),
        "maximum_participation": Decimal("0.1"),
        "sector": "Tech",
        "country": "US",
    }
    values.update(updates)
    return InstrumentConstraint(**values)


def instrument_forecast(symbol="AAA", **updates):
    values = {
        "symbol": symbol,
        "expected_return": Decimal("0.10"),
        "uncertainty": Decimal("0.02"),
        "reference_price": Decimal("100"),
        "forecast_record_id": "sha256:" + ("a" if symbol == "AAA" else "b") * 64,
    }
    values.update(updates)
    return InstrumentForecast(**values)


def optimizer_risk(symbols=("AAA",), variance="0.01"):
    covariance = (
        tuple(
            Decimal(variance) if left == right else Decimal("0")
            for right in range(len(symbols))
        )
        for left in range(len(symbols))
    )
    return RiskModel(
        symbols=tuple(sorted(symbols)),
        covariance=tuple(covariance),
        observations=2,
        shrinkage=Decimal("1"),
        minimum_eigenvalue=Decimal(variance),
    )


def test_optimizer_allocates_positive_net_edge_with_costs():
    result = optimize_portfolio(
        optimizer_state(),
        (instrument_forecast(),),
        (instrument_constraint(),),
        optimizer_constraints(),
        optimizer_risk(),
    )

    assert result.status is OptimizationStatus.OPTIMIZED
    assert result.target_weights[0].symbol == "AAA"
    assert result.target_weights[0].target_weight == Decimal("0.5")
    assert result.target_weights[0].net_edge == Decimal("0.0793")
    assert result.cash_weight == Decimal("0.5")


def test_optimizer_returns_no_trade_when_edge_does_not_exceed_cost_and_uncertainty():
    result = optimize_portfolio(
        optimizer_state(),
        (instrument_forecast(expected_return=Decimal("0.01"), uncertainty=Decimal("0.02")),),
        (instrument_constraint(),),
        optimizer_constraints(),
        optimizer_risk(),
    )

    assert result.status is OptimizationStatus.NO_TRADE
    assert result.turnover == Decimal("0")
    assert any(item.code == "no_positive_edge" for item in result.diagnostics)


def test_optimizer_enforces_liquidity_and_turnover_caps():
    result = optimize_portfolio(
        optimizer_state(),
        (instrument_forecast(),),
        (instrument_constraint(average_daily_value=Decimal("1000")),),
        optimizer_constraints(maximum_turnover=Decimal("0.2")),
        optimizer_risk(),
    )

    assert result.target_weights[0].target_weight == Decimal("0.1")
    assert result.turnover == Decimal("0.1")


def test_optimizer_enforces_sector_and_country_caps():
    result = optimize_portfolio(
        optimizer_state(),
        (instrument_forecast("AAA"), instrument_forecast("BBB")),
        (instrument_constraint("AAA"), instrument_constraint("BBB")),
        optimizer_constraints(
            sector_limits=(GroupLimit(name="Tech", maximum_weight=Decimal("0.3")),),
            country_limits=(GroupLimit(name="US", maximum_weight=Decimal("0.4")),),
        ),
        optimizer_risk(("AAA", "BBB")),
    )

    assert sum(item.target_weight for item in result.target_weights) == Decimal("0.3")


def test_optimizer_scales_weights_to_risk_budget():
    result = optimize_portfolio(
        optimizer_state(),
        (instrument_forecast(),),
        (instrument_constraint(maximum_weight=Decimal("0.8")),),
        optimizer_constraints(maximum_volatility=Decimal("0.02")),
        optimizer_risk(),
    )

    assert result.expected_volatility <= Decimal("0.02") + Decimal("1e-18")
    assert result.target_weights[0].target_weight < Decimal("0.8")


def test_optimizer_breaks_equal_edge_ties_by_symbol():
    result = optimize_portfolio(
        optimizer_state(),
        (instrument_forecast("BBB"), instrument_forecast("AAA")),
        (instrument_constraint("BBB"), instrument_constraint("AAA")),
        optimizer_constraints(maximum_gross_weight=Decimal("0.2"), minimum_cash_weight=Decimal("0.8")),
        optimizer_risk(("AAA", "BBB")),
    )

    assert result.target_weights[0].symbol == "AAA"
    assert result.target_weights[0].target_weight == Decimal("0.2")
    assert result.target_weights[1].target_weight == Decimal("0")


def test_independent_validator_detects_tampered_position_limit():
    result = optimize_portfolio(
        optimizer_state(),
        (instrument_forecast(),),
        (instrument_constraint(),),
        optimizer_constraints(),
        optimizer_risk(),
    )
    tampered_target = result.target_weights[0].model_copy(
        update={"target_weight": Decimal("0.9"), "trade_value": Decimal("900")}
    )
    tampered = result.model_copy(
        update={"target_weights": (tampered_target,), "cash_weight": Decimal("0.1")}
    )

    diagnostics = validate_target_weights(
        tampered,
        optimizer_state(),
        (instrument_constraint(),),
        optimizer_constraints(),
        optimizer_risk(),
    )

    assert any(item.code == "position_limit:AAA" and not item.passed for item in diagnostics)


def optimization_payload():
    return {
        "state": optimizer_state().model_dump(mode="json"),
        "forecasts": [instrument_forecast().model_dump(mode="json")],
        "instrument_constraints": [instrument_constraint().model_dump(mode="json")],
        "portfolio_constraints": optimizer_constraints().model_dump(mode="json"),
        "risk_model": optimizer_risk().model_dump(mode="json"),
    }


def test_web_service_optimizer_adapter_returns_serialized_diagnostics():
    result = optimize_portfolio_payload(optimization_payload())

    assert result["status"] == "optimized"
    assert result["target_weights"][0]["symbol"] == "AAA"
    assert any(item["code"] == "risk_budget" for item in result["diagnostics"])


def test_cli_optimizer_reads_json_without_executing_orders(tmp_path):
    request_path = tmp_path / "portfolio.json"
    request_path.write_text(json.dumps(optimization_payload()))

    result = CliRunner().invoke(
        cli_app,
        ["optimize-portfolio", "--input", str(request_path)],
    )

    assert result.exit_code == 0
    assert "Portfolio optimization: optimized" in result.stdout
    assert "AAA" in result.stdout
