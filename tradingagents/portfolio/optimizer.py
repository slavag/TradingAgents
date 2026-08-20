"""Deterministic long-only allocation under hard portfolio constraints."""

from __future__ import annotations

from decimal import Decimal

from tradingagents.portfolio.risk_model import RiskModel, portfolio_volatility
from tradingagents.portfolio.state import (
    ConstraintDiagnostic,
    InstrumentConstraint,
    InstrumentForecast,
    OptimizationStatus,
    PortfolioConstraints,
    PortfolioOptimizationResult,
    PortfolioState,
    TargetWeight,
)

_TOLERANCE = Decimal("1e-12")


def _unique_by_symbol(values, label: str):
    result = {value.symbol: value for value in values}
    if len(result) != len(values):
        raise ValueError(f"{label} symbols must be unique")
    return result


def _group_weights(weights, constraint_by_symbol, attribute: str):
    groups: dict[str, Decimal] = {}
    for symbol, weight in weights.items():
        constraint = constraint_by_symbol[symbol]
        name = getattr(constraint, attribute).strip().title()
        groups[name] = groups.get(name, Decimal("0")) + weight
    return groups


def _risk_contributions(weights: dict[str, Decimal], risk_model: RiskModel):
    volatility = portfolio_volatility(weights, risk_model)
    if volatility <= _TOLERANCE:
        return dict.fromkeys(weights)
    vector = tuple(weights.get(symbol, Decimal("0")) for symbol in risk_model.symbols)
    contributions = {}
    for index, symbol in enumerate(risk_model.symbols):
        marginal = sum(
            (
                risk_model.covariance[index][right] * vector[right]
                for right in range(len(vector))
            ),
            Decimal("0"),
        )
        contributions[symbol] = vector[index] * marginal / volatility
    return contributions


def validate_target_weights(
    result: PortfolioOptimizationResult,
    state: PortfolioState,
    instrument_constraints: tuple[InstrumentConstraint, ...],
    portfolio_constraints: PortfolioConstraints,
    risk_model: RiskModel,
) -> tuple[ConstraintDiagnostic, ...]:
    """Independently validate every hard constraint on target weights."""
    constraints = _unique_by_symbol(instrument_constraints, "constraint")
    targets = {target.symbol: target for target in result.target_weights}
    weights = {symbol: target.target_weight for symbol, target in targets.items()}
    diagnostics = []
    for symbol, constraint in sorted(constraints.items()):
        target = targets.get(symbol)
        weight = target.target_weight if target else Decimal("0")
        diagnostics.append(
            ConstraintDiagnostic(
                code=f"position_limit:{symbol}",
                passed=weight <= constraint.maximum_weight + _TOLERANCE,
                detail=f"{weight} <= {constraint.maximum_weight}",
            )
        )
        trade_value = abs(target.trade_value) if target else Decimal("0")
        capacity = constraint.average_daily_value * constraint.maximum_participation
        diagnostics.append(
            ConstraintDiagnostic(
                code=f"liquidity_limit:{symbol}",
                passed=trade_value <= capacity + _TOLERANCE,
                detail=f"{trade_value} <= {capacity}",
            )
        )

    gross = sum(weights.values(), Decimal("0"))
    diagnostics.extend([
        ConstraintDiagnostic(
            code="gross_exposure",
            passed=gross <= portfolio_constraints.maximum_gross_weight + _TOLERANCE,
            detail=f"{gross} <= {portfolio_constraints.maximum_gross_weight}",
        ),
        ConstraintDiagnostic(
            code="minimum_cash",
            passed=result.cash_weight + _TOLERANCE >= portfolio_constraints.minimum_cash_weight,
            detail=f"{result.cash_weight} >= {portfolio_constraints.minimum_cash_weight}",
        ),
        ConstraintDiagnostic(
            code="turnover",
            passed=result.turnover <= portfolio_constraints.maximum_turnover + _TOLERANCE,
            detail=f"{result.turnover} <= {portfolio_constraints.maximum_turnover}",
        ),
    ])
    for attribute, limits, prefix in (
        ("sector", portfolio_constraints.sector_limits, "sector_limit"),
        ("country", portfolio_constraints.country_limits, "country_limit"),
    ):
        group_weights = _group_weights(weights, constraints, attribute)
        for limit in limits:
            weight = group_weights.get(limit.name, Decimal("0"))
            diagnostics.append(
                ConstraintDiagnostic(
                    code=f"{prefix}:{limit.name}",
                    passed=weight <= limit.maximum_weight + _TOLERANCE,
                    detail=f"{weight} <= {limit.maximum_weight}",
                )
            )
    missing_risk = sorted(symbol for symbol, weight in weights.items() if weight > 0 and symbol not in risk_model.symbols)
    diagnostics.append(
        ConstraintDiagnostic(
            code="risk_model_coverage",
            passed=not missing_risk,
            detail="covered" if not missing_risk else f"missing: {', '.join(missing_risk)}",
        )
    )
    volatility = portfolio_volatility(weights, risk_model) if not missing_risk else Decimal("0")
    diagnostics.append(
        ConstraintDiagnostic(
            code="risk_budget",
            passed=not missing_risk and volatility <= portfolio_constraints.maximum_volatility + _TOLERANCE,
            detail=f"{volatility} <= {portfolio_constraints.maximum_volatility}",
        )
    )
    return tuple(diagnostics)


def optimize_portfolio(
    state: PortfolioState,
    forecasts: tuple[InstrumentForecast, ...],
    instrument_constraints: tuple[InstrumentConstraint, ...],
    portfolio_constraints: PortfolioConstraints,
    risk_model: RiskModel,
) -> PortfolioOptimizationResult:
    """Allocate positive net edge in stable order while enforcing hard limits."""
    forecast_by_symbol = _unique_by_symbol(forecasts, "forecast")
    constraint_by_symbol = _unique_by_symbol(instrument_constraints, "constraint")
    holding_symbols = {holding.symbol for holding in state.holdings}
    universe = sorted(holding_symbols | set(forecast_by_symbol))
    missing_constraints = sorted(set(universe) - set(constraint_by_symbol))
    if missing_constraints:
        raise ValueError(f"missing instrument constraints: {', '.join(missing_constraints)}")

    weights = {symbol: state.current_weight(symbol) for symbol in universe}
    current_weights = dict(weights)
    cost = (portfolio_constraints.fee_bps + portfolio_constraints.spread_bps) / Decimal("10000")
    net_edges = {
        symbol: (
            forecast.expected_return
            - portfolio_constraints.uncertainty_penalty * forecast.uncertainty
            - cost
        )
        for symbol, forecast in forecast_by_symbol.items()
    }
    sector_limits = {limit.name: limit.maximum_weight for limit in portfolio_constraints.sector_limits}
    country_limits = {limit.name: limit.maximum_weight for limit in portfolio_constraints.country_limits}
    turnover = Decimal("0")

    for symbol, edge in sorted(net_edges.items(), key=lambda item: (-item[1], item[0])):
        if edge <= 0:
            continue
        constraint = constraint_by_symbol[symbol]
        gross = sum(weights.values(), Decimal("0"))
        cash_weight = Decimal("1") - gross
        sector = constraint.sector.strip().title()
        country = constraint.country.strip().title()
        sector_weight = _group_weights(weights, constraint_by_symbol, "sector").get(sector, Decimal("0"))
        country_weight = _group_weights(weights, constraint_by_symbol, "country").get(country, Decimal("0"))
        liquidity_weight = (
            constraint.average_daily_value
            * constraint.maximum_participation
            / state.total_value
        )
        capacity = min(
            constraint.maximum_weight - weights.get(symbol, Decimal("0")),
            portfolio_constraints.maximum_gross_weight - gross,
            cash_weight - portfolio_constraints.minimum_cash_weight,
            portfolio_constraints.maximum_turnover - turnover,
            liquidity_weight,
            sector_limits.get(sector, portfolio_constraints.maximum_gross_weight) - sector_weight,
            country_limits.get(country, portfolio_constraints.maximum_gross_weight) - country_weight,
        )
        increment = max(capacity, Decimal("0"))
        weights[symbol] = weights.get(symbol, Decimal("0")) + increment
        turnover += increment

    volatility = portfolio_volatility(weights, risk_model)
    if volatility > portfolio_constraints.maximum_volatility:
        factor = portfolio_constraints.maximum_volatility / volatility
        weights = {symbol: weight * factor for symbol, weight in weights.items()}
        turnover = sum(
            (abs(weights[symbol] - current_weights.get(symbol, Decimal("0"))) for symbol in universe),
            Decimal("0"),
        )
        volatility = portfolio_volatility(weights, risk_model)

    contributions = _risk_contributions(weights, risk_model)
    targets = tuple(
        TargetWeight(
            symbol=symbol,
            current_weight=current_weights.get(symbol, Decimal("0")),
            target_weight=weights.get(symbol, Decimal("0")),
            trade_value=(weights.get(symbol, Decimal("0")) - current_weights.get(symbol, Decimal("0"))) * state.total_value,
            net_edge=net_edges.get(symbol, Decimal("0")),
            risk_contribution=contributions.get(symbol),
        )
        for symbol in universe
    )
    gross = sum(weights.values(), Decimal("0"))
    expected_net_edge = sum(
        (
            max(weights.get(symbol, Decimal("0")) - current_weights.get(symbol, Decimal("0")), Decimal("0"))
            * net_edges.get(symbol, Decimal("0"))
            for symbol in universe
        ),
        Decimal("0"),
    )
    status = OptimizationStatus.OPTIMIZED if turnover > _TOLERANCE else OptimizationStatus.NO_TRADE
    provisional = PortfolioOptimizationResult(
        status=status,
        target_weights=targets,
        cash_weight=Decimal("1") - gross,
        turnover=turnover,
        expected_net_edge=expected_net_edge,
        expected_volatility=volatility,
        diagnostics=(),
    )
    diagnostics = list(
        validate_target_weights(
            provisional,
            state,
            instrument_constraints,
            portfolio_constraints,
            risk_model,
        )
    )
    if status is OptimizationStatus.NO_TRADE:
        diagnostics.append(
            ConstraintDiagnostic(
                code="no_positive_edge",
                passed=True,
                detail="No feasible positive net edge exceeded costs and uncertainty.",
            )
        )
    if any(not diagnostic.passed for diagnostic in diagnostics):
        failures = ", ".join(item.code for item in diagnostics if not item.passed)
        raise ValueError(f"optimizer produced infeasible target weights: {failures}")
    return provisional.model_copy(update={"diagnostics": tuple(diagnostics)})
