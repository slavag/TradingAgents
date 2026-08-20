# Constrained Portfolio Optimizer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert eligible forecast records and current holdings into deterministic long-only target weights that satisfy hard portfolio, liquidity, turnover, cost, and risk limits or return an explicit no-trade decision.

**Architecture:** Add frozen portfolio-state and constraint models, then a shrinkage covariance estimator and a deterministic constrained allocator. The allocator starts from current weights, computes net edge after costs and uncertainty, allocates in stable edge order while checking every hard limit, and scales active risk when needed. A final validator rejects any infeasible result rather than allowing prose or numerical drift to override constraints.

**Tech Stack:** Python 3.10+, Pydantic v2, `Decimal`, NumPy, pytest.

**Spec:** `docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md`

## Global Constraints

- Long-only target weights are between zero and each instrument's maximum.
- Cash, holdings, prices, and all weights are explicit inputs.
- Forecasts without expected return, reference price, or eligible actionable status cannot create new exposure.
- Net edge subtracts spreads, fees, and an uncertainty penalty before allocation.
- Position, sector, country, liquidity, turnover, gross-exposure, cash, and risk limits are hard constraints.
- Final constraints are revalidated independently; infeasible output becomes an error, never a best-effort portfolio.
- When no feasible net edge exceeds zero, return `no_trade` with diagnostics.
- No order execution or broker integration is included.

### Task 1: Portfolio State and Constraint Contract

**Files:**
- Create: `tradingagents/portfolio/__init__.py`
- Create: `tradingagents/portfolio/state.py`
- Create: `tests/test_portfolio_optimizer.py`

**Interfaces:** `Holding`, `PortfolioState`, `InstrumentForecast`, `InstrumentConstraint`, `PortfolioConstraints`, `OptimizationStatus`, `ConstraintDiagnostic`, and `PortfolioOptimizationResult`.

- [ ] Write failing schema tests for valuation, duplicate symbols, negative cash/weights, invalid limits, and immutable output.
- [ ] Implement frozen Decimal-based state and result models.
- [ ] Run focused tests and Ruff.
- [ ] Commit as `feat: define constrained portfolio state`.

### Task 2: Shrinkage Risk Model

**Files:**
- Create: `tradingagents/portfolio/risk_model.py`
- Modify: `tests/test_portfolio_optimizer.py`

**Interfaces:** `RiskModel`, `estimate_shrinkage_covariance(symbols, returns, shrinkage=Decimal("0.25"))`, and `portfolio_volatility(weights, risk_model)`.

- [ ] Write failing tests for deterministic symbol ordering, missing/NaN data, positive-semidefinite shrinkage, and hand-calculated volatility.
- [ ] Implement sample covariance shrunk toward its diagonal and frozen tuple serialization.
- [ ] Run risk tests and Ruff.
- [ ] Commit as `feat: estimate shrinkage portfolio risk`.

### Task 3: Deterministic Constrained Allocation

**Files:**
- Create: `tradingagents/portfolio/optimizer.py`
- Modify: `tradingagents/portfolio/__init__.py`
- Modify: `tests/test_portfolio_optimizer.py`

**Interfaces:** `optimize_portfolio(state, forecasts, instrument_constraints, portfolio_constraints, risk_model) -> PortfolioOptimizationResult` and `validate_target_weights(result, ...)`.

- [ ] Write failing tests for positive-edge allocation, no-trade, transaction costs, uncertainty, position/sector/country/liquidity caps, turnover, cash, risk scaling, deterministic ties, and final validation.
- [ ] Implement stable net-edge ranking and constraint-aware incremental allocation.
- [ ] Add independent final constraint diagnostics and risk contribution output.
- [ ] Run optimizer and risk tests plus Ruff.
- [ ] Commit as `feat: optimize portfolios under hard constraints`.

### Task 4: Operational Entry Points and Verification

**Files:**
- Modify: `cli/main.py`
- Modify: `tradingagents/web/app.py`
- Modify: `tradingagents/web/service.py`
- Test: `tests/test_portfolio_optimizer.py`
- Test: `tests/test_web_service.py`
- Modify: `docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md`
- Modify: `docs/superpowers/plans/2026-08-20-constrained-portfolio-optimizer.md`

- [ ] Add JSON-input CLI and FastAPI optimizer adapters over the shared optimizer.
- [ ] Add adapter and serialization tests without order execution.
- [ ] Run focused and complete suites, Ruff, and diff checks.
- [ ] Record exact evidence and mark only Project D complete.
- [ ] Commit entry points and documentation separately.
