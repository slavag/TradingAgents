# Horizon-Aligned Outcome Scoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve immutable forecast records against caller-supplied point-in-time
price observations and compute deterministic, horizon-aligned accuracy and
calibration metrics without using an LLM.

**Architecture:** Add a frozen evaluation domain that first resolves the exact
future common-session endpoint and price basis, then scores only metrics whose
inputs exist. Keep per-record scoring separate from aggregate calibration so
missing labels and uncalibrated forecasts remain explicit rather than becoming
zeros.

**Tech Stack:** Python 3.10+, Pydantic v2, `Decimal`, pytest.

**Spec:** `docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md`

## Global Constraints

- Evaluation accepts supplied observations and performs no network request.
- Session dates are strictly increasing and unique.
- Horizon session 1 resolves to the first observation after `data_cutoff`.
- Resolution requires the same corporate-action adjustment basis as the record.
- Missing horizon, reference price, future sessions, or labels return stable reasons; they never become zero returns or failed predictions.
- Costs are deterministic basis-point inputs and never inferred.
- Brier and calibration metrics are computed only for records carrying valid direction probabilities.
- LLM text cannot determine labels or scores.

---

### Task 1: Common-Session Outcome Resolution

**Files:**
- Create: `tradingagents/evaluation/__init__.py`
- Create: `tradingagents/evaluation/outcomes.py`
- Create: `tests/test_forecast_outcomes.py`

**Interfaces:**
- Produces: `PriceObservation`, `OutcomeResolutionStatus`, `ResolvedOutcome`,
  and `resolve_forecast_outcome(record, observations, adjustment_basis,
  benchmark_reference_price=None, benchmark_observations=())`.
- Contract: select the `horizon_sessions`-th supplied session strictly after the
  record cutoff; calculate gross return from the stored reference price; align
  benchmark return to the same resolved date; retain the evaluated path for
  drawdown scoring.

- [x] **Step 1: Write failing resolution tests**

Cover one-session and three-session endpoints, weekend gaps, basis mismatch,
missing reference/horizon, insufficient sessions, non-monotonic observations,
and benchmark alignment to the instrument's resolved date.

- [x] **Step 2: Run resolution tests and confirm RED**

Run: `python -m pytest tests/test_forecast_outcomes.py -q`

Expected: collection fails because the evaluation package does not exist.

- [x] **Step 3: Implement frozen resolution models and resolver**

`ResolvedOutcome` stores record ID, stable status/reason, cutoff, resolved date,
horizon sessions, reference/end prices, realized return, benchmark return,
excess return, evaluated path, and adjustment basis. Unresolvable outcomes keep
all dependent metrics `None`.

- [x] **Step 4: Run outcome tests and Ruff**

Run: `python -m pytest tests/test_forecast_outcomes.py -q`

Run: `python -m ruff check tradingagents/evaluation tests/test_forecast_outcomes.py`

- [x] **Step 5: Commit outcome resolution**

```bash
git add -- tradingagents/evaluation/__init__.py tradingagents/evaluation/outcomes.py tests/test_forecast_outcomes.py
git commit -m "feat: resolve forecast outcomes by trading sessions"
```

---

### Task 2: Deterministic Per-Record Scoring

**Files:**
- Create: `tradingagents/evaluation/scoring.py`
- Create: `tests/test_forecast_scoring.py`

**Interfaces:**
- Produces: `RealizedDirection`, `ForecastScore`, and
  `score_forecast(record, outcome, transaction_cost_bps=0,
  flat_return_band=Decimal("0.02"))`.
- Contract: Buy/Overweight map up, Sell/Underweight map down, and Hold maps flat;
  realized direction uses the explicit flat band. Compute gross/net/excess
  return, target MAE/MAPE, target-range coverage, three-class Brier score, and
  path maximum drawdown only when their inputs exist.

- [x] **Step 1: Write failing scoring tests**

Cover positive/negative/flat direction, deterministic costs, central-target
error, interval coverage, Brier score, drawdown, unresolved outcomes, and
missing-metric reasons.

- [x] **Step 2: Run scoring tests and confirm RED**

Run: `python -m pytest tests/test_forecast_scoring.py -q`

Expected: import failure because the scorer does not exist.

- [x] **Step 3: Implement scoring without fallback values**

Every unavailable metric remains `None` and appears in `missing_metrics`.
Transaction costs equal `transaction_cost_bps / 10_000`; net return equals gross
return minus that cost. Brier score is the mean squared error across down, flat,
and up probabilities.

- [x] **Step 4: Run outcome and scoring tests**

Run: `python -m pytest tests/test_forecast_outcomes.py tests/test_forecast_scoring.py -q`

- [x] **Step 5: Commit scoring separately**

```bash
git add -- tradingagents/evaluation/scoring.py tests/test_forecast_scoring.py
git commit -m "feat: score resolved forecast outcomes"
```

---

### Task 3: Aggregate Calibration

**Files:**
- Create: `tradingagents/evaluation/calibration.py`
- Modify: `tests/test_forecast_scoring.py`

**Interfaces:**
- Produces: `CalibrationBin`, `CalibrationSummary`, and
  `summarize_calibration(scored_forecasts, bins=10)`.
- Contract: include only resolved forecasts with direction probabilities;
  report eligible/excluded counts, mean Brier score, per-confidence-bin observed
  frequency, and weighted expected calibration error. Empty eligible input
  returns `None` metrics with explicit exclusion counts.

- [x] **Step 1: Write failing aggregation tests**

Use hand-calculated two-record examples for Brier mean and calibration error,
plus empty and partially ineligible inputs.

- [x] **Step 2: Run calibration tests and confirm RED**

Run: `python -m pytest tests/test_forecast_scoring.py -k calibration -q`

- [x] **Step 3: Implement deterministic aggregation**

Assign each forecast's predicted class confidence to one of `bins` equal-width
intervals, compare confidence with whether the predicted class occurred, and
weight absolute gaps by bin sample count.

- [x] **Step 4: Run all Project B tests and Ruff**

Run: `python -m pytest tests/test_forecast_outcomes.py tests/test_forecast_scoring.py -q`

Run: `python -m ruff check tradingagents/evaluation tests/test_forecast_outcomes.py tests/test_forecast_scoring.py`

- [x] **Step 5: Commit calibration separately**

```bash
git add -- tradingagents/evaluation/__init__.py tradingagents/evaluation/calibration.py tests/test_forecast_scoring.py
git commit -m "feat: summarize forecast calibration"
```

---

### Task 4: Project B Verification and Roadmap Status

**Files:**
- Modify: `docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md`
- Modify: `docs/superpowers/plans/2026-08-20-horizon-aligned-outcome-scoring.md`

- [x] **Step 1: Run focused evaluation verification**

Run: `python -m pytest tests/test_forecast_record.py tests/test_forecast_outcomes.py tests/test_forecast_scoring.py -q`

- [x] **Step 2: Run the complete suite and Ruff**

Run: `python -m pytest -q`

Run: `python -m ruff check tradingagents/forecasting tradingagents/evaluation tests/test_forecast_record.py tests/test_forecast_outcomes.py tests/test_forecast_scoring.py`

- [x] **Step 3: Mark only Project B complete**

Record exact verification results. Keep walk-forward evaluation, portfolio
optimization, and role-specific model promotion pending.

- [x] **Step 4: Commit documentation separately**

Verification on 2026-08-20: `38 passed` in the focused forecast/evaluation
suite and `808 passed, 2 skipped` in the complete suite. Ruff and
`git diff --check` completed without errors. The optional skips were the missing
Bedrock dependency and absent DeepSeek live API key.

```bash
git add -- docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md docs/superpowers/plans/2026-08-20-horizon-aligned-outcome-scoring.md
git commit -m "docs: complete forecast outcome scoring project"
```
