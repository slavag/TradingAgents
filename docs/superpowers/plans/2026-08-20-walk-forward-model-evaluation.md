# Walk-Forward Model Evaluation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compare pinned incumbent and challenger model-role configurations on
strict point-in-time folds and promote only challengers that satisfy explicit,
paired out-of-sample thresholds.

**Architecture:** Build immutable fold definitions from dated scored forecasts,
with separate training, promotion, and evaluation windows. Compare configurations
only on shared eligible record IDs, then produce deterministic role leaderboards
from promotion-gate results. Model aliases remain descriptive; evaluated
identities use provider/model/prompt/config hashes.

**Tech Stack:** Python 3.10+, Pydantic v2, `Decimal`, pytest.

**Spec:** `docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md`

## Global Constraints

- Fold windows are chronological, non-overlapping, and use only records whose data cutoff is inside that window.
- Promotion and final evaluation windows never overlap.
- Comparisons use paired shared record IDs; missing challenger results cannot improve its score.
- Minimum paired coverage and sample count are explicit gate inputs.
- Configuration identities require pinned provider/model IDs plus prompt and config hashes.
- Promotion thresholds are deterministic and cannot be overridden by LLM prose.
- Regime and source-availability slices are reported, not silently pooled away.
- Project D portfolio optimization and Project E UI defaults remain out of scope.

---

### Task 1: Point-in-Time Fold Construction

**Files:**
- Create: `tradingagents/evaluation/walk_forward.py`
- Create: `tests/test_walk_forward_evaluation.py`

**Interfaces:**
- Produces: `EvaluationSample`, `WalkForwardFold`, and
  `build_walk_forward_folds(samples, train_days, promotion_days,
  evaluation_days, step_days)`.
- Contract: each sample contains record ID, cutoff date, resolved score, role,
  configuration ID, regime, and source-availability slice. Fold construction
  rejects duplicate record/config pairs and returns only complete chronological
  windows.

- [ ] **Step 1: Write failing fold tests**

Cover exact boundaries, incomplete trailing windows, overlapping-window
rejection, duplicate samples, deterministic ordering, and the invariant that no
record ID appears in more than one window of a fold.

- [ ] **Step 2: Run fold tests and confirm RED**

Run: `python -m pytest tests/test_walk_forward_evaluation.py -k fold -q`

- [ ] **Step 3: Implement frozen samples and folds**

Use date comparisons rather than row offsets. Store window start/end dates and
sorted tuples of sample identifiers. Validate `train_days`, `promotion_days`,
`evaluation_days`, and `step_days` as positive integers.

- [ ] **Step 4: Run fold tests and Ruff**

Run: `python -m pytest tests/test_walk_forward_evaluation.py -k fold -q`

Run: `python -m ruff check tradingagents/evaluation/walk_forward.py tests/test_walk_forward_evaluation.py`

- [ ] **Step 5: Commit folds separately**

```bash
git add -- tradingagents/evaluation/walk_forward.py tests/test_walk_forward_evaluation.py
git commit -m "feat: build point-in-time evaluation folds"
```

---

### Task 2: Paired Promotion Gates

**Files:**
- Modify: `tradingagents/evaluation/walk_forward.py`
- Modify: `tests/test_walk_forward_evaluation.py`

**Interfaces:**
- Produces: `PromotionThresholds`, `PairedComparison`, `PromotionDecision`, and
  `compare_paired_configurations(incumbent, challenger, thresholds)`.
- Contract: pair by record ID, enforce minimum samples and challenger coverage,
  calculate literal metric deltas, and promote only when every configured gate
  passes. Supported gates are direction accuracy, mean excess return, mean
  Brier score, and maximum allowed drawdown regression.

- [ ] **Step 1: Write failing paired-comparison tests**

Cover a passing challenger, insufficient shared samples, insufficient coverage,
improved accuracy with unacceptable Brier regression, missing metrics, and
order-independent results.

- [ ] **Step 2: Run comparison tests and confirm RED**

Run: `python -m pytest tests/test_walk_forward_evaluation.py -k "paired or promotion" -q`

- [ ] **Step 3: Implement explicit gates and rejection reasons**

Store paired counts, coverage, incumbent/challenger aggregates, metric deltas,
`promoted: bool`, and sorted stable rejection reasons. A missing required metric
rejects promotion rather than becoming zero.

- [ ] **Step 4: Run all walk-forward tests**

Run: `python -m pytest tests/test_walk_forward_evaluation.py -q`

- [ ] **Step 5: Commit promotion gates separately**

```bash
git add -- tradingagents/evaluation/walk_forward.py tests/test_walk_forward_evaluation.py
git commit -m "feat: gate model promotion on paired results"
```

---

### Task 3: Deterministic Role Leaderboard

**Files:**
- Create: `tradingagents/evaluation/leaderboard.py`
- Modify: `tradingagents/evaluation/__init__.py`
- Modify: `tests/test_walk_forward_evaluation.py`

**Interfaces:**
- Produces: `ConfigurationIdentity`, `LeaderboardEntry`, `RoleLeaderboard`, and
  `build_role_leaderboard(role, comparisons, incumbent_configuration_id)`.
- Contract: rank only pinned configurations that passed coverage; retain the
  incumbent unless a challenger has a passing promotion decision; break exact
  ties lexicographically by configuration ID.

- [ ] **Step 1: Write failing leaderboard tests**

Cover retained incumbent, promoted challenger, failed challenger exclusion,
multiple passing challengers, stable ties, and separate role isolation.

- [ ] **Step 2: Run leaderboard tests and confirm RED**

Run: `python -m pytest tests/test_walk_forward_evaluation.py -k leaderboard -q`

- [ ] **Step 3: Implement immutable leaderboard output**

Entries expose rank, configuration identity, paired coverage, metric deltas,
promotion status, and rejection reasons. The selected configuration is the
incumbent or the first passing ranked challenger.

- [ ] **Step 4: Run Project C tests and Ruff**

Run: `python -m pytest tests/test_walk_forward_evaluation.py -q`

Run: `python -m ruff check tradingagents/evaluation tests/test_walk_forward_evaluation.py`

- [ ] **Step 5: Commit leaderboard separately**

```bash
git add -- tradingagents/evaluation/__init__.py tradingagents/evaluation/leaderboard.py tests/test_walk_forward_evaluation.py
git commit -m "feat: rank role-specific model configurations"
```

---

### Task 4: Project C Verification and Roadmap Status

**Files:**
- Modify: `docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md`
- Modify: `docs/superpowers/plans/2026-08-20-walk-forward-model-evaluation.md`

- [ ] **Step 1: Run focused Project C verification**

Run: `python -m pytest tests/test_forecast_outcomes.py tests/test_forecast_scoring.py tests/test_walk_forward_evaluation.py -q`

- [ ] **Step 2: Run the complete suite and Ruff**

Run: `python -m pytest -q`

Run: `python -m ruff check tradingagents/evaluation tests/test_forecast_outcomes.py tests/test_forecast_scoring.py tests/test_walk_forward_evaluation.py`

- [ ] **Step 3: Mark only Project C complete**

Record exact verification results. Keep portfolio optimization and
leaderboard-driven UI/model defaults pending.

- [ ] **Step 4: Commit documentation separately**

```bash
git add -- docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md docs/superpowers/plans/2026-08-20-walk-forward-model-evaluation.md
git commit -m "docs: complete walk-forward evaluation project"
```
