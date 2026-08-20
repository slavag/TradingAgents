# Evaluation Runtime and Provenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Supply exact available market provenance to forecast records and automatically resolve, score, and persist matured forecasts without fabricating unavailable timestamps or metrics.

**Architecture:** Extend the verified OHLCV boundary with structured daily-snapshot metadata while retaining its Markdown renderer. Add an append-only filesystem evaluation registry and a deterministic runner that consumes forecast records through a price-history provider protocol. A yfinance-backed adapter uses the existing point-in-time OHLCV loader; tests use in-memory providers and perform no network calls.

**Tech Stack:** Python 3.10+, Pydantic v2, pandas, existing yfinance dataflow, pytest.

**Spec:** `docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md`

## Global Constraints

- Daily data records an observation date without inventing an intraday timestamp.
- Adjustment basis and vendor are copied from the dataflow that produced the values.
- Evaluation artifacts are append-only and content-addressed.
- An unresolved horizon writes no final score and remains eligible for a later retry.
- Provider errors are recorded as retryable runtime results, not zero-return outcomes.
- The runner performs no LLM call.
- Optimizer and UI/model-default changes remain separate projects.

---

### Task 1: Structured Verified Market Snapshot

**Files:**
- Modify: `tradingagents/dataflows/market_data_validator.py`
- Modify: `tradingagents/forecasting/schemas.py`
- Test: `tests/test_market_data_validator.py`
- Test: `tests/test_forecast_record.py`

**Interfaces:**
- Produces: `VerifiedMarketSnapshot`, `verified_market_snapshot(symbol, curr_date)`, and the existing `build_verified_market_snapshot` renderer.
- Changes `ReferencePriceSnapshot` to accept exact `observed_on: date` and optional timezone-aware `observed_at`; at least one is required.
- Contract: latest row date, close, quote currency when known, vendor, and adjustment basis come from the verified data source.

- [x] Write failing structured-snapshot and date-precision tests.
- [x] Run focused tests and confirm RED.
- [x] Implement the structured boundary and render Markdown from it.
- [x] Run focused tests and Ruff.
- [x] Commit as `feat: expose verified market snapshot metadata`.

### Task 2: Forecast Provenance Plumbing

**Files:**
- Modify: `tradingagents/agents/utils/agent_states.py`
- Modify: `tradingagents/agents/analysts/market_analyst.py`
- Modify: `tradingagents/forecasting/record_factory.py`
- Modify: `tradingagents/web/service.py`
- Modify: `cli/main.py`
- Test: `tests/test_forecast_record.py`
- Test: `tests/test_web_service.py`

**Interfaces:**
- Adds `verified_market_snapshot: dict` to graph state.
- Passes quote currency, reference value/date, adjustment basis, and vendor into report run metadata.
- Forecast records consume this structured snapshot before considering compatibility metadata.

- [x] Write failing graph-state and record-plumbing tests.
- [x] Confirm missing structured metadata causes the tests to fail.
- [x] Preserve the verified snapshot in graph state for shared report consumers.
- [x] Run record, market-data, Portfolio Manager, and graph tests.
- [x] Commit as `feat: carry market provenance into forecasts`.

### Task 3: Append-Only Evaluation Registry

**Files:**
- Create: `tradingagents/evaluation/registry.py`
- Create: `tests/test_evaluation_runtime.py`

**Interfaces:**
- Produces: `EvaluationRegistry`, `EvaluationArtifact`, and immutable methods `write_outcome`, `write_score`, `read_outcome`, and `read_score`.
- Stores `evaluation/outcome.json` and `evaluation/score.json` beside each `forecast_record.json`.
- Exact repeated writes are idempotent; conflicting content raises `FileExistsError`.

- [x] Write failing creation, idempotence, conflict, and corrupt-file tests.
- [x] Implement canonical append-only persistence.
- [x] Run registry tests and Ruff.
- [x] Commit as `feat: persist forecast evaluation artifacts`.

### Task 4: Deterministic Evaluation Runner and Price Provider

**Files:**
- Create: `tradingagents/evaluation/runtime.py`
- Modify: `tradingagents/evaluation/__init__.py`
- Modify: `tests/test_evaluation_runtime.py`

**Interfaces:**
- Produces: `PriceHistoryBundle`, `OutcomePriceProvider` protocol, `YFinanceOutcomePriceProvider`, `EvaluationRunResult`, `evaluate_forecast`, and `evaluate_report_tree`.
- Uses `resolve_forecast_outcome`, `score_forecast`, and `EvaluationRegistry`.
- Statuses: `scored`, `not_mature`, `retryable_provider_error`, `already_scored`, and `invalid_record`.

- [x] Write failing matured, immature, retryable-error, idempotent, and benchmark tests.
- [x] Implement the protocol-driven runner with an in-memory test provider.
- [x] Implement the yfinance adapter through existing `load_ohlcv` with total-return-adjusted basis.
- [x] Run runtime, outcome, scoring, and registry tests.
- [x] Commit as `feat: evaluate matured forecast records`.

### Task 5: Batch Entry Points and Verification

**Files:**
- Modify: `cli/main.py`
- Modify: `tradingagents/web/app.py`
- Modify: `tradingagents/web/service.py`
- Test: `tests/test_evaluation_runtime.py`
- Test: `tests/test_web_service.py`
- Modify: `docs/superpowers/plans/2026-08-20-evaluation-runtime-and-provenance.md`

**Interfaces:**
- Adds a CLI batch evaluation command and web evaluation endpoint.
- API and CLI results show coverage and resolved/pending/error counts without financial-performance claims. Visual dashboard presentation is part of Project E's unified evaluation/model screen.

- [x] Write failing CLI/API adapter tests.
- [x] Implement batch scanning and serialized summaries.
- [x] Run focused and complete suites plus Ruff and diff checks.
- [x] Record exact verification evidence.
- [x] Commit as `feat: expose forecast evaluation runtime`, then commit documentation separately.

Verification on 2026-08-20: `63 passed` in the focused operational suite and
`842 passed, 2 skipped` in the complete suite. Ruff and `git diff --check`
completed without errors. The optional skips were the missing Bedrock dependency
and absent DeepSeek live API key.
