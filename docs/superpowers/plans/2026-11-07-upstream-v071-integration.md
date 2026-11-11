# Selective Upstream v0.7.1 Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate compatible upstream behavior from releases after v0.4.0 through v0.7.1 into the current customized fork, without losing fork-specific work or silently changing auth/network defaults.

**Architecture:** Start from current fork `main` in an isolated worktree with an exact copy of its in-progress AMD/Discover edits. Use the old v0.6 integration branch and upstream release commits as comparison sources, then make focused, test-first semantic changes in fork modules; do not merge the upstream tree wholesale. Maintain a release-disposition ledger and hand off an uncommitted, reviewable diff.

**Tech Stack:** Python 3.10–3.13, FastAPI, Pydantic, LangChain provider adapters, pytest, Ruff, vanilla JS/Node tests, Git worktrees.

**Spec:** `docs/superpowers/specs/2026-11-07-upstream-v071-integration-design.md`

## Global Constraints

- Target only upstream tag `v0.7.1` (`c4544913a14ed3df5f1ed7b9e8483270ce20682a`), not upstream `main` tip.
- Current committed fork base is `e7cb81a1cacec4dd18993dd441ff730490b16591`; preserve and copy its current tracked/untracked AMD and Discover edits. Do not switch, reset, or clean the primary checkout.
- Treat `codex/upstream-v0.6.0-integration` (`ee531c3`) as previously completed semantic work to audit, not a branch to merge wholesale. Existing current-fork implementations count as integrated when tests prove equivalent behavior.
- Preserve fork decision statuses, point-in-time evidence, reports, prompt caching, provider credential boundaries, local-only settings, Discover, and checkpoint/stream lifecycle.
- New ChatGPT-plan sign-in is explicit and opt-in. The upstream update/announcement check remains off by default; no startup network request. Existing settled outcomes are not rewritten or mixed into a comparable backtest score without labeling.
- Do not commit, push, create a PR, merge into `main`, run paid model calls, or initiate live market/provider/auth QA. These require separate user approval or action.
- Keep a release-note/material-commit disposition ledger: already present, adopted, adapted, superseded, or deferred with reason. No blanket “v0.7.1 integrated” claim merely because tests pass.

## Review Focus

- An unpadded model date such as `2024-7-5` must not admit a later SEC filing; Task 3 tests canonical date parsing at the vendor boundary.
- A decision made before a long market closure must settle on the first completed subsequent close, never an in-progress bar or remain pending indefinitely; Task 5 tests both conditions.
- Broker aliases must resolve to the analyzed instrument without accepting non-finite holdings or treating missing portfolio context as a flat book; Task 4 tests those distinctions.
- Saving a run into a reused report folder must not leave stale sections or erase another run's immutable report; Task 7 tests both paths.
- Importing optional auth/update features must not read tokens, log in, or contact a release server on startup or ordinary analysis; Task 9 tests the no-side-effect default.

---

### Task 1: Isolated snapshot and deterministic baseline

**Files:** Modify the copied `tests/test_momentum_sources.py` only as needed for a frozen quote clock; create an ignored integration ledger under this plan's `.superpowers/sdd/` workspace.

**Interfaces:** Produce an isolated `codex/upstream-v071-integration` worktree from fork `main` containing byte-identical tracked and untracked AMD/Discover work; record source/target hashes and a baseline test report. Prefer a native worktree tool; if unavailable, use a validated `/private/tmp/tradingagents-v071-integration` worktree. Do not change primary `main`.

- [ ] **Step 1: Inventory** `git status --porcelain -uall`, branch/worktree registrations, and file hashes. Copy only those exact paths into the isolated worktree and verify copied hashes; keep any credential or ignored file out.
- [ ] **Step 2: Reproduce baseline failure.** Run `pytest -q tests/test_momentum_sources.py` in the copied tree. Expected: seven date-dependent identity/market-evidence failures on 2026-11-07 because the fixed October 15 quote is stale.
- [ ] **Step 3: Freeze the fixture clock.** In `tests/test_momentum_sources.py`, set module `_utcnow` to a completed October 2026 NYSE session for tests using `market_frame`; preserve the test that deliberately varies the clock for unfinished-session coverage. Do not change production freshness logic.
- [ ] **Step 4: Verify baseline.** Run the narrow test, full `pytest -q`, `ruff check .`, Node test suite, and `git diff --check`; record exact pass/skip/warning counts before upstream changes.

### Task 2: Reconcile prior v0.6 work and inventory upstream deltas

**Files:** Create `docs/upstream-v0.7.1-integration.md` in the isolated tree. No production file changes in this task.

**Interfaces:** Produce a disposition ledger with source release/commit, fork target, status, test evidence, and reason. Use `v0.4.0..v0.7.1` and the completed `ee531c3` branch; distinguish a missing Git ancestor from missing behavior.

- [ ] **Step 1: Inventory** every release-note change and material non-merge commit from v0.4.1–v0.7.1. Record upstream diff path/commit and the current fork equivalent or gap.
- [ ] **Step 2: Verify v0.4.1–v0.6 behavior** against current fork tests and source: time windows, vendor/SEC, portfolio input, safe caches, CLI, parallel analysts, all-ticker settlement, report provenance/HTML, backtest, and Jev's current key-gated behavior. Mark only observed equivalence “already present.”
- [ ] **Step 3: Gate gaps.** Map missing v0.6 behavior to Tasks 3–10; for any uncovered gap, add a focused test/implementation step to the owning task before coding it. Record intentionally superseded architecture rather than cherry-picking it.

### Task 3: Canonical run dates and point-in-time SEC statements

**Files:** Modify `tradingagents/dataflows/sec.py`, `sec_documents.py`, `fred.py`, `reddit.py`, `stocktwits.py`, applicable date/tool routing and `tradingagents/agents/analysts/sentiment_analyst.py`; create a small Jev adapter with upstream v0.7.1 key-gated live behavior and the fork's historical no-post safeguard. Extend `tests/test_sec_evidence.py`, `test_sec_documents.py`, `test_date_boundaries.py`, `test_fred.py`, `test_reddit_fallback.py`, and `test_stocktwits_resilience.py`. Reference upstream `a5b0f21`, `2717033`, `50bf38e`, `bf7f4b5`, `ee06dcb` and earlier v0.6 equivalents.

**Interfaces:** Normalize any model-supplied date before comparing it with filing dates; select one statement row per report period and retain sub-million precision; reject impossible vendor frequencies and reversed date windows with a correction signal rather than false no-data.

- [ ] **Step 1: Write regressions** for `2024-7-5` vs a later SEC filing, duplicate quarter/year-to-date statement columns, a 0.6-million figure, invalid frequency, `end < start`, euro-area FRED series, crypto Reddit communities, and Jev's key-gated/live-only endpoint gate. Run each targeted test and confirm the intended pre-change failure or record equivalent existing behavior in the ledger.
- [ ] **Step 2: Implement** only missing behavior at the fork's SEC/tool boundaries; keep existing as-filed and cutoff evidence semantics.
- [ ] **Step 3: Verify** targeted SEC/date/vendor tests, then `pytest -q` and Ruff.

### Task 4: Portfolio identity and finite values

**Files:** Modify `tradingagents/portfolio/state.py`, portfolio handoff in `tradingagents/graph/trading_graph.py` and CLI/web request validation where applicable; extend `tests/test_portfolio_optimizer.py`, `tests/test_portfolio_manager_integrity.py`, `tests/test_cli_symbol_handling.py`. Reference upstream `62a06ed`, `3225a3c` and prior v0.5 portfolio work.

**Interfaces:** A supplied holding alias resolves to the exact analyzed instrument; NaN/Infinity in cash, quantity or basis fails before a run; absent context remains distinct from an explicitly flat portfolio.

- [ ] **Step 1: Write regressions** for `BTCUSDT`/`BTC-USD`, `700.HK`/`0700.HK`, ambiguous ticker spellings, non-finite numbers, and absent-vs-empty context. Observe RED only for actual gaps.
- [ ] **Step 2: Adapt** upstream normalization/finite-value checks to fork `PortfolioState` and decision prompts, retaining existing optimizer constraints.
- [ ] **Step 3: Verify** portfolio/graph/CLI tests and full suite.

### Task 5: Completed-close memory and outcome settlement

**Files:** Modify `tradingagents/agents/utils/memory.py`, `tradingagents/graph/trading_graph.py`, `setup.py`, `analyst_execution.py`, `default_config.py`, `tradingagents/evaluation/outcomes.py` or `runtime.py`; extend `tests/test_memory_log.py`, `tests/test_forecast_outcomes.py`, `tests/test_graph_execution_lifecycle.py`. Reference upstream `f5e3eae`, `eb4a486`, `caf6792` and prior v0.6 parallel/settlement work.

**Interfaces:** A decision's entry price is the first completed close after its date; holding-window settlement finds the next completed close across long closures; a still-trading bar never resolves an outcome. Historical lessons sort by decision date but remain gated by outcome availability date. Already-settled legacy entries stay unchanged and are labeled if mixed with new-basis results.

- [ ] **Step 1: Write regressions** for same-day/in-progress bars, a closure longer than seven days, cross-ticker lesson ordering, legacy-settled records, opt-in parallel analyst fan-out, bounded analyst tool rounds, and all-ticker settlement after the analyst phase. Run RED or mark already covered with evidence.
- [ ] **Step 2: Implement** missing close/session and optional graph behavior through the fork's outcome provider, without replacing the append-only memory log or disabling its lock/atomic writes. Sequential execution stays the default and checkpoint identity separates graph shapes.
- [ ] **Step 3: Verify** memory/outcome tests and full suite.

### Task 6: Independent backtest sweeps and failure reporting

**Files:** Create/adapt `tradingagents/backtest.py` only if the fork lacks an equivalent runner; otherwise extend `tradingagents/evaluation/runtime.py` and `cli/main.py`. Add/extend `tests/test_backtest.py`, `tests/test_forecast_scoring.py`, `tests/test_cli_env_skip.py`. Reference upstream v0.5 backtest, `28d3265`, `1c0e3e3`.

**Interfaces:** `run_backtest(tickers, dates, config, *, run_id=None, portfolio=None) -> BacktestResult` and `summarize(source) -> BacktestSummary` or a documented fork-equivalent API. Cells remain independent decisions, not simulated fills; a generated run ID claims a unique directory, resume skips completed cells, final settlement errors appear in results.

- [ ] **Step 1: Write regressions** for same-second run collisions, duplicate cells, interrupted resume, final settlement failure, no-data/REVIEW cells, and first-close scoring.
- [ ] **Step 2: Reuse** the old integration branch's tested backtest shape where compatible; adapt to fork `EvaluationRegistry`, report/manifest paths, and outcome basis rather than creating a second competing scoreboard.
- [ ] **Step 3: Verify** backtest/evaluation/CLI tests and full suite.

### Task 7: Precision prices and safe report replacement

**Files:** Modify the fork's market quote/formatting module in `tradingagents/dataflows/`, `tradingagents/reporting.py`, and `tests/test_reporting.py`, `tests/test_report_names.py`, `tests/test_forecast_record.py`; preserve `tradingagents/investor_deck.py`/HTML and consolidated renderers. Reference upstream `6c23d75`, `21bc5d8`, and v0.6 report provenance.

**Interfaces:** A sub-cent price and currency-pair pip retain significant digits; replacing reports in an exact reused run folder removes only stale sections from that run, never another run's timestamped report or source evidence.

- [ ] **Step 1: Write regressions** for sub-cent/pip quotes and a reused report folder containing obsolete sections beside immutable artifacts; run RED or record already covered.
- [ ] **Step 2: Implement** precision and exact-folder cleanup with path-safety guards; do not truncate investor guidance or replace fork report templates.
- [ ] **Step 3: Verify** data/report tests and full suite.

### Task 8: Provider runtime and CLI compatibility fixes

**Files:** Modify `tradingagents/llm_clients/azure_client.py`, `anthropic_client.py`, `factory.py`, `model_catalog.py`, and relevant `cli/` modules; extend `tests/test_anthropic_effort.py`, `tests/test_provider_registry.py`, `tests/test_cli_config_precedence.py`; add `tests/test_azure_client.py` if needed. Reference upstream `347974a`, `ae04247`, `7142746`, `a334a1f`.

**Interfaces:** Azure receives configured endpoint and output-token cap; dated Claude snapshots parse their version for capability gating; tier-specific thinking settings survive a different primary provider; CLI symbol checks reuse the data layer's canonical rule. Configured/promoted/custom model IDs remain selected.

- [ ] **Step 1: Write regressions** for each provider/CLI failure mode with fake transports and no live calls. Confirm RED or record equivalence.
- [ ] **Step 2: Adapt** missing behavior inside fork adapters and provider registry; preserve prompt-caching telemetry and secret redaction.
- [ ] **Step 3: Verify** provider/CLI tests, full suite, and Ruff.

### Task 9: Opt-in ChatGPT-plan authentication without startup side effects

**Files:** Create/adapt `tradingagents/llm_clients/chatgpt_auth.py` and provider adapter only if the fork can safely support upstream's documented flow; modify `factory.py`, `api_key_env.py`, `cli/main.py`, and `pyproject.toml` as required; add `tests/test_chatgpt_auth.py`, `tests/test_chatgpt_provider.py`. Reference upstream `071a6ce`, `69042d6`, `36479ae`.

**Interfaces:** `tradingagents login chatgpt` is the sole explicit sign-in entry; logout revokes/forgets safely. Ordinary imports, model selection, CLI/web startup, and unrelated providers do not read a token file or make auth requests. Token storage is owner-only, atomic and rotation-safe; refresh is locked. If these boundaries cannot be met in this fork, mark the feature deferred with exact reason and test evidence rather than ship a weakened sign-in.

- [ ] **Step 1: Review** current upstream OAuth/PKCE/ID-token implementation against fork credential paths and supported dependencies. Do not copy credentials or perform a real sign-in.
- [ ] **Step 2: Write offline regressions** for no-startup-network/token-read, wrong state/nonce/issuer/audience, private atomic token file, refresh rotation, logout failure, and no exposure through web settings. Confirm RED for missing behavior.
- [ ] **Step 3: Adapt** provider registration and explicit CLI commands; add `cryptography>=42` and `packaging>=24` only if required and compatible with the fork's Python floor.
- [ ] **Step 4: Verify** auth/provider/CLI tests, package import smoke and full suite; document untested live OAuth.

### Task 10: Release check, packaging, Docker and remaining small fixes

**Files:** Modify `cli/main.py` or `cli/updates.py`, `pyproject.toml`, `Dockerfile`, `.dockerignore`, plus focused tests. Reference upstream `c3cdaeb`, `c9be8b6`, `ec4e2c8`, `b78f136`, `d46c052` and any ledgered small gaps.

**Interfaces:** No background release call on normal startup; an explicit opt-in update check may use bounded cache/timeouts. Release artifacts include the fork's HTML/report/assets. Docker never copies `.env*`. Polymarket odds retain sub-percent precision; platform tests do not depend on POSIX-only behavior.

- [ ] **Step 1: Write regressions** for no-startup-network, `.env.enterprise` exclusion, small odds rendering, and identified platform compatibility gaps. Run RED or record already covered.
- [ ] **Step 2: Implement** only compatible changes; keep version metadata honest about a selective fork integration, not an unmodified upstream distribution.
- [ ] **Step 3: Verify** targeted tests, wheel/build/import smoke, full suite, Ruff, and diff checks.

### Task 11: Whole-tree audit and handoff

**Files:** Finalize `docs/upstream-v0.7.1-integration.md` and the plan's ignored progress ledger; no unrelated source edits.

**Interfaces:** Every release-note change/material commit has a disposition and evidence; the copied AMD/Discover snapshot remains distinguishable from upstream-derived edits.

- [ ] **Step 1: Run** full `pytest -q`, `ruff check .`, Node web tests, `git diff --check`, package/import smoke, and conflict-marker scan. Record exact failures, skips, and warnings.
- [ ] **Step 2: Review** the whole working-tree diff (including untracked files) against the spec, old v0.6 ledger, and upstream release notes. Reconcile critical findings with failing tests before claiming completion.
- [ ] **Step 3: Verify** primary `main` status/hashes are unchanged. Hand off the isolated worktree path, release-disposition summary, and remaining live/optional-risk gaps. Do not commit, push, merge, or clean another worktree without separate approval.
