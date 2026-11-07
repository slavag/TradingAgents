# Momentum Discover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the existing Discover tab into a dated, explainable shortlist of up to 20 US-listed equities with verified momentum and liquidity, new-to-research names first.

**Architecture:** Keep source retrieval, pure screening/ranking, cache/API orchestration, and browser rendering separate. Fixed server adapters supply candidate symbols and bounded market evidence; a deterministic screen decides eligibility and rank; the local API adds saved-research badges and safe freshness status; the existing Discover view renders cards and reuses Add to analysis.

**Tech Stack:** Python 3.13, FastAPI, pandas/yfinance, requests, `pandas-market-calendars>=5.5,<6` for NYSE trading sessions, vanilla JavaScript/CSS, pytest, Node tests, Ruff.

**Spec:** `docs/superpowers/specs/2026-10-16-momentum-discover-design.md`

## Global Constraints

- Existing Discover tab, US-listed stocks, default 20/optional 15, new-to-My-Research first; do not add another nav tab.
- A mover **or** attention feed can nominate a stock; cross-feed corroboration improves rank. FMP is optional; no endpoint, key, or account entitlement may be required for a fallback path.
- Active equity identity, at least 50 completed sessions, close >= 5 USD, 20-session mean unadjusted-close × actual-volume >= 10 million USD, positive five-session split-adjusted return, and quote at most two NYSE sessions old are hard gates. Unknown identity/currency/price/volume/date fails closed; never pad to 15.
- Sources, adjusted/unadjusted basis, UTC retrieval time, completed quote date, measured reasons, and partial/stale status must remain visible. Research-priority score is not a probability or recommendation.
- Fixed hosted endpoints, bounded request sizes/timeouts/concurrency, local-only API, sanitized warnings, in-memory cache. No LLM call, analysis job, live provider QA call, secret, raw provider error, or local history export.
- Preserve all pre-existing uncommitted AMD changes. Do not commit, push, switch the checkout, or alter saved reports without the user's explicit separate approval.

## Review Focus

- A split or missing unadjusted close must not make adjusted-close × volume pass the liquidity gate; Task 1 tests the two distinct series.
- The day after a market holiday or weekend must use NYSE sessions, not calendar days, for quote freshness; Task 1 tests this.
- A provider returning an oversized, malformed, redirected, or unauthorized response must yield a bounded warning with no key/body leakage; Task 2 tests each class.
- A failed/queued-only prior job must not mark a ticker as previously analyzed; Task 3 tests saved-report eligibility.
- Rapid 15/20 changes and refreshes must not let an older browser response replace a newer shortlist; Task 4 tests stale-response rejection.

---

### Task 1: Pure eligibility and ranking

**Files:** Create `tradingagents/web/momentum_screen.py`, `tests/test_momentum_screen.py`; modify `pyproject.toml` for the NYSE calendar dependency.

**Interfaces:** Produce `DailyBar(date, adjusted_close, unadjusted_close, volume)`, `EquityIdentity(symbol, name, exchange, currency, quote_type, active)`, and `MomentumCandidate(ticker, company_name, close, currency, quote_date, price_basis, ret_1d_pct, ret_5d_pct, ret_20d_pct, avg_dollar_volume, relative_volume, trend_checks, score, sources, reason, risk_flags, already_analyzed)`. Expose `screen_candidates(source_membership: Mapping[str, set[str]], histories: Mapping[str, Sequence[DailyBar]], identities: Mapping[str, EquityIdentity], *, as_of: date, analyzed: set[str]) -> list[MomentumCandidate]` and `quote_is_fresh(observed: date, as_of: date) -> bool` using `pandas_market_calendars.get_calendar("NYSE")`.

- [ ] **Step 1: Write failing tests** in `tests/test_momentum_screen.py`: a qualifying new equity ranks before an equally strong analyzed equity; ETF, OTC, unknown type/currency, $4.99 close, <$10m liquidity, nonpositive 5D return, fewer than 50 rows, and stale quotes are omitted; a Good Friday/weekend quote passes the two-session rule; adjusted return and unadjusted dollar volume use different fields. Assert exact ticker order, metrics, and exclusion, not source text.
- [ ] **Step 2: Run red tests.** `pytest -q tests/test_momentum_screen.py` must fail for missing screen interfaces.
- [ ] **Step 3: Implement the minimal pure screen.** Score = `min(max(ret5_pct,0),20)/4 + min(max(ret20_pct,0),24)/8 + trend_checks + min(max(relative_volume-1,0),2) + corroboration - spike_penalty`, floored at zero and rounded to two decimals. Corroboration is 2 for both mover and attention families, 1 for two distinct feeds in one family, else 0; `spike_penalty=2` when 1D gain exceeds 25%. Clamp each term; sort `(already_analyzed, -score, ticker)`. Generate complete reasons only from measured fields and flag the spike. Keep the function network-free.
- [ ] **Step 4: Run green tests.** `pytest -q tests/test_momentum_screen.py` passes.

### Task 2: Fixed candidate and market-evidence adapters

**Files:** Create `tradingagents/web/momentum_sources.py`, `tests/test_momentum_sources.py`; reuse `tradingagents/web/speaking_sources.py` and `tradingagents/settings.py` without changing their public contracts.

**Interfaces:** Expose `fetch_source_membership() -> tuple[dict[str, set[str]], list[str]]` using the in-repo ApeWisdom/StockTwits parsers, Alpha Vantage `TOP_GAINERS_LOSERS`, and optional FMP `biggest-gainers`/`most-actives`; source names are fixed literals. Expose `fetch_market_evidence(symbols: Sequence[str]) -> tuple[dict[str, tuple[DailyBar, ...]], dict[str, EquityIdentity], list[str]]`, with at most 100 normalized candidate symbols and at most 40 bounded identity checks after preliminary price ranking. Adapters use Task 1 types; warnings are stable codes only.

- [ ] **Step 1: Write failing adapter tests** with fake HTTP/yfinance responses: either feed family alone nominates a symbol; same symbol deduplicates; missing FMP entitlement leaves Alpha Vantage/ApeWisdom usable; bad symbols and non-US identity are rejected; HTTP redirects, 401/403/429, timeout, malformed/oversized JSON, and partial price frames yield safe warning codes; no credential or raw body appears in results/log capture.
- [ ] **Step 2: Run red tests.** `pytest -q tests/test_momentum_sources.py` fails for missing adapters.
- [ ] **Step 3: Implement fixed HTTPS adapters and bounded yfinance history/identity loading.** Read configured keys only server-side; use explicit timeouts and response caps. Do not load the hard-coded external MyAgent module. When identity verification fails, omit that symbol rather than guessing its type.
- [ ] **Step 4: Run green tests.** `pytest -q tests/test_momentum_sources.py` passes.

### Task 3: Cached discovery service and local API

**Files:** Create `tradingagents/web/momentum_discovery.py`, `tests/test_momentum_discovery.py`; modify `tradingagents/web/app.py` and add `tests/test_web_momentum_discovery.py`.

**Interfaces:** Expose `get_momentum_watchlist(*, limit: Literal[15,20] = 20, force: bool = False) -> dict`. The local-only route is `GET /api/discover/stocks?limit=20&refresh=false`, with response `{status, retrieved_at, quote_as_of, items, provider_status, warnings}`. Items use Task 1 candidate fields. `saved_jobs(RESULTS_ROOT)` plus `report_path(...)` from `tradingagents/web/history.py` supply `analyzed`; count only tickers with a saved successful investor/full report, not a failed or queued attempt. Source/price scores are cached for 30 minutes independently of history badges; coalesce in-flight refreshes; an explicit refresh bypasses the cache subject to a 60-second cooldown. On refresh failure, return the last good snapshot as `stale` with its original times and warning, otherwise `unavailable` with no items.

- [ ] **Step 1: Write failing service/API tests** for 15/20 validation, cached versus forced refresh, cooldown, concurrent coalescing, partial provider failure, stale retained timestamp, no-history case, failed/queued-only jobs, loopback/same-origin rejection, and no LLM/analysis invocation. Assert sanitized payloads and no secret/raw provider text.
- [ ] **Step 2: Run red tests.** `pytest -q tests/test_momentum_discovery.py tests/test_web_momentum_discovery.py` fails for missing service/route.
- [ ] **Step 3: Implement service and route.** Reuse `_require_local_settings_request` and `saved_jobs`; keep existing `/api/speaking-stocks` unchanged for compatibility. Never accept a provider URL or arbitrary symbol list from the browser.
- [ ] **Step 4: Run green tests.** The two task test files pass.

### Task 4: Discover watchlist UI and interactions

**Files:** Modify `tradingagents/web/static/index.html`, `app.js`, `styles.css`; extend `tests/test_web_static_tape.py` and create `tests/js/discover.test.cjs` for the DOM-independent renderer/state helpers.

**Interfaces:** `loadMomentumDiscovery({force = false})` calls Task 3's endpoint. `renderMomentumCandidate(item)` escapes all provider-supplied fields and presents measured 1D/5D/20D moves, volume/trend, sources, reason and flags. `window.TradingAgentsRuntime.loadDiscovery()` keeps lazy first-view loading and market overview; the existing `tradingagents:ticker-added` event transfers Add to analysis into the form without submitting. Previously analyzed candidates link to the existing research route.

- [ ] **Step 1: Write failing UI tests** for new/analyzed groups, 15/20 count, timestamp and partial/stale/empty copy, HTML injection escaping, a visible Add button with no form submit, saved-research link, and two overlapping fetches where the older response arrives last.
- [ ] **Step 2: Run red tests.** `pytest -q tests/test_web_static_tape.py` and `node --test tests/js/discover.test.cjs` fail for the new behavior.
- [ ] **Step 3: Replace the Discover chatter area with accessible watchlist cards and controls.** Keep the market overview and ticker-detail modal; use card-local Add and the existing form event. Make mobile cards stack and use text labels in addition to color.
- [ ] **Step 4: Run green tests.** Both task test commands pass.

### Task 5: Documentation and end-to-end verification

**Files:** Modify `README.md`; use existing web tests and temporary, uncommitted browser fixtures for QA.

- [ ] **Step 1: Document** the research-only meaning, US/equity/price/liquidity gates, score factors, new-to-you grouping, provider-degraded states, Refresh/cache rules, and that no model or analysis job runs on discovery.
- [ ] **Step 2: Run all deterministic checks.** `pytest -q`, `node --test tests/js/*.test.cjs tests/test_model_picker_js.mjs`, `ruff check .`, and `git diff --check` all exit 0; report named skips/warnings.
- [ ] **Step 3: Browser QA against fixture responses only.** At desktop and 390px, verify page identity, nonblank cards, no page-wide overflow, no relevant console error, 15/20 selection, stale/empty state, Add to analysis, and previously analyzed link. Do not hit live provider endpoints or launch a paid run.
- [ ] **Step 4: Review the final diff and hand off.** Keep AMD changes distinct, report any untested live-provider behavior, and ask for a fresh user-initiated Discover Refresh if live acceptance is desired. Do not commit or push without a new explicit approval.
