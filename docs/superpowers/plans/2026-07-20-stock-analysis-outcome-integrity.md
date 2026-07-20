# Stock Analysis Outcome Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent future-data leakage in historical stock analyses and make deferred outcomes, displayed targets, and streaming execution temporally honest and consistent.

**Architecture:** A request-scoped temporal context classifies each run as live or historical and gives all data sources one cutoff contract. The graph owns the complete execution lifecycle for both invoke and stream paths, while memory and return labels use the same cutoff. Historical sources without archives return explicit unavailable sentinels; no component converts missing evidence into a neutral estimate.

**Tech Stack:** Python 3.10+, LangGraph, pandas, yfinance, Pydantic, pytest, unittest.mock, ContextVar.

## Global Constraints

- Preserve current live-mode behavior unless this plan explicitly changes it.
- Historical mode means `as_of_date < date.today()` using the host's local date.
- Missing historical evidence is unavailable, not neutral.
- No new paid vendor or model-training dependency is introduced.
- Existing memory entries and saved result files remain readable.
- Every production change follows a failing-test, minimal-fix, passing-test cycle.
- Do not stage or revert unrelated user changes.

## File Map

- Create `tradingagents/dataflows/temporal.py`: request-scoped analysis date, mode, and historical-unavailability sentinel.
- Modify `tradingagents/agents/utils/agent_states.py`: expose `as_of_date` and `analysis_mode` in graph state.
- Modify `tradingagents/graph/propagation.py`: initialize the temporal state fields.
- Modify current-only dataflows: `stocktwits.py`, `reddit.py`, `polymarket.py`, `y_finance.py`, and `alpha_vantage_fundamentals.py`.
- Modify `fred.py`: request the data vintage known on the analysis date.
- Modify `memory.py`: chronological context filtering and backward-compatible outcome metadata.
- Modify `trading_graph.py`: cutoff-aware memory, executable outcome labels, and shared invoke/stream lifecycle.
- Modify `reflection.py`: call benchmark differences “excess return,” not risk-adjusted alpha.
- Modify `cli/main.py` and `tradingagents/web/service.py`: shared streaming and honest target presentation.
- Add or extend focused tests under `tests/` for every behavior.

---

### Task 1: Request-scoped temporal contract

**Files:**
- Create: `tradingagents/dataflows/temporal.py`
- Modify: `tradingagents/agents/utils/agent_states.py`
- Modify: `tradingagents/graph/propagation.py`
- Create: `tests/test_temporal_context.py`

**Interfaces:**
- Produces: `AnalysisMode`, `AnalysisContext`, `build_analysis_context(as_of_date, today=None)`, `use_analysis_context(as_of_date)`, `get_analysis_context()`, `is_historical(as_of_date=None)`, and `historical_unavailable(source, as_of_date=None)`.
- Produces graph-state keys: `as_of_date: str` and `analysis_mode: str`.

- [ ] **Step 1: Write failing temporal-context tests**

```python
from datetime import date

import pytest

from tradingagents.dataflows.temporal import (
    AnalysisMode,
    build_analysis_context,
    get_analysis_context,
    historical_unavailable,
    use_analysis_context,
)
from tradingagents.graph.propagation import Propagator


def test_past_date_is_historical():
    context = build_analysis_context("2026-07-19", today=date(2026, 7, 20))
    assert context.mode is AnalysisMode.HISTORICAL


def test_today_is_live():
    context = build_analysis_context("2026-07-20", today=date(2026, 7, 20))
    assert context.mode is AnalysisMode.LIVE


def test_future_date_is_rejected():
    with pytest.raises(ValueError, match="cannot be in the future"):
        build_analysis_context("2026-07-21", today=date(2026, 7, 20))


def test_context_is_restored_after_scope():
    assert get_analysis_context() is None
    with use_analysis_context("2026-07-19", today=date(2026, 7, 20)):
        assert get_analysis_context().mode is AnalysisMode.HISTORICAL
        message = historical_unavailable("StockTwits")
        assert "StockTwits" in message
        assert "2026-07-19" in message
        assert "do not treat missing data as neutral" in message
    assert get_analysis_context() is None


def test_initial_state_exposes_temporal_contract():
    state = Propagator().create_initial_state("NVDA", "2020-01-02")
    assert state["as_of_date"] == "2020-01-02"
    assert state["analysis_mode"] == "historical"
```

- [ ] **Step 2: Run the tests and verify the missing-module failure**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_temporal_context.py -q`

Expected: collection fails because `tradingagents.dataflows.temporal` does not exist.

- [ ] **Step 3: Add the temporal implementation**

```python
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Iterator


class AnalysisMode(str, Enum):
    LIVE = "live"
    HISTORICAL = "historical"


@dataclass(frozen=True)
class AnalysisContext:
    as_of_date: date
    mode: AnalysisMode


_CURRENT_CONTEXT: ContextVar[AnalysisContext | None] = ContextVar(
    "tradingagents_analysis_context", default=None
)


def build_analysis_context(as_of_date: str | date, today: date | None = None) -> AnalysisContext:
    parsed = as_of_date if isinstance(as_of_date, date) else date.fromisoformat(str(as_of_date))
    current = today or date.today()
    if parsed > current:
        raise ValueError(f"analysis date {parsed.isoformat()} cannot be in the future")
    mode = AnalysisMode.HISTORICAL if parsed < current else AnalysisMode.LIVE
    return AnalysisContext(parsed, mode)


@contextmanager
def use_analysis_context(
    as_of_date: str | date, today: date | None = None
) -> Iterator[AnalysisContext]:
    context = build_analysis_context(as_of_date, today=today)
    token = _CURRENT_CONTEXT.set(context)
    try:
        yield context
    finally:
        _CURRENT_CONTEXT.reset(token)


def get_analysis_context() -> AnalysisContext | None:
    return _CURRENT_CONTEXT.get()


def is_historical(as_of_date: str | date | None = None) -> bool:
    context = build_analysis_context(as_of_date) if as_of_date is not None else get_analysis_context()
    return bool(context and context.mode is AnalysisMode.HISTORICAL)


def historical_unavailable(source: str, as_of_date: str | date | None = None) -> str | None:
    context = build_analysis_context(as_of_date) if as_of_date is not None else get_analysis_context()
    if context is None or context.mode is AnalysisMode.LIVE:
        return None
    cutoff = context.as_of_date.isoformat()
    return (
        f"DATA_UNAVAILABLE: {source} has no point-in-time archive for historical "
        f"analysis as of {cutoff}. Proceed without it; do not treat missing data "
        "as neutral and do not fabricate values."
    )
```

Add `as_of_date` and `analysis_mode` annotations to `AgentState`. In `Propagator.create_initial_state`, call `build_analysis_context(trade_date)` and populate both fields from the result.

- [ ] **Step 4: Run the focused tests**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_temporal_context.py -q`

Expected: all temporal-context tests pass.

- [ ] **Step 5: Commit Task 1**

```bash
git add tradingagents/dataflows/temporal.py tradingagents/agents/utils/agent_states.py tradingagents/graph/propagation.py tests/test_temporal_context.py
git commit -m "feat: add analysis temporal context"
```

---

### Task 2: Disable current-only sources in historical mode

**Files:**
- Modify: `tradingagents/dataflows/stocktwits.py`
- Modify: `tradingagents/dataflows/reddit.py`
- Modify: `tradingagents/dataflows/polymarket.py`
- Modify: `tradingagents/agents/analysts/sentiment_analyst.py`
- Modify: `tests/test_stocktwits_resilience.py`
- Modify: `tests/test_reddit_fallback.py`
- Modify: `tests/test_polymarket.py`

**Interfaces:**
- `fetch_stocktwits_messages(..., as_of_date: str | None = None) -> str`
- `fetch_reddit_posts(..., as_of_date: str | None = None) -> str`
- `get_prediction_markets(...)` uses the request context because the LangChain tool does not receive the graph date directly.

- [ ] **Step 1: Add failing tests proving historical calls perform no network I/O**

```python
def test_historical_stocktwits_is_unavailable_without_network(monkeypatch):
    def fail_network(*args, **kwargs):
        raise AssertionError("network must not be called")
    monkeypatch.setattr("tradingagents.dataflows.stocktwits.urlopen", fail_network)
    result = fetch_stocktwits_messages("NVDA", as_of_date="2020-01-02")
    assert result.startswith("DATA_UNAVAILABLE:")
    assert "StockTwits" in result


def test_historical_reddit_is_unavailable_without_sleep_or_network(monkeypatch):
    def fail_network(*args, **kwargs):
        raise AssertionError("network must not be called")
    monkeypatch.setattr("tradingagents.dataflows.reddit._fetch_subreddit", fail_network)
    result = fetch_reddit_posts(
        "NVDA", as_of_date="2020-01-02", inter_request_delay=0
    )
    assert result.startswith("DATA_UNAVAILABLE:")
    assert "Reddit" in result


def test_historical_polymarket_uses_request_context(monkeypatch):
    monkeypatch.setattr("tradingagents.dataflows.polymarket._fetch_markets", lambda: (_ for _ in ()).throw(
        AssertionError("network must not be called")
    ))
    with use_analysis_context("2020-01-02"):
        result = get_prediction_markets("recession")
    assert result.startswith("DATA_UNAVAILABLE:")
    assert "Polymarket" in result
```

- [ ] **Step 2: Verify the tests fail because current-only sources still execute**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_stocktwits_resilience.py tests/test_reddit_fallback.py tests/test_polymarket.py -q`

Expected: the new historical tests fail by reaching the patched network functions.

- [ ] **Step 3: Add the source gates**

At the beginning of each public fetcher, call:

```python
unavailable = historical_unavailable("StockTwits", as_of_date)
if unavailable:
    return unavailable
```

Use source names `Reddit` and `Polymarket live prediction markets` in their respective modules. In the sentiment analyst, pass `current_date` to both social fetchers:

```python
stocktwits_block = fetch_stocktwits_messages(ticker, limit=30, as_of_date=current_date)
reddit_block = fetch_reddit_posts(ticker, as_of_date=current_date)
```

- [ ] **Step 4: Run focused source tests**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_stocktwits_resilience.py tests/test_reddit_fallback.py tests/test_polymarket.py tests/test_structured_agents.py -q`

Expected: all selected tests pass.

- [ ] **Step 5: Commit Task 2**

```bash
git add tradingagents/dataflows/stocktwits.py tradingagents/dataflows/reddit.py tradingagents/dataflows/polymarket.py tradingagents/agents/analysts/sentiment_analyst.py tests/test_stocktwits_resilience.py tests/test_reddit_fallback.py tests/test_polymarket.py
git commit -m "fix: block live social data in historical runs"
```

---

### Task 3: Point-in-time fundamentals and macro vintages

**Files:**
- Modify: `tradingagents/dataflows/y_finance.py`
- Modify: `tradingagents/dataflows/alpha_vantage_fundamentals.py`
- Modify: `tradingagents/dataflows/fred.py`
- Create: `tests/test_temporal_fundamentals.py`
- Modify: `tests/test_fred.py`

**Interfaces:**
- Historical fundamental calls return the standard `DATA_UNAVAILABLE` sentinel before vendor I/O.
- FRED observation requests include `realtime_start=curr_date` and `realtime_end=curr_date`.

- [ ] **Step 1: Add failing vendor-boundary tests**

```python
def test_yfinance_overview_is_unavailable_historically(monkeypatch):
    monkeypatch.setattr("tradingagents.dataflows.y_finance.yf.Ticker", lambda *_: (_ for _ in ()).throw(
        AssertionError("yfinance must not be called")
    ))
    result = y_finance.get_fundamentals("NVDA", "2020-01-02")
    assert result.startswith("DATA_UNAVAILABLE:")


def test_alpha_vantage_statements_are_unavailable_historically(monkeypatch):
    monkeypatch.setattr(
        "tradingagents.dataflows.alpha_vantage_fundamentals._make_api_request",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("API must not be called")),
    )
    result = alpha_vantage_fundamentals.get_balance_sheet("NVDA", curr_date="2020-01-02")
    assert result.startswith("DATA_UNAVAILABLE:")


def test_fred_observations_request_vintage_at_cutoff(monkeypatch):
    calls = []
    monkeypatch.setattr(fred, "_request", lambda path, params: calls.append((path, params.copy())) or (
        {"seriess": [{"title": "CPI", "units": "Index", "frequency": "Monthly"}]}
        if path == "series" else {"observations": [{"date": "2020-01-01", "value": "100"}]}
    ))
    fred.get_macro_data("cpi", "2020-01-31")
    params = next(params for path, params in calls if path == "series/observations")
    assert params["realtime_start"] == "2020-01-31"
    assert params["realtime_end"] == "2020-01-31"
```

- [ ] **Step 2: Run and verify failures at the vendor boundary**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_temporal_fundamentals.py tests/test_fred.py -q`

Expected: historical vendor calls reach the patched clients and FRED lacks real-time parameters.

- [ ] **Step 3: Gate all non-point-in-time fundamental endpoints**

Use `historical_unavailable` before network calls in Yahoo overview, balance sheet, cash flow, income statement, and insider transactions. Do the same for Alpha Vantage overview and statements. Functions that already receive `curr_date` pass it explicitly; insider functions use the request context.

Add the vintage fields to the FRED observation request:

```python
{
    "series_id": series_id,
    "observation_start": start_date,
    "observation_end": curr_date,
    "realtime_start": curr_date,
    "realtime_end": curr_date,
    "sort_order": "asc",
}
```

- [ ] **Step 4: Run fundamental, FRED, and routing tests**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_temporal_fundamentals.py tests/test_fred.py tests/test_alpha_vantage_hardening.py tests/test_date_boundaries.py tests/test_interface.py -q`

Expected: all selected tests pass.

- [ ] **Step 5: Commit Task 3**

```bash
git add tradingagents/dataflows/y_finance.py tradingagents/dataflows/alpha_vantage_fundamentals.py tradingagents/dataflows/fred.py tests/test_temporal_fundamentals.py tests/test_fred.py
git commit -m "fix: enforce point-in-time source boundaries"
```

---

### Task 4: Chronological memory context

**Files:**
- Modify: `tradingagents/agents/utils/memory.py`
- Modify: `tradingagents/graph/trading_graph.py`
- Modify: `tests/test_memory_log.py`

**Interfaces:**
- `TradingMemoryLog.get_past_context(ticker, n_same=5, n_cross=3, as_of_date=None) -> str`
- Historical context includes only resolved entries with a parseable date strictly before `as_of_date`.

- [ ] **Step 1: Add failing chronology tests**

```python
def test_past_context_excludes_same_day_and_future_entries(tmp_path):
    log = make_log(tmp_path)
    _seed_completed(tmp_path, "NVDA", "2026-01-05", "Old decision.", "Old lesson.")
    _seed_completed(tmp_path, "NVDA", "2026-01-10", "Same-day decision.", "Same-day lesson.")
    _seed_completed(tmp_path, "AAPL", "2026-01-12", "Future decision.", "Future lesson.")
    context = log.get_past_context("NVDA", as_of_date="2026-01-10")
    assert "Old lesson" in context
    assert "Same-day lesson" not in context
    assert "Future lesson" not in context


def test_historical_context_excludes_malformed_legacy_date(tmp_path):
    log = make_log(tmp_path)
    _seed_completed(tmp_path, "NVDA", "unknown-date", "Legacy decision.", "Legacy lesson.")
    assert log.get_past_context("NVDA", as_of_date="2026-01-10") == ""
```

- [ ] **Step 2: Run and verify both tests fail**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_memory_log.py -k "past_context_excludes" -q`

Expected: same-day, future, or malformed entries appear in context.

- [ ] **Step 3: Filter entries before same/cross selection**

```python
cutoff = date.fromisoformat(str(as_of_date)) if as_of_date is not None else None
entries = [entry for entry in self.load_entries() if not entry.get("pending")]
if cutoff is not None:
    chronological = []
    for entry in entries:
        try:
            entry_date = date.fromisoformat(entry["date"])
        except (TypeError, ValueError):
            continue
        if entry_date < cutoff:
            chronological.append(entry)
    entries = chronological
```

Pass `trade_date` from the graph when building memory context:

```python
past_context = self.memory_log.get_past_context(company_name, as_of_date=str(trade_date))
```

- [ ] **Step 4: Run all memory tests**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_memory_log.py -q`

Expected: all memory tests pass.

- [ ] **Step 5: Commit Task 4**

```bash
git add tradingagents/agents/utils/memory.py tradingagents/graph/trading_graph.py tests/test_memory_log.py
git commit -m "fix: keep historical memory chronological"
```

---

### Task 5: Executable, calendar-aligned outcome labels

**Files:**
- Modify: `tradingagents/default_config.py`
- Modify: `tradingagents/graph/trading_graph.py`
- Modify: `tradingagents/graph/reflection.py`
- Modify: `tradingagents/agents/utils/memory.py`
- Modify: `tests/test_memory_log.py`
- Modify: `tests/test_symbol_normalization_paths.py`

**Interfaces:**
- Add immutable `RealizedOutcome` with `raw_return`, `benchmark_return`, `excess_return`, `holding_days`, `entry_date`, `exit_date`, and `return_basis="gross"`.
- `_fetch_returns(..., available_through=None) -> RealizedOutcome | None`.
- Add config key `outcome_holding_days`, default `5`, and environment override `TRADINGAGENTS_OUTCOME_HOLDING_DAYS`.

- [ ] **Step 1: Replace tuple-oriented tests with failing execution tests**

```python
def test_fetch_returns_enters_next_common_open_and_exits_fifth_close():
    stock = pd.DataFrame(
        {"Open": [100, 101, 102, 103, 104], "Close": [101, 102, 103, 104, 110]},
        index=pd.to_datetime(["2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09", "2026-01-12"]),
    )
    benchmark = pd.DataFrame(
        {"Open": [200, 201, 202, 203, 204], "Close": [201, 202, 203, 204, 210]},
        index=stock.index,
    )
    outcome = _fetch_with_frames(stock, benchmark, signal_date="2026-01-05", holding_days=5)
    assert outcome.entry_date == "2026-01-06"
    assert outcome.exit_date == "2026-01-12"
    assert outcome.raw_return == pytest.approx(0.10)
    assert outcome.benchmark_return == pytest.approx(0.05)
    assert outcome.excess_return == pytest.approx(0.05)


def test_fetch_returns_inner_aligns_mismatched_calendars():
    stock = _ohlc_frame(["2026-01-06", "2026-01-07", "2026-01-08"], 100)
    benchmark = _ohlc_frame(["2026-01-06", "2026-01-08", "2026-01-09"], 200)
    outcome = _fetch_with_frames(stock, benchmark, signal_date="2026-01-05", holding_days=2)
    assert outcome.entry_date == "2026-01-06"
    assert outcome.exit_date == "2026-01-08"


def test_pending_outcome_cannot_read_beyond_current_run_date(tmp_path):
    log = make_log(tmp_path)
    log.store_decision("NVDA", "2026-01-05", DECISION_BUY)
    graph = MagicMock(spec=TradingAgentsGraph)
    graph.memory_log = log
    graph.config = {"outcome_holding_days": 5}
    graph._resolve_benchmark.return_value = "SPY"
    graph._fetch_returns.return_value = None
    TradingAgentsGraph._resolve_pending_entries(
        graph, "NVDA", as_of_date="2026-01-07"
    )
    graph._fetch_returns.assert_called_once_with(
        "NVDA", "2026-01-05", holding_days=5, benchmark="SPY", available_through="2026-01-07"
    )
```

Add this concrete helper beside the return tests:

```python
def _fetch_with_frames(stock, benchmark, signal_date, holding_days):
    graph = MagicMock(spec=TradingAgentsGraph)
    with patch("yfinance.Ticker") as ticker_class:
        def make_ticker(symbol):
            ticker = MagicMock()
            ticker.history.return_value = benchmark if symbol == "SPY" else stock
            return ticker
        ticker_class.side_effect = make_ticker
        return TradingAgentsGraph._fetch_returns(
            graph,
            "NVDA",
            signal_date,
            holding_days=holding_days,
            benchmark="SPY",
            available_through="2026-12-31",
        )


def _ohlc_frame(session_dates, base):
    count = len(session_dates)
    return pd.DataFrame(
        {
            "Open": [base + index for index in range(count)],
            "Close": [base + index + 1 for index in range(count)],
        },
        index=pd.to_datetime(session_dates),
    )
```

- [ ] **Step 2: Run and verify failures reflect the old close-to-close tuple implementation**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_memory_log.py tests/test_symbol_normalization_paths.py -q`

Expected: new tests fail because `_fetch_returns` returns a tuple, uses row positions, and starts at the signal-date close.

- [ ] **Step 3: Implement the label object and aligned calculation**

The core calculation must normalize both indexes, inner-join them, and use adjusted OHLC consistently:

```python
signal = datetime.strptime(trade_date, "%Y-%m-%d")
fetch_start = (signal + timedelta(days=1)).strftime("%Y-%m-%d")
buffered_end = signal + timedelta(days=holding_days * 3 + 7)
if available_through is not None:
    cutoff = datetime.strptime(available_through, "%Y-%m-%d")
    buffered_end = min(buffered_end, cutoff + timedelta(days=1))
fetch_end = buffered_end.strftime("%Y-%m-%d")

stock = yf.Ticker(normalize_symbol(ticker)).history(
    start=fetch_start, end=fetch_end, auto_adjust=True, actions=False
)
bench = yf.Ticker(benchmark).history(
    start=fetch_start, end=fetch_end, auto_adjust=True, actions=False
)

stock_prices = stock[["Open", "Close"]].rename(
    columns={"Open": "stock_open", "Close": "stock_close"}
)
benchmark_prices = bench[["Open", "Close"]].rename(
    columns={"Open": "benchmark_open", "Close": "benchmark_close"}
)
stock_prices.index = pd.to_datetime(stock_prices.index, utc=True).tz_localize(None).normalize()
benchmark_prices.index = pd.to_datetime(benchmark_prices.index, utc=True).tz_localize(None).normalize()
common = stock_prices.join(benchmark_prices, how="inner").dropna()
common = common[common.index > pd.Timestamp(signal_date)]
if available_through is not None:
    common = common[common.index <= pd.Timestamp(available_through)]
if len(common) < holding_days:
    return None
window = common.iloc[:holding_days]
entry = window.iloc[0]
exit_row = window.iloc[-1]
raw_return = float(exit_row["stock_close"] / entry["stock_open"] - 1)
benchmark_return = float(exit_row["benchmark_close"] / entry["benchmark_open"] - 1)
return RealizedOutcome(
    raw_return=raw_return,
    benchmark_return=benchmark_return,
    excess_return=raw_return - benchmark_return,
    holding_days=holding_days,
    entry_date=window.index[0].date().isoformat(),
    exit_date=window.index[-1].date().isoformat(),
)
```

Use `self.config["outcome_holding_days"]` when resolving pending entries. Pass the current run's analysis date as `available_through`. Extend new memory tags with the execution window and `gross`, while keeping `_parse_entry` tolerant of old six-field tags. Change reflection prompt labels from `Alpha vs {benchmark}` to `Excess return vs {benchmark}`.

- [ ] **Step 4: Run outcome, memory, symbol, and reflection tests**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_memory_log.py tests/test_symbol_normalization_paths.py tests/test_signal_processing.py -q`

Expected: all selected tests pass.

- [ ] **Step 5: Commit Task 5**

```bash
git add tradingagents/default_config.py tradingagents/graph/trading_graph.py tradingagents/graph/reflection.py tradingagents/agents/utils/memory.py tests/test_memory_log.py tests/test_symbol_normalization_paths.py
git commit -m "fix: measure executable benchmark-relative outcomes"
```

---

### Task 6: Remove fabricated target and confidence fallbacks

**Files:**
- Modify: `cli/main.py`
- Create: `tests/test_target_profile.py`
- Modify: `tests/test_consolidated_report_formatting.py`

**Interfaces:**
- `estimate_target_profile` deterministically extracts only an explicit Portfolio Manager target and horizon.
- `confidence_score` is always `None` until empirical calibration exists.
- `format_price_target(value, currency=None)` omits a currency prefix when currency is unknown.

- [ ] **Step 1: Add failing target-integrity tests**

```python
def test_missing_target_and_confidence_remain_none():
    final_state = {"final_trade_decision": "**Rating**: Hold\n\n**Executive Summary**: Wait."}
    profile = estimate_target_profile(None, "NVDA", "2026-07-20", final_state, "Hold")
    assert profile["price_target"] is None
    assert profile["confidence_score"] is None


def test_explicit_structured_target_is_extracted_without_llm_call():
    llm = MagicMock()
    final_state = {
        "final_trade_decision": (
            "**Rating**: Buy\n\n**Price Target**: 145.5\n\n**Time Horizon**: 3 months"
        )
    }
    profile = estimate_target_profile(llm, "NVDA", "2026-07-20", final_state, "Buy")
    assert profile["price_target"] == 145.5
    assert profile["target_horizon"] == "3 months"
    assert profile["confidence_score"] is None
    llm.invoke.assert_not_called()


def test_unknown_currency_does_not_render_usd_symbol():
    assert format_price_target(145.5) == "145.50"
    assert format_price_target(145.5, currency="USD") == "$145.50"
```

- [ ] **Step 2: Run and verify the old fallback and LLM call fail the tests**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_target_profile.py tests/test_consolidated_report_formatting.py -q`

Expected: missing values become current price and 50, the LLM is called, and unknown currency renders `$`.

- [ ] **Step 3: Replace probabilistic generation with deterministic extraction**

Extract `**Price Target**` and `**Time Horizon**` from `final_trade_decision` with anchored, case-insensitive regular expressions. Keep `reference_price` extraction from the verified market snapshot. Return this exact shape:

```python
return {
    "price_target": price_target,
    "confidence_score": None,
    "target_horizon": target_horizon,
    "target_summary": "Explicit Portfolio Manager target." if price_target is not None else None,
    "reference_price": current_price,
}
```

Update Markdown and HTML headings from `Confidence` to `Model confidence (uncalibrated)`. Render `None` as `-`. Add the optional currency argument to `format_price_target`; use `$` only for `USD`, the ISO code plus a space for other known currencies, and no prefix when unknown.

- [ ] **Step 4: Run report and target tests**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_target_profile.py tests/test_consolidated_report_formatting.py tests/test_web_service.py -q`

Expected: all selected tests pass.

- [ ] **Step 5: Commit Task 6**

```bash
git add cli/main.py tests/test_target_profile.py tests/test_consolidated_report_formatting.py
git commit -m "fix: stop presenting invented target confidence"
```

---

### Task 7: Unify invoke and streaming lifecycle

**Files:**
- Modify: `tradingagents/graph/trading_graph.py`
- Modify: `cli/main.py`
- Modify: `tradingagents/web/service.py`
- Create: `tests/test_graph_execution_lifecycle.py`
- Modify: `tests/test_web_service.py`

**Interfaces:**
- `TradingAgentsGraph.stream(company_name, trade_date, asset_type="stock", callbacks=None)` yields graph value chunks.
- `_execution_scope(...)` owns temporal context, pending resolution, checkpoint setup, chronological memory, instrument identity, initial state, and cleanup.
- `_complete_run(...)` logs and stores only a completed final state.

- [ ] **Step 1: Add failing lifecycle tests**

```python
def test_stream_injects_memory_and_instrument_context():
    graph = make_lifecycle_graph()
    graph.memory_log.get_past_context.return_value = "chronological lesson"
    chunks = list(graph.stream("NVDA", "2026-01-10", callbacks=[object()]))
    assert chunks[-1]["final_trade_decision"] == "Buy"
    graph.memory_log.get_past_context.assert_called_once_with(
        "NVDA", as_of_date="2026-01-10"
    )
    initial = graph.graph.stream.call_args.args[0]
    assert initial["past_context"] == "chronological lesson"
    assert initial["instrument_context"] == "NVDA identity"


def test_completed_stream_logs_and_stores_decision():
    graph = make_lifecycle_graph()
    list(graph.stream("NVDA", "2026-01-10"))
    graph.memory_log.store_decision.assert_called_once()
    assert graph.curr_state["final_trade_decision"] == "Buy"


def test_interrupted_stream_does_not_store_decision():
    graph = make_lifecycle_graph(two_chunks=True)
    iterator = graph.stream("NVDA", "2026-01-10")
    next(iterator)
    iterator.close()
    graph.memory_log.store_decision.assert_not_called()


def test_temporal_context_is_cleared_after_stream_exception():
    graph = make_lifecycle_graph(stream_error=RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        list(graph.stream("NVDA", "2026-01-10"))
    assert get_analysis_context() is None
```

- [ ] **Step 2: Run and verify `TradingAgentsGraph.stream` is missing**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_graph_execution_lifecycle.py tests/test_web_service.py -q`

Expected: lifecycle tests fail because the public graph-owned stream method does not exist.

- [ ] **Step 3: Extract the shared execution scope and completion hook**

The shared scope must have this shape:

```python
@contextmanager
def _execution_scope(self, company_name, trade_date, asset_type, callbacks=None):
    with use_analysis_context(trade_date):
        self.ticker = company_name
        self._resolve_pending_entries(company_name, as_of_date=str(trade_date))
        checkpoint_context = None
        try:
            if self.config.get("checkpoint_enabled"):
                checkpoint_context = get_checkpointer(self.config["data_cache_dir"], company_name)
                saver = checkpoint_context.__enter__()
                self.graph = self.workflow.compile(checkpointer=saver)
            past_context = self.memory_log.get_past_context(
                company_name, as_of_date=str(trade_date)
            )
            instrument_context = self.resolve_instrument_context(company_name, asset_type)
            initial_state = self.propagator.create_initial_state(
                company_name,
                trade_date,
                asset_type=asset_type,
                past_context=past_context,
                instrument_context=instrument_context,
            )
            args = self.propagator.get_graph_args(callbacks=callbacks)
            if self.config.get("checkpoint_enabled"):
                run_thread_id = thread_id(
                    company_name, str(trade_date), self._run_signature(asset_type)
                )
                args.setdefault("config", {}).setdefault("configurable", {})[
                    "thread_id"
                ] = run_thread_id
            yield initial_state, args
        finally:
            if checkpoint_context is not None:
                checkpoint_context.__exit__(None, None, None)
                self.graph = self.workflow.compile()
```

Preserve the existing `checkpoint_step` lookup and resume/start log message immediately after compiling with the saver. `stream` accumulates the most recent value chunk, yields every chunk to the caller, and calls `_complete_run` only after normal iterator exhaustion. `propagate` uses the same scope with `graph.invoke` and the same completion hook.

Use this completion hook for both paths:

```python
def _complete_run(self, company_name, trade_date, asset_type, final_state):
    self.curr_state = final_state
    self._log_state(trade_date, final_state)
    self.memory_log.store_decision(
        ticker=company_name,
        trade_date=str(trade_date),
        final_trade_decision=final_state["final_trade_decision"],
    )
    if self.config.get("checkpoint_enabled"):
        clear_checkpoint(
            self.config["data_cache_dir"],
            company_name,
            str(trade_date),
            self._run_signature(asset_type),
        )
```

Replace both direct `graph.graph.stream(...)` loop sources with the graph-owned stream. The web loop becomes:

```python
for chunk in graph.stream(
    ticker,
    analysis_date,
    asset_type=asset_type,
    callbacks=[stats_handler],
):
    messages = chunk.get("messages") or []
    if messages:
        tracker.process_message(messages[-1])
    tracker.update_analysts(chunk)
    tracker.update_research(chunk)
    tracker.update_trader(chunk)
    tracker.update_risk(chunk)
    trace.append(chunk)
```

The CLI loop uses the same `graph.stream(...)` call and retains its existing message, tool-call, analyst-status, and debate rendering body. Only graph preparation and completion move into `TradingAgentsGraph`.

- [ ] **Step 4: Run lifecycle, CLI, checkpoint, and web tests**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_graph_execution_lifecycle.py tests/test_web_service.py tests/test_checkpoint_resume.py tests/test_checkpoint_signature.py tests/test_memory_log.py -q`

Expected: all selected tests pass when `langgraph-checkpoint-sqlite` is installed. If it is missing, install the declared project dependency before interpreting checkpoint failures.

- [ ] **Step 5: Commit Task 7**

```bash
git add tradingagents/graph/trading_graph.py cli/main.py tradingagents/web/service.py tests/test_graph_execution_lifecycle.py tests/test_web_service.py
git commit -m "refactor: unify graph execution lifecycle"
```

---

### Task 8: Documentation and complete verification

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-07-20-stock-analysis-outcome-integrity-design.md` only if implementation reveals a factual mismatch.

**Interfaces:**
- Users can tell which sources are unavailable historically, how outcomes are measured, and that confidence is uncalibrated.

- [ ] **Step 1: Add README assertions to an existing documentation test or create `tests/test_readme_temporal_integrity.py`**

```python
from pathlib import Path


def test_readme_documents_historical_data_boundaries():
    readme = Path("README.md").read_text(encoding="utf-8")
    assert "Historical analysis boundaries" in readme
    assert "StockTwits, Reddit, and Polymarket are unavailable" in readme
    assert "next common trading session's adjusted open" in readme
    assert "Model confidence is not a calibrated probability" in readme
```

- [ ] **Step 2: Run and verify the documentation test fails**

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_readme_temporal_integrity.py -q`

Expected: failure because the README lacks the new section.

- [ ] **Step 3: Add the exact user-facing documentation**

Add a `Historical analysis boundaries` section explaining:

- Dated news, OHLCV, indicators, and FRED vintages use the requested cutoff.
- StockTwits, Reddit, Polymarket, current company overviews, insider feeds, and statements without publication timestamps are unavailable historically.
- Memory is filtered strictly before the analysis date.
- Deferred outcomes enter at the next common adjusted open and exit after the configured common-session horizon.
- Benchmark difference is a gross excess return before costs, not risk-adjusted alpha.
- Model confidence is not a calibrated probability and is omitted until calibration data exists.

- [ ] **Step 4: Run formatting and focused regression suites**

Run: `/Users/slava/.pyenv/shims/python -m ruff check tradingagents cli tests`

Expected: exit code 0 with no lint findings.

Run: `/Users/slava/.pyenv/shims/python -m pytest tests/test_temporal_context.py tests/test_temporal_fundamentals.py tests/test_fred.py tests/test_stocktwits_resilience.py tests/test_reddit_fallback.py tests/test_polymarket.py tests/test_memory_log.py tests/test_symbol_normalization_paths.py tests/test_target_profile.py tests/test_graph_execution_lifecycle.py tests/test_web_service.py tests/test_consolidated_report_formatting.py tests/test_readme_temporal_integrity.py -q`

Expected: all selected tests pass.

- [ ] **Step 5: Run the complete test suite**

Run: `/Users/slava/.pyenv/shims/python -m pytest -q`

Expected: exit code 0. The two existing environment-gated tests may skip. Missing `langgraph-checkpoint-sqlite` is an environment defect because it is a declared dependency; install the project dependencies and rerun instead of accepting checkpoint failures.

- [ ] **Step 6: Review the final diff against the design**

Run: `git diff c11512a --check`

Expected: exit code 0 and no whitespace errors.

Run: `git status --short`

Expected: only the files listed in this plan are modified or newly added.

- [ ] **Step 7: Commit Task 8**

```bash
git add README.md tests/test_readme_temporal_integrity.py docs/superpowers/specs/2026-07-20-stock-analysis-outcome-integrity-design.md
git commit -m "docs: explain historical analysis boundaries"
```
