# Stock Analysis Outcome Integrity Design

## Goal

Make historical analyses temporally honest and make their realized outcomes comparable to trades that could actually have been executed. This release is the prerequisite for later forecast calibration, source ablation, portfolio optimization, and paid-data integration.

## Scope

This release covers five related correctness gaps:

1. Historical runs must not consume current-only social, prediction-market, fundamental, macro, or memory information.
2. Every run must carry an explicit analysis mode and `as_of` date through the data-access layer.
3. Deferred outcome measurement must enter after the signal and align stock and benchmark sessions by date.
4. Unavailable price targets and confidence values must remain unavailable rather than being replaced with invented neutral values.
5. Web and CLI execution must share the graph's pre-run and post-run lifecycle.

This release does not add a new commercial data vendor, train a forecasting model, implement a portfolio optimizer, or claim improved investment performance. Those depend on the trustworthy observations and labels created here.

## Analysis Modes and Temporal Contract

The graph derives an analysis mode from the requested date:

- `live`: the requested date is the host's current local date. Current-only sources may run.
- `historical`: the requested date is earlier than the host's current local date. Every source must either enforce that cutoff or report that historical data is unavailable.

The initial graph state will include an `as_of_date` and `analysis_mode`. Data tools continue to receive the existing date argument where they already support it. Current-only tools receive the mode explicitly through the shared dataflow configuration so they cannot silently substitute today's data.

Source behavior in historical mode:

- Yahoo and Alpha Vantage news retain their existing publication-date filtering.
- StockTwits and Reddit return an explicit unavailable message because the current integrations do not provide historical archives.
- Polymarket returns an explicit unavailable message because the current integration reads live market state.
- Yahoo `info` and Alpha Vantage company overview return an explicit unavailable message. Historical statements remain usable only when their public availability timestamp is known; fiscal-period-end filtering alone is insufficient.
- FRED must request a real-time vintage bounded by the analysis date. If the vendor cannot provide it, the result is unavailable rather than silently revised.
- OHLCV and technical indicators retain date filtering. The data contract records that adjusted historical prices may be restated by later corporate actions; solving full corporate-action point-in-time storage belongs to the snapshot-store project.

Unavailable data is a first-class result. Analysts must distinguish missing evidence from neutral evidence and must not infer a neutral signal merely because a source is absent.

## Chronological Memory

`TradingMemoryLog.get_past_context` will accept an `as_of_date` parameter. It will include only resolved entries whose trade date is strictly earlier than the requested analysis date. Pending entries remain excluded.

Pending outcomes will only be resolved through prices available on or before the current run date. A historical replay must not resolve or inject outcomes from later decisions. Live-learning memory and chronological replay therefore share the same file format but obey a strict date boundary.

If an old entry has no parseable date, it is excluded from historical context and may remain available to a live run for backward compatibility.

## Executable Outcome Labels

The realized-return calculation will use a clear signal/execution convention:

- The analysis uses information through the requested session.
- Entry is the next common tradable session's vendor-adjusted open for both the stock and benchmark.
- Exit is the vendor-adjusted close of the Nth common session, where N is the configured holding horizon.
- Stock and benchmark frames are inner-joined by normalized session date before selecting entry and exit.
- The returned label records the signal date, entry date, exit date, requested horizon, and actual common-session count.

The first release keeps the existing default of five common trading sessions, exposed as `outcome_holding_days` in configuration. It does not attempt to parse the Portfolio Manager's free-form horizon. Multi-horizon labels will be a separate structured-forecast project.

The label remains a gross return because the application does not yet know portfolio size, venue, spread, commission, or borrow conditions. The stored record will explicitly identify it as gross and will not call the simple benchmark difference risk-adjusted alpha.

## Confidence and Price Targets

The post-processing LLM may still summarize an explicit target already present in the structured Portfolio Manager decision, but it may not manufacture a target or calibrated probability.

- Missing target becomes `null`, never current price.
- Missing confidence becomes `null`, never `50`.
- Display code renders missing values as unavailable.
- Existing numeric fields remain backward compatible for stored results.
- The label changes from probability-like `Confidence` to `Model confidence (uncalibrated)` until the walk-forward calibration project replaces it with an empirical probability.

The formatter must use the instrument's quote currency when known and otherwise omit a currency symbol. Hard-coded USD formatting is out of scope for this release unless the resolved instrument identity already exposes the currency without another network call.

## Unified Execution Lifecycle

`TradingAgentsGraph` will expose a streaming execution method that uses the same lifecycle as `propagate`:

1. Resolve eligible pending outcomes.
2. Establish checkpointing.
3. Build chronological memory context and deterministic instrument context.
4. Create the initial state.
5. Stream graph chunks while accumulating the final state.
6. Log the state and store the deferred decision.
7. Close checkpoint resources.

`propagate`, CLI streaming, and web streaming will call this shared implementation. Presentation callbacks remain entry-point-specific, but they will no longer construct blank graph state or bypass graph bookkeeping.

If streaming is interrupted, no completed decision is stored. Existing checkpoints remain available when checkpointing is enabled.

## Error Handling

- Historical unavailability is a normal typed/data result, not a transport exception.
- Invalid analysis dates fail before any source or LLM call.
- Vendor transport and authentication errors retain the existing explicit fallback behavior.
- FRED vintage parameters are tested at the request boundary.
- Data-source messages include source name and requested cutoff so the LLM cannot mistake current data for historical data.
- Outcome resolution returns no label when fewer than the required common sessions exist.

## Testing

Tests will be added before each behavior change and will prove:

- Current-only sources are callable in live mode and unavailable in historical mode.
- FRED receives the requested vintage boundary.
- Historical memory excludes later and same-day entries.
- Outcome returns use next-session open, common dates, and the configured holding horizon.
- Target and confidence fallbacks remain `None`.
- CLI and web use the shared graph lifecycle.
- Interrupted streaming does not store a completed decision.
- Existing live runs and stored result formats remain backward compatible.

Focused tests will cover each unit. Completion requires the full project test suite; optional dependency skips or failures will be reported explicitly rather than concealed.

## Follow-on Projects

After this release, implementation proceeds in this order:

1. Frozen snapshot store and walk-forward evaluation with costs, delistings, coverage metrics, and source ablations.
2. Structured multi-horizon analyst forecasts with evidence identifiers and missingness.
3. Regularized forecast aggregation and held-out probability calibration.
4. Portfolio-aware sizing with holdings, liquidity, covariance, factor, and turnover constraints.
5. Point-in-time SEC, estimate-revision, short-interest, transcript, and options sources, retained only when out-of-sample ablations show value.
