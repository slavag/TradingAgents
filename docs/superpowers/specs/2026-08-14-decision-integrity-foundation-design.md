# Decision Integrity Foundation Design

## Goal

Prevent unsupported target metrics and provider/schema failures from becoming
apparently valid portfolio recommendations, while preserving compatibility with
existing five-tier decisions and stored reports.

This is the first reliability subproject. It establishes a strict final-decision
boundary that later forecasting, evaluation, and portfolio-construction work can
reuse. It does not claim improved investment performance by itself.

## Scope

This release covers five connected correctness gaps:

1. Primary Portfolio Manager targets must satisfy the same deterministic
   evidence and direction checks as fallback-generated targets.
2. Target, horizon, confidence, rationale, and supporting quote must behave as
   one coherent optional bundle.
3. Structured-output failures must become explicit unavailable decisions rather
   than unchecked free text or an implicit Hold.
4. Insufficient directional evidence must be distinguishable from an intentional
   Hold through an explicit Abstain status.
5. CLI, web, memory, reports, and signal processing must interpret the same final
   decision statuses and target-validation results.

The release does not yet normalize forecast horizons into trading sessions,
calculate calibrated probabilities, implement walk-forward evaluation, or
construct target portfolio weights. Those belong to the immutable
`ForecastRecord` follow-on described below.

## Decision Contract

The provider-facing `PortfolioDecisionDraft` gains a `DecisionStatus` with three
values:

- `actionable`: the system has a valid five-tier rating.
- `abstain`: the available evidence cannot support a directional rating.
- `unavailable`: the final decision could not be produced or validated because
  of a provider, structured-output, or schema failure.

An actionable decision requires one of Buy, Overweight, Hold, Underweight, or
Sell. Abstain and unavailable decisions do not carry a five-tier rating. This
keeps an intentional neutral Hold distinct from missing evidence or a technical
failure.

The draft captures the model response, including a potentially partial optional
target proposal. A separate finalized `PortfolioDecision` is the domain object
used by rendering, signal processing, reports, and memory. Every finalized
decision retains a human-readable executive summary and investment thesis. For
unavailable decisions, those fields contain a concise failure explanation that
is safe to display and persist. Raw provider responses, credentials, and stack
traces are not copied into user-facing output.

## Target Bundle

The target bundle consists of:

- central-case price target;
- target time horizon;
- uncalibrated evidence-strength score;
- target rationale;
- verbatim supporting quote; and
- contextual validation status and optional rejection reason.

The provider draft permits a partial proposal so the system can preserve an
otherwise valid rating and record what was rejected. The finalized decision
guarantees that the five model-supplied target fields are either all present or
all absent. Finalization removes a partial bundle and records
`target_bundle_incomplete`; it does not turn a valid rating into an unavailable
decision. Nullish provider placeholders remain normalized to absence where the
current schema already supports that behavior.

The contextual validator accepts a complete bundle only when:

- the target is finite and greater than zero;
- the quote is non-empty, bounded in length, and appears verbatim in the
  completed analysis evidence after whitespace normalization;
- the quote contains the exact target number within numeric tolerance;
- the target number has nearby price-level or currency context;
- the number is not a percentage, date, ratio, volume, share count, or other
  non-price quantity; and
- when a verified reference price is available, Buy/Overweight targets are
  above it and Sell/Underweight targets are below it.

If contextual validation rejects a complete optional target bundle, the system
removes all target fields, preserves an otherwise valid rating, and records a
machine-readable rejection reason that is visible in the report. An unsupported
target therefore cannot contaminate the decision, and the rejection is not
silently hidden.

An actionable rating is allowed without a target bundle. The absence of a
defensible price target does not by itself prove that the directional rating is
invalid.

## Central Validation Boundary

A focused decision-integrity module will own decision finalization and contextual
target validation. It will consume a `PortfolioDecisionDraft`, the combined
completed evidence, and an optional verified reference price. It will return a
finalized `PortfolioDecision` containing either an accepted bundle or a stable
rejection reason.

The evidence input is assembled deterministically from the reports and plans
already present in graph state: market, sentiment, news, fundamentals, research
plan, trader proposal, risk debate, and eligible prior context. The validator
does not call an LLM or a data vendor.

The same module is used by:

- the Portfolio Manager immediately after primary structured output;
- CLI target-profile extraction;
- web target-profile extraction; and
- compatibility parsing of older rendered decisions.

The current fallback quote-validation implementation moves out of `cli/main.py`
into this shared module. There will be one definition of price context, numeric
tolerance, direction rules, and rejection reasons.

## Structured Invocation and Failure Policy

The Portfolio Manager requires structured output for new final decisions. Its
invocation path does not retry as unchecked free text.

- Successful draft-schema parsing continues through finalization and contextual
  validation.
- A provider that cannot bind structured output produces an unavailable
  decision.
- A null, malformed, or schema-invalid structured response produces an
  unavailable decision.
- A transport or provider exception produces an unavailable decision.
- A complete decision with an invalid optional target remains actionable after
  the target bundle is removed and annotated.

Research Manager and Trader fallback behavior is not changed in this release.
Their reliability can be tightened independently after the final decision
boundary is protected.

The unavailable renderer exposes a stable, sanitized reason code and concise
message. It does not expose exception representations that could contain vendor
details or secrets.

## Rendering and Backward Compatibility

New Portfolio Manager markdown includes an explicit `Decision Status` line.
Actionable output retains the existing `Rating`, `Executive Summary`, and
`Investment Thesis` headers. Accepted target bundles additionally render the
target, horizon, evidence-strength score, rationale, and supporting quote.
Rejected bundles render a target-validation note but no target metrics.

Signal processing follows these rules:

1. Explicit new statuses take precedence.
2. Actionable decisions return their five-tier rating.
3. Abstain and unavailable decisions return those explicit values.
4. Legacy saved text containing a recognizable five-tier rating remains
   readable.
5. New unparseable final decisions never default to Hold.

The general-purpose legacy `parse_rating` helper can retain its existing default
for callers that explicitly request legacy behavior. Final Portfolio Manager
processing uses a strict decision-signal parser with no implicit Hold default.

Memory persists actionable ratings, Abstain, and Unavailable distinctly. It
does not schedule outcome learning for Abstain or Unavailable because neither is
an executable directional forecast. Existing pending and resolved memory entries
remain readable.

## CLI and Web Parity

CLI and web execution use the same target-bundle parser and contextual validator.
Both surfaces display:

- decision status;
- five-tier rating when actionable;
- accepted target metrics and supporting evidence when available; and
- target rejection reason when a proposed bundle was removed.

Fallback extraction for older reports may still request missing metrics from an
LLM, but every returned bundle must include a supporting quote and pass the same
shared validation boundary. A fallback failure leaves metrics unavailable and
does not change the underlying decision status.

## Error Handling

Error outcomes are explicit and deterministic:

- `structured_binding_unsupported`: provider cannot bind the required schema.
- `structured_response_missing`: provider returned no parsed decision.
- `structured_response_invalid`: response failed schema validation.
- `structured_invocation_failed`: provider invocation failed.
- `target_bundle_incomplete`: model supplied only part of the target bundle.
- `target_not_positive_finite`: target is non-finite or not positive.
- `supporting_quote_missing`: quote is absent or empty.
- `supporting_quote_not_in_evidence`: quote is not present in supplied evidence.
- `supporting_quote_number_mismatch`: quote does not contain the target value.
- `supporting_quote_not_price_context`: matching number is not identified as a
  price level.
- `target_direction_conflict`: target contradicts the rating and verified
  reference price.

User-facing rendering may translate these codes into concise prose, but stored
and tested behavior uses the stable codes.

## Testing

Tests are written before each production change and prove:

- all valid and invalid status/rating combinations;
- complete, absent, and partial target bundles;
- nullish placeholder compatibility;
- exact-number provenance with price context;
- rejection of percentages, dates, ratios, volumes, shares, unrelated numbers,
  oversized quotes, and evidence not supplied to the run;
- bullish and bearish direction consistency when a reference price exists;
- invalid optional bundles preserve the rating while removing all target fields;
- binding, invocation, null-result, and schema failures produce Unavailable;
- explicit Abstain and intentional Hold remain distinguishable;
- strict final signal parsing never invents Hold;
- legacy five-tier reports remain readable;
- CLI and web produce the same target-validation result;
- memory does not schedule outcomes for Abstain or Unavailable; and
- existing actionable decisions and reports remain compatible.

Focused schema, integrity, Portfolio Manager, signal, target-profile, web, and
memory tests must pass. Completion requires the full project test suite, with
optional dependency skips and environmental failures reported explicitly.

## Future Option 3: Immutable Forecast Record

After the decision-integrity foundation is stable, the system will replace the
free-form forecast portions of `PortfolioDecision` with an immutable
`ForecastRecord`. This is deliberately retained in the roadmap rather than
being folded into Phase 1.

The record will contain:

- canonical instrument identifier and quote currency;
- analysis timestamp and point-in-time data cutoff;
- reference-price value, timestamp, adjustment basis, and vendor;
- normalized evaluation horizon in common trading sessions;
- expected absolute and benchmark-relative return;
- central estimate plus p10, p50, and p90 outcome levels;
- calibrated directional probabilities;
- target range, invalidation conditions, and evidence identifiers;
- missingness and data-quality indicators;
- model, provider, prompt, configuration, and source-snapshot hashes; and
- creation status, abstention reason, and validation results.

Subsequent projects will consume that record in this order:

1. Horizon-aligned point-in-time outcome resolution.
2. Deterministic direction, excess-return, target-error, Brier, calibration,
   drawdown, turnover, and cost scoring.
3. Walk-forward model and role leaderboards with promotion gates.
4. Holdings-, liquidity-, covariance-, exposure-, turnover-, and cost-aware
   portfolio optimization that converts forecasts into target weights.
5. Role-specific model catalogs and capability-aware UI controls, promoted by
   measured out-of-sample performance rather than model age alone.

Phase 1 validation types and rejection codes are designed to become fields on
`ForecastRecord`, so this foundation is migrated rather than discarded.

## Non-Goals

This release does not:

- claim calibrated confidence or improved financial returns;
- choose new default AI models;
- redesign debate topology;
- add market-data vendors;
- calculate transaction costs or slippage;
- infer current holdings or portfolio constraints;
- implement a portfolio optimizer; or
- change Research Manager or Trader structured-output fallback behavior.
