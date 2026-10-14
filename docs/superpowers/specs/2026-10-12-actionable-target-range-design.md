# Actionable Target or Target Range

## Goal and acceptance case

Every new actionable stock decision must include an evidence-backed numeric point target or target range, even when target confidence is low. The value must carry a stated horizon, method, source quote(s), and a concise account of risks. Confidence is an uncalibrated evidence-strength judgement, not a probability; no minimum confidence score is required to publish a validated target. If a defensible value still cannot be produced after one bounded repair, the run keeps its company research but withholds an actionable rating instead of inventing a number.

The October 12 PENG run `52ba0d6e110f49e8813c055a2ff1768d` is the regression case: it saved an actionable Hold with recommendation confidence 78/100, but all target fields were null. The validator marked `Not Proposed`, not `Rejected`, and the investor slide displayed “No validated price target.” Ten admissible verified price-level rows existed, including the 77.10 session high and 72.83 low. Those observed levels are candidate anchors, not automatically a forward target. Analyst estimates were unavailable; that does not preclude a clearly labeled, lower-confidence technical scenario if the Portfolio Manager can justify its horizon and endpoints.

## Selected approach

Extend the typed Portfolio Manager draft/final decision contract to express either:

1. a point target with its exact supporting price-context quote; or
2. a range with ordered low/high bounds, one exact supporting price-context quote for each bound, and an explicit method (for example, a technical scenario rather than intrinsic valuation).

Both shapes require a horizon, target confidence from 0 to 100, and a rationale that explains why those anchors are a plausible forward scenario rather than merely observed prices. The Portfolio Manager prompt should normally propose one shape for every actionable stock, including Hold; a Hold range may straddle the verified reference price. Technical scenarios must be labeled as such, not presented as fair value. Keep the existing recommendation confidence separate from target confidence. A price level is never silently promoted to a forecast solely because it appears in OHLCV, an indicator, or a debate transcript.

Validate each numeric value as positive, finite, as-of eligible, and traceable to an exact verified price-level quote. Check low ≤ high and direction against the reference price for directional Buy/Overweight and Underweight/Sell decisions; a Hold range may bracket the reference. Reject contradictory horizons, quote/number mismatches, or a range with only one supported endpoint. Require a bounded method/risk explanation and show those limits on the final slide. The validator is deterministic: it never fabricates a target, confidence, or horizon.

If the first structured PM draft omits or fails target validation, make at most one target-only structured repair request containing the unchanged rating/thesis, verified candidates, and validation reason. This repair may fill only the target bundle; it cannot revise the rating, position actions, company facts, or evidence. Validate the repaired bundle with the same deterministic gate. If it still fails, convert the would-be actionable decision to an explicit non-actionable Abstain with a sanitized target-unavailable reason. Do not loop or silently fall back to an arbitrary daily trading range. The extra request is made only in this failure path and is included in LLM usage accounting.

Alternatives rejected: forcing a single point target creates false precision; using a recent high/low or ATR band as an automatic “target range” turns observations or assumptions into a forecast; and adding a range only to the HTML slide would leave saved forecasts, CLI, and web results inconsistent. As a quality bar for investor communication, the method and risks must be visible alongside a target or range; this is not a claim that TradingAgents is a regulated research publisher. [FINRA research-rule reference](https://www.finra.org/sites/default/files/2019-10/2019_AR_Rules_Reference_Guide.pdf).

## Data flow and compatibility

- The typed PM schema distinguishes point and range bundles; it never accepts both at once. Final decision rendering keeps existing labels for point targets and adds explicit range/method/endpoint-source labels. Saved old reports remain immutable.
- `finalize_portfolio_decision` validates both bundles and returns a precise status/reason. The new PM execution boundary enforces the actionable-with-target rule and performs the one bounded repair. The legacy target-profile helper must not create a second contradictory decision.
- `ForecastRecord` already has `target_low`/`target_high`; populate them for an accepted range and allow Accepted with a complete range even if `central_target` is absent. Keep `expected_return` absent rather than inventing a midpoint. For point targets, retain the existing central-target behavior.
- CLI, web result JSON, consolidated/technical report labels, and the per-ticker investor `DecisionBlock` render the same validated point or range with target confidence, horizon, method, evidence, and risk note. No UI may show a rejected or unproposed number as a target. A non-actionable run shows no actionable rating/target but preserves its source-backed research deck.
- In source-only investor decks, deterministic facts and the validated target/range remain visible; optional deck narration is not an authority for either numeric shape.

## Verification and limits

Write RED→GREEN tests for low-confidence accepted point and range targets; Hold ranges bracketing the reference; directional conflicts; unsupported, post-date, reversed, or mixed-shape ranges; target-only repair preserving rating/actions; exactly one retry; repair failure becoming Abstain; forecast serialization; CLI/web/HTML consistency; and the saved PENG candidate set. Run full pytest, scoped Ruff, and `git diff --check`. No live PENG/SMXT or paid model call is required for deterministic verification; a fresh user-run PENG report is the final visual acceptance. Do not commit, push, or open a PR without separate authorization.
