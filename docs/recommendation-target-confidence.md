# Recommendation and target confidence

For completed research, the Portfolio Manager provides a five-tier recommendation and a numeric point target or range. Recommendation confidence and target confidence are separate uncalibrated evidence-strength scores from 0 to 100. There is no publication threshold: weak evidence belongs in the confidence, assumptions, and risk explanation.

This policy supersedes the target-failure-to-Abstain behavior in the October 12 actionable-target-range design. In the November 11 run `b115b4853ef945deb0057efa61253524`, MU, SNDK, and TSM lost both outputs after `target_shape_mixed` validation. ASML and GLW retained targets and independent confidence scores.

## Target basis

- **Source level** uses exact admissible price levels and verbatim supporting rows. Existing saved decisions retain this default.
- **Model estimate** projects from a verified reference close. It requires explicit assumptions, a calculation/method, horizon, rationale, risks, and target confidence. The supporting quote validates the input close; the forecast itself need not already appear in source material. Assumptions are model judgments, not observed facts. Validation checks the input and output contract, not the correctness of a future forecast or its economic assumptions.

Both shapes must be positive and finite. Ranges must be ordered, and directional targets must agree with the recommendation relative to the reference close. Estimated targets require an eligible price row from admitted market evidence. A provider response that fills both point and range fields is validated as two candidates; a valid point takes precedence, otherwise a valid range is retained.

## Failure behavior and outputs

The initial request asks for both outputs even at low confidence. One bounded target-only repair can fill an omitted or invalid target, including a model estimate. A repair cannot change the recommendation, thesis, actions, or recommendation confidence. If input data or provider output still cannot pass validation, the valid recommendation survives with an explicit target error; the app does not fabricate a price or confidence score. Technical failure to produce the recommendation itself remains Unavailable.

The investor deck and full report expose the target basis and assumptions. CLI/web target profiles preserve them, consolidated outlooks label estimates, and saved forecast quality notes retain the estimate assumptions. Existing reports are immutable and are not retroactively assigned a new recommendation or forecast.

## Verification

Offline regression coverage exercises mixed shapes, independent confidence after failed repair, zero-confidence estimates, missing or ineligible input prices, invalid direction/values, and model-estimate repair through the investor deck, web result, and forecast record. No paid model run is needed for these tests. Live behavior requires restarting the app with this checkout and a fresh user-initiated analysis.
