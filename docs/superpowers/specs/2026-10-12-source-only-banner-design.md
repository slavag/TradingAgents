# Source-Only Investor Deck Status

## Goal

Remove the prominent “Interpretation withheld” banner from investor-facing HTML. That banner currently appears whenever optional deck narration fails validation, even though the deterministic, cited research slides and validated Portfolio Manager decision still render. It is a generator status, not an investment conclusion, and the October 12 PENG report did not persist the specific rejection reason needed to explain it.

## Behavior

Keep the current validation boundary: do not include any rejected model-written prose. Continue rendering the source-backed deck when it passes the substantive-topic gate, and continue using the honest unavailable page when source coverage itself is too thin. Do not add a replacement warning, badge, or footnote to the investor HTML. Preserve `synthesis_status` and add a bounded internal reason code to the deck/report result so CLI/web job metadata and logs can distinguish `model_unavailable`, `topic_budget`, `required_topics_missing`, `source_id_mismatch`, `unsupported_interpretation`, `unsupported_factual_point`, and structured-output failure. Only known application-defined codes are persisted; unknown exception text becomes `invalid_structured_response`. Do not save raw model responses or prompt text.

The HTML report remains self-contained, escaped, timestamped, and cited. Old job files are never rewritten. This is presentation and diagnostics work only; it does not loosen fact/quote validation or alter the Portfolio Manager decision.

## Verification

Write RED→GREEN tests showing a rich source-only deck has no banner but still excludes invalid optional prose; thin-source runs still receive the unavailable page; a specific internal reason survives serialization without leaking model output. Run scoped and full tests, Ruff, and `git diff --check`. No PENG or SMXT generation is part of deterministic tests. Leave changes uncommitted absent separate authorization.
