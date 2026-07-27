# TUI Result Summary Design

## Goal

Give terminal users the same immediate decision overview that the web app
provides after an analysis completes, without changing the live TUI layout or
the existing full-report workflow.

## Scope

After all requested tickers finish, the TUI will print one compact Rich table.
The same table supports a single ticker and a sequential multi-ticker batch,
with one row per result.

Each successful row contains:

- ticker;
- decision;
- evidence-grounded price target;
- model confidence, explicitly labeled as uncalibrated;
- target horizon; and
- target outlook.

Failed results remain in the table with a failed decision/status and a concise
error message. Missing target metrics render as an em dash. The presentation
must not invent a target, confidence score, horizon, or outlook.

This change does not redesign the live progress layout, change graph execution,
alter target extraction, add LLM calls, or replace the existing prompts for
saving and displaying complete reports.

## Data Flow

`run_single_analysis` already returns the required result fields after
`estimate_target_profile` validates the Portfolio Manager output. `run_analysis`
collects those dictionaries in `analysis_results`.

A focused formatter will build the result table directly from
`analysis_results`. `run_analysis` will print it once after all ticker runs
finish and before the existing save and full-report prompts. This keeps single
and batch behavior consistent and avoids duplicate output between ticker runs.

## Presentation

The table will use the TUI's existing Rich styling and compact terminal-safe
wrapping. It will include these columns:

| Ticker | Decision | Target | Confidence (uncalibrated) | Horizon | Outlook |
| --- | --- | --- | --- | --- | --- |

Outlook text may wrap but must remain concise enough to keep batch output
readable. Failed rows will use the decision column to show `Failed` and the
outlook column to show the error. A successful result with unavailable metrics
will display an em dash in the corresponding cells.

The existing completion messages, default batch-report generation, save
behavior, and optional full-report display remain unchanged.

## Error Handling

The summary formatter must tolerate missing optional fields and failed result
dictionaries. Formatting a summary must not turn a completed analysis into a
failed run. Non-string error values will be converted to readable text.

## Testing

Tests will be written before implementation and will verify:

- a successful single-ticker result renders all six columns and values;
- multiple results render one row per ticker;
- missing evidence-backed metrics render as unavailable;
- failed results render a concise failure row;
- `run_analysis` prints the compact summary before the existing post-analysis
  prompts; and
- no extra LLM or target-estimation work is triggered by rendering.

Focused TUI tests and the relevant existing target-profile and lifecycle tests
must pass. Completion also requires the full project test suite, with any
environmental or baseline failures reported explicitly.
