# Investor report clarity, depth, and six-/twelve-month outlooks

Status: approved by the user with “go” and implemented. Verification: 1,601 Python tests passed, two skipped; no live provider/model calls, commits or pushes during development.

## Requested result

Fix the unreadable CBRS investor slide, provide more explanation across the report, and add distinct six-month and twelve-month outlooks. This is a generic reporting change for every ticker, with saved CBRS run `49f3606169034753a019ffe36fe5e59b` as the acceptance case. Existing recommendation/target-confidence and sizing changes must remain intact.

## Confirmed diagnosis

The CBRS investor deck has nine slides and `source_only` synthesis status with `unsupported_interpretation`. Its saved Portfolio Manager decision is Underweight with recommendation confidence 78/100 and target confidence 38/100. The existing target horizon is one to three months; no six- or twelve-month forecast exists in that saved decision.

Three code paths explain the screenshot:

1. `extract_sections` in `tradingagents/dataflows/sec_documents.py` attaches the last preceding text nodes to each table as its `content`. Those nodes can be the tail of a different financial table. CBRS's Table 6 content contains the preceding quarterly core-revenue reconciliation, while its structured rows describe the nine-month reconciliation.
2. `_support_points` in `tradingagents/investor_deck.py` accepts sentences mentioning “customer” as relationship evidence. The phrase “amortization of customer warrant assets” therefore qualifies an accounting reconciliation for Customers and partners.
3. `_outline_takeaway` repeats the first supporting point as the headline. When optional narration fails validation, raw excerpt selection becomes the reader-facing report. Current source-only coverage also omits business/product material when it is present only in quarterly filing excerpts, and the financial table reader does not handle this release's multi-row year/duration headers.

## Approach and alternatives

Recommended: repair source/table ownership and topic selection, build readable evidence-led slides that work without optional narration, and generate typed horizon-specific outlooks in the decision stage. This fixes both the fallback and future model-generated reports.

A narration-only change is smaller but leaves the malformed fallback and mislabeled evidence in place. A full deck/layout rewrite is larger than this request requires and does not itself correct financial source interpretation.

## Readable, richer slides

Financial tables remain tables. Preserve the owning caption, unit scale, accounting basis, fiscal period, comparison period, and column hierarchy. Do not reuse text from an earlier table. Preserve bounded complete meaningful columns instead of silently clipping a comparison group. Where saved evidence lacks reliable headers/units, exclude ambiguous numbers and identify the missing context; never guess which period a value belongs to.

Customers and partners must contain attributable commercial relationships, customer concentration, distribution arrangements, or named counterparties. Accounting uses of “customer,” including warrant amortization, do not qualify. Use relevant quarterly business disclosures as well as annual filings. Omit a relationship slide when none of its available sources supports that topic.

Each substantive slide should explain what happened, why it matters, and the main uncertainty/counterargument. Aim for three to five substantive points or a readable table/chart plus two to three explanatory points, roughly 80–140 words where evidence supports it. Use more than one slide when necessary instead of shrinking text or cutting sentences. Do not repeat the first bullet verbatim as the headline. Source-only reports must remain understandable when optional model narration is unavailable or rejected. Facts and arithmetic remain attributable; interpretation is labeled and grounded in those facts. Do not weaken citation validation to achieve longer text.

### Example CBRS profitability slide

Illustrative content, based on saved November 12 earnings-release Table 1; not a new investment recommendation.

**Headline:** Revenue growth is not yet translating into stronger operating results.

**Quarter ended September 30 — USD millions, GAAP**

| Measure | 2026 | 2025 |
| --- | ---: | ---: |
| Revenue | 210.809 | 135.714 |
| Gross profit | 42.094 | 55.135 |
| Operating loss | (224.149) | (26.469) |

The source reports these amounts in thousands; this example converts them to millions with the scale shown explicitly.

- Revenue increased, but gross profit declined. The report should explain this divergence rather than imply that sales growth alone establishes stronger profitability.
- The reported business mix changed: hardware revenue declined while cloud and other services revenue increased. This belongs in the operating story, separate from customer warrant accounting adjustments.
- Higher operating expenses and the larger operating loss make the cost trajectory a central issue to monitor. Accounting losses and operating cash consumption must be discussed separately.
- Counterpoint: services growth may improve the business mix; subsequent filings must establish whether that translates into stronger margins and cash conversion.

Source attribution must link directly to the saved evidence and filing record. GAAP and “core” non-GAAP reconciliations must be clearly distinguished on any additional slide.

## Six-month and twelve-month outlooks

Add two distinct outlook sections near the final recommendation. Six and twelve calendar months are measured from the analysis date, not approximated by the existing tactical horizon. For this CBRS analysis, the endpoint dates are May 13, 2027 and November 13, 2027.

Each horizon includes:

- A five-tier recommendation with its own recommendation confidence.
- A point target or range in the quote currency, expected change from the verified reference close, and a separate target confidence.
- Base-case rationale and explicit assumptions/method, clearly labeled as model estimates when applicable.
- Upside/downside conditions, material drivers or catalysts, key risks, and what would change the view.
- Evidence references, analysis date, and the reference-price observation date.

The six-month case emphasizes the next reporting cycles and operating milestones. The twelve-month case explains the longer-term business, profitability, funding, and valuation assumptions. Different horizons may reasonably have different recommendations and confidence; explain the difference instead of forcing matching conclusions. Confidence remains an uncalibrated evidence-strength score, not a probability. Low confidence is allowed.

Generate these as structured decision/outlook data from the existing research and verified inputs, using the existing model stage rather than adding a separate full research run. Persist them with the run and propagate them to the investor deck, full report, and web result. Keep the current tactical decision/target intact; do not overwrite or relabel it as a six-/twelve-month forecast. Validate targets with the established Source level/Model estimate contract, preserving method, assumptions, risks, and independent confidence. Missing/invalid horizon output must be handled by a bounded repair and an explicit diagnostic, never silently replaced by the tactical target or invented confidence.

## Verification and delivery

Use saved CBRS evidence and a small synthetic SEC HTML fixture to reproduce the preceding-table contamination, accounting/customer false match, multi-row period headers, and lost/truncated comparison columns. Add report tests for units, periods, GAAP/non-GAAP labeling, distinct headlines, substantive fallback explanations, and no unsupported commercial claims.

Use fake structured responses to verify both horizon targets and confidence scores, calendar endpoints, different cross-horizon views, model estimates, validation/repair behavior, legacy runs without outlooks, serialization, and HTML/full-report consistency. Inspect a separately generated CBRS preview offline without overwriting the original saved report. The preview must not claim that newly generated six-/twelve-month predictions already existed in the original run.

Run focused and full offline checks, then reload the idle local app. No paid analysis, commit, or push is authorized by this design step. A fresh user-initiated run is needed to validate real generated outlooks after implementation.

## Approved extended-hours information marker

The user additionally requested pre-/post-market behaviour as an information marker for the latest news. Current live analyses collect it once per market stage, with the provider quote, exchange/UTC observation time, capture time, age, currency, and percentage change against a verified regular-session reference. It remains separate from daily OHLCV, technical indicators, and target anchors. Timing alone is not evidence of news causation.

The initial adapter supports verified New York US exchanges and USD. A local NYSE session calendar establishes the latest completed regular close, including holidays and shortened sessions. Missing, future, stale, unsupported, or ambiguous quote/reference metadata yields an explicit readable diagnostic; historical runs do not request current quotes. Both full/consolidated reports, saved JSON, investor slides and web results retain the marker. Real provider availability remains to be confirmed in a fresh user-initiated analysis.

## Offline CBRS preview

The preview was rebuilt from run `49f3606169034753a019ffe36fe5e59b` and cached original SEC HTML, repairing twelve table records without fetching data or overwriting the saved run. It contains fourteen slides, including business/product coverage and corrected quarterly GAAP comparison. The saved run's missing horizon forecasts and extended-hours quote are disclosed explicitly. Preview: `/Users/slava/.codex/visualizations/2026/11/11/01a1cb9a-9cac-7513-b173-cc32036d81df/cbrs-investor-preview.html`.
