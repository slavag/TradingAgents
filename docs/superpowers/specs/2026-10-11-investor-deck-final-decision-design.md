# Investor Deck Final Decision Slide

## Goal and boundary

End every source-supported per-ticker investor deck with a compact, investor-readable account of the completed run's validated Portfolio Manager decision. The slide reports the team's decision; it does not make a second recommendation or treat a price target as a fundamental valuation. The full technical and batch reports remain unchanged. This is a separate change from provider prompt caching.

The October 11 PENG job `89e773b8390d4daca9b4e7381c46c9b3` provides the acceptance case. Its 13-slide deck has distinct bull and bear cases but no final action. Its typed portfolio decision is actionable and Overweight, with separate Hold/Conditional Buy actions, recommendation confidence 74/100, an accepted target of 78.75 over 1–3 months, and target confidence 56/100. The target rationale identifies a near-term retest of a verified session high, not fair value. The same live deck exposed a malformed bull-team excerpt beginning mid-sentence; that is a bounded quality repair in this work.

## Selected approach

Use the already validated `PortfolioDecision` in final graph state as the sole decision authority. Build a typed, deterministic decision block and render it as the last slide after the case slides. Reuse the report's existing source appendix, responsive layout, CSP, escaping, and timestamped filename. No model call, new market-data request, or recalculation of the decision is permitted.

Alternatives rejected:

- A new LLM summary could contradict the saved decision or reintroduce an unvalidated target.
- Parsing the final Markdown as the primary path loses the typed contract and target-validation status. Keep Markdown only as a source note, not as an authority for numeric display.

## Slide content

The title is **Final recommendation**. A short lead states the rating and the distinction between existing and prospective positions. Two action panels show the full validated existing-position action/guidance and new-position action/guidance. A conditions strip shows confirmation, alternative entry (when present), and invalidation. These validated decision fields are not length-capped or silently shortened; cards grow vertically as needed.

Show recommendation confidence as `N/100` with the label **uncalibrated evidence-strength judgement, not a probability**. Show a numeric target only when the typed decision has `target_validation_status=Accepted` and the complete validated target bundle. The target panel includes value, stated horizon, separate target confidence, and the full attributed rationale. It must say when the target is a technical reference/retest rather than a fundamental valuation, using the supplied rationale rather than inferring an intrinsic value. Do not derive upside from a reference price whose adjustment basis may differ from the target. When quote currency is not verified, display the number without an invented currency symbol and note that the quote currency was not recorded.

For rejected, absent, or incomplete targets, show **No validated price target** and the available validation reason; never display a stale proposed number. For Abstain/Unavailable decisions, state that no actionable recommendation was produced, omit action/confidence/target claims, and show a sanitized reason if supplied. If no typed decision exists, do not manufacture a final recommendation from prose. The research deck remains usable without the slide.

The slide cites the Portfolio Manager analysis source and, when present in the admitted ledger, any exact evidence source used to explain a price level. A team statement is labeled as analysis, not a reported company fact. Displayed prose is escaped and free of raw Markdown headings or internal field names; its validated meaning is never cut mid-sentence.

## No-cut presentation rule

The investor deck must not use character-count clipping, CSS line clamping, or ellipses for displayed claims, takeaways, or team views. A source-backed point selected for display is shown as a complete sentence. If a raw source passage is too long to be useful on a slide, omit that passage and select a different complete sentence or omit the topic when evidence is too thin. Editorial selection may keep the deck compact, but must not silently shorten a chosen claim. This rule does not remove bounded source-collection or model-input budgets; those remain operational safeguards, while full filings and the technical report remain separately accessible.

## Live-quality repair

Select complete analyst sentences for the bull and bear interpretation points. Pair Markdown emphasis delimiters correctly; never treat text between a closing bold marker and the next opening marker as an emphasized thesis. Prefer an explicit `My view`/`My conclusion` line when a useful complete sentence exists, strip conversational prefixes, and omit the interpretation point if no clean complete sentence can be extracted. Keep the deterministic factual case points regardless.

## Integration and tests

Add the decision block to the existing deck schema and HTML renderer, then append the slide in `build_investor_deck` after optional research-slide narration is validated or withheld. The decision slide is not sent to the optional deck-narration model. The shared builder already serves web, CLI, and programmatic report paths, so no endpoint or filename change is needed.

Tests cover the PENG typed decision, separately labeled confidences, accepted-target gate, no-target and non-actionable states, existing/new action distinction, source markers, escaped HTML, mobile/print layout, the malformed analyst-emphasis regression, and no-cut display for long decision and source text. Replay saved PENG inputs without overwriting past output; then inspect one new PENG-only served report at desktop and mobile widths. Do not generate SMXT. Run scoped Ruff, full pytest, and `git diff --check`. No commit is authorized by this spec.
