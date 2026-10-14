# Investor Deck Final Decision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. This work stays inline in the approved primary checkout; no subagent or commit is authorized.

**Goal:** Finish each source-supported per-ticker investor deck with the already validated Portfolio Manager recommendation, actions, confidence, and accepted target while repairing the malformed analyst-view excerpt.

**Architecture:** The existing builder produces and optionally narrates research slides. A separate deterministic adapter validates the typed `PortfolioDecision` from final state and appends a typed decision block only after that narration succeeds or falls back to source-only. The HTML renderer escapes the block and uses the existing source-number map and responsive card layout.

**Tech Stack:** Python 3.13, Pydantic, LangChain graph state, offline HTML/CSS, pytest, Ruff.

**Approved spec:** `docs/superpowers/specs/2026-10-11-investor-deck-final-decision-design.md`

---

## File responsibilities

- `tradingagents/investor_deck.py`: validated state-to-decision-block adapter and append boundary; bounded analyst thesis selection.
- `tradingagents/investor_deck_html.py`: escaped decision-card rendering and responsive/print styling.
- `tests/test_investor_deck.py`: typed decision/target gates and malformed-emphasis regression.
- `tests/test_investor_deck_html.py`: final-slide structure, escaping and source-marker checks.
- `tests/test_investor_deck_acceptance.py`: PENG source-only integration and last-slide ordering.

### Task 1: Typed decision contract and omission gates

**Files:** Modify `tradingagents/investor_deck.py`; test `tests/test_investor_deck.py`.

- [ ] **Step 1: Write failing tests.** Construct `PortfolioDecision` inputs by calling `PortfolioDecision.model_validate` through the deck builder. Assert that an actionable accepted target yields `decision` as the last topic, with existing Hold versus new Conditional Buy, recommendation confidence 74, target 78.75, horizon `1-3 months`, target confidence 56, and one `analysis:portfolio` citation. Assert that a rejected target never renders its proposed number; non-actionable status has no rating/actions/confidence/target; and absent/malformed typed state adds no decision slide. Use a ledger with at least two substantive source-supported research topics so the existing deck gate is satisfied.

```python
assert deck.slides[-1].topic == "decision"
block = next(b for b in deck.slides[-1].blocks if b.kind == "decision")
assert (block.rating, block.existing_action, block.new_action) == (
    "Overweight", "Hold", "Conditional Buy"
)
assert (block.recommendation_confidence, block.target, block.target_confidence) == (
    74, Decimal("78.75"), 56
)
assert "analysis:portfolio" in block.source_ids
```

- [ ] **Step 2: Run the focused tests and confirm they fail** because no decision block/slide exists: `pytest -q tests/test_investor_deck.py -k decision_slide`.
- [ ] **Step 3: Add `DecisionBlock` to the discriminated `DeckBlock` union.** Its fields are status, rating, recommendation confidence, separate existing/new action and guidance, confirmation/alternative/invalidation, accepted target/value/horizon/confidence/rationale, quote currency if verified, and source IDs. Give optional values `None`; keep the numeric target as `Decimal`.

```python
class DecisionBlock(BaseModel):
    kind: Literal["decision"] = "decision"
    status: str
    rating: str | None = None
    recommendation_confidence: int | None = None
    existing_action: str | None = None
    existing_guidance: str | None = None
    new_action: str | None = None
    new_guidance: str | None = None
    confirmation: str | None = None
    alternative: str | None = None
    invalidation: str | None = None
    target: Decimal | None = None
    target_horizon: str | None = None
    target_confidence: int | None = None
    target_rationale: str | None = None
    quote_currency: str | None = None
    status_reason: str | None = None
    target_reason: str | None = None
    source_ids: list[str] = Field(default_factory=list)
```

- [ ] **Step 4: Implement `_decision_slide(state, ledger)` using `PortfolioDecision.model_validate` on a mapping at `state["portfolio_decision"]`.** Catch `ValidationError` and omit the slide. Never parse a target from Markdown. Use `analysis:portfolio` when the ledger contains it. Copy the accepted target bundle only when `target_validation_status is TargetValidationStatus.ACCEPTED`; otherwise leave target fields `None`. For actionable status use the validated enum values and bounded summaries. For other statuses use a `No actionable recommendation` takeaway and no action/target metrics. Read `quote_currency` only from a validated market snapshot field, never infer USD from the ticker.

```python
from tradingagents.agents.schemas import DecisionStatus, PortfolioDecision, TargetValidationStatus

def _decision_slide(state: Mapping[str, Any], ledger: ResearchLedger) -> DeckSlide | None:
    raw = state.get("portfolio_decision")
    if not isinstance(raw, Mapping):
        return None
    try:
        decision = PortfolioDecision.model_validate(raw)
    except ValidationError:
        return None
    ids = ["analysis:portfolio"] if "analysis:portfolio" in ledger.sources else []
    actionable = decision.status is DecisionStatus.ACTIONABLE
    accepted = actionable and decision.target_validation_status is TargetValidationStatus.ACCEPTED
    market = state.get("verified_market_snapshot")
    currency = market.get("quote_currency") if isinstance(market, Mapping) else None
    if not isinstance(currency, str) or not re.fullmatch(r"[A-Z]{3}", currency):
        currency = None
    plan = decision.conditional_plan
    block = DecisionBlock(
        status=decision.status.value,
        rating=decision.rating.value if actionable and decision.rating else None,
        recommendation_confidence=decision.recommendation_confidence_score if actionable else None,
        existing_action=decision.existing_position_action.value if actionable and decision.existing_position_action else None,
        existing_guidance=_short_decision_text(decision.existing_position_summary) if actionable else None,
        new_action=decision.new_position_action.value if actionable and decision.new_position_action else None,
        new_guidance=_short_decision_text(decision.new_position_summary) if actionable else None,
        confirmation=_short_decision_text(plan.confirmation) if actionable and plan else None,
        alternative=_short_decision_text(plan.alternative) if actionable and plan else None,
        invalidation=_short_decision_text(plan.invalidation) if actionable and plan else None,
        target=Decimal(str(decision.price_target)) if accepted else None,
        target_horizon=decision.time_horizon if accepted else None,
        target_confidence=decision.confidence_score if accepted else None,
        target_rationale=_short_decision_text(decision.target_summary) if accepted else None,
        quote_currency=currency if accepted else None,
        status_reason=decision.status_reason if not actionable else None,
        target_reason=decision.target_validation_reason if not accepted else None,
        source_ids=ids,
    )
    takeaway = (
        f"{block.rating}: {block.existing_action or 'review'} existing holdings; "
        f"{block.new_action or 'review'} for a new position."
        if actionable else "No actionable recommendation was produced for this run."
    )
    return DeckSlide(topic="decision", title="Final recommendation",
                     takeaway=takeaway, source_ids=ids, blocks=[block])

def _short_decision_text(value: str | None) -> str | None:
    if not value:
        return None
    return " ".join(re.sub(r"[*_#]+", "", value).split()) or None
```

- [ ] **Step 5: Append the decision slide inside `build_investor_deck.result()` after optional research narration.** Do not add it to `build_deck_outline()` or the model `required_topics` packet. Preserve the two-substantive-research-topic gate and the `source_only` fallback. Update `_block_source_ids` for `DecisionBlock`.

```python
# In _block_source_ids, before the existing metric/table/text branches:
if isinstance(block, DecisionBlock):
    keys.extend(block.source_ids)
    continue

# In build_investor_deck, after outline and the substantive-topic gate:
decision_slide = _decision_slide(state, ledger)

# In the nested result() factory only; optional narration still receives outline:
slides = [*outline, decision_slide] if decision_slide is not None else outline
return InvestorDeck(
    ticker=ticker, analysis_date=analysis_date,
    headline=f"{ticker} investor research", slides=slides,
    sources=dict(ledger.sources), synthesis_status=status,
)
```
- [ ] **Step 6: Run `pytest -q tests/test_investor_deck.py` and scoped Ruff**; fix only failures caused by this task.

### Task 2: Investor-readable HTML conclusion

**Files:** Modify `tradingagents/investor_deck_html.py`; test `tests/test_investor_deck_html.py`.

- [ ] **Step 1: Write failing renderer tests.** Render a deck ending in an actionable `DecisionBlock` and assert separate labels for recommendation confidence and target confidence, existing/new actions, accepted target/horizon, bounded rationale, conditions, source link, and footer-right slide count. Render a non-actionable block and assert that no target or action is displayed. Put `<script>` in guidance/rationale and assert escaped output and unchanged CSP.

```python
slide = Selector(text=html).css('section.slide[data-topic="decision"]')
assert slide.css(".decision-existing").get()
assert slide.css(".decision-new").get()
assert "Recommendation confidence" in slide.get()
assert "Target confidence" in slide.get()
assert "74/100" in slide.get() and "56/100" in slide.get()
assert "<script>" not in html
```

- [ ] **Step 2: Run `pytest -q tests/test_investor_deck_html.py -k decision` and confirm RED.**
- [ ] **Step 3: Add `_render_decision` to the renderer dispatch.** Use small escaped helper functions for optional text/number and existing `_references`; never interpolate source prose without `escape`. Display accepted target as a bare number when currency is absent, with `Quote currency not recorded`; do not calculate percentage upside. Give actions two separate cards and render confirmation, alternative, and invalidation only when present. Show `Uncalibrated judgement, not a probability` with the recommendation score. A missing target reads `No validated price target`.

```python
def _render_decision(block: DecisionBlock, numbers: dict[str, int]) -> str:
    def line(label: str, value: str | None) -> str:
        return (f'<p><strong>{escape(label)}</strong> {escape(value)}</p>'
                if value else "")
    if block.rating is None:
        reason = (block.status_reason or "").replace("_", " ")
        return ('<div class="decision-card"><h3>No actionable recommendation</h3>'
                f'{line("Reason:", reason)}</div>')
    confidence = (f"{block.recommendation_confidence}/100"
                  if block.recommendation_confidence is not None else None)
    target = "No validated price target"
    if block.target is not None:
        target = f"{block.target:.2f}"
        if block.quote_currency:
            target += f" {block.quote_currency}"
    target_reason = (block.target_reason or "").replace("_", " ")
    target_note = "" if block.target is None or block.quote_currency else line(
        "Note:", "Quote currency not recorded"
    )
    target_card = (
        '<div class="decision-card decision-target"><h3>Target</h3>'
        f'<p class="decision-value">{escape(target)}</p>'
        f'{target_note}{line("Horizon:", block.target_horizon)}'
        f'{line("Target confidence:", f"{block.target_confidence}/100" if block.target_confidence is not None else None)}'
        f'{line("Team rationale:", block.target_rationale)}'
        f'{line("Why no target:", target_reason if block.target is None else None)}</div>'
    )
    conditions = (
        line("Confirmation:", block.confirmation)
        + line("Alternative:", block.alternative)
        + line("Invalidation:", block.invalidation)
    )
    conditions_html = f'<div class="decision-conditions">{conditions}</div>' if conditions else ""
    return (
        '<div class="decision-grid">'
        '<div class="decision-card"><h3>Team recommendation</h3>'
        f'<p class="decision-value">{escape(block.rating)}</p>'
        f'{line("Recommendation confidence:", confidence)}'
        '<small>Uncalibrated judgement, not a probability</small></div>'
        f'{target_card}'
        '<div class="decision-card decision-existing"><h3>Already own it</h3>'
        f'{line("Action:", block.existing_action)}{line("How:", block.existing_guidance)}</div>'
        '<div class="decision-card decision-new"><h3>Considering a new position</h3>'
        f'{line("Action:", block.new_action)}{line("How:", block.new_guidance)}</div>'
        f'</div>{conditions_html}'
    )

# Add `"decision": _render_decision` to _render_slide's renderer map.
```

- [ ] **Step 4: Add `.decision-grid`, `.decision-card`, `.decision-conditions`, and narrow-screen/print rules** to the existing inline CSS. Keep one-column mobile actions and readable source/footer line.

```css
.decision-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;margin:28px 0}
.decision-card{border:1px solid var(--line);border-radius:9px;padding:20px;background:var(--soft)}
.decision-card h3{margin:0 0 10px;font-size:1rem}.decision-value{font-size:1.5rem;font-weight:700}
.decision-card p{overflow-wrap:anywhere}.decision-card small{color:var(--muted)}
.decision-conditions{border-top:1px solid var(--line);padding-top:12px}
@media(max-width:650px){.decision-grid{grid-template-columns:1fr}}
@media print{.decision-card{break-inside:avoid}}
```
- [ ] **Step 5: Run the focused HTML tests, then both deck and HTML test modules.**

### Task 3: Repair live PENG analyst-view extraction and integration

**Files:** Modify `tradingagents/investor_deck.py`; test `tests/test_investor_deck.py`, `tests/test_investor_deck_acceptance.py`.

- [ ] **Step 1: Write a regression from the October 11 bull text.** It contains `**entry timing and execution risk**` followed later by `My view: **PENG is investable ...**`; assert the extracted point is a complete `PENG is investable ...` sentence and never begins `, not a broken business thesis` or ends at `My view:`. Keep existing bear extraction and analysis-only deck tests.
- [ ] **Step 2: Run the targeted extractor test and confirm RED.**
- [ ] **Step 3: Pair Markdown `**` delimiters sequentially rather than allowing a regex to treat a closing delimiter as the start of a new span.** Prefer a complete explicit `My view`/`My conclusion` sentence when present; strip the label and dialogue prefix. If no clean complete sentence survives, return `None` without suppressing source-backed case facts.

```python
markers = [match.start() for match in re.finditer(r"\*\*", item.text[:2500])]
bold_spans = [item.text[start + 2:end] for start, end in zip(
    markers[::2], markers[1::2], strict=False
)]
explicit = next((line.strip() for line in item.text.splitlines()
                 if re.match(r"(?i)^\s*(?:my view|my conclusion)\b", line)), "")
opening = next((row.strip() for row in item.text.split("\n\n")
                if row.strip() and not row.lstrip().startswith("#")
                and not re.match(r"(?i)^\s*(?:bull|bear) analyst:\s*#", row)), "")
passage = explicit or next((span for span in bold_spans if 45 <= len(span) <= 260), "") or opening
passage = re.sub(r"(?i)^(?:my view|my conclusion(?: remains)?):?\s*", "", passage)
passage = " ".join(re.sub(r"[*_#]+", "", passage).split())
passage = re.sub(r"(?i)^(?:bull|bear) analyst:\s*", "", passage)
if passage and passage[0].islower() and not passage.startswith("bearish"):
    return None
return DeckPoint(text=f"Team interpretation: {passage}", source_ids=[item.evidence_id]) if passage else None
```

- [ ] **Step 4: Extend PENG acceptance with a typed decision fixture and source-only model failure.** Assert the final deck has separate `bull_case`, `bear_case`, and last `decision`, with no new model required topic. Assert target appears only under Accepted and confidence labels remain distinct. Confirm the older `89e773...` saved HTML is not rewritten.

```python
deck = build_investor_deck(state_with_typed_decision, "PENG", "2026-10-11", invalid_llm)
assert deck is not None and deck.synthesis_status == "source_only"
assert [slide.topic for slide in deck.slides[-3:]] == ["bull_case", "bear_case", "decision"]
assert "decision" not in model_required_topics
```
- [ ] **Step 5: Run `pytest -q tests/test_investor_deck.py tests/test_investor_deck_html.py tests/test_investor_deck_acceptance.py`.**

### Task 4: Regression and served quality

**Files:** No production changes unless a focused defect is found; update the existing SDD progress ledger.

- [ ] Run `pytest -q`, scoped `ruff check` on changed Python files, and `git diff --check`.
- [ ] Replay the saved PENG evidence and typed decision in memory; verify accepted target, separate action/confidence fields, final slide ordering, citation targets, and no mid-sentence analyst excerpt. Do not overwrite any saved report.
- [ ] After an app restart, request a fresh PENG-only report URL and inspect desktop/mobile/print-equivalent structure and source navigation. Do not run SMXT.
- [ ] Record limitations and test counts in `.superpowers/sdd/2026-10-09-source-grounded-investor-deck/progress.md`. Do not commit, push, or open a PR without separate approval.

### Task 5: No cut or trimmed displayed text

**Files:** Modify `tradingagents/investor_deck.py`; test `tests/test_investor_deck.py`, `tests/test_investor_deck_acceptance.py`, `tests/test_investor_deck_html.py`.

- [ ] **Step 1: RED tests from the October 12 PENG report.** Assert full validated existing/new-position guidance and full target rationale survive in the final decision block even beyond 420 characters. Assert a long leadership source sentence appears whole or is omitted, never with `…`. Assert all rendered slide takeaways and text points lack arbitrary ellipses, including analyst views. Confirm no CSS line clamp.
- [ ] **Step 2: Run the targeted tests and confirm they fail** on the current sentence/character caps.
- [ ] **Step 3: Remove the final-decision length filter.** `_short_decision_text` becomes whitespace/Markdown cleanup only, with no max length or sentence drop. Rename it to `_decision_text` if doing so improves clarity; update its call sites consistently.
- [ ] **Step 4: In `_support_points`, skip an unwieldy source sentence rather than clipping it; in `_outline_takeaway`, use an entire selected sentence; in `_team_case_point`, select a complete sentence or omit the interpretation rather than clipping.** Leave source-collection and model-packet budgets unchanged.
- [ ] **Step 5: Run targeted/full pytest, scoped Ruff, and `git diff --check`; inspect a newly served PENG-only deck** after app restart. Do not run SMXT or overwrite historical HTML.
