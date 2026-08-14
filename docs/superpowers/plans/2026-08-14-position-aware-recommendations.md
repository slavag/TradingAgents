# Position-Aware Recommendations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce and display separate thesis, existing-position action, and prospective-position action fields, including symmetric Conditional Buy and Conditional Sell plans with accessible bull/bear visuals.

**Architecture:** Extend the typed Portfolio Manager draft/final models, preserve the new bundle through deterministic finalization and markdown rendering, then parse the stable labels into the existing result dictionary used by CLI and web reporting. Keep the current evidence-grounded target validator unchanged and add no extra LLM or vendor call.

**Tech Stack:** Python 3.10+, Pydantic v2, LangChain structured output, pytest, FastAPI result serialization, server-generated HTML/CSS with inline accessible SVG.

---

## File Structure

- Modify `tradingagents/agents/schemas.py`: position-aware enums, conditional-plan model, validation, and markdown labels.
- Modify `tradingagents/agents/utils/decision_integrity.py`: carry the validated position bundle into the final decision.
- Modify `tradingagents/agents/managers/portfolio_manager.py`: define independent thesis and symmetric conditional-action instructions.
- Modify `cli/main.py`: parse the stable labels and render consolidated markdown/HTML decision compasses.
- Modify `tradingagents/web/service.py`: expose the position-aware fields to the browser API.
- Modify `tests/test_decision_integrity.py`: schema, finalization, and renderer contracts.
- Modify `tests/test_portfolio_manager_integrity.py`: prompt and generated markdown integration.
- Modify `tests/test_target_profile.py`: stable-label extraction and legacy compatibility.
- Modify `tests/test_web_service.py`: serialized API contract.
- Modify `tests/test_consolidated_report_formatting.py`: report columns, accessible icons, conditional plan, and escaping.

### Task 1: Typed Position-Aware Decision Contract

**Files:**
- Modify: `tradingagents/agents/schemas.py`
- Modify: `tradingagents/agents/utils/decision_integrity.py`
- Test: `tests/test_decision_integrity.py`

- [x] **Step 1: Write failing schema tests**

Add tests that construct an actionable draft with this bundle:

```python
{
    "thesis": ThesisRating.BULLISH,
    "existing_position_action": ExistingPositionAction.HOLD,
    "existing_position_summary": "Keep a medium position.",
    "new_position_action": NewPositionAction.CONDITIONAL_BUY,
    "new_position_summary": "Wait for confirmation or a controlled pullback.",
    "conditional_plan": ConditionalActionPlan(
        confirmation="Buy after a sustained move above 248-253.",
        alternative="Accumulate near 222, with 211 as deeper support.",
        invalidation="A sustained break below 210 weakens the setup.",
    ),
}
```

Prove that Conditional Buy and Conditional Sell require a plan, unconditional
actions reject a plan, actionable drafts require the position bundle, and
Abstain/Unavailable reject it.

- [x] **Step 2: Verify the schema tests fail for the missing types**

Run:

```bash
python -m pytest tests/test_decision_integrity.py -k "position_bundle or conditional" -q
```

Expected: collection/import failure because the enums and conditional model do
not yet exist.

- [x] **Step 3: Add the enums, conditional model, and shared validator**

Implement:

```python
class ThesisRating(str, Enum):
    STRONGLY_BULLISH = "Strongly Bullish"
    BULLISH = "Bullish"
    NEUTRAL = "Neutral"
    BEARISH = "Bearish"
    STRONGLY_BEARISH = "Strongly Bearish"


class ExistingPositionAction(str, Enum):
    ADD = "Add"
    HOLD = "Hold"
    TRIM = "Trim"
    EXIT = "Exit"


class NewPositionAction(str, Enum):
    BUY = "Buy"
    CONDITIONAL_BUY = "Conditional Buy"
    WAIT = "Wait"
    AVOID = "Avoid"
    CONDITIONAL_SELL = "Conditional Sell"
    SELL = "Sell"


class ConditionalActionPlan(BaseModel):
    confirmation: str = Field(min_length=1)
    alternative: str | None = None
    invalidation: str = Field(min_length=1)
```

Add optional position fields to draft/final models. A reusable validation helper
enforces the actionable bundle and conditional-plan combinations. Keep the
fields optional at declaration time so providers can return non-actionable
decisions, but reject invalid combinations after parsing.

- [x] **Step 4: Carry and render the bundle**

Extend `_base_fields()` in `decision_integrity.py`. Extend `render_pm_decision()`
with stable labels:

```markdown
**Thesis**: Bullish
**Existing Position**: Hold
**Existing Position Guidance**: Keep a medium position.
**New Position**: Conditional Buy
**New Position Guidance**: Wait for confirmation or a controlled pullback.
**Conditional Confirmation**: Buy after a sustained move above 248-253.
**Conditional Alternative**: Accumulate near 222, with 211 as deeper support.
**Conditional Invalidation**: A sustained break below 210 weakens the setup.
```

- [x] **Step 5: Verify focused tests pass**

Run:

```bash
python -m pytest tests/test_decision_integrity.py -q
```

Expected: all decision-integrity tests pass.

### Task 2: Portfolio Manager Generation Rules

**Files:**
- Modify: `tradingagents/agents/managers/portfolio_manager.py`
- Test: `tests/test_portfolio_manager_integrity.py`

- [x] **Step 1: Write failing Portfolio Manager integration tests**

Update the shared actionable draft fixture with the position bundle. Add one
test that captures the structured prompt and asserts it defines Conditional Buy
and Conditional Sell symmetrically and tells the manager to determine thesis
independently rather than counting upstream labels as votes.

- [x] **Step 2: Verify the prompt test fails**

Run:

```bash
python -m pytest tests/test_portfolio_manager_integrity.py -q
```

Expected: the prompt-content assertion fails before the prompt is updated.

- [x] **Step 3: Add position-aware prompt instructions**

Add these rules to the consistency protocol:

```text
- Determine the thesis from evidence before choosing position actions. Treat upstream recommendations as arguments, not independent votes.
- Existing Position describes Add, Hold, Trim, or Exit for a holder.
- New Position describes Buy, Conditional Buy, Wait, Avoid, Conditional Sell, or Sell for a prospective long or short.
- Conditional Buy requires a confirmation trigger, optional pullback entry, and downside invalidation copied from supplied evidence.
- Conditional Sell requires a breakdown trigger, optional failed-rally exit/short entry, and upside invalidation copied from supplied evidence.
- Never invent a price level when the evidence does not supply one.
```

- [x] **Step 4: Verify Portfolio Manager tests pass**

Run:

```bash
python -m pytest tests/test_portfolio_manager_integrity.py -q
```

Expected: all Portfolio Manager integrity tests pass.

### Task 3: Result Extraction and Web API

**Files:**
- Modify: `cli/main.py`
- Modify: `tradingagents/web/service.py`
- Test: `tests/test_target_profile.py`
- Test: `tests/test_web_service.py`

- [x] **Step 1: Write failing extraction and serialization tests**

Supply Portfolio Manager markdown with all stable labels. Assert
`estimate_target_profile()` returns:

```python
{
    "thesis": "Bullish",
    "existing_position_action": "Hold",
    "existing_position_summary": "Keep a medium position.",
    "new_position_action": "Conditional Buy",
    "new_position_summary": "Wait for confirmation or a controlled pullback.",
    "conditional_confirmation": "Buy after a sustained move above 248-253.",
    "conditional_alternative": "Accumulate near 222, with 211 as deeper support.",
    "conditional_invalidation": "A sustained break below 210 weakens the setup.",
}
```

Assert `serialize_result()` preserves the same fields. Add a legacy case that
returns `None` for every new field without changing the old decision.

- [x] **Step 2: Verify the new tests fail**

Run:

```bash
python -m pytest tests/test_target_profile.py tests/test_web_service.py -q
```

Expected: assertions fail because the new keys are absent.

- [x] **Step 3: Parse stable labels in the existing target profile path**

Reuse the local `markdown_field()` helper in `estimate_target_profile()` and add
the position fields to every return branch. Do not infer missing fields from the
five-tier decision; legacy absence stays explicit.

- [x] **Step 4: Serialize the new keys**

Add all eight fields to successful and failed `serialize_result()` dictionaries.
Failed runs and legacy decisions expose `None` rather than synthesized advice.

- [x] **Step 5: Verify extraction and API tests pass**

Run:

```bash
python -m pytest tests/test_target_profile.py tests/test_web_service.py -q
```

Expected: all selected tests pass.

### Task 4: Consolidated Markdown and Visual Decision Compass

**Files:**
- Modify: `cli/main.py`
- Modify: `tradingagents/web/static/app.js`
- Modify: `tradingagents/web/static/styles.css`
- Test: `tests/test_consolidated_report_formatting.py`
- Test: `tests/test_web_static_tape.py`

- [x] **Step 1: Write failing consolidated-report tests**

Add Bullish/Conditional Buy and Bearish/Conditional Sell fixtures. Assert:

- markdown includes Thesis, Existing Position, and New Position columns;
- the per-stock Position Plan contains all conditional instructions;
- HTML contains `aria-label='Bullish thesis'` or `aria-label='Bearish thesis'`;
- Neutral uses `aria-label='Neutral thesis'`;
- conditional rows have `condition-confirmation`, `condition-alternative`, and
  `condition-invalidation` classes; and
- model-provided `<script>` content is escaped.

- [x] **Step 2: Verify the report tests fail**

Run:

```bash
python -m pytest tests/test_consolidated_report_formatting.py -q
```

Expected: new columns, icon labels, and condition classes are absent.

- [x] **Step 3: Add report helpers and markdown output**

Add focused local helpers inside `build_consolidated_report_html()` for thesis
kind and accessible inline SVG. Extend markdown summary/detail output with the
position plan while preserving legacy `-` values.

- [x] **Step 4: Add the HTML decision compass and CSS**

Render a `.decision-compass` containing `.thesis-card`, `.position-card`, and
`.condition-list`. Use teal for bullish, coral for bearish, and gold for neutral
or conditional accents. Every icon includes a text label and `aria-label`; every
model string passes through `escape()`.

- [x] **Step 5: Verify report tests pass**

Run:

```bash
python -m pytest tests/test_consolidated_report_formatting.py tests/test_web_service.py -q
```

Expected: all selected tests pass.

- [x] **Step 6: Add the same compass to live web result cards**

Render the serialized thesis, existing-position action, new-position action, and
conditional rows in `resultCard()`. Reuse accessible inline SVG bull, bear, and
neutral marks; escape every model-provided value. Add compact dark-theme styles
and static-contract coverage in `tests/test_web_static_tape.py`.

### Task 5: Regression Verification

**Files:**
- Verify only; no planned production edits.

- [x] **Step 1: Run the focused decision/report suite**

```bash
python -m pytest \
  tests/test_decision_integrity.py \
  tests/test_portfolio_manager_integrity.py \
  tests/test_target_profile.py \
  tests/test_consolidated_report_formatting.py \
  tests/test_web_service.py \
  tests/test_reporting.py \
  -q
```

Expected: all selected tests pass.

- [x] **Step 2: Run the full project suite**

```bash
python -m pytest -q
```

Expected: all tests pass, with any optional dependency skips reported.

- [x] **Step 3: Inspect the final worktree diff**

```bash
git diff --check
git status --short
git diff --stat
```

Expected: no whitespace errors; existing model-catalog changes remain intact;
only the planned recommendation files and tests are additionally modified.
