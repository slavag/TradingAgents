# Decision Integrity Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every new final Portfolio Manager decision explicitly actionable, abstained, or unavailable, and prevent unsupported target metrics or structured-output failures from becoming apparently valid recommendations.

**Architecture:** Bind the LLM to a permissive provider-facing `PortfolioDecisionDraft`, then finalize it through a deterministic decision-integrity boundary into a stricter `PortfolioDecision`. Final decisions render explicit status and validation metadata; CLI, web, signal processing, and memory consume the same status and target-validation rules.

**Tech Stack:** Python 3.10+, Pydantic v2 through LangChain, LangGraph state dictionaries, pytest, Rich CLI, FastAPI web service, vanilla JavaScript UI.

## Global Constraints

- Preserve the existing five-tier Buy / Overweight / Hold / Underweight / Sell scale for actionable decisions.
- Keep intentional Hold distinct from Abstain and Unavailable.
- Never turn an unparseable new final decision into Hold.
- Preserve readable legacy reports that contain a valid five-tier rating.
- A finalized target bundle contains target, horizon, confidence, rationale, and supporting quote together, or contains none of them.
- Invalid optional target metrics do not invalidate an otherwise valid actionable rating.
- Primary and fallback targets use the same deterministic provenance and direction validator.
- No new LLM call or external data request is introduced by validation.
- Raw exceptions, credentials, and provider responses are not rendered to users.
- Research Manager and Trader fallback behavior remains unchanged in this release.
- Tests are written and observed failing before each production change.
- This release makes no claim of calibrated confidence or improved financial returns.

---

## File Structure

- Create `tradingagents/agents/utils/decision_integrity.py`: evidence assembly, typed target validation, reference-price extraction, and draft finalization.
- Create `tests/test_decision_integrity.py`: schema and contextual-validator unit tests.
- Create `tests/test_portfolio_manager_integrity.py`: strict final-agent invocation and evidence integration tests.
- Modify `tradingagents/agents/schemas.py`: decision-status, draft/final schemas, target validation metadata, and rendering.
- Modify `tradingagents/agents/utils/structured.py`: add a structured-required invocation path without changing existing graceful fallback users.
- Modify `tradingagents/agents/managers/portfolio_manager.py`: bind the draft schema, finalize deterministic output, and render explicit unavailable decisions.
- Modify `tradingagents/agents/utils/rating.py`: add strict final-decision parsing while retaining the legacy rating parser.
- Modify `tradingagents/graph/signal_processing.py`: use strict final-decision parsing.
- Modify `tradingagents/agents/utils/memory.py`: persist non-actionable decisions without scheduling outcomes.
- Modify `cli/main.py`: remove local provenance logic and use the shared target validator for primary and fallback profiles.
- Modify `tradingagents/web/static/app.js`: expose target-validation rejection details returned by the shared service path.
- Modify `tests/test_structured_agents.py`, `tests/test_signal_processing.py`, `tests/test_memory_log.py`, `tests/test_target_profile.py`, `tests/test_web_service.py`, and `tests/test_web_static_tape.py`: compatibility and integration coverage.

---

### Task 1: Provider Draft and Final Decision Schemas

**Files:**
- Modify: `tradingagents/agents/schemas.py:188-281`
- Create: `tests/test_decision_integrity.py`
- Modify: `tests/test_structured_agents.py:1-125`
- Modify: `tests/test_memory_log.py:1140-1190`

**Interfaces:**
- Produces: `DecisionStatus`, `TargetValidationStatus`, `TargetValidationReason`, `PortfolioDecisionDraft`, `PortfolioDecision`, `PortfolioDecision.unavailable(reason)`, and `render_pm_decision(decision)`.
- Contract: `PortfolioDecisionDraft` may contain a partial target proposal; `PortfolioDecision` may contain either a complete accepted bundle or no target fields.

- [ ] **Step 1: Write failing status/rating contract tests**

```python
def test_actionable_draft_requires_rating():
    with pytest.raises(ValidationError):
        PortfolioDecisionDraft(
            status=DecisionStatus.ACTIONABLE,
            rating=None,
            executive_summary="Evidence supports a position.",
            investment_thesis="Supported thesis.",
        )


@pytest.mark.parametrize("status", [DecisionStatus.ABSTAIN, DecisionStatus.UNAVAILABLE])
def test_non_actionable_draft_rejects_rating(status):
    with pytest.raises(ValidationError):
        PortfolioDecisionDraft(
            status=status,
            rating=PortfolioRating.HOLD,
            executive_summary="No actionable decision.",
            investment_thesis="Evidence is insufficient.",
        )
```

- [ ] **Step 2: Run the status tests and verify RED**

Run: `python -m pytest tests/test_decision_integrity.py -k "draft_requires or draft_rejects" -q`

Expected: collection or import failure because the new status and draft types do not exist.

- [ ] **Step 3: Add the provider-facing draft and status enums**

```python
class DecisionStatus(str, Enum):
    ACTIONABLE = "Actionable"
    ABSTAIN = "Abstain"
    UNAVAILABLE = "Unavailable"


class TargetValidationStatus(str, Enum):
    NOT_PROPOSED = "Not Proposed"
    ACCEPTED = "Accepted"
    REJECTED = "Rejected"


class TargetValidationReason(str, Enum):
    BUNDLE_INCOMPLETE = "target_bundle_incomplete"
    TARGET_NOT_POSITIVE_FINITE = "target_not_positive_finite"
    SUPPORTING_QUOTE_MISSING = "supporting_quote_missing"
    SUPPORTING_QUOTE_NOT_IN_EVIDENCE = "supporting_quote_not_in_evidence"
    SUPPORTING_QUOTE_NUMBER_MISMATCH = "supporting_quote_number_mismatch"
    SUPPORTING_QUOTE_NOT_PRICE_CONTEXT = "supporting_quote_not_price_context"
    TARGET_DIRECTION_CONFLICT = "target_direction_conflict"


class PortfolioDecisionDraft(BaseModel):
    status: DecisionStatus = DecisionStatus.ACTIONABLE
    rating: PortfolioRating | None = None
    executive_summary: str
    investment_thesis: str
    price_target: float | None = None
    time_horizon: str | None = None
    confidence_score: int | None = Field(default=None, ge=0, le=100)
    target_summary: str | None = None
    supporting_quote: str | None = None

    @model_validator(mode="after")
    def validate_status_rating(self):
        if self.status is DecisionStatus.ACTIONABLE and self.rating is None:
            raise ValueError("actionable decisions require a rating")
        if self.status is not DecisionStatus.ACTIONABLE and self.rating is not None:
            raise ValueError("non-actionable decisions cannot carry a rating")
        return self
```

- [ ] **Step 4: Run the status tests and verify GREEN**

Run: `python -m pytest tests/test_decision_integrity.py -k "draft_requires or draft_rejects" -q`

Expected: all selected tests pass.

- [ ] **Step 5: Write failing finalized-bundle and rendering tests**

```python
def test_final_decision_rejects_partial_target_bundle():
    with pytest.raises(ValidationError):
        PortfolioDecision(
            status=DecisionStatus.ACTIONABLE,
            rating=PortfolioRating.BUY,
            executive_summary="Build gradually.",
            investment_thesis="Supported thesis.",
            price_target=120.0,
            target_validation_status=TargetValidationStatus.ACCEPTED,
        )


def test_unavailable_factory_renders_explicit_status_without_rating():
    decision = PortfolioDecision.unavailable("structured_response_invalid")
    rendered = render_pm_decision(decision)
    assert "**Decision Status**: Unavailable" in rendered
    assert "**Rating**" not in rendered
    assert "structured_response_invalid" in rendered
```

- [ ] **Step 6: Run finalized-schema tests and verify RED**

Run: `python -m pytest tests/test_decision_integrity.py -k "final_decision or unavailable_factory" -q`

Expected: failure because finalized validation, factory, and status rendering are absent.

- [ ] **Step 7: Implement finalized decision invariants and renderer**

Implement `PortfolioDecision` with system-owned `target_validation_status`, `target_validation_reason`, the five optional target fields, and an after-model validator that enforces:

```python
bundle = (
    self.price_target,
    self.time_horizon,
    self.confidence_score,
    self.target_summary,
    self.supporting_quote,
)
if any(value is not None for value in bundle) and not all(
    value is not None for value in bundle
):
    raise ValueError("finalized target bundle must be complete or absent")
if self.target_validation_status is TargetValidationStatus.ACCEPTED and not all(
    value is not None for value in bundle
):
    raise ValueError("accepted target validation requires a complete bundle")
```

Render `Decision Status` first, omit `Rating` for non-actionable decisions, render accepted supporting evidence as `**Target Supporting Quote**`, and render rejected reason as `**Target Validation**: Rejected (<reason>)`.

- [ ] **Step 8: Run schema and existing structured-agent tests**

Run: `python -m pytest tests/test_decision_integrity.py tests/test_structured_agents.py tests/test_memory_log.py -k "decision or pm_nullish or pm_returns" -q`

Expected: selected tests pass after updating existing constructors to use the finalized contract where required.

- [ ] **Step 9: Commit Task 1**

```bash
git add tradingagents/agents/schemas.py tests/test_decision_integrity.py tests/test_structured_agents.py tests/test_memory_log.py
git commit -m "feat: define explicit portfolio decision states"
```

---

### Task 2: Shared Decision-Integrity Validator

**Files:**
- Create: `tradingagents/agents/utils/decision_integrity.py`
- Modify: `tests/test_decision_integrity.py`

**Interfaces:**
- Consumes: `PortfolioDecisionDraft`, completed graph-state evidence, optional verified reference price.
- Produces: `build_decision_evidence(state: Mapping[str, Any]) -> str`, `extract_verified_reference_price(text: str) -> float | None`, and `finalize_portfolio_decision(draft, evidence_text, reference_price=None) -> PortfolioDecision`.

- [ ] **Step 1: Write failing accepted-target provenance test**

```python
def test_finalize_accepts_complete_verbatim_price_quote():
    draft = actionable_draft(
        price_target=120.0,
        time_horizon="3 months",
        confidence_score=78,
        target_summary="Resistance supports the central case.",
        supporting_quote="Verified close: 100. Resistance: 120.",
    )
    result = finalize_portfolio_decision(
        draft,
        evidence_text="Verified close: 100. Resistance: 120.",
        reference_price=100.0,
    )
    assert result.target_validation_status is TargetValidationStatus.ACCEPTED
    assert result.price_target == 120.0
```

- [ ] **Step 2: Run the accepted-target test and verify RED**

Run: `python -m pytest tests/test_decision_integrity.py::test_finalize_accepts_complete_verbatim_price_quote -q`

Expected: import or name failure because the integrity module does not exist.

- [ ] **Step 3: Implement evidence normalization and exact price-context matching**

Move the proven numeric and context matching behavior from `_fallback_target_has_provenance` into private helpers in `decision_integrity.py`. Use whitespace-normalized substring matching, `math.isclose(..., rel_tol=1e-9, abs_tol=0.005)`, the existing price-label/currency patterns, a 300-character quote bound, and explicit rejection of percentage, volume, share, and magnitude suffixes.

- [ ] **Step 4: Implement finalization for accepted bundles**

```python
def finalize_portfolio_decision(
    draft: PortfolioDecisionDraft,
    evidence_text: str,
    reference_price: float | None = None,
) -> PortfolioDecision:
    proposed = _target_fields(draft)
    if not any(value is not None for value in proposed):
        return _without_target(draft, TargetValidationStatus.NOT_PROPOSED, None)
    if not all(value is not None for value in proposed):
        return _without_target(
            draft,
            TargetValidationStatus.REJECTED,
            TargetValidationReason.BUNDLE_INCOMPLETE,
        )
    reason = _target_rejection_reason(draft, evidence_text, reference_price)
    if reason is not None:
        return _without_target(draft, TargetValidationStatus.REJECTED, reason)
    return PortfolioDecision(
        **_base_fields(draft),
        **_target_fields_dict(draft),
        target_validation_status=TargetValidationStatus.ACCEPTED,
    )
```

- [ ] **Step 5: Run the accepted-target test and verify GREEN**

Run: `python -m pytest tests/test_decision_integrity.py::test_finalize_accepts_complete_verbatim_price_quote -q`

Expected: pass.

- [ ] **Step 6: Write failing rejection-matrix tests**

Use parametrized cases for incomplete bundles, non-positive/non-finite targets, missing quotes, quotes absent from evidence, mismatched numbers, percentages, dates, ratios, volume/share counts, oversized quotes, and bullish/bearish direction conflicts. Every case must assert that all five target fields are cleared, the rating is preserved, status remains actionable, and the exact rejection reason is recorded.

- [ ] **Step 7: Run the rejection matrix and verify RED**

Run: `python -m pytest tests/test_decision_integrity.py -k "rejects or incomplete or direction" -q`

Expected: failures for each rejection path not yet implemented.

- [ ] **Step 8: Implement rejection reasons, evidence assembly, and conservative reference-price extraction**

`build_decision_evidence` joins non-empty values from `market_report`, `sentiment_report`, `news_report`, `fundamentals_report`, `investment_plan`, `trader_investment_plan`, risk-debate history, and `past_context`, with stable section labels. `extract_verified_reference_price` accepts only explicit `Verified close: <number>` text and returns `None` for ambiguous unlabeled numbers.

- [ ] **Step 9: Run all integrity tests and Ruff**

Run: `python -m pytest tests/test_decision_integrity.py -q`

Run: `python -m ruff check tradingagents/agents/utils/decision_integrity.py tradingagents/agents/schemas.py tests/test_decision_integrity.py`

Expected: all tests pass and Ruff reports no errors.

- [ ] **Step 10: Commit Task 2**

```bash
git add tradingagents/agents/utils/decision_integrity.py tradingagents/agents/schemas.py tests/test_decision_integrity.py
git commit -m "feat: validate portfolio targets against supplied evidence"
```

---

### Task 3: Strict Portfolio Manager Invocation

**Files:**
- Modify: `tradingagents/agents/utils/structured.py:1-90`
- Modify: `tradingagents/agents/managers/portfolio_manager.py:1-105`
- Create: `tests/test_portfolio_manager_integrity.py`
- Modify: `tests/test_memory_log.py:1140-1190`

**Interfaces:**
- Produces: `StructuredOutputFailure(code: str)` and `invoke_structured_required(structured_llm, prompt, agent_name) -> BaseModel`.
- Consumes: `finalize_portfolio_decision` and `PortfolioDecision.unavailable`.
- Constraint: existing `invoke_structured_or_freetext` behavior for Research Manager and Trader remains unchanged.

- [ ] **Step 1: Write failing strict-invocation tests**

```python
@pytest.mark.parametrize(
    ("failure_mode", "expected_code"),
    [
        ("unsupported", "structured_binding_unsupported"),
        ("missing", "structured_response_missing"),
        ("provider_error", "structured_invocation_failed"),
    ],
)
def test_required_structured_invocation_returns_sanitized_unavailable(
    failure_mode, expected_code
):
    llm = MagicMock()
    if failure_mode == "unsupported":
        llm.with_structured_output.side_effect = NotImplementedError("unsupported")
    else:
        structured = MagicMock()
        if failure_mode == "missing":
            structured.invoke.return_value = None
        else:
            structured.invoke.side_effect = RuntimeError("provider failed")
        llm.with_structured_output.return_value = structured

    node = create_portfolio_manager(llm)
    result = node(make_pm_state())
    assert "**Decision Status**: Unavailable" in result["final_trade_decision"]
    assert expected_code in result["final_trade_decision"]
    llm.invoke.assert_not_called()
```

- [ ] **Step 2: Run strict-invocation tests and verify RED**

Run: `python -m pytest tests/test_portfolio_manager_integrity.py -k "required_structured or unavailable" -q`

Expected: failures because Portfolio Manager still retries unchecked free text.

- [ ] **Step 3: Add the structured-required helper**

Implement a small exception carrying only a stable code. Translate binding absence, missing parsed result, Pydantic validation failure, and generic invocation failure into the four approved codes. Log exception details internally, but expose only the code to the manager.

- [ ] **Step 4: Bind `PortfolioDecisionDraft` and finalize successful results**

In `create_portfolio_manager`, bind `PortfolioDecisionDraft`. Build deterministic evidence from state, extract an optional verified close from that evidence, call `invoke_structured_required`, finalize the draft, and render the final decision. On `StructuredOutputFailure`, render `PortfolioDecision.unavailable(exc.code)`.

- [ ] **Step 5: Add a failing primary-target rejection integration test**

```python
def test_primary_pm_target_without_verbatim_quote_is_removed_but_rating_survives():
    draft = actionable_draft(
        rating=PortfolioRating.BUY,
        price_target=999.0,
        time_horizon="12 months",
        confidence_score=90,
        target_summary="Large upside.",
        supporting_quote="Target 999",
    )
    result = create_portfolio_manager(structured_llm_returning(draft))(
        make_pm_state(market_report="Verified close: 100. Resistance: 120.")
    )["final_trade_decision"]
    assert "**Rating**: Buy" in result
    assert "**Price Target**" not in result
    assert "supporting_quote_not_in_evidence" in result
```

- [ ] **Step 6: Run the primary-target test and verify RED, then implement the evidence path**

Run: `python -m pytest tests/test_portfolio_manager_integrity.py::test_primary_pm_target_without_verbatim_quote_is_removed_but_rating_survives -q`

Expected before implementation: target remains present or validation metadata is absent.

- [ ] **Step 7: Run Portfolio Manager and existing structured fallback tests**

Run: `python -m pytest tests/test_portfolio_manager_integrity.py tests/test_memory_log.py tests/test_structured_agents.py -q`

Expected: Portfolio Manager strict tests pass; Research Manager and Trader free-text fallback tests still pass.

- [ ] **Step 8: Commit Task 3**

```bash
git add tradingagents/agents/utils/structured.py tradingagents/agents/managers/portfolio_manager.py tests/test_portfolio_manager_integrity.py tests/test_memory_log.py
git commit -m "fix: fail closed on portfolio decision generation"
```

---

### Task 4: Strict Signals and Non-Actionable Memory

**Files:**
- Modify: `tradingagents/agents/utils/rating.py:1-55`
- Modify: `tradingagents/graph/signal_processing.py:1-35`
- Modify: `tradingagents/agents/utils/memory.py:1-75`
- Modify: `tests/test_signal_processing.py`
- Modify: `tests/test_memory_log.py`

**Interfaces:**
- Produces: `parse_decision_signal(text: str) -> str` returning a five-tier rating, `Abstain`, or `Unavailable`.
- Compatibility: `parse_rating(text, default="Hold")` remains available for legacy callers.
- Memory format: actionable decisions use `pending`; Abstain and Unavailable use `no-outcome` and are excluded from `get_pending_entries()`.

- [ ] **Step 1: Write failing strict-signal tests**

```python
def test_explicit_abstain_and_unavailable_are_not_hold():
    assert parse_decision_signal("**Decision Status**: Abstain") == "Abstain"
    assert parse_decision_signal("**Decision Status**: Unavailable") == "Unavailable"


def test_unparseable_new_signal_is_unavailable():
    assert SignalProcessor().process_signal("Plain prose") == "Unavailable"


def test_legacy_labeled_rating_remains_readable():
    assert SignalProcessor().process_signal("**Rating**: Sell") == "Sell"
```

- [ ] **Step 2: Run strict-signal tests and verify RED**

Run: `python -m pytest tests/test_signal_processing.py -k "abstain or unavailable or legacy_labeled" -q`

Expected: explicit statuses and plain prose currently become Hold.

- [ ] **Step 3: Implement strict final-decision parsing**

Parse an explicit `Decision Status` label first. For actionable or legacy text, parse a five-tier rating with no default. Return `Unavailable` when neither a valid status nor rating exists. Update `SignalProcessor` to call this strict helper.

- [ ] **Step 4: Run signal tests and verify GREEN**

Run: `python -m pytest tests/test_signal_processing.py -q`

Expected: strict tests pass after updating the old default-Hold expectation; legacy `parse_rating` tests continue to pass.

- [ ] **Step 5: Write failing memory eligibility tests**

```python
@pytest.mark.parametrize("status", ["Abstain", "Unavailable"])
def test_non_actionable_decision_is_stored_without_pending_outcome(tmp_path, status):
    log = make_log(tmp_path)
    log.store_decision(
        "NVDA",
        "2026-08-14",
        f"**Decision Status**: {status}\n\n**Executive Summary**: No trade.",
    )
    entry = log.load_entries()[0]
    assert entry["rating"] == status
    assert entry["pending"] is False
    assert entry["raw"] == "no-outcome"
    assert log.get_pending_entries() == []
```

- [ ] **Step 6: Run memory eligibility tests and verify RED**

Run: `python -m pytest tests/test_memory_log.py -k "non_actionable_decision" -q`

Expected: current memory parser stores a fabricated Hold as pending.

- [ ] **Step 7: Persist explicit non-actionable records**

Use `parse_decision_signal`. Write `[date | ticker | <status> | no-outcome]` for Abstain and Unavailable, and the existing pending tag for five-tier ratings. Preserve idempotency and legacy tag parsing.

- [ ] **Step 8: Run signal and memory suites**

Run: `python -m pytest tests/test_signal_processing.py tests/test_memory_log.py -q`

Expected: all tests pass.

- [ ] **Step 9: Commit Task 4**

```bash
git add tradingagents/agents/utils/rating.py tradingagents/graph/signal_processing.py tradingagents/agents/utils/memory.py tests/test_signal_processing.py tests/test_memory_log.py
git commit -m "fix: distinguish abstention from hold in final signals"
```

---

### Task 5: Shared CLI and Web Target Profiles

**Files:**
- Modify: `cli/main.py:850-1165`
- Modify: `tradingagents/web/static/app.js:560-590`
- Modify: `tests/test_target_profile.py`
- Modify: `tests/test_web_service.py`
- Modify: `tests/test_web_static_tape.py`

**Interfaces:**
- Consumes: shared `finalize_portfolio_decision` or the lower-level typed target validator.
- Produces from `estimate_target_profile`: existing five keys plus `supporting_quote`, `target_validation_status`, and `target_rejection_reason`.
- Constraint: CLI and web continue sharing the same Python function; web must not fork validation logic.

- [ ] **Step 1: Write failing primary-profile provenance test**

```python
def test_primary_rendered_target_requires_supporting_quote_from_evidence():
    final_state = {
        "final_trade_decision": (
            "**Decision Status**: Actionable\n\n"
            "**Rating**: Buy\n\n"
            "**Price Target**: 999\n\n"
            "**Time Horizon**: 12 months\n\n"
            "**Decision Confidence**: 90/100\n\n"
            "**Target Rationale**: Large upside.\n\n"
            "**Target Supporting Quote**: Target 999"
        ),
        "market_report": "Verified close: 100. Resistance: 120.",
    }
    with patch("cli.main.fetch_reference_price", return_value=100.0):
        profile = estimate_target_profile(None, "TEST", "2026-08-14", final_state, "Buy")
    assert profile["price_target"] is None
    assert profile["target_rejection_reason"] == "supporting_quote_not_in_evidence"
```

- [ ] **Step 2: Run the primary-profile test and verify RED**

Run: `python -m pytest tests/test_target_profile.py -k "primary_rendered_target_requires" -q`

Expected: current primary parsing accepts 999 without provenance validation.

- [ ] **Step 3: Replace local validation with the shared boundary**

Remove `_fallback_target_has_provenance`. Parse all five target fields from rendered new decisions, including `Target Supporting Quote`, and build a typed draft. For older reports or missing fields, retain the existing constrained fallback prompt. Finalize primary and fallback bundles through the same shared validator and return its validation metadata.

- [ ] **Step 4: Add fallback and direction parity assertions**

Update existing target-profile tests so accepted bundles include `supporting_quote` and `Accepted`; rejected bundles assert the exact stable rejection reason. Preserve `None` for all dependent metrics whenever validation rejects the target.

- [ ] **Step 5: Run target-profile tests and verify GREEN**

Run: `python -m pytest tests/test_target_profile.py -q`

Expected: all primary and fallback provenance cases pass.

- [ ] **Step 6: Write failing web-serialization and display tests**

Assert `_run_job` retains `target_validation_status` and `target_rejection_reason` in serialized results. Assert `app.js` includes a visible `Target validation:` message when a rejection reason exists and does not show rejected target metrics.

- [ ] **Step 7: Run web tests and verify RED**

Run: `python -m pytest tests/test_web_service.py tests/test_web_static_tape.py -k "target_validation" -q`

Expected: validation metadata is not yet asserted or displayed.

- [ ] **Step 8: Implement web metadata display without new validation logic**

Use escaped server-returned rejection text in the result detail. Keep Python validation centralized in `estimate_target_profile`; JavaScript only presents the result.

- [ ] **Step 9: Run CLI/web target suites and Ruff**

Run: `python -m pytest tests/test_target_profile.py tests/test_web_service.py tests/test_web_static_tape.py tests/test_tui_result_summary.py -q`

Run: `python -m ruff check cli/main.py tradingagents/web/service.py tests/test_target_profile.py tests/test_web_service.py`

Expected: all tests pass and Ruff reports no errors.

- [ ] **Step 10: Commit Task 5**

```bash
git add cli/main.py tradingagents/web/static/app.js tests/test_target_profile.py tests/test_web_service.py tests/test_web_static_tape.py
git commit -m "fix: unify target validation across decision surfaces"
```

---

### Task 6: Integration Verification and Documentation

**Files:**
- Modify only if required by verified behavior: `README.md`
- Modify: `docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md` to check completed task boxes after evidence exists.

**Interfaces:**
- Verifies all Phase 1 contracts and backward compatibility.

- [ ] **Step 1: Run all focused decision-integrity tests**

Run:

```bash
python -m pytest \
  tests/test_decision_integrity.py \
  tests/test_portfolio_manager_integrity.py \
  tests/test_structured_agents.py \
  tests/test_signal_processing.py \
  tests/test_memory_log.py \
  tests/test_target_profile.py \
  tests/test_web_service.py \
  tests/test_web_static_tape.py \
  tests/test_tui_result_summary.py -q
```

Expected: zero failures.

- [ ] **Step 2: Run Ruff on every changed Python file**

Run:

```bash
python -m ruff check \
  tradingagents/agents/schemas.py \
  tradingagents/agents/utils/decision_integrity.py \
  tradingagents/agents/utils/structured.py \
  tradingagents/agents/managers/portfolio_manager.py \
  tradingagents/agents/utils/rating.py \
  tradingagents/graph/signal_processing.py \
  tradingagents/agents/utils/memory.py \
  cli/main.py \
  tests/test_decision_integrity.py \
  tests/test_portfolio_manager_integrity.py \
  tests/test_structured_agents.py \
  tests/test_signal_processing.py \
  tests/test_memory_log.py \
  tests/test_target_profile.py \
  tests/test_web_service.py \
  tests/test_web_static_tape.py
```

Expected: no Ruff errors.

- [ ] **Step 3: Run the full project suite**

Run: `python -m pytest -q`

Expected: zero failures; optional dependency skips are reported explicitly.

- [ ] **Step 4: Verify repository diff and contract coverage**

Run: `git diff --check`

Run: `git status --short`

Run: `git diff --stat main...HEAD`

Read the approved design line by line and confirm every Phase 1 requirement has a corresponding implementation and passing test. Record any gap rather than marking the plan complete.

- [ ] **Step 5: Update user documentation only if the visible output contract changed materially**

If README examples describe final decision output, update them to show explicit status and non-actionable outcomes. Do not add performance claims or model recommendations to this correctness branch.

- [ ] **Step 6: Commit verification documentation**

```bash
git add README.md docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md
git commit -m "docs: document explicit portfolio decision outcomes"
```

If README does not require a change, stage and commit only the plan checklist update.

---

## Future Option 3: Immutable `ForecastRecord` Roadmap

This roadmap is intentionally excluded from the current branch's production
changes. It preserves the approved long-term option as independently reviewable
projects whose interfaces build on Phase 1.

### Future Project A: Immutable Forecast Record

**Planned files:**
- Create `tradingagents/forecasting/schemas.py`
- Create `tradingagents/forecasting/record_factory.py`
- Create `tests/test_forecast_record.py`
- Modify graph state and Portfolio Manager integration only after a separate design review.

**Planned contract:** `ForecastRecord` contains canonical symbol, quote currency,
as-of timestamp, data cutoff, reference-price value/timestamp/adjustment basis,
normalized horizon sessions, expected absolute and excess return, p10/p50/p90,
direction probabilities, target range, invalidation conditions, evidence IDs,
missingness/data quality, provider/model/prompt/config/source hashes, decision
status, abstention reason, and Phase 1 validation results. Records are append-only
and content-addressable.

**Promotion tests:** stable serialization, hash determinism, target-bundle
compatibility, timezone boundaries, corporate-action basis, immutable snapshots,
and migration of legacy `PortfolioDecision` reports.

### Future Project B: Horizon-Aligned Outcome Scoring

**Planned files:**
- Create `tradingagents/evaluation/outcomes.py`
- Create `tradingagents/evaluation/scoring.py`
- Create `tradingagents/evaluation/calibration.py`
- Create `tests/test_forecast_outcomes.py`
- Create `tests/test_forecast_scoring.py`

**Planned contract:** resolve each record at its stored common-session horizon and
price basis. Compute deterministic signed direction, gross and benchmark-relative
return, target MAE/MAPE, interval coverage, Brier score, calibration error, costs,
turnover, drawdown, and missing-label reason. LLM reflection may explain a score
but cannot determine it.

### Future Project C: Walk-Forward Model and Role Evaluation

**Planned files:**
- Create `tradingagents/evaluation/walk_forward.py`
- Create `tradingagents/evaluation/leaderboard.py`
- Create `tests/test_walk_forward_evaluation.py`

**Planned contract:** point-in-time folds, immutable source snapshots, no overlap
between promotion and evaluation windows, regime/source-availability slices,
paired incumbent/challenger comparisons, minimum coverage, and explicit promotion
thresholds. Model aliases are exploratory; promoted configurations use pinned
model IDs and prompt/config hashes.

### Future Project D: Constrained Portfolio Optimizer

**Planned files:**
- Create `tradingagents/portfolio/state.py`
- Create `tradingagents/portfolio/risk_model.py`
- Create `tradingagents/portfolio/optimizer.py`
- Create `tests/test_portfolio_optimizer.py`

**Planned contract:** inputs include holdings, cash, benchmark weights, forecast
records, shrinkage covariance, liquidity, spreads/fees, position/sector/country
limits, turnover budget, and risk budget. The deterministic optimizer emits target
weights, expected net edge, risk contribution, constraint diagnostics, and a
no-trade result when edge does not exceed costs and uncertainty. LLM prose cannot
override hard constraints.

### Future Project E: Role-Specific Model Catalog and UI

**Planned files:**
- Modify `tradingagents/llm_clients/model_catalog.py`
- Modify `tradingagents/web/static/app.js`
- Modify `tradingagents/web/static/index.html`
- Add catalog and web capability tests.

**Planned contract:** preserve separate Quick, Deep, and Verifier/Reflection model
lists; expose only supported reasoning and sampling controls per model; omit
unsupported temperature/top-p/top-k parameters; pin promoted configurations; and
select defaults from the walk-forward role leaderboard rather than catalog order.
