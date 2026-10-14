# Actionable Target or Range Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every newly generated actionable stock decision contains a validated point target or target range, including low-confidence scenarios; otherwise it becomes non-actionable after one bounded repair attempt.

**Architecture:** The Portfolio Manager proposes either a point or range with a horizon, method, risk, confidence, and exact eligible price-level quotes. A deterministic validator accepts or rejects the bundle against as-of-admissible market snapshot rows. The PM node alone enforces one target-only repair and converts unresolved actionable drafts to Abstain; forecast, CLI/web, and investor views read the same typed final decision.

**Tech Stack:** Python 3.13, Pydantic, LangChain structured output, pytest, Ruff.

**Spec:** `docs/superpowers/specs/2026-10-12-actionable-target-range-design.md`

## Global Constraints

- An observed price level is an anchor, not an automatic forward forecast.
- A technical range is labeled a scenario, not intrinsic fair value.
- Target confidence is an uncalibrated evidence-strength score from 0 to 100; no minimum score.
- Never infer a midpoint or expected return from a range-only target.
- One target-only repair call maximum; it cannot change rating, actions, facts, or evidence.
- If no validated target/range follows repair, withhold the actionable rating. Preserve the other research outputs.
- Do not rewrite historical reports, run PENG/SMXT, make paid model calls for tests, or commit without separate authorization.
- Older saved accepted point targets lacking new method/risk fields must remain readable; stricter explanation requirements apply at the new PM execution boundary.

## Review Focus

- A price quote copied from unverified debate text: reject it even if textually present in the evidence corpus.
- A market quote dated after analysis: exclude it from eligible candidates and reject the target.
- A Hold range straddles the close: accept if both endpoints and rationale are supported.
- A Buy/Sell range crosses or opposes the reference price: reject as directionally inconsistent.
- A target-only repair changes rating/actions or returns malformed output: ignore those changes; one retry only, then Abstain.

---

### Task 1: Typed point/range contract and Markdown

**Files:**
- Modify: `tradingagents/agents/schemas.py:71-80,405-749`
- Test: `tests/test_decision_integrity.py`
- Test: `tests/test_structured_agents.py`

**Interfaces:**
- Produces: `PortfolioDecisionDraft` and `PortfolioDecision` with `target_low`, `target_high`, `supporting_quote_low`, `supporting_quote_high`, `target_method`, `target_risks`; retains existing point fields. Add a target-only `PortfolioTargetRepair` model containing only target fields.
- `render_pm_decision(decision)` emits `Price Target Range`, `Target Method`, `Target Risks`, and both bound-source labels while preserving existing point labels.

- [ ] **Step 1: Write RED schema tests.** Construct `actionable_draft(target_low=72.83, target_high=77.10, supporting_quote_low="| Low | 72.83 |", supporting_quote_high="| High | 77.10 |", time_horizon="3 months", confidence_score=18, target_summary="A conditional technical recovery scenario.", target_method="Technical scenario", target_risks="The breakout may fail.")`. Assert draft keeps confidence 18 and `PortfolioTargetRepair.model_fields` contains no `rating`, `existing_position_action`, or `new_position_action`. Assert `PortfolioDecision` rejects mixed point/range accepted bundles and incomplete accepted ranges. Assert Markdown renders full, untrimmed rationale/risk and both endpoints.
- [ ] **Step 2: Prove RED.** Run `pytest -q tests/test_decision_integrity.py tests/test_structured_agents.py`; expect missing fields/model.
- [ ] **Step 3: Implement the typed fields.** Keep draft target fields optional so incomplete model output reaches deterministic validation. Add `PortfolioTargetRepair(BaseModel)` with only target fields and `model_config = ConfigDict(extra="forbid")` so rating/action attempts fail. In `PortfolioDecision._validate_final_contract`, the legacy point bundle is the existing five fields; the range bundle is low/high, both quotes, horizon/confidence/summary/method/risks. Accepted requires exactly one complete shape; nonaccepted retains no target field. Do not make old point decisions unreadable. Extend `render_pm_decision` only with non-null labels.

  ```python
  target_low: float | None = None
  target_high: float | None = None
  supporting_quote_low: str | None = None
  supporting_quote_high: str | None = None
  target_method: str | None = None
  target_risks: str | None = None
  # Existing point acceptance remains valid: price_target, time_horizon,
  # confidence_score, target_summary, supporting_quote.
  ```
- [ ] **Step 4: Prove GREEN.** Run the two test modules; retain the current recommendation-confidence and position-guidance guards.

### Task 2: Deterministic verified-candidate validation

**Files:**
- Modify: `tradingagents/agents/utils/decision_integrity.py:80-306`
- Modify: `tradingagents/agents/schemas.py:71-80`
- Test: `tests/test_decision_integrity.py`

**Interfaces:**
- Consumes: Task 1 target fields.
- Produces: `finalize_portfolio_decision(draft, evidence_text, reference_price=None, *, eligible_quotes: list[str] | None = None) -> PortfolioDecision`. The PM path must pass `verified_target_candidates(state)`; legacy callers may omit the new keyword for compatibility.

- [ ] **Step 1: Write RED validation tests.** Use `eligible_quotes=["| Low | 72.83 |", "| High | 77.10 |", "| Close | 75.74 |"]`. Assert a Hold 72.83–77.10 range with confidence 18 is Accepted, has no point target, and preserves method/risks. Assert reversed endpoints, same quote for both bounds, one absent quote, a bound not in `eligible_quotes`, a Buy range entirely below 75.74, a post-date/noncandidate quote, mixed point/range, nonpositive/NaN endpoints, and blank method/risk explanations all become Rejected with stable reason codes. Include the saved PENG candidate rows as fixture data, not a paid run.
- [ ] **Step 2: Prove RED.** Run `pytest -q tests/test_decision_integrity.py`; expect new cases to fail.
- [ ] **Step 3: Implement minimal validation.** Extend `_target_fields` and `_base_fields`; make `_quote_rejection_reason` reusable for each bound. When `eligible_quotes is not None`, require normalized quote membership in that list in addition to exact number/price-context match; that list inherits as-of filtering from `verified_target_candidates`. Restrict existing `_find_exact_price_quote` auto-correction to the same eligible list so a noncandidate evidence line cannot bypass this gate. For a range require `low <= high`; apply Buy/Overweight lower bound `> reference_price` and Sell/Underweight upper bound `< reference_price`, while Hold may bracket. Require nonblank method, rationale, and risks for new range bundles; do not pretend deterministic code can judge the economic persuasiveness of prose. Do not derive an automatic range from available rows. Use distinct stable `TargetValidationReason` enum values for mixed shape, reversed bounds, ineligible quote, and unsupported method. Populate one accepted shape or `_without_target` on rejection.

  ```python
  if eligible_quotes is not None and normalized_quote not in {
      _normalize_whitespace(row) for row in eligible_quotes
  }:
      return TargetValidationReason.SUPPORTING_QUOTE_NOT_ELIGIBLE
  if draft.target_low is not None and draft.target_high is not None:
      if draft.target_low > draft.target_high:
          return TargetValidationReason.TARGET_RANGE_REVERSED
  ```
- [ ] **Step 4: Prove GREEN.** Run `pytest -q tests/test_decision_integrity.py`; inspect that existing point-target behavior remains accepted when its quote is eligible.

### Task 3: PM-only bounded repair and safe Abstain

**Files:**
- Modify: `tradingagents/agents/managers/portfolio_manager.py:62-169`
- Modify: `tradingagents/agents/schemas.py` for `PortfolioTargetRepair`
- Test: `tests/test_structured_agents.py`
- Test: `tests/test_structured_agent_prompts.py`

**Interfaces:**
- Consumes: `PortfolioTargetRepair`, eligible quotes, and Task 2 validator.
- Produces: a `portfolio_decision` that is actionable only when `target_validation_status == Accepted` for this new PM execution path. Existing direct `finalize_portfolio_decision` callers retain diagnostic Not Proposed/Rejected results.

- [ ] **Step 1: Write RED fake-LLM tests.** Feed an initial actionable PENG-style draft with all target fields null, then a target-only structured response with Hold range 72.83–77.10, horizon 3 months, confidence 18, method, risks, rationale, and exact rows; assert two model calls total and unchanged rating/actions. Feed an invalid repair and assert exactly one repair call, `status=Abstain`, `rating=None`, all position actions null, and `status_reason="target_unavailable"`. Feed an initially accepted point target and assert no repair call. Feed a repair payload containing `rating="Buy"` and assert it cannot change the original rating.
- [ ] **Step 2: Prove RED.** Run `pytest -q tests/test_structured_agents.py tests/test_structured_agent_prompts.py`; expect the no-target actionable case to fail.
- [ ] **Step 3: Implement minimal PM boundary.** Bind a second structured chain with `PortfolioTargetRepair` only when the first actionable decision is not Accepted. The repair prompt includes original rating/thesis, target validation reason, and `candidate_lines`, and says all non-target fields are frozen. Apply only `repair.model_dump()` via `draft.model_copy(update=...)`, call `finalize_portfolio_decision(..., eligible_quotes=target_candidates)` again, and convert a still-unaccepted or malformed repair to `PortfolioDecision(status=DecisionStatus.ABSTAIN, rating=None, executive_summary="No actionable recommendation: a defensible price target or range was unavailable.", investment_thesis="The source-backed research remains available, but the target requirement was not met.", status_reason="target_unavailable")`. New PM decisions additionally require a nonblank method/risk explanation even for a point target; old saved point decisions are accepted by the model. Do not reuse arbitrary model prose in the abstention. Existing upstream-Unavailable behavior remains unchanged.

  ```python
  decision = finalize_portfolio_decision(
      draft, evidence, reference_price=reference_price,
      eligible_quotes=target_candidates,
  )
  if draft.status is DecisionStatus.ACTIONABLE and (
      decision.target_validation_status is not TargetValidationStatus.ACCEPTED
      or not decision.target_method or not decision.target_risks
  ):
      repair_llm = bind_structured(
          llm, PortfolioTargetRepair, "Target Repair", fallback_to_text=False,
      )
      repair_prompt = (
          f"Original rating: {draft.rating.value}; thesis: {draft.investment_thesis}\n"
          f"Validation reason: {decision.target_validation_reason}\n"
          f"Eligible price rows:\n{candidate_lines}\n"
          "Return target fields only. Preserve the original rating and actions."
      )
      try:
          repair = invoke_structured_required(repair_llm, repair_prompt, "Target Repair")
          repaired = draft.model_copy(update=repair.model_dump())
          decision = finalize_portfolio_decision(
              repaired, evidence, reference_price=reference_price,
              eligible_quotes=target_candidates,
          )
      except (StructuredOutputFailure, ValidationError, TypeError, ValueError):
          decision = PortfolioDecision(
              status=DecisionStatus.ABSTAIN, rating=None,
              executive_summary="No actionable recommendation: a defensible price target or range was unavailable.",
              investment_thesis="The source-backed research remains available, but the target requirement was not met.",
              status_reason="target_unavailable",
          )
      if decision.status is DecisionStatus.ACTIONABLE and (
          decision.target_validation_status is not TargetValidationStatus.ACCEPTED
          or not decision.target_method or not decision.target_risks
      ):
          decision = PortfolioDecision(
              status=DecisionStatus.ABSTAIN, rating=None,
              executive_summary="No actionable recommendation: a defensible price target or range was unavailable.",
              investment_thesis="The source-backed research remains available, but the target requirement was not met.",
              status_reason="target_unavailable",
          )
  ```
- [ ] **Step 4: Prove GREEN.** Run the focused structured-agent tests and inspect exact fake-LLM call counts; no live provider call.

### Task 4: Forecast record and result projection

**Files:**
- Modify: `tradingagents/forecasting/schemas.py:180-234`
- Modify: `tradingagents/forecasting/record_factory.py:211-337`
- Modify: `cli/main.py:901-1154,1295-1320`
- Modify: `tradingagents/web/service.py:469-567`
- Test: `tests/test_reporting.py`
- Test: `tests/test_web_service.py`

**Interfaces:**
- Consumes: accepted point/range typed decision from Task 3.
- Produces: `ForecastRecordPayload` with point `central_target` or range `target_low/target_high`; the web and CLI result dictionaries use `target_low`/`target_high` alongside existing point keys.

- [ ] **Step 1: Write RED projection tests.** For an accepted range-only typed decision, assert forecast `central_target is None`, low/high match the decision, `expected_return is None`, `"target_range" not in missing_fields`, and JSON serialization succeeds. Assert a point target still computes expected return. Assert `estimate_target_profile` returns the range fields from an explicit typed decision without invoking its legacy fallback LLM; assert the serialized web result returns low/high, horizon, method, risk, and confidence.
- [ ] **Step 2: Prove RED.** Run `pytest -q tests/test_reporting.py tests/test_web_service.py tests/test_decision_integrity.py`; expect range record/result assertions to fail.
- [ ] **Step 3: Implement conservative projection.** In forecast schema, change Accepted validation to require exactly a complete point or complete range; reject target values on non-accepted status. In factory, read `target_low/high` only for Accepted, set `central_target` only for points, remove `target_range` from `missing_fields` only for accepted ranges, and leave `expected_return` null for a range. In `estimate_target_profile`, parse range values from `final_state["portfolio_decision"]` for typed decisions and preserve the no-fallback path when a rendered Decision Status exists. Add low/high/method/risk to CLI and `serialize_result` result fields without ever publishing rejected values.

  ```python
  accepted = target_status == "Accepted"
  central_target = Decimal(str(decision["price_target"])) if accepted and decision.get("price_target") is not None else None
  target_low = Decimal(str(decision["target_low"])) if accepted and decision.get("target_low") is not None else None
  target_high = Decimal(str(decision["target_high"])) if accepted and decision.get("target_high") is not None else None
  expected_return = None  # Change only for a point target with a reference price.
  ```
- [ ] **Step 4: Prove GREEN.** Run focused forecast/report/web/CLI tests and compare range versus point JSON.

### Task 5: Investor and consolidated report presentation

**Files:**
- Modify: `tradingagents/investor_deck.py:78-97,653-697`
- Modify: `tradingagents/investor_deck_html.py:181-244`
- Modify: `tradingagents/reporting.py:207-383`
- Modify: `cli/main.py:1295-1320,1725-1750`
- Test: `tests/test_investor_deck.py`
- Test: `tests/test_investor_deck_html.py`
- Test: `tests/test_reporting.py`

**Interfaces:**
- Consumes: Task 1-4 typed and forecast fields.
- Produces: final decision slide and report summaries showing either the validated point or explicitly labeled range, full rationale, method, risks, horizon, confidence, and exact endpoint source quotes; Abstain shows no actionable rating.

- [ ] **Step 1: Write RED rendering tests.** Build an accepted range-only `PortfolioDecision` in `state["portfolio_decision"]`; assert `DecisionBlock.target is None`, low/high match, and HTML contains `72.83–77.10`, `Technical scenario`, `3 months`, `18/100`, the full rationale and risk sentences, with no `No validated price target`. Assert the two exact source rows appear in the technical/Markdown report and no number is shown when validation is Rejected. Assert source-only decks retain a valid target/range and an Abstain slide says no actionable recommendation.
- [ ] **Step 2: Prove RED.** Run `pytest -q tests/test_investor_deck.py tests/test_investor_deck_html.py tests/test_reporting.py`; expect missing range fields/output.
- [ ] **Step 3: Implement minimal rendering.** Add `target_low/high`, `target_method`, `target_risks`, and endpoint quotes to `DecisionBlock`; fill them only when the typed decision is Accepted. In `_render_decision`, display a range line when both endpoints exist and a point line only when `target` exists; preserve escaping and full text. Keep `No validated price target` only for old actionable saved states without accepted target. Extend Markdown/CLI summary labels for ranges without substituting a midpoint. The existing report writer must render the same typed final decision, never raw draft values.

  ```python
  if block.target_low is not None and block.target_high is not None:
      target = f"{block.target_low:.2f}–{block.target_high:.2f}"
  elif block.target is not None:
      target = f"{block.target:.2f}"
  else:
      target = "No validated price target"
  ```
- [ ] **Step 4: Prove GREEN.** Run focused investor/CLI/report tests and inspect generated local HTML with a fake fixture.

### Task 6: Cross-layer verification

**Files:**
- Test: `tests/test_decision_integrity.py`
- Test: `tests/test_structured_agents.py`
- Test: `tests/test_investor_deck_acceptance.py`
- Test: `tests/test_investor_deck_html.py`
- Test: `tests/test_reporting.py`
- Test: `tests/test_web_service.py`

**Interfaces:** No new production interface; verifies Tasks 1-5 together.

- [ ] **Step 1: Add the offline PENG acceptance fixture.** Use the saved candidate rows `| Low | 72.83 |`, `| Close | 75.74 |`, `| High | 77.10 |` in the fake PM, record, and investor deck; assert a low-confidence, evidence-backed Hold range survives from structured draft to HTML and serialized web result. Add a no-candidate variant asserting safe Abstain after one repair.
- [ ] **Step 2: Prove GREEN.** Run `pytest -q tests/test_decision_integrity.py tests/test_structured_agents.py tests/test_investor_deck_acceptance.py tests/test_investor_deck_html.py tests/test_reporting.py tests/test_web_service.py`.
- [ ] **Step 3: Run project checks.** Run `pytest -q`, `ruff check tradingagents/agents/schemas.py tradingagents/agents/utils/decision_integrity.py tradingagents/agents/managers/portfolio_manager.py tradingagents/forecasting/schemas.py tradingagents/forecasting/record_factory.py tradingagents/investor_deck.py tradingagents/investor_deck_html.py tradingagents/reporting.py tradingagents/web/service.py cli/main.py tests/test_decision_integrity.py tests/test_structured_agents.py tests/test_investor_deck_acceptance.py tests/test_investor_deck_html.py tests/test_reporting.py tests/test_web_service.py`, and `git diff --check`. Do not launch the app or pay for a model run; ask for a fresh user-run PENG-only URL for final visual acceptance.
