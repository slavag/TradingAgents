# Source-Only Banner Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the investor-facing “Interpretation withheld” banner while retaining the narration validation gate and saving a bounded internal reason code.

**Architecture:** `build_investor_deck` retains its all-or-nothing optional-narration check and adds a typed reason to `InvestorDeck`. The HTML renderer ignores that generator state. Report writing persists only the status and reason in the new run manifest; the web result exposes the same codes for operator diagnostics.

**Tech Stack:** Python 3.13, Pydantic, pytest, Ruff.

**Spec:** `docs/superpowers/specs/2026-10-12-source-only-banner-design.md`

## Global Constraints

- Never publish invalid optional prose, raw model responses, or prompt text.
- Preserve the source-backed deck and the thin-source unavailable page.
- Do not rewrite saved reports or run PENG/SMXT for deterministic tests.
- Leave changes uncommitted unless separately authorized.

## Review Focus

- A model returns a syntactically valid but unsupported narrative: exclude all optional prose, render facts, and record `unsupported_interpretation`.
- A model returns malformed structured output or an unknown exception code: save a bounded `invalid_structured_response` code, not exception content.
- The LLM is unavailable: show the rich deterministic deck without banner and record `model_unavailable`.
- There is insufficient source coverage: keep the unavailable page, with no fabricated complete deck.
- Multiple tickers share one batch: each report/manifest/result must retain its own reason code.

---

### Task 1: Internal synthesis reason and banner-free rendering

**Files:**
- Modify: `tradingagents/investor_deck.py:115-121,955-1018`
- Modify: `tradingagents/investor_deck_html.py:404-427`
- Test: `tests/test_investor_deck.py`
- Test: `tests/test_investor_deck_html.py`

**Interfaces:**
- Produces: `InvestorDeck.synthesis_reason: str | None`; `build_investor_deck(...)` still returns `InvestorDeck | None`.
- Retains: `synthesis_status` and `render_unavailable_investor_deck(...)` behavior.

- [ ] **Step 1: Write failing tests.** In `test_model_missing_required_topic_keeps_only_validated_source_deck`, assert `deck.synthesis_reason == "required_topics_missing"`. Add a `peng_deck_fixture()` rendering case that sets `synthesis_status="source_only", synthesis_reason="unsupported_interpretation"`, then asserts `"Interpretation withheld" not in html`, `"Optional deck narration" not in html`, and `"FY2026 Q4" in html`. In the unsupported-prose test, assert `deck.synthesis_reason == "unsupported_interpretation"` and `"900%" not in render_investor_deck(deck, "20261009T154737Z")`. Feed a fake `StructuredOutputFailure("secret-provider-text")` and assert persisted reason is `invalid_structured_response`.
- [ ] **Step 2: Prove RED.** Run `pytest -q tests/test_investor_deck.py tests/test_investor_deck_html.py`; expect the missing field or banner assertion to fail.
- [ ] **Step 3: Implement minimal change.** Add `synthesis_reason: str | None = None` to `InvestorDeck`; set it to an application-defined code in the nested `result(...)` helper. Accept known application and `StructuredOutputFailure` codes only; normalize unknown text to `invalid_structured_response`. Keep the existing validation returns. Remove only the `if deck.synthesis_status == "source_only":` banner-prepend block in `render_investor_deck`; do not loosen `_valid_point` or `_safe_interpretation`.

  ```python
  class InvestorDeck(BaseModel):
      # Keep the existing fields unchanged.
      synthesis_status: Literal["complete", "source_only"] = "complete"
      synthesis_reason: str | None = None

  # In build_investor_deck.result(...), with known codes declared once:
  KNOWN_SYNTHESIS_REASONS = frozenset({
      "model_unavailable", "topic_budget", "required_topics_missing",
      "source_id_mismatch", "unsupported_interpretation",
      "unsupported_factual_point", "structured_binding_unsupported",
      "structured_response_invalid", "structured_invocation_failed",
      "structured_response_missing", "invalid_structured_response",
  })
  safe_reason = reason if reason in KNOWN_SYNTHESIS_REASONS else "invalid_structured_response"
  return InvestorDeck(
      ticker=ticker, analysis_date=analysis_date,
      headline=f"{ticker} investor research",
      slides=[*outline, decision_slide] if decision_slide is not None else outline,
      sources=dict(ledger.sources), synthesis_status=status,
      synthesis_reason=safe_reason if reason else None,
  )
  # render_investor_deck passes `slides` directly into `_document`.
  ```
- [ ] **Step 4: Prove GREEN.** Run the two test modules and inspect the HTML assertion output.

### Task 2: Persist bounded reason without leaking model output

**Files:**
- Modify: `tradingagents/reporting.py:207-250`
- Modify: `tradingagents/web/service.py:1108-1195`
- Test: `tests/test_reporting.py`
- Test: `tests/test_web_service.py`

**Interfaces:**
- Consumes: `InvestorDeck.synthesis_status` and `.synthesis_reason` from Task 1.
- Produces: `run_manifest.json` keys `investor_deck_synthesis_status` and `investor_deck_synthesis_reason`, and matching per-ticker web result keys.

- [ ] **Step 1: Write failing tests.** Save a two-slide `InvestorDeck` with `source_only/unsupported_interpretation` and `run_metadata={"run_id": "job-1"}` through `write_report_tree`; assert the manifest contains exactly the two synthesis fields and omits a supplied sentinel raw model phrase. In the web service fake-job test, make the fake builder return a source-only deck and assert the serialized PENG result carries `unsupported_interpretation` while an unaffected ticker carries `complete`/null.
- [ ] **Step 2: Prove RED.** Run `pytest -q tests/test_reporting.py tests/test_web_service.py`; expect missing manifest/result fields.
- [ ] **Step 3: Implement minimal change.** Before writing `run_manifest.json`, add `investor_deck_synthesis_status = investor_deck.synthesis_status if investor_deck else "unavailable"` and `investor_deck_synthesis_reason = investor_deck.synthesis_reason if investor_deck else None` to the manifest. At the web per-ticker result construction, add the same two scalar fields; include them in `serialize_result`'s whitelist/default result. Log the code, never the model text. Do not put them into investor HTML.

  ```python
  if manifest is not None:
      manifest["investor_deck_synthesis_status"] = (
          investor_deck.synthesis_status if investor_deck else "unavailable"
      )
      manifest["investor_deck_synthesis_reason"] = (
          investor_deck.synthesis_reason if investor_deck else None
      )
  # The web result uses the same two scalar expressions for that ticker.
  ```
- [ ] **Step 4: Prove GREEN.** Run the focused tests and inspect one generated manifest fixture.

### Task 3: Boundary regression verification

**Files:**
- Test: `tests/test_investor_deck.py`
- Test: `tests/test_investor_deck_html.py`
- Test: `tests/test_reporting.py`

**Interfaces:** No new production interface; this task exercises the Task 1/2 contract.

- [ ] **Step 1: Add a thin-source assertion.** In `test_analysis_only_evidence_cannot_publish_a_research_deck`, keep the existing `is None` assertion and add a `render_unavailable_investor_deck` assertion in the HTML suite that it says the investor report is unavailable without `Interpretation withheld`.
- [ ] **Step 2: Prove GREEN.** Run `pytest -q tests/test_investor_deck.py tests/test_investor_deck_html.py tests/test_reporting.py tests/test_web_service.py`.
- [ ] **Step 3: Verify project checks.** Run `pytest -q`, `ruff check tradingagents/investor_deck.py tradingagents/investor_deck_html.py tradingagents/reporting.py tradingagents/web/service.py tests/test_investor_deck.py tests/test_investor_deck_html.py tests/test_reporting.py tests/test_web_service.py`, and `git diff --check`. Report any unrelated baseline failure separately; do not commit.
