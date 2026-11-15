# Investor Deck Clarity and Outlooks Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development for isolated tasks and review; preserve concurrent edits and do not commit or push without user authorization.

**Goal:** Correct CBRS's financial-table contamination and thin slides, and add independent six-/twelve-month outlooks for future research runs.

**Architecture:** SEC extraction owns complete, attributable tables. The research ledger converts labeled multi-row period tables into typed facts. The existing deck builds richer deterministic source-only slides. A focused outlook module validates provider-facing horizon drafts using the existing target validator and persists independent outlooks beside the unchanged tactical decision.

**Tech Stack:** Python, existing Pydantic models, static HTML deck renderer, pytest; no new dependencies or paid calls.

## Task 1 — SEC table fidelity and ledger periods

Files: `tradingagents/dataflows/sec_documents.py`, `tradingagents/agents/utils/evidence.py`, `tradingagents/research_ledger.py`; tests `test_sec_documents.py`, `test_research_ledger.py`, `test_shared_evidence.py`.

- [x] Add RED tests for preceding-table contamination and multi-row three-/nine-month 2026/2025 statement headers; assert 210809000 Q3 revenue versus 584324000 nine-month revenue, explicit USD scale, and intact comparison columns.
- [x] Preserve owning captions/unit context and full bounded logical columns; exclude prior-table body text. Parse separate duration/year column groups and financial labels, preserving GAAP versus adjusted labels. Reject unsupported header mappings.
- [x] Run focused extraction/ledger/evidence tests and review spec compliance then implementation quality.

## Task 2 — Readable, substantive source-only slides

Files: `tradingagents/investor_deck.py`; tests `test_investor_deck.py` plus a new saved-CBRS fixture acceptance test.

- [x] Add RED tests excluding reconciliation prose from relationships, selecting relevant quarterly company narrative, and distinct headline/body text.
- [x] Keep accounting tables in financial topics, choose commercial relationship evidence only, and supply deterministic explanatory context/counterarguments where facts support them. Preserve source attribution and avoid generic filler or unsupported names.
- [x] Verify existing PENG/AMD and new CBRS source-only cases. Review spec compliance then quality.

## Task 3 — Typed independent outlooks

Files: new `tradingagents/agents/utils/outlooks.py`; `agents/schemas.py`, `agents/managers/portfolio_manager.py`, `agents/utils/decision_integrity.py`, `agents/utils/agent_states.py`.

- [x] Add RED tests for six/twelve calendar month endpoints including end-of-month, independent ratings and confidence, grounded point/range model estimates, missing/duplicate horizons and invalid targets.
- [x] Define horizon drafts with month literal 6/12, recommendation/confidence, target bundle, rationale, upside/downside, drivers and invalidation. Keep these separate from tactical targets. Validate through `finalize_portfolio_decision` and preserve only admitted targets, with explicit diagnostics for missing outputs.
- [x] Ask for both horizons in the existing PM structured call; use at most one horizon-only bounded repair when needed. A failed horizon repair cannot change tactical decision fields.

## Task 4 — Outlook propagation and rendering

Files: `cli/main.py`, `tradingagents/web/service.py`, `tradingagents/reporting.py`, new outlook slide helper or `tradingagents/investor_deck.py` integration.

- [x] Add RED tests for deck, full report and web JSON containing independent outlooks with confidence, basis/assumptions, exact endpoint and reference date, and model-estimate labeling. Legacy reports must not acquire invented forecasts.
- [x] Persist outlook records in PM state/run artifacts, render two clear outlook sections before the final tactical decision, and preserve tactical forecast/evaluation behavior. Use existing typed deck blocks for readable tables/points where possible.

## Task 5 — Offline CBRS preview and final verification

- [x] Build a new local preview from saved run `49f3606169034753a019ffe36fe5e59b`, restoring cached source HTML where available. Do not overwrite original reports or invent missing six-/twelve-month forecasts.
- [x] Run targeted tests, full `pytest -q --disable-warnings`, changed-file Ruff, existing Node tests if frontend JS changes, and `git diff --check`.
- [x] Review the full diff for source correctness and backward compatibility; inspect rendered preview; restart/start idle local app and check `/api/health` without creating analysis jobs.

No paid provider calls, commits, or pushes. User approved the design with “go”; proceed through all tasks without another implementation permission gate.

## Completion evidence

Implemented and independently reviewed source fidelity, deck clarity, horizon contracts and extended-hours marker. RED/GREEN regressions cover malformed tables, signs, period ownership, misleading loss wording, forecast isolation, currency/reference provenance, producer-to-deck reaction evidence, and calendar/session validity. Full offline suite: 1,601 passed, two skipped. No frontend JavaScript changed; Python HTML rendering and browser inspection cover the presentation change. Saved CBRS preview visually inspected; live numeric horizons and real extended quote availability require a fresh user-initiated run. No paid calls, commits or pushes.
