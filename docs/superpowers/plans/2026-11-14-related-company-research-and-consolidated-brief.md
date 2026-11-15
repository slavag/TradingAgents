# Related-Company Research and Consolidated Brief Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development with spec and quality reviews. Preserve concurrent edits; no commits, pushes, or live provider/model calls during development.

**Goal:** Feed verified direct-competitor, supplier and customer evidence into decisions, with detailed investor coverage and concise complete consolidated summaries.

**Architecture:** A bounded research node after primary enrichment discovers cited relationships, validates entity identity and retrieves related facts/news into a dedicated typed state collection. Decision contexts can cite those sources without mixing their figures into primary-company facts or target anchors. A shared consolidated-brief formatter synthesizes all existing and new topics, keeping authoritative decision/horizon numbers in compact rows and full detail in linked reports.

**Tech Stack:** Existing Python/Pydantic/LangGraph, source adapters, pandas price history, static HTML, pytest. No new paid provider dependency.

## Task 1 — Scoped related-company collection

Files: new `tradingagents/related_companies/{__init__,schemas,research,providers}.py`, new `tests/test_related_company_research.py`.

- [x] Write RED fixtures for genuine direct competitors and named supplier/customer relationships, unsupported sector peers, accounting false positives, unresolved/private/anonymous entities, and multiple roles.
- [x] Implement `create_related_company_researcher(llm)` returning only `related_company_research`; preserve `shared_evidence` as primary-company data.
- [x] Discover with one source-bound structured pass, require exact admitted quotes and identity evidence, enforce a global nine-entity limit and three-peer default (up to five configured). Collect peer financial/performance context and seven-day news with thirty-day material coverage through configured providers. Bound calls/cache by entity/date/scope and preserve explicit unavailable coverage.
- [x] Export `render_related_context(bundle)`, `render_related_summary(bundle)`, and `admitted_related_sources(state, as_of)` for parent integration. Historical runs never use current quote/fundamental/identity snapshots.
- [x] Run focused tests; complete independent spec review then quality review, fixing material findings.

## Task 2 — Primary disclosure coverage and graph integration

Files: `dataflows/sec.py`, `dataflows/sec_documents.py`, `graph/setup.py`, `graph/trading_graph.py`, `agents/utils/agent_states.py`, `agents/utils/agent_utils.py`, `agents/utils/decision_integrity.py`, `agents/utils/outlooks.py`, `default_config.py`, `reporting.py`.

- [x] Add RED coverage for bounded IPO registration/prospectus business/competition evidence when annual filings are unavailable, without displacing annual/quarterly evidence for established issuers.
- [x] Preserve correct cutoff, unit/period/column handling. Do not fetch arbitrary model-generated URLs.
- [x] Add the related-company node after Evidence Collector and before sequential/parallel analyst fan-out. Configure bounded feature defaults and checkpoint signature/version. Add typed state field.
- [x] Supply labeled related-company contexts to all decision stages, admit their citations for outlook rationale, keep primary reference/target-candidate validation isolated, and fingerprint related inputs as evidence.
- [x] Test graph order, cutoff, disabled/partial failures, peer-figure/price isolation and evidence-change classification.

## Task 3 — Detailed report presentation

Files: new `related_companies/presentation.py`; `investor_deck.py`, `reporting.py`, `web/service.py`; tests `test_related_company_reports.py`.

- [x] Add RED fixtures for two compact peer-comparison views, separate supplier/customer developments, dates/units/currency/basis/coverage and conditional impact wording.
- [x] Render typed scoped results, preserve source links, distinguish disclosed relationships from new events, and avoid claims that no coverage means no risk.
- [x] Persist an immutable related-research artifact; propagate web results and detailed/full reports. Never generate peer recommendations or main-stock target anchors from peer prices.

## Task 4 — Concise complete consolidated reports

Files: new `tradingagents/consolidated_brief.py`; `cli/main.py`; tests `test_consolidated_brief.py` and existing report suites.

- [x] Add RED long-input tests for complete narrative synthesis and all critical decision/horizon/target/confidence facts, reaction session/time/reference, material related findings/coverage, conditional guidance and source/detail links.
- [x] Replace long outlook/marker/trader dumps with a shared concise per-stock brief and compact metrics table in Markdown and HTML. Target roughly 200–300 words plus structured rows; no fixed-character clipping or ellipses. Preserve material qualifications even if they require more text.
- [x] Use complete-sentence source-backed fallback when optional summarization is unavailable. Validate generated summary claims and do not rewrite authoritative numeric fields.
- [x] Keep detailed investor/full reports unchanged in depth and ensure Markdown/HTML parity.

## Task 5 — Integrated verification and delivery

- [x] Run scoped tests, full pytest, Ruff for changed Python files and `git diff --check`; run Node checks only if JavaScript changes.
- [x] Independently review integration for source/date/entity isolation, budget and summary correctness.
- [x] Produce an offline preview with clearly identified saved/fixture coverage; do not fabricate missing actual peer relationships or live news.
- [x] Load verified changes into the idle local app and check health without creating analysis jobs. Leave real-provider acceptance to a fresh user-initiated run.

The user approved implementation with “go” after clarifying decision use and consolidated-summary requirements. No further implementation permission gate is needed.

## Completion evidence

Implemented and independently reviewed source discovery, scoped collection, decision-context/graph integration, detailed presentation, immutable persistence, web output and complete consolidated summaries. Full suite: 1,677 passed, two skipped. Ruff checked all 49 changed/new Python files; diff checks passed. Visual inspection found no horizontal overflow and confirmed no standalone extended-hours display. The idle local app was reloaded and health checked. No live provider/model calls, commits or pushes during development. Historical related secondary retrieval remains unavailable until stable as-of issuer/adjustment support exists.
