# Shared Evidence and Company Enrichment Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task by task. Independent provider adapters can use superpowers:dispatching-parallel-agents.

**Goal:** Deliver original source evidence and additional filings/earnings data to every analysis and decision role.

**Architecture:** Collect optional SEC and FMP records once before the analyst sequence. Preserve original tool responses at analyst handoff in serializable graph state. Render bounded, source-labeled evidence in agent prompts, retain original reports for managers and the trader, and include source content in deterministic target validation and saved state.

**Tech Stack:** Python, LangGraph, Pydantic, requests, pytest; existing dependencies only.

**Spec:** The user approved the proposal in this task: shared evidence delivery plus filings and earnings data first. Options feeds and portfolio account connections are outside this implementation.

## Constraints

- Preserve unrelated working-tree changes; no publishing or commits requested.
- Preserve explicit provider selection/fallback behavior for existing tools.
- Do not fabricate missing evidence, source timestamps, units, credentials, or targets.
- Historical runs admit only information known by the analysis date. Providers without archives report unavailable.
- Provider failures must not prevent existing analysis from running; failures remain visible.
- Original source records and model-authored reports remain distinguishable.
- Keep prompts bounded and show when evidence is excerpted or omitted.

## Tasks

### 1. Source adapters

- [x] Add fixture-driven SEC adapter tests, then implement `get_company_filings(ticker, curr_date)` in `tradingagents/dataflows/sec.py`.
- [x] Add fixture-driven FMP adapter tests, then implement `get_earnings_context(ticker, curr_date)` in `tradingagents/dataflows/fmp.py`.
- [x] Adapters return lists of records with source, category, title, content, url, published_at, as_of_date, status. Missing credentials and coverage return explicit unavailable records.
- [x] Verify historical boundaries, financial reporting periods, partial failures, and credential-safe errors.

### 2. Shared evidence lifecycle

- [x] Add tests for collection, stable IDs, deduplication, cutoff enforcement, prompt bounds, and persistence through message clearing.
- [x] Implement `tradingagents/agents/utils/evidence.py` for evidence normalization, collection and rendering.
- [x] Register optional providers in `dataflows/interface.py` and defaults in `default_config.py`.
- [x] Initialize evidence in `graph/propagation.py`, declare it in `agent_states.py`, and collect it before analysts in `graph/setup.py`.
- [x] Preserve tool outputs in `agent_utils.create_msg_delete` and sentiment prefetch results in the sentiment node.

### 3. All-agent delivery and validation

- [x] Add a shared analysis-context helper and migrate all 12 agent roles to it.
- [x] Feed original analyst reports to the research manager, trader, and portfolio manager; keep source references in all decisions.
- [x] Extend deterministic decision evidence to include the same admitted source content.
- [x] Verify actual captured prompts for every agent family and a compiled workflow with mocked providers/LLMs.

### 4. Documentation and regression verification

- [x] Document configuration, supported coverage, source timestamps and unavailable states.
- [x] Run focused tests, Ruff, and the broader suite where the shared graph contracts require it.
- [x] Review the final diff for source loss, look-ahead leakage, unbounded prompts, and unrelated edits.
