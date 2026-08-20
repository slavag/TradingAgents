# Role Model Promotion and UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Select Quick, Deep, and Verifier/Reflection defaults from pinned walk-forward promotion results and expose model capabilities, evaluation coverage, and promotion status in the web UI.

**Architecture:** Extend the shared catalog with explicit role-specific options and capability metadata. Persist immutable role leaderboards in a promotion registry, resolve selected pinned defaults through the backend, and inject both catalog and promotion state into one UI. The browser presents results; all promotion and capability decisions remain deterministic Python contracts.

**Tech Stack:** Python 3.10+, Pydantic v2, FastAPI, vanilla JavaScript/CSS, pytest.

**Spec:** `docs/superpowers/plans/2026-08-14-decision-integrity-foundation.md`

## Global Constraints

- Roles are exactly `quick`, `deep`, and `verifier` in stored configuration.
- Verifier/Reflection replaces the misleading Final Report UI label while preserving request/config compatibility keys.
- Promoted identities require pinned provider/model, prompt hash, and config hash.
- Missing or invalid promotion state falls back to configured defaults, not catalog order silently.
- Capability controls are shown only when at least one selected role supports them; unsupported parameters remain filtered by provider clients.
- Evaluation metrics include coverage/sample counts and carry no performance guarantee.
- Promotion writes are append-only by leaderboard content.

### Task 1: Role-Specific Catalog and Capability Contract

**Files:** `tradingagents/llm_clients/model_catalog.py`, `tests/test_web_model_catalog.py`.

- [ ] Add failing tests for Quick/Deep/Verifier options and temperature/reasoning/thinking capability metadata.
- [ ] Implement `get_role_model_options()` and `get_model_capabilities()` with custom-provider fallbacks.
- [ ] Preserve legacy merged web catalog and known-model validation.
- [ ] Run catalog tests and Ruff.
- [ ] Commit as `feat: define role-specific model capabilities`.

### Task 2: Immutable Promotion Registry

**Files:** `tradingagents/evaluation/promotion_registry.py`, `tradingagents/evaluation/__init__.py`, `tests/test_model_promotion_registry.py`.

- [ ] Write failing append-only write/read, conflict, corrupt, role, and fallback tests.
- [ ] Implement `ModelPromotionRegistry.write_leaderboard`, `read_leaderboard`, and `selected_defaults`.
- [ ] Run focused tests and Ruff.
- [ ] Commit as `feat: persist promoted model defaults`.

### Task 3: Backend Defaults and Promotion Entry Points

**Files:** `tradingagents/web/app.py`, `tradingagents/web/service.py`, `cli/main.py`, `tests/test_web_model_catalog.py`, `tests/test_model_promotion_registry.py`.

- [ ] Add failing tests for injected promoted defaults, fallback configured defaults, role-model API, and CLI leaderboard import.
- [ ] Inject role options, capabilities, evaluation summary, and promoted defaults into the index response.
- [ ] Add read-only status API plus append-only CLI promotion import.
- [ ] Run backend/catalog/registry tests.
- [ ] Commit as `feat: apply promoted role model defaults`.

### Task 4: Unified Evaluation and Model UI

**Files:** `tradingagents/web/static/index.html`, `app.js`, `styles.css`, `tests/test_web_static_tape.py`, `tests/test_web_model_catalog.py`.

- [ ] Add failing static-contract tests for Verifier/Reflection copy, promoted badges, capability-aware controls, evaluation counts, and Run Evaluation action.
- [ ] Render role-specific options and promoted defaults without overriding user changes.
- [ ] Add evaluation status panel calling `/api/evaluations/run` and present coverage/counts without return claims.
- [ ] Add accessible status labels and responsive styling.
- [ ] Run web/static tests and commit as `feat: show evaluated model promotions`.

### Task 5: Final Roadmap Verification

**Files:** all Option 3 plans and user documentation.

- [ ] Run all forecasting, evaluation, optimizer, catalog, web, CLI, and full-suite tests.
- [ ] Run Ruff and diff checks.
- [ ] Mark Project E and the full approved roadmap complete with exact evidence.
- [ ] Commit documentation separately and push every Project E commit.
