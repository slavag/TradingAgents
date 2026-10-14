# Research Workspace Implementation Plan

**Goal:** Implement the selected option 3 research inbox with durable ticker/run history and visible consolidated reports.

**Design:** `/Users/slava/.codex/generated_images/01a1352a-f67c-7a01-9448-3741e6b5eb5a/exec-c3a7a3ab-cd06-4007-bb5d-bc221792f096.png`.

**Architecture:** Keep FastAPI and the existing analysis/settings/model-discovery runtime. Add a local atomic run catalog and import existing report manifests without modifying historical reports. Separate workspace navigation/research rendering from analysis runtime. Use normal responsive document flow.

**Constraints:** Preserve unrelated uncommitted work and provider-discovery additions. No paid runs, commits, pushes, or server interruption. Report reads remain local-only and contained in the results root. Consolidated reports bind to the selected run, display all included tickers and date, and remain accessible for partially failed batches. Native implementation in this checkout is authorized by the user's repeated “go”.

- [x] Catalog/API: tests for restart recovery, legacy import, corrupt/missing files, local access, traversal, exact batch reports; implement `tradingagents/web/history.py`, service persistence and history/report endpoints.
- [x] Workspace: rewrite the page around My Research / New Analysis / Runs / Discover / Settings; add `workspace.js` and reusable report helpers; retain existing model configuration and credential controls; add chips, visible submission feedback and duplicate prevention.
- [x] Validation: targeted/full pytest, Ruff and JS syntax; browser checks for history/search/report links, consolidated scope, navigation/refresh, model controls, empty/error/partial states and 1280×720 / 390×844 layouts. Use a separate loopback preview process without paid execution.
- [x] Visual QA: compare actual rendered inbox with selected mockup; save evidence and `design-qa.md`; fix substantive differences before handoff.

**Review focus:** failed newest run must retain prior successful reports; empty/corrupt history must not break healthy siblings; artifacts/symlinks must remain contained; interruption must not appear as live execution; ticker suffixes and model selections must survive UI interaction.
