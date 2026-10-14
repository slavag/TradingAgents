# Research workspace visual and interaction QA

final result: passed

Date: 14 October 2026. Selected direction: option 3, Research Inbox, with the requested consolidated-report action.

## Visual evidence

- Source visual truth: `/Users/slava/.codex/generated_images/01a1352a-f67c-7a01-9448-3741e6b5eb5a/exec-c3a7a3ab-cd06-4007-bb5d-bc221792f096.png`.
- Implemented page: `http://127.0.0.1:8000/#research/PENG`.
- Screenshot: `/Users/slava/.codex/visualizations/2026/10/13/01a1352a-f67c-7a01-9448-3741e6b5eb5a/web-ui-implementation/inbox-final.jpg`.
- Combined full-view comparison: `web-ui-implementation/design-comparison.jpg` in that evidence folder.
- Combined focused header/action comparison: `web-ui-implementation/detail-comparison.jpg` in that evidence folder.
- Mobile: `web-ui-implementation/mobile-final.jpg`; analysis controls: `web-ui-implementation/analysis-setup.jpg`; saved consolidated report: `web-ui-implementation/consolidated-report.jpg`.

Source pixels: 1487×1058, normalized proportionally to 1440×1024. Implemented screenshot: 1440×1024 CSS/pixel viewport, density 1. Both comparisons show the Overview state for PENG. The source uses illustrative records and a short illustrative summary; the implementation uses existing saved research and preserves complete decision text. Full-view comparison assessed composition; the focused comparison assessed readable typography and action placement.

## Findings and iterations

1. Initial mobile pass: the library and details shared a small screen, pushing report actions down. Fixed with a full ticker-list view and a separate selected-ticker view, plus Back to all tickers. Post-fix capture at 390×844 confirms normal document scrolling, no horizontal page overflow and report actions near the top.
2. Initial desktop pass: column proportions differed from the selected reference, and the longer actual summary pushed the consolidated report below the fold. Fixed desktop tracks to 240px navigation / 460px library / remaining detail width at 1440px, and added a consolidated-report shortcut above the narrative. Retained the exact-batch entry under Saved reports. Final capture shows the shortcut at approximately y=158px, including at 1280×720.
3. Browser form check: ticker helper text was included in the input's accessible name. Replaced the wrapping label with an explicit label associated with the ticker input. Rechecked entry, deduplication, chip removal, button count and provider/model accessible names.
4. Independent source review: interrupted jobs retained unfinished agent rows and could display Idle; saved Google reasoning/export defaults were not restored. Added failing behavioral regressions, fixed both, and obtained review approval. Search copy now says Search tickers because old catalog records do not carry verified company names.

No actionable P0/P1/P2 visual or interaction findings remain in the checked flows.

## Required fidelity surfaces

- Fonts/typography: readable system sans-serif, restrained heading scale and clear ticker/recommendation hierarchy. Complete saved text remains accessible. Small copy is secondary metadata, not a primary action.
- Spacing/layout rhythm: three desktop columns, flat navigation and ticker rows, overview/report grouping, visible top actions. Responsive views avoid the previous fixed-height clipping and competing scroll panels.
- Colors/tokens: navy surfaces, lavender primary/selected controls, teal completion/recommendation, muted metadata and rose failure states match the selected direction. No decorative gradients were introduced.
- Images/assets: retained the existing text product wordmark; the mock's illustrative logo and decorative navigation icons were not replaced with fabricated artwork. The workspace requires no raster imagery or external font/icon requests. Standard icons are optional follow-up polish.
- Copy/content: actual saved tickers, dates and recommendation text replace the mock's illustrative data. Missing company metadata is described as saved ticker analysis. Consolidated report labels identify the exact batch and date. The user's requested batch report is available in the ticker toolbar, Saved reports, Runs rows and run header.

## Interaction verification

- Existing history loads in a fresh browser page without a live analysis worker.
- Search, status/sort controls and ticker selection; per-run report versions/history.
- Consolidated report opens through the visible toolbar link; browser Back returns to the app.
- New Analysis deduplicates `PENG, NVDA, PENG, SHOP.TO` into three selections; removing NVDA updates the action to two tickers. No analysis was submitted.
- Advanced provider/model controls remain visible and retain the concurrent model-discovery functionality.
- Settings dialog opens, Escape closes, and focus returns to Manage data sources; no credentials were changed.
- 1440×1024, 1280×720 and 390×844 layout checks; no horizontal page overflow; mobile list/detail return route works.
- Browser console errors checked: none on the verified workspace flows.

Offline regressions cover recovery, malformed metadata, missing and escaped/symlinked artifacts, partial batches, local-only report access, idempotent submission, previous successful reports, defaults and native discovery buttons. Live provider/model runs and live Discover feed retrieval were not initiated. Full WCAG certification, a paid end-to-end model run and fresh investor-deck generation remain outside this verification.

## Follow-up polish

Optional standard navigation icons, locale-formatted dates and verified company-name metadata could refine presentation. They do not block ticker retrieval or report access.

## Implementation checklist

- [x] Durable history and legacy import, including Markdown-only research and failed batch members.
- [x] Ticker inbox and direct investor/full/consolidated actions.
- [x] New Analysis, Runs, Discover and Settings navigation.
- [x] Responsive document flow and accessible primary controls.
- [x] Independent review fixes and behavioral regressions.
- [x] Browser captures and combined visual comparison.
