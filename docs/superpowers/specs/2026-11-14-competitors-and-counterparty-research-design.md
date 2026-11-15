# Direct competitors, suppliers, and customers in investment research

Status: approved by the user with “go” and implemented. Generic stock research, not a CBRS-specific peer list. Full offline verification: 1,677 Python tests passed, two skipped; changed-file Ruff and diff checks passed. No live provider/model calls, commits or pushes during development.

## Outcome

Add three source-backed report sections and make their evidence available before the research, risk and Portfolio Manager decisions:

1. Direct competitor comparison.
2. Supplier exposure and recent developments.
3. Customer exposure and recent developments.

The sections must explain the implications for the main stock and its six-/twelve-month outlooks. They must not imply that every event at another company affects the stock, or replace evidence with a sector association.

## Decision use and consolidated-report requirement clarified by the user

Competitor, supplier and customer research is decision input, not a report-only decoration. Collect and expose its verified facts before the analyst/research/risk/Portfolio Manager synthesis. Material findings may change the main recommendation, target rationale, assumptions, risk assessment and confidence, including the six- and twelve-month views. Do not impose a mechanical rating change from a headline or competitor performance alone. The final reasoning should identify the material evidence that mattered, including counterarguments and coverage limits.

The earlier enhancements follow the same separation: corrected filing facts and the extended-hours reaction marker inform analysis; the six-/twelve-month views are Portfolio Manager outputs; fuller investor-slide prose explains their evidence and reasoning. Generated summaries and forecast outputs must not be recycled as independent external evidence.

The consolidated report must provide a finished concise synthesis of all the existing and new information. Replace the current full horizon/marker text dumps with compact summaries; do not simply append more long sections. Target roughly 200–300 words per completed stock plus a small metrics/outlook table, allowing additional complete text when material qualifications require it. This is a soft editorial target, not a character or word truncation rule.

For each stock include:

- The main recommendation, current reference/session date, actionable position guidance, and the key reasons/risks.
- Two compact horizon rows: six and twelve months, recommendation, point target/range, recommendation confidence and target confidence. Keep horizon endpoints and currency identifiable.
- Extended-hours information remains a decision input and audit record. Per the user’s subsequent clarification, it has no dedicated slide or standalone report section, including unavailable-quote notices. A material effect may be discussed naturally in the decision reasoning.
- A concise peer-position conclusion and the most consequential supplier/customer development or exposure; state missing coverage where it changes interpretation.
- Links to the full investor/technical reports and original supporting sources for detail.

Business, product and financial explanations should be synthesized into the decision-relevant reasons rather than copied from every investor slide. Avoid repeating the same fact in the summary, horizon explanation and news block. Summaries must use complete sentences and retain material adverse evidence, conditional triggers, as-of dates, uncertainty and accounting/unit distinctions. No ellipses, fixed-character cuts, sliced sentences, or automatic deletion of essential qualifications. If optional model summarization fails, use complete source-backed statements or a clear coverage summary; never a clipped prefix of the full report.

Apply this policy to Markdown and HTML consistently, including the report content added in the prior work: corrected financial tables/periods/losses, fuller company/product/financial explanations, both forward outlooks with independent confidence, and the separate extended-hours information marker. Full detail remains in the investor/full report and saved run evidence.

## Current owning paths and foundation

`collect_shared_evidence` retrieves the primary company's filing/earnings enrichment before analysts start. The deck has Competitive position and Customers and partners prose topics, but the news analyst's tools are scoped to the main ticker. The current primary-company research ledger must not absorb another issuer's revenue, share price, or price targets.

Inspection also found that `_document_records` in `dataflows/sec.py` still clipped table rows to twenty columns after the earlier extractor/normalizer improvements. This remaining adapter boundary was corrected under the already-approved table-fidelity scope, with a provider-adapter-to-ledger regression (full offline suite: 1,602 passed, two skipped). Accurate comparison depends on preserving those columns.

## Recommended approach and alternatives

Use bounded, explicitly scoped research for related companies: discover source-supported entities, validate identities and relationships, collect comparable facts and dated news, then expose the results to the existing decision teams and report builders. Do not launch a full investment analysis for each company.

A prompt-only request is cheaper but cannot ensure fresh peer facts, comparable periods, or reliable supplier/customer identities. Full separate analyses of every related company would add substantially more runtime and model usage than needed for this feature. The recommended middle approach adds one bounded source-based entity-discovery pass plus limited data retrieval during a user-requested run; it reuses configured providers and does not require a new subscription.

## Direct competitor comparison

Default to three verified direct competitors, allowing up to five when evidence supports them. Fewer supported competitors is preferable to filling a quota with loosely related sector names. Explain the overlap for each company: substitutable product, customer need, business segment, or spending budget. Separate direct competition from adjacent industry context; do not mix adjacent companies into the direct-comparison ranking.

Candidate names may come from provider peer lists or a model proposal, but those are discovery hints, not verification. Establish overlap from admitted competition disclosures or attributable issuer product/business sources. For recent IPOs without annual filings, allow bounded relevant registration/prospectus evidence and official company material through supported sources. If coverage cannot establish direct competition, disclose that gap instead of promoting a sector peer into the table.

Use public companies for quantitative comparison. A verified private competitor may appear qualitatively with unavailable financial/market fields. Resolve aliases, share classes and ticker identity explicitly. A company may occupy multiple roles, but each relationship requires its own supporting source.

Provide two readable comparison slides rather than one excessively wide table:

- **Business and financial comparison:** competitive overlap, latest reported revenue growth, relevant profitability measures, valuation where meaningful, and financial resilience. Show reporting dates, currency, units, accounting basis and sources. Prefer GAAP/IFRS reported metrics over adjusted figures, labeling differences that prevent like-for-like comparison. A loss-making company's P/E is not meaningful, rather than zero. Compute currency-dependent ratios only when the inputs are compatible or a verified conversion is available. Do not assume a USD quote from the listing or an instrument alias.
- **Market performance and interpretation:** one-, three-, six- and twelve-month price/return comparisons using the same cutoff and adjustment convention. Label local-currency returns if currencies differ. Explain the main stock's advantages, disadvantages and valuation premium/discount only to the extent supported by the facts. Do not invent recommendations or price targets for peers that did not receive full analyses.

Different fiscal calendars must be visible. Compare each company's properly matched year-over-year quarter within that company, rather than combining a quarterly amount with a year-to-date amount or pretending all fiscal quarters have identical dates.

## Suppliers and customers

Discover named relationships from filings, earnings disclosures and attributable company announcements. Preserve relationship type, supporting quote/source, disclosure date, relevant segment and any disclosed concentration/dependence. Label a relationship as disclosed at a date, not confirmed active today unless evidence establishes that. Preserve anonymous customer/supplier disclosures without guessing identities.

Supplier coverage should look for relevant capacity constraints, shortages, pricing changes, quality issues, outages/cyber incidents, financial distress, trade restrictions and single-source exposure. Customer coverage should look for order changes, cancellations, budgets/capital spending, deployments, renewals, demand changes, credit problems and concentration risk.

Distinguish three kinds of information:

- A verified development with a demonstrated connection to the main company.
- A reported development whose impact is conditional on a disclosed relationship continuing.
- A broader potential industry exposure with no confirmed direct link.

For each item, show the related company, relationship evidence, event/publication date, source link, reported fact, transmission mechanism to the main stock, materiality and uncertainty, plus what to monitor. Supplier and customer sections remain separate even when the same entity has several roles. A competitor's growth does not automatically establish lost sales; a customer's capital raise does not establish an order; a supplier's disruption does not establish an actual production halt at the main company.

Use the latest seven days for news and highlight material developments within the prior thirty days. Older disclosed structural risks may remain as dated background, not masquerade as recent news. State the searched window and coverage status. “No relevant news found in the available coverage” is different from “no issues,” and unavailable feeds are different from a clean search.

## Data flow, budget, and isolation

After primary source enrichment and any bounded supported-source discovery, run one bounded structured discovery pass to propose entities and exact relationship/competition evidence. Validate each proposal against admitted sources and identity lookup before fetching related-company data. Reject accounting terms, ambiguous names, unsupported relationships and invented ticker mappings.

Use a separate typed related-company collection, with entity IDs, symbols, roles, source IDs and explicit coverage. Keep the primary ledger and technical price anchors isolated. Build peer rows from per-entity facts; allow the decision teams to cite related-company sources without treating their prices as the main stock's target candidates. Include related-company inputs in evidence fingerprints and checkpoint compatibility so source changes are not misreported as identical evidence.

Default to at most nine distinct related companies per run: normally three peers, three suppliers and three customers, prioritizing disclosed material exposure. Extra peer slots replace lower-priority related entities within that same total budget, with the resulting coverage shown. Bound the total retrieval budget, article counts and concurrency, deduplicate multi-role entities, and cache by entity, analysis date and provider/query scope. Partial failures preserve the rest of the main analysis. Private or unresolved entities retain only the qualitative coverage their sources support.

Use configured providers with their existing credentials and limits. Do not add a mandatory service or silently switch to an unconfigured paid provider. Do not fetch arbitrary model-generated URLs as if they were verified company sources. Historical runs must use information known by the requested cutoff; today's quote/fundamentals or today's entity identities must not be backdated. When historical coverage is unavailable, report that gap.

Persist the typed collection and its sources with the run. Render it in the investor deck, full/consolidated reports and web result. The existing main recommendation, target and separate confidence scores remain independent. Competitor and counterparty evidence informs the reasoning; it does not impose an automatic rating change.

## Verification and acceptance

Use self-contained source fixtures and fake transports to test verified versus sector-only peers, private/anonymous/unresolved entities, multiple roles, customer accounting false positives, comparable periods, currencies, loss-making valuation, adjustment conventions and historical cutoffs. Test provider failures, empty/partial coverage, bounded calls/cache keys, and entity/news identity/date filtering.

Add an end-to-end regression where a peer has different revenue and a much higher share price: neither may appear as a primary-company fact, reference close or target anchor. Test that changed related-company news changes the run's evidence fingerprint. Verify supplier/customer impact statements distinguish reported facts from conditional exposure.

Inspect an offline report preview with clearly marked fixture data and test all output paths. Add long-input regression cases for both consolidated formats: all key decision/target/confidence facts and material qualifications survive in concise complete summaries, without ellipses or cut sentence fragments, including deterministic fallback when summarization is unavailable. Preserve the existing uncommitted work, source integrity fixes, horizon forecasts and extended-hours marker. No live provider/model calls during development, commits or pushes without separate authorization. Fresh user-run validation follows implementation.

## Implemented limits and verification

Related-company identity checks and live retrieval caches expire after five minutes. Current identity must be independently verified before secondary retrieval; dated old ticker pairings alone cannot identify the current issuer. Historical related-company secondary financial/news/performance retrieval is explicitly unavailable under the existing symbol-keyed adapters, which do not guarantee stable as-of issuer identity and point-in-time adjustments. Original dated relationship disclosures remain qualitative evidence.

Statement figures require verified reporting currency; quote currency is separate. Major-currency market caps remain unscaled when share quotes use minor units such as GBp. TTM P/E snapshots have an observation date and a vendor-reported label; a quarterly loss alone does not establish TTM loss.

Independent source/core, presentation/specification and integration-quality reviews completed. Regression coverage includes role ownership, issuer/ticker reuse, date and currency scope, cache expiry, source-only context budgets, target-condition retention, immutable artifacts, Markdown/HTML parity, and saved CLI detail links. The real saved-CBRS brief was reduced from roughly 1,004 words to roughly 405 including structured guidance, tables and links; its core HTML narrative is about 260 words. This is complete-statement synthesis with material-condition exceptions to the soft target.

Offline previews: `cbrs-consolidated-preview.html` uses actual saved CBRS evidence and explicitly discloses absent later research; `related-company-layout-demo.html` is clearly labeled synthetic test data. Both are under `/Users/slava/.codex/visualizations/2026/11/11/01a1cb9a-9cac-7513-b173-cc32036d81df/`. No original run artifacts were overwritten. Fresh user-run provider/model output remains the live acceptance step.
