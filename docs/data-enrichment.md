# Decision Evidence Enrichment

TradingAgents collects source evidence once per run and makes a bounded source corpus available to all analyst, researcher, trader, risk, and portfolio-manager roles. Executed tool responses are retained when analyst message history is cleared. Source records remain separate from model-authored reports and are saved under `0_sources/` in report trees.

## Configuration

In the web app, select **Data Sources** beside **Launch Analysis**. You can save,
replace, or remove SEC, FMP, FRED, and Alpha Vantage credentials there. Secret
keys are stored locally in `~/.tradingagents/settings/data_sources.json` with
owner-only permissions; the web API returns only their configured status.

Enrichment is enabled by default for stocks. Disable it with:

```dotenv
TRADINGAGENTS_ENRICHMENT_ENABLED=false
```

Limit source text inserted into each agent prompt with:

```dotenv
TRADINGAGENTS_EVIDENCE_PROMPT_MAX_CHARS=18000
```

The full retained record excerpt remains in graph state and `0_sources/evidence.json`. When a source or prompt limit truncates content, the record says so and includes a SHA-256 digest of the original response.

### SEC filings and company facts

SEC EDGAR APIs do not require an API key, but automated access must identify the application and a real contact address:

```dotenv
SEC_USER_AGENT="Your Application Name your-real-email@domain.com"
```

For each analysis date, the collector selects the latest eligible annual filing
(10-K, 20-F, or 40-F), the latest eligible quarterly filing (10-Q), and at most
three current reports (8-K or 6-K). It retains filing metadata and up to five
selected standardized XBRL facts, then extracts bounded sections from primary
filing HTML and relevant EX-99.1 exhibits. These can include business, product,
and competition descriptions, earnings-release narrative and tables, and reported
management changes. Each excerpt keeps its exact SEC document URL, filing
form, accession, section, and report period when available. The investor deck
can use those records as attributed source material; an extracted excerpt is
partial coverage, not a complete reading of the filing.

The collector makes at most seven HTML fetch attempts and emits at most 15
document-section records per run. Each fetched HTML document is limited to
8 MB. Successful responses are cached in the configured `data_cache_dir` under
`sec-documents/`, keyed by the SEC archive URL; oversized cached files are
rejected. SEC requests are paced. Redirects, non-SEC archive URLs, non-HTML
responses, empty documents, and oversized documents are rejected. A selected
document that cannot be retrieved or parsed is recorded as unavailable; its
failure does not discard other filings, XBRL facts, market data, or the analyst
run. The full technical report remains available even if deck-quality source
coverage is thin.

Historical runs admit only filings available by the analysis date and facts
whose filing and financial-period end were no later than that date. The
collector searches at most two older submission files. It does not claim
exhaustive filing coverage, custom-taxonomy facts, or complete segment
disclosures. Issuers without an eligible SEC match can still use other
configured providers and market evidence.

### FMP earnings context

Set an FMP API key with the endpoint entitlements you intend to use:

```dotenv
FMP_API_KEY=...
```

For live analysis, FMP can contribute annual analyst estimates, recent actual-versus-estimated earnings, and a bounded recent earnings-call transcript. Each endpoint degrades independently when the current plan does not include it. FMP does not expose the point-in-time revision archive required by this application, so historical analyses skip these live endpoints and record the source as unavailable.

## Provenance behavior

- Every record carries its provider, category, analysis date, retrieval timestamp, status, source URL when available, and a provider publication/availability timestamp only when the provider supplies a trustworthy one.
- Future-dated and cross-run records are excluded from prompts and final target validation.
- Unavailable records remain visible to agents as coverage gaps but cannot validate price targets.
- Provider credentials are excluded from source URLs, records, and returned errors.
- The Portfolio Manager receives the source corpus and original analyst reports. Target validation uses only admitted, available source records and completed analysis sections.

Official provider references:

- [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
- [SEC automated access guidance](https://www.sec.gov/about/webmaster-frequently-asked-questions)
- [FMP stable API documentation](https://site.financialmodelingprep.com/developer/docs/stable)
