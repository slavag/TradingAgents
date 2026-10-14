# Source-Grounded Investor Research Deck

## Goal

Replace the generated per-ticker investor brief with a company-research
presentation that helps a reader understand the business and form a view that
may differ from the TradingAgents decision. Apply this to every completed stock
in a batch, not just PENG. Keep the full technical report and consolidated
batch report as separate outputs.

The supplied `PENG_20261007.html` is a reference for presentation depth and
visual hierarchy, not a source of facts or a template to copy. A strong PENG
deck should cover the business, segments, products, management developments,
commercial relationships, competition, financial trends, cash and capital,
valuation, risks, catalysts, and an independent bull/bear synthesis. It should
not create a slide merely to reach a count. Approximately 12–20 substantive
slides are plausible for PENG when source coverage permits; other tickers may
have fewer. An empty or metadata-only topic is omitted, with a material
limitation stated beside the conclusion it affects.

## Why the Current Report Fails

The October 9 PENG run `db4f76cfa0914c51b8d8fb87cc30628b` generated four
cards about the portfolio decision, earnings, valuation, and technical trend.
Its vendor income statements end on May 31, 2026, and its news source says no
company news was found. The SEC collector recorded three recent 8-Ks as filing
metadata only and retained five selected XBRL facts. It selected the latest
three filings by date, so older annual business disclosure was not included.

That misses information already public by the analysis date. Penguin's October
6, 2026, 8-K links to Exhibit 99.1, which reports fiscal Q4 results for the
period ended August 28, 2026, and fiscal 2027 guidance. The issuer reports Q4
sales of $566.685 million versus $478.713 million in Q3. Its fiscal 2026
segment sales imply approximately 53.4% Integrated Memory, 32.3% Advanced
Computing, and 14.4% Optimized LED. The reference deck's approximate segment
mix of 60%+, 30%+, and <10% must not be reproduced as a verified fact.

Primary-source examples for the acceptance case:

- [October 6 earnings 8-K](https://www.sec.gov/Archives/edgar/data/1616533/000161653326000046/peng-20261006.htm)
  and its [Exhibit 99.1](https://www.sec.gov/Archives/edgar/data/1616533/000161653326000046/pengq4-26form8xkxex991.htm);
- [FY2025 Form 10-K business disclosure](https://www.sec.gov/Archives/edgar/data/1616533/000161653325000061/peng-20250829.htm);
- [October 6 CFO appointment 8-K](https://www.sec.gov/Archives/edgar/data/1616533/000119312526415645/d162527d8k.htm).

## Selected Approach

Extend shared evidence collection before the analyst teams, then synthesize a
typed, source-grounded investor deck from the same completed run. Use SEC
filings and their relevant exhibits as the generic primary-source path for
U.S. stocks. Use existing vendor statements and verified market snapshots for
price and financial history, while giving a newer eligible issuer filing
priority for the same metric and period. The deck is a research presentation,
not a restyling of the portfolio decision.

Two narrower approaches were rejected:

- Reformatting the current four cards cannot recover a missing Q4 release,
  company description, segment mix, or customer evidence.
- Hand-authoring PENG from the supplied HTML would not improve other batch
  tickers and would risk copying unsupported or inconsistent claims.

## Source Collection and As-Of Contract

1. Retain the existing exact ticker-to-CIK lookup and `SEC_USER_AGENT`
   requirement. Select the latest eligible annual report (10-K, 20-F, or
   40-F), latest eligible quarterly report (10-Q where applicable), and a
   bounded set of recent current reports (8-K or 6-K). Selection is per form
   family, not simply the latest three filings overall.
2. Retrieve bounded, text-only excerpts from the selected annual business,
   risk, and competition sections; selected quarterly discussion; and current
   report items and relevant earnings or material-announcement exhibits such
   as 99.1. Follow only SEC archive URLs derived from filing metadata. Do not
   execute embedded scripts or render source HTML in a report.
3. Each source record carries issuer/ticker identity, document and accession,
   form/exhibit, source URL, filing or publication date, report period, capture
   time, coverage, and an exact excerpt or table row. A source published after
   the requested analysis date is excluded. Unknown publication timing cannot
   support a historical claim.
4. Bound the number of documents, bytes, text length, request time, and cache
   age per ticker. Respect SEC request pacing. A failed or unparseable source
   becomes an `unavailable` record; it never silently turns into a positive
   fact or blocks the full technical report.
5. Do not add an unconstrained web search or arbitrary issuer-site crawler.
   If SEC coverage cannot establish a topic for a non-U.S. or thinly filing
   issuer, omit that topic. A separately validated issuer-IR connector can be
   considered later if SEC exhibits prove insufficient across real cases.

## Evidence Reconciliation

Build a compact research ledger from existing XBRL facts, vendor statements,
market snapshots, and the new primary-source excerpts. A ledger entry states:

- the claim or metric and its value/unit, if numerical;
- the fiscal/reporting period and whether it is quarterly, annual, or TTM;
- whether it is GAAP reported, non-GAAP, management guidance, a company
  description or announcement, a third-party market figure, or team analysis;
- the exact source record and document URL; and
- the source's availability date and any unresolved conflict.

For the same metric and period, prefer an eligible primary filing or exhibit
over a lagging vendor feed. Do not compare values with different periods or
accounting definitions as though they were the same. Derived growth rates,
segment mix, and simple ratios are computed deterministically from compatible
ledger values and retain links to their inputs. If the most recent primary
release is newer than vendor financials, the deck leads with the primary
period and labels older vendor history; it must not call the older quarter
"latest." Figures in a furnished 8-K earnings exhibit are company-reported;
do not call them audited. A material unresolved conflict is visible where it
changes the investment interpretation, not on a standalone missing-data slide.

## Deck Contract and Rendering

Replace the current title/takeaway/claims-only shape with typed slide content:
short narrative, sourced metric tiles, period-aligned tables, simple trend
charts, relationship or timeline rows, and bull/bear or decision-condition
panels. Rendering is deterministic and escapes all model/source text. Tables
and charts are built from validated ledger values, not numeric strings in
model prose. The self-contained HTML keeps inline styles, no external assets
or scripts, restrictive CSP, responsive scrolling, and usable print layout.

The reference's white-slide and teal editorial direction is useful, but its
markup and prose are not copied. The slide outline is evidence-dependent:

1. Company identity and one-paragraph investment question.
2. Business model, products, and revenue segments.
3. Management and governance developments when material and dated.
4. Named customers, partners, suppliers, and competitors with precise
   relationship labels; no implied customer revenue share.
   Unit economics, addressable market, or moat claims appear only when both
   the source and calculation method support them.
5. Revenue and earnings trends with fiscal periods and reported values.
6. Earnings quality, working capital, cash flow, liquidity, and dilution.
7. Valuation, and peer comparison only when peer choice and each metric have
   contemporaneous, attributable evidence.
8. Confirmed catalysts and management guidance, with announced dates kept
   distinct from analytical scenarios or estimated windows.
9. Strongest bull case, strongest bear case, decision conditions, and a
   concise conclusion that identifies what would change the reader's view.

These are topic groups, not nine mandatory slides. Split a dense group only
when the reader gains a distinct answer; omit a group without useful evidence.
No generic company classification, metadata-only filing, empty news search,
unsupported TAM/moat score, speculative catalyst date, or raw tool response
earns a slide. Technical indicators may support a concise entry-risk panel but
cannot dominate the company-research deck.

Each factual statement has a visible source reference that resolves to the
specific document, date, and section or table row; references may be grouped
visually but retain exact record identity. Clearly distinguish reported fact,
company guidance, model/team interpretation, and scenario. Source links are
optional for offline reading but present when the source URL is known. The
portfolio team's action is one input, not the deck's organizing principle.

## Integration and Compatibility

`collect_shared_evidence` remains the one pre-analyst enrichment boundary, so
analysts and the portfolio manager can use the same current documents the deck
uses. The web job, CLI, and programmatic report paths all call one per-ticker
deck builder after analysis. Every successful batch ticker receives its own
timestamped investor HTML alongside the timestamped full Markdown and HTML
reports. Keep the existing job-scoped local report endpoint and URL shape.
The consolidated batch output and its ticker-bearing, timestamped name remain
unchanged. Old saved reports are never rewritten as a side effect of deploying
the new builder.

If source enrichment or deck synthesis fails, the ticker's full technical
report and job result still complete. The investor output states that a
validated research deck was unavailable and links to the full report rather
than presenting a thin one-card page as though it were the requested deck.
No SMXT generation is part of verification.

## Security and Operational Limits

- Treat retrieved filings, attached reference HTML, and model output as data,
  never instructions. Fetch only the selected SEC archive hosts and validated
  document paths; reject redirects outside that boundary.
- Preserve source HTML as text after safe extraction. Escape all displayed
  content and URLs, preserve the report CSP, and avoid remote images/scripts.
- Keep per-ticker source and model budgets predictable; cache immutable
  filings by accession and separate transient failures from an empty result.
- Source records and report files remain job-scoped. Do not expose arbitrary
  local files through the report endpoint.

## Verification and Acceptance

Write tests before implementation for filing selection, exhibit discovery,
size/host limits, historical cutoffs, source failure, period reconciliation,
derived figures, source identity, slide omission, HTML escaping, batch output,
and the existing report endpoint. Use recorded public PENG filing fixtures for
an end-to-end acceptance test that does not require a live model or stock-data
request. Verify the rendered page at desktop and mobile widths, including
source navigation and print readability; run the full Python suite and lint.

For PENG as of October 9, 2026, acceptance requires the new deck to use the
eligible October 6 fiscal Q4 earnings exhibit, identify Q4 sales of $566.685
million and Q3 sales of $478.713 million with their periods, show the official
FY2026 segment breakdown, explain OriginAI/ClusterWareAI and the material
customer and CFO developments only with direct sources, and analyze cash and
capital using correctly dated records. It must not repeat the reference's
unsupported 60%+/30%+/<10% segment mix or present estimated events as
announced. It must include meaningful company, operating, financial, and
contrary-case coverage rather than merely four trading-decision cards.

After deterministic fixture and visual checks, a new app-generated PENG report
is the final live-content review. Its output must be inspected against this
acceptance list; a passing test suite alone does not establish report quality.

## Non-Goals

This work does not provide personalized investment advice, guarantee a fixed
slide count, clone the reference's prose or CSS, infer customer economics or
market share without evidence, populate a peer table from mismatched dates,
or change the underlying position-aware decision policy except that the teams
receive better primary-source context.
