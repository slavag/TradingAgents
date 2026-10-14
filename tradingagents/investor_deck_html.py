"""Self-contained, escaped presentation of validated investor-deck data."""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from html import escape
from urllib.parse import urlsplit

from tradingagents.investor_deck import (
    DecisionBlock,
    DeckSlide,
    InvestorDeck,
    MetricBlock,
    TableBlock,
    TextBlock,
    TimelineBlock,
    TrendBlock,
)
from tradingagents.research_ledger import ResearchSource


def _safe_href(url: str | None) -> str | None:
    if not url or any(character.isspace() for character in url):
        return None
    try:
        parsed = urlsplit(url)
        safe = (parsed.scheme == "https" and bool(parsed.hostname)
                and not parsed.username and not parsed.password
                and parsed.port in {None, 443})
    except ValueError:
        return None
    if not safe:
        return None
    return escape(url, quote=True)


def _references(keys: list[str], numbers: dict[str, int]) -> str:
    return " ".join(
        f'<a class="source-ref" href="#source-{numbers[key]}" '
        f'aria-label="Source {numbers[key]}">[{numbers[key]}]</a>'
        for key in dict.fromkeys(keys) if key in numbers
    )


def _render_metrics(block: MetricBlock, numbers: dict[str, int]) -> str:
    cards = "".join(
        '<div class="metric">'
        f'<span class="metric-label">{escape(item.label)}</span>'
        f'<strong>{escape(item.value)}</strong>'
        f'<span class="metric-period">{escape(item.period)} '
        f'{"· " + escape(item.basis) if item.basis else ""} '
        f'{_references(item.source_ids, numbers)}</span></div>'
        for item in block.metrics
    )
    return f'<div class="metrics">{cards}</div>'


def _render_table(block: TableBlock, numbers: dict[str, int]) -> str:
    headings = "".join(f'<th scope="col">{escape(text)}</th>' for text in block.headings)
    rows = "".join(
        "<tr>" + "".join(f"<td>{escape(cell)}</td>" for cell in row.cells)
        + f'<td class="row-source">{_references(row.source_ids, numbers)}</td></tr>'
        for row in block.rows
    )
    return (
        '<div class="table-scroll"><table><thead><tr>'
        f'{headings}<th scope="col">Source</th></tr></thead>'
        f'<tbody>{rows}</tbody></table></div>'
    )


def _render_text(block: TextBlock, numbers: dict[str, int]) -> str:
    points = "".join(
        f'<li>{escape(point.text)} {_references(point.source_ids, numbers)}</li>'
        for point in block.points
    )
    return f'<ul class="points">{points}</ul>'


def _trend_domain(values: list[Decimal]) -> tuple[Decimal, Decimal]:
    low, high = min(values), max(values)
    span = high - low
    padding = span * Decimal("0.12") if span else max(abs(high) / 10, Decimal(1))
    return low - padding, high + padding


def _trend_time_positions(periods: list[str]) -> list[int]:
    quarters = [re.fullmatch(r"Q([1-4])-(\d{2})", period) for period in periods]
    if all(quarters):
        return [(2000 + int(match.group(2))) * 4 + int(match.group(1))
                for match in quarters if match]
    try:
        return [date.fromisoformat(period).toordinal() for period in periods]
    except ValueError:
        return list(range(len(periods)))


def _trend_svg(
    values: list[Decimal], periods: list[str], low: Decimal, high: Decimal,
    width: int, name: str,
) -> str:
    height = 220
    top, bottom, side = Decimal(18), Decimal(202), Decimal(14)
    plot_height = bottom - top
    spread = high - low
    times = _trend_time_positions(periods)
    first_time, time_span = min(times), max(times) - min(times)
    if not time_span:
        times, first_time, time_span = list(range(len(values))), 0, len(values) - 1
    coords = [
        (
            side + (Decimal(width) - 2 * side) * (time - first_time) / time_span,
            top + (high - value) / spread * plot_height,
        )
        for time, value in zip(times, values, strict=True)
    ]
    points = " ".join(f"{x:.1f},{y:.1f}" for x, y in coords)
    dots = "".join(
        f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" />' for x, y in coords
    )
    zero = ""
    if min(values) < 0 < max(values):
        y = top + high / spread * plot_height
        zero = (f'<line class="zero-line" x1="0" y1="{y:.1f}" '
                f'x2="{width}" y2="{y:.1f}" />')
    return (
        f'<svg class="trend-line {name}" viewBox="0 0 {width} {height}" '
        'aria-hidden="true" focusable="false">'
        f'<line class="grid-line" x1="0" y1="{top}" x2="{width}" y2="{top}" />'
        f'<line class="grid-line" x1="0" y1="{bottom}" x2="{width}" y2="{bottom}" />'
        f'{zero}<polyline points="{points}" />{dots}</svg>'
    )


def _render_trend(block: TrendBlock, numbers: dict[str, int]) -> str:
    usable = [
        item for item in block.points
        if item.raw_value is not None and item.raw_value.is_finite()
    ]
    values = [item.raw_value for item in usable]
    labels = "".join(
        '<li><span class="trend-period">'
        f'{escape(item.period)}</span><strong>{escape(item.value)}</strong>'
        f'<span>{escape(item.basis)}</span> {_references(item.source_ids, numbers)}</li>'
        for item in block.points
    )
    if len(values) < 2:
        return f'<ol class="trend-values">{labels}</ol>'
    low, high = _trend_domain(values)
    scale = Decimal("1000000000") if max(map(abs, values)) >= 1_000_000_000 else Decimal("1000000")
    unit = "USD billions" if scale == 1_000_000_000 else "USD millions"
    zero_label = ""
    if min(values) < 0 < max(values):
        position = (Decimal(18) + high / (high - low) * Decimal(184)) / Decimal(220) * 100
        zero_label = f'<span class="zero-label" style="top:{position:.1f}%">0</span>'
    return (
        '<figure class="trend"><figcaption>Reported values over time · '
        f'{unit}</figcaption><div class="trend-plot">'
        f'<div class="trend-scale" aria-hidden="true"><span>{high / scale:.1f}</span>'
        f'{zero_label}<span>{low / scale:.1f}</span></div>'
        f'{_trend_svg(values, [item.period for item in usable], low, high, 720, "trend-desktop")}'
        f'{_trend_svg(values, [item.period for item in usable], low, high, 360, "trend-mobile")}'
        f'</div><ol class="trend-values" aria-label="Values in chronological order">'
        f'{labels}</ol></figure>'
    )


def _render_timeline(block: TimelineBlock, numbers: dict[str, int]) -> str:
    events = "".join(
        '<li><span class="event-period">'
        f'{escape(item.period)}</span><strong>{escape(item.label)}</strong>'
        f'<span>{escape(item.value)}</span> '
        f'{_references(item.source_ids, numbers)}</li>'
        for item in block.events
    )
    return f'<ol class="timeline">{events}</ol>'


def _render_decision(block: DecisionBlock, numbers: dict[str, int]) -> str:
    def line(label: str, value: str | None) -> str:
        return (
            f'<p><strong>{escape(label)}</strong> {escape(value)}</p>'
            if value else ""
        )

    if block.rating is None:
        reason = (block.status_reason or "").replace("_", " ")
        return (
            '<div class="decision-card"><h3>No actionable recommendation</h3>'
            f'{line("Reason:", reason)}</div>'
        )

    confidence = (
        f"{block.recommendation_confidence}/100"
        if block.recommendation_confidence is not None else None
    )
    target = "No validated price target"
    if block.target_low is not None and block.target_high is not None:
        target = f"{block.target_low:.2f}–{block.target_high:.2f}"
    elif block.target is not None:
        target = f"{block.target:.2f}"
    has_target = block.target is not None or (
        block.target_low is not None and block.target_high is not None
    )
    if has_target and block.quote_currency:
        target += f" {block.quote_currency}"
    target_note = (
        line("Note:", "Quote currency not recorded")
        if has_target and not block.quote_currency else ""
    )
    target_reason = (block.target_reason or "").replace("_", " ")
    target_confidence = (
        f"{block.target_confidence}/100"
        if block.target_confidence is not None else None
    )
    target_card = (
        '<div class="decision-card decision-target decision-wide"><h3>Target</h3>'
        f'<p class="decision-value">{escape(target)}</p>'
        f'{target_note}{line("Horizon:", block.target_horizon)}'
        f'{line("Target confidence:", target_confidence)}'
        f'{line("Method:", block.target_method)}'
        f'{line("Team rationale:", block.target_rationale)}'
        f'{line("Risks:", block.target_risks)}'
        f'{line("Price anchor:", block.target_quote)}'
        f'{line("Lower anchor:", block.target_quote_low)}'
        f'{line("Upper anchor:", block.target_quote_high)}'
        f'{line("Why no target:", target_reason if not has_target else None)}'
        '</div>'
    )
    conditions = (
        line("Confirmation:", block.confirmation)
        + line("Alternative:", block.alternative)
        + line("Invalidation:", block.invalidation)
    )
    conditions_html = (
        f'<div class="decision-conditions">{conditions}</div>' if conditions else ""
    )
    return (
        '<div class="decision-grid">'
        '<div class="decision-card decision-overview"><h3>Team recommendation</h3>'
        f'<p class="decision-value">{escape(block.rating)}</p>'
        f'{line("Recommendation confidence:", confidence)}'
        f'{line("Why this rating:", block.recommendation_summary)}'
        '<small>Confidence is an uncalibrated judgement, not a probability.</small></div>'
        f'{target_card}'
        '<div class="decision-card decision-existing"><h3>Already own it</h3>'
        f'{line("Action:", block.existing_action)}'
        f'{line("How:", block.existing_guidance)}</div>'
        '<div class="decision-card decision-new"><h3>Considering a new position</h3>'
        f'{line("Action:", block.new_action)}'
        f'{line("How:", block.new_guidance)}</div>'
        f'</div>{conditions_html}'
    )


def _render_slide(slide: DeckSlide, numbers: dict[str, int], index: int, count: int) -> str:
    renderers = {
        "metrics": _render_metrics,
        "table": _render_table,
        "text": _render_text,
        "trend": _render_trend,
        "timeline": _render_timeline,
        "decision": _render_decision,
    }
    blocks = "".join(renderers[block.kind](block, numbers) for block in slide.blocks)
    narrative = f'<p class="narrative">{escape(slide.narrative)}</p>' if slide.narrative else ""
    return (
        f'<section class="slide" data-topic="{escape(slide.topic, quote=True)}">'
        f'<div class="slide-header"><h2>{escape(slide.title)}</h2></div>'
        f'<p class="takeaway">{escape(slide.takeaway)}</p>'
        f'{narrative}{blocks}'
        '<footer class="slide-footer">'
        f'<span class="footer-sources">Source records: {_references(slide.source_ids, numbers)}</span>'
        f'<span class="slide-number">{index:02d} / {count:02d}</span></footer>'
        '</section>'
    )


def _render_source(row: ResearchSource, number: int) -> str:
    href = _safe_href(row.url)
    link = f'<a href="{href}" rel="noopener noreferrer">{href}</a>' if href else ""
    tool_titles = {
        "get_cashflow": "Cash-flow statement",
        "get_balance_sheet": "Balance sheet",
        "get_fundamentals": "Market and valuation data",
        "get_income_statement": "Income statement",
    }
    title = tool_titles.get(row.provider, row.title)
    if title.startswith("SEC reported "):
        title = "Company filing financial data"
    elif row.provider == "analysis" and row.title in {"bull", "bear", "portfolio"}:
        title = f"{row.title.capitalize()} team view"
    provider = ("data provider" if row.provider.startswith("get_") else
                "SEC filing" if row.provider == "sec" else
                "team analysis" if row.provider == "analysis" else row.provider)
    metadata = [
        provider,
        row.form or "",
        row.document_type or "",
        f"accession {row.accession}" if row.accession else "",
        row.section or "",
        f"period {row.report_period}" if row.report_period else "",
        f"available {row.published_at}" if row.published_at else "",
        f"{row.coverage} coverage" if row.coverage else "",
    ]
    return (
        f'<li id="source-{number}"><span class="source-number">[{number}]</span>'
        f'<div><strong>{escape(title)}</strong>'
        f'<span>{" · ".join(escape(item) for item in metadata if item)}</span>'
        f'<code>{escape(row.evidence_id)}</code>{link}</div></li>'
    )


_CSS = """
:root{--ink:#17262e;--muted:#5d6b70;--teal:#0d7267;--paper:#f5f3ed;
  --line:#cbd5d0;--soft:#e9f1ed}*{box-sizing:border-box}
html{scroll-behavior:smooth}body{margin:0;background:var(--paper);color:var(--ink);
  font:16px/1.5 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
header.page-header{max-width:1040px;margin:0 auto;padding:66px 34px 28px}
.kicker{color:var(--teal);font-size:.78rem;font-weight:750;letter-spacing:.16em;
  text-transform:uppercase}h1,h2{font-family:Georgia,"Times New Roman",serif;line-height:1.08}
h1{font-size:clamp(2.6rem,6vw,5.2rem);letter-spacing:-.035em;margin:12px 0 18px}
.page-meta{color:var(--muted);font-size:.9rem}.slide{max-width:1040px;
  margin:22px auto;padding:44px 52px;background:#fff;border:1px solid var(--line);
  border-radius:12px;box-shadow:0 6px 22px rgba(23,38,46,.035)}
.slide-header{display:block;text-align:left;border-bottom:1px solid var(--line);padding-bottom:18px}
.slide h2{font-size:clamp(2rem,4vw,3.5rem);margin:0 0 8px;max-width:900px;text-align:left}
.takeaway{font-size:1.24rem;line-height:1.4;font-weight:650;color:var(--teal);max-width:900px}
.narrative{font-size:1.06rem;max-width:880px}.metrics{display:grid;
  grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:12px;margin:28px 0}
.metric{display:flex;flex-direction:column;gap:6px;background:var(--soft);
  padding:20px 22px;border-radius:9px;border:1px solid #d6e4de}
.metric-label{font-size:.79rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted)}
.metric strong{font-size:1.7rem;line-height:1.2}.metric-period{color:var(--muted);font-size:.86rem}
.table-scroll{overflow-x:auto;margin:26px 0}table{width:100%;border-collapse:collapse;
  font-variant-numeric:tabular-nums}th{text-align:left;color:var(--muted);font-size:.8rem;
  text-transform:uppercase;letter-spacing:.06em}th,td{padding:12px;border-bottom:1px solid var(--line)}
tbody tr:nth-child(even){background:#f8faf8}.row-source{white-space:nowrap}
.points{padding-left:1.35rem;max-width:850px}.points li{margin:13px 0}
.decision-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));
  gap:14px;margin:28px 0}.decision-card{border:1px solid var(--line);
  border-radius:9px;padding:20px;background:var(--soft)}
.decision-overview,.decision-wide{grid-column:1/-1}
.decision-card h3{margin:0 0 10px;font-size:1rem}.decision-value{font-size:1.5rem;
  font-weight:700}.decision-card p{overflow-wrap:anywhere}.decision-card small{color:var(--muted)}
.decision-conditions{border-top:1px solid var(--line);padding-top:12px}
.trend{margin:28px 0}.trend figcaption{font-size:.82rem;font-weight:650;color:var(--muted);
  margin-bottom:10px}.trend-plot{display:grid;grid-template-columns:58px minmax(0,1fr);
  gap:8px;align-items:stretch}.trend-scale{position:relative;display:flex;
  flex-direction:column;justify-content:space-between;padding:14px 0;
  color:var(--muted);font-size:.76rem;font-variant-numeric:tabular-nums;text-align:right}
.trend-scale .zero-label{position:absolute;right:0;transform:translateY(-50%)}
.trend-line{display:block;width:100%;height:auto;overflow:visible}
.trend-line .grid-line{stroke:#d5dfda;stroke-width:1;shape-rendering:crispEdges}
.trend-line .zero-line{stroke:#879b92;stroke-width:1;stroke-dasharray:5 5;
  shape-rendering:crispEdges}.trend-line polyline{fill:none;stroke:var(--teal);
  stroke-width:3;stroke-linecap:round;stroke-linejoin:round;
  vector-effect:non-scaling-stroke}.trend-line circle{fill:#fff;stroke:var(--teal);
  stroke-width:3;vector-effect:non-scaling-stroke}.trend-mobile{display:none}
.trend-values{list-style:none;display:grid;grid-template-columns:repeat(auto-fit,minmax(125px,1fr));
  gap:12px;padding:0 0 0 66px;margin:10px 0 0}.trend-values li{display:flex;
  flex-wrap:wrap;align-items:baseline;gap:2px 7px;border-top:1px solid var(--line);
  padding-top:8px;font-variant-numeric:tabular-nums}.trend-values .trend-period{color:var(--muted);
  min-width:100%}.trend-values strong{font-size:1rem}.trend-values li>span:last-of-type{
  color:var(--muted);font-size:.75rem}
.timeline{list-style:none;padding:0}.timeline li{display:grid;grid-template-columns:130px 1fr 2fr auto;
  gap:12px;padding:14px 0;border-bottom:1px solid var(--line)}
.event-period{color:var(--muted)}a{color:var(--teal);text-underline-offset:3px}
.synthesis-note{max-width:1040px;margin:0 auto 22px;padding:18px 24px;
  border-left:4px solid var(--teal);background:#e9f1ed;color:var(--ink)}
.synthesis-note strong{display:block;margin-bottom:4px}
.source-ref{font-size:.85rem;white-space:nowrap}.slide-footer{display:flex;
  justify-content:space-between;align-items:baseline;gap:16px;border-top:1px solid var(--line);
  color:var(--muted);font-size:.82rem;margin-top:32px;padding-top:14px}
.slide-number{color:var(--teal);font-weight:750;letter-spacing:.1em;white-space:nowrap}
.sources{max-width:1040px;margin:44px auto 80px;padding:0 34px}
.sources summary{cursor:pointer;border:1px solid var(--line);border-radius:9px;
  background:#fff;padding:18px 22px;color:var(--teal);font-size:1.25rem;
  font-weight:700}.sources summary:hover{background:var(--soft)}
.sources summary:focus-visible{outline:3px solid var(--teal);outline-offset:3px}
.sources ol{list-style:none;padding:0}.sources li{display:flex;gap:16px;padding:14px 0;
  border-top:1px solid var(--line);scroll-margin-top:20px}.sources li>div{display:grid;gap:3px;
  min-width:0}.sources li a{overflow-wrap:anywhere}.sources li span{color:var(--muted)}
.source-number{font-weight:700}.sources li:target{background:#e2f1e9}
@media(max-width:650px){header.page-header{padding:32px 18px 16px}.slide{min-height:0;
  margin:10px;padding:24px 20px;border-radius:0}.slide h2{margin-top:0}
  .trend-desktop{display:none}.trend-mobile{display:block}.trend-plot{grid-template-columns:42px minmax(0,1fr)}
  .trend-values{padding-left:50px}.slide-footer{align-items:center}.footer-sources{min-width:0}
  .decision-grid{grid-template-columns:1fr}
  .timeline li{grid-template-columns:1fr}.sources{padding:0 18px}}
@media print{body{background:#fff}.page-header{padding-top:20px}.slide{box-shadow:none;
  border:none;border-radius:0;margin:0;padding:28px 8px;min-height:0;break-inside:avoid;
  break-before:page}.slide:first-of-type{break-before:auto}.sources{break-before:page}
  .sources:not([open])>ol{display:block!important}
  a{color:inherit}.trend-mobile{display:none}.trend-desktop{display:block}
  .trend-line{-webkit-print-color-adjust:exact;print-color-adjust:exact}}
.decision-card{break-inside:avoid}
"""


def _document(
    ticker: str, headline: str, analysis_date: str, report_timestamp: str,
    slides: str, appendix: str, source_count: int = 0,
) -> str:
    sources = (
        f'<details class="sources" id="sources"><summary>Sources ({source_count})</summary>'
        f'<ol>{appendix}</ol></details>'
        if appendix else ""
    )
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        '<meta http-equiv="Content-Security-Policy" content="default-src \'none\'; '
        "style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'\">"
        f'<title>{escape(ticker)} investor research</title><style>{_CSS}</style></head><body>'
        '<header class="page-header"><div class="kicker">TradingAgents · Investor research</div>'
        f'<h1>{escape(headline)}</h1><div class="page-meta">'
        f'Analysis {escape(analysis_date)} · Report {escape(report_timestamp)}</div></header>'
        f'<main>{slides}</main>{sources}</body></html>'
    )


def render_investor_deck(deck: InvestorDeck, report_timestamp: str) -> str:
    """Render validated blocks and exact source metadata, with no remote assets."""
    used = list(dict.fromkeys(
        key for slide in deck.slides for key in slide.source_ids if key in deck.sources
    ))
    numbers = {key: index for index, key in enumerate(used, 1)}
    slides = "".join(
        _render_slide(slide, numbers, index, len(deck.slides))
        for index, slide in enumerate(deck.slides, 1)
    )
    appendix = "".join(
        _render_source(deck.sources[key], numbers[key]) for key in used
    )
    return _document(
        deck.ticker, deck.headline, deck.analysis_date, report_timestamp,
        slides, appendix, len(used),
    )


def render_unavailable_investor_deck(
    ticker: str, analysis_date: str, full_report_href: str, report_timestamp: str,
) -> str:
    """Use only a same-ticker local full-report link, never an arbitrary URL."""
    safe_ticker = bool(re.fullmatch(r"[A-Z0-9.-]+", ticker))
    escaped_ticker = re.escape(ticker)
    safe = safe_ticker and bool(re.fullmatch(
        rf"(?:{escaped_ticker}_full_\d{{8}}T\d{{6}}Z\.html|"
        rf"/api/jobs/[a-f0-9]{{32}}/reports/{escaped_ticker}/full)",
        full_report_href,
    ))
    link = (
        f'<a href="{escape(full_report_href, quote=True)}">Full technical report</a>'
        if safe else ""
    )
    message = (
        '<section class="slide" data-topic="unavailable">'
        '<h2>Research deck unavailable</h2>'
        '<p>The available source material or validated synthesis was not sufficient '
        'to publish an investor research deck for this run.</p>'
        f'<p>{link}</p></section>'
    )
    return _document(
        ticker, "Research deck unavailable", analysis_date, report_timestamp,
        message, "",
    )
