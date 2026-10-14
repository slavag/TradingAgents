"""Dated source facts for investor research; no model claims or presentation markup."""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from tradingagents.agents.utils.evidence import evidence_is_admissible


@dataclass(frozen=True)
class ResearchSource:
    evidence_id: str
    provider: str
    title: str
    url: str | None
    published_at: str | None
    section: str | None
    status: str
    accession: str | None = None
    form: str | None = None
    document_type: str | None = None
    report_period: str | None = None
    coverage: str | None = None


@dataclass(frozen=True)
class ResearchExcerpt:
    category: str
    evidence_id: str
    text: str
    table_rows: tuple[tuple[str, ...], ...] = ()


@dataclass(frozen=True)
class ResearchFact:
    metric: str
    value: Decimal
    unit: str
    period_label: str
    period_end: str | None
    period_kind: Literal["quarterly", "annual", "ytd", "ttm", "instant"]
    basis: Literal["gaap", "non_gaap", "guidance", "derived", "market", "estimate"]
    source_ids: tuple[str, ...]


@dataclass(frozen=True)
class ResearchLedger:
    facts: tuple[ResearchFact, ...]
    sources: Mapping[str, ResearchSource]
    excerpts: Mapping[str, tuple[ResearchExcerpt, ...]]

    def facts_for(self, metric: str) -> tuple[ResearchFact, ...]:
        return tuple(fact for fact in self.facts if fact.metric == metric)

    def latest(
        self, metric: str, period_kind: str = "quarterly", basis: str = "gaap"
    ) -> ResearchFact | None:
        eligible = (
            fact for fact in self.facts_for(metric)
            if fact.period_kind == period_kind and fact.basis == basis
            and fact.period_end is not None
        )
        return max(
            eligible,
            key=lambda fact: (
                fact.period_end or "",
                _source_rank(fact, self.sources),
            ),
            default=None,
        )

    def segment_mix(self, period_label: str) -> dict[str, Decimal]:
        totals = [
            fact for fact in self.facts
            if fact.metric == "net_sales" and fact.period_label == period_label
            and fact.period_kind == "annual" and fact.basis == "gaap" and fact.value > 0
        ]
        if not totals:
            return {}
        total = max(totals, key=lambda fact: _source_rank(fact, self.sources))
        return {
            fact.metric.removeprefix("segment:"): (
                fact.value / total.value * 100
            ).quantize(Decimal("0.1"))
            for fact in self.facts
            if fact.metric.startswith("segment:") and fact.period_label == period_label
            and fact.period_kind == "annual" and fact.basis == total.basis
            and fact.source_ids == total.source_ids
        }


def _source_rank(fact: ResearchFact, sources: Mapping[str, ResearchSource]) -> int:
    return int(any(
        sources.get(key) is not None and sources[key].provider == "sec"
        for key in fact.source_ids
    ))


_PERIOD = re.compile(r"(?:Q[1-4]-\d{2}|FY\d{2})\Z", re.I)
_DATE = re.compile(r"20\d{2}-\d{2}-\d{2}\Z")
_METRIC_ALIASES = {
    "total net sales": "net_sales",
    "total revenue": "net_sales",
    "revenue": "net_sales",
    "operating income": "operating_income",
    "net income": "net_income",
    "operating cash flow": "operating_cash_flow",
    "free cash flow": "free_cash_flow",
    "cash and cash equivalents": "cash",
    "total debt": "total_debt",
    "diluted earnings per share": "diluted_eps",
    "diluted eps": "diluted_eps",
}


def _metric(label: str) -> str | None:
    normalized = " ".join(label.casefold().split())
    if normalized.startswith("net income attributable to "):
        return "net_income"
    return _METRIC_ALIASES.get(normalized)


def _decimal_cell(cell: str) -> Decimal | None:
    raw = cell.strip().replace(",", "").replace("$", "")
    if raw.startswith("(") and raw.endswith(")"):
        raw = "-" + raw[1:-1]
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def _period_end(label: str, report_period: Any, first: bool) -> str | None:
    if first and isinstance(report_period, str) and _DATE.fullmatch(report_period):
        return report_period
    return label if _DATE.fullmatch(label) else None


def _sec_table_facts(row: Mapping[str, Any], key: str) -> list[ResearchFact]:
    rows = row.get("table_rows")
    if not isinstance(rows, list):
        return []
    header_index = next((
        index for index, cells in enumerate(rows)
        if isinstance(cells, list) and any(
            _PERIOD.fullmatch(str(cell).strip()) for cell in cells
        )
    ), None)
    if header_index is None:
        return []
    header = rows[header_index]
    marker_rows = rows[max(0, header_index - 2):header_index + 2]
    basis_markers = next((
        cells for cells in marker_rows if isinstance(cells, list)
        and any("non-gaap" in str(cell).casefold() for cell in cells)
    ), [])
    has_adjusted_columns = bool(basis_markers)
    columns: list[tuple[str, str, bool]] = []
    seen_periods: set[str] = set()
    for index, cell in enumerate(header[1:], 1):
        label = str(cell).strip().upper()
        if not _PERIOD.fullmatch(label):
            continue
        marker_text = (
            str(basis_markers[index]).casefold()
            if len(basis_markers) >= len(header) and index < len(basis_markers) else ""
        )
        adjusted = ("non-gaap" in marker_text or "non gaap" in marker_text
                    or (has_adjusted_columns and label in seen_periods
                        and not marker_text))
        columns.append((label, "non_gaap" if adjusted else "gaap", not columns))
        seen_periods.add(label)
    if not columns:
        return []

    scale = Decimal(1000) if re.search(
        r"\bin thousands\b",
        str(row.get("content") or "") + " " + str(header[0]), re.I,
    ) else Decimal(1)
    total_index = next((
        index for index, cells in enumerate(rows)
        if isinstance(cells, list) and cells and _metric(str(cells[0])) == "net_sales"
    ), None)
    segment_labels = set()
    if total_index is not None:
        segment_labels = {
            str(cells[0]).strip() for cells in rows[header_index + 1:total_index]
            if isinstance(cells, list) and cells and str(cells[0]).strip()
            and _metric(str(cells[0])) is None
            and str(cells[0]).casefold() not in {"net sales:", "metric", "gaap", "non-gaap"}
            and not any("gaap" in str(cell).casefold() for cell in cells)
        }

    facts: list[ResearchFact] = []
    for cells in rows[header_index + 1:]:
        if not isinstance(cells, list) or not cells or not str(cells[0]).strip():
            continue
        label = str(cells[0]).strip()
        metric = _metric(label)
        if metric is None and label in segment_labels:
            metric = "segment:" + label
        if metric is None:
            continue
        values = [value for cell in cells[1:] if (value := _decimal_cell(str(cell))) is not None]
        for (period, basis, first), value in zip(columns, values, strict=False):
            is_eps = metric == "diluted_eps"
            facts.append(ResearchFact(
                metric=metric, value=value if is_eps else value * scale,
                unit="USD_per_share" if is_eps else "USD",
                period_label=period,
                period_end=_period_end(period, row.get("report_period"), first),
                period_kind="annual" if period.startswith("FY") else "quarterly",
                basis=basis, source_ids=(key,),
            ))
    return facts


def _vendor_csv_facts(row: Mapping[str, Any], key: str) -> list[ResearchFact]:
    content = str(row.get("content") or "")
    csv_rows = list(csv.reader(content.splitlines()))
    header = next((
        cells for cells in csv_rows if cells and not cells[0]
        and any(_DATE.fullmatch(cell.strip()) for cell in cells[1:])
    ), None)
    if header is None:
        return []
    source = str(row.get("source") or "")
    kind = (
        "instant" if source == "get_balance_sheet" else
        "annual" if re.search(r"\(annual\)", content[:500], re.I) else
        "quarterly"
    )
    facts: list[ResearchFact] = []
    for cells in csv_rows:
        metric = _metric(cells[0]) if cells else None
        if metric is None:
            continue
        for date_text, value_text in zip(header[1:], cells[1:], strict=False):
            period = date_text.strip()
            value = _decimal_cell(value_text)
            if not _DATE.fullmatch(period) or value is None:
                continue
            facts.append(ResearchFact(
                metric, value, "USD_per_share" if metric == "diluted_eps" else "USD",
                period, period, kind, "gaap", (key,),
            ))
    return facts


def _fundamentals_facts(row: Mapping[str, Any], key: str) -> list[ResearchFact]:
    labels = {
        "Market Cap": ("market_cap", "USD", "instant", "market"),
        "PE Ratio (TTM)": ("pe_ttm", "multiple", "ttm", "market"),
        "Forward PE": ("pe_forward", "multiple", "instant", "estimate"),
        "Free Cash Flow": ("free_cash_flow", "USD", "ttm", "derived"),
    }
    facts: list[ResearchFact] = []
    for line in str(row.get("content") or "").splitlines():
        label, separator, raw = line.partition(":")
        if not separator or label not in labels:
            continue
        value = _decimal_cell(raw)
        if value is None:
            continue
        metric, unit, kind, basis = labels[label]
        period = "TTM" if kind == "ttm" else str(row.get("as_of_date"))
        facts.append(ResearchFact(
            metric, value, unit, period,
            None if kind == "ttm" else period, kind, basis, (key,),
        ))
    return facts


def _record_fields(content: str) -> dict[str, str]:
    return dict(
        line.partition(": ")[::2] for line in content.splitlines() if ": " in line
    )


def _sec_companyfact(row: Mapping[str, Any], key: str, analysis_date: str) -> ResearchFact | None:
    """Parse one as-filed XBRL fact without inventing a quarterly cash period."""
    content = str(row.get("content") or "")
    if "SEC companyfacts reported XBRL fact" not in content:
        return None
    fields = _record_fields(content)
    metric = _metric(fields.get("Metric", ""))
    value_match = re.fullmatch(r"(-?[\d,.]+)\s+(USD|USD/shares)", fields.get("Value", ""))
    period_match = re.fullmatch(
        r"(20\d{2}-\d{2}-\d{2}) to (20\d{2}-\d{2}-\d{2})",
        fields.get("Period", ""),
    )
    filing_date = fields.get("Filing date", "")
    if not metric or not value_match or not period_match or not _DATE.fullmatch(filing_date):
        return None
    try:
        start, end, filed, cutoff = (
            date.fromisoformat(period_match.group(1)),
            date.fromisoformat(period_match.group(2)),
            date.fromisoformat(filing_date),
            date.fromisoformat(analysis_date),
        )
    except ValueError:
        return None
    if start > end or end > cutoff or filed > cutoff:
        return None
    value = _decimal_cell(value_match.group(1))
    if value is None:
        return None
    fiscal_year = fields.get("Fiscal year", "")
    fiscal_period = fields.get("Fiscal period", "")
    if not re.fullmatch(r"20\d{2}", fiscal_year):
        return None
    if start == end:
        kind = "instant"
        label = end.isoformat()
    elif (end - start).days <= 125 and re.fullmatch(r"Q[1-4]", fiscal_period):
        kind = "quarterly"
        label = f"{fiscal_period}-{fiscal_year[-2:]}"
    elif fields.get("Form") in {"10-K", "20-F", "40-F"} and (end - start).days <= 400:
        kind = "annual"
        label = f"FY{fiscal_year[-2:]}"
    else:
        kind = "ytd"
        label = f"FY{fiscal_year[-2:]} {fiscal_period} YTD"
    return ResearchFact(
        metric, value,
        "USD_per_share" if value_match.group(2) == "USD/shares" else "USD",
        label, end.isoformat(), kind, "gaap", (key,),
    )


def _team_excerpt_fields(state: Mapping[str, Any]) -> dict[str, str]:
    debate = state.get("investment_debate_state") or {}
    return {
        "bull": str(debate.get("bull_history") or "") if isinstance(debate, Mapping) else "",
        "bear": str(debate.get("bear_history") or "") if isinstance(debate, Mapping) else "",
        "portfolio": str(state.get("final_trade_decision") or ""),
    }


def build_research_ledger(state: Mapping[str, Any], analysis_date: str) -> ResearchLedger:
    """Convert admitted evidence to typed facts without treating team prose as facts."""
    facts: list[ResearchFact] = []
    sources: dict[str, ResearchSource] = {}
    excerpts: defaultdict[str, list[ResearchExcerpt]] = defaultdict(list)
    for row in state.get("shared_evidence") or []:
        if (not isinstance(row, Mapping) or not evidence_is_admissible(row, analysis_date)
                or row.get("coverage") == "metadata_only"):
            continue
        key = str(row.get("evidence_id") or "")
        if not key:
            continue
        source = str(row.get("source") or "")
        category = str(row.get("category") or "")
        sec_fact_fields = (
            _record_fields(str(row.get("content") or ""))
            if source == "sec" and category == "fundamentals"
            and row.get("coverage") == "reported_fact" else {}
        )
        fact_period = sec_fact_fields.get("Period", "").rsplit(" to ", 1)[-1]
        if not _DATE.fullmatch(fact_period):
            fact_period = ""
        sources[key] = ResearchSource(
            key, source, str(row.get("title") or ""), row.get("url"),
            row.get("published_at"), row.get("section"), "available",
            accession=row.get("accession") or sec_fact_fields.get("Accession number"),
            form=row.get("form") or sec_fact_fields.get("Form"),
            document_type=row.get("document_type"),
            report_period=fact_period or row.get("report_period"), coverage=row.get("coverage"),
        )
        if row.get("table_rows"):
            facts.extend(_sec_table_facts(row, key))
        elif source in {"get_income_statement", "get_cashflow", "get_balance_sheet"}:
            facts.extend(_vendor_csv_facts(row, key))
        elif source == "get_fundamentals":
            facts.extend(_fundamentals_facts(row, key))
        elif source == "sec" and category == "fundamentals" and (
            fact := _sec_companyfact(row, key, analysis_date)
        ):
            facts.append(fact)
        if category in {"business", "governance", "earnings_release", "quarterly_filing"}:
            table_rows = row.get("table_rows") or []
            excerpts[category].append(ResearchExcerpt(
                category, key, str(row.get("content") or "")[:6000],
                tuple(
                    tuple(str(cell) for cell in cells[:20])
                    for cells in table_rows[:120] if isinstance(cells, (list, tuple))
                ) if isinstance(table_rows, (list, tuple)) else (),
            ))
    for name, content in _team_excerpt_fields(state).items():
        if content:
            key = "analysis:" + name
            sources[key] = ResearchSource(key, "analysis", name, None, None, None, "available")
            excerpts["analysis"].append(ResearchExcerpt("analysis", key, content[:6000]))
    return ResearchLedger(
        tuple(facts), sources,
        {category: tuple(items) for category, items in excerpts.items()},
    )
