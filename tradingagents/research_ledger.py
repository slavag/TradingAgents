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
    "gross profit": "gross_profit",
    "loss from operations": "operating_income",
    "net income (loss)": "net_income",
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


_MONTHS = {name.casefold(): number for number, name in enumerate((
    "January", "February", "March", "April", "May", "June", "July", "August",
    "September", "October", "November", "December",
), 1)}
_DURATION = re.compile(
    r"\b(Three|Six|Nine|Twelve|3|6|9|12)\s+Months?\s+Ended\s+"
    r"(" + "|".join(_MONTHS) + r")\s+(\d{1,2})", re.I,
)


def _accounting_bases(text: str) -> set[str]:
    bases = set()
    without_non_gaap, count = re.subn(r"\bnon[- ]gaap\b", "", text, flags=re.I)
    if count or re.search(r"\badjusted\b", text, re.I):
        bases.add("non_gaap")
    if re.search(r"\bgaap\b", without_non_gaap, re.I):
        bases.add("gaap")
    return bases


def _duration_metric(label: str) -> tuple[str | None, set[str]]:
    prefix = re.match(r"^(non[- ]gaap|gaap|adjusted)\b[\s:–-]*", label, re.I)
    if prefix:
        return _metric(label[prefix.end():]), _accounting_bases(prefix.group())
    return _metric(label), set()


def _duration_table_facts(row: Mapping[str, Any], key: str, rows: list) -> list[ResearchFact]:
    """Read explicit month-duration/year headers, rejecting incomplete comparatives."""
    for header_index, header in enumerate(rows[:12]):
        if not isinstance(header, list):
            continue
        duration_header = next((
            cells for cells in reversed(rows[:header_index])
            if isinstance(cells, list) and any(_DURATION.search(str(c)) for c in cells)
        ), None)
        if duration_header is None:
            continue

        def duration_identity(position, owned_header=duration_header):
            match = (_DURATION.search(str(owned_header[position]))
                     if position < len(owned_header) else None)
            return tuple(part.casefold() for part in match.groups()) if match else None

        years = []
        for column, cell in enumerate(header):
            year = str(cell).strip()
            if re.fullmatch(r"20\d{2}", year):
                # Colspan-expanded year labels repeat across formatting columns.
                def column_basis(position):
                    return set().union(*(
                        _accounting_bases(str(cells[position]))
                        for cells in rows[:12]
                        if isinstance(cells, list) and len(cells) > position
                        and not str(cells[0]).strip()
                    ))

                if (not years or years[-1][1] != year or years[-1][2] != column - 1
                        or column_basis(column) != column_basis(column - 1)
                        or duration_identity(column) != duration_identity(column - 1)):
                    years.append([column, year, column])
                else:
                    years[-1][2] = column
        if not years:
            continue
        columns = []
        for column, year, _ in years:
            # Only aligned, expanded cells can establish duration ownership.
            match = (_DURATION.search(str(duration_header[column]))
                     if column < len(duration_header) else None)
            if match is None:
                columns = []
                break
            duration, month, day = match.groups()
            months = {"three": 3, "six": 6, "nine": 9, "twelve": 12}.get(duration.casefold())
            months = months or int(duration)
            try:
                end = date(int(year), _MONTHS[month.casefold()], int(day)).isoformat()
            except ValueError:
                columns = []
                break
            kind = "quarterly" if months == 3 else "annual" if months == 12 else "ytd"
            columns.append((f"{months}M ended {end}", end, kind))
        if not columns:
            return []
        context = " ".join(str(row.get("content") or "").split()) + " " + " ".join(
            str(cell) for cells in rows[:header_index + 1] if isinstance(cells, list) for cell in cells
        )
        scales = set(re.findall(r"\bin (thousands|millions|billions|dollars)\b", context, re.I))
        scales = {value.casefold() for value in scales}
        if not scales and re.search(r"\bUSD\b|\bU\.S\. dollars\b", context, re.I):
            scales.add("dollars")
        if len(scales) != 1 or "billions" in scales:
            return []
        scale = {"thousands": Decimal(1000), "millions": Decimal(1000000), "dollars": Decimal(1)}[scales.pop()]
        context_bases = _accounting_bases(context)
        basis_headers = []
        for cells in rows:
            if not isinstance(cells, list) or not cells:
                continue
            if _duration_metric(str(cells[0]))[0] is not None:
                break
            basis_headers.append(cells)
        column_bases = [set() for _ in columns]
        for index, (start, _, _) in enumerate(years):
            stop = years[index + 1][0] if index + 1 < len(years) else len(header)
            for cells in basis_headers:
                for cell in cells[start:stop]:
                    column_bases[index].update(_accounting_bases(str(cell)))
        facts = []
        for cells in rows[header_index + 1:]:
            if not isinstance(cells, list) or not cells:
                continue
            metric, row_bases = _duration_metric(str(cells[0]))
            if metric is None:
                continue
            # A detached sign/parenthesis must never be discarded while accepting
            # its unsigned numeric neighbor, including in the ragged-row fallback.
            if any(re.fullmatch(r"[\s$()+−-]*[()+−-][\s$()+−-]*", str(cell)) for cell in cells[1:]):
                continue
            values = []
            if len(cells) == len(header):
                for index, (start, _, _) in enumerate(years):
                    stop = years[index + 1][0] if index + 1 < len(years) else len(cells)
                    # Expanded colspan values repeat; each period must own one value.
                    candidates = {
                        value for cell in cells[start:stop]
                        if (value := _decimal_cell(str(cell))) is not None
                    }
                    if len(candidates) != 1:
                        values = []
                        break
                    values.append(candidates.pop())
            else:
                values = [v for c in cells[1:] if (v := _decimal_cell(str(c))) is not None]
            if len(values) != len(columns):
                continue
            for (label, end, kind), value, explicit_bases in zip(columns, values, column_bases, strict=True):
                bases = explicit_bases | row_bases
                if not bases:
                    bases = context_bases or {"gaap"}
                if len(bases) != 1:
                    continue
                basis = next(iter(bases))
                eps = metric == "diluted_eps"
                facts.append(ResearchFact(
                    metric, value if eps else value * scale,
                    "USD_per_share" if eps else "USD", label, end, kind, basis, (key,),
                ))
        return facts
    return []


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
        return _duration_table_facts(row, key, rows)
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
        if len(values) != len(columns):
            continue
        for (period, basis, first), value in zip(columns, values, strict=True):
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
        stated_currency = re.fullmatch(r"\s*(.+?)\s+([A-Za-z]{3})\s*", raw)
        numeric = stated_currency.group(1) if stated_currency else raw
        value = _decimal_cell(numeric)
        if value is None:
            continue
        metric, unit, kind, basis = labels[label]
        if stated_currency and unit == "USD":
            unit = stated_currency.group(2)
        observed = str(row.get("as_of_date") or "")
        period = "TTM" if kind == "ttm" else observed
        end = None if kind == "ttm" else period
        if metric == "pe_ttm" and _DATE.fullmatch(observed):
            period, end = f"TTM multiple snapshot {observed}", observed
        facts.append(ResearchFact(metric, value, unit, period, end, kind, basis, (key,)))
    return facts


def _market_snapshot_facts(row: Mapping[str, Any], key: str, analysis_date: str) -> list[ResearchFact]:
    """Extract only the latest dated row and named indicators from a verified snapshot."""
    content = str(row.get("content") or "")
    date_match = re.search(r"(?m)^- Latest trading row used: (20\d{2}-\d{2}-\d{2})$", content)
    if date_match is None or date_match.group(1) > analysis_date:
        return []
    observed_on = date_match.group(1)
    before_history, _, history = content.partition("### Recent verified closes")
    wanted = {
        "close": "price", "close_10_ema": "price",
        "close_50_sma": "price", "close_200_sma": "price",
        "boll": "price", "boll_lb": "price",
        "boll_ub": "price", "atr": "price",
        "rsi": "score", "macd": "price", "macds": "price",
    }
    facts = []
    for label, raw in re.findall(
        r"(?m)^\|\s*([A-Za-z][A-Za-z0-9_]*)\s*\|\s*([\d,.]+)\s*\|$",
        before_history,
    ):
        metric = label.casefold()
        value = _decimal_cell(raw)
        if metric in wanted and value is not None and value > 0:
            facts.append(ResearchFact(
                metric, value, wanted[metric], observed_on, observed_on,
                "instant", "market", (key,),
            ))
    for period, raw in re.findall(
        r"(?m)^\|\s*(20\d{2}-\d{2}-\d{2})\s*\|\s*([\d,.]+)\s*\|$",
        history,
    ):
        value = _decimal_cell(raw)
        if period >= observed_on or value is None or value <= 0:
            continue
        facts.append(ResearchFact(
            "close", value, "price", period, period,
            "instant", "market", (key,),
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
        title = (
            "Verified market snapshot" if source == "get_verified_market_snapshot"
            else str(row.get("title") or "")
        )
        sources[key] = ResearchSource(
            key, source, title, row.get("url"),
            row.get("published_at"), row.get("section"), "available",
            accession=row.get("accession") or sec_fact_fields.get("Accession number"),
            form=row.get("form") or sec_fact_fields.get("Form"),
            document_type=row.get("document_type"),
            report_period=fact_period or row.get("report_period"), coverage=row.get("coverage"),
        )
        if source == "get_verified_market_snapshot":
            facts.extend(_market_snapshot_facts(row, key, analysis_date))
        elif row.get("table_rows"):
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
                    tuple(str(cell) for cell in cells)
                    for cells in table_rows[:120]
                    if isinstance(cells, (list, tuple)) and len(cells) <= 64
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
