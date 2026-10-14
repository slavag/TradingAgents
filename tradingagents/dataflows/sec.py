"""Bounded, point-in-time SEC filing and reported-fundamental evidence.

The public EDGAR submissions and company-facts APIs do not require a key, but
automated clients must identify themselves. This provider therefore requires a
declared ``SEC_USER_AGENT`` containing an organization or application name and
contact email. It returns selected filing metadata, bounded document excerpts,
and standardized XBRL facts without claiming exhaustive filing coverage.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from parsel import Selector

from tradingagents.settings import get_data_source_setting

from .config import get_config
from .sec_documents import (
    SecDocumentError,
    SecSection,
    _pace_sec_request,
    discover_exhibits,
    extract_sections,
    fetch_archive_html,
)
from .temporal import build_analysis_context

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_SUBMISSION_FILE_URL = "https://data.sec.gov/submissions/{name}"
SEC_COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
SEC_ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}"

REQUEST_TIMEOUT = 20
MAX_CURRENT_REPORTS = 3
MAX_FACTS = 5
MAX_ARCHIVE_FILES = 2
MAX_DOCUMENT_FETCHES = 7
MAX_SECTION_RECORDS = 15

_EMAIL_RE = re.compile(r"(?i)\b[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9.-]+\.[a-z]{2,}\b")
_TICKER_RE = re.compile(r"[A-Z0-9][A-Z0-9.-]{0,19}")
_ACCESSION_RE = re.compile(r"\d{10}-\d{2}-\d{6}")
_DOCUMENT_RE = re.compile(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*")
_ARCHIVE_RE = re.compile(r"CIK\d{10}-submissions-\d{3}\.json")
_FORMS = {"10-K", "10-Q", "8-K", "20-F", "40-F", "6-K"}
_SECTION_CATEGORY = {
    "Business": "business",
    "Competition": "business",
    "Management discussion": "quarterly_filing",
    "Results of operations": "earnings_release",
    "Quarterly highlights": "earnings_release",
    "Business highlights": "earnings_release",
    "Outlook": "earnings_release",
    "Officer appointment": "governance",
}

# Ordered aliases select a single current observation for each decision-useful
# metric. Standard taxonomies only: issuer-specific extension facts are outside
# this bounded initial collector.
_METRICS = (
    (
        "Revenue",
        (
            ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
            ("us-gaap", "Revenues"),
            ("ifrs-full", "Revenue"),
        ),
    ),
    (
        "Net income",
        (
            ("us-gaap", "NetIncomeLoss"),
            ("us-gaap", "ProfitLoss"),
            ("ifrs-full", "ProfitLoss"),
        ),
    ),
    (
        "Diluted earnings per share",
        (
            ("us-gaap", "EarningsPerShareDiluted"),
            ("ifrs-full", "DilutedEarningsLossPerShare"),
        ),
    ),
    (
        "Cash and cash equivalents",
        (
            ("us-gaap", "CashAndCashEquivalentsAtCarryingValue"),
            ("ifrs-full", "CashAndCashEquivalents"),
        ),
    ),
    (
        "Operating cash flow",
        (
            ("us-gaap", "NetCashProvidedByUsedInOperatingActivities"),
            ("ifrs-full", "CashFlowsFromUsedInOperatingActivities"),
        ),
    ),
)


def _record(
    category: str,
    title: str,
    content: str,
    url: str,
    published_at: str | None,
    as_of_date: str,
    status: str = "available",
    coverage: str = "full",
    **metadata: Any,
) -> dict[str, Any]:
    """Create the collector-neutral evidence record contract."""
    record = {
        "source": "sec",
        "category": category,
        "title": title,
        "content": content[:4000],
        "url": url,
        "published_at": published_at,
        "as_of_date": as_of_date,
        "status": status,
        "coverage": coverage,
    }
    record.update(metadata)
    return record


def _unavailable(category: str, reason: str, as_of_date: str) -> dict[str, Any]:
    return _record(
        category,
        f"SEC {category} unavailable",
        reason,
        "https://www.sec.gov/search-filings/edgar-application-programming-interfaces",
        None,
        as_of_date,
        "unavailable",
    )


def _user_agent() -> str | None:
    value = get_data_source_setting("SEC_USER_AGENT") or ""
    if len(value) < 8 or not _EMAIL_RE.search(value):
        return None
    return value[:256]


def _request_json(url: str, user_agent: str) -> dict[str, Any]:
    _pace_sec_request()
    response = requests.get(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("SEC returned a non-object JSON payload")
    return payload


def _parse_date(value: Any) -> date | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _ticker_aliases(ticker: str) -> set[str]:
    normalized = ticker.strip().upper()
    if not _TICKER_RE.fullmatch(normalized):
        return set()
    aliases = {normalized}
    if "." in normalized:
        aliases.add(normalized.replace(".", "-"))
    if "-" in normalized:
        aliases.add(normalized.replace("-", "."))
    return aliases


def _resolve_cik(payload: dict[str, Any], ticker: str) -> tuple[str, str] | None:
    aliases = _ticker_aliases(ticker)
    if not aliases:
        return None
    for entry in payload.values():
        if not isinstance(entry, dict):
            continue
        reported_ticker = entry.get("ticker")
        cik = entry.get("cik_str")
        if not isinstance(reported_ticker, str) or reported_ticker.upper() not in aliases:
            continue
        if isinstance(cik, bool) or not isinstance(cik, (int, str)):
            continue
        cik_text = str(cik)
        if not cik_text.isdigit() or len(cik_text) > 10:
            continue
        return cik_text.zfill(10), reported_ticker.upper()
    return None


def _rows_from_columns(columns: Any) -> list[dict[str, Any]]:
    if not isinstance(columns, dict):
        return []
    accessions = columns.get("accessionNumber")
    if not isinstance(accessions, list):
        return []
    rows: list[dict[str, Any]] = []
    for index in range(len(accessions)):
        row = {}
        for key, values in columns.items():
            if isinstance(key, str) and isinstance(values, list) and index < len(values):
                row[key] = values[index]
        rows.append(row)
    return rows


def _eligible_filing(row: dict[str, Any], cutoff: date) -> bool:
    filing_date = _parse_date(row.get("filingDate"))
    form = row.get("form")
    accession = row.get("accessionNumber")
    if not filing_date or filing_date > cutoff:
        return False
    if not isinstance(form, str) or form.removesuffix("/A") not in _FORMS:
        return False
    return isinstance(accession, str) and bool(_ACCESSION_RE.fullmatch(accession))


def _filing_url(cik: str, row: dict[str, Any]) -> str:
    accession = str(row["accessionNumber"])
    accession_path = accession.replace("-", "")
    document = row.get("primaryDocument")
    if not isinstance(document, str) or not _DOCUMENT_RE.fullmatch(document) or ".." in document:
        document = f"{accession}-index.html"
    return SEC_ARCHIVES_URL.format(
        cik=int(cik),
        accession=accession_path,
        document=quote(document, safe="/"),
    )


def _archive_candidates(files: Any, cutoff: date) -> list[str]:
    candidates = []
    if not isinstance(files, list):
        return candidates
    for item in files:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        first = _parse_date(item.get("filingFrom"))
        last = _parse_date(item.get("filingTo"))
        if isinstance(name, str) and _ARCHIVE_RE.fullmatch(name) and first and first <= cutoff:
            candidates.append(((last or first), name))
    candidates.sort(reverse=True)
    return [name for _, name in candidates[:MAX_ARCHIVE_FILES]]


def _collect_filing_rows(
    payload: dict[str, Any], cik: str, cutoff: date, user_agent: str
) -> tuple[list[dict[str, Any]], bool]:
    filings = payload.get("filings")
    if not isinstance(filings, dict):
        return [], False
    rows = _rows_from_columns(filings.get("recent"))
    archive_failed = False
    recent_eligible = [row for row in rows if _eligible_filing(row, cutoff)]
    annual = next(
        (row for row in recent_eligible if row["form"].removesuffix("/A") in
         {"10-K", "20-F", "40-F"}),
        None,
    )
    quarterly = any(row["form"].removesuffix("/A") == "10-Q" for row in recent_eligible)
    needs_archive = annual is None or (annual["form"].removesuffix("/A") == "10-K" and not quarterly)
    if needs_archive:
        for name in _archive_candidates(filings.get("files"), cutoff):
            try:
                archive = _request_json(
                    SEC_SUBMISSION_FILE_URL.format(name=quote(name)), user_agent
                )
            except (requests.RequestException, ValueError):
                archive_failed = True
                continue
            archive_columns = (
                archive.get("filings", {}).get("recent")
                if isinstance(archive.get("filings"), dict)
                else archive
            )
            rows.extend(_rows_from_columns(archive_columns))
    eligible = [row for row in rows if _eligible_filing(row, cutoff)]
    eligible.sort(key=lambda row: row["filingDate"], reverse=True)
    return _select_filing_rows(eligible), archive_failed


def _select_filing_rows(eligible: list[dict[str, Any]]) -> list[dict[str, Any]]:
    annual = next(
        (row for row in eligible if row["form"].removesuffix("/A") in
         {"10-K", "20-F", "40-F"}),
        None,
    )
    quarterly = next(
        (row for row in eligible if row["form"].removesuffix("/A") == "10-Q"),
        None,
    )
    current = [
        row for row in eligible
        if row["form"].removesuffix("/A") in {"8-K", "6-K"}
    ][:MAX_CURRENT_REPORTS]
    selected = {row["accessionNumber"]: row for row in (annual, quarterly, *current) if row}
    return sorted(selected.values(), key=lambda row: row["filingDate"], reverse=True)


def _format_filings(
    rows: list[dict[str, Any]], cik: str, ticker: str, cutoff: str, archive_failed: bool
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    records = []
    urls = {}
    for row in rows:
        accession = row["accessionNumber"]
        url = _filing_url(cik, row)
        urls[accession] = url
        report_date = _parse_date(row.get("reportDate"))
        report_text = (
            report_date.isoformat()
            if report_date and report_date <= date.fromisoformat(cutoff)
            else "not reported"
        )
        caveat = (
            "Selected from recent submissions and at most 2 older submission files; "
            "this is not exhaustive."
        )
        if archive_failed:
            caveat += " An eligible older submission file could not be retrieved."
        content = (
            f"Form: {row['form']}\n"
            f"Filing date: {row['filingDate']}\n"
            f"Report period: {report_text}\n"
            f"Accession number: {accession}\n"
            "Evidence type: SEC filing metadata; filing narrative was not extracted.\n"
            f"Coverage: {caveat} Current SEC ticker-to-CIK mapping was used."
        )
        records.append(
            _record(
                "filings",
                f"SEC {row['form']} filing for {ticker}",
                content,
                url,
                row["filingDate"],
                cutoff,
                coverage="metadata_only",
            )
        )
    return records, urls


def _exhibit_report_period(html: str) -> str | None:
    text = " ".join(" ".join(Selector(text=html).xpath("//body//text()").getall()).split())
    match = re.search(r"\bended\s+([A-Z][a-z]+)\s+(\d{1,2}),\s*(20\d{2})", text)
    if not match:
        return None
    try:
        return datetime.strptime(" ".join(match.groups()), "%B %d %Y").date().isoformat()
    except ValueError:
        return None


def _section_category(form: str, section: SecSection) -> str:
    if form in {"10-K", "20-F", "40-F"}:
        return "business"
    if form == "10-Q":
        return "quarterly_filing"
    if form == "EX-99.1":
        return "earnings_release"
    return _SECTION_CATEGORY.get(section.name, "document")


def _document_records(
    rows: list[dict[str, Any]], cik: str, ticker: str, as_of: str, user_agent: str
) -> list[dict[str, Any]]:
    """Keep selected SEC document failures separate from sibling evidence."""
    cache_dir = Path(get_config()["data_cache_dir"])
    records: list[dict[str, Any]] = []
    fetches = 0
    document_limits = {
        "10-K": 3, "20-F": 3, "40-F": 3, "10-Q": 2,
        "8-K": 2, "6-K": 2, "EX-99.1": 6,
    }

    def append_sections(
        sections: list[SecSection], row: dict[str, Any], url: str,
        document_type: str, report_period: str | None,
    ) -> None:
        limit = min(
            MAX_SECTION_RECORDS,
            len(records) + document_limits.get(document_type, 2),
        )
        for section in sections:
            if len(records) >= limit:
                break
            category = _section_category(document_type, section)
            records.append(
                _record(
                    category,
                    f"SEC {document_type} {section.name} for {ticker}",
                    section.content,
                    url,
                    row["filingDate"],
                    as_of,
                    coverage="partial",
                    form=row["form"],
                    accession=row["accessionNumber"],
                    document_type=document_type,
                    section=section.name,
                    report_period=report_period,
                    table_rows=[
                        [cell[:160] for cell in cells[:20]]
                        for cells in section.table_rows[:60]
                    ],
                )
            )

    def unavailable(row: dict[str, Any], url: str, document_type: str) -> None:
        if len(records) >= MAX_SECTION_RECORDS:
            return
        records.append(
            _record(
                "document",
                f"SEC {document_type} document unavailable for {ticker}",
                "A selected SEC document could not be retrieved or parsed.",
                url,
                row["filingDate"],
                as_of,
                status="unavailable",
                coverage="partial",
                form=row["form"],
                accession=row["accessionNumber"],
                document_type=document_type,
            )
        )

    # Reserve document and section capacity for business/quarterly context before
    # optional current reports and their exhibits consume the global budgets.
    ordered = sorted(
        rows,
        key=lambda row: (
            0 if row["form"].removesuffix("/A") in {"10-K", "20-F", "40-F"}
            else 1 if row["form"].removesuffix("/A") == "10-Q" else 2,
            -date.fromisoformat(row["filingDate"]).toordinal(),
        ),
    )
    for row in ordered:
        if fetches >= MAX_DOCUMENT_FETCHES or len(records) >= MAX_SECTION_RECORDS:
            break
        url = _filing_url(cik, row)
        document_type = row["form"].removesuffix("/A")
        fetches += 1
        try:
            html = fetch_archive_html(
                url,
                cik=cik,
                accession=row["accessionNumber"],
                user_agent=user_agent,
                cache_dir=cache_dir,
            )
            sections = extract_sections(html, document_type)
            if sections:
                append_sections(sections, row, url, document_type, row.get("reportDate"))
            else:
                unavailable(row, url, document_type)
        except (SecDocumentError, OSError, TypeError, ValueError):
            unavailable(row, url, document_type)
            continue

        if document_type not in {"8-K", "6-K"}:
            continue
        for exhibit_url in discover_exhibits(html, url, cik, row["accessionNumber"]):
            if fetches >= MAX_DOCUMENT_FETCHES or len(records) >= MAX_SECTION_RECORDS:
                break
            fetches += 1
            try:
                exhibit_html = fetch_archive_html(
                    exhibit_url,
                    cik=cik,
                    accession=row["accessionNumber"],
                    user_agent=user_agent,
                    cache_dir=cache_dir,
                )
                sections = extract_sections(exhibit_html, "EX-99.1")
                if sections:
                    append_sections(
                        sections, row, exhibit_url, "EX-99.1",
                        _exhibit_report_period(exhibit_html),
                    )
                else:
                    unavailable(row, exhibit_url, "EX-99.1")
            except (SecDocumentError, OSError, TypeError, ValueError):
                unavailable(row, exhibit_url, "EX-99.1")
    return records


def _safe_fact_value(value: Any) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return str(value)


def _fact_candidates(
    facts: dict[str, Any], aliases: tuple[tuple[str, str], ...], cutoff: date
) -> list[tuple[tuple[date, date, date, int, int], dict[str, Any]]]:
    candidates = []
    for alias_priority, (taxonomy, tag) in enumerate(aliases):
        namespace = facts.get(taxonomy)
        concept = namespace.get(tag) if isinstance(namespace, dict) else None
        units = concept.get("units") if isinstance(concept, dict) else None
        if not isinstance(units, dict):
            continue
        for unit_priority, (unit, observations) in enumerate(units.items()):
            if not isinstance(unit, str) or not isinstance(observations, list):
                continue
            for observation in observations:
                if not isinstance(observation, dict):
                    continue
                filed = _parse_date(observation.get("filed"))
                end = _parse_date(observation.get("end"))
                start = _parse_date(observation.get("start")) or end
                accession = observation.get("accn")
                value = _safe_fact_value(observation.get("val"))
                if (
                    not filed
                    or not end
                    or not start
                    or filed > cutoff
                    or end > cutoff
                    or value is None
                    or not isinstance(accession, str)
                    or not _ACCESSION_RE.fullmatch(accession)
                ):
                    continue
                data = {
                    **observation,
                    "taxonomy": taxonomy,
                    "tag": tag,
                    "unit": unit[:32],
                    "value": value,
                    "filed_date": filed,
                    "end_date": end,
                    "start_date": start,
                }
                candidates.append(((end, filed, start, -alias_priority, -unit_priority), data))
    return candidates


def _format_facts(
    payload: dict[str, Any], cik: str, ticker: str, cutoff: str, filing_urls: dict[str, str]
) -> list[dict[str, Any]]:
    facts = payload.get("facts")
    if not isinstance(facts, dict):
        return []
    cutoff_date = date.fromisoformat(cutoff)
    records = []
    for label, aliases in _METRICS:
        candidates = _fact_candidates(facts, aliases, cutoff_date)
        if not candidates:
            continue
        _, fact = max(candidates, key=lambda item: item[0])
        accession = str(fact["accn"])
        url = filing_urls.get(accession)
        if not url:
            url = SEC_ARCHIVES_URL.format(
                cik=int(cik),
                accession=accession.replace("-", ""),
                document=f"{accession}-index.html",
            )
        fiscal_year = fact.get("fy") if isinstance(fact.get("fy"), (str, int)) else "not reported"
        fiscal_period = fact.get("fp") if isinstance(fact.get("fp"), str) else "not reported"
        content = (
            f"Metric: {label}\n"
            f"Value: {fact['value']} {fact['unit']}\n"
            f"Period: {fact['start_date'].isoformat()} to {fact['end_date'].isoformat()}\n"
            f"Fiscal year: {fiscal_year}\n"
            f"Fiscal period: {fiscal_period}\n"
            f"Filing date: {fact['filed_date'].isoformat()}\n"
            f"Form: {fact.get('form', 'not reported')}\n"
            f"Taxonomy tag: {fact['taxonomy']}:{fact['tag']}\n"
            f"Accession number: {accession}\n"
            "Evidence type: SEC companyfacts reported XBRL fact. Custom taxonomy, "
            "segment-only values, and filing narrative are outside this selection."
        )
        records.append(
            _record(
                "fundamentals",
                f"SEC reported {label} for {ticker}",
                content,
                url,
                fact["filed_date"].isoformat(),
                cutoff,
                coverage="reported_fact",
            )
        )
        if len(records) == MAX_FACTS:
            break
    return records


def get_company_filings(ticker: str, curr_date: str) -> list[dict[str, Any]]:
    """Return selected SEC filing and XBRL evidence available by ``curr_date``.

    Failures remain data: this optional provider returns an ``unavailable``
    record rather than exposing request details or aborting the analysis run.
    """
    as_of = curr_date if isinstance(curr_date, str) else ""
    try:
        if not isinstance(curr_date, str) or not curr_date:
            raise ValueError("missing date")
        context = build_analysis_context(curr_date)
        as_of = context.as_of_date.isoformat()
        if curr_date != as_of:
            raise ValueError("date is not canonical")
    except (TypeError, ValueError):
        return [
            _unavailable(
                "filings",
                "Invalid required analysis date. Use YYYY-MM-DD and do not use a future date.",
                as_of,
            )
        ]

    user_agent = _user_agent()
    if not user_agent:
        return [
            _unavailable(
                "filings",
                "SEC_USER_AGENT is required and must identify an organization or application "
                "with a contact email before SEC data can be requested.",
                as_of,
            )
        ]

    try:
        ticker_payload = _request_json(SEC_TICKERS_URL, user_agent)
    except (requests.RequestException, ValueError):
        return [_unavailable("filings", "SEC issuer lookup could not be retrieved.", as_of)]
    resolved = _resolve_cik(ticker_payload, ticker)
    if not resolved:
        return [
            _unavailable(
                "filings",
                "The exact ticker, including an optional dot/dash share-class alias, "
                "was not found in the SEC issuer index.",
                as_of,
            )
        ]
    cik, reported_ticker = resolved

    records: list[dict[str, Any]] = []
    filing_urls: dict[str, str] = {}
    rows: list[dict[str, Any]] = []
    try:
        submissions = _request_json(SEC_SUBMISSIONS_URL.format(cik=cik), user_agent)
        rows, archive_failed = _collect_filing_rows(
            submissions, cik, context.as_of_date, user_agent
        )
        filing_records, filing_urls = _format_filings(
            rows, cik, reported_ticker, as_of, archive_failed
        )
        records.extend(filing_records)
        if not filing_records:
            records.append(
                _unavailable(
                    "filings",
                    "No supported primary filing metadata was found in the bounded SEC "
                    "submissions search by the analysis date.",
                    as_of,
                )
            )
    except (requests.RequestException, TypeError, ValueError):
        records.append(
            _unavailable("filings", "SEC filing metadata could not be retrieved.", as_of)
        )

    if rows:
        try:
            records.extend(_document_records(rows, cik, reported_ticker, as_of, user_agent))
        except Exception:  # noqa: BLE001 - optional enrichment cannot abort the full run
            records.append(
                _unavailable("document", "SEC document enrichment could not be completed.", as_of)
            )

    try:
        company_facts = _request_json(SEC_COMPANY_FACTS_URL.format(cik=cik), user_agent)
        fact_records = _format_facts(company_facts, cik, reported_ticker, as_of, filing_urls)
        records.extend(fact_records)
        if not fact_records:
            records.append(
                _unavailable(
                    "fundamentals",
                    "No supported standardized SEC XBRL financial fact was published by "
                    "the analysis date; custom taxonomy and segment-only facts are not selected.",
                    as_of,
                )
            )
    except (requests.RequestException, TypeError, ValueError):
        records.append(
            _unavailable(
                "fundamentals", "SEC reported financial facts could not be retrieved.", as_of
            )
        )

    return records
