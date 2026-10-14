"""SEC evidence is dated, attributable, bounded, and optional without configuration."""

import importlib
import json
from datetime import date, timedelta
from pathlib import Path

import pytest
import requests

from tradingagents.dataflows.sec_documents import SecDocumentError, SecSection

pytestmark = pytest.mark.unit

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK0000320193.json"
FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"
TICKERS = {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}
ACCESSION = "0000320193-25-000010"
FILING_URL = "https://www.sec.gov/Archives/edgar/data/320193/000032019325000010/aapl-20241228.htm"


@pytest.fixture
def sec(monkeypatch):
    module = importlib.import_module("tradingagents.dataflows.sec")

    def no_document_fixture(*args, **kwargs):
        raise SecDocumentError("No document fixture provided")

    monkeypatch.setattr(module, "fetch_archive_html", no_document_fixture, raising=False)
    monkeypatch.setattr(module, "_pace_sec_request", lambda: None)
    return module


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setenv("SEC_USER_AGENT", "Research Team analyst@example.org")


def _filing(**overrides):
    return {
        "accessionNumber": ACCESSION,
        "filingDate": "2025-01-31",
        "reportDate": "2024-12-28",
        "acceptanceDateTime": "2025-01-31T18:30:00.000Z",
        "act": "34",
        "form": "10-Q",
        "fileNumber": "001-36743",
        "filmNumber": "25123456",
        "items": "",
        "size": 123456,
        "isXBRL": 1,
        "isInlineXBRL": 1,
        "primaryDocument": "aapl-20241228.htm",
        "primaryDocDescription": "10-Q",
        **overrides,
    }


def _columns(rows):
    return {key: [row.get(key, "") for row in rows] for key in _filing()}


def _submissions(rows=(), files=()):
    return {
        "cik": "0000320193",
        "name": "Apple Inc.",
        "tickers": ["AAPL"],
        "exchanges": ["Nasdaq"],
        "filings": {"recent": _columns(rows), "files": list(files)},
    }


def _fact(**overrides):
    return {
        "start": "2024-09-29",
        "end": "2024-12-28",
        "val": 124300000000,
        "accn": ACCESSION,
        "fy": 2025,
        "fp": "Q1",
        "form": "10-Q",
        "filed": "2025-01-31",
        "frame": "CY2024Q4",
        **overrides,
    }


def _facts(observations=(), tag="RevenueFromContractWithCustomerExcludingAssessedTax", unit="USD"):
    return {
        "cik": 320193,
        "entityName": "Apple Inc.",
        "facts": {
            "us-gaap": {
                tag: {
                    "label": "Revenue",
                    "description": "Revenue from contracts with customers",
                    "units": {unit: list(observations)},
                }
            }
        },
    }


def _mock_http(monkeypatch, sec, payloads):
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        assert url in payloads, f"Unexpected SEC URL: {url}"
        payload = payloads[url]
        if isinstance(payload, Exception):
            raise payload
        response = requests.Response()
        response.status_code = 200
        response.url = url
        response._content = json.dumps(payload).encode("utf-8")
        return response

    monkeypatch.setattr(sec.requests, "get", get)
    return calls


def test_sec_json_uses_shared_request_pacing(sec, monkeypatch):
    paced = []
    monkeypatch.setattr(sec, "_pace_sec_request", lambda: paced.append(True))
    _mock_http(monkeypatch, sec, {TICKERS_URL: TICKERS})

    assert sec._request_json(TICKERS_URL, "Research Team analyst@example.org") == TICKERS
    assert paced == [True]


def test_missing_user_agent_returns_unavailable_without_network(sec, monkeypatch):
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    calls = _mock_http(monkeypatch, sec, {})

    records = sec.get_company_filings("AAPL", "2025-02-01")

    assert records[0]["status"] == "unavailable"
    assert "SEC_USER_AGENT" in records[0]["content"]
    assert records[0]["published_at"] is None
    assert records[0]["as_of_date"] == "2025-02-01"
    assert calls == []


@pytest.mark.parametrize("cutoff", [None, "", "not-a-date", "2025-02-30"])
def test_invalid_required_date_does_not_fetch_current_data(sec, configured, monkeypatch, cutoff):
    calls = _mock_http(monkeypatch, sec, {})

    records = sec.get_company_filings("AAPL", cutoff)

    assert records[0]["status"] == "unavailable"
    assert "date" in records[0]["content"].lower()
    assert calls == []


def test_future_date_is_rejected_before_http(sec, configured, monkeypatch):
    calls = _mock_http(monkeypatch, sec, {})
    cutoff = (date.today() + timedelta(days=1)).isoformat()

    records = sec.get_company_filings("AAPL", cutoff)

    assert records[0]["status"] == "unavailable"
    assert calls == []


def test_filings_are_dated_source_links_and_metadata_only(sec, configured, monkeypatch):
    calls = _mock_http(
        monkeypatch,
        sec,
        {
            TICKERS_URL: TICKERS,
            SUBMISSIONS_URL: _submissions(
                [
                    _filing(filingDate="2025-02-04", accessionNumber="0000320193-25-000020"),
                    _filing(),
                    _filing(form="4", filingDate="2025-01-30"),
                ]
            ),
            FACTS_URL: _facts(),
        },
    )

    records = sec.get_company_filings("aapl", "2025-02-01")
    filings = [record for record in records if record["category"] == "filings"]

    assert len(filings) == 1
    filing = filings[0]
    assert filing["status"] == "available"
    assert filing["coverage"] == "metadata_only"
    assert filing["source"] == "sec"
    assert filing["published_at"] == "2025-01-31"
    assert filing["as_of_date"] == "2025-02-01"
    assert filing["url"] == FILING_URL
    assert "10-Q" in filing["title"]
    assert "2024-12-28" in filing["content"]
    assert "metadata" in filing["content"].lower()
    assert "not exhaustive" in filing["content"].lower()
    assert "2025-02-04" not in json.dumps(records)
    for _, kwargs in calls:
        assert kwargs["headers"]["User-Agent"] == "Research Team analyst@example.org"
        assert 0 < kwargs["timeout"] <= 30


def test_facts_exclude_later_restatements_and_future_periods(sec, configured, monkeypatch):
    _mock_http(
        monkeypatch,
        sec,
        {
            TICKERS_URL: TICKERS,
            SUBMISSIONS_URL: _submissions([_filing()]),
            FACTS_URL: _facts(
                [
                    _fact(),
                    _fact(val=999000000000, filed="2025-04-30", accn="0000320193-25-000030"),
                    _fact(val=888000000000, end="2025-03-31"),
                    _fact(val=777000000000, filed=""),
                ]
            ),
        },
    )

    records = sec.get_company_filings("AAPL", "2025-02-01")
    facts = [record for record in records if record["category"] == "fundamentals"]

    assert len(facts) == 1
    fact = facts[0]
    assert fact["status"] == "available"
    assert fact["coverage"] == "reported_fact"
    assert fact["published_at"] == "2025-01-31"
    assert fact["url"] == FILING_URL
    assert "124300000000" in fact["content"]
    for required in ("Revenue", "USD", "2024-09-29", "2024-12-28", "2025", "Q1", "2025-01-31"):
        assert required in fact["content"]
    assert all(
        str(value) not in json.dumps(records)
        for value in (999000000000, 888000000000, 777000000000)
    )


def test_latest_period_and_shortest_duration_are_explicit(sec, configured, monkeypatch):
    _mock_http(
        monkeypatch,
        sec,
        {
            TICKERS_URL: TICKERS,
            SUBMISSIONS_URL: _submissions([_filing()]),
            FACTS_URL: _facts(
                [
                    _fact(val=100, start="2024-01-01", end="2024-06-30"),
                    _fact(val=40, start="2024-04-01", end="2024-06-30"),
                    _fact(val=90, start="2023-01-01", end="2023-12-31"),
                ]
            ),
        },
    )

    facts = [
        record
        for record in sec.get_company_filings("AAPL", "2025-02-01")
        if record["category"] == "fundamentals"
    ]

    assert "Value: 40 USD" in facts[0]["content"]
    assert "2024-04-01 to 2024-06-30" in facts[0]["content"]


def test_symbol_alias_is_exact_and_never_strips_an_exchange(sec, configured, monkeypatch):
    tickers = {"0": {"cik_str": 320193, "ticker": "BRK-B", "title": "Example issuer"}}
    calls = _mock_http(
        monkeypatch,
        sec,
        {
            TICKERS_URL: tickers,
            SUBMISSIONS_URL: _submissions([_filing()]),
            FACTS_URL: _facts(),
        },
    )

    alias = sec.get_company_filings("BRK.B", "2025-02-01")
    unsupported = sec.get_company_filings("BRK.L", "2025-02-01")

    assert any(record["status"] == "available" for record in alias)
    assert all(record["status"] == "unavailable" for record in unsupported)
    assert len(calls) == 4


def test_historical_filing_search_reads_bounded_archive(sec, configured, monkeypatch):
    archive_url = "https://data.sec.gov/submissions/CIK0000320193-submissions-001.json"
    _mock_http(
        monkeypatch,
        sec,
        {
            TICKERS_URL: TICKERS,
            SUBMISSIONS_URL: _submissions(
                [_filing()],
                files=[
                    {
                        "name": "CIK0000320193-submissions-001.json",
                        "filingCount": 1000,
                        "filingFrom": "2019-01-01",
                        "filingTo": "2024-01-31",
                    }
                ],
            ),
            archive_url: _columns([_filing(filingDate="2020-01-30", reportDate="2019-12-28")]),
            FACTS_URL: _facts(),
        },
    )

    records = sec.get_company_filings("AAPL", "2020-02-01")

    assert records[0]["status"] == "available"
    assert records[0]["published_at"] == "2020-01-30"
    assert "not exhaustive" in records[0]["content"].lower()


def test_sec_failures_are_sanitized_and_preserve_other_evidence(sec, configured, monkeypatch):
    _mock_http(
        monkeypatch,
        sec,
        {
            TICKERS_URL: TICKERS,
            SUBMISSIONS_URL: _submissions([_filing()]),
            FACTS_URL: requests.RequestException("secret token=do-not-print analyst@example.org"),
        },
    )

    records = sec.get_company_filings("AAPL", "2025-02-01")

    assert records[0]["status"] == "available"
    assert records[-1]["category"] == "fundamentals"
    assert records[-1]["status"] == "unavailable"
    assert "do-not-print" not in json.dumps(records)
    assert "analyst@example.org" not in json.dumps(records)


@pytest.mark.parametrize(
    "payload", [[], {"facts": []}, {"facts": {"us-gaap": {"Revenues": {"units": []}}}}]
)
def test_malformed_sec_payload_is_unavailable(sec, configured, monkeypatch, payload):
    _mock_http(
        monkeypatch,
        sec,
        {
            TICKERS_URL: TICKERS,
            SUBMISSIONS_URL: _submissions(),
            FACTS_URL: payload,
        },
    )

    records = sec.get_company_filings("AAPL", "2025-02-01")

    assert records
    assert all(record["status"] == "unavailable" for record in records)


PENG_CIK = "0001616533"
PENG_EARNINGS_ACCESSION = "0001616533-26-000046"
PENG_CFO_ACCESSION = "0001193125-26-415645"
PENG_FIXTURES = Path(__file__).parent / "fixtures" / "sec"


def _peng_rows():
    return [
        _filing(
            form="10-K", accessionNumber="0001616533-25-000061",
            filingDate="2025-10-24", reportDate="2025-08-29",
            primaryDocument="peng-20250829.htm",
        ),
        _filing(
            form="10-Q", accessionNumber="0001616533-26-000043",
            filingDate="2026-07-07", reportDate="2026-05-29",
            primaryDocument="peng-20260529.htm",
        ),
        _filing(
            form="8-K", accessionNumber=PENG_EARNINGS_ACCESSION,
            filingDate="2026-10-06", reportDate="2026-10-06",
            primaryDocument="peng-20261006.htm",
        ),
        _filing(
            form="8-K", accessionNumber=PENG_CFO_ACCESSION,
            filingDate="2026-10-06", reportDate="2026-10-06",
            primaryDocument="d162527d8k.htm",
        ),
        _filing(
            form="8-K", accessionNumber="0001616533-26-000040",
            filingDate="2026-07-17", reportDate="2026-07-13",
            primaryDocument="older8k.htm",
        ),
    ]


def _install_peng_sources(sec, monkeypatch):
    monkeypatch.setattr(sec, "_resolve_cik", lambda *args: (PENG_CIK, "PENG"))

    def request_json(url, user_agent):
        if "/submissions/" in url:
            return _submissions(_peng_rows())
        if "companyfacts" in url:
            return _facts([_fact()])
        return TICKERS

    def fetch_html(url, **kwargs):
        filename = (
            "peng_q4_ex991.html" if "ex991" in url else
            "peng_cfo_8k.html" if "d162527d8k" in url else
            "peng_q4_8k.html" if "peng-20261006" in url else
            "peng_2025_10k.html"
        )
        return (PENG_FIXTURES / filename).read_text(encoding="utf-8")

    monkeypatch.setattr(sec, "_request_json", request_json)
    monkeypatch.setattr(sec, "fetch_archive_html", fetch_html, raising=False)


def test_recent_8ks_do_not_hide_annual_or_quarterly_filing(sec, configured):
    selected, _ = sec._collect_filing_rows(
        _submissions(_peng_rows()), PENG_CIK, date(2026, 10, 9),
        "Research Team analyst@example.org",
    )

    assert {row["form"] for row in selected} >= {"10-K", "10-Q", "8-K"}
    assert {row["accessionNumber"] for row in selected} >= {
        PENG_EARNINGS_ACCESSION, PENG_CFO_ACCESSION,
    }


def test_archive_search_still_finds_annual_after_three_recent_8ks(
    sec, configured, monkeypatch
):
    current = [row for row in _peng_rows() if row["form"] == "8-K"]
    annual = next(row for row in _peng_rows() if row["form"] == "10-K")
    payload = _submissions(
        current,
        files=[{
            "name": "CIK0001616533-submissions-001.json",
            "filingCount": 1000,
            "filingFrom": "2024-01-01",
            "filingTo": "2025-12-31",
        }],
    )
    monkeypatch.setattr(sec, "_request_json", lambda *args: _columns([annual]))

    selected, _ = sec._collect_filing_rows(
        payload, PENG_CIK, date(2026, 10, 9), "Research Team analyst@example.org"
    )

    assert any(row["form"] == "10-K" for row in selected)


def test_peng_october_9_includes_q4_exhibit_business_and_cfo(
    sec, configured, monkeypatch
):
    _install_peng_sources(sec, monkeypatch)

    records = sec.get_company_filings("PENG", "2026-10-09")

    q4_tables = [
        row for row in records
        if row.get("document_type") == "EX-99.1"
        and "566,685" in str(row.get("table_rows"))
    ]
    assert q4_tables
    assert q4_tables[0]["report_period"] == "2026-08-28"
    assert any(row.get("form") == "10-K" and "OriginAI" in row["content"] for row in records)
    assert any(row.get("form") == "8-K" and "Stephen Cumming" in row["content"] for row in records)


def test_october_5_cutoff_excludes_october_6_exhibits(sec, configured, monkeypatch):
    _install_peng_sources(sec, monkeypatch)

    records = sec.get_company_filings("PENG", "2026-10-05")

    assert not any("566,685" in str(row) for row in records)
    assert not any("Stephen Cumming" in row["content"] for row in records)


def test_oversize_document_preserves_xbrl_and_marks_document_unavailable(
    sec, configured, monkeypatch
):
    _install_peng_sources(sec, monkeypatch)
    monkeypatch.setattr(
        sec, "fetch_archive_html",
        lambda *args, **kwargs: (_ for _ in ()).throw(SecDocumentError("oversize")),
        raising=False,
    )

    records = sec.get_company_filings("PENG", "2026-10-09")

    assert any(row["category"] == "fundamentals" and row["status"] == "available" for row in records)
    assert any(row["category"] == "document" and row["status"] == "unavailable" for row in records)


def test_current_report_flood_reserves_document_coverage_for_annual_and_quarterly(
    sec, configured, monkeypatch
):
    _install_peng_sources(sec, monkeypatch)
    monkeypatch.setattr(
        sec, "discover_exhibits",
        lambda html, url, cik, accession: [url.replace(".htm", "-ex991.htm")]
        if "8-K" in html else [],
    )
    monkeypatch.setattr(
        sec, "extract_sections",
        lambda html, form: [SecSection(f"Section {index}", "Substantive document text")
                            for index in range(6 if form in {"8-K", "EX-99.1"} else 1)],
    )

    records = sec.get_company_filings("PENG", "2026-10-09")
    documents = [row for row in records if row.get("section")]

    assert any(row.get("document_type") == "10-K" for row in documents)
    assert any(row.get("document_type") == "10-Q" for row in documents)
    assert any(row.get("document_type") == "8-K" for row in documents)


def test_empty_section_extraction_marks_document_unavailable_without_losing_facts(
    sec, configured, monkeypatch
):
    _install_peng_sources(sec, monkeypatch)
    monkeypatch.setattr(sec, "extract_sections", lambda html, form: [])

    records = sec.get_company_filings("PENG", "2026-10-09")

    assert any(row["category"] == "fundamentals" and row["status"] == "available"
               for row in records)
    assert any(row.get("document_type") == "10-K" and row["status"] == "unavailable"
               for row in records)
