"""SEC archive documents are fetched only from a bounded, filing-owned URL."""

from __future__ import annotations

import importlib
from io import BytesIO
from pathlib import Path

import pytest
import requests

pytestmark = pytest.mark.unit

ACCESSION = "0001616533-26-000046"
VALID_URL = (
    "https://www.sec.gov/Archives/edgar/data/1616533/"
    "000161653326000046/pengq4-26form8xkxex991.htm"
)
FILING_URL = (
    "https://www.sec.gov/Archives/edgar/data/1616533/"
    "000161653326000046/peng-20261006.htm"
)
FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sec"


@pytest.fixture
def sec_documents():
    return importlib.import_module("tradingagents.dataflows.sec_documents")


def _response(status: int, body: bytes, content_type: str = "text/html") -> requests.Response:
    result = requests.Response()
    result.status_code = status
    result.raw = BytesIO(body)
    result.url = VALID_URL
    result.headers["Content-Type"] = content_type
    return result


@pytest.mark.parametrize(
    "url",
    [
        "http://www.sec.gov/Archives/edgar/data/1616533/000161653326000046/ex991.htm",
        "https://www.sec.gov/Archives/edgar/data/1616533/other/ex991.htm",
        "https://www.sec.gov/Archives/edgar/data/1616533/000161653326000046/%2e%2e/x",
        "https://evil.example/Archives/edgar/data/1616533/000161653326000046/ex991.htm",
        "https://www.sec.gov/Archives/edgar/data/1616533/000161653326000046/ex991.htm?next=1",
    ],
)
def test_archive_url_must_stay_in_selected_accession(url, sec_documents):
    with pytest.raises(ValueError, match="untrusted SEC archive URL"):
        sec_documents.validate_archive_url(url, "1616533", ACCESSION)


def test_archive_url_accepts_selected_document(sec_documents):
    assert sec_documents.validate_archive_url(VALID_URL, "1616533", ACCESSION) == VALID_URL


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (_response(302, b"redirect"), "redirect"),
        (_response(200, b"x" * 8_000_001), "size limit"),
        (_response(200, b"%PDF", "application/pdf"), "not HTML"),
        (_response(200, b""), "empty"),
    ],
)
def test_failed_archive_response_is_not_cached(tmp_path, monkeypatch, response, reason, sec_documents):
    monkeypatch.setattr(sec_documents.requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(sec_documents.SecDocumentError, match=reason):
        sec_documents.fetch_archive_html(
            VALID_URL,
            cik="1616533",
            accession=ACCESSION,
            user_agent="Research analyst@example.org",
            cache_dir=tmp_path,
        )

    assert not list(tmp_path.rglob("*.html"))


def test_successful_archive_fetch_is_cached_without_second_request(tmp_path, monkeypatch, sec_documents):
    calls = []

    def first_get(url, **kwargs):
        calls.append((url, kwargs))
        return _response(200, b"<html><body>Q4 results</body></html>")

    monkeypatch.setattr(sec_documents.requests, "get", first_get)
    first = sec_documents.fetch_archive_html(
        VALID_URL, cik="1616533", accession=ACCESSION,
        user_agent="Research analyst@example.org", cache_dir=tmp_path,
    )
    monkeypatch.setattr(
        sec_documents.requests, "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("cache missed")),
    )
    second = sec_documents.fetch_archive_html(
        VALID_URL, cik="1616533", accession=ACCESSION,
        user_agent="Research analyst@example.org", cache_dir=tmp_path,
    )

    assert first == second == "<html><body>Q4 results</body></html>"
    assert len(calls) == 1
    assert calls[0][1]["allow_redirects"] is False
    assert calls[0][1]["stream"] is True


def test_uncached_sec_requests_are_paced(monkeypatch, sec_documents):
    now = [10.0]
    sleeps = []
    monkeypatch.setattr(sec_documents, "_last_sec_request_at", 9.9)
    monkeypatch.setattr(sec_documents, "monotonic", lambda: now[0])

    def fake_sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(sec_documents, "sleep", fake_sleep)

    sec_documents._pace_sec_request()

    assert sleeps == [pytest.approx(0.1)]
    assert sec_documents._last_sec_request_at == pytest.approx(10.1)


def test_transport_error_is_sanitized_and_not_cached(tmp_path, monkeypatch, sec_documents):
    def timeout(*args, **kwargs):
        raise requests.Timeout("Research analyst@example.org private diagnostic")

    monkeypatch.setattr(sec_documents.requests, "get", timeout)

    with pytest.raises(sec_documents.SecDocumentError) as caught:
        sec_documents.fetch_archive_html(
            VALID_URL, cik="1616533", accession=ACCESSION,
            user_agent="Research analyst@example.org", cache_dir=tmp_path,
        )

    assert "analyst@example.org" not in str(caught.value)
    assert not list(tmp_path.rglob("*.html"))


def test_peng_8k_discovers_only_its_own_99_1_exhibit(sec_documents):
    html = (FIXTURE_DIR / "peng_q4_8k.html").read_text(encoding="utf-8")

    assert sec_documents.discover_exhibits(html, FILING_URL, "1616533", ACCESSION) == [
        VALID_URL
    ]


def test_peng_exhibit_keeps_quarterly_table_and_guidance_but_not_script(sec_documents):
    html = (FIXTURE_DIR / "peng_q4_ex991.html").read_text(encoding="utf-8")

    sections = sec_documents.extract_sections(html, "EX-99.1")

    assert any("566,685" in str(section.table_rows) for section in sections)
    assert any("40%" in section.content for section in sections)
    assert all("ignore previous" not in section.content for section in sections)
    assert all(len(section.content) <= 4000 for section in sections)


def test_outlook_prefers_issued_guidance_over_later_legal_reference(sec_documents):
    issued = (
        "As of October 6, management expects fiscal-year net-sales growth of "
        "approximately 40%, plus or minus ten percentage points. "
        "Diluted EPS guidance is $3.50 plus or minus $0.70. "
    )
    legal = "Forward-looking statements have many risks and uncertainties. " * 200
    html = (
        f"<body><h2>Business Outlook</h2><p>{issued * 3}</p>"
        "<h2>Use of Forward-Looking Statements</h2>"
        f"<p>Business Outlook {legal}</p></body>"
    )

    sections = sec_documents.extract_sections(html, "EX-99.1")

    assert any(section.name == "Outlook" and "40%" in section.content
               for section in sections)
    assert all("Forward-looking statements have many risks" not in section.content
               for section in sections if section.name == "Outlook")


def test_annual_business_and_competition_are_named_bounded_sections(sec_documents):
    html = (FIXTURE_DIR / "peng_2025_10k.html").read_text(encoding="utf-8")

    sections = sec_documents.extract_sections(html, "10-K")

    assert any(section.name == "Business" and "OriginAI" in section.content for section in sections)
    assert any(
        section.name == "Competition" and "purchasing power" in section.content
        for section in sections
    )
    assert all(len(section.content) <= 4000 for section in sections)


def test_annual_parser_prefers_actual_business_section_and_retains_later_products(sec_documents):
    overview = (
        "Overview Penguin supplies advanced computing systems and integrated memory "
        "for enterprise infrastructure customers. "
    )
    business = overview + ("Market demand for computing infrastructure varies. " * 125)
    business += (
        " Our solutions include OriginAI for AI factories and ClusterWareAI "
        "software for managing computing clusters. "
        "Competition from larger suppliers affects pricing and customer wins. "
        "Additional competitive pressure can reduce margins."
    )
    html = (
        "<body><p>Item 1. Business Item 1A. Risk Factors</p>"
        f"<h2>Item 1. Business</h2><p>{business}</p><h2>Item 1A. Risk Factors</h2>"
        "<p>Item 1. Business</p>"
        + ("Later unrelated financial discussion. " * 450)
        + "<p>Item 1A. Reference</p></body>"
    )

    sections = sec_documents.extract_sections(html, "10-K")

    assert any(section.name == "Business" and section.content.startswith("Overview Penguin")
               for section in sections)
    assert any(section.name == "Products" and "OriginAI" in section.content
               for section in sections)


def test_cfo_filing_has_a_dated_officer_section(sec_documents):
    html = (FIXTURE_DIR / "peng_cfo_8k.html").read_text(encoding="utf-8")

    sections = sec_documents.extract_sections(html, "8-K")

    assert any(
        section.name == "Officer appointment" and "Stephen Cumming" in section.content
        for section in sections
    )


def test_missing_section_headings_do_not_create_source_content(sec_documents):
    assert sec_documents.extract_sections("<html><body>Unrelated text</body></html>", "10-K") == []


def test_financial_table_rows_and_cells_are_bounded(sec_documents):
    rows = "".join(
        f"<tr><td>Revenue {index}</td><td>{'x' * 400}</td></tr>"
        for index in range(150)
    )
    html = f"<html><body><table>{rows}</table></body></html>"

    sections = sec_documents.extract_sections(html, "EX-99.1")

    assert len(sections) == 1
    assert len(sections[0].table_rows) == 120
    assert max(len(cell) for row in sections[0].table_rows for cell in row) <= 300


def test_table_context_excludes_previous_table_body_and_keeps_caption(sec_documents):
    html = '''<html><body><p>Prior non-GAAP reconciliation (in millions)</p><table><tr><td>Revenue</td><td>SECRET PRIOR ROW</td></tr></table>
    <p>CONSOLIDATED STATEMENTS OF OPERATIONS</p><p>(in thousands)</p>
    <table><tr><td></td><td colspan="2">Three Months Ended September 30,</td></tr>
    <tr><td></td><td>2026</td><td>2025</td></tr>
    <tr><td>Total revenue</td><td>210809</td><td>135714</td></tr></table></body></html>'''
    table = sec_documents.extract_sections(html, 'EX-99.1')[1]
    assert 'SECRET PRIOR ROW' not in table.content
    assert 'Prior non-GAAP' not in table.content
    assert 'in thousands' in table.content
    assert 'STATEMENTS OF OPERATIONS' in table.content
    assert table.table_rows[0] == ('', 'Three Months Ended September 30,', 'Three Months Ended September 30,')


def test_financial_tables_keep_complete_wide_rows_and_skip_nested_containers(sec_documents):
    cells = ''.join(f'<td>{i}</td>' for i in range(30))
    html = f'<html><body><table><tr><td><table><tr><td>Total revenue</td>{cells}</tr></table></td></tr></table></body></html>'
    sections = sec_documents.extract_sections(html, 'EX-99.1')
    assert len(sections) == 1
    assert len(sections[0].table_rows[0]) == 31
    assert sections[0].table_rows[0][-1] == '29'


@pytest.mark.parametrize('form', ['S-1', 'F-1', '424B4'])
def test_registration_business_competition_and_counterparties_keep_heading_boundaries(sec_documents, form):
    html = '''<html><body><h1>BUSINESS</h1><p>We provide specialist compute systems and cloud services to enterprise customers. Our products support training and inference workloads. We sell hardware and usage-based cloud access.</p>
    <h2>Competition</h2><p>We compete directly with Alpha Systems and Beta Labs for enterprise inference workloads. Their alternative products serve similar buyers and compete on performance and total ownership cost.</p>
    <h2>Customers</h2><p>Our customers include Orion Cloud under a multi-year supply agreement. Orion Cloud purchases systems for its commercial infrastructure and remains subject to the terms of that agreement.</p>
    <h2>Suppliers</h2><p>Our suppliers include Delta Foundry, which provides advanced manufacturing services. Capacity availability and delivery terms are important to our planned production and inventory management.</p>
    <h1>MANAGEMENT</h1><p>UNRELATED MANAGEMENT SECRET should not become customer or supplier evidence.</p></body></html>'''
    sections = {section.name: section.content for section in sec_documents.extract_sections(html, form)}
    assert 'specialist compute' in sections['Business']
    assert 'Alpha Systems and Beta Labs' in sections['Competition']
    assert 'Orion Cloud' in sections['Customers']
    assert 'Delta Foundry' in sections['Suppliers']
    assert all('MANAGEMENT SECRET' not in text for text in sections.values())


def test_registration_styled_paragraph_heading_stops_customer_section(sec_documents):
    body = 'Orion Cloud purchases our systems under a disclosed commercial agreement. Delivery terms and future orders depend on that agreement. '
    html = f'<html><body><p><b>Customers</b></p><p>{body}</p><p><b>Management’s Discussion and Analysis</b></p><p>UNRELATED MANAGEMENT DETAILS must stay outside customer evidence.</p></body></html>'
    sections = {section.name: section.content for section in sec_documents.extract_sections(html, 'S-1')}
    assert 'Orion Cloud' in sections['Customers']
    assert 'MANAGEMENT DETAILS' not in sections['Customers']
