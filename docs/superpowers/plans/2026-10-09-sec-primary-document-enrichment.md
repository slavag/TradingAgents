# SEC Primary-Document Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every eligible stock analysis dated, bounded SEC business and current-report evidence before the analyst teams run, including earnings exhibits such as PENG's October 6, 2026 Exhibit 99.1.

**Architecture:** Keep `get_company_filings(ticker, curr_date)` as the vendor entry point. Add a safe SEC archive transport/parser, select one annual and one quarterly filing alongside recent current reports, and emit short document-section records through the existing `shared_evidence` normalization and prompt path. Existing XBRL facts and optional-provider failure behavior remain intact.

**Tech Stack:** Python 3.13, `requests`, existing `parsel` dependency, `pytest`, `ruff`.

**Spec:** `docs/superpowers/specs/2026-10-09-source-grounded-investor-deck-design.md`

## Global Constraints

- A source published after the requested `YYYY-MM-DD` analysis date is excluded; unknown publication timing cannot support a historical claim.
- Fetch only validated `https://www.sec.gov/Archives/edgar/data/` paths derived from a selected filing; do not follow redirects outside the boundary.
- Retain the `SEC_USER_AGENT` requirement, exact ticker-to-CIK mapping, current XBRL facts, and independent failure handling.
- Limit requests, bytes, excerpts, table rows, and prompt characters; never execute or render filing HTML.
- Keep old saved reports immutable and do not run SMXT during verification.
- Do not commit, push, or open a PR without Slava's explicit per-action approval. Commit steps below are approval-gated checkpoints, not standing authorization.

## Review Focus

1. A future-dated 8-K or amended annual report must not leak into a historical analysis. Task 3 tests a cutoff with newer and older filings.
2. An archive URL with a different accession, encoded traversal, HTTP scheme, or redirect must not be fetched. Task 1 tests each case.
3. An oversized or malformed SEC response must become an unavailable document record while other sources survive. Tasks 1 and 3 test this path.
4. A flood of indicator/tool records must not crowd the latest earnings exhibit and annual business record out of analyst prompts. Task 4 tests this with 39 market records.
5. A non-U.S. or unsupported issuer with no eligible SEC filing must retain its normal analysis/full report without fabricated business claims. Task 5 tests the empty-source path.

---

## File Structure

- `tradingagents/dataflows/sec_documents.py` owns SEC archive URL validation, bounded/cached HTML transport, exhibit discovery, text section extraction, and table-row extraction. It never resolves tickers or chooses filings.
- `tradingagents/dataflows/sec.py` owns CIK lookup, as-of filing selection, and converting extracted sections into collector-neutral records. Keep the public `get_company_filings` signature.
- `tradingagents/agents/utils/evidence.py` owns normalization and analyst prompt ordering of the new source metadata. Do not put SEC HTTP logic here.
- `tests/test_sec_documents.py` tests transport and parsing; `tests/test_sec_evidence.py` tests selection and record production; `tests/test_shared_evidence.py` tests normalized handoff and prompt visibility.
- `tests/fixtures/sec/` contains short, public, representative HTML fragments for the PENG 8-K, Exhibit 99.1, CFO 8-K, and FY2025 10-K. Do not vendor complete SEC filings.
- `docs/data-enrichment.md` records configuration, form coverage, bounds, and failure semantics.

### Task 1: Safe SEC Archive Transport

**Files:**
- Create: `tradingagents/dataflows/sec_documents.py`
- Create: `tests/test_sec_documents.py`

**Interfaces:**
- Consumes: a filing-derived `cik`, accession number, archive URL, `SEC_USER_AGENT`, and `Path(get_config()["data_cache_dir"])`.
- Produces: `validate_archive_url(url: str, cik: str, accession: str) -> str` and `fetch_archive_html(url: str, *, cik: str, accession: str, user_agent: str, cache_dir: Path) -> str`.

- [ ] **Step 1: Write failing URL and response-boundary tests.**

```python
ACCESSION = "0001616533-26-000046"
VALID_URL = ("https://www.sec.gov/Archives/edgar/data/1616533/"
             "000161653326000046/pengq4-26form8xkxex991.htm")

def response(status: int, body: bytes) -> requests.Response:
    result = requests.Response()
    result.status_code = status
    result._content = body
    result.url = VALID_URL
    result.headers["Content-Type"] = "text/html; charset=utf-8"
    return result

@pytest.mark.parametrize("url", [
    "http://www.sec.gov/Archives/edgar/data/1616533/000161653326000046/ex991.htm",
    "https://www.sec.gov/Archives/edgar/data/1616533/other/ex991.htm",
    "https://www.sec.gov/Archives/edgar/data/1616533/000161653326000046/%2e%2e/x",
    "https://evil.example/Archives/edgar/data/1616533/000161653326000046/ex991.htm",
])
def test_archive_url_must_stay_in_selected_accession(url):
    with pytest.raises(ValueError):
        validate_archive_url(url, "1616533", "0001616533-26-000046")

def test_archive_redirect_and_oversize_fail_without_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(sec_documents.requests, "get", lambda *a, **k: response(302, b"x"))
    with pytest.raises(SecDocumentError):
        fetch_archive_html(VALID_URL, cik="1616533", accession=ACCESSION,
                           user_agent="Research analyst@example.org", cache_dir=tmp_path)
    monkeypatch.setattr(sec_documents.requests, "get", lambda *a, **k: response(200, b"x" * 8_000_001))
    with pytest.raises(SecDocumentError):
        fetch_archive_html(VALID_URL, cik="1616533", accession=ACCESSION,
                           user_agent="Research analyst@example.org", cache_dir=tmp_path)
    assert not list(tmp_path.rglob("*.html"))
```

Add a second test proving that a successful fetch is cached and a second call does not touch `requests.get`.
Add a pacing test that patches `monotonic`/`sleep` and proves two distinct uncached documents start at least 0.2 seconds apart without slowing cache hits.

- [ ] **Step 2: Run the tests and confirm import/behavior failures.**

```bash
pytest -q tests/test_sec_documents.py -k 'archive_url or archive_redirect or cache' --tb=short
```

Expected: failure because `sec_documents` or its public transport functions do not exist.

- [ ] **Step 3: Implement the minimal transport.**

```python
MAX_SEC_HTML_BYTES = 8_000_000
MIN_SEC_REQUEST_INTERVAL = 0.2
_sec_request_lock = Lock()
_last_sec_request_at = 0.0

class SecDocumentError(ValueError):
    pass

def _pace_sec_request() -> None:
    global _last_sec_request_at
    with _sec_request_lock:
        wait = MIN_SEC_REQUEST_INTERVAL - (monotonic() - _last_sec_request_at)
        if wait > 0:
            sleep(wait)
        _last_sec_request_at = monotonic()

def validate_archive_url(url: str, cik: str, accession: str) -> str:
    parsed = urlsplit(url)
    prefix = f"/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/"
    if (parsed.scheme != "https" or parsed.netloc != "www.sec.gov"
            or not parsed.path.startswith(prefix) or "%" in parsed.path
            or ".." in parsed.path or parsed.query or parsed.fragment):
        raise ValueError("untrusted SEC archive URL")
    return url

def fetch_archive_html(url: str, *, cik: str, accession: str,
                       user_agent: str, cache_dir: Path) -> str:
    validate_archive_url(url, cik, accession)
    key = hashlib.sha256(url.encode()).hexdigest()
    target = cache_dir / "sec-documents" / f"{key}.html"
    if target.is_file():
        return target.read_text(encoding="utf-8")
    _pace_sec_request()
    with requests.get(url, headers={"User-Agent": user_agent},
                      timeout=20, stream=True, allow_redirects=False) as response:
        if response.status_code in range(300, 400):
            raise SecDocumentError("SEC archive redirect rejected")
        response.raise_for_status()
        if "html" not in response.headers.get("Content-Type", "").lower():
            raise SecDocumentError("SEC archive document is not HTML")
        chunks, size = [], 0
        for chunk in response.iter_content(65536):
            size += len(chunk)
            if size > MAX_SEC_HTML_BYTES:
                raise SecDocumentError("SEC archive document exceeds size limit")
            chunks.append(chunk)
    document = b"".join(chunks).decode("utf-8", errors="replace")
    target.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", dir=target.parent,
                            prefix=key, suffix=".part", delete=False) as handle:
        handle.write(document)
        temporary = Path(handle.name)
    try:
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    return document
```

Import `NamedTemporaryFile`, `Lock`, `monotonic`, and `sleep` from standard-library modules; wrap `requests.RequestException` without exposing headers or the configured user agent. A cache hit must still validate its URL before reading. The process-wide lock applies to document requests; retain existing SEC JSON requests' serial behavior and avoid parallel bursts across the collector.

- [ ] **Step 4: Run focused and module tests.**

```bash
pytest -q tests/test_sec_documents.py --tb=short
ruff check tradingagents/dataflows/sec_documents.py tests/test_sec_documents.py
```

Expected: green tests and lint.

- [ ] **Step 5: Offer an approval-gated commit checkpoint.**

```bash
git add tradingagents/dataflows/sec_documents.py tests/test_sec_documents.py
git commit -m "feat: add bounded SEC archive transport"
```

Run those commands only after explicit per-action approval; otherwise leave the files uncommitted and continue.

### Task 2: Filing Text, Exhibit, and Table Extraction

**Files:**
- Modify: `tradingagents/dataflows/sec_documents.py`
- Modify: `tests/test_sec_documents.py`
- Create: `tests/fixtures/sec/peng_q4_8k.html`
- Create: `tests/fixtures/sec/peng_q4_ex991.html`
- Create: `tests/fixtures/sec/peng_cfo_8k.html`
- Create: `tests/fixtures/sec/peng_2025_10k.html`

**Interfaces:**
- Consumes: validated filing HTML and its exact archive URL from Task 1.
- Produces: `SecSection(name: str, content: str, table_rows: tuple[tuple[str, ...], ...])`, `extract_sections(html: str, form: str) -> list[SecSection]`, and `discover_exhibits(html: str, filing_url: str, cik: str, accession: str) -> list[str]`.

- [ ] **Step 1: Write failing extraction tests with short public fragments.**

```python
FIXTURE_DIR = Path(__file__).parent / "fixtures/sec"
FILING_URL = ("https://www.sec.gov/Archives/edgar/data/1616533/"
              "000161653326000046/peng-20261006.htm")
EX991_URL = VALID_URL

def test_peng_8k_discovers_only_its_99_1_exhibit():
    html = FIXTURE_DIR.joinpath("peng_q4_8k.html").read_text()
    assert discover_exhibits(html, FILING_URL, "1616533", ACCESSION) == [EX991_URL]

def test_peng_exhibit_retains_q4_table_and_guidance_without_script():
    html = FIXTURE_DIR.joinpath("peng_q4_ex991.html").read_text()
    sections = extract_sections(html, "EX-99.1")
    assert any("566,685" in str(section.table_rows) for section in sections)
    assert any("40%" in section.content for section in sections)
    assert all("ignore previous" not in section.content for section in sections)

def test_annual_business_excerpt_is_bounded_and_named():
    sections = extract_sections(FIXTURE_DIR.joinpath("peng_2025_10k.html").read_text(), "10-K")
    assert any(section.name == "Business" and "OriginAI" in section.content for section in sections)
    assert all(len(section.content) <= 4000 for section in sections)
```

The fixture embeds representative 8-K exhibit rows (`Total net sales`, `Q4-26`, `Q3-26`) and 10-K Item 1/competition headings; it is not the attached reference presentation.

- [ ] **Step 2: Run and observe the missing parser behavior.**

```bash
pytest -q tests/test_sec_documents.py -k 'peng_8k or peng_exhibit or annual_business' --tb=short
```

Expected: failure because `discover_exhibits` and `extract_sections` are absent.

- [ ] **Step 3: Implement bounded parsing with the existing `parsel` dependency.**

```python
@dataclass(frozen=True)
class SecSection:
    name: str
    content: str
    table_rows: tuple[tuple[str, ...], ...] = ()

def discover_exhibits(html: str, filing_url: str, cik: str, accession: str) -> list[str]:
    selector = Selector(text=html, type="html")
    found = []
    for href in selector.xpath("//tr[contains(., '99.1')]//a/@href").getall():
        candidate = urljoin(filing_url, href)
        try:
            found.append(validate_archive_url(candidate, cik, accession))
        except ValueError:
            continue
    return list(dict.fromkeys(found))[:2]

def extract_sections(html: str, form: str) -> list[SecSection]:
    selector = Selector(text=html, type="html")
    for node in selector.xpath("//script|//style|//noscript"):
        node.root.drop_tree()
    text = " ".join(" ".join(selector.xpath("//body//text()").getall()).split())
    sections = _named_bounded_sections(form, text)
    for index, table in enumerate(selector.xpath("//table")[:12], 1):
        rows = tuple(
            tuple(" ".join(" ".join(cell.xpath(".//text()").getall()).split())[:300]
                  for cell in tr.xpath("./th|./td")[:20])
            for tr in table.xpath(".//tr")[:120]
        )
        if any(re.search(r"net sales|revenue|operating income|cash flow", " ".join(row), re.I)
               for row in rows):
            preceding = " ".join(" ".join(table.xpath("preceding::text()").getall()[-30:]).split())
            sections.append(SecSection(f"Table {index}", preceding[-1000:], rows))
        if len(sections) == 6:
            break
    return sections[:6]

def _named_bounded_sections(form: str, text: str) -> list[SecSection]:
    markers = (
        [("Business", r"\bItem\s+1\.?\s+Business\b", r"\bItem\s+1A\.?\b")]
        if form in {"10-K", "20-F", "40-F"} else
        [("Management discussion", r"\bItem\s+2\.?\s+Management", r"\bItem\s+3\.?\b")]
        if form == "10-Q" else
        [("Results of operations", r"\bItem\s+2\.02\b", r"\bItem\s+5\.02\b|\bItem\s+9\.01\b"),
         ("Officer appointment", r"\bItem\s+5\.02\b", r"\bItem\s+7\.01\b|\bItem\s+9\.01\b")]
        if form in {"8-K", "6-K"} else
        [("Quarterly highlights", r"\bFourth Quarter Financial Highlights\b", r"\bRecent Business Highlights\b"),
         ("Business highlights", r"\bRecent Business Highlights\b", r"\bFiscal 2026 Highlights\b"),
         ("Outlook", r"\bBusiness Outlook\b", r"\bUse of Forward-Looking Statements\b")]
    )
    result, full_spans = [], {}
    for name, start_pattern, end_pattern in markers:
        for match in reversed(list(re.finditer(start_pattern, text, re.I))):
            end = re.search(end_pattern, text[match.end():], re.I)
            span = text[match.end(): match.end() + end.start()] if end else text[match.end():]
            if len(span) >= 200:  # rejects short table-of-contents entries
                full_spans[name] = span
                result.append(SecSection(name, span[:4000]))
                break
    if form in {"10-K", "20-F", "40-F"} and (business := full_spans.get("Business")):
        for match in reversed(list(re.finditer(r"\bCompetition\b", business, re.I))):
            if len(business) - match.start() >= 200:
                result.append(SecSection("Competition", business[match.start():][:4000]))
                break
    return result
```

Import `re`, `urljoin`, `dataclass`, and `Selector` in this module. The concrete patterns above are a starting contract: add alternative Item 1 / report-item headings only when a fixture demonstrates them. Return no section when a heading cannot be identified; do not invent text. Preserve at most 120 rows, 20 cells per row, and 300 characters per cell. Add a hostile-link fixture that proves an out-of-accession exhibit is ignored.

- [ ] **Step 4: Run parser tests and lint.**

```bash
pytest -q tests/test_sec_documents.py --tb=short
ruff check tradingagents/dataflows/sec_documents.py tests/test_sec_documents.py
```

Expected: all parser tests pass; the extracted body has no executable source HTML.

- [ ] **Step 5: Offer an approval-gated commit checkpoint.**

```bash
git add tradingagents/dataflows/sec_documents.py tests/test_sec_documents.py tests/fixtures/sec
git commit -m "feat: extract bounded SEC filing sections and exhibits"
```

Run only after explicit per-action approval.

### Task 3: As-Of Filing Selection and Rich SEC Records

**Files:**
- Modify: `tradingagents/dataflows/sec.py:32-41,244-312,421-516`
- Modify: `tests/test_sec_evidence.py`

**Interfaces:**
- Consumes: `fetch_archive_html`, `discover_exhibits`, and `extract_sections` from Tasks 1–2.
- Produces: the unchanged `get_company_filings(ticker: str, curr_date: str) -> list[dict[str, Any]]` contract, now with metadata plus bounded `coverage="partial"` records for annual/quarterly/current sections and `table_rows` where available.

- [ ] **Step 1: Write failing selection and failure-isolation tests.**

```python
CIK = "0001616533"
AGENT = "Research Team analyst@example.org"
EARNINGS_ACCESSION = "0001616533-26-000046"
CFO_ACCESSION = "0001193125-26-415645"

def peng_rows():
    return [
        _filing(form="10-K", accessionNumber="0001616533-25-000061",
                filingDate="2025-10-24", primaryDocument="peng-20250829.htm"),
        _filing(form="10-Q", accessionNumber="0001616533-26-000043",
                filingDate="2026-07-07", primaryDocument="peng-20260529.htm"),
        _filing(form="8-K", accessionNumber=EARNINGS_ACCESSION,
                filingDate="2026-10-06", primaryDocument="peng-20261006.htm"),
        _filing(form="8-K", accessionNumber=CFO_ACCESSION,
                filingDate="2026-10-06", primaryDocument="d162527d8k.htm"),
        _filing(form="8-K", accessionNumber="0001616533-26-000040",
                filingDate="2026-07-17", primaryDocument="older8k.htm"),
    ]

def run_with_stubbed_sec_rows(sec, cutoff, monkeypatch):
    monkeypatch.setattr(sec, "_resolve_cik", lambda *args: (CIK, "PENG"))
    monkeypatch.setattr(sec, "_request_json", lambda url, agent:
                        _submissions(peng_rows()) if "/submissions/" in url
                        else _facts([_fact()]) if "companyfacts" in url else TICKERS)
    monkeypatch.setattr(sec, "fetch_archive_html", lambda url, **kwargs:
                        (FIXTURE_DIR / ("peng_q4_ex991.html" if "ex991" in url else
                                        "peng_cfo_8k.html" if "d162527d8k" in url else
                                        "peng_q4_8k.html" if "20261006" in url else
                                        "peng_2025_10k.html")).read_text())
    return sec.get_company_filings("PENG", cutoff)

def test_recent_8ks_do_not_hide_last_annual_or_quarterly_filing(sec, configured, monkeypatch):
    selected, _ = sec._collect_filing_rows(
        _submissions(peng_rows()), CIK, date(2026, 10, 9), AGENT)
    assert {row["form"] for row in selected} >= {"10-K", "10-Q", "8-K"}
    assert {row["accessionNumber"] for row in selected} >= {EARNINGS_ACCESSION, CFO_ACCESSION}

def test_october_5_cutoff_excludes_october_6_exhibits(sec, configured, monkeypatch):
    records = run_with_stubbed_sec_rows(sec, cutoff="2026-10-05", monkeypatch=monkeypatch)
    assert not any("566,685" in row["content"] for row in records)

def test_oversize_exhibit_does_not_erase_annual_or_xbrl_evidence(sec, configured, monkeypatch):
    run_with_stubbed_sec_rows(sec, cutoff="2026-10-09", monkeypatch=monkeypatch)
    monkeypatch.setattr(sec, "fetch_archive_html", lambda *a, **k: (_ for _ in ()).throw(SecDocumentError("oversize")))
    records = sec.get_company_filings("PENG", "2026-10-09")
    assert any(row["category"] == "fundamentals" and row["status"] == "available" for row in records)
    assert any(row["category"] == "filings" and row["status"] == "unavailable" for row in records)
```

Set `FIXTURE_DIR = Path(__file__).parent / "fixtures/sec"` in `tests/test_sec_evidence.py`. The helper uses its existing `_filing`, `_submissions`, `_fact`, and `_facts` helpers; no network. Keep existing invalid-date, missing-agent, alias, restatement, and archive-bound tests.

- [ ] **Step 2: Run the tests and confirm the annual/quarterly and narrative expectations fail.**

```bash
pytest -q tests/test_sec_evidence.py -k 'recent_8ks or october_5 or oversize_exhibit' --tb=short
```

Expected: current latest-three-only selection and metadata-only output fail the new expectations.

- [ ] **Step 3: Implement per-form selection and section records.**

```python
MAX_CURRENT_REPORTS = 3
_SECTION_CATEGORY = {
    "Business": "business", "Competition": "business",
    "Management discussion": "quarterly_filing",
    "Results of operations": "earnings_release",
    "Quarterly highlights": "earnings_release",
    "Business highlights": "earnings_release",
    "Outlook": "earnings_release",
    "Officer appointment": "governance",
}

def _select_filing_rows(eligible: list[dict[str, Any]]) -> list[dict[str, Any]]:
    annual = next((r for r in eligible if r["form"] in {"10-K", "20-F", "40-F"}), None)
    quarter = next((r for r in eligible if r["form"] == "10-Q"), None)
    current = [r for r in eligible if r["form"] in {"8-K", "6-K"}][:MAX_CURRENT_REPORTS]
    return list({r["accessionNumber"]: r for r in [annual, quarter, *current] if r}.values())
```

Sort `eligible` newest-first before calling `_select_filing_rows`. Keep the bounded archive-file search, but do not stop it merely because recent 8-Ks meet the old `MAX_FILINGS` count when an eligible annual/quarterly report is absent. In `get_company_filings`, append source-section records after metadata using `_SECTION_CATEGORY` and `form`, `accession`, `document_type`, `section`, `report_period`, and bounded `table_rows`. Give annual/quarterly table sections their corresponding `business`/`quarterly_filing` category; give 8-K 99.1 financial tables `earnings_release`. Increase `_record`'s narrative cap from 2,000 to 4,000 characters per section without lifting the per-run document limit. Preserve the existing five XBRL fact records. Replace the final `records[: MAX_FILINGS + MAX_FACTS]` truncation with explicit per-type limits. Pace SEC JSON requests with the Task 1 limiter as well as HTML downloads. Use `SecDocumentError` to append one sanitized `unavailable` document record while preserving sibling records.

- [ ] **Step 4: Run SEC tests and lint.**

```bash
pytest -q tests/test_sec_documents.py tests/test_sec_evidence.py --tb=short
ruff check tradingagents/dataflows/sec.py tradingagents/dataflows/sec_documents.py tests/test_sec_evidence.py
```

Expected: source records include the PENG-style 99.1 and older annual report within the cutoff.

- [ ] **Step 5: Offer an approval-gated commit checkpoint.**

```bash
git add tradingagents/dataflows/sec.py tests/test_sec_evidence.py
git commit -m "feat: collect latest annual and earnings documents"
```

Run only after explicit per-action approval.

### Task 4: Preserve Provenance and Prioritize Decision-Useful Excerpts

**Files:**
- Modify: `tradingagents/agents/utils/evidence.py:87-123,268-342`
- Modify: `tests/test_shared_evidence.py`
- Modify: `tradingagents/default_config.py:138-140` only if the 18,000-character default cannot retain one key record per required category.

**Interfaces:**
- Consumes: SEC records from Task 3.
- Produces: normalized records retaining `form`, `accession`, `document_type`, `section`, `report_period`, and bounded `table_rows`, plus a prompt containing the most recent earnings and annual-business excerpts ahead of lower-priority market chatter.

- [ ] **Step 1: Write failing metadata and prompt-budget tests.**

```python
def test_sec_section_metadata_survives_normalization():
    row = source_record(category="earnings_release", form="8-K",
                        accession="0001616533-26-000046", document_type="EX-99.1",
                        section="Quarterly Financial Results", report_period="2026-08-28",
                        table_rows=[["Total net sales", "566,685", "478,713"]])
    normalized = normalize_evidence(row, "2026-10-09")
    assert normalized["table_rows"] == [["Total net sales", "566,685", "478,713"]]
    assert normalized["document_type"] == "EX-99.1"

def test_q4_release_and_business_survive_indicator_heavy_prompt_budget():
    state = state_with_evidence(trade_date="2026-10-09", shared_evidence=[
        *[source_record(source="get_indicators", category="market", title=f"I{i}",
                        published_at="2026-10-09", as_of_date="2026-10-09") for i in range(39)],
        source_record(category="earnings_release", title="Q4 earnings",
                      content="Q4 FY2026 net sales 566,685", published_at="2026-10-06",
                      as_of_date="2026-10-09"),
        source_record(category="business", title="Annual business",
                      content="OriginAI and Integrated Memory", published_at="2025-10-24",
                      as_of_date="2026-10-09"),
    ])
    prompt = render_shared_evidence(state, "fundamentals", max_chars=9000)
    assert "566,685" in prompt and "OriginAI" in prompt
```

Add a maliciously long `table_rows` case to prove normalization truncates rows/cells, rather than copying unbounded nested data. Keep existing evidence IDs deterministic: document URL, section-bearing title, and content already participate in the ID; add a test that two sections in one document have distinct IDs.

- [ ] **Step 2: Run the tests and confirm metadata is dropped and new topics lose priority.**

```bash
pytest -q tests/test_shared_evidence.py -k 'sec_section_metadata or q4_release' --tb=short
```

Expected: `normalize_evidence` currently drops the additional keys.

- [ ] **Step 3: Preserve safe optional fields and rank sources.**

```python
_SEC_METADATA_FIELDS = ("form", "accession", "document_type", "section", "report_period")

def _bounded_table_rows(value) -> list[list[str]]:
    if not isinstance(value, (list, tuple)):
        return []
    return [[_redact(cell)[:300] for cell in row[:20]]
            for row in value[:120] if isinstance(row, (list, tuple))]

# In normalize_evidence, after the core record is constructed:
normalized.update({key: _redact(record[key])[:120]
                   for key in _SEC_METADATA_FIELDS if record.get(key)})
if record.get("table_rows"):
    normalized["table_rows"] = _bounded_table_rows(record["table_rows"])

# In render_shared_evidence, retain verified market snapshot first, then
# earnings_release, business, fundamentals, quarterly_filing, filings,
# news, sentiment, macro, prediction_markets, and remaining market data.
```

Include `section`, period, and source URL in `_record_block` for an SEC excerpt. Keep `evidence_is_admissible` as the cutoff gate. Do not increase `evidence_prompt_max_chars` until a red budget test shows the ranked records cannot fit.

- [ ] **Step 4: Run source handoff tests and lint.**

```bash
pytest -q tests/test_shared_evidence.py tests/test_sec_evidence.py --tb=short
ruff check tradingagents/agents/utils/evidence.py tests/test_shared_evidence.py
```

Expected: analysts see dated earnings/business evidence and existing no-future-data tests remain green.

- [ ] **Step 5: Offer an approval-gated commit checkpoint.**

```bash
git add tradingagents/agents/utils/evidence.py tests/test_shared_evidence.py tradingagents/default_config.py
git commit -m "feat: prioritize dated SEC research evidence"
```

Stage `tradingagents/default_config.py` only if it actually changed. Run the commit only after explicit per-action approval.

### Task 5: PENG Source Acceptance and Operator Guidance

**Files:**
- Modify: `tests/test_sec_evidence.py`
- Modify: `tests/test_shared_evidence.py`
- Modify: `docs/data-enrichment.md`

**Interfaces:**
- Consumes: Tasks 1–4; produces a fixture-backed source corpus usable by the deck plan without live SEC or LLM calls.

- [ ] **Step 1: Write a fixture-backed acceptance test.**

```python
def test_peng_as_of_october_9_has_q4_exhibit_annual_business_and_cfo(sec, configured, monkeypatch):
    records = run_with_stubbed_sec_rows(sec, "2026-10-09", monkeypatch)
    assert any(r.get("document_type") == "EX-99.1" and "566,685" in r["content"] for r in records)
    assert any(r.get("form") == "10-K" and "OriginAI" in r["content"] for r in records)
    assert any(r.get("form") == "8-K" and "Stephen Cumming" in r["content"] for r in records)

def test_issuer_without_eligible_sec_source_still_returns_other_provider_evidence(monkeypatch):
    monkeypatch.setattr(evidence, "route_to_vendor", lambda *args: [])
    state = {"company_of_interest": "NONUS", "trade_date": "2026-10-09",
             "asset_type": "stock", "shared_evidence": [{
                 "source": "get_stock_data", "category": "market", "title": "Market history",
                 "content": "Valid historical prices", "status": "available",
                 "as_of_date": "2026-10-09", "published_at": "2026-10-09"}]}
    result = evidence.collect_shared_evidence(state)
    assert any(r["source"] != "sec" and r["status"] == "available"
               for r in result["shared_evidence"])
```

Import `tradingagents.agents.utils.evidence` as `evidence` in the test file. Reuse the Task 3 `run_with_stubbed_sec_rows` helper and its four short local HTML fixtures. It must not fetch the network or use `PENG_20261007.html` as evidence.

- [ ] **Step 2: Run and confirm the end-to-end expectation is initially red.**

```bash
pytest -q tests/test_sec_evidence.py -k 'peng_as_of_october_9 or issuer_without' --tb=short
```

Expected: failure until fixture wiring and any final collector edge cases are finished.

- [ ] **Step 3: Complete fixture wiring and document operator behavior.**

```markdown
SEC enrichment requires SEC_USER_AGENT with an organization/application and email.
For each run date it selects the latest eligible annual and quarterly filings
plus bounded current reports and relevant 99.1 exhibits. Failed HTML retrieval
is recorded as unavailable; the analyst run and full report continue. Historical
runs never admit later filings. Source records preserve exact document URLs.
```

Put that guidance in `docs/data-enrichment.md`, including document-size and cache limits and how the investor deck will use the emitted records. Do not add a PENG-specific production code path.

- [ ] **Step 4: Verify the first independently useful release.**

```bash
pytest -q tests/test_sec_documents.py tests/test_sec_evidence.py tests/test_shared_evidence.py --tb=short
pytest -q --disable-warnings --tb=short
ruff check tradingagents/dataflows/sec_documents.py tradingagents/dataflows/sec.py tradingagents/agents/utils/evidence.py tests/test_sec_documents.py tests/test_sec_evidence.py tests/test_shared_evidence.py
git diff --check
```

Expected: enrichment and the full suite pass; no SMXT analysis is run. Report every failure or skip by name.

- [ ] **Step 5: Offer an approval-gated commit checkpoint.**

```bash
git add tests/test_sec_evidence.py tests/test_shared_evidence.py docs/data-enrichment.md
git commit -m "test: prove PENG primary-source enrichment"
```

Run only after explicit per-action approval.
