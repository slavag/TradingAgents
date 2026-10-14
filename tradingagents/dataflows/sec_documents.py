"""Bounded, filing-owned SEC archive documents."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Lock
from time import monotonic, sleep
from urllib.parse import urljoin, urlsplit

import requests
from parsel import Selector

_ACCESSION = re.compile(r"\d{10}-\d{2}-\d{6}")
_CIK = re.compile(r"\d+")
MAX_SEC_HTML_BYTES = 8_000_000
MIN_SEC_REQUEST_INTERVAL = 0.2
_sec_request_lock = Lock()
_last_sec_request_at = 0.0


class SecDocumentError(ValueError):
    """An optional SEC document could not be safely retrieved."""


@dataclass(frozen=True)
class SecSection:
    """One bounded, attributable section or table of a selected SEC document."""

    name: str
    content: str
    table_rows: tuple[tuple[str, ...], ...] = ()


def _pace_sec_request() -> None:
    global _last_sec_request_at
    with _sec_request_lock:
        wait = MIN_SEC_REQUEST_INTERVAL - (monotonic() - _last_sec_request_at)
        if wait > 0:
            sleep(wait)
        _last_sec_request_at = monotonic()


def validate_archive_url(url: str, cik: str, accession: str) -> str:
    """Accept only an HTTPS HTML document inside the selected SEC accession."""
    if not _CIK.fullmatch(cik) or not _ACCESSION.fullmatch(accession):
        raise ValueError("untrusted SEC archive URL")
    parsed = urlsplit(url)
    prefix = f"/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/"
    if (
        parsed.scheme != "https"
        or parsed.netloc != "www.sec.gov"
        or not parsed.path.startswith(prefix)
        or "%" in parsed.path
        or ".." in parsed.path
        or parsed.query
        or parsed.fragment
        or not parsed.path.lower().endswith((".htm", ".html"))
    ):
        raise ValueError("untrusted SEC archive URL")
    return url


def fetch_archive_html(
    url: str,
    *,
    cik: str,
    accession: str,
    user_agent: str,
    cache_dir: Path,
) -> str:
    """Fetch a selected HTML document once, with bounded size and safe caching."""
    validate_archive_url(url, cik, accession)
    key = hashlib.sha256(url.encode()).hexdigest()
    target = cache_dir / "sec-documents" / f"{key}.html"
    if target.is_file():
        if target.stat().st_size > MAX_SEC_HTML_BYTES:
            raise SecDocumentError("cached SEC archive document exceeds size limit")
        return target.read_text(encoding="utf-8")

    _pace_sec_request()
    try:
        with requests.get(
            url,
            headers={"User-Agent": user_agent, "Accept": "text/html"},
            timeout=20,
            stream=True,
            allow_redirects=False,
        ) as response:
            if 300 <= response.status_code < 400:
                raise SecDocumentError("SEC archive redirect rejected")
            response.raise_for_status()
            if "html" not in response.headers.get("Content-Type", "").lower():
                raise SecDocumentError("SEC archive document is not HTML")
            chunks: list[bytes] = []
            size = 0
            for chunk in response.iter_content(65536):
                size += len(chunk)
                if size > MAX_SEC_HTML_BYTES:
                    raise SecDocumentError("SEC archive document exceeds size limit")
                chunks.append(chunk)
    except requests.RequestException:
        raise SecDocumentError("SEC archive request failed") from None

    document = b"".join(chunks).decode("utf-8", errors="replace")
    if not document.strip():
        raise SecDocumentError("SEC archive document is empty")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(
            "w", encoding="utf-8", dir=target.parent,
            prefix=key, suffix=".part", delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(document)
        temporary.replace(target)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return document


def discover_exhibits(
    html: str, filing_url: str, cik: str, accession: str
) -> list[str]:
    """Return at most two Exhibit 99.1 links within the filing's accession."""
    selector = Selector(text=html, type="html")
    found: list[str] = []
    for href in selector.xpath("//tr[contains(., '99.1')]//a/@href").getall():
        candidate = urljoin(filing_url, href)
        try:
            found.append(validate_archive_url(candidate, cik, accession))
        except ValueError:
            continue
    return list(dict.fromkeys(found))[:2]


def _named_bounded_sections(form: str, text: str) -> list[SecSection]:
    if form in {"10-K", "20-F", "40-F"}:
        markers = [
            ("Business", r"\bItem\s+1\.?\s+Business\b", r"\bItem\s+1A\.?\b")
        ]
    elif form == "10-Q":
        markers = [
            (
                "Management discussion",
                r"\bItem\s+2\.?\s+Management",
                r"\bItem\s+3\.?\b",
            )
        ]
    elif form in {"8-K", "6-K"}:
        markers = [
            (
                "Results of operations",
                r"\bItem\s+2\.02\b",
                r"\bItem\s+5\.02\b|\bItem\s+9\.01\b",
            ),
            (
                "Officer appointment",
                r"\bItem\s+5\.02\b",
                r"\bItem\s+7\.01\b|\bItem\s+9\.01\b",
            ),
        ]
    elif form == "EX-99.1":
        markers = [
            (
                "Quarterly highlights",
                r"\bFourth Quarter Financial Highlights\b",
                r"\bRecent Business Highlights\b",
            ),
            (
                "Business highlights",
                r"\bRecent Business Highlights\b",
                r"\bFiscal 2026 Highlights\b",
            ),
            (
                "Outlook",
                r"\bBusiness Outlook\b",
                r"\bUse of Forward-Looking Statements\b",
            ),
        ]
    else:
        return []

    sections: list[SecSection] = []
    full_spans: dict[str, str] = {}
    for name, start_pattern, end_pattern in markers:
        candidates = []
        for match in re.finditer(start_pattern, text, re.I):
            tail = text[match.end() :]
            end = re.search(end_pattern, tail, re.I)
            span = tail[: end.start()] if end else tail
            if len(span) >= 200:
                candidates.append(span.strip())
        if candidates:
            span = (
                min(candidates, key=len)
                if form in {"10-K", "20-F", "40-F"} or name == "Outlook"
                else candidates[-1]
            )
            full_spans[name] = span
            sections.append(SecSection(name, span[:4000]))
    if form in {"10-K", "20-F", "40-F"} and (business := full_spans.get("Business")):
        product_match = (
            re.search(r"\bOur (?:solutions|products) include\b", business, re.I)
            or re.search(r"\bProduct Offerings\b", business, re.I)
            or re.search(r"\bProducts and Services\b", business, re.I)
        )
        if product_match:
            sections.append(SecSection("Products", business[product_match.start():][:4000]))
        for match in reversed(list(re.finditer(r"\bCompetition\b", business, re.I))):
            span = business[match.start() :]
            if len(span) >= 200:
                sections.append(SecSection("Competition", span[:4000]))
                break
    return sections


def extract_sections(html: str, form: str) -> list[SecSection]:
    """Extract short filing narratives and source-preserving financial tables."""
    selector = Selector(text=html, type="html")
    for node in selector.xpath("//script|//style|//noscript"):
        node.root.drop_tree()
    text = " ".join(" ".join(selector.xpath("//body//text()").getall()).split())
    sections = _named_bounded_sections(form, text)
    for index, table in enumerate(selector.xpath("//table")[:12], 1):
        rows = tuple(
            tuple(
                " ".join(" ".join(cell.xpath(".//text()").getall()).split())[:300]
                for cell in tr.xpath("./th|./td")[:20]
            )
            for tr in table.xpath(".//tr")[:120]
        )
        if any(
            re.search(r"net sales|revenue|operating income|cash flow", " ".join(row), re.I)
            for row in rows
        ):
            preceding = " ".join(
                " ".join(table.xpath("preceding::text()").getall()[-30:]).split()
            )
            sections.append(SecSection(f"Table {index}", preceding[-1000:], rows))
        if len(sections) >= 6:
            break
    return sections[:6]
