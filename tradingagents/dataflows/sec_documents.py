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
REGISTRATION_FORMS = frozenset({"S-1", "F-1", "424B4", "424B3"})
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


def _registration_sections(selector: Selector) -> list[SecSection]:
    """Read bounded IPO business disclosures using actual block-heading boundaries."""
    aliases = {
        "business": "Business", "our business": "Business", "business overview": "Business",
        "competition": "Competition", "competitive landscape": "Competition",
        "our competitors": "Competition", "customers": "Customers", "our customers": "Customers",
        "suppliers": "Suppliers", "our suppliers": "Suppliers",
        "manufacturing and supply chain": "Suppliers",
    }
    boundaries = set(aliases) | {
        "management", "risk factors", "principal stockholders", "use of proceeds",
        "intellectual property", "employees", "facilities", "legal proceedings",
        "description of capital stock", "executive compensation", "capitalization",
        "sales and marketing", "regulation", "government regulation", "financial statements",
    }
    blocks = []
    for node in selector.xpath(
        "//body//*[self::p or self::h1 or self::h2 or self::h3 or self::h4 "
        "or self::h5 or self::h6 or (self::div and not(.//p or .//div or .//table))]"
        "[not(ancestor::table)]"
    ):
        text = " ".join(" ".join(node.xpath(".//text()").getall()).split())
        if text:
            emphasized = any(
                " ".join(" ".join(child.xpath(".//text()").getall()).split()) == text
                for child in node.xpath(".//b|.//strong|.//*[@style]")
                if str(child.root.tag).lower() in {"b", "strong"}
                or re.search(r"font-weight\s*:\s*(?:bold|[6-9]00)", child.attrib.get("style", ""), re.I)
            )
            styled_heading = (len(text) <= 120 and not re.search(r"[.!?;]$", text)
                              and (emphasized or bool(re.search(
                                  r"font-weight\s*:\s*(?:bold|[6-9]00)", node.attrib.get("style", ""), re.I))))
            blocks.append((text, str(node.root.tag).lower().startswith("h") or styled_heading))
    candidates: dict[str, str] = {}
    for index, (text, _) in enumerate(blocks):
        name = aliases.get(text.rstrip(":").casefold())
        if name is None:
            continue
        body = []
        for value, heading in blocks[index + 1:]:
            if heading or value.rstrip(":").casefold() in boundaries:
                break
            body.append(value)
            if sum(map(len, body)) >= 4000:
                break
        content = " ".join(body)[:4000]
        if len(content) >= 80 and len(content) > len(candidates.get(name, "")):
            candidates[name] = content
    return [SecSection(name, candidates[name]) for name in
            ("Business", "Competition", "Customers", "Suppliers") if name in candidates]


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
    sections = (_registration_sections(selector) if form in REGISTRATION_FORMS
                else _named_bounded_sections(form, text))
    # Layout wrappers duplicate their descendants; only leaf tables own rows.
    for index, table in enumerate(selector.xpath("//table[not(.//table)]")[:12], 1):
        grid = []
        pending = {}
        oversized = False
        for tr in table.xpath("./tr|./thead/tr|./tbody/tr|./tfoot/tr")[:120]:
            cells = {}
            for column, (value, remaining) in list(pending.items()):
                cells[column] = value
                if remaining > 1:
                    pending[column] = (value, remaining - 1)
                else:
                    del pending[column]
            column = 0
            for cell in tr.xpath("./th|./td"):
                while column in cells:
                    column += 1
                value = " ".join(" ".join(cell.xpath(".//text()").getall()).split())[:300]
                try:
                    width = max(1, int(cell.attrib.get("colspan", "1")))
                    height = max(1, int(cell.attrib.get("rowspan", "1")))
                except ValueError:
                    oversized = True
                    break
                if column + width > 64 or height > 120:
                    oversized = True
                    break
                for offset in range(width):
                    cells[column + offset] = value
                    if height > 1:
                        pending[column + offset] = (value, height - 1)
                column += width
            if oversized:
                break
            grid.append(tuple(cells.get(column, "") for column in range(max(cells, default=-1) + 1)))
        # Never silently chop off comparative periods from a wide statement.
        if oversized:
            continue
        rows = tuple(grid)
        if any(
            re.search(r"net sales|revenue|operating income|cash flow", " ".join(row), re.I)
            for row in rows
        ):
            context = []
            layout_ancestors = set(table.root.iterancestors("table"))
            # Stop at the preceding table, including its caption and units.
            # Filtering table text alone would retain that prior caption.
            for text_node in reversed(table.root.xpath("preceding::text()")):
                parent = text_node.getparent()
                ancestors = set(parent.iterancestors("table"))
                if parent.tag == "table":
                    ancestors.add(parent)
                if ancestors - layout_ancestors:
                    break
                context.append(str(text_node))
                if sum(map(len, context)) >= 1000:
                    break
            preceding = " ".join(" ".join(reversed(context)).split())
            caption = " ".join(table.xpath("./caption//text()").getall())
            sections.append(SecSection(f"Table {index}", (preceding + " " + caption).strip()[-1000:], rows))
        if len(sections) >= 6:
            break
    return sections[:6]
