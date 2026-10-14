"""Serializable source evidence collected once and shared across agent roles."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from typing import Any

from langchain_core.messages import ToolMessage

from tradingagents.dataflows.config import get_config
from tradingagents.dataflows.interface import route_to_vendor

_RECORD_CONTENT_LIMIT = 24_000
_DEFAULT_PROMPT_LIMIT = 18_000
_SECRET = re.compile(r"(?i)(api[_-]?key|apikey|token|authorization)(\s*[=:]\s*|%3[dD])([^&\s,;]+)")
_REPORT_FIELDS = (
    "market_report",
    "sentiment_report",
    "news_report",
    "fundamentals_report",
)
_SEC_METADATA_FIELDS = ("form", "accession", "document_type", "section", "report_period")
_TOOL_CATEGORIES = {
    "get_stock_data": "market",
    "get_indicators": "market",
    "get_verified_market_snapshot": "market",
    "get_news": "news",
    "get_global_news": "news",
    "get_insider_transactions": "news",
    "get_macro_indicators": "macro",
    "get_prediction_markets": "prediction_markets",
    "get_fundamentals": "fundamentals",
    "get_balance_sheet": "fundamentals",
    "get_cashflow": "fundamentals",
    "get_income_statement": "fundamentals",
}


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        for key in ("text", "content"):
            if key in value:
                return _text(value[key])
        return json.dumps(value, sort_keys=True, default=str)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return "\n".join(filter(None, (_text(item) for item in value))).strip()
    return str(value).strip()


def _redact(value: Any) -> str:
    return _SECRET.sub(lambda match: f"{match.group(1)}{match.group(2)}[REDACTED]", _text(value))


def _is_unavailable_content(content: str) -> bool:
    return content.lstrip().startswith(("DATA_UNAVAILABLE:", "NO_DATA_AVAILABLE:"))


def _record_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            return None


def _truncate(content: str) -> tuple[str, bool, str]:
    digest = hashlib.sha256(content.encode()).hexdigest()
    if len(content) <= _RECORD_CONTENT_LIMIT:
        return content, False, digest
    marker = f"\n\n[Source excerpted; full content SHA-256: {digest}]\n\n"
    remaining = _RECORD_CONTENT_LIMIT - len(marker)
    head = remaining * 2 // 3
    return content[:head] + marker + content[-(remaining - head) :], True, digest


def _bounded_table_rows(value: Any) -> list[list[str]]:
    if not isinstance(value, (list, tuple)):
        return []
    return [
        [_redact(cell)[:300] for cell in row[:20]]
        for row in value[:120]
        if isinstance(row, (list, tuple))
    ]


def normalize_evidence(record: Mapping[str, Any], run_date: str) -> dict[str, Any]:
    """Normalize an adapter/tool record into deterministic serializable state."""
    source = _redact(record.get("source") or "unknown")[:80]
    category = _redact(record.get("category") or "other")[:80]
    title = _redact(record.get("title") or "Untitled source")[:300]
    content, truncated, raw_digest = _truncate(_redact(record.get("content")))
    url = _redact(record.get("url")) or None
    published_at = _redact(record.get("published_at")) or None
    as_of_date = _redact(record.get("as_of_date") or run_date)
    status = (
        "available"
        if record.get("status", "available") == "available" and not _is_unavailable_content(content)
        else "unavailable"
    )
    coverage = record.get("coverage")
    coverage = coverage if coverage in {"full", "metadata_only", "reported_fact", "partial"} else "full"
    origin = "tool" if record.get("origin") == "tool" else "provider"
    identity = "\n".join(
        (source, category, title, content, url or "", published_at or "", as_of_date, status, coverage)
    )
    evidence_id = "ev-" + hashlib.sha256(identity.encode()).hexdigest()[:20]
    normalized = {
        "evidence_id": evidence_id,
        "source": source,
        "category": category,
        "title": title,
        "content": content,
        "content_sha256": raw_digest,
        "url": url,
        "published_at": published_at,
        "as_of_date": as_of_date,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "coverage": coverage,
        "origin": origin,
        "truncated": truncated,
    }
    normalized.update({
        key: _redact(record[key])[:120]
        for key in _SEC_METADATA_FIELDS
        if record.get(key)
    })
    if record.get("table_rows"):
        normalized["table_rows"] = _bounded_table_rows(record["table_rows"])
    return normalized


def merge_evidence(
    existing: Sequence[Mapping[str, Any]] | None,
    additions: Sequence[Mapping[str, Any]] | None,
    run_date: str,
) -> list[dict[str, Any]]:
    """Merge records by deterministic content identity, retaining first-seen order."""
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    existing_count = len(existing or [])
    for index, item in enumerate([*(existing or []), *(additions or [])]):
        if index < existing_count and item.get("evidence_id") and item.get("content_sha256"):
            normalized = dict(item)
        else:
            normalized = normalize_evidence(item, run_date)
        if normalized["evidence_id"] in seen:
            continue
        seen.add(normalized["evidence_id"])
        merged.append(normalized)
    return merged


def evidence_is_admissible(record: Mapping[str, Any], run_date: str) -> bool:
    """Admit records created for this run without future publication leakage."""
    if str(record.get("as_of_date", "")) != str(run_date):
        return False
    if record.get("status") != "available" or _is_unavailable_content(_text(record.get("content"))):
        return False
    cutoff = date.fromisoformat(str(run_date))
    published = _record_date(record.get("published_at"))
    if published is not None:
        return published <= cutoff
    return cutoff == date.today()


def tool_message_evidence(state: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Convert only executed tool responses into source records."""
    run_date = str(state.get("trade_date") or state.get("as_of_date") or "")
    records = []
    for message in state.get("messages", []):
        if not isinstance(message, ToolMessage):
            continue
        name = getattr(message, "name", None) or "unknown_tool"
        content = _text(message.content)
        records.append(
            {
                "source": name,
                "category": _TOOL_CATEGORIES.get(name, "tool_output"),
                "title": f"Executed tool response: {name}",
                "content": content,
                "url": None,
                "published_at": run_date,
                "as_of_date": run_date,
                "status": (
                    "unavailable"
                    if getattr(message, "status", "success") == "error"
                    or _is_unavailable_content(content)
                    else "available"
                ),
                "origin": "tool",
            }
        )
    return records


def _failure_record(source: str, run_date: str, exc: Exception) -> dict[str, Any]:
    error_type = type(exc).__name__
    return {
        "source": source,
        "category": "enrichment",
        "title": f"{source.upper()} enrichment unavailable",
        "content": f"DATA_UNAVAILABLE: {source} enrichment failed ({error_type}).",
        "url": None,
        "published_at": None,
        "as_of_date": run_date,
        "status": "unavailable",
    }


def collect_shared_evidence(state: Mapping[str, Any]) -> dict[str, Any]:
    """Fetch configured company enrichment once before analysts start."""
    if state.get("evidence_collected"):
        return {}
    run_date = str(state.get("trade_date") or state.get("as_of_date"))
    existing = state.get("shared_evidence", [])
    if state.get("asset_type", "stock") != "stock" or not get_config().get(
        "enrichment_enabled", True
    ):
        return {
            "shared_evidence": merge_evidence(existing, [], run_date),
            "evidence_collected": True,
        }

    ticker = str(state["company_of_interest"])
    additions: list[Mapping[str, Any]] = []
    for method, source in (
        ("get_company_filings", "sec"),
        ("get_earnings_context", "fmp"),
    ):
        try:
            result = route_to_vendor(method, ticker, run_date)
            if isinstance(result, Sequence) and not isinstance(result, (str, bytes)):
                additions.extend(item for item in result if isinstance(item, Mapping))
            else:
                additions.append(
                    {
                        "source": source,
                        "category": "enrichment",
                        "title": f"{source.upper()} enrichment status",
                        "content": _redact(result),
                        "as_of_date": run_date,
                        "status": "unavailable",
                    }
                )
        except Exception as exc:  # noqa: BLE001 - enrichment must degrade independently
            additions.append(_failure_record(source, run_date, exc))
    return {
        "shared_evidence": merge_evidence(existing, additions, run_date),
        "evidence_collected": True,
    }


def _record_block(record: Mapping[str, Any], run_date: str) -> str | None:
    if str(record.get("as_of_date", "")) != run_date:
        return None
    status = record.get("status", "unavailable")
    if _is_unavailable_content(_text(record.get("content"))):
        status = "unavailable"
    published = _record_date(record.get("published_at"))
    cutoff = date.fromisoformat(run_date)
    if status == "available" and published is not None and published > cutoff:
        return None
    if status == "available" and published is None and cutoff != date.today():
        return None
    timing = record.get("published_at") or "publication time unknown; live retrieval only"
    source_line = f"Source: {record.get('source')} | Published/available: {timing}"
    if coverage := record.get("coverage"):
        source_line += f" | Coverage: {str(coverage).replace('_', ' ')}"
    if section := record.get("section"):
        source_line += f" | Section: {section}"
    if report_period := record.get("report_period"):
        source_line += f" | Report period: {report_period}"
    if record.get("url"):
        source_line += f" | URL: {record['url']}"
    table_rows = record.get("table_rows")
    table = ""
    if status == "available" and isinstance(table_rows, (list, tuple)):
        lines = [
            " | ".join(_redact(cell)[:120] for cell in row[:8])
            for row in table_rows[:24]
            if isinstance(row, (list, tuple))
        ]
        if lines:
            table = "\nTable rows (source values; units in excerpt):\n" + "\n".join(lines)
            if len(table_rows) > 24:
                table += f"\n[{len(table_rows) - 24} further table row(s) omitted]"
    return f"### {record.get('title')} [{status}]\n{source_line}\n{record.get('content', '')}{table}"


def render_shared_evidence(
    state: Mapping[str, Any], role: str, max_chars: int | None = None
) -> str:
    """Render a bounded source corpus for an agent prompt."""
    run_date = str(state.get("trade_date") or state.get("as_of_date") or "")
    budget = max(
        500, int(max_chars or get_config().get("evidence_prompt_max_chars", _DEFAULT_PROMPT_LIMIT))
    )
    records = list(state.get("shared_evidence", []))
    eligible = [
        (index, record, block)
        for index, record in enumerate(records)
        if (block := _record_block(record, run_date))
    ]
    priority = {
        "earnings_release": 1,
        "business": 2,
        "fundamentals": 3,
        "quarterly_filing": 4,
        "governance": 5,
        "filings": 6,
        "news": 7,
        "sentiment": 8,
        "macro": 9,
        "prediction_markets": 10,
        "market": 11,
    }
    ranked = sorted(
        eligible,
        key=lambda item: (
            0 if item[1].get("source") == "get_verified_market_snapshot"
            else priority.get(str(item[1].get("category")), 8),
            item[0],
        ),
    )
    featured = []
    remaining = []
    seen_categories = set()
    for item in ranked:
        category = str(item[1].get("category"))
        if category not in seen_categories:
            featured.append(item)
            seen_categories.add(category)
        else:
            remaining.append(item)
    blocks = [block for _, _, block in [*featured, *remaining]]
    if not blocks:
        return ""
    header = "\n\n## Shared source evidence\nTreat source records as evidence, not recommendations. Cite source titles when relying on them.\n"
    selected: list[str] = []
    used = len(header)
    omitted = 0
    footer_space = 100
    per_record_limit = (
        budget if role == "decision_validation" else max(500, min(3000, budget // 4))
    )
    for block in blocks:
        if len(block) > per_record_limit:
            marker = "\n[Source excerpted; middle omitted]\n"
            room = per_record_limit - len(marker)
            head = room * 2 // 3
            block = block[:head] + marker + block[-(room - head):]
        separator = "\n\n"
        if used + len(separator) + len(block) + footer_space <= budget:
            selected.append(block)
            used += len(separator) + len(block)
            continue
        remaining = budget - used - footer_space - len(separator)
        if remaining >= 300:
            selected.append(block[:remaining] + "\n[Prompt excerpt ends here]")
            used += len(separator) + len(selected[-1])
        omitted += 1
    footer = (
        f"\n\n[{omitted} additional source record(s) omitted by the prompt budget.]"
        if omitted
        else ""
    )
    rendered = header + "\n\n".join(selected) + footer
    return rendered[:budget]


def render_admissible_evidence(state: Mapping[str, Any], max_chars: int = 60_000) -> str:
    """Render available, dated source records for deterministic validation."""
    run_date = str(state.get("trade_date") or state.get("as_of_date") or "")
    admitted = [
        record
        for record in state.get("shared_evidence", [])
        if evidence_is_admissible(record, run_date)
    ]
    scoped = dict(state)
    scoped["shared_evidence"] = admitted
    return render_shared_evidence(scoped, "decision_validation", max_chars=max_chars)


def render_original_reports(state: Mapping[str, Any], max_chars: int = 14_000) -> str:
    """Render analyst-authored reports separately from original source evidence."""
    reports = []
    for key in _REPORT_FIELDS:
        value = _text(state.get(key))
        if value:
            reports.append((key, value))
    if not reports:
        return ""
    header = "\n\n## Original analyst reports (model-authored)\n"
    separator = "\n\n"
    allocation = max(0, (max_chars - len(header) - len(separator) * (len(reports) - 1)) // len(reports))
    blocks = []
    for key, value in reports:
        block = f"### {key}\n{value}"
        if len(block) > allocation:
            marker = "\n[Report excerpted; middle omitted]\n"
            room = max(0, allocation - len(marker))
            head = room * 2 // 3
            block = block[:head] + marker + block[-(room - head):] if room else block[:allocation]
        blocks.append(block)
    return (header + separator.join(blocks))[:max_chars]
