"""Scoped, cached adapters. Related prices never enter target trading state."""

from __future__ import annotations

import copy
import json
import re
from collections import OrderedDict
from dataclasses import asdict
from datetime import date, datetime, timedelta
from time import monotonic
from typing import Any

from tradingagents.agents.utils.agent_utils import resolve_instrument_identity
from tradingagents.agents.utils.evidence import evidence_is_admissible, normalize_evidence
from tradingagents.dataflows.config import get_config
from tradingagents.dataflows.interface import route_to_vendor
from tradingagents.dataflows.stockstats_utils import load_ohlcv
from tradingagents.dataflows.y_finance import _MAIN_CURRENCY_UNIT
from tradingagents.research_ledger import build_research_ledger

_CACHE: OrderedDict[str, tuple[float, dict]] = OrderedDict()
_CACHE_TTL_SECONDS = 300
_IDENTITY_CACHE: OrderedDict[tuple[Any, str], tuple[float, dict]] = OrderedDict()


def _current_identity(symbol: str) -> dict:
    """Bound related identity freshness without disturbing the main instrument cache."""
    resolver = getattr(resolve_instrument_identity, "__wrapped__", resolve_instrument_identity)
    key = (resolver, symbol)
    cached = _IDENTITY_CACHE.get(key)
    if cached and 0 <= monotonic() - cached[0] < _CACHE_TTL_SECONDS:
        _IDENTITY_CACHE.move_to_end(key)
        return copy.deepcopy(cached[1])
    identity = resolver(symbol)
    _IDENTITY_CACHE[key] = (monotonic(), copy.deepcopy(identity))
    _IDENTITY_CACHE.move_to_end(key)
    while len(_IDENTITY_CACHE) > 128:
        _IDENTITY_CACHE.popitem(last=False)
    return identity


def _identity_name(value: str) -> str:
    return re.sub(
        r"[^a-z0-9]",
        "",
        re.sub(r"\b(incorporated|corporation|corp|inc|ltd|limited|plc)\b", "", value.casefold()),
    )


def verify_symbol(
    name: str, symbol: str | None, sources: list[dict], as_of: str
) -> tuple[str | None, str, dict]:
    if not symbol or not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,15}", symbol):
        return None, "unresolved", {}
    # A ticker must be explicitly paired with the entity, never inferred from an alias.
    paired = any(
        re.search(
            r"(?<!\w)"
            + re.escape(name)
            + r"(?!\w)\s*\((?:(?:NASDAQ|NYSE|AMEX)\s*:\s*)?"
            + re.escape(symbol)
            + r"\)",
            str(s.get("content", "")),
            re.I,
        )
        for s in sources
    )
    if as_of != date.today().isoformat():
        return (symbol, "source_verified", {}) if paired else (None, "unresolved", {})
    identity = _current_identity(symbol)
    if _identity_name(identity.get("company_name", "")) == _identity_name(name) and _identity_name(
        name
    ):
        return symbol, "live_identity_verified", identity
    return None, "unresolved", {}


def _news_rows(result: Any, symbol: str) -> list[dict]:
    payload = result
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            # The configured Yahoo wrapper has explicit per-article publication headers.
            rows = []
            for match in re.finditer(
                r"^### (.*?) \(published: (\d{4}-\d{2}-\d{2}); source: ([^\n]+)\)\n(.*?)(?=^### |\Z)",
                payload,
                re.M | re.S,
            ):
                title, published, publisher, content = match.groups()
                link = re.search(r"^Link: (https?://\S+)", content, re.M)
                rows.append(
                    {
                        "source": publisher,
                        "category": "news",
                        "title": title,
                        "content": content.strip(),
                        "published_at": published,
                        "url": link.group(1) if link else None,
                        "status": "available",
                    }
                )
            return rows
    if isinstance(payload, dict) and isinstance(payload.get("feed"), list):
        rows = []
        for article in payload["feed"][:50]:
            if not isinstance(article, dict):
                continue
            tags = {
                str(t.get("ticker")).upper()
                for t in article.get("ticker_sentiment", [])
                if isinstance(t, dict)
            }
            if symbol.upper() not in tags:
                continue
            try:
                published = (
                    datetime.strptime(str(article.get("time_published"))[:8], "%Y%m%d")
                    .date()
                    .isoformat()
                )
            except ValueError:
                continue
            rows.append(
                {
                    "source": article.get("source") or "alpha_vantage",
                    "category": "news",
                    "title": article.get("title") or "Dated news",
                    "content": article.get("summary") or article.get("title") or "",
                    "published_at": published,
                    "url": article.get("url"),
                    "status": "available",
                }
            )
        return rows
    if isinstance(payload, (list, tuple)):
        return [dict(r) for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict) and "content" in payload:
        return [payload]
    return []


def _records(result: Any, method: str, symbol: str, as_of: str) -> list[dict]:
    if method == "get_news":
        rows = _news_rows(result, symbol)
    elif isinstance(result, (list, tuple)):
        rows = [dict(r) for r in result if isinstance(r, dict)]
    elif isinstance(result, dict) and "content" in result:
        rows = [result]
    else:
        rows = [
            {
                "source": method,
                "title": f"{symbol} {method}",
                "content": str(result or ""),
                "category": "fundamentals",
                "published_at": None,
                "status": "available",
                "as_of_date": as_of,
            }
        ]
    normalized = []
    for row in rows[:24]:
        row.setdefault("source", method)
        row.setdefault("title", f"{symbol} {method}")
        row.setdefault("category", "news" if method == "get_news" else "fundamentals")
        row["as_of_date"] = as_of
        record = normalize_evidence(row, as_of)
        for field in ("currency", "reporting_currency"):
            value = str(row.get(field) or "").upper()
            if re.fullmatch(r"[A-Z]{3}", value):
                record[field] = value
        if evidence_is_admissible(record, as_of) and record["content"].strip():
            normalized.append(record)
    return normalized


def _performance(symbol: str, as_of: str, currency: str | None) -> tuple[list[dict], list[str]]:
    if as_of != date.today().isoformat():
        return [], [
            "Historical performance unavailable: configured adjusted OHLCV cache is not point-in-time adjustment verified."
        ]
    if not currency:
        return [], ["Performance unavailable: quote currency unverified."]
    import pandas as pd

    data = load_ohlcv(symbol, as_of).copy()
    if "Date" not in data or "Close" not in data:
        return [], ["Performance unavailable: dated adjusted closes missing."]
    data["Date"] = pd.to_datetime(data["Date"], utc=True).dt.tz_convert(None)
    data = data[data["Date"] <= pd.Timestamp(as_of)].sort_values("Date").drop_duplicates("Date")
    if data.empty or (pd.Timestamp(as_of) - data.iloc[-1]["Date"]).days > 5:
        return [], ["Performance unavailable: cutoff close missing or stale."]
    end = data.iloc[-1]
    results = []
    for months in (1, 3, 6, 12):
        anchor = pd.Timestamp(as_of) - pd.DateOffset(months=months)
        eligible = data[data["Date"] <= anchor]
        if eligible.empty:
            continue
        start = eligible.iloc[-1]
        if (anchor - start["Date"]).days > 5 or float(start["Close"]) <= 0:
            continue
        results.append(
            {
                "months": months,
                "start_date": start["Date"].date().isoformat(),
                "end_date": end["Date"].date().isoformat(),
                "return_pct": round((float(end["Close"]) / float(start["Close"]) - 1) * 100, 4),
                "currency": currency,
                "adjustment": "Yahoo auto_adjust=True (current adjustment basis)",
                "source": "load_ohlcv",
            }
        )
    return results, [] if len(results) == 4 else [
        "Some return horizons lack comparable dated closes."
    ]


def _source_currency(source: dict) -> str | None:
    declared = str(source.get("reporting_currency") or source.get("currency") or "").upper()
    if re.fullmatch(r"[A-Z]{3}", declared):
        return declared
    content = str(source.get("content") or "")
    matches = set(
        re.findall(
            r"(?:currency\s*[:=]\s*|\bin\s+(?:(?:thousands|millions|billions)\s+of\s+)?|\bValue:\s*[-\d,.]+\s+)(USD|CAD|EUR|CNY|HKD|GBP|JPY|AUD|CHF)\b",
            content,
            flags=re.I,
        )
    )
    return next(iter(matches)).upper() if len(matches) == 1 else None


def _verified_fact_unit(fact: Any, sources: dict[str, dict], identity: dict) -> str | None:
    """Correct legacy ledger USD defaults only with verified unit provenance."""
    if fact.metric == "market_cap" and fact.basis == "market":
        # Price units such as GBp are not the major currency used for market cap.
        # This is a compatibility check, never a numerical conversion.
        raw_quote = str(identity.get("quote_currency") or "")
        main_quote = _MAIN_CURRENCY_UNIT.get(raw_quote, raw_quote).upper()
        declared_units = set()
        for key in fact.source_ids:
            source = sources.get(key, {})
            matches = re.findall(
                r"(?mi)^Market Cap:\s*[-\d,.]+\s+([A-Z]{3})\s*$", str(source.get("content") or "")
            )
            declared_units.update(matches)
            if caption := _source_currency(source):
                declared_units.add(caption)
        if len(declared_units) > 1:
            return None
        explicit = next(iter(declared_units), None)
        if explicit:
            if main_quote and explicit != main_quote:
                return None
            if fact.unit != explicit:
                return None
            return explicit
        if not re.fullmatch(r"[A-Z]{3}", main_quote):
            return None
        if fact.unit != "USD" and fact.unit != main_quote:
            return None
        return main_quote
    if not re.fullmatch(r"[A-Z]{3}(?:_per_share)?", fact.unit):
        return fact.unit
    reporting = str(identity.get("financial_currency") or "").upper()
    quote = str(identity.get("quote_currency") or "").upper()
    currency = quote if fact.metric == "market_cap" and fact.basis == "market" else reporting
    if not re.fullmatch(r"[A-Z]{3}", currency):
        return None
    if not fact.unit.startswith("USD") and fact.unit.split("_", 1)[0] != currency:
        return None
    for key in fact.source_ids:
        source = sources.get(key, {})
        caption = _source_currency(source)
        if fact.metric == "free_cash_flow":
            money_label = re.search(
                r"(?mi)^Free Cash Flow:\s*[-\d,.]+\s+([A-Z]{3})\s*$",
                str(source.get("content") or ""),
            )
            if money_label:
                explicit = money_label.group(1)
                if caption and caption != explicit:
                    return None
                caption = explicit
        # Filing tables may use a presentation currency different from the issuer's.
        # A caption (or a typed source currency) must agree; never infer from listing currency.
        if source.get("source") == "sec" and caption != currency:
            return None
        if caption is not None and caption != currency:
            return None
    return currency + ("_per_share" if fact.unit.endswith("_per_share") else "")


def collect_entity(entity: dict, as_of: str, identity: dict | None = None) -> dict:
    symbol = entity.get("symbol")
    if symbol and as_of != date.today().isoformat():
        return {
            "sources": [],
            "facts": [],
            "performance": [],
            "news": {"status": "unavailable", "latest_7_days": [], "material_30_days": []},
            "coverage": [
                "Historical secondary fundamentals, filings, news and performance unavailable: current symbol-keyed adapters do not guarantee stable as-of issuer identity. Dated relationship disclosure is retained as qualitative evidence."
            ],
        }
    if not symbol:
        return {
            "sources": [],
            "facts": [],
            "performance": [],
            "news": {"status": "unresolved", "latest_7_days": [], "material_30_days": []},
            "coverage": [
                "Private, anonymous, or unresolved public identity; retained as qualitative relationship coverage."
            ],
        }
    config_key = json.dumps(get_config(), sort_keys=True, default=str)
    roles = sorted({p["role"] for p in entity["roles"]})
    key = json.dumps([symbol, as_of, config_key, roles, identity], sort_keys=True)
    if key in _CACHE and monotonic() - _CACHE[key][0] < _CACHE_TTL_SECONDS:
        _CACHE.move_to_end(key)
        result = copy.deepcopy(_CACHE[key][1])
        for row in result["sources"]:
            row["entity_name"] = entity["name"]
        return result
    sources, coverage = [], []
    is_peer = "competitor" in roles
    methods = [
        ("get_news", (symbol, (date.fromisoformat(as_of) - timedelta(days=30)).isoformat(), as_of))
    ]
    if is_peer:
        methods.append(("get_company_filings", (symbol, as_of)))
        methods.extend(
            [
                ("get_fundamentals", (symbol, as_of)),
                ("get_income_statement", (symbol, "quarterly", as_of)),
            ]
        )
    news_ok = False
    news_clean = False
    for method, args in methods:
        try:
            response = route_to_vendor(method, *args)
            rows = _records(response, method, symbol, as_of)
            if method == "get_news":
                news_clean = (
                    isinstance(response, dict)
                    and response.get("status") == "clean"
                    and response.get("as_of_date") == as_of
                )
                # Unknown publication timestamps cannot support a dated news window.
                rows = [
                    r
                    for r in rows
                    if r.get("published_at")
                    and (date.fromisoformat(as_of) - timedelta(days=30)).isoformat()
                    <= str(r["published_at"])[:10]
                    <= as_of
                ]
                news_ok = bool(rows)
            sources.extend(rows)
            if not rows:
                coverage.append(f"{method}: no admissible dated source records.")
        except Exception:
            coverage.append(f"{method}: retrieval unavailable.")
    for row in sources:
        row.update(entity_name=entity["name"], entity_symbol=symbol)
    # The dedicated state guarantees peer facts cannot contaminate target price candidates.
    ledger = build_research_ledger({"shared_evidence": sources}, as_of)
    facts = []
    omitted_currency = False
    source_map = {source["evidence_id"]: source for source in sources}
    for fact in ledger.facts:
        unit = _verified_fact_unit(fact, source_map, identity or {})
        if unit is None:
            omitted_currency = True
            continue
        row = asdict(fact)
        row["unit"] = unit
        row["value"] = str(row["value"])
        row["source_ids"] = list(row["source_ids"])
        facts.append(row)
    if omitted_currency:
        coverage.append(
            "Monetary facts omitted: verified reporting currency or agreement with the source currency caption is unavailable; raw source preserved."
        )
    performance = []
    if is_peer:
        try:
            performance, gaps = _performance(symbol, as_of, (identity or {}).get("quote_currency"))
            coverage.extend(gaps)
        except Exception:
            coverage.append("Performance unavailable: OHLCV retrieval failed.")
        if not facts:
            coverage.append("Comparable financial metrics unavailable from admitted sources.")
    news_rows = [s for s in sources if s.get("category") == "news"]
    latest_cutoff = (date.fromisoformat(as_of) - timedelta(days=7)).isoformat()
    news = {
        "status": "available" if news_ok else "clean" if news_clean else "unavailable",
        "latest_7_days": [
            s["evidence_id"] for s in news_rows if str(s["published_at"])[:10] >= latest_cutoff
        ],
        "material_30_days": [s["evidence_id"] for s in news_rows],
    }
    # Materiality is not fabricated: the full 30-day candidate window is explicitly unclassified.
    if news_rows:
        coverage.append(
            "30-day news contains dated source candidates; materiality and target-order impact are unverified."
        )
    result = {
        "sources": sources,
        "facts": facts,
        "performance": performance,
        "news": news,
        "coverage": coverage,
    }
    _CACHE[key] = (monotonic(), copy.deepcopy(result))
    while len(_CACHE) > 128:
        _CACHE.popitem(last=False)
    return result
