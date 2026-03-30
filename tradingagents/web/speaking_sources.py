from __future__ import annotations

import logging
import re
import time
from io import StringIO
from typing import Callable

import pandas as pd
import requests
from parsel import Selector

logger = logging.getLogger("tradingagents.web.speaking_sources")

MOTLEY_FOOL_GAINERS_URL = "https://www.fool.com/markets/top-stock-gainers/"
MOTLEY_FOOL_MOST_ACTIVE_URL = "https://www.fool.com/markets/most-active-stocks/"
MOTLEY_FOOL_MARKETS_URL = "https://www.fool.com/markets/"
APEWISDOM_ALL_STOCKS_URL = "https://apewisdom.io/api/v1.0/filter/all-stocks/page/{page}"
STOCKTWITS_TRENDING_URL = "https://api.stocktwits.com/api/2/trending/symbols.json"

SOURCE_URLS: dict[str, str] = {
    "motley_fool_gainers": MOTLEY_FOOL_GAINERS_URL,
    "motley_fool_most_active": MOTLEY_FOOL_MOST_ACTIVE_URL,
    "motley_fool_markets": MOTLEY_FOOL_MARKETS_URL,
}

SOURCE_LABELS: dict[str, str] = {
    "apewisdom": "ApeWisdom",
    "stocktwits": "StockTwits",
    "motley_fool_gainers": "Motley Fool Gainers",
    "motley_fool_most_active": "Motley Fool Most Active",
    "motley_fool_markets": "Motley Fool Markets",
}

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

STOCKTWITS_HEADERS = {
    "User-Agent": REQUEST_HEADERS["User-Agent"],
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://stocktwits.com",
    "Referer": "https://stocktwits.com/sentiment",
    "Connection": "keep-alive",
}

NON_SYMBOL_TOKENS = {
    "BUY",
    "CEO",
    "ETF",
    "GDP",
    "IRA",
    "NASDAQ",
    "NYSE",
    "PM",
    "SELL",
    "USD",
}


def _normalize_symbol(value: object) -> str | None:
    raw = str(value or "").strip().upper()
    if not raw:
        return None

    token = re.sub(r"[^A-Z.\-]", "", raw)
    if not token or len(token) > 6:
        return None
    if token in NON_SYMBOL_TOKENS:
        return None
    if not re.fullmatch(r"[A-Z][A-Z.\-]{0,5}", token):
        return None
    return token


def _extract_symbols_from_table_html(html: str) -> set[str]:
    try:
        frames = pd.read_html(StringIO(html), displayed_only=False)
    except ValueError:
        return set()

    symbols: set[str] = set()
    for frame in frames:
        columns = {str(column).strip().lower(): column for column in frame.columns}
        symbol_column = next(
            (
                original
                for lowered, original in columns.items()
                if "symbol" in lowered or "ticker" in lowered
            ),
            None,
        )
        if symbol_column is None:
            continue

        for value in frame[symbol_column].tolist():
            symbol = _normalize_symbol(value)
            if symbol:
                symbols.add(symbol)
    return symbols


def _extract_symbols_from_quote_links(html: str) -> set[str]:
    selector = Selector(text=html)
    symbols: set[str] = set()
    for anchor in selector.css("a[href]"):
        href = anchor.attrib.get("href", "")
        if "/quote/" not in href:
            continue

        match = re.search(r"/quote/(?:[^/]+/)?([A-Za-z.\-]+)/?$", href)
        if match:
            href_symbol = _normalize_symbol(match.group(1))
            if href_symbol:
                symbols.add(href_symbol)

        text = " ".join(part.strip() for part in anchor.css("::text").getall()).strip()
        for candidate in re.findall(r"\b[A-Z][A-Z.\-]{0,5}\b", text):
            text_symbol = _normalize_symbol(candidate)
            if text_symbol:
                symbols.add(text_symbol)

    return symbols


def extract_symbols_from_stock_page_html(html: str) -> set[str]:
    return _extract_symbols_from_table_html(html) | _extract_symbols_from_quote_links(html)


def extract_symbols_from_market_page_html(html: str) -> set[str]:
    symbols = _extract_symbols_from_quote_links(html)
    if symbols:
        return symbols

    selector = Selector(text=html)
    for text in selector.css("body *::text").getall():
        line = " ".join(text.split())
        if "$" not in line and "%" not in line:
            continue
        match = re.match(r"^([A-Z][A-Z.\-]{0,5})\b", line)
        if not match:
            continue
        symbol = _normalize_symbol(match.group(1))
        if symbol:
            symbols.add(symbol)
    return symbols


def fetch_html(url: str, timeout: int = 20) -> str:
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def parse_apewisdom_results(payload: dict, allowed_symbols: set[str] | None = None) -> dict[str, dict[str, float | int]]:
    results: dict[str, dict[str, float | int]] = {}
    for rank, row in enumerate(payload.get("results", []), start=1):
        symbol = _normalize_symbol(row.get("ticker"))
        if not symbol:
            continue
        if allowed_symbols is not None and symbol not in allowed_symbols:
            continue
        mentions = int(row.get("mentions") or 0)
        results[symbol] = {
            "mentions": mentions,
            "rank": int(row.get("rank") or rank),
        }
    return results


def parse_stocktwits_results(payload: dict, allowed_symbols: set[str] | None = None) -> dict[str, dict[str, float | int]]:
    results: dict[str, dict[str, float | int]] = {}
    symbols = payload.get("symbols", [])
    total = len(symbols)
    for rank, row in enumerate(symbols, start=1):
        symbol = _normalize_symbol(row.get("symbol"))
        if not symbol:
            continue
        if allowed_symbols is not None and symbol not in allowed_symbols:
            continue

        trending_score = row.get("trending_score")
        if trending_score is None:
            # Fall back to list position so the feed still has a stable daily priority.
            trending_score = total - rank + 1

        results[symbol] = {
            "trending_score": float(trending_score),
            "rank": rank,
        }
    return results


def fetch_apewisdom_activity(
    pages: int = 2,
    allowed_symbols: set[str] | None = None,
    timeout: int = 30,
) -> dict[str, dict[str, float | int]]:
    combined: dict[str, dict[str, float | int]] = {}
    for page in range(1, pages + 1):
        response = requests.get(
            APEWISDOM_ALL_STOCKS_URL.format(page=page),
            headers=REQUEST_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        page_results = parse_apewisdom_results(response.json(), allowed_symbols=allowed_symbols)
        for symbol, metrics in page_results.items():
            existing = combined.get(symbol)
            if existing is None or int(metrics["rank"]) < int(existing["rank"]):
                combined[symbol] = metrics
    return combined


def fetch_stocktwits_activity(
    allowed_symbols: set[str] | None = None,
    timeout: int = 15,
    retries: int = 2,
    backoff: float = 1.5,
) -> dict[str, dict[str, float | int]]:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = requests.get(
                STOCKTWITS_TRENDING_URL,
                headers=STOCKTWITS_HEADERS,
                timeout=timeout,
            )
            response.raise_for_status()
            return parse_stocktwits_results(response.json(), allowed_symbols=allowed_symbols)
        except Exception as exc:
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(backoff ** attempt)
    raise RuntimeError(f"StockTwits activity fetch failed: {last_error}")


def fetch_external_market_symbols(
    allowed_symbols: set[str] | None = None,
    html_fetcher: Callable[[str], str] | None = None,
) -> dict[str, set[str]]:
    fetcher = html_fetcher or fetch_html
    parsers: dict[str, Callable[[str], set[str]]] = {
        "motley_fool_gainers": extract_symbols_from_stock_page_html,
        "motley_fool_most_active": extract_symbols_from_stock_page_html,
        "motley_fool_markets": extract_symbols_from_market_page_html,
    }

    results: dict[str, set[str]] = {}
    for source_name, url in SOURCE_URLS.items():
        parser = parsers[source_name]
        try:
            html = fetcher(url)
            symbols = parser(html)
            if allowed_symbols is not None:
                symbols = {symbol for symbol in symbols if symbol in allowed_symbols}
            results[source_name] = symbols
        except Exception as exc:
            logger.warning("Speaking stocks: failed to fetch %s from %s: %s", source_name, url, exc)
            results[source_name] = set()
    return results
