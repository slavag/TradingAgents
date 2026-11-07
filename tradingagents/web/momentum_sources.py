"""Fixed, bounded public-market sources for the research-only Discover screen."""

from __future__ import annotations

import json
import math
import re
import time
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as market_calendars
import requests
import yfinance as yf

from tradingagents.settings import get_data_source_setting
from tradingagents.web.momentum_screen import (
    DailyBar,
    EquityIdentity,
    price_priority,
    price_signals,
)
from tradingagents.web.speaking_sources import (
    APEWISDOM_ALL_STOCKS_URL,
    STOCKTWITS_TRENDING_URL,
    parse_apewisdom_results,
    parse_stocktwits_results,
)

_SYMBOL = re.compile(r"[A-Z][A-Z0-9.\-]{0,5}\Z")
_AV_URL = "https://www.alphavantage.co/query"
_FMP_GAINERS_URL = "https://financialmodelingprep.com/stable/biggest-gainers"
_FMP_ACTIVE_URL = "https://financialmodelingprep.com/stable/most-actives"
_MAX_RESPONSE_BYTES = 200_000
_MAX_SYMBOLS = 100
_MAX_IDENTITIES = 40
_IDENTITY_WORKERS = 8
_US_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "NYSE AMERICAN", "NMS", "NYQ", "ASE"}
_NYSE = market_calendars.get_calendar("NYSE")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _latest_completed_date() -> date:
    now = _utcnow()
    market_date = now.astimezone(ZoneInfo("America/New_York")).date()
    schedule = _NYSE.schedule(start_date=market_date, end_date=market_date)
    if not schedule.empty and now >= schedule.iloc[0]["market_close"].to_pydatetime():
        return market_date
    return market_date - timedelta(days=1)


def _symbol(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    return normalized if _SYMBOL.fullmatch(normalized) else None


def _json_get(url: str, *, params: dict[str, str] | None = None) -> object:
    """Retrieve only fixed endpoints; never expose HTTP errors or bodies."""
    response = requests.get(
        url, params=params, timeout=10, allow_redirects=False, stream=True,
        headers={"Accept": "application/json", "User-Agent": "TradingAgents-Discover/1.0"},
    )
    started = time.monotonic()
    try:
        if response.status_code != 200:
            raise ValueError("unavailable")
        chunks: list[bytes] = []
        received = 0
        for chunk in response.iter_content(chunk_size=16_384):
            if time.monotonic() - started > 20:
                raise ValueError("deadline exceeded")
            received += len(chunk)
            if received > _MAX_RESPONSE_BYTES:
                raise ValueError("response too large")
            chunks.append(chunk)
        return json.loads(b"".join(chunks).decode("utf-8"))
    finally:
        response.close()


def _add_rows(
    membership: dict[str, set[str]], rows: object, *, source: str, field: str,
) -> None:
    if not isinstance(rows, list):
        raise ValueError("invalid feed")
    for row in rows[:100]:
        if not isinstance(row, dict):
            continue
        symbol = _symbol(row.get(field))
        if symbol:
            membership[symbol].add(source)


def fetch_source_membership() -> tuple[dict[str, set[str]], list[str]]:
    """Nominate symbols from fixed mover/attention feeds; either family suffices."""
    membership: dict[str, set[str]] = defaultdict(set)
    warnings: list[str] = []

    av_key = get_data_source_setting("ALPHA_VANTAGE_API_KEY")
    if av_key:
        try:
            payload = _json_get(_AV_URL, params={"function": "TOP_GAINERS_LOSERS", "apikey": av_key})
            if not isinstance(payload, dict):
                raise ValueError("invalid feed")
            _add_rows(membership, payload.get("top_gainers"), source="alpha_vantage_gainers", field="ticker")
            _add_rows(membership, payload.get("most_actively_traded"), source="alpha_vantage_active", field="ticker")
        except (requests.RequestException, ValueError, TypeError, KeyError):
            warnings.append("alpha_vantage_unavailable")
    else:
        warnings.append("alpha_vantage_unconfigured")

    fmp_key = get_data_source_setting("FMP_API_KEY")
    if fmp_key:
        for url, source in (
            (_FMP_GAINERS_URL, "fmp_gainers"), (_FMP_ACTIVE_URL, "fmp_active"),
        ):
            try:
                _add_rows(membership, _json_get(url, params={"apikey": fmp_key}), source=source, field="symbol")
            except (requests.RequestException, ValueError, TypeError, KeyError):
                if "fmp_unavailable" not in warnings:
                    warnings.append("fmp_unavailable")

    try:
        payload = _json_get(APEWISDOM_ALL_STOCKS_URL.format(page=1))
        if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
            raise ValueError("invalid feed")
        for symbol in list(parse_apewisdom_results(payload))[:100]:
            normalized = _symbol(symbol)
            if normalized:
                membership[normalized].add("apewisdom")
    except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError):
        warnings.append("apewisdom_unavailable")

    try:
        payload = _json_get(STOCKTWITS_TRENDING_URL)
        if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
            raise ValueError("invalid feed")
        for symbol in list(parse_stocktwits_results(payload))[:100]:
            normalized = _symbol(symbol)
            if normalized:
                membership[normalized].add("stocktwits")
    except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError):
        warnings.append("stocktwits_unavailable")

    return dict(membership), warnings


def _number(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _series_for(frame: pd.DataFrame, symbol: str, *, single_symbol: bool) -> pd.DataFrame | None:
    if frame.empty:
        return None
    if not isinstance(frame.columns, pd.MultiIndex):
        return frame if single_symbol and len(frame.columns) else None
    if symbol in frame.columns.get_level_values(0):
        return frame[symbol]
    if symbol in frame.columns.get_level_values(1):
        return frame.xs(symbol, axis=1, level=1)
    return None


def _bars(
    frame: pd.DataFrame, symbol: str, *, single_symbol: bool, completed_through: date,
) -> tuple[DailyBar, ...]:
    data = _series_for(frame, symbol, single_symbol=single_symbol)
    if data is None or "Close" not in data or "Volume" not in data:
        return ()
    adjusted = data["Adj Close"] if "Adj Close" in data else data["Close"]
    bars: list[DailyBar] = []
    for stamp, close, adj_close, volume in zip(
        data.index, data["Close"], adjusted, data["Volume"], strict=True,
    ):
        try:
            observed = pd.Timestamp(stamp).date()
        except (TypeError, ValueError):
            continue
        if not isinstance(observed, date) or observed > completed_through:
            continue
        bars.append(DailyBar(observed, _number(adj_close), _number(close), _number(volume)))
    return tuple(bars)


def _identity(symbol: str) -> EquityIdentity | None:
    search = yf.Search(symbol, max_results=5, news_count=0, timeout=8)
    for quote in search.quotes[:5]:
        if not isinstance(quote, dict) or _symbol(quote.get("symbol")) != symbol:
            continue
        exchange = str(quote.get("exchange") or quote.get("fullExchangeName") or "").upper()
        quote_type = str(quote.get("quoteType") or "").upper()
        active = quote.get("isActive", quote.get("active", True)) is not False
        if exchange not in _US_EXCHANGES or quote_type != "EQUITY" or not active:
            return None
        currency = str(quote.get("currency") or "").upper()
        if not currency:
            # Search quotes can omit currency. Chart metadata verifies it without
            # inferring USD merely from a US exchange code.
            ticker = yf.Ticker(symbol)
            ticker.history(period="5d", interval="1h", auto_adjust=False,
                           timeout=8, raise_errors=True)
            metadata = ticker.get_history_metadata()
            if not isinstance(metadata, dict):
                raise ValueError("identity metadata unavailable")
            chart_symbol = _symbol(metadata.get("symbol"))
            chart_exchange = str(metadata.get("exchangeName") or "").upper()
            chart_type = str(metadata.get("instrumentType") or "").upper()
            currency = str(metadata.get("currency") or "").upper()
            if not currency or not chart_exchange or not chart_type:
                raise ValueError("identity metadata unavailable")
            if (chart_symbol is not None and chart_symbol != symbol) or (
                chart_exchange != exchange or chart_type != quote_type
            ):
                return None
        if currency != "USD":
            return None
        return EquityIdentity(
            symbol=symbol, name=str(quote.get("longname") or quote.get("shortname") or symbol),
            exchange=exchange, currency=currency, quote_type=quote_type, active=True,
        )
    return None


def fetch_market_evidence(
    symbols: Sequence[str],
) -> tuple[dict[str, tuple[DailyBar, ...]], dict[str, EquityIdentity], list[str]]:
    """Load completed history and verify exact US-equity identity for bounded symbols."""
    normalized = list(dict.fromkeys(filter(None, (_symbol(raw) for raw in symbols))))[:_MAX_SYMBOLS]
    if not normalized:
        return {}, {}, []
    try:
        frame = yf.download(
            normalized, period="6mo", interval="1d", auto_adjust=False,
            group_by="ticker", multi_level_index=True, progress=False,
            threads=4, timeout=20,
        )
        if not isinstance(frame, pd.DataFrame):
            raise ValueError("invalid market frame")
    except Exception:  # yfinance can raise provider-specific exceptions; never leak them.
        return {}, {}, ["market_data_unavailable"]

    completed_through = _latest_completed_date()
    histories = {
        symbol: bars for symbol in normalized
        if (bars := _bars(
            frame, symbol, single_symbol=len(normalized) == 1,
            completed_through=completed_through,
        ))
    }
    if not histories:
        return {}, {}, ["market_data_unavailable"]
    eligible = {
        symbol: signals for symbol, bars in histories.items()
        if (signals := price_signals(bars, as_of=_utcnow().date())) is not None
    }
    ordered = sorted(
        eligible,
        key=lambda symbol: (
            price_priority(eligible[symbol]), eligible[symbol].avg_dollar_volume, symbol,
        ),
        reverse=True,
    )[:_MAX_IDENTITIES]
    identities: dict[str, EquityIdentity] = {}
    failed_identity = False
    with ThreadPoolExecutor(max_workers=_IDENTITY_WORKERS) as pool:
        futures = {pool.submit(_identity, symbol): symbol for symbol in ordered}
        for future in as_completed(futures):
            try:
                identity = future.result()
                if identity is not None:
                    identities[futures[future]] = identity
            except Exception:  # Search/chart errors vary; warning is deliberately generic.
                failed_identity = True
    warnings = ["identity_unavailable"] if failed_identity else []
    return histories, identities, warnings
