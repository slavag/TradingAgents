"""Optional extended-hours context, kept separate from daily OHLCV anchors."""
from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal
import yfinance as yf

MAX_QUOTE_AGE = timedelta(hours=18)
# Allow final trades stamped in the closing minute or the immediate auction minute.
CLOSE_TIMESTAMP_TOLERANCE = timedelta(minutes=1)
_US_EXCHANGES = {'NMS', 'NAS', 'NYQ', 'ASE', 'NGM', 'NCM', 'PCX', 'BTS'}
_NOTE = ('Provider quotes may be delayed; extended-hours liquidity can be low. '
         'This is a market-reaction marker, with no causal proof linking a move to news. '
         'Daily OHLCV, indicators and target anchors retain the regular-session close.')


def _now():
    return datetime.now(timezone.utc)


def _timestamp(value):
    """Accept epoch seconds or an explicitly zoned ISO timestamp; never guess a zone."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        if isinstance(value, (float, int)):
            result = datetime.fromtimestamp(value, timezone.utc)
        elif isinstance(value, datetime):
            result = value
        elif isinstance(value, str):
            result = datetime.fromisoformat(value.replace('Z', '+00:00'))
        else:
            return None
        if result.tzinfo is None or result.utcoffset() is None:
            return None
        return result.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def _price(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def unavailable_reaction(symbol, analysis_date, reason, *, status='unavailable'):
    return {'symbol': symbol, 'analysis_date': analysis_date, 'status': status, 'reason': reason,
                'provider': 'yfinance', 'fetched_at': _now().isoformat(), 'note': _NOTE}


def fetch_extended_hours_reaction(symbol, analysis_date):
    """Fetch once per live market stage; admit US session quotes at most 18 hours old.

    A prior evening quote may remain informative before the next regular open.
    Weekends, historical dates, and ambiguous timestamps are never filled forward.
    """
    now = _now()
    ny_now = now.astimezone(ZoneInfo('America/New_York'))
    try:
        requested = date.fromisoformat(str(analysis_date))
    except ValueError:
        return unavailable_reaction(symbol, analysis_date, 'invalid_analysis_date')
    if requested != ny_now.date():
        return unavailable_reaction(symbol, analysis_date, 'not_current_exchange_date')
    # Calendar data is local: no provider access before historical/session guards.
    # The schedule includes holidays and actual early closes, in UTC.
    try:
        schedule = mcal.get_calendar('NYSE').schedule(
            start_date=requested - timedelta(days=14), end_date=requested,
        )
        today = schedule[schedule.index.date == requested]
        completed = schedule[schedule['market_close'] <= now]
        if today.empty or completed.empty:
            return unavailable_reaction(symbol, analysis_date, 'outside_supported_session')
        current_open = today.iloc[0]['market_open'].to_pydatetime()
        current_close = today.iloc[0]['market_close'].to_pydatetime()
        if current_open <= now < current_close:
            return unavailable_reaction(symbol, analysis_date, 'regular_session_in_progress')
        expected_close = completed.iloc[-1]['market_close'].to_pydatetime()
    except Exception:
        return unavailable_reaction(symbol, analysis_date, 'session_calendar_unavailable')
    try:
        info = yf.Ticker(symbol).get_info()
        if not isinstance(info, dict):
            return unavailable_reaction(symbol, analysis_date, 'missing_metadata')
    except Exception:
        return unavailable_reaction(symbol, analysis_date, 'provider_error')
    if info.get('exchange') not in _US_EXCHANGES or info.get('exchangeTimezoneName') != 'America/New_York':
        return unavailable_reaction(symbol, analysis_date, 'unsupported_exchange', status='unsupported')
    currency = info.get('currency')
    if currency != 'USD':
        return unavailable_reaction(symbol, analysis_date, 'unsupported_currency', status='unsupported')
    regular = _price(info.get('regularMarketPrice'))
    regular_time = _timestamp(info.get('regularMarketTime'))
    if regular is None or regular_time is None or regular_time > now:
        return unavailable_reaction(symbol, analysis_date, 'invalid_regular_reference')
    zone = ZoneInfo('America/New_York')
    regular_local = regular_time.astimezone(zone)
    if (regular_local.date() != expected_close.astimezone(zone).date()
            or abs(regular_time - expected_close) > CLOSE_TIMESTAMP_TOLERANCE):
        return unavailable_reaction(symbol, analysis_date, 'invalid_regular_reference')
    candidates = []
    for prefix, quote_type in [('pre', 'pre_market'), ('post', 'post_market')]:
        price = _price(info.get(prefix + 'MarketPrice'))
        stamp = _timestamp(info.get(prefix + 'MarketTime'))
        if price is None or stamp is None or not regular_time < stamp <= now or now - stamp > MAX_QUOTE_AGE:
            continue
        local = stamp.astimezone(zone)
        if local.weekday() >= 5:
            continue
        if prefix == 'pre':
            valid = (local.date() == requested and time(4) <= local.time()
                     and stamp < current_open and regular_local.date() < local.date())
        else:
            # Four hours after the actual close also bounds half-day post sessions.
            valid = (local.date() == regular_local.date()
                     and expected_close <= stamp <= expected_close + timedelta(hours=4))
        if valid:
            candidates.append((stamp, price, quote_type))
    if not candidates:
        return unavailable_reaction(symbol, analysis_date, 'no_recent_extended_hours_quote')
    stamp, price, quote_type = max(candidates)
    return {'symbol': symbol, 'analysis_date': analysis_date, 'status': 'available', 'quote_type': quote_type,
                'observed_at': stamp.isoformat(), 'observed_at_exchange': stamp.astimezone(zone).isoformat(),
                'regular_close': regular, 'regular_observed_at': regular_time.isoformat(),
                'expected_regular_close_at': expected_close.isoformat(),
                'price': price, 'change_pct': (price / regular - 1) * 100, 'currency': currency,
                'exchange_timezone': str(zone), 'exchange': info['exchange'], 'provider': 'yfinance',
                'fetched_at': now.isoformat(), 'age_seconds': (now - stamp).total_seconds(), 'note': _NOTE}


_REASON_TEXT = {
    "not_captured_in_saved_run": "the quote was not recorded in the saved run",
    "not_collected": "no extended-hours quote was collected",
    "historical_analysis": "current quotes are excluded from historical analysis",
    "not_current_exchange_date": "this is not a current exchange-session analysis",
    "outside_supported_session": "the requested date is outside a supported trading session",
    "unsupported_exchange": "extended-hours coverage is unavailable for this exchange",
    "unsupported_currency": "the quote currency is outside the supported coverage",
    "provider_error": "the data provider could not supply a verified quote",
    "invalid_regular_reference": "the regular-session reference could not be verified",
    "no_recent_extended_hours_quote": "no sufficiently recent, verified extended-hours quote was supplied",
    "missing_metadata": "the data provider omitted required quote metadata",
}


def render_market_reaction(record):
    """Stable text for prompts and reports; unavailable reasons contain no provider errors."""
    if not isinstance(record, dict) or record.get('status') != 'available':
        return ('Extended-hours market reaction: unavailable (' +
                _REASON_TEXT.get((record or {}).get('reason', 'not_collected'), 'required quote or session details could not be verified') + '). ' + _NOTE)
    return (f"Extended-hours market reaction: {record['quote_type'].replace('_', ' ')} "
            f"{record['price']:.2f} {record['currency']} at {record['observed_at']} "
            f"({record['change_pct']:+.2f}% versus regular-session reference "
            f"{record['regular_close']:.2f} at {record['regular_observed_at']}). "
            f"Source: {record['provider']}; retrieved {record['fetched_at']}; "
            f"quote age {record['age_seconds'] / 60:.0f} minutes. {_NOTE}")
