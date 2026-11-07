"""Pure, dated eligibility and ranking for the Discover momentum watchlist."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date

import pandas_market_calendars as market_calendars

_NYSE = market_calendars.get_calendar("NYSE")
_US_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "NYSE AMERICAN", "NMS", "NYQ", "ASE"}
_MOVER_SOURCES = {
    "alpha_vantage_gainers", "alpha_vantage_active", "fmp_gainers", "fmp_active",
}
_ATTENTION_SOURCES = {"apewisdom", "stocktwits"}
_SYMBOL = re.compile(r"[A-Z][A-Z0-9.\-]{0,5}\Z")


@dataclass(frozen=True)
class DailyBar:
    date: date
    adjusted_close: float | None
    unadjusted_close: float | None
    volume: float | None


@dataclass(frozen=True)
class EquityIdentity:
    symbol: str
    name: str
    exchange: str
    currency: str
    quote_type: str
    active: bool


@dataclass(frozen=True)
class MomentumCandidate:
    ticker: str
    company_name: str
    close: float
    currency: str
    quote_date: date
    price_basis: str
    ret_1d_pct: float
    ret_5d_pct: float
    ret_20d_pct: float
    avg_dollar_volume: float
    relative_volume: float
    trend_checks: int
    score: float
    sources: tuple[str, ...]
    reason: str
    risk_flags: tuple[str, ...]
    already_analyzed: bool


@dataclass(frozen=True)
class PriceSignals:
    last: DailyBar
    avg_dollar_volume: float
    ret_1d_pct: float
    ret_5d_pct: float
    ret_20d_pct: float
    trend_checks: int
    relative_volume: float
    spike: bool


def quote_is_fresh(observed: date, as_of: date) -> bool:
    """Accept at most two NYSE sessions after an observed completed quote."""
    if observed > as_of:
        return False
    sessions = _NYSE.valid_days(start_date=observed, end_date=as_of)
    days = [stamp.date() for stamp in sessions]
    return observed in days and len(days) - 1 <= 2


def _valid_number(value: float | None, *, positive: bool = True) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and (
        math.isfinite(value) and (value > 0 if positive else value >= 0)
    )


def _verified_identity(symbol: str, identity: EquityIdentity | None) -> bool:
    return bool(
        identity is not None
        and identity.symbol.upper() == symbol
        and identity.active
        and identity.quote_type.upper() == "EQUITY"
        and identity.exchange.upper() in _US_EXCHANGES
        and identity.currency.upper() == "USD"
    )


def _corroboration(sources: set[str]) -> int:
    movers = bool(sources & _MOVER_SOURCES)
    attention = bool(sources & _ATTENTION_SOURCES)
    if movers and attention:
        return 2
    return 1 if len(sources) >= 2 else 0


def price_signals(history: Sequence[DailyBar], *, as_of: date) -> PriceSignals | None:
    """Apply the same price/liquidity gates before and after identity lookup."""
    ordered = sorted(history, key=lambda item: item.date)
    if len(ordered) < 50 or len({bar.date for bar in ordered}) != len(ordered):
        return None
    recent = ordered[-50:]
    if any(
        not _valid_number(bar.adjusted_close)
        or not _valid_number(bar.unadjusted_close)
        or not _valid_number(bar.volume, positive=False)
        for bar in recent
    ):
        return None
    last = recent[-1]
    if not quote_is_fresh(last.date, as_of) or last.unadjusted_close < 5:
        return None
    avg_dollar_volume = sum(bar.unadjusted_close * bar.volume for bar in recent[-20:]) / 20
    if avg_dollar_volume < 10_000_000:
        return None
    adjusted = [bar.adjusted_close for bar in recent]
    ret_1d = (adjusted[-1] / adjusted[-2] - 1) * 100
    ret_5d = (adjusted[-1] / adjusted[-6] - 1) * 100
    ret_20d = (adjusted[-1] / adjusted[-21] - 1) * 100
    if ret_5d <= 0:
        return None
    sma20 = sum(adjusted[-20:]) / 20
    sma50 = sum(adjusted) / 50
    trend_checks = int(adjusted[-1] > sma20) + int(sma20 > sma50)
    prior_volume = sum(bar.volume for bar in recent[-21:-1]) / 20
    if prior_volume <= 0:
        return None
    return PriceSignals(
        last=last, avg_dollar_volume=avg_dollar_volume,
        ret_1d_pct=ret_1d, ret_5d_pct=ret_5d, ret_20d_pct=ret_20d,
        trend_checks=trend_checks, relative_volume=last.volume / prior_volume,
        spike=ret_1d > 25,
    )


def price_priority(signals: PriceSignals) -> float:
    """Signal-only score for spending the bounded identity lookup budget."""
    return (
        min(max(signals.ret_5d_pct, 0), 20) / 4
        + min(max(signals.ret_20d_pct, 0), 24) / 8
        + signals.trend_checks
        + min(max(signals.relative_volume - 1, 0), 2)
        - (2 if signals.spike else 0)
    )


def screen_candidates(
    source_membership: Mapping[str, set[str]],
    histories: Mapping[str, Sequence[DailyBar]],
    identities: Mapping[str, EquityIdentity],
    *,
    as_of: date,
    analyzed: set[str],
) -> list[MomentumCandidate]:
    """Return qualified research ideas; no quote or feed call occurs here."""
    found: list[MomentumCandidate] = []
    for symbol, raw_sources in source_membership.items():
        if not _SYMBOL.fullmatch(symbol) or not _verified_identity(symbol, identities.get(symbol)):
            continue
        sources = set(raw_sources) & (_MOVER_SOURCES | _ATTENTION_SOURCES)
        if not sources:
            continue
        signals = price_signals(histories.get(symbol, ()), as_of=as_of)
        if signals is None:
            continue
        score = max(0.0, round(
            price_priority(signals) + _corroboration(sources),
            2,
        ))
        reason = (
            f"Five-session move {signals.ret_5d_pct:+.1f}%; "
            f"{signals.trend_checks} of 2 trend checks pass; "
            f"latest volume is {signals.relative_volume:.1f} times its prior 20-session average. "
            f"Nominated by {len(sources)} independent feed{'s' if len(sources) != 1 else ''}."
        )
        found.append(MomentumCandidate(
            ticker=symbol, company_name=identities[symbol].name or symbol,
            close=round(signals.last.unadjusted_close, 2), currency="USD",
            quote_date=signals.last.date,
            price_basis="unadjusted close; split-adjusted returns",
            ret_1d_pct=round(signals.ret_1d_pct, 2),
            ret_5d_pct=round(signals.ret_5d_pct, 2),
            ret_20d_pct=round(signals.ret_20d_pct, 2),
            avg_dollar_volume=round(signals.avg_dollar_volume, 2),
            relative_volume=round(signals.relative_volume, 2),
            trend_checks=signals.trend_checks, score=score, sources=tuple(sorted(sources)),
            reason=reason,
            risk_flags=("high_volatility_spike",) if signals.spike else (),
            already_analyzed=symbol in analyzed,
        ))
    return sorted(found, key=lambda item: (item.already_analyzed, -item.score, item.ticker))
