"""Cached, research-only momentum watchlist for the local Discover view."""

from __future__ import annotations

import threading
import time
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from tradingagents.web.history import report_path, saved_jobs
from tradingagents.web.momentum_screen import (
    MomentumCandidate,
    price_signals,
    screen_candidates,
)
from tradingagents.web.momentum_sources import fetch_market_evidence, fetch_source_membership
from tradingagents.web.service import RESULTS_ROOT

_CACHE_TTL_SECONDS = 30 * 60
_REFRESH_COOLDOWN_SECONDS = 60
_WAIT_SECONDS = 35
_MARKET_SYMBOL_LIMIT = 100
_MOVER_SOURCES = {"alpha_vantage_gainers", "alpha_vantage_active", "fmp_gainers", "fmp_active"}
_ATTENTION_SOURCES = {"apewisdom", "stocktwits"}
_SAFE_WARNINGS = {
    "alpha_vantage_unavailable", "alpha_vantage_unconfigured", "fmp_unavailable",
    "apewisdom_unavailable", "stocktwits_unavailable", "market_data_unavailable",
    "identity_unavailable", "refresh_failed", "refresh_cooldown",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _monotonic() -> float:
    return time.monotonic()


@dataclass(frozen=True)
class _Snapshot:
    candidates: tuple[MomentumCandidate, ...]
    screening: dict[str, int]
    quote_as_of: str | None
    warnings: tuple[str, ...]
    retrieved_at: str
    created_monotonic: float


_condition = threading.Condition()
_snapshot: _Snapshot | None = None
_last_attempt: float | None = None
_refreshing = False
_stale = False
_last_failure_warning: str | None = None


def _reset_cache_for_tests() -> None:
    global _snapshot, _last_attempt, _refreshing, _stale, _last_failure_warning
    with _condition:
        _snapshot = None
        _last_attempt = None
        _refreshing = False
        _stale = False
        _last_failure_warning = None
        _condition.notify_all()


def _safe_warnings(values: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(value for value in values if value in _SAFE_WARNINGS))


def _analyzed_report_links(root: Path) -> dict[str, str]:
    links: dict[str, str] = {}
    jobs = saved_jobs(root)
    for job in sorted(jobs.values(), key=lambda row: row.get("created_at", ""), reverse=True):
        for row in job.get("results", []):
            if not isinstance(row, dict) or row.get("status") != "completed":
                continue
            symbol = row.get("ticker")
            if not isinstance(symbol, str) or symbol in links:
                continue
            for kind in ("investor", "full"):
                if report_path(root, job, symbol, kind) is not None:
                    links[symbol] = (
                        f"/api/jobs/{quote(job['id'], safe='')}/reports/"
                        f"{quote(symbol, safe='')}/{kind}"
                    )
                    break
    return links


def _render(
    snapshot: _Snapshot | None, *, limit: int, status: str,
    extra_warnings: tuple[str, ...] = (),
) -> dict:
    if snapshot is None:
        return {
            "status": "unavailable", "retrieved_at": None, "quote_as_of": None,
            "items": [], "screening": None,
            "provider_status": {"feeds": "unavailable", "market": "unavailable"},
            "warnings": list(_safe_warnings(extra_warnings or ("refresh_failed",))),
        }
    try:
        links = _analyzed_report_links(RESULTS_ROOT)
    except (OSError, ValueError):
        links = {}
    candidates = sorted(
        (replace(candidate, already_analyzed=candidate.ticker in links)
         for candidate in snapshot.candidates),
        key=lambda item: (item.already_analyzed, -item.score, item.ticker),
    )
    items = []
    for candidate in candidates[:limit]:
        item = asdict(candidate)
        item["quote_date"] = candidate.quote_date.isoformat()
        if candidate.ticker in links:
            item["research_url"] = links[candidate.ticker]
        items.append(item)
    warnings = list(snapshot.warnings)
    warnings.extend(extra_warnings)
    return {
        "status": status,
        "retrieved_at": snapshot.retrieved_at,
        "quote_as_of": snapshot.quote_as_of,
        "items": items,
        "screening": snapshot.screening,
        "provider_status": {
            "feeds": "partial" if any(name.endswith("unavailable") for name in snapshot.warnings) else "available",
            "market": "partial" if "identity_unavailable" in snapshot.warnings else "available",
        },
        "warnings": list(_safe_warnings(warnings)),
    }


def _status(snapshot: _Snapshot | None) -> str:
    if snapshot is None:
        return "unavailable"
    if _stale:
        return "stale"
    return "partial" if snapshot.warnings else "fresh"


def _failure_warnings() -> tuple[str, ...]:
    return (_last_failure_warning,) if _last_failure_warning else ()


def _selected_market_symbols(membership: dict[str, set[str]]) -> list[str]:
    """Spend the bounded price budget across corroborated, mover and attention names."""
    shared: list[str] = []
    movers: list[str] = []
    attention: list[str] = []
    for symbol, sources in membership.items():
        has_mover = bool(sources & _MOVER_SOURCES)
        has_attention = bool(sources & _ATTENTION_SOURCES)
        if has_mover and has_attention:
            shared.append(symbol)
        elif has_mover:
            movers.append(symbol)
        elif has_attention:
            attention.append(symbol)
    chosen = shared[:_MARKET_SYMBOL_LIMIT]
    mover_index = attention_index = 0
    while len(chosen) < _MARKET_SYMBOL_LIMIT and (
        mover_index < len(movers) or attention_index < len(attention)
    ):
        if mover_index < len(movers):
            chosen.append(movers[mover_index])
            mover_index += 1
        if len(chosen) < _MARKET_SYMBOL_LIMIT and attention_index < len(attention):
            chosen.append(attention[attention_index])
            attention_index += 1
    return chosen


def _fetch_snapshot(now: float) -> _Snapshot | None:
    membership, feed_warnings = fetch_source_membership()
    selected_symbols = _selected_market_symbols(membership)
    if not membership:
        if feed_warnings:
            return None
        histories, identities, market_warnings = {}, {}, []
    else:
        histories, identities, market_warnings = fetch_market_evidence(selected_symbols)
        if not histories and market_warnings:
            return None
        if not identities and "identity_unavailable" in market_warnings:
            return None
    candidates = tuple(screen_candidates(
        membership, histories, identities, as_of=_utcnow().date(), analyzed=set(),
    ))
    price_eligible = sum(
        price_signals(bars, as_of=_utcnow().date()) is not None
        for bars in histories.values()
    )
    return _Snapshot(
        candidates=candidates,
        screening={
            "nominated": len(membership),
            "selected_for_prices": len(selected_symbols),
            "with_history": len(histories),
            "price_eligible": price_eligible,
            "identity_checked": min(price_eligible, 40),
            "verified_equities": len(identities),
            "qualified": len(candidates),
        },
        quote_as_of=max((item.quote_date.isoformat() for item in candidates), default=None),
        warnings=_safe_warnings(feed_warnings + market_warnings),
        retrieved_at=_utcnow().isoformat(), created_monotonic=now,
    )


def get_momentum_watchlist(*, limit: Literal[15, 20] = 20, force: bool = False) -> dict:
    """Return 15 or 20 bounded ideas; badges are re-read on every request."""
    global _snapshot, _last_attempt, _refreshing, _stale, _last_failure_warning
    if limit not in (15, 20) or isinstance(limit, bool):
        raise ValueError("limit must be 15 or 20")
    with _condition:
        now = _monotonic()
        if _refreshing:
            _condition.wait_for(lambda: not _refreshing, timeout=_WAIT_SECONDS)
            if _refreshing:
                return _render(_snapshot, limit=limit, status="stale", extra_warnings=("refresh_failed",))
            return _render(_snapshot, limit=limit, status=_status(_snapshot), extra_warnings=_failure_warnings())
        if force and _last_attempt is not None and now - _last_attempt < _REFRESH_COOLDOWN_SECONDS:
            return _render(
                _snapshot, limit=limit, status=_status(_snapshot),
                extra_warnings=(*_failure_warnings(), "refresh_cooldown"),
            )
        if not force and _snapshot is not None and now - _snapshot.created_monotonic < _CACHE_TTL_SECONDS:
            return _render(_snapshot, limit=limit, status=_status(_snapshot), extra_warnings=_failure_warnings())
        if not force and _stale and _last_attempt is not None and now - _last_attempt < _REFRESH_COOLDOWN_SECONDS:
            return _render(_snapshot, limit=limit, status="stale", extra_warnings=_failure_warnings())
        _last_attempt = now
        _refreshing = True
    failed_warning = "refresh_failed"
    try:
        fresh = _fetch_snapshot(now)
        if fresh is None:
            failed_warning = "market_data_unavailable"
    except Exception:  # External providers may raise arbitrary errors; never expose their details.
        fresh = None
    with _condition:
        if fresh is not None:
            _snapshot = fresh
            _stale = False
            _last_failure_warning = None
        else:
            _stale = _snapshot is not None
            _last_failure_warning = failed_warning
        _refreshing = False
        _condition.notify_all()
        snapshot = _snapshot
    if fresh is None:
        return _render(snapshot, limit=limit, status=_status(snapshot), extra_warnings=_failure_warnings())
    return _render(snapshot, limit=limit, status=_status(snapshot))
