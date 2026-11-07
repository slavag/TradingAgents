"""Offline contracts for the cached Discover shortlist."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from importlib import import_module
from threading import Event

import pandas as pd
import pytest

from tradingagents.web.momentum_screen import DailyBar, EquityIdentity


def module():
    return import_module("tradingagents.web.momentum_discovery")


def evidence(symbols=("AMD",)):
    days = [stamp.date() for stamp in pd.bdate_range(end="2026-10-15", periods=50)]
    histories = {
        symbol: tuple(DailyBar(day, 100 + index, 100 + index, 1_000_000)
                      for index, day in enumerate(days))
        for symbol in symbols
    }
    identities = {
        symbol: EquityIdentity(symbol, symbol + " Inc.", "NMS", "USD", "EQUITY", True)
        for symbol in symbols
    }
    return histories, identities, []


@pytest.fixture
def offline(monkeypatch, tmp_path):
    discovery = module()
    discovery._reset_cache_for_tests()
    monkeypatch.setattr(discovery, "RESULTS_ROOT", tmp_path)
    monkeypatch.setattr(discovery, "saved_jobs", lambda root: {})
    monkeypatch.setattr(discovery, "fetch_source_membership", lambda: ({"AMD": {"apewisdom"}}, []))
    monkeypatch.setattr(discovery, "fetch_market_evidence", lambda symbols: evidence())
    monkeypatch.setattr(discovery, "_utcnow", lambda: datetime(2026, 10, 16, 12, tzinfo=timezone.utc))
    clock = [1000.0]
    monkeypatch.setattr(discovery, "_monotonic", lambda: clock[0])
    yield discovery, clock
    discovery._reset_cache_for_tests()


def test_only_15_or_20_and_no_padding(offline):
    discovery, _ = offline
    with pytest.raises(ValueError):
        discovery.get_momentum_watchlist(limit=16)
    result = discovery.get_momentum_watchlist(limit=15)
    assert result["status"] == "fresh"
    assert [item["ticker"] for item in result["items"]] == ["AMD"]
    assert result["retrieved_at"] == "2026-10-16T12:00:00+00:00"
    assert result["quote_as_of"] == "2026-10-15"


def test_100_symbol_market_budget_preserves_movers_and_attention(monkeypatch, offline):
    discovery, _ = offline
    attention = {f"A{number:03d}" for number in range(20)}
    membership = {f"M{number:03d}": {"alpha_vantage_gainers"} for number in range(120)}
    membership.update({symbol: {"apewisdom"} for symbol in sorted(attention)})
    membership["DUAL"] = {"alpha_vantage_gainers", "stocktwits"}
    monkeypatch.setattr(discovery, "fetch_source_membership", lambda: (membership, []))
    selected = []

    def fetch(symbols):
        selected.extend(symbols)
        return {}, {}, []

    monkeypatch.setattr(discovery, "fetch_market_evidence", fetch)
    discovery.get_momentum_watchlist()
    assert len(selected) == len(set(selected)) == 100
    assert "DUAL" in selected
    assert attention <= set(selected)
    assert any(symbol.startswith("M") for symbol in selected)


def test_empty_screen_distinguishes_verified_stocks_from_data_gaps(monkeypatch, offline):
    discovery, _ = offline
    histories, identities, _ = evidence()
    thin_history = {
        symbol: tuple(DailyBar(bar.date, bar.adjusted_close, bar.unadjusted_close, 1)
                      for bar in bars)
        for symbol, bars in histories.items()
    }
    monkeypatch.setattr(discovery, "fetch_market_evidence",
                        lambda symbols: (thin_history, {}, []))
    result = discovery.get_momentum_watchlist()
    assert result["status"] == "fresh"
    assert result["items"] == []
    assert result["screening"] == {
        "nominated": 1, "selected_for_prices": 1, "with_history": 1, "price_eligible": 0,
        "identity_checked": 0, "verified_equities": 0, "qualified": 0,
    }


def test_scores_cached_and_15_20_limits_share_snapshot(monkeypatch, offline):
    discovery, clock = offline
    symbols = tuple(f"T{number:03d}" for number in range(22))
    calls = []
    monkeypatch.setattr(discovery, "fetch_source_membership",
                        lambda: ({symbol: {"apewisdom"} for symbol in symbols}, []))

    def fetch(symbols):
        calls.append(tuple(symbols))
        return evidence(symbols)

    monkeypatch.setattr(discovery, "fetch_market_evidence", fetch)
    assert len(discovery.get_momentum_watchlist(limit=20)["items"]) == 20
    assert len(discovery.get_momentum_watchlist(limit=15)["items"]) == 15
    assert len(calls) == 1
    clock[0] += 31 * 60
    assert len(discovery.get_momentum_watchlist(limit=20)["items"]) == 20
    assert len(calls) == 2


def test_refresh_bypasses_cache_but_has_cooldown(monkeypatch, offline):
    discovery, clock = offline
    calls = []

    def fetch(symbols):
        calls.append(1)
        return evidence()

    monkeypatch.setattr(discovery, "fetch_market_evidence", fetch)
    discovery.get_momentum_watchlist()
    assert discovery.get_momentum_watchlist(force=True)["warnings"] == ["refresh_cooldown"]
    assert len(calls) == 1
    clock[0] += 61
    discovery.get_momentum_watchlist(force=True)
    assert len(calls) == 2


def test_failed_refresh_keeps_old_timestamp_and_only_safe_warning(monkeypatch, offline):
    discovery, clock = offline
    first = discovery.get_momentum_watchlist()
    clock[0] += 61
    secret = "private-test-api-key"

    def fail():
        raise RuntimeError(secret)

    monkeypatch.setattr(discovery, "fetch_source_membership", fail)
    stale = discovery.get_momentum_watchlist(force=True)
    assert stale["status"] == "stale"
    assert stale["retrieved_at"] == first["retrieved_at"]
    assert stale["items"] == first["items"]
    assert stale["warnings"] == ["refresh_failed"]
    assert secret not in repr(stale)


def test_stale_snapshot_preserves_its_qualified_items_after_quote_ages(monkeypatch, offline):
    discovery, clock = offline
    first = discovery.get_momentum_watchlist()
    clock[0] += 10 * 24 * 60 * 60
    monkeypatch.setattr(discovery, "_utcnow",
                        lambda: datetime(2026, 10, 26, 12, tzinfo=timezone.utc))

    def fail():
        raise RuntimeError("provider error")

    monkeypatch.setattr(discovery, "fetch_source_membership", fail)
    stale = discovery.get_momentum_watchlist(force=True)
    assert stale["status"] == "stale"
    assert stale["quote_as_of"] == first["quote_as_of"]
    assert stale["items"] == first["items"]


def test_failed_refresh_stays_stale_through_cooldown_and_cache(monkeypatch, offline):
    discovery, clock = offline
    first = discovery.get_momentum_watchlist()
    clock[0] += 61
    calls = []

    def fail():
        calls.append(1)
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(discovery, "fetch_source_membership", fail)
    assert discovery.get_momentum_watchlist(force=True)["status"] == "stale"
    cooldown = discovery.get_momentum_watchlist(force=True)
    cached = discovery.get_momentum_watchlist()
    assert cooldown["status"] == cached["status"] == "stale"
    assert cooldown["items"] == cached["items"] == first["items"]
    assert len(calls) == 1


def test_identity_provider_outage_retains_snapshot_or_reports_unavailable(monkeypatch, offline):
    discovery, clock = offline
    monkeypatch.setattr(discovery, "fetch_market_evidence",
                        lambda symbols: (evidence()[0], {}, ["identity_unavailable"]))
    unavailable = discovery.get_momentum_watchlist()
    assert unavailable["status"] == "unavailable"
    assert unavailable["items"] == []
    discovery._reset_cache_for_tests()
    monkeypatch.setattr(discovery, "fetch_market_evidence", lambda symbols: evidence())
    first = discovery.get_momentum_watchlist()
    clock[0] += 61
    monkeypatch.setattr(discovery, "fetch_market_evidence",
                        lambda symbols: (evidence()[0], {}, ["identity_unavailable"]))
    stale = discovery.get_momentum_watchlist(force=True)
    assert stale["status"] == "stale"
    assert stale["items"] == first["items"]


def test_coalesced_failed_refresh_keeps_both_callers_stale(monkeypatch, offline):
    discovery, clock = offline
    discovery.get_momentum_watchlist()
    clock[0] += 61
    entered, release = Event(), Event()

    def fail():
        entered.set()
        assert release.wait(5)
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(discovery, "fetch_source_membership", fail)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(discovery.get_momentum_watchlist, force=True)
        assert entered.wait(5)
        second = pool.submit(discovery.get_momentum_watchlist, force=True)
        release.set()
        assert first.result(5)["status"] == "stale"
        assert second.result(5)["status"] == "stale"


def test_first_fetch_failure_unavailable_without_fake_items(monkeypatch, offline):
    discovery, _ = offline
    monkeypatch.setattr(discovery, "fetch_market_evidence",
                        lambda symbols: ({}, {}, ["market_data_unavailable"]))
    result = discovery.get_momentum_watchlist()
    assert result["status"] == "unavailable"
    assert result["items"] == []
    assert result["retrieved_at"] is None
    assert result["warnings"] == ["market_data_unavailable"]


def test_partial_feed_failure_retains_qualified_items(monkeypatch, offline):
    discovery, _ = offline
    monkeypatch.setattr(discovery, "fetch_source_membership",
                        lambda: ({"AMD": {"apewisdom"}}, ["fmp_unavailable"]))
    result = discovery.get_momentum_watchlist()
    assert result["status"] == "partial"
    assert result["items"][0]["ticker"] == "AMD"
    assert result["warnings"] == ["fmp_unavailable"]


def test_badges_recomputed_from_successful_saved_reports(monkeypatch, offline, tmp_path):
    discovery, _ = offline
    symbols = ("AMD", "PENG", "NVDA")
    monkeypatch.setattr(discovery, "fetch_source_membership",
                        lambda: ({symbol: {"apewisdom"} for symbol in symbols}, []))
    monkeypatch.setattr(discovery, "fetch_market_evidence", lambda symbols: evidence(symbols))
    reports = {"AMD"}
    jobs = {
        "good": {"id": "a" * 32, "analysis_date": "2026-10-15", "tickers": ["AMD"],
                 "results": [{"ticker": "AMD", "status": "completed"}]},
        "failed": {"id": "b" * 32, "analysis_date": "2026-10-15", "tickers": ["PENG"],
                   "results": [{"ticker": "PENG", "status": "failed"}]},
        "queued": {"id": "c" * 32, "analysis_date": "2026-10-15", "tickers": ["NVDA"],
                   "results": [{"ticker": "NVDA", "status": "queued"}]},
    }
    monkeypatch.setattr(discovery, "saved_jobs", lambda root: jobs)
    monkeypatch.setattr(discovery, "report_path",
                        lambda root, job, ticker, kind: tmp_path / "report.html"
                        if ticker in reports and kind == "investor" else None)
    first = discovery.get_momentum_watchlist()
    assert [item["ticker"] for item in first["items"]] == ["NVDA", "PENG", "AMD"]
    assert [item["already_analyzed"] for item in first["items"]] == [False, False, True]
    reports.add("PENG")
    jobs["failed"]["results"][0]["status"] = "completed"
    second = discovery.get_momentum_watchlist()
    assert [item["ticker"] for item in second["items"]] == ["NVDA", "AMD", "PENG"]
    assert second["retrieved_at"] == first["retrieved_at"]


def test_concurrent_requests_share_one_inflight_refresh(monkeypatch, offline):
    discovery, _ = offline
    entered, release = Event(), Event()
    calls = []

    def fetch():
        calls.append(1)
        entered.set()
        assert release.wait(5)
        return {"AMD": {"apewisdom"}}, []

    monkeypatch.setattr(discovery, "fetch_source_membership", fetch)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(discovery.get_momentum_watchlist)
        assert entered.wait(5)
        second = pool.submit(discovery.get_momentum_watchlist)
        release.set()
        assert first.result(5)["items"] == second.result(5)["items"]
    assert len(calls) == 1
