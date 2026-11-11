"""Same-day OHLCV cache must not serve a stale snapshot all day (#1150).

The cache file is keyed per day, so a run started before the day's bar was final
would be reused by every later run, feeding a stale close into technical
analysis. Two cases matter for a current-day request: the bar may be missing, or
present but still in progress (Yahoo publishes a partial daily candle intraday).
Refresh is bounded by a TTL so repeated runs cannot hammer the vendor.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

import tradingagents.dataflows.stockstats_utils as su

TODAY = pd.Timestamp("2026-07-18 14:00:00")
STALE = su.OHLCV_CACHE_TTL_SECONDS + 60


def _local_epoch(timestamp):
    return timestamp.to_pydatetime().astimezone().timestamp()


def _write(tmp_path, name="cache.csv", age_seconds=0.0, last_date="2026-07-17"):
    f = tmp_path / name
    pd.DataFrame({"Date": [last_date], "Close": [1.0]}).to_csv(f, index=False)
    written = _local_epoch(TODAY) - age_seconds
    os.utime(f, (written, written))
    return str(f)


@pytest.mark.unit
def test_current_day_cache_past_ttl_is_refreshed(tmp_path):
    # Bar missing (rows stop at yesterday) and file older than the TTL -> refetch.
    assert su._cache_is_fresh(_write(tmp_path, age_seconds=STALE), TODAY, TODAY) is False


@pytest.mark.unit
def test_partial_current_day_bar_is_still_refreshed(tmp_path):
    # Today's row is present but may be an in-progress candle whose Close is not
    # the closing price. Row inspection can't distinguish it, so the TTL governs.
    f = _write(tmp_path, age_seconds=STALE, last_date="2026-07-18")
    assert su._cache_is_fresh(f, TODAY, TODAY) is False


@pytest.mark.unit
def test_recent_cache_is_not_refetched(tmp_path):
    # Written moments ago: don't hammer the vendor (weekend/holiday guard).
    assert su._cache_is_fresh(_write(tmp_path), TODAY, TODAY) is True


@pytest.mark.unit
def test_historical_request_uses_same_day_cache(tmp_path):
    # Past dates reuse today's download, even when the live-bar TTL has expired.
    past = pd.Timestamp("2026-05-01")
    f = _write(tmp_path, age_seconds=STALE, last_date="2026-04-30")
    assert su._cache_is_fresh(f, past, TODAY) is True


def test_yesterdays_symbol_cache_is_refetched_for_historical_request(tmp_path):
    past = pd.Timestamp("2026-05-01")
    f = _write(tmp_path, age_seconds=24 * 3600, last_date="2026-04-30")
    assert su._cache_is_fresh(f, past, TODAY) is False


@pytest.mark.unit
def test_load_ohlcv_refetches_stale_same_day_cache(tmp_path, monkeypatch):
    """End-to-end: the helper is actually wired into load_ohlcv's cache branch.

    Without this, the unit tests above would still pass if the helper were never
    called from the real code path.
    """
    monkeypatch.setattr(su, "get_config", lambda: {"data_cache_dir": str(tmp_path)})
    monkeypatch.setattr(su.pd.Timestamp, "today", staticmethod(lambda: TODAY))

    # Pre-seed the cache file load_ohlcv will look for, aged past the TTL.
    cache_file = tmp_path / "AAPL-YFin-data.csv"
    pd.DataFrame({"Date": ["2026-07-17"], "Close": [100.0]}).to_csv(cache_file, index=False)
    old = _local_epoch(TODAY) - STALE
    os.utime(cache_file, (old, old))

    calls = []

    def _fake_download(*a, **k):
        calls.append(1)
        return pd.DataFrame(
            {"Date": pd.to_datetime(["2026-07-17", "2026-07-18"]), "Close": [100.0, 222.0]}
        ).set_index("Date")

    monkeypatch.setattr(su.yf, "download", _fake_download)

    out = su.load_ohlcv("AAPL", TODAY.strftime("%Y-%m-%d"))

    assert calls, "stale same-day cache must trigger a refetch"
    assert 222.0 in out["Close"].values, "refreshed close must reach the caller"


@pytest.mark.unit
def test_load_ohlcv_reuses_fresh_same_day_cache(tmp_path, monkeypatch):
    # Mirror image: a fresh cache must NOT trigger a download.
    monkeypatch.setattr(su, "get_config", lambda: {"data_cache_dir": str(tmp_path)})
    monkeypatch.setattr(su.pd.Timestamp, "today", staticmethod(lambda: TODAY))

    cache_file = tmp_path / "AAPL-YFin-data.csv"
    pd.DataFrame({"Date": ["2026-07-18"], "Close": [100.0]}).to_csv(cache_file, index=False)
    os.utime(cache_file, (_local_epoch(TODAY), _local_epoch(TODAY)))

    def _fail_download(*a, **k):
        raise AssertionError("fresh cache must not refetch")

    monkeypatch.setattr(su.yf, "download", _fail_download)
    su.load_ohlcv("AAPL", TODAY.strftime("%Y-%m-%d"))


def test_ohlcv_cache_keeps_one_symbol_file_across_days(tmp_path, monkeypatch):
    monkeypatch.setattr(su, "get_config", lambda: {"data_cache_dir": str(tmp_path)})
    clock = {"now": pd.Timestamp("2026-07-18 14:00:00")}
    monkeypatch.setattr(su.pd.Timestamp, "today", staticmethod(lambda: clock["now"]))
    calls = []

    def download(*_args, **_kwargs):
        calls.append(clock["now"].date())
        return pd.DataFrame(
            {"Close": [100.0, 102.0]},
            index=pd.to_datetime(["2026-07-17", "2026-07-18"]),
        )

    monkeypatch.setattr(su.yf, "download", download)
    su.load_ohlcv("AAPL", "2026-07-18")
    clock["now"] = pd.Timestamp("2026-07-19 14:00:00")
    su.load_ohlcv("AAPL", "2026-07-18")

    assert len(calls) == 2
    assert [p.name for p in tmp_path.glob("AAPL-YFin-data*.csv")] == [
        "AAPL-YFin-data.csv"
    ]


def test_interrupted_cache_write_preserves_previous_complete_download(tmp_path, monkeypatch):
    monkeypatch.setattr(su, "get_config", lambda: {"data_cache_dir": str(tmp_path)})
    monkeypatch.setattr(su.pd.Timestamp, "today", staticmethod(lambda: TODAY))
    cache_file = tmp_path / "AAPL-YFin-data.csv"
    cache_file.write_text("Date,Close\n2026-07-17,100\n", encoding="utf-8")
    old = _local_epoch(TODAY) - 24 * 3600
    os.utime(cache_file, (old, old))

    monkeypatch.setattr(
        su.yf, "download",
        lambda *_args, **_kwargs: pd.DataFrame(
            {"Close": [102.0]}, index=pd.to_datetime(["2026-07-18"])
        ),
    )

    def interrupted_write(self, path, **_kwargs):
        Path(path).write_text("partial", encoding="utf-8")
        raise OSError("write interrupted")

    monkeypatch.setattr(pd.DataFrame, "to_csv", interrupted_write)
    with pytest.raises(OSError, match="interrupted"):
        su.load_ohlcv("AAPL", "2026-07-18")
    assert cache_file.read_text(encoding="utf-8") == "Date,Close\n2026-07-17,100\n"
