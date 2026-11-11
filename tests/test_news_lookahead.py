"""yfinance news must not leak future-dated (or undated, in a backtest) articles
into a historical window.

Regressions for #992 (flat articles bypassed the date filter), #1007 (global
news injected future articles), #993 (empty-after-filter returned a blank body),
and #1126 (inclusive upper bound leaked the midnight-after article; host-local
timestamp parsing made filtering machine-dependent).
"""
from datetime import datetime, timezone

import pytest

import tradingagents.dataflows.yfinance_news as ynews
from tradingagents.dataflows import interface
from tradingagents.dataflows.errors import VendorUnavailableError


def _epoch(date_str):
    """Epoch seconds for UTC midnight of ``date_str`` (host-timezone independent)."""
    return int(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


@pytest.mark.unit
def test_flat_article_publish_time_is_parsed():
    # #992: flat articles now carry a pub_date (was always None -> unfilterable).
    # #1126: parsed as UTC-aware, so the date can't shift with the host timezone.
    data = ynews._extract_article_data(
        {"title": "X", "publisher": "P", "link": "l", "providerPublishTime": _epoch("2025-05-09")}
    )
    assert data["pub_date"] is not None
    assert data["pub_date"].tzinfo is not None
    assert data["pub_date"] == datetime(2025, 5, 9, tzinfo=timezone.utc)


@pytest.mark.unit
def test_window_excludes_future_and_undated_in_backtest():
    start = datetime(2025, 5, 1)
    end = datetime(2025, 5, 9)  # historical window (well in the past)
    inside = datetime(2025, 5, 5)
    future = datetime(2025, 6, 1)
    assert ynews._in_news_window(inside, start, end) is True
    assert ynews._in_news_window(future, start, end) is False     # look-ahead blocked
    assert ynews._in_news_window(None, start, end) is False        # undated -> excluded in backtest


@pytest.mark.unit
def test_window_keeps_undated_in_live_window():
    # Live window (reaches today): undated articles can't be "future", so keep them.
    now = datetime.now(timezone.utc)
    assert ynews._in_news_window(None, now, now) is True


@pytest.mark.unit
def test_upper_bound_is_exclusive():
    # #1126: an article stamped exactly midnight AFTER end_date leaked in under
    # the old inclusive bound; the whole of end_date itself must still be kept.
    start = datetime(2025, 5, 1)
    end = datetime(2025, 5, 9)
    midnight_after = datetime(2025, 5, 10, 0, 0, 0, tzinfo=timezone.utc)
    last_moment = datetime(2025, 5, 9, 23, 59, 59, tzinfo=timezone.utc)
    assert ynews._in_news_window(midnight_after, start, end) is False
    assert ynews._in_news_window(last_moment, start, end) is True


@pytest.mark.unit
def test_offset_aware_timestamp_is_converted_not_truncated():
    # #1126: 2025-05-10T01:00+05:00 is really 2025-05-09T20:00Z -> inside the
    # window. Stripping tzinfo (old behavior) misread it as 05-10 and dropped it.
    start = datetime(2025, 5, 1)
    end = datetime(2025, 5, 9)
    aware = datetime.fromisoformat("2025-05-10T01:00:00+05:00")
    assert ynews._in_news_window(aware, start, end) is True


def test_yahoo_news_published_date_matches_utc_window(monkeypatch):
    article = {"content": {
        "title": "NVDA export update", "summary": "Company update",
        "provider": {"displayName": "Source"},
        "canonicalUrl": {"url": "https://example.test/update"},
        "pubDate": "2025-05-10T01:00:00+05:00",
    }}

    class FakeTicker:
        def __init__(self, symbol):
            assert symbol == "NVDA"

        def get_news(self, count):
            return [article]

    monkeypatch.setattr(ynews.yf, "Ticker", FakeTicker)
    output = ynews.get_news_yfinance("NVDA", "2025-05-09", "2025-05-09")
    assert "published: 2025-05-09" in output
    assert "Link: https://example.test/update" in output


@pytest.mark.unit
def test_global_news_future_flat_article_excluded(monkeypatch):
    # #1007: a flat, future-dated global article must not appear in a historical run.
    future_article = {"title": "FUTURE EVENT", "publisher": "P", "link": "l",
                      "providerPublishTime": _epoch("2025-06-01")}
    past_article = {"title": "PAST EVENT", "publisher": "P", "link": "l",
                    "providerPublishTime": _epoch("2025-05-05")}

    class FakeSearch:
        def __init__(self, *a, **k):
            self.news = [future_article, past_article]

    monkeypatch.setattr(ynews.yf, "Search", FakeSearch)
    out = ynews.get_global_news_yfinance("2025-05-09", look_back_days=7, limit=10)
    assert "PAST EVENT" in out
    assert "FUTURE EVENT" not in out  # #1007


@pytest.mark.unit
def test_global_news_empty_after_filter_is_informative(monkeypatch):
    # #993: everything filtered out -> a clear message, not a blank-bodied report.
    only_future = {"title": "FUTURE", "publisher": "P", "link": "l",
                   "providerPublishTime": _epoch("2025-06-01")}

    class FakeSearch:
        def __init__(self, *a, **k):
            self.news = [only_future]

    monkeypatch.setattr(ynews.yf, "Search", FakeSearch)
    out = ynews.get_global_news_yfinance("2025-05-09", look_back_days=7, limit=10)
    assert "No global news found" in out
    assert "###" not in out  # no empty article body


@pytest.mark.unit
def test_ticker_news_uses_tagged_search_fallback_with_same_date_window(monkeypatch):
    past = {"title": "NVDA PAST", "publisher": "P", "link": "https://example.test/past",
            "providerPublishTime": _epoch("2025-05-05"), "relatedTickers": ["NVDA"]}
    future = {"title": "NVDA FUTURE", "publisher": "P", "link": "https://example.test/future",
              "providerPublishTime": _epoch("2025-06-01"), "relatedTickers": ["NVDA"]}
    unrelated = {"title": "OTHER COMPANY", "publisher": "P", "link": "https://example.test/other",
                 "providerPublishTime": _epoch("2025-05-05"), "relatedTickers": ["AMD"]}
    seen = []

    class FakeTicker:
        def __init__(self, symbol):
            assert symbol == "NVDA"

        def get_news(self, count):
            return []

    class FakeSearch:
        def __init__(self, query, **kwargs):
            seen.append(query)
            self.news = [past, future, unrelated]
            self.quotes = [{"symbol": "NVDA"}]

    monkeypatch.setattr(ynews.yf, "Ticker", FakeTicker)
    monkeypatch.setattr(ynews.yf, "Search", FakeSearch)
    out = ynews.get_news_yfinance("NVDA", "2025-05-01", "2025-05-09")
    assert seen == ["NVDA"]
    assert "NVDA PAST" in out and "https://example.test/past" in out
    assert "NVDA FUTURE" not in out
    assert "OTHER COMPANY" not in out


def test_tagged_search_sample_outside_window_is_coverage_unavailable(monkeypatch):
    only_recent = {
        "title": "RECENT NVDA", "publisher": "P",
        "link": "https://example.test/recent",
        "providerPublishTime": _epoch("2025-06-01"),
        "relatedTickers": ["NVDA"],
    }

    class FakeTicker:
        def __init__(self, symbol):
            assert symbol == "NVDA"

        def get_news(self, count):
            return []

    class FakeSearch:
        def __init__(self, query, **kwargs):
            self.news = [only_recent]

    monkeypatch.setattr(ynews.yf, "Ticker", FakeTicker)
    monkeypatch.setattr(ynews.yf, "Search", FakeSearch)
    with pytest.raises(VendorUnavailableError, match="sample"):
        ynews.get_news_yfinance("NVDA", "2025-05-01", "2025-05-09")


def test_yahoo_jev_triage_receives_only_dated_in_window_articles(monkeypatch):
    inside_offtopic = {
        "title": "OFF TOPIC", "summary": "other business",
        "link": "https://example.test/off", "providerPublishTime": _epoch("2025-05-04"),
    }
    inside_relevant = {
        "title": "NVDA DEMAND", "summary": "chip demand",
        "link": "https://example.test/nvda", "providerPublishTime": _epoch("2025-05-05"),
    }
    future = {
        "title": "FUTURE CLAIM", "summary": "not yet known",
        "link": "https://example.test/future", "providerPublishTime": _epoch("2025-06-01"),
    }
    unrelated_tagged = {
        "title": "AMD ONLY", "summary": "other ticker",
        "link": "https://example.test/amd", "providerPublishTime": _epoch("2025-05-05"),
        "relatedTickers": ["AMD"],
    }

    class FakeTicker:
        def __init__(self, symbol):
            assert symbol == "NVDA"

        def get_news(self, count):
            return [inside_offtopic, inside_relevant, future, unrelated_tagged]

    captured = []

    def triage(ticker, as_of_date, articles):
        captured.append((ticker, as_of_date, articles))
        return [False, True], "Jev kept one relevant article"

    monkeypatch.setattr(ynews.yf, "Ticker", FakeTicker)
    monkeypatch.setattr(ynews, "triage_news_articles", triage, raising=False)
    output = ynews.get_news_yfinance("NVDA", "2025-05-01", "2025-05-09")
    assert [item["title"] for item in captured[0][2]] == ["OFF TOPIC", "NVDA DEMAND"]
    assert "NVDA DEMAND" in output and "2025-05-05" in output
    assert "Link: https://example.test/nvda" in output
    assert "OFF TOPIC" not in output and "FUTURE CLAIM" not in output
    assert "Jev kept one" in output


def test_undated_live_yahoo_article_is_visible_but_never_sent_to_jev(monkeypatch):
    today = datetime.now(timezone.utc).date().isoformat()
    dated = {
        "title": "DATED NVDA", "summary": "company earnings",
        "link": "https://example.test/dated",
        "providerPublishTime": _epoch(today),
    }
    undated = {
        "title": "UNDATED NVDA", "summary": "company outlook",
        "link": "https://example.test/undated",
    }

    class FakeTicker:
        def __init__(self, symbol):
            assert symbol == "NVDA"

        def get_news(self, count):
            return [dated, undated]

    seen = []

    def triage(ticker, as_of_date, articles):
        seen.append([item["title"] for item in articles])
        return [False], "Jev screened one dated article"

    monkeypatch.setattr(ynews.yf, "Ticker", FakeTicker)
    monkeypatch.setattr(ynews, "triage_news_articles", triage)
    output = ynews.get_news_yfinance("NVDA", today, today)
    assert seen == [["DATED NVDA"]]
    assert "UNDATED NVDA" in output
    assert "Link: https://example.test/undated" in output
    assert "date unavailable" in output and "unscreened" in output
    assert "### DATED NVDA" not in output


@pytest.mark.unit
def test_untagged_yahoo_search_tries_the_next_configured_news_vendor(monkeypatch):
    class FakeTicker:
        def __init__(self, symbol):
            assert symbol == "NVDA"

        def get_news(self, count):
            return []

    class FakeSearch:
        def __init__(self, query, **kwargs):
            self.news = [{"title": "OTHER", "relatedTickers": ["AMD"]}]
            self.quotes = [{"symbol": "NVDA"}]

    monkeypatch.setattr(ynews.yf, "Ticker", FakeTicker)
    monkeypatch.setattr(ynews.yf, "Search", FakeSearch)
    monkeypatch.setattr(interface, "get_vendor", lambda category, method: "yfinance,alpha_vantage")
    monkeypatch.setitem(interface.VENDOR_METHODS["get_news"], "alpha_vantage",
                        lambda *args: "Alpha Vantage news")

    result = interface.route_to_vendor("get_news", "NVDA", "2025-05-01", "2025-05-09")
    assert result == "Alpha Vantage news"
