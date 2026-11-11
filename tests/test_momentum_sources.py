"""Fixed, bounded momentum-feed and market-evidence adapters (no live HTTP)."""

import json
from datetime import datetime, timezone
from importlib import import_module
from threading import Event

import pandas as pd
import pytest
import requests
import yfinance as yf


def sources_module():
    return import_module("tradingagents.web.momentum_sources")


@pytest.fixture(autouse=True)
def completed_market_session_clock(monkeypatch):
    """Keep dated market fixtures fresh independently of the wall clock."""
    monkeypatch.setattr(
        sources_module(), "_utcnow",
        lambda: datetime(2026, 10, 16, 21, tzinfo=timezone.utc),
    )


class Response:
    def __init__(self, payload=None, *, status=200, text=None):
        self.status_code = status
        self.text = text if text is not None else json.dumps(payload)

    def json(self):
        return json.loads(self.text)

    def iter_content(self, chunk_size=65536):
        data = self.text.encode("utf-8")
        for start in range(0, len(data), chunk_size):
            yield data[start:start + chunk_size]

    def close(self):
        pass


def feed_payload(url):
    if "alphavantage.co" in url:
        return {"top_gainers": [{"ticker": "AMD"}],
                "most_actively_traded": [{"ticker": "AMD"}, {"ticker": "NVDA"}]}
    if "biggest-gainers" in url:
        return [{"symbol": "AMD"}, {"symbol": "PENG"}]
    if "most-actives" in url:
        return [{"symbol": "NVDA"}]
    if "apewisdom" in url:
        return {"results": [{"ticker": "AMD", "rank": 1, "mentions": 80}]}
    if "stocktwits" in url:
        return {"symbols": [{"symbol": "AMD", "trending_score": 14}]}
    raise AssertionError(f"Unexpected fixed endpoint: {url}")


def test_either_feed_family_nominates_and_shared_symbols_deduplicate(monkeypatch):
    module = sources_module()
    monkeypatch.setattr(module, "get_data_source_setting", lambda key: "test-key")
    called = []

    def fake_get(url, **kwargs):
        called.append((url, kwargs))
        return Response(feed_payload(url))

    monkeypatch.setattr(module.requests, "get", fake_get)
    membership, warnings = module.fetch_source_membership()

    assert warnings == []
    assert {"AMD", "NVDA", "PENG"} <= set(membership)
    assert {"alpha_vantage_gainers", "alpha_vantage_active", "fmp_gainers",
            "apewisdom", "stocktwits"} <= membership["AMD"]
    assert membership["PENG"] == {"fmp_gainers"}
    assert all(kwargs["allow_redirects"] is False and kwargs["stream"] is True
               and kwargs["timeout"] <= 30
               for _, kwargs in called)


def test_fmp_entitlement_failure_keeps_other_feeds_usable(monkeypatch):
    module = sources_module()
    monkeypatch.setattr(module, "get_data_source_setting", lambda key: "test-key")

    def fake_get(url, **_kwargs):
        return Response({}, status=403) if "financialmodelingprep" in url else Response(feed_payload(url))

    monkeypatch.setattr(module.requests, "get", fake_get)
    membership, warnings = module.fetch_source_membership()

    assert "AMD" in membership and "NVDA" in membership
    assert "fmp_unavailable" in warnings
    assert not any(source.startswith("fmp_") for source in membership["AMD"])


@pytest.mark.parametrize("failure", [
    "redirect", "unauthorized", "rate_limit", "malformed", "oversized", "timeout",
])
def test_bad_provider_responses_return_bounded_warning_without_secret(monkeypatch, caplog, failure):
    module = sources_module()
    secret = "private-test-api-key"
    monkeypatch.setattr(module, "get_data_source_setting",
                        lambda key: secret if key == "ALPHA_VANTAGE_API_KEY" else None)

    def fake_get(url, **_kwargs):
        if "alphavantage.co" in url:
            if failure == "timeout":
                raise requests.Timeout(f"timeout while using {secret}")
            if failure == "redirect":
                return Response({}, status=302)
            if failure == "unauthorized":
                return Response({"error": secret}, status=401)
            if failure == "rate_limit":
                return Response({"Note": secret}, status=429)
            if failure == "malformed":
                return Response(text=f"not JSON {secret}")
            return Response(text="x" * 200_001)
        return Response(feed_payload(url))

    monkeypatch.setattr(module.requests, "get", fake_get)
    membership, warnings = module.fetch_source_membership()

    assert "AMD" in membership  # another source still nominated it
    assert "alpha_vantage_unavailable" in warnings
    assert secret not in repr((membership, warnings)) + caplog.text


def market_frame():
    columns = pd.MultiIndex.from_tuples([
        ("AMD", "Close"), ("AMD", "Adj Close"), ("AMD", "Volume"),
        ("PENG", "Close"), ("PENG", "Adj Close"), ("PENG", "Volume"),
    ])
    rows = [
        [52.0 + index, 42.0 + index, 1_000_000,
         2.0 + index, 1.0 + index, 500_000]
        for index in range(48)
    ]
    rows.extend([
        [100.0, 90.0, 1_000_000, 50.0, 45.0, 500_000],
        [101.0, 91.0, 1_100_000, 51.0, 46.0, 550_000],
    ])
    return pd.DataFrame(
        rows, index=pd.bdate_range(end="2026-10-15", periods=50), columns=columns,
    )


def test_market_evidence_keeps_adjusted_unadjusted_and_rejects_bad_identity(monkeypatch):
    module = sources_module()
    seen = []

    def fake_download(symbols, **kwargs):
        seen.extend(symbols)
        assert kwargs["auto_adjust"] is False
        assert kwargs["timeout"] <= 30
        return market_frame()

    class FakeSearch:
        def __init__(self, symbol, **kwargs):
            assert kwargs["timeout"] <= 30
            metadata = {
                "AMD": {"symbol": "AMD", "longname": "Advanced Micro Devices",
                        "exchange": "NMS", "currency": "USD", "quoteType": "EQUITY"},
                "PENG": {"symbol": "PENG", "shortname": "Penguin Solutions",
                         "exchange": "FRA", "currency": "EUR", "quoteType": "EQUITY"},
            }
            self.quotes = [metadata[symbol]]

    monkeypatch.setattr(yf, "download", fake_download)
    monkeypatch.setattr(yf, "Search", FakeSearch)

    histories, identities, warnings = module.fetch_market_evidence(["AMD", "PENG", "../secret"])

    assert seen == ["AMD", "PENG"]
    assert warnings == []
    assert [row.adjusted_close for row in histories["AMD"]][-2:] == [90.0, 91.0]
    assert [row.unadjusted_close for row in histories["AMD"]][-2:] == [100.0, 101.0]
    assert identities["AMD"].name == "Advanced Micro Devices"
    assert "PENG" not in identities
    assert "../secret" not in histories


def test_search_quote_without_currency_uses_matching_chart_metadata(monkeypatch):
    module = sources_module()
    monkeypatch.setattr(yf, "download", lambda symbols, **kwargs: market_frame())

    class FakeSearch:
        def __init__(self, symbol, **kwargs):
            self.quotes = [{"symbol": symbol, "longname": "Advanced Micro Devices",
                            "exchange": "NMS", "quoteType": "EQUITY"}]

    class FakeTicker:
        def __init__(self, symbol):
            assert symbol == "AMD"

        def history(self, **kwargs):
            assert kwargs["timeout"] <= 10
            assert kwargs["interval"] == "1h"
            return pd.DataFrame()

        def get_history_metadata(self):
            return {"symbol": "AMD", "currency": "USD", "exchangeName": "NMS",
                    "instrumentType": "EQUITY"}

    monkeypatch.setattr(yf, "Search", FakeSearch)
    monkeypatch.setattr(yf, "Ticker", FakeTicker)
    histories, identities, warnings = module.fetch_market_evidence(["AMD"])
    assert "AMD" in histories
    assert identities["AMD"].currency == "USD"
    assert identities["AMD"].name == "Advanced Micro Devices"
    assert warnings == []


def test_missing_currency_in_both_identity_sources_is_reported_unavailable(monkeypatch):
    module = sources_module()
    monkeypatch.setattr(yf, "download", lambda symbols, **kwargs: market_frame())

    class FakeSearch:
        def __init__(self, symbol, **kwargs):
            self.quotes = [{"symbol": symbol, "exchange": "NMS", "quoteType": "EQUITY"}]

    class FakeTicker:
        def __init__(self, symbol):
            pass

        def history(self, **kwargs):
            return pd.DataFrame()

        def get_history_metadata(self):
            return {"symbol": "AMD", "exchangeName": "NMS", "instrumentType": "EQUITY"}

    monkeypatch.setattr(yf, "Search", FakeSearch)
    monkeypatch.setattr(yf, "Ticker", FakeTicker)
    _, identities, warnings = module.fetch_market_evidence(["AMD"])
    assert identities == {}
    assert warnings == ["identity_unavailable"]


def test_bounded_identity_lookups_overlap_instead_of_waiting_serially(monkeypatch):
    module = sources_module()
    monkeypatch.setattr(yf, "download", lambda symbols, **kwargs: market_frame())
    first_entered = Event()
    second_entered = Event()

    def slow_identity(symbol):
        if symbol == "AMD":
            first_entered.set()
            assert second_entered.wait(1)
        else:
            second_entered.set()
            assert first_entered.wait(1)
        return module.EquityIdentity(symbol, symbol, "NMS", "USD", "EQUITY", True)

    monkeypatch.setattr(module, "_identity", slow_identity)
    _, identities, warnings = module.fetch_market_evidence(["AMD", "PENG"])
    assert set(identities) == {"AMD", "PENG"}
    assert warnings == []


def test_partial_price_frame_never_reuses_one_symbols_prices_for_another(monkeypatch):
    module = sources_module()
    only_amd = market_frame()["AMD"]
    monkeypatch.setattr(yf, "download", lambda symbols, **kwargs: only_amd)

    def unexpected_search(*args, **kwargs):
        raise AssertionError("An ambiguous flat frame cannot verify either symbol")

    monkeypatch.setattr(yf, "Search", unexpected_search)
    histories, identities, warnings = module.fetch_market_evidence(["AMD", "PENG"])
    assert histories == {}
    assert identities == {}
    assert warnings == ["market_data_unavailable"]


def test_candidate_and_identity_lookup_caps(monkeypatch):
    module = sources_module()
    tickers = [f"T{number:03d}" for number in range(150)]
    dates = pd.bdate_range(end="2026-10-15", periods=50)
    frame = pd.DataFrame(
        {(ticker, field): [100.0 + row if field != "Volume" else 1_000_000
                          for row in range(50)]
         for ticker in tickers[:100] for field in ("Close", "Adj Close", "Volume")},
        index=dates,
    )
    searches = []

    def fake_download(symbols, **kwargs):
        assert len(symbols) == 100
        return frame

    class FakeSearch:
        def __init__(self, symbol, **kwargs):
            searches.append(symbol)
            self.quotes = [{"symbol": symbol, "exchange": "NMS", "currency": "USD",
                            "quoteType": "EQUITY"}]

    monkeypatch.setattr(yf, "download", fake_download)
    monkeypatch.setattr(yf, "Search", FakeSearch)
    histories, identities, warnings = module.fetch_market_evidence(tickers)
    assert len(histories) == 100
    assert len(searches) == len(identities) == 40
    assert warnings == []


def test_identity_budget_is_not_spent_on_known_price_ineligible_symbols(monkeypatch):
    module = sources_module()
    symbols = [f"X{number:03d}" for number in range(41)] + ["GOOD"]
    dates = pd.bdate_range(end="2026-10-15", periods=50)
    invalid_prices = [1 + 3 * index / 49 for index in range(50)]
    valid_prices = [50 + 10 * index / 49 for index in range(50)]
    columns = {}
    for symbol in symbols:
        prices = valid_prices if symbol == "GOOD" else invalid_prices
        columns[(symbol, "Close")] = prices
        columns[(symbol, "Adj Close")] = prices
        columns[(symbol, "Volume")] = [1_000_000] * 50
    frame = pd.DataFrame(columns, index=dates)
    monkeypatch.setattr(yf, "download", lambda symbols, **kwargs: frame)
    checked = []

    def fake_identity(symbol):
        checked.append(symbol)
        return module.EquityIdentity(symbol, symbol, "NMS", "USD", "EQUITY", True)

    monkeypatch.setattr(module, "_identity", fake_identity)
    _, identities, warnings = module.fetch_market_evidence(symbols)
    assert set(identities) == {"GOOD"}
    assert checked == ["GOOD"]
    assert warnings == []


def test_identity_errors_do_not_expose_provider_details(monkeypatch, caplog):
    module = sources_module()
    secret = "private-test-api-key"
    monkeypatch.setattr(yf, "download", lambda symbols, **kwargs: market_frame())

    def bad_search(*args, **kwargs):
        raise RuntimeError(secret)

    monkeypatch.setattr(yf, "Search", bad_search)
    histories, identities, warnings = module.fetch_market_evidence(["AMD"])
    assert "AMD" in histories
    assert identities == {}
    assert warnings == ["identity_unavailable"]
    assert secret not in repr((histories, identities, warnings)) + caplog.text


def test_unfinished_current_us_session_is_not_a_completed_daily_bar(monkeypatch):
    module = sources_module()
    frame = market_frame().loc[pd.to_datetime(["2026-10-15"])].copy()
    current = frame.iloc[-1].copy()
    current[("AMD", "Close")] = 150.0
    current[("AMD", "Adj Close")] = 140.0
    frame.loc[pd.Timestamp("2026-10-16")] = current
    monkeypatch.setattr(yf, "download", lambda symbols, **kwargs: frame)

    class FakeSearch:
        def __init__(self, symbol, **kwargs):
            self.quotes = [{"symbol": symbol, "exchange": "NMS", "currency": "USD",
                            "quoteType": "EQUITY"}]

    monkeypatch.setattr(yf, "Search", FakeSearch)
    clock = [datetime(2026, 10, 16, 15, tzinfo=timezone.utc)]
    monkeypatch.setattr(module, "_utcnow", lambda: clock[0])
    before_close = module.fetch_market_evidence(["AMD"])[0]["AMD"]
    assert before_close[-1].date.isoformat() == "2026-10-15"
    clock[0] = datetime(2026, 10, 16, 21, tzinfo=timezone.utc)
    after_close = module.fetch_market_evidence(["AMD"])[0]["AMD"]
    assert after_close[-1].date.isoformat() == "2026-10-16"


def test_oversized_http_body_stops_streaming_and_closes(monkeypatch):
    module = sources_module()
    monkeypatch.setattr(module, "get_data_source_setting",
                        lambda key: "test-key" if key == "ALPHA_VANTAGE_API_KEY" else None)
    chunks_read = []
    closed = []

    class StreamingResponse:
        status_code = 200

        @property
        def text(self):
            raise AssertionError("Body must not be eagerly buffered")

        def iter_content(self, chunk_size):
            for index in range(10):
                chunks_read.append(index)
                yield b"x" * 65_536

        def close(self):
            closed.append(True)

    monkeypatch.setattr(module.requests, "get",
                        lambda url, **kwargs: StreamingResponse() if "alphavantage" in url
                        else Response(feed_payload(url)))
    membership, warnings = module.fetch_source_membership()
    assert "alpha_vantage_unavailable" in warnings
    assert "AMD" in membership
    assert len(chunks_read) <= 4  # stop once 200 KB is crossed, not after all ten chunks
    assert closed == [True]
