from __future__ import annotations

import json
from datetime import date, timedelta

import pytest
import requests

from tradingagents.dataflows import fmp


class FakeResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def _today() -> str:
    return date.today().isoformat()


def _assert_record_contract(record: dict, as_of_date: str) -> None:
    assert record["source"] == "fmp"
    assert record["category"] == "earnings"
    assert isinstance(record["title"], str) and record["title"]
    assert isinstance(record["content"], str) and record["content"]
    assert isinstance(record["url"], str) and record["url"].startswith("https://")
    assert record["published_at"] is None or isinstance(record["published_at"], str)
    assert record["as_of_date"] == as_of_date
    assert record["status"] in {"available", "unavailable"}
    json.dumps(record)


def test_missing_key_returns_unavailable_without_network(monkeypatch):
    monkeypatch.delenv("FMP_API_KEY", raising=False)

    def unexpected_request(*args, **kwargs):
        raise AssertionError("network request should not be made")

    monkeypatch.setattr(fmp.requests, "get", unexpected_request)

    records = fmp.get_earnings_context("AAPL", _today())

    assert len(records) == 1
    _assert_record_contract(records[0], _today())
    assert records[0]["status"] == "unavailable"
    assert "FMP_API_KEY" in records[0]["content"]
    assert "apikey" not in records[0]["url"].lower()


def test_historical_date_returns_point_in_time_unavailable_before_network(monkeypatch):
    monkeypatch.setenv("FMP_API_KEY", "must-not-be-used")

    def unexpected_request(*args, **kwargs):
        raise AssertionError("historical snapshot must not perform live FMP requests")

    monkeypatch.setattr(fmp.requests, "get", unexpected_request)

    records = fmp.get_earnings_context("AAPL", "2020-01-15")

    assert isinstance(records, list)
    assert len(records) == 1
    _assert_record_contract(records[0], "2020-01-15")
    assert records[0]["status"] == "unavailable"
    assert "no point-in-time archive" in records[0]["content"]
    assert "2020-01-15" in records[0]["content"]


def test_live_context_keeps_estimates_and_transcript_when_earnings_is_unavailable(
    monkeypatch,
):
    api_key = "top-secret-fmp-key"
    monkeypatch.setenv("FMP_API_KEY", api_key)
    today = date.today()
    call_date = (today - timedelta(days=30)).isoformat()
    future_call_date = (today + timedelta(days=30)).isoformat()
    transcript = "Management discussed demand and margin discipline. " * 300
    calls = []

    def fake_get(url, *, params, timeout):
        calls.append((url, params, timeout))
        if url.endswith("/analyst-estimates"):
            return FakeResponse(
                [
                    {
                        "symbol": "AAPL",
                        "date": (today + timedelta(days=120)).isoformat(),
                        "revenueLow": 93_000_000_000,
                        "revenueAvg": 96_500_000_000,
                        "revenueHigh": 99_000_000_000,
                        "epsLow": 1.41,
                        "epsAvg": 1.52,
                        "epsHigh": 1.61,
                        "numAnalystsRevenue": 29,
                        "numAnalystsEps": 31,
                    }
                ]
            )
        if url.endswith("/earnings"):
            return FakeResponse({"error": f"upgrade plan; token={api_key}"}, status_code=402)
        if url.endswith("/earning-call-transcript-dates"):
            return FakeResponse(
                [
                    {
                        "symbol": "AAPL",
                        "fiscalYear": today.year,
                        "quarter": 2,
                        "date": call_date,
                    },
                    {
                        "symbol": "AAPL",
                        "fiscalYear": today.year,
                        "quarter": 3,
                        "date": future_call_date,
                    },
                    {
                        "symbol": "MSFT",
                        "fiscalYear": today.year,
                        "quarter": 1,
                        "date": call_date,
                    },
                ]
            )
        if url.endswith("/earning-call-transcript"):
            assert params["year"] == today.year
            assert params["quarter"] == 2
            return FakeResponse(
                [
                    {
                        "symbol": "AAPL",
                        "year": today.year,
                        "quarter": 2,
                        "date": call_date,
                        "content": transcript,
                    }
                ]
            )
        raise AssertionError(f"unexpected endpoint: {url}")

    monkeypatch.setattr(fmp.requests, "get", fake_get)

    records = fmp.get_earnings_context("aapl", today.isoformat())

    assert len(records) == 3
    assert [record["status"] for record in records] == [
        "available",
        "unavailable",
        "available",
    ]
    for record in records:
        _assert_record_contract(record, today.isoformat())
        assert api_key not in repr(record)
        assert "apikey" not in record["url"].lower()

    estimates = records[0]
    assert "Fiscal period end:" in estimates["content"]
    assert "EPS consensus (currency/share): 1.52" in estimates["content"]
    assert "Revenue consensus (currency units): 96500000000" in estimates["content"]
    assert "current snapshot" in estimates["content"]
    assert estimates["published_at"] is None

    earnings_error = records[1]
    assert "HTTP 402" in earnings_error["content"]
    assert "plan entitlement" in earnings_error["content"]

    transcript_record = records[2]
    assert "Call date:" in transcript_record["content"]
    assert "publication timestamp is not supplied" in transcript_record["content"]
    assert "Transcript excerpt (truncated to 8000 characters" in transcript_record["content"]
    assert len(transcript_record["content"]) < 9000
    assert transcript_record["published_at"] is None

    assert len(calls) == 4
    assert all(call[1]["apikey"] == api_key for call in calls)
    assert all(call[2] == fmp.REQUEST_TIMEOUT for call in calls)
    assert all(api_key not in call[0] for call in calls)


def test_parses_actual_vs_estimate_and_ignores_future_or_wrong_symbol_rows(monkeypatch):
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    today = date.today()
    report_date = (today - timedelta(days=10)).isoformat()
    report_timestamp = f"{report_date} 16:05:00"

    def fake_get(url, *, params, timeout):
        if url.endswith("/analyst-estimates"):
            return FakeResponse([])
        if url.endswith("/earnings"):
            return FakeResponse(
                [
                    {
                        "symbol": "AAPL",
                        "date": report_timestamp,
                        "fiscalDateEnding": (today - timedelta(days=40)).isoformat(),
                        "epsActual": 1.64,
                        "epsEstimated": 1.59,
                        "revenueActual": 98_300_000_000,
                        "revenueEstimated": 97_900_000_000,
                    },
                    {
                        "symbol": "AAPL",
                        "date": (today + timedelta(days=20)).isoformat(),
                        "epsEstimated": 1.70,
                    },
                    {
                        "symbol": "MSFT",
                        "date": report_date,
                        "epsActual": 9.99,
                    },
                ]
            )
        if url.endswith("/earning-call-transcript-dates"):
            return FakeResponse([])
        raise AssertionError(f"unexpected endpoint: {url}")

    monkeypatch.setattr(fmp.requests, "get", fake_get)

    records = fmp.get_earnings_context("AAPL", today.isoformat())

    assert [record["status"] for record in records] == [
        "unavailable",
        "available",
        "unavailable",
    ]
    actuals = records[1]
    # The endpoint's date is the earnings event/report date, not a guaranteed
    # provider publication timestamp. Keep it in content and leave provenance null.
    assert actuals["published_at"] is None
    assert "Fiscal period end:" in actuals["content"]
    assert "EPS actual (currency/share): 1.64" in actuals["content"]
    assert "EPS estimate (currency/share): 1.59" in actuals["content"]
    assert "Revenue actual (currency units): 98300000000" in actuals["content"]
    assert "Revenue estimate (currency units): 97900000000" in actuals["content"]
    assert "9.99" not in actuals["content"]


def test_malformed_payloads_and_sensitive_network_errors_are_generic(monkeypatch):
    api_key = "never-return-this-key"
    monkeypatch.setenv("FMP_API_KEY", api_key)

    def fake_get(url, *, params, timeout):
        if url.endswith("/analyst-estimates"):
            return FakeResponse({"unexpected": "mapping"})
        if url.endswith("/earnings"):
            raise requests.RequestException(f"connection failed with {api_key}")
        if url.endswith("/earning-call-transcript-dates"):
            return FakeResponse(["not-a-record"])
        raise AssertionError(f"unexpected endpoint: {url}")

    monkeypatch.setattr(fmp.requests, "get", fake_get)

    records = fmp.get_earnings_context("AAPL", _today())

    assert len(records) == 3
    assert all(record["status"] == "unavailable" for record in records)
    assert "malformed JSON payload" in records[0]["content"]
    assert "network request failed" in records[1]["content"]
    assert "No eligible transcript date" in records[2]["content"]
    assert api_key not in repr(records)


def test_future_analysis_date_is_rejected_without_network(monkeypatch):
    monkeypatch.setenv("FMP_API_KEY", "test-key")
    future = (date.today() + timedelta(days=1)).isoformat()

    def unexpected_request(*args, **kwargs):
        raise AssertionError("invalid date must be rejected before network")

    monkeypatch.setattr(fmp.requests, "get", unexpected_request)

    with pytest.raises(ValueError, match="cannot be in the future"):
        fmp.get_earnings_context("AAPL", future)
