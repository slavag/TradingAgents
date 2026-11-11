"""Ticker-news relevance triage is opt-in, bounded and fail-open."""

import json
import time
from datetime import date, datetime, timedelta, timezone
from threading import Event

import pytest

from tradingagents.agents.news_triage import triage_news_articles
from tradingagents.dataflows import alpha_vantage_news as alpha_news, yfinance_news as yahoo_news
from tradingagents.dataflows.config import run_config

ARTICLES = [
    {"title": "Unrelated roundup", "summary": "Other companies", "link": "https://example.test/a"},
    {"title": "NVDA demand", "summary": "Company demand rose at www.example.test", "link": "https://example.test/b"},
]


def test_news_triage_is_off_by_default_even_with_social_jev_key(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.setattr(
        "tradingagents.agents.news_triage.system_one",
        lambda *a, **k: pytest.fail("opt-in flag is required"),
    )
    keep, note = triage_news_articles("NVDA", date.today().isoformat(), ARTICLES)
    assert keep == [True, True]
    assert note == ""


def test_news_triage_never_exports_historical_articles(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.setattr(
        "tradingagents.agents.news_triage.system_one",
        lambda *a, **k: pytest.fail("historical news must not be exported"),
    )
    with run_config({"jev_news_triage_enabled": True}):
        keep, note = triage_news_articles("NVDA", "2020-01-02", ARTICLES)
    assert keep == [True, True]
    assert note == ""


def test_opted_in_news_triage_without_key_keeps_articles_and_reports_status(monkeypatch):
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    monkeypatch.setattr(
        "tradingagents.agents.news_triage.system_one",
        lambda *a, **k: pytest.fail("missing key must prevent Jev request"),
    )
    with run_config({"jev_news_triage_enabled": True}):
        keep, note = triage_news_articles("NVDA", date.today().isoformat(), ARTICLES)
    assert keep == [True, True]
    assert "key missing" in note and "unscreened" in note


def test_enabled_news_triage_drops_only_strongly_off_topic(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    seen = []

    def ask(state, questions, **_kwargs):
        seen.append((state, questions))
        score = 0.02 if state["article"].startswith("Unrelated") else 0.55
        return {"relevant": {"type": "noul", "noul": score}}

    monkeypatch.setattr("tradingagents.agents.news_triage.system_one", ask)
    with run_config({"jev_news_triage_enabled": True}):
        keep, note = triage_news_articles("NVDA", date.today().isoformat(), ARTICLES)
    assert keep == [False, True]
    assert "1 of 2" in note
    assert all(
        "link" not in state and "https://" not in state["article"]
        and "www." not in state["article"]
        for state, _ in seen
    )
    assert all(set(questions) == {"relevant"} for _, questions in seen)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -0.1, 1.1, "0.9"])
def test_malformed_news_triage_score_keeps_all_articles(monkeypatch, bad):
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.setattr(
        "tradingagents.agents.news_triage.system_one",
        lambda *a, **k: {"relevant": {"type": "noul", "noul": bad}},
    )
    with run_config({"jev_news_triage_enabled": True}):
        keep, note = triage_news_articles("NVDA", date.today().isoformat(), ARTICLES)
    assert keep == [True, True]
    assert "unscreened" in note


def test_news_triage_caps_jev_requests_and_keeps_overflow(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    calls = []

    def ask(state, questions, **_kwargs):
        calls.append(state["article"])
        return {"relevant": {"type": "noul", "noul": 0.5}}

    monkeypatch.setattr("tradingagents.agents.news_triage.system_one", ask)
    articles = [
        {"title": f"NVDA article {index}", "summary": "demand"}
        for index in range(25)
    ]
    with run_config({"jev_news_triage_enabled": True}):
        keep, note = triage_news_articles("NVDA", date.today().isoformat(), articles)
    assert len(calls) == 20
    assert keep == [True] * 25
    assert "5 additional articles shown unscreened" in note


def test_alpha_news_triage_checks_date_and_exact_ticker_before_jev(monkeypatch):
    today = date.today()
    stamp = today.strftime("%Y%m%d") + "T120000"
    future = (today + timedelta(days=1)).strftime("%Y%m%d") + "T120000"

    def article(title, when=stamp, tags=None):
        value = {
            "title": title, "summary": title + " details",
            "url": "https://example.test/" + title.lower().replace(" ", "-"),
        }
        if when is not None:
            value["time_published"] = when
        if tags is not None:
            value["ticker_sentiment"] = [{"ticker": tag} for tag in tags]
        return value

    feed = [
        article("OFF TOPIC", tags=["NVDA"]),
        article("NVDA RESULTS", tags=["NVDA"]),
        article("OTHER COMPANY", tags=["AMD"]),
        article("FUTURE NVDA", when=future, tags=["NVDA"]),
        article("UNTAGGED ARTICLE"),
    ]
    monkeypatch.setattr(alpha_news, "_make_api_request", lambda *a, **k: json.dumps({"feed": feed, "items": 5}))
    seen = []

    def triage(ticker, as_of_date, articles):
        seen.append([item["title"] for item in articles])
        return [False, True], "Jev kept one relevant article"

    monkeypatch.setattr(alpha_news, "triage_news_articles", triage, raising=False)
    with run_config({"jev_news_triage_enabled": True}):
        output = alpha_news.get_news("NVDA", today.isoformat(), today.isoformat())
    payload = json.loads(output)
    assert seen == [["OFF TOPIC", "NVDA RESULTS"]]
    assert [item["title"] for item in payload["feed"]] == ["NVDA RESULTS", "UNTAGGED ARTICLE"]
    assert payload["feed"][0]["url"] == "https://example.test/nvda-results"
    assert payload["feed"][0]["time_published"] == stamp
    assert "Jev kept one" in payload["tradingagents_news_triage"]


def test_alpha_news_is_byte_for_byte_unchanged_without_opt_in(monkeypatch):
    raw = '{"feed":[{"title":"NVDA"}],"items":1}'
    monkeypatch.setattr(alpha_news, "_make_api_request", lambda *a, **k: raw)
    monkeypatch.setattr(
        alpha_news, "triage_news_articles",
        lambda *a, **k: pytest.fail("default-off Alpha path must not triage"),
    )
    with run_config({"jev_news_triage_enabled": False}):
        assert alpha_news.get_news("NVDA", "2026-01-01", "2026-01-02") == raw


def test_alpha_ticker_news_query_includes_entire_end_date(monkeypatch):
    captured = {}

    def request(_function, params):
        captured.update(params)
        return "{}"

    monkeypatch.setattr(alpha_news, "_make_api_request", request)
    with run_config({"jev_news_triage_enabled": False}):
        alpha_news.get_news("NVDA", "2026-01-01", "2026-01-02")
    assert captured["time_from"] == "20260101T0000"
    assert captured["time_to"] == "20260102T2359"


def test_alpha_jev_failure_keeps_dated_linked_article(monkeypatch):
    today = date.today()
    stamp = today.strftime("%Y%m%d") + "T120000"
    original = {
        "title": "NVDA results", "summary": "earnings",
        "url": "https://example.test/nvda", "time_published": stamp,
        "ticker_sentiment": [{"ticker": "NVDA"}],
    }
    monkeypatch.setattr(
        alpha_news, "_make_api_request",
        lambda *a, **k: json.dumps({"feed": [original], "items": 1}),
    )
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.setattr(
        "tradingagents.agents.news_triage.system_one",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("temporary")),
    )
    with run_config({"jev_news_triage_enabled": True}):
        result = json.loads(alpha_news.get_news("NVDA", today.isoformat(), today.isoformat()))
    assert result["feed"] == [original]
    assert "unscreened" in result["tradingagents_news_triage"]


def test_global_yahoo_news_is_never_sent_to_jev(monkeypatch):
    today = date.today()
    published = int(datetime(
        today.year, today.month, today.day, 12, tzinfo=timezone.utc
    ).timestamp())

    class FakeSearch:
        def __init__(self, *a, **k):
            self.news = [{
                "title": "Global macro story", "publisher": "P",
                "link": "https://example.test/global",
                "providerPublishTime": published,
            }]

    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.setattr(yahoo_news.yf, "Search", FakeSearch)
    monkeypatch.setattr(
        yahoo_news, "triage_news_articles",
        lambda *a, **k: pytest.fail("global news must not reach Jev"),
    )
    with run_config({"jev_news_triage_enabled": True}):
        result = yahoo_news.get_global_news_yfinance(today.isoformat(), 1, 1)
    assert "Global macro story" in result


def test_historical_alpha_news_is_never_sent_to_jev(monkeypatch):
    raw = '{"feed":[{"title":"Historical NVDA"}],"items":1}'
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.setattr(alpha_news, "_make_api_request", lambda *a, **k: raw)
    monkeypatch.setattr(
        alpha_news, "triage_news_articles",
        lambda *a, **k: pytest.fail("historical news must not reach Jev"),
    )
    with run_config({"jev_news_triage_enabled": True}):
        assert alpha_news.get_news("NVDA", "2020-01-01", "2020-01-02") == raw


def test_news_triage_returns_promptly_when_one_article_fails(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    slow_started = Event()
    release = Event()

    def ask(state, questions, **_kwargs):
        if state["article"].startswith("Unrelated"):
            slow_started.set()
            release.wait(1.5)
            return {"relevant": {"type": "noul", "noul": 0.5}}
        slow_started.wait(1)
        raise RuntimeError("classifier failed")

    monkeypatch.setattr("tradingagents.agents.news_triage.system_one", ask)
    start = time.monotonic()
    with run_config({"jev_news_triage_enabled": True}):
        keep, note = triage_news_articles("NVDA", date.today().isoformat(), ARTICLES)
    elapsed = time.monotonic() - start
    release.set()
    assert elapsed < 0.6
    assert keep == [True, True] and "unscreened" in note
