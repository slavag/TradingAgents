"""Jev social-post screening is explicit, live-only, and failure-safe."""

import importlib
import json
from datetime import date
from types import SimpleNamespace

import pytest

from tradingagents.dataflows import reddit, stocktwits

pytestmark = pytest.mark.unit


def screen_module():
    return importlib.import_module("tradingagents.agents.post_screen")


def test_key_alone_enables_live_jev_screening(monkeypatch):
    screen = screen_module()
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.delenv("TRADINGAGENTS_JEV_SCREENING", raising=False)
    monkeypatch.setattr(screen, "resolve_instrument_identity", lambda ticker: {})
    monkeypatch.setattr(screen, "_ask_each", lambda instrument, posts: [
        {"about": {"noul": 0.9}, "stance": {"choice": "bullish", "confidence": 0.9}}
    ])
    enabled = screen.jev_screen("AAPL", as_of_date=date.today().isoformat())
    assert callable(enabled)
    keep, note = enabled(["buy AAPL"])
    assert keep == [True]
    assert "Screened by Jev" in note


def test_no_key_disables_jev_even_with_legacy_flag(monkeypatch):
    screen = screen_module()
    monkeypatch.setenv("TRADINGAGENTS_JEV_SCREENING", "1")
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    monkeypatch.setattr(screen, "resolve_instrument_identity",
                        lambda ticker: pytest.fail("identity lookup must not happen"))
    assert screen.jev_screen("AAPL") is None


def test_historical_run_never_sends_posts_even_when_opted_in(monkeypatch):
    screen = screen_module()
    monkeypatch.setenv("TRADINGAGENTS_JEV_SCREENING", "1")
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.setattr(screen, "resolve_instrument_identity",
                        lambda ticker: pytest.fail("identity lookup must not happen"))
    assert screen.jev_screen("AAPL", as_of_date="2020-01-02") is None


def test_enabled_screen_drops_only_clear_off_topic_posts(monkeypatch):
    screen = screen_module()
    monkeypatch.setenv("TRADINGAGENTS_JEV_SCREENING", "1")
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.setattr(screen, "resolve_instrument_identity",
                        lambda ticker: {"company_name": "Apple"})
    monkeypatch.setattr(screen, "_ask_each", lambda instrument, posts: [
        {"about": {"noul": 0.1}, "stance": {"choice": "neutral", "confidence": 0.9}},
        {"about": {"noul": 0.9}, "stance": {"choice": "bullish", "confidence": 0.9}},
        {"about": {"noul": 0.5}, "stance": {"choice": "bearish", "confidence": 0.1}},
    ])
    keep, note = screen.jev_screen("AAPL", as_of_date=date.today().isoformat())(
        ["unrelated", "buy AAPL", "maybe AAPL"]
    )
    assert keep == [False, True, True]
    assert "2 of the 3" in note
    assert "1 bullish" in note and "1 unclear" in note


def test_screen_failure_keeps_posts_and_omits_secret(monkeypatch):
    screen = screen_module()
    secret = "private-test-key"
    monkeypatch.setenv("TRADINGAGENTS_JEV_SCREENING", "1")
    monkeypatch.setenv("TYPESAFE_API_KEY", secret)
    monkeypatch.setattr(screen, "resolve_instrument_identity", lambda ticker: {})
    monkeypatch.setattr(screen, "_ask_each",
                        lambda instrument, posts: (_ for _ in ()).throw(screen.TypeSafeError("HTTP 529")))
    keep, note = screen.jev_screen("AAPL")(["one", "two"])
    assert keep == [True, True]
    assert "unscreened" in note
    assert secret not in note


@pytest.mark.parametrize("answer", [
    {"about": {"noul": float("nan")}, "stance": {"choice": "bullish", "confidence": 0.9}},
    {"about": {"noul": 0.9}, "stance": {"choice": "bullish", "confidence": float("nan")}},
    {"about": {"noul": 1.1}, "stance": {"choice": "bullish", "confidence": 0.9}},
])
def test_malformed_jev_scores_keep_posts_unscreened(monkeypatch, answer):
    screen = screen_module()
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.setattr(screen, "resolve_instrument_identity", lambda ticker: {})
    monkeypatch.setattr(screen, "_ask_each", lambda instrument, posts: [answer])
    keep, note = screen.jev_screen("AAPL", as_of_date=date.today().isoformat())(
        ["uncertain post"]
    )
    assert keep == [True]
    assert "unscreened" in note


def test_configured_endpoint_and_typed_questions_are_used_without_live_http(monkeypatch):
    screen = screen_module()
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.setenv("TYPESAFE_BASE_URL", "https://example.test/api")
    seen = {}
    answers = {
        "about": {"type": "noul", "noul": 0.9},
        "stance": {"type": "choice", "choice": "bullish", "confidence": 0.9},
    }

    def post(url, **kwargs):
        seen.update(url=url, **kwargs)
        return SimpleNamespace(status_code=200, json=lambda: {"answers": answers})

    monkeypatch.setattr(screen.requests, "post", post)
    assert screen.system_one({"instrument": "AAPL", "post": "buy"}, screen.QUESTIONS) == answers
    assert seen["url"] == "https://example.test/api/v1/systemone"
    assert seen["json"]["questions"]["about"]["type"] == "noul"
    assert seen["headers"]["Authorization"] == "Bearer test-key"


def test_remote_plain_http_jev_endpoint_never_receives_key_or_posts(monkeypatch):
    screen = screen_module()
    monkeypatch.setenv("TYPESAFE_API_KEY", "test-key")
    monkeypatch.setenv("TYPESAFE_BASE_URL", "http://remote.example/api")
    called = []
    monkeypatch.setattr(screen.requests, "post", lambda *a, **kw: called.append((a, kw)))
    with pytest.raises(screen.TypeSafeError, match="HTTPS"):
        screen.system_one({"instrument": "AAPL", "post": "buy"}, screen.QUESTIONS)
    assert called == []


def test_loopback_http_jev_endpoint_is_allowed_for_local_testing(monkeypatch):
    screen = screen_module()
    monkeypatch.setenv("TYPESAFE_BASE_URL", "http://127.0.0.1:9000")
    assert screen._url() == "http://127.0.0.1:9000/v1/systemone"


def test_stocktwits_screening_filters_before_display_limit(monkeypatch):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return json.dumps({"messages": [
                {"body": "unrelated"}, {"body": "buy NVDA"},
            ]}).encode()

    monkeypatch.setattr(stocktwits, "urlopen", lambda *args, **kwargs: Response())
    out = stocktwits.fetch_stocktwits_messages(
        "NVDA", limit=1, screen=lambda posts: ([False, True], "Screened by Jev")
    )
    assert "buy NVDA" in out and "unrelated" not in out
    assert "Screened by Jev" in out


def test_reddit_screening_preserves_per_subreddit_layout(monkeypatch):
    posts = [
        {"title": "unrelated", "selftext": "", "created_utc": None},
        {"title": "buy NVDA", "selftext": "", "created_utc": None},
    ]
    monkeypatch.setattr(reddit, "_fetch_subreddit", lambda *args, **kwargs: posts)
    out = reddit.fetch_reddit_posts(
        "NVDA", subreddits=("stocks",), inter_request_delay=0,
        screen=lambda texts: ([False, True], "Screened by Jev"),
    )
    assert "buy NVDA" in out and "unrelated" not in out
    assert "Screened by Jev" in out
