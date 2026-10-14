"""Read-only account-model listing with fixed endpoints and bounded output."""

import importlib.util
import json
from unittest.mock import Mock

import pytest
import requests

from tradingagents.llm_clients.api_key_env import PROVIDER_API_KEY_ENV


class FakeResponse:
    def __init__(self, payload=None, *, status=200, raw=None, headers=None):
        self.status_code = status
        self.headers = headers or {}
        self.body = raw if raw is not None else json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.body), chunk_size):
            yield self.body[offset:offset + chunk_size]


@pytest.fixture
def discovery(monkeypatch):
    assert importlib.util.find_spec("tradingagents.llm_clients.model_discovery"), (
        "model discovery service is missing"
    )
    from tradingagents.llm_clients import model_discovery

    model_discovery._CACHE.clear()
    for provider, env_name in PROVIDER_API_KEY_ENV.items():
        if env_name and provider != "openai_compatible":
            monkeypatch.setenv(env_name, "test-secret-" + provider)
    return model_discovery


@pytest.mark.parametrize(("provider", "payload", "expected", "endpoint"), [
    ("openai", {"data": [{"id": "gpt-6.1-sol"}, {"id": "text-embedding-3-large"}]},
     "gpt-6.1-sol", "https://api.openai.com/v1/models"),
    ("anthropic", {"data": [{"id": "claude-sonnet-5-5"}], "has_more": False},
     "claude-sonnet-5-5", "https://api.anthropic.com/v1/models"),
    ("google", {"models": [{"name": "models/gemini-3.8-flash",
                            "supportedGenerationMethods": ["generateContent"]}]},
     "gemini-3.8-flash", "https://generativelanguage.googleapis.com/v1beta/models"),
    ("xai", {"models": [{"id": "grok-4.7"}]},
     "grok-4.7", "https://api.x.ai/v1/language-models"),
    ("deepseek", {"data": [{"id": "deepseek-flash"}]},
     "deepseek-flash", "https://api.deepseek.com/models"),
    ("meta", {"data": [{"id": "muse-spark-1.3"}]},
     "muse-spark-1.3", "https://api.meta.ai/v1/models"),
    ("openrouter", {"data": [{"id": "openai/gpt-6-luna",
                               "architecture": {"modality": "text->text"}}]},
     "openai/gpt-6-luna", "https://openrouter.ai/api/v1/models"),
    ("mistral", {"data": [{"id": "mistral-small-2603"}]},
     "mistral-small-2603", "https://api.mistral.ai/v1/models"),
    ("groq", {"data": [{"id": "openai/gpt-oss-20b", "active": True},
                       {"id": "whisper-large-v3", "active": True}]},
     "openai/gpt-oss-20b", "https://api.groq.com/openai/v1/models"),
    ("ollama", {"models": [{"name": "qwen3:latest"}]},
     "qwen3:latest", "http://127.0.0.1:11434/api/tags"),
])
def test_provider_listing_uses_only_fixed_read_endpoint(
    monkeypatch, discovery, provider, payload, expected, endpoint,
):
    monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return FakeResponse(payload)

    monkeypatch.setattr(requests, "get", fake_get)
    result = discovery.discover_models(provider, force=True)
    assert result.status == "fresh"
    assert result.models == (expected,)
    assert result.fetched_at
    assert result.warning is None
    assert calls[0][0] == endpoint
    assert calls[0][1]["allow_redirects"] is False
    assert calls[0][1]["stream"] is True
    assert calls[0][1]["timeout"] == (3, 7)
    assert "test-secret" not in str(result)


def test_missing_credentials_never_call_network(monkeypatch, discovery):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    called = Mock(side_effect=AssertionError("network must not be called"))
    monkeypatch.setattr(requests, "get", called)
    result = discovery.discover_models("openai", force=True)
    assert (result.status, result.models, result.warning) == (
        "unavailable", (), "missing_credentials",
    )
    called.assert_not_called()


def test_unknown_and_unsupported_provider_never_call_network(monkeypatch, discovery):
    called = Mock(side_effect=AssertionError("network must not be called"))
    monkeypatch.setattr(requests, "get", called)
    with pytest.raises(ValueError, match="unknown_provider"):
        discovery.discover_models("evil", force=True)
    result = discovery.discover_models("azure", force=True)
    assert (result.status, result.warning) == ("unsupported", "listing_not_supported")
    called.assert_not_called()


@pytest.mark.parametrize("status", [302, 303, 307])
def test_redirect_is_not_followed_or_exposed(monkeypatch, discovery, status):
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return FakeResponse(status=status, headers={"Location": "https://evil.invalid/steal"})

    monkeypatch.setattr(requests, "get", fake_get)
    result = discovery.discover_models("openai", force=True)
    assert result.warning == "provider_unavailable"
    assert result.models == ()
    assert "evil.invalid" not in str(result)
    assert len(calls) == 1 and calls[0][1]["allow_redirects"] is False


@pytest.mark.parametrize(("response", "warning"), [
    (FakeResponse(status=401, raw=b"secret-provider-error"), "unauthorized"),
    (FakeResponse(raw=b"not json"), "malformed_response"),
    (FakeResponse(raw=b"x" * (2 * 1024 * 1024 + 1)), "response_too_large"),
])
def test_bad_responses_return_bounded_warnings(monkeypatch, discovery, response, warning):
    monkeypatch.setattr(requests, "get", lambda *_args, **_kwargs: response)
    result = discovery.discover_models("openai", force=True)
    assert result.warning == warning
    assert result.models == ()
    assert "secret-provider-error" not in str(result)


def test_timeout_does_not_expose_request_or_key(monkeypatch, discovery):
    def timed_out(*_args, **_kwargs):
        raise requests.Timeout("test-secret-openai https://api.openai.com/v1/models")

    monkeypatch.setattr(requests, "get", timed_out)
    result = discovery.discover_models("openai", force=True)
    assert result.warning == "provider_unavailable"
    assert "test-secret" not in str(result)


def test_duplicate_ids_and_oversized_listing_are_capped(monkeypatch, discovery):
    ids = [{"id": f"gpt-{index}"} for index in range(10_000)]
    ids.insert(1, {"id": "gpt-0"})
    monkeypatch.setattr(requests, "get", lambda *_args, **_kwargs: FakeResponse({"data": ids}))
    result = discovery.discover_models("openai", force=True)
    assert len(result.models) == 250
    assert result.models[:2] == ("gpt-0", "gpt-1")


def test_cache_is_account_scoped_and_user_refresh_bypasses_cache(monkeypatch, discovery):
    calls = []

    def fake_get(*_args, **_kwargs):
        calls.append(1)
        return FakeResponse({"data": [{"id": "gpt-6-luna"}]})

    monkeypatch.setattr(requests, "get", fake_get)
    assert discovery.discover_models("openai", force=False).status == "fresh"
    assert discovery.discover_models("openai", force=False).status == "cached"
    assert len(calls) == 1
    monkeypatch.setenv("OPENAI_API_KEY", "second-account-secret")
    assert discovery.discover_models("openai", force=False).status == "fresh"
    assert len(calls) == 2
    assert discovery.discover_models("openai", force=True).status == "fresh"
    assert len(calls) == 3


def test_anthropic_pagination_uses_bounded_cursor(monkeypatch, discovery):
    pages = [
        FakeResponse({"data": [{"id": "claude-haiku-5-5"}], "has_more": True,
                      "last_id": "page-one"}),
        FakeResponse({"data": [{"id": "claude-sonnet-5-5"}], "has_more": False}),
    ]
    calls = []

    def fake_get(_url, **kwargs):
        calls.append(kwargs)
        return pages.pop(0)

    monkeypatch.setattr(requests, "get", fake_get)
    result = discovery.discover_models("anthropic", force=True)
    assert result.models == ("claude-haiku-5-5", "claude-sonnet-5-5")
    assert calls[1]["params"] == {"after_id": "page-one"}


def test_repeated_page_cursor_fails_closed(monkeypatch, discovery):
    calls = []

    def fake_get(_url, **kwargs):
        calls.append(kwargs)
        return FakeResponse({"data": [{"id": "claude-haiku-5-5"}],
                             "has_more": True, "last_id": "same-token"})

    monkeypatch.setattr(requests, "get", fake_get)
    result = discovery.discover_models("anthropic", force=True)
    assert result.warning == "malformed_response"
    assert len(calls) == 2


def test_malformed_openrouter_architecture_is_bounded_failure(monkeypatch, discovery):
    monkeypatch.setattr(
        requests, "get",
        lambda *_args, **_kwargs: FakeResponse({"data": [
            {"id": "provider/chat", "architecture": None},
        ]}),
    )
    result = discovery.discover_models("openrouter", force=True)
    assert result.warning == "malformed_response"
    assert result.models == ()


def test_openrouter_image_only_model_is_not_exposed(monkeypatch, discovery):
    monkeypatch.setattr(
        requests, "get",
        lambda *_args, **_kwargs: FakeResponse({"data": [
            {"id": "provider/render", "architecture": {"output_modalities": ["image"]}},
            {"id": "provider/chat", "architecture": {"output_modalities": ["text"]}},
        ]}),
    )
    result = discovery.discover_models("openrouter", force=True)
    assert result.models == ("provider/chat",)


def test_google_key_is_header_only_and_never_returned(monkeypatch, discovery):
    captured = {}

    def fake_get(url, **kwargs):
        captured["url"] = url
        captured["headers"] = kwargs["headers"]
        return FakeResponse({"models": [{"name": "models/gemini-3.8-flash"}]})

    monkeypatch.setattr(requests, "get", fake_get)
    result = discovery.discover_models("google", force=True)
    assert captured["headers"]["x-goog-api-key"] == "test-secret-google"
    assert "test-secret-google" not in captured["url"]
    assert "test-secret-google" not in str(result)


@pytest.mark.parametrize("base_url", [
    "http://localhost.evil.invalid:11434/v1",
    "http://192.168.1.9:11434/v1",
    "https://localhost:11434/v1",
    "http://localhost:9999/v1",
])
def test_remote_or_nonstandard_ollama_is_not_probed(monkeypatch, discovery, base_url):
    monkeypatch.setenv("OLLAMA_BASE_URL", base_url)
    called = Mock(side_effect=AssertionError("network must not be called"))
    monkeypatch.setattr(requests, "get", called)
    result = discovery.discover_models("ollama", force=True)
    assert (result.status, result.warning) == ("unsupported", "listing_not_supported")
    called.assert_not_called()
