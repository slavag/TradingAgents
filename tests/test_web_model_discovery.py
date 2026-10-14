"""Web model refresh exposes bounded listing to the local app only."""

import importlib
from unittest.mock import Mock

from fastapi.testclient import TestClient

from tradingagents.llm_clients.api_key_env import PROVIDER_API_KEY_ENV
from tradingagents.llm_clients.model_discovery import DiscoveryResult


def _client(*, remote=False):
    from tradingagents.web.app import app

    return TestClient(
        app,
        base_url="http://127.0.0.1:8000",
        client=("10.0.0.8" if remote else "127.0.0.1", 50123),
    )


def test_local_refresh_returns_bounded_no_store_listing(monkeypatch):
    web_app = importlib.import_module("tradingagents.web.app")
    called = []

    def fake_discover(provider, *, force):
        called.append((provider, force))
        return DiscoveryResult(provider, "fresh", ("gpt-6.1-sol",), "2026-10-13T12:00:00Z", None)

    monkeypatch.setattr(web_app, "discover_models", fake_discover, raising=False)
    response = _client().get("/api/models/openai/refresh")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "provider": "openai", "status": "fresh", "models": ["gpt-6.1-sol"],
        "fetched_at": "2026-10-13T12:00:00Z", "warning": None,
    }
    assert called == [("openai", True)]


def test_cross_origin_and_remote_clients_cannot_trigger_discovery(monkeypatch):
    web_app = importlib.import_module("tradingagents.web.app")
    discover = Mock(side_effect=AssertionError("discovery must not run"))
    monkeypatch.setattr(web_app, "discover_models", discover, raising=False)
    assert _client().get(
        "/api/models/openai/refresh", headers={"Origin": "https://evil.invalid"},
    ).status_code == 403
    assert _client(remote=True).get("/api/models/openai/refresh").status_code == 403
    discover.assert_not_called()


def test_unknown_provider_is_rejected_before_discovery(monkeypatch):
    web_app = importlib.import_module("tradingagents.web.app")
    discover = Mock(side_effect=AssertionError("unknown provider must not reach network"))
    monkeypatch.setattr(web_app, "discover_models", discover, raising=False)
    response = _client().get("/api/models/evil/refresh?url=https://evil.invalid")
    assert response.status_code == 400
    assert "evil.invalid" not in response.text
    discover.assert_not_called()


def test_home_page_does_not_discover_models(monkeypatch):
    web_app = importlib.import_module("tradingagents.web.app")
    discover = Mock(side_effect=AssertionError("page load must not discover models"))
    monkeypatch.setattr(web_app, "discover_models", discover, raising=False)
    response = _client().get("/")
    assert response.status_code == 200
    discover.assert_not_called()


def test_each_runtime_role_shows_every_supported_provider_and_refresh_control():
    html = _client().get("/").text
    for provider in PROVIDER_API_KEY_ENV:
        assert html.count(f'<option value="{provider}"') == 3, provider
    for role in ("quick", "deep", "verifier"):
        assert f'id="refresh-{role}-models"' in html
        assert f'id="{role}-model-refresh-status"' in html
    assert "Curated choices remain available" in html


def test_page_injects_catalog_metadata_and_custom_id_fields():
    html = _client().get("/").text
    assert "window.TRADINGAGENTS_CATALOG_METADATA" in html
    assert "window.TRADINGAGENTS_DISCOVERY_PROVIDERS" in html
    for role in ("quick", "deep", "verifier"):
        assert f'id="{role}-custom-model"' in html
    assert '/static/model_picker.js?v=' in html
