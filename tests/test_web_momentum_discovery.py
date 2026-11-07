"""Local API access and payload contract for Discover, without external IO."""

from importlib import import_module

from fastapi.testclient import TestClient


def app_module():
    return import_module("tradingagents.web.app")


def test_discover_route_is_local_only_and_rejects_other_limits(monkeypatch):
    module = app_module()
    called = []

    def fake_watchlist(*, limit, force):
        called.append((limit, force))
        return {"status": "fresh", "retrieved_at": "2026-10-16T12:00:00+00:00",
                "quote_as_of": "2026-10-15", "items": [], "provider_status": {}, "warnings": []}

    monkeypatch.setattr(module, "get_momentum_watchlist", fake_watchlist)
    local = TestClient(module.app, base_url="http://127.0.0.1:8000", client=("127.0.0.1", 50001))
    remote = TestClient(module.app, base_url="http://127.0.0.1:8000", client=("10.0.0.2", 50002))
    response = local.get("/api/discover/stocks?limit=15&refresh=true")
    assert response.status_code == 200
    assert response.json()["status"] == "fresh"
    assert called == [(15, True)]
    assert response.headers["cache-control"] == "no-store"
    assert local.get("/api/discover/stocks?limit=16").status_code == 422
    assert remote.get("/api/discover/stocks").status_code == 403
    assert local.get("/api/discover/stocks", headers={"Origin": "https://other.example"}).status_code == 403
    assert local.get("/api/discover/stocks", headers={"Sec-Fetch-Site": "cross-site"}).status_code == 403
    assert called == [(15, True)]
