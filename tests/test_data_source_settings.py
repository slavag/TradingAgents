"""Persistent data-source settings are local, secret-safe, and used immediately."""

from __future__ import annotations

import json
import stat

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def settings_dir(tmp_path, monkeypatch):
    path = tmp_path / "settings"
    monkeypatch.setenv("TRADINGAGENTS_SETTINGS_DIR", str(path))
    for key in (
        "SEC_USER_AGENT",
        "FMP_API_KEY",
        "FRED_API_KEY",
        "ALPHA_VANTAGE_API_KEY",
    ):
        monkeypatch.delenv(key, raising=False)
    return path


def test_saved_credentials_persist_with_owner_only_permissions(settings_dir):
    from tradingagents.settings import credential_status, update_credentials

    settings_dir.mkdir(mode=0o777)
    settings_dir.chmod(0o777)
    update_credentials(
        {
            "SEC_USER_AGENT": "TradingAgents owner@example.com",
            "FMP_API_KEY": "fmp-secret",
        },
        [],
    )

    path = settings_dir / "data_sources.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload == {
        "FMP_API_KEY": "fmp-secret",
        "SEC_USER_AGENT": "TradingAgents owner@example.com",
    }
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(settings_dir.stat().st_mode) == 0o700

    status = {item["key"]: item for item in credential_status()}
    assert status["FMP_API_KEY"] == {
        **{key: status["FMP_API_KEY"][key] for key in status["FMP_API_KEY"] if key != "value"},
    }
    assert "value" not in status["FMP_API_KEY"]
    assert status["FMP_API_KEY"]["configured"] is True
    assert status["FMP_API_KEY"]["origin"] == "saved"
    assert status["SEC_USER_AGENT"]["value"] == "TradingAgents owner@example.com"


def test_saved_value_overrides_environment_and_remove_falls_back(settings_dir, monkeypatch):
    from tradingagents.settings import get_data_source_setting, update_credentials

    monkeypatch.setenv("FRED_API_KEY", "environment-key")
    update_credentials({"FRED_API_KEY": "saved-key"}, [])
    assert get_data_source_setting("FRED_API_KEY") == "saved-key"

    update_credentials({}, ["FRED_API_KEY"])
    assert get_data_source_setting("FRED_API_KEY") == "environment-key"


@pytest.mark.parametrize(
    ("values", "remove"),
    [
        ({"UNKNOWN_KEY": "value"}, []),
        ({"FMP_API_KEY": "line1\nline2"}, []),
        ({"SEC_USER_AGENT": "missing-contact"}, []),
        ({"FRED_API_KEY": 123}, []),
        ({"FRED_API_KEY": "same"}, ["FRED_API_KEY"]),
    ],
)
def test_invalid_updates_do_not_replace_existing_credentials(settings_dir, values, remove):
    from tradingagents.settings import SettingsValidationError, update_credentials

    update_credentials({"FMP_API_KEY": "existing"}, [])
    with pytest.raises(SettingsValidationError):
        update_credentials(values, remove)
    assert json.loads((settings_dir / "data_sources.json").read_text())["FMP_API_KEY"] == "existing"


def test_all_source_providers_resolve_saved_settings(settings_dir, monkeypatch):
    from tradingagents.dataflows import alpha_vantage_common, fmp, fred, sec
    from tradingagents.settings import update_credentials

    update_credentials(
        {
            "SEC_USER_AGENT": "TradingAgents owner@example.com",
            "FMP_API_KEY": "saved-fmp",
            "FRED_API_KEY": "saved-fred",
            "ALPHA_VANTAGE_API_KEY": "saved-av",
        },
        [],
    )
    assert sec._user_agent() == "TradingAgents owner@example.com"
    assert fred.get_api_key() == "saved-fred"
    assert alpha_vantage_common.get_api_key() == "saved-av"

    captured = []

    def fake_request(path, params, api_key, endpoint_name):
        captured.append(api_key)
        return []

    monkeypatch.setattr(fmp, "_request_list", fake_request)
    fmp.get_earnings_context("AAPL", __import__("datetime").date.today().isoformat())
    assert captured and set(captured) == {"saved-fmp"}


def test_settings_api_never_returns_secret_values(settings_dir):
    from tradingagents.settings import update_credentials
    from tradingagents.web.app import app

    update_credentials({"FMP_API_KEY": "must-not-leak"}, [])
    client = TestClient(
        app,
        base_url="http://127.0.0.1:8000",
        client=("127.0.0.1", 50123),
    )
    response = client.get("/api/settings/data-sources")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert "must-not-leak" not in response.text
    fmp_status = next(item for item in response.json()["fields"] if item["key"] == "FMP_API_KEY")
    assert fmp_status["configured"] is True
    assert fmp_status["origin"] == "saved"


def test_settings_api_updates_and_removes_saved_values(settings_dir):
    from tradingagents.web.app import app

    client = TestClient(
        app,
        base_url="http://127.0.0.1:8000",
        client=("127.0.0.1", 50123),
    )
    headers = {"Origin": "http://127.0.0.1:8000", "Content-Type": "application/json"}
    saved = client.put(
        "/api/settings/data-sources",
        headers=headers,
        json={"values": {"FMP_API_KEY": "browser-secret"}, "remove": []},
    )
    assert saved.status_code == 200
    assert "browser-secret" not in saved.text

    removed = client.put(
        "/api/settings/data-sources",
        headers=headers,
        json={"values": {}, "remove": ["FMP_API_KEY"]},
    )
    assert removed.status_code == 200
    assert next(
        item for item in removed.json()["fields"] if item["key"] == "FMP_API_KEY"
    )["configured"] is False


def test_settings_api_rejects_cross_origin_and_nonlocal_clients(settings_dir):
    from tradingagents.web.app import app

    local = TestClient(app, base_url="http://127.0.0.1:8000", client=("127.0.0.1", 50000))
    cross_origin = local.put(
        "/api/settings/data-sources",
        headers={"Origin": "https://attacker.example", "Content-Type": "application/json"},
        json={"values": {"FMP_API_KEY": "secret"}, "remove": []},
    )
    assert cross_origin.status_code == 403
    assert "secret" not in cross_origin.text

    remote = TestClient(app, base_url="http://127.0.0.1:8000", client=("10.0.0.8", 50000))
    assert remote.get("/api/settings/data-sources").status_code == 403


def test_update_rejects_a_non_dedicated_settings_directory(settings_dir):
    from tradingagents.settings import SettingsStorageError, update_credentials

    settings_dir.mkdir(mode=0o755)
    (settings_dir / "unrelated.txt").write_text("keep")
    with pytest.raises(SettingsStorageError):
        update_credentials({"FMP_API_KEY": "secret"}, [])
    assert stat.S_IMODE(settings_dir.stat().st_mode) == 0o755
    assert (settings_dir / "unrelated.txt").read_text() == "keep"


def test_lock_timeout_is_a_generic_storage_error(settings_dir, monkeypatch):
    import tradingagents.settings as settings

    class Locked:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            raise settings.FileLockTimeout("busy")

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(settings, "FileLock", Locked)
    with pytest.raises(settings.SettingsStorageError, match="Could not save"):
        settings.update_credentials({"FMP_API_KEY": "secret"}, [])
