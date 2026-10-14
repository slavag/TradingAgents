"""The web UI exposes the persistent data-source settings workflow."""

from pathlib import Path


def test_settings_ui_has_accessible_fields_and_no_browser_storage():
    root = Path(__file__).resolve().parents[1] / "tradingagents" / "web" / "static"
    html = (root / "index.html").read_text(encoding="utf-8")
    js = (root / "app.js").read_text(encoding="utf-8")

    assert 'id="open-data-settings"' in html
    assert 'id="data-settings-modal"' in html
    for field in (
        "SEC_USER_AGENT",
        "FMP_API_KEY",
        "FRED_API_KEY",
        "ALPHA_VANTAGE_API_KEY",
    ):
        assert f'data-setting-key="{field}"' in html
    assert 'type="password"' in html
    assert 'aria-live="polite"' in html
    assert 'fetch("/api/settings/data-sources"' in js
    assert "localStorage" not in js
    assert "sessionStorage" not in js


def test_secret_values_are_never_inserted_from_settings_status_payload():
    js = (
        Path(__file__).resolve().parents[1]
        / "tradingagents"
        / "web"
        / "static"
        / "app.js"
    ).read_text(encoding="utf-8")

    assert "field.value" not in js
    assert "item.value" in js  # Only the non-secret SEC contact value is returned.
    assert "input.type === \"password\"" in js
