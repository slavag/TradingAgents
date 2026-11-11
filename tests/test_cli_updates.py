"""Explicit upstream release checks are bounded, cached, and off by default."""

from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from cli.main import app
from cli.updates import latest_upstream_release


def test_explicit_release_check_uses_bounded_request_and_cache(tmp_path):
    calls = []

    def fetch(url, *, timeout):
        calls.append((url, timeout))
        return SimpleNamespace(
            status_code=200,
            json=lambda: {"tag_name": "v0.7.2"},
            raise_for_status=lambda: None,
        )

    cache = tmp_path / "upstream-release.json"
    first = latest_upstream_release(cache_path=cache, now=1000, request_get=fetch)
    cached = latest_upstream_release(cache_path=cache, now=1001, request_get=fetch)
    refreshed = latest_upstream_release(
        cache_path=cache, now=1002, request_get=fetch, refresh=True
    )
    assert first == cached == refreshed == "v0.7.2"
    assert len(calls) == 2
    assert all(timeout <= 5 for _, timeout in calls)


def test_release_check_rejects_unexpected_tag(tmp_path):
    response = SimpleNamespace(
        json=lambda: {"tag_name": "../../other"},
        raise_for_status=lambda: None,
    )
    with pytest.raises(ValueError, match="release tag"):
        latest_upstream_release(
            cache_path=tmp_path / "cache.json", now=1000,
            request_get=lambda _url, *, timeout: response,
        )


def test_check_updates_command_is_explicit_and_uses_release_target(monkeypatch):
    import cli.updates as updates

    monkeypatch.setattr(updates, "latest_upstream_release", lambda **kw: "v0.7.2")
    result = CliRunner().invoke(app, ["check-updates"])
    assert result.exit_code == 0, result.output
    assert "v0.7.2" in result.output
    assert "v0.7.1" in result.output
