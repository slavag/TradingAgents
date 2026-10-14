"""CLI account-model refresh is explicit and preserves exact custom IDs."""

from unittest.mock import Mock

import pytest

from cli import utils
from tradingagents.llm_clients.model_discovery import DiscoveryResult


def _answer(value):
    return Mock(ask=Mock(return_value=value))


@pytest.fixture(autouse=True)
def forbid_legacy_automatic_fetch(monkeypatch):
    monkeypatch.setattr(
        utils, "_fetch_openrouter_models",
        Mock(side_effect=AssertionError("legacy automatic fetch must not run")),
        raising=False,
    )


def test_openrouter_custom_choice_does_not_discover(monkeypatch):
    discover = Mock(side_effect=AssertionError("discovery must be explicit"))
    monkeypatch.setattr(utils, "discover_models", discover, raising=False)
    monkeypatch.setattr(utils.questionary, "select", lambda *_args, **_kwargs: _answer("custom"))
    monkeypatch.setattr(utils.questionary, "text", lambda *_args, **_kwargs: _answer("account/chat"))
    assert utils._select_model("openrouter", "quick") == "account/chat"
    discover.assert_not_called()


def test_refresh_adds_account_model_below_curated_and_only_selected_provider(monkeypatch):
    calls = []
    menus = []
    answers = iter(["refresh_models", "account/chat"])

    def fake_discover(provider, *, force):
        calls.append((provider, force))
        return DiscoveryResult(provider, "fresh", ("account/chat",), "2026-10-13T17:00:00Z", None)

    def fake_select(message, **kwargs):
        menus.append((message, [choice.value for choice in kwargs["choices"]]))
        return _answer(next(answers))

    monkeypatch.setattr(utils, "discover_models", fake_discover, raising=False)
    monkeypatch.setattr(utils.questionary, "select", fake_select)
    assert utils._select_model("openai", "deep") == "account/chat"
    assert calls == [("openai", True)]
    assert menus[0][1][0] == "gpt-6.1-sol"
    assert menus[1][1].index("account/chat") > menus[1][1].index("gpt-6.1-sol")


def test_failed_refresh_falls_back_to_curated_and_never_prints_secret(monkeypatch, capsys):
    answers = iter(["refresh_models", "custom"])
    monkeypatch.setattr(
        utils, "discover_models",
        lambda provider, *, force: DiscoveryResult(provider, "unavailable", (), None, "unauthorized"),
        raising=False,
    )
    monkeypatch.setattr(
        utils.questionary, "select", lambda *_args, **_kwargs: _answer(next(answers)),
    )
    monkeypatch.setattr(utils.questionary, "text", lambda *_args, **_kwargs: _answer("my/exact-id"))
    assert utils._select_model("openrouter", "quick") == "my/exact-id"
    output = capsys.readouterr().out
    assert "test-secret" not in output
    assert "unavailable" in output.lower()


def test_cancel_after_refresh_exits_cleanly(monkeypatch):
    answers = iter(["refresh_models", None])
    monkeypatch.setattr(
        utils, "discover_models",
        lambda provider, *, force: DiscoveryResult(provider, "fresh", (), "2026-10-13T17:00:00Z", None),
        raising=False,
    )
    monkeypatch.setattr(
        utils.questionary, "select", lambda *_args, **_kwargs: _answer(next(answers)),
    )
    with pytest.raises(SystemExit):
        utils._select_model("openai", "quick")


def test_unsupported_provider_keeps_custom_without_refresh_action(monkeypatch):
    menu = []
    monkeypatch.setattr(utils, "discover_models", Mock(side_effect=AssertionError("no request")), raising=False)

    def fake_select(_message, **kwargs):
        menu.extend(choice.value for choice in kwargs["choices"])
        return _answer("custom")

    monkeypatch.setattr(utils.questionary, "select", fake_select)
    monkeypatch.setattr(utils.questionary, "text", lambda *_args, **_kwargs: _answer("deployment-name"))
    assert utils._select_model("bedrock", "quick") == "deployment-name"
    assert "refresh_models" not in menu


def test_cli_labels_lifecycle_and_deduplicates_account_ids():
    mistral = utils.merge_curated_and_available("mistral", "deep", ())
    by_id = {choice.value: choice.title for choice in mistral}
    assert "preview" in by_id["mistral-large-4"].lower()
    assert "recommended" in by_id["mistral-small-2603"].lower()

    choices = utils.merge_curated_and_available("openai", "quick", ("account/chat", "account/chat"))
    assert [choice.value for choice in choices].count("account/chat") == 1


def test_verifier_role_uses_its_own_ranked_suggestions(monkeypatch):
    captured = {}

    def fake_select(_message, **kwargs):
        captured["values"] = [choice.value for choice in kwargs["choices"]]
        return _answer("gpt-6.1-sol")

    monkeypatch.setattr(utils.questionary, "select", fake_select)
    monkeypatch.setattr(utils, "discover_models", Mock(side_effect=AssertionError("not refreshed")))
    assert utils.select_verifier_model("openai") == "gpt-6.1-sol"
    assert captured["values"][0] == "gpt-6.1-sol"
    assert "refresh_models" in captured["values"]
