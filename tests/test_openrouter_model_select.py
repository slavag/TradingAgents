"""OpenRouter uses the shared explicit-refresh picker and preserves cancellation."""

from unittest import mock

import pytest

from cli import utils
from tradingagents.llm_clients.model_catalog import get_model_options


def _asks(value):
    return mock.Mock(ask=mock.Mock(return_value=value))


@pytest.mark.unit
@pytest.mark.parametrize("mode,label", [("quick", "Quick-Thinking"), ("deep", "Deep-Thinking")])
def test_prompt_states_the_mode_without_automatic_discovery(mode, label):
    captured = {}

    def fake_select(message, **kwargs):
        captured["message"] = message
        captured["values"] = [choice.value for choice in kwargs["choices"]]
        return _asks("custom")

    with mock.patch.object(utils, "discover_models", side_effect=AssertionError("no refresh")), \
         mock.patch.object(utils.questionary, "select", side_effect=fake_select), \
         mock.patch.object(utils.questionary, "text", return_value=_asks("openai/gpt-6-luna")):
        out = utils.select_openrouter_model(mode)

    assert label in captured["message"]
    assert "refresh_models" in captured["values"]
    assert out == "openai/gpt-6-luna"


@pytest.mark.unit
def test_mainstream_models_are_first_but_niche_remain_selectable():
    choices = utils.merge_curated_and_available(
        "openrouter", "quick",
        ("nex-agi/nex-n2-pro:free", "anthropic/claude-x", "openai/gpt-x"),
    )
    values = [choice.value for choice in choices]
    assert values.index("anthropic/claude-x") < values.index("nex-agi/nex-n2-pro:free")
    assert values.index("openai/gpt-x") < values.index("nex-agi/nex-n2-pro:free")
    assert "nex-agi/nex-n2-pro:free" in values
    assert "custom" in values


@pytest.mark.unit
def test_refresh_deduplicates_curated_model_id():
    curated_id = get_model_options("openrouter", "quick")[0][1]
    choices = utils.merge_curated_and_available("openrouter", "quick", (curated_id, curated_id))
    assert [choice.value for choice in choices].count(curated_id) == 1


@pytest.mark.unit
class TestCancelExitsCleanly:
    def test_dropdown_cancel_exits(self):
        with mock.patch.object(utils.questionary, "select", return_value=_asks(None)), \
             pytest.raises(SystemExit):
            utils.select_openrouter_model("quick")

    def test_custom_id_cancel_exits(self):
        with mock.patch.object(utils.questionary, "select", return_value=_asks("custom")), \
             mock.patch.object(utils.questionary, "text", return_value=_asks(None)), \
             pytest.raises(SystemExit):
            utils.select_openrouter_model("deep")

    def test_prompt_custom_model_id_cancel_exits(self):
        with mock.patch.object(utils.questionary, "text", return_value=_asks(None)), \
             pytest.raises(SystemExit):
            utils._prompt_custom_model_id()


@pytest.mark.unit
class TestLanguageDefaultsToEnglish:
    def test_select_cancel_defaults_english(self):
        with mock.patch.object(utils.questionary, "select", return_value=_asks(None)):
            assert utils.ask_output_language() == "English"

    def test_custom_language_cancel_defaults_english(self):
        with mock.patch.object(utils.questionary, "select", return_value=_asks("custom")), \
             mock.patch.object(utils.questionary, "text", return_value=_asks(None)):
            assert utils.ask_output_language() == "English"
