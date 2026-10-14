"""Provider-native prompt-prefix caching never reuses completed answers."""

import importlib

import pytest
from langchain_core.messages import HumanMessage

pytestmark = pytest.mark.unit


class RecordingModel:
    def __init__(self):
        self.calls = []

    def invoke(self, prompt, **kwargs):
        self.calls.append((prompt, kwargs))
        return "answer"


def _module():
    try:
        return importlib.import_module("tradingagents.llm_clients.prompt_cache")
    except ModuleNotFoundError:
        pytest.fail("provider-native prompt cache helper is missing")


@pytest.mark.parametrize("provider", ["anthropic", "openai", "azure", "google", "deepseek"])
def test_single_use_prompt_keeps_original_string_request(provider):
    cache = _module()
    model = RecordingModel()
    policy = cache.PromptCachePolicy(provider=provider, model="claude-sonnet-5")

    response = cache.invoke_with_prompt_cache(model, "stable prefix", "dynamic suffix", policy)

    assert response == "answer"
    assert model.calls == [("stable prefixdynamic suffix", {})]


def test_reused_eligible_anthropic_prefix_marks_only_stable_block():
    cache = _module()
    model = RecordingModel()
    policy = cache.PromptCachePolicy(
        provider="anthropic", model="claude-sonnet-5", expected_reuse=True,
    )

    cache.invoke_with_prompt_cache(model, "A" * 5000, "new debate turn", policy)

    prompt, kwargs = model.calls[0]
    assert kwargs == {}
    assert isinstance(prompt, list) and len(prompt) == 1
    assert isinstance(prompt[0], HumanMessage)
    stable, dynamic = prompt[0].content
    assert stable == {
        "type": "text", "text": "A" * 5000,
        "cache_control": {"type": "ephemeral"},
    }
    assert dynamic == {"type": "text", "text": "new debate turn"}


@pytest.mark.parametrize("overrides", [
    {"model": "unknown-claude"},
    {"base_url": "https://proxy.example"},
    {"enabled": False},
])
def test_unverified_anthropic_cache_eligibility_uses_plain_prompt(overrides):
    cache = _module()
    model = RecordingModel()
    fields = {"provider": "anthropic", "model": "claude-sonnet-5",
              "expected_reuse": True, **overrides}

    cache.invoke_with_prompt_cache(model, "A" * 5000, "dynamic", cache.PromptCachePolicy(**fields))

    assert model.calls == [("A" * 5000 + "dynamic", {})]


def test_short_anthropic_prefix_uses_plain_prompt():
    cache = _module()
    model = RecordingModel()

    cache.invoke_with_prompt_cache(
        model, "short", "dynamic",
        cache.PromptCachePolicy("anthropic", "claude-sonnet-5", expected_reuse=True),
    )

    assert model.calls == [("shortdynamic", {})]


def test_reused_native_openai_prefix_gets_stable_non_sensitive_key():
    cache = _module()
    model = RecordingModel()
    policy = cache.PromptCachePolicy("openai", "gpt-5.6-sol", expected_reuse=True)

    cache.invoke_with_prompt_cache(model, "stable prompt", "first", policy)
    cache.invoke_with_prompt_cache(model, "stable prompt", "second", policy)

    assert model.calls[0][0] == "stable promptfirst"
    assert model.calls[1][0] == "stable promptsecond"
    first_key = model.calls[0][1]["prompt_cache_key"]
    assert first_key == model.calls[1][1]["prompt_cache_key"]
    assert first_key.startswith("ta-")
    assert "stable prompt" not in first_key


@pytest.mark.parametrize("provider", ["azure", "google", "deepseek", "openrouter"])
def test_non_openai_repeated_provider_never_gets_openai_cache_key(provider):
    cache = _module()
    model = RecordingModel()

    cache.invoke_with_prompt_cache(
        model, "stable prompt", "dynamic",
        cache.PromptCachePolicy(provider, "model", expected_reuse=True),
    )

    assert model.calls == [("stable promptdynamic", {})]
