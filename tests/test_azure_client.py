"""Azure deployment configuration reaches the SDK without a live request."""

import pytest

from tradingagents.llm_clients import azure_client


def test_azure_client_uses_tier_endpoint_and_output_token_cap(monkeypatch):
    captured = {}
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://default.example")
    monkeypatch.setattr(
        azure_client, "NormalizedAzureChatOpenAI",
        lambda **kwargs: captured.update(kwargs) or object(),
    )
    client = azure_client.AzureOpenAIClient(
        model="deployment-a", base_url="https://tier.example",
        max_tokens=3072, api_key="placeholder",
    )
    client.get_llm()
    assert captured["azure_endpoint"] == "https://tier.example"
    assert captured["max_tokens"] == 3072
    assert captured["api_key"] == "placeholder"


def test_azure_client_uses_endpoint_env_when_tier_url_absent(monkeypatch):
    captured = {}
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://default.example")
    monkeypatch.setattr(
        azure_client, "NormalizedAzureChatOpenAI",
        lambda **kwargs: captured.update(kwargs) or object(),
    )
    azure_client.AzureOpenAIClient(model="deployment-a", api_key="x").get_llm()
    assert captured["azure_endpoint"] == "https://default.example"


@pytest.mark.parametrize("value", [0, -1, True])
def test_azure_client_rejects_invalid_output_token_cap(monkeypatch, value):
    monkeypatch.setattr(
        azure_client, "NormalizedAzureChatOpenAI",
        lambda **kwargs: object(),
    )
    with pytest.raises(ValueError, match="max_tokens"):
        azure_client.AzureOpenAIClient(
            model="deployment-a", max_tokens=value, api_key="x"
        ).get_llm()
