"""Current Kimi model IDs need provider-specific request compatibility."""

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from tradingagents.llm_clients.capabilities import get_capabilities
from tradingagents.llm_clients.openai_client import OpenAIClient


@pytest.mark.parametrize(
    "model",
    ["kimi-k3", "kimi-k2.6", "kimi-k2.7-code", "kimi-k2.7-code-highspeed"],
)
def test_current_kimi_models_omit_fixed_temperature(monkeypatch, model):
    monkeypatch.setenv("MOONSHOT_API_KEY", "test-key")
    llm = OpenAIClient(model, provider="kimi", temperature=0.0).get_llm()
    assert llm.temperature is None


def test_kimi_k3_preserves_reasoning_content_across_tool_turns(monkeypatch):
    monkeypatch.setenv("MOONSHOT_API_KEY", "test-key")
    llm = OpenAIClient("kimi-k3", provider="kimi").get_llm()
    prior = AIMessage(content="Checked filing", additional_kwargs={"reasoning_content": "weighed evidence"})
    payload = llm._get_request_payload([prior, HumanMessage(content="Continue")])
    assistant = next(item for item in payload["messages"] if item.get("role") == "assistant")
    assert assistant["reasoning_content"] == "weighed evidence"


def test_kimi_structured_output_uses_documented_safe_methods():
    assert get_capabilities("kimi-k3").preferred_structured_method == "json_schema"
    assert get_capabilities("kimi-k2.6").supports_tool_choice is False
    assert get_capabilities("kimi-k2.7-code").supports_tool_choice is False
