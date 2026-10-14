"""Meta Model API provider integration tests."""

import pytest
from pydantic import BaseModel

from tradingagents.llm_clients.factory import create_llm_client


@pytest.mark.unit
def test_meta_provider_uses_official_endpoint_and_key(monkeypatch):
    monkeypatch.setenv("MODEL_API_KEY", "LLM|123|test-secret")

    llm = create_llm_client("meta", "muse-spark-1.2").get_llm()

    assert type(llm).__name__ == "NormalizedChatOpenAI"
    assert str(llm.openai_api_base).rstrip("/") == "https://api.meta.ai/v1"
    assert llm.openai_api_key.get_secret_value() == "LLM|123|test-secret"
    assert getattr(llm, "use_responses_api", False) in (False, None)


@pytest.mark.unit
def test_meta_provider_requires_model_api_key(monkeypatch):
    monkeypatch.delenv("MODEL_API_KEY", raising=False)

    with pytest.raises(ValueError, match="MODEL_API_KEY"):
        create_llm_client("meta", "muse-spark-1.2").get_llm()


@pytest.mark.unit
def test_muse_structured_output_uses_json_schema(monkeypatch):
    class Decision(BaseModel):
        action: str

    monkeypatch.setenv("MODEL_API_KEY", "LLM|123|test-secret")
    llm = create_llm_client("meta", "muse-spark-1.2").get_llm()

    runnable = llm.with_structured_output(Decision)
    first = runnable.steps[0] if hasattr(runnable, "steps") else runnable
    kwargs = getattr(first, "kwargs", {})

    assert "response_format" in kwargs
    assert kwargs["ls_structured_output_format"]["kwargs"]["method"] == "json_schema"
    assert "tools" not in kwargs
