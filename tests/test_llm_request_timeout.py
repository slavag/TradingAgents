"""Stalled model calls have a finite network timeout and one bounded retry."""

import json

import httpx
import pytest
from openai import APITimeoutError

from tradingagents.llm_clients.factory import create_llm_client
from tradingagents.llm_clients.openai_client import NormalizedChatOpenAI


def test_openai_client_defaults_to_finite_timeout_and_one_retry(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "offline-test-key")
    llm = create_llm_client(provider="openai", model="gpt-4.1").get_llm()
    assert llm.request_timeout == 300
    assert llm.max_retries == 1
    assert getattr(llm.root_client.timeout, "read", llm.root_client.timeout) == 300
    assert llm.root_client.max_retries == 1


def test_explicit_timeout_and_retry_values_are_preserved(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "offline-test-key")
    llm = create_llm_client(provider="openai", model="gpt-4.1", timeout=90, max_retries=0).get_llm()
    assert llm.request_timeout == 90
    assert llm.max_retries == 0


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), True])
def test_invalid_numeric_timeout_cannot_disable_the_guard(monkeypatch, value):
    monkeypatch.setenv("OPENAI_API_KEY", "offline-test-key")
    with pytest.raises(ValueError, match="timeout"):
        create_llm_client(provider="openai", model="gpt-4.1", timeout=value).get_llm()


def test_timeout_retries_the_same_request_then_returns_response():
    requests = []

    def transport(request):
        requests.append(json.loads(request.content))
        assert request.extensions["timeout"]["read"] == 2
        if len(requests) == 1:
            raise httpx.ReadTimeout("offline stalled response", request=request)
        return httpx.Response(
            200,
            json={
                "id": "offline",
                "object": "chat.completion",
                "created": 0,
                "model": "gpt-4.1",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "Recovered."},
                    }
                ],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(transport)) as http:
        llm = NormalizedChatOpenAI(
            model="gpt-4.1",
            api_key="offline-test-key",
            base_url="https://offline.example/v1",
            http_client=http,
            timeout=2,
            max_retries=1,
        )
        assert (
            llm.invoke("Continue from the already-collected financial statements.").content
            == "Recovered."
        )
    assert len(requests) == 2
    assert requests[0] == requests[1]


def test_second_timeout_stops_instead_of_retrying_forever():
    requests = []

    def transport(request):
        requests.append(request)
        raise httpx.ReadTimeout("offline stalled response", request=request)

    with httpx.Client(transport=httpx.MockTransport(transport)) as http:
        llm = NormalizedChatOpenAI(
            model="gpt-4.1",
            api_key="offline-test-key",
            base_url="https://offline.example/v1",
            http_client=http,
            timeout=2,
            max_retries=1,
        )
        with pytest.raises(APITimeoutError):
            llm.invoke("Collected context is preserved.")
    assert len(requests) == 2


def test_timeout_env_reaches_openai_role_configuration(monkeypatch):
    from tradingagents.default_config import DEFAULT_CONFIG, _apply_env_overrides
    from tradingagents.graph.trading_graph import TradingAgentsGraph

    monkeypatch.setenv("TRADINGAGENTS_LLM_REQUEST_TIMEOUT_SECONDS", "75")
    config = _apply_env_overrides(DEFAULT_CONFIG.copy())
    graph = TradingAgentsGraph.__new__(TradingAgentsGraph)
    graph.config = config
    assert graph._get_provider_kwargs("openai")["timeout"] == 75
    assert "timeout" not in graph._get_provider_kwargs("google")


def test_timeout_error_is_reported_without_aborting_other_tickers():
    from tradingagents.web.service import classify_runtime_error

    error = classify_runtime_error(TimeoutError("The provider request timed out."))
    assert error["kind"] == "request_timeout"
    assert error["fatal_for_batch"] is False
    assert "timed out" in error["user_message"]
