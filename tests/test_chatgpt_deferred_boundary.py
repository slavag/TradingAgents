"""No implicit ChatGPT-plan token path while sign-in integration is deferred."""

from pathlib import Path

import pytest
import requests

from tradingagents.llm_clients.factory import create_llm_client
from tradingagents.llm_clients.model_catalog import get_web_model_options


def test_plan_provider_is_not_exposed_by_unprotected_web_jobs(monkeypatch):
    original_read = Path.read_text

    def watched_read(path, *args, **kwargs):
        if path.name == "chatgpt-auth.json":
            raise AssertionError("ordinary provider selection read a plan token")
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", watched_read)
    monkeypatch.setattr(
        requests, "get",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("unexpected auth request")),
    )
    monkeypatch.setattr(
        requests, "post",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("unexpected auth request")),
    )
    assert "chatgpt" not in get_web_model_options()
    with pytest.raises(ValueError, match="Unsupported LLM provider"):
        create_llm_client("chatgpt", "gpt-6.1-sol")
