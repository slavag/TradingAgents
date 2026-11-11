"""A role using another provider must not inherit the primary provider endpoint."""

from unittest.mock import MagicMock, patch

from tradingagents.graph.trading_graph import (
    TradingAgentsGraph,
    _quick_prompt_cache_policy,
)


def test_distinct_role_provider_uses_its_own_default_endpoint():
    graph = MagicMock(spec=TradingAgentsGraph)
    graph.config = {
        "llm_provider": "openai",
        "backend_url": "https://primary.example/v1",
        "quick_think_provider": "anthropic",
        "quick_think_llm": "claude-sonnet-5-5",
        "quick_backend_url": None,
    }
    graph.callbacks = []
    graph._get_provider_kwargs.return_value = {}
    with patch("tradingagents.graph.trading_graph.create_llm_client") as create:
        TradingAgentsGraph._create_role_llm(
            graph, "quick_think_provider", "quick_think_llm", "quick_backend_url"
        )
    assert create.call_args.kwargs["provider"] == "anthropic"
    assert create.call_args.kwargs["base_url"] is None


def test_same_provider_role_can_inherit_primary_endpoint():
    graph = MagicMock(spec=TradingAgentsGraph)
    graph.config = {
        "llm_provider": "openai",
        "backend_url": "https://primary.example/v1",
        "quick_think_provider": "openai",
        "quick_think_llm": "gpt-5.4-mini",
        "quick_backend_url": None,
    }
    graph.callbacks = []
    graph._get_provider_kwargs.return_value = {}
    with patch("tradingagents.graph.trading_graph.create_llm_client") as create:
        TradingAgentsGraph._create_role_llm(
            graph, "quick_think_provider", "quick_think_llm", "quick_backend_url"
        )
    assert create.call_args.kwargs["base_url"] == "https://primary.example/v1"


def test_prompt_cache_policy_uses_role_endpoint_boundary():
    config = {
        "llm_provider": "openai",
        "backend_url": "https://primary.example/v1",
        "quick_think_provider": "anthropic",
        "quick_think_llm": "claude-sonnet-5-5",
        "quick_backend_url": None,
    }
    assert _quick_prompt_cache_policy(config).base_url is None
