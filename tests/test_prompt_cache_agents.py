"""Repeated debate calls preserve text while marking eligible provider prefixes."""

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from tradingagents.agents.researchers.bull_researcher import create_bull_researcher
from tradingagents.agents.risk_mgmt.aggressive_debator import create_aggressive_debator
from tradingagents.graph import trading_graph
from tradingagents.graph.conditional_logic import ConditionalLogic
from tradingagents.graph.setup import GraphSetup
from tradingagents.llm_clients.prompt_cache import PromptCachePolicy

pytestmark = pytest.mark.unit


class RecordingModel:
    def __init__(self):
        self.prompts = []

    def invoke(self, prompt, **kwargs):
        self.prompts.append((prompt, kwargs))
        return AIMessage(content="A sourced argument.")


def _research_state(history):
    return {
        "company_of_interest": "PENG", "asset_type": "stock",
        "instrument_context": "PENG · 2026-10-11.\n", "trade_date": "2026-10-11",
        "shared_evidence": [],
        "market_report": "Market evidence " * 500,
        "sentiment_report": "Sentiment evidence.",
        "news_report": "News evidence.",
        "fundamentals_report": "Financial evidence.",
        "investment_debate_state": {
            "history": history, "bull_history": "", "bear_history": "",
            "current_response": "Latest opposing argument.", "count": 0,
        },
    }


def _risk_state(history):
    state = _research_state("")
    state["trader_investment_plan"] = "Trader proposal."
    state["risk_debate_state"] = {
        "history": history, "aggressive_history": "",
        "conservative_history": "", "neutral_history": "",
        "current_conservative_response": "Conservative objection.",
        "current_neutral_response": "Neutral objection.",
        "count": 0,
    }
    return state


def _joined_prompt(prompt):
    assert isinstance(prompt, list) and isinstance(prompt[0], HumanMessage)
    return "".join(block["text"] for block in prompt[0].content)


def test_bull_researcher_cache_blocks_preserve_original_prompt_text():
    state = _research_state("First round.")
    plain_model = RecordingModel()
    cached_model = RecordingModel()
    create_bull_researcher(plain_model)(state)
    create_bull_researcher(
        cached_model,
        cache_policy=PromptCachePolicy("anthropic", "claude-sonnet-5", expected_reuse=True),
    )(state)

    plain = plain_model.prompts[0][0]
    cached = cached_model.prompts[0][0]
    assert isinstance(plain, str)
    assert _joined_prompt(cached) == plain
    assert cached[0].content[0]["cache_control"] == {"type": "ephemeral"}


def test_researcher_stable_prefix_survives_changing_history():
    model = RecordingModel()
    node = create_bull_researcher(
        model,
        cache_policy=PromptCachePolicy("anthropic", "claude-sonnet-5", expected_reuse=True),
    )

    node(_research_state("First round."))
    node(_research_state("First round. Second round."))

    first = model.prompts[0][0][0].content
    second = model.prompts[1][0][0].content
    assert first[0] == second[0]
    assert first[1]["text"] != second[1]["text"]


def test_researcher_history_cannot_move_cache_boundary():
    model = RecordingModel()
    node = create_bull_researcher(
        model,
        cache_policy=PromptCachePolicy("anthropic", "claude-sonnet-5", expected_reuse=True),
    )

    node(_research_state("First round."))
    node(_research_state("First round. Conversation history of the debate: echoed text."))

    first = model.prompts[0][0][0].content
    second = model.prompts[1][0][0].content
    assert first[0] == second[0]
    assert "echoed text" in second[1]["text"]


def test_risk_debator_cache_blocks_preserve_original_prompt_text():
    state = _risk_state("First risk round.")
    plain_model = RecordingModel()
    cached_model = RecordingModel()
    create_aggressive_debator(plain_model)(state)
    create_aggressive_debator(
        cached_model,
        cache_policy=PromptCachePolicy("anthropic", "claude-sonnet-5", expected_reuse=True),
    )(state)

    assert _joined_prompt(cached_model.prompts[0][0]) == plain_model.prompts[0][0]


def test_risk_history_cannot_move_cache_boundary():
    model = RecordingModel()
    node = create_aggressive_debator(
        model,
        cache_policy=PromptCachePolicy("anthropic", "claude-sonnet-5", expected_reuse=True),
    )

    node(_risk_state("First risk round."))
    node(_risk_state("First risk round. Here is the current conversation history: echoed."))

    first = model.prompts[0][0][0].content
    second = model.prompts[1][0][0].content
    assert first[0] == second[0]
    assert "echoed" in second[1]["text"]


def test_graph_cache_policy_requires_repeated_rounds_and_enabled_switch():
    base = PromptCachePolicy("anthropic", "claude-sonnet-5", enabled=True)
    setup = GraphSetup(None, None, {}, ConditionalLogic(1, 2), quick_cache_policy=base)

    assert setup._policy_for_rounds(1).expected_reuse is False
    assert setup._policy_for_rounds(2).expected_reuse is True
    assert setup._policy_for_rounds(2).provider == "anthropic"

    disabled = GraphSetup(
        None, None, {}, ConditionalLogic(3, 3),
        quick_cache_policy=PromptCachePolicy("anthropic", "claude-sonnet-5", enabled=False),
    )
    assert disabled._policy_for_rounds(3).expected_reuse is False


def test_anthropic_disable_switch_does_not_disable_native_openai_cache_key():
    config = {
        "llm_provider": "openai", "quick_think_llm": "gpt-5.6-sol",
        "anthropic_prompt_cache": False,
    }

    policy = trading_graph._quick_prompt_cache_policy(config)

    assert policy.provider == "openai"
    assert policy.enabled is True
    assert policy.model == "gpt-5.6-sol"


def test_anthropic_disable_switch_respects_quick_role_provider():
    config = {
        "llm_provider": "openai", "quick_think_provider": "anthropic",
        "quick_think_llm": "claude-sonnet-5", "anthropic_prompt_cache": False,
    }

    policy = trading_graph._quick_prompt_cache_policy(config)

    assert policy.provider == "anthropic"
    assert policy.enabled is False
