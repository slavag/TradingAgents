"""Agents on the schema-only structured-output path must not invite tool calls (#1130).

`with_structured_output` binds exactly one tool (the schema). A prompt that
primes tool use makes models emit an unknown `web_search` call, which discards
the structured attempt and forces a free-text retry — an extra LLM round trip
and the loss of typed output.

These assert the constraint reaches the *rendered* prompt each agent actually
sends, not merely that the constant is referenced in the module.
"""
from __future__ import annotations

import inspect
from unittest.mock import MagicMock

import pytest

import tradingagents.agents.analysts.sentiment_analyst as sentiment
from tradingagents.agents.managers.portfolio_manager import create_portfolio_manager
from tradingagents.agents.managers.research_manager import create_research_manager
from tradingagents.agents.trader.trader import create_trader
from tradingagents.agents.utils.structured import NO_EXTERNAL_TOOLS


def _capturing_llm(captured: dict, result):
    """LLM whose structured binding records the prompt it was handed."""
    structured = MagicMock()
    structured.invoke.side_effect = lambda prompt: (
        captured.__setitem__("prompt", prompt) or result
    )
    llm = MagicMock()
    llm.with_structured_output.return_value = structured
    return llm


def _prompt_text(prompt) -> str:
    """Flatten a captured prompt (str, message list, or objects) to text."""
    if isinstance(prompt, str):
        return prompt
    parts = []
    for m in prompt:
        parts.append(m.get("content", "") if isinstance(m, dict) else getattr(m, "content", ""))
    return "\n".join(str(p) for p in parts)


@pytest.mark.unit
def test_trader_prompt_states_constraint():
    from tradingagents.agents.schemas import TraderAction, TraderProposal

    captured = {}
    llm = _capturing_llm(captured, TraderProposal(action=TraderAction.BUY, reasoning="x"))
    create_trader(llm)({
        "company_of_interest": "NVDA",
        "investment_plan": "**Recommendation**: Buy",
    })
    assert NO_EXTERNAL_TOOLS in _prompt_text(captured["prompt"])
    assert "Do not invent a numeric portfolio weight" in _prompt_text(captured["prompt"])


@pytest.mark.unit
def test_research_manager_prompt_states_constraint():
    from tradingagents.agents.schemas import PortfolioRating, ResearchPlan

    captured = {}
    llm = _capturing_llm(
        captured,
        ResearchPlan(
            recommendation=PortfolioRating.BUY, rationale="x", strategic_actions="y"
        ),
    )
    create_research_manager(llm)({
        "company_of_interest": "NVDA",
        "investment_debate_state": {
            "history": "h", "bull_history": "b", "bear_history": "r",
            "current_response": "", "judge_decision": "", "count": 1,
        },
    })
    assert NO_EXTERNAL_TOOLS in _prompt_text(captured["prompt"])


@pytest.mark.unit
def test_portfolio_manager_prompt_states_constraint():
    from tradingagents.agents.schemas import (
        ConditionalActionPlan,
        ExistingPositionAction,
        NewPositionAction,
        PortfolioDecisionDraft,
        PortfolioRating,
        ThesisRating,
    )

    captured = {}
    llm = _capturing_llm(
        captured,
        PortfolioDecisionDraft(
            rating=PortfolioRating.HOLD,
            executive_summary="x",
            investment_thesis="y",
            thesis=ThesisRating.NEUTRAL,
            existing_position_action=ExistingPositionAction.HOLD,
            existing_position_summary="hold",
            new_position_action=NewPositionAction.WAIT,
            new_position_summary="wait",
            recommendation_confidence_score=50,
            conditional_plan=ConditionalActionPlan(
                confirmation="Reassess when evidence improves.",
                invalidation="Avoid entry if evidence deteriorates.",
            ),
        ),
    )
    risk = {
        "history": "h", "aggressive_history": "a", "conservative_history": "c",
        "neutral_history": "n", "current_aggressive_response": "",
        "current_conservative_response": "", "current_neutral_response": "",
        "latest_speaker": "Neutral", "count": 1,
    }
    create_portfolio_manager(llm)({
        "company_of_interest": "NVDA",
        "risk_debate_state": risk,
        "investment_plan": "plan",
        "trader_investment_plan": "trader plan",
    })
    assert NO_EXTERNAL_TOOLS in _prompt_text(captured["prompt"])
    assert "Do not invent a numeric portfolio weight" in _prompt_text(captured["prompt"])


@pytest.mark.unit
def test_portfolio_manager_receives_only_verified_price_level_candidates():
    from tradingagents.agents.schemas import (
        ConditionalActionPlan,
        ExistingPositionAction,
        NewPositionAction,
        PortfolioDecisionDraft,
        PortfolioRating,
        ThesisRating,
    )

    captured = {}
    llm = _capturing_llm(captured, PortfolioDecisionDraft(
        rating=PortfolioRating.HOLD,
        executive_summary="x",
        investment_thesis="y",
        thesis=ThesisRating.NEUTRAL,
        existing_position_action=ExistingPositionAction.HOLD,
        existing_position_summary="hold",
        new_position_action=NewPositionAction.WAIT,
        new_position_summary="wait",
        recommendation_confidence_score=50,
        price_target=120.0,
        time_horizon="3 months",
        confidence_score=60,
        target_summary="The verified high is an observed scenario level.",
        target_method="Technical scenario",
        target_risks="The breakout may reverse.",
        supporting_quote="| High | 120.00 |",
        conditional_plan=ConditionalActionPlan(
            confirmation="Reassess when evidence improves.",
            invalidation="Avoid entry if evidence deteriorates.",
        ),
    ))
    risk = {
        "history": "h", "aggressive_history": "a", "conservative_history": "c",
        "neutral_history": "n", "current_aggressive_response": "",
        "current_conservative_response": "", "current_neutral_response": "",
        "latest_speaker": "Neutral", "count": 1,
    }
    snapshot = {
        "source": "get_verified_market_snapshot",
        "category": "market",
        "title": "Verified market snapshot",
        "content": (
            "### Latest verified OHLCV row\n| Field | Value |\n|---|---:|\n"
            "| Close | 100.00 |\n| High | 120.00 |\n| Volume | 999999 |\n"
            "### Verified technical indicators (latest row)\n"
            "| Indicator | Value |\n|---|---:|\n| rsi | 78.2 |\n"
        ),
        "published_at": "2025-05-01",
        "as_of_date": "2025-05-02",
        "status": "available",
    }

    result = create_portfolio_manager(llm)({
        "company_of_interest": "NVDA",
        "trade_date": "2025-05-02",
        "shared_evidence": [
            snapshot,
            {**snapshot, "published_at": "2025-05-03", "content": "| Resistance | 999.00 |"},
        ],
        "risk_debate_state": risk,
        "investment_plan": "plan",
        "trader_investment_plan": "trader plan",
    })

    prompt = _prompt_text(captured["prompt"])
    candidates = prompt.split("Verified target candidates:", 1)[1].split("\n---", 1)[0]
    assert "| High | 120.00 |" in candidates
    assert "| Close | 100.00 |" in candidates
    assert "| Volume | 999999 |" not in candidates
    assert "| rsi | 78.2 |" not in candidates
    assert "999.00" not in candidates
    assert result["portfolio_decision"]["target_validation_status"] == "Accepted"


def _peng_pm_state():
    snapshot = {
        "source": "get_verified_market_snapshot", "category": "market",
        "title": "Verified market snapshot",
        "content": (
            "Verified close: 75.74.\n| Low | 72.83 |\n"
            "| Close | 75.74 |\n| High | 77.10 |"
        ),
        "published_at": "2026-10-09", "as_of_date": "2026-10-09",
        "status": "available",
    }
    return {
        "company_of_interest": "PENG", "trade_date": "2026-10-09",
        "market_report": "Verified close: 75.74.",
        "shared_evidence": [snapshot],
        "risk_debate_state": {
            "history": "h", "aggressive_history": "a", "conservative_history": "c",
            "neutral_history": "n", "current_aggressive_response": "",
            "current_conservative_response": "", "current_neutral_response": "",
            "latest_speaker": "Neutral", "count": 1,
        },
        "investment_plan": "Hold with conditional entry.",
        "trader_investment_plan": "Wait for confirmation.",
    }


def _peng_pm_draft(**overrides):
    from tradingagents.agents.schemas import (
        ConditionalActionPlan,
        ExistingPositionAction,
        NewPositionAction,
        PortfolioDecisionDraft,
        PortfolioRating,
        ThesisRating,
    )

    values = {
        "rating": PortfolioRating.HOLD,
        "executive_summary": "Maintain exposure pending confirmation.",
        "investment_thesis": "Verified evidence supports a conditional scenario.",
        "thesis": ThesisRating.NEUTRAL,
        "existing_position_action": ExistingPositionAction.HOLD,
        "existing_position_summary": "Keep a right-sized position.",
        "new_position_action": NewPositionAction.WAIT,
        "new_position_summary": "Enter only after confirmation.",
        "recommendation_confidence_score": 78,
        "conditional_plan": ConditionalActionPlan(
            confirmation="Require sustained demand.",
            invalidation="A failed breakout weakens the case.",
        ),
    }
    values.update(overrides)
    return PortfolioDecisionDraft(**values)


def _pm_llm(initial, repair):
    from tradingagents.agents.schemas import PortfolioDecisionDraft, PortfolioTargetRepair

    first_chain = MagicMock()
    first_chain.invoke.return_value = initial
    repair_chain = MagicMock()
    repair_chain.invoke.return_value = repair
    llm = MagicMock()

    def bind(schema):
        return first_chain if schema is PortfolioDecisionDraft else repair_chain if schema is PortfolioTargetRepair else None

    llm.with_structured_output.side_effect = bind
    return llm, first_chain, repair_chain


@pytest.mark.unit
def test_pm_repairs_missing_target_with_low_confidence_peng_range_once():
    from tradingagents.agents.schemas import PortfolioTargetRepair

    repair = PortfolioTargetRepair(
        target_low=72.83, target_high=77.10,
        supporting_quote_low="| Low | 72.83 |",
        supporting_quote_high="| High | 77.10 |",
        time_horizon="3 months", confidence_score=18,
        target_summary="Demand persistence could revisit verified resistance over three months.",
        target_method="Technical scenario",
        target_risks="A failed breakout would invalidate the range.",
    )
    llm, first_chain, repair_chain = _pm_llm(_peng_pm_draft(), repair)

    decision = create_portfolio_manager(llm)(_peng_pm_state())["portfolio_decision"]

    assert decision["status"] == "Actionable"
    assert decision["rating"] == "Hold"
    assert decision["existing_position_action"] == "Hold"
    assert decision["new_position_action"] == "Wait"
    assert decision["target_validation_status"] == "Accepted"
    assert (decision["target_low"], decision["target_high"]) == (72.83, 77.10)
    assert decision["confidence_score"] == 18
    assert first_chain.invoke.call_count == 1
    assert repair_chain.invoke.call_count == 1


@pytest.mark.unit
def test_pm_abstains_after_one_invalid_target_repair():
    from tradingagents.agents.schemas import PortfolioTargetRepair

    repair = PortfolioTargetRepair(
        target_low=72.83, target_high=77.10,
        supporting_quote_low="| Low | 72.83 |",
        supporting_quote_high="| Unverified high | 77.10 |",
        time_horizon="3 months", confidence_score=18,
        target_summary="Possible recovery.", target_method="Technical scenario",
        target_risks="Breakout may fail.",
    )
    llm, first_chain, repair_chain = _pm_llm(_peng_pm_draft(), repair)
    state = _peng_pm_state()
    state["market_report"] += "\n| Unverified high | 77.10 |"

    decision = create_portfolio_manager(llm)(state)["portfolio_decision"]

    assert decision["status"] == "Abstain"
    assert decision["rating"] is None
    assert decision["existing_position_action"] is None
    assert decision["new_position_action"] is None
    assert decision["status_reason"] == "target_unavailable"
    assert first_chain.invoke.call_count == 1
    assert repair_chain.invoke.call_count == 1


@pytest.mark.unit
def test_pm_accepted_point_needs_no_repair_call():
    draft = _peng_pm_draft(
        price_target=77.10, supporting_quote="| High | 77.10 |",
        time_horizon="3 months", confidence_score=24,
        target_summary="Sustained demand may revisit verified resistance.",
        target_method="Technical scenario",
        target_risks="The breakout may fail.",
    )
    llm, first_chain, repair_chain = _pm_llm(draft, None)

    decision = create_portfolio_manager(llm)(_peng_pm_state())["portfolio_decision"]

    assert decision["status"] == "Actionable"
    assert decision["target_validation_status"] == "Accepted"
    assert decision["price_target"] == 77.10
    assert first_chain.invoke.call_count == 1
    assert repair_chain.invoke.call_count == 0


@pytest.mark.unit
def test_pm_repair_cannot_change_original_rating():
    malicious_repair = _peng_pm_draft(rating="Buy")
    llm, first_chain, repair_chain = _pm_llm(_peng_pm_draft(), malicious_repair)

    decision = create_portfolio_manager(llm)(_peng_pm_state())["portfolio_decision"]

    assert decision["status"] == "Abstain"
    assert decision["rating"] is None
    assert decision["status_reason"] == "target_unavailable"
    assert first_chain.invoke.call_count == 1
    assert repair_chain.invoke.call_count == 1


@pytest.mark.unit
def test_pm_with_no_verified_price_rows_abstains_without_paid_repair():
    llm, first_chain, repair_chain = _pm_llm(_peng_pm_draft(), None)
    state = _peng_pm_state()
    state["shared_evidence"] = []

    decision = create_portfolio_manager(llm)(state)["portfolio_decision"]

    assert decision["status"] == "Abstain"
    assert decision["status_reason"] == "target_unavailable"
    assert first_chain.invoke.call_count == 1
    assert repair_chain.invoke.call_count == 0


@pytest.mark.unit
def test_portfolio_manager_rejects_numeric_nav_size_without_portfolio_context():
    from tradingagents.agents.schemas import (
        ExistingPositionAction,
        NewPositionAction,
        PortfolioDecisionDraft,
        PortfolioRating,
        ThesisRating,
    )

    llm = _capturing_llm({}, PortfolioDecisionDraft(
        rating=PortfolioRating.UNDERWEIGHT,
        executive_summary="Reduce exposure.",
        investment_thesis="Evidence is weak.",
        thesis=ThesisRating.BEARISH,
        existing_position_action=ExistingPositionAction.TRIM,
        existing_position_summary="Trim to 0.25% of NAV.",
        new_position_action=NewPositionAction.AVOID,
        new_position_summary="Avoid a new long.",
        recommendation_confidence_score=70,
    ))
    risk = {
        "history": "h", "aggressive_history": "a", "conservative_history": "c",
        "neutral_history": "n", "current_aggressive_response": "",
        "current_conservative_response": "", "current_neutral_response": "",
        "latest_speaker": "Neutral", "count": 1,
    }

    result = create_portfolio_manager(llm)({
        "company_of_interest": "NVDA",
        "risk_debate_state": risk,
        "investment_plan": "plan",
        "trader_investment_plan": "trader plan",
    })

    assert result["portfolio_decision"]["status"] == "Unavailable"
    assert "unsupported_portfolio_sizing" in result["final_trade_decision"]


@pytest.mark.unit
def test_sentiment_prompt_states_constraint(monkeypatch):
    from tradingagents.agents.schemas import SentimentBand, SentimentReport

    # Pre-fetched sources are stubbed so the prompt builds without network I/O.
    monkeypatch.setattr(sentiment, "fetch_stocktwits_messages", lambda *a, **k: "st")
    monkeypatch.setattr(sentiment, "fetch_reddit_posts", lambda *a, **k: "rd")
    monkeypatch.setattr(sentiment.get_news, "func", lambda *a, **k: "news", raising=False)

    captured = {}
    llm = _capturing_llm(captured, SentimentReport(
        overall_band=SentimentBand.BULLISH, overall_score=7.5,
        confidence="high", narrative="n",
    ))
    sentiment.create_sentiment_analyst(llm)({
        "company_of_interest": "NVDA", "trade_date": "2026-01-15",
        "asset_type": "stock", "messages": [],
    })
    text = _prompt_text(captured["prompt"])
    assert NO_EXTERNAL_TOOLS in text
    # This agent binds no tools, so tool-range wording must not reappear.
    assert "tool-call date ranges" not in text


@pytest.mark.unit
def test_tool_using_analysts_keep_their_date_guidance():
    # The analysts that really do call tools keep the wording that anchors their
    # tool date ranges (#836) — this fix is scoped to no-tool agents.
    import tradingagents.agents.analysts.market_analyst as market
    import tradingagents.agents.analysts.news_analyst as news
    for module in (market, news):
        assert "tool-call date ranges" in inspect.getsource(module)


@pytest.mark.unit
def test_constraint_text_is_unambiguous():
    assert "do not call external tools" in NO_EXTERNAL_TOOLS.lower()
    # No template braces: it is embedded in ChatPromptTemplate strings, where
    # braces would be parsed as input variables.
    assert "{" not in NO_EXTERNAL_TOOLS and "}" not in NO_EXTERNAL_TOOLS
