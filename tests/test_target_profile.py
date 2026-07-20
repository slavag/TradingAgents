from unittest.mock import MagicMock

from cli.main import estimate_target_profile, format_price_target


def test_missing_target_and_confidence_remain_none():
    final_state = {"final_trade_decision": "**Rating**: Hold\n\n**Executive Summary**: Wait."}

    profile = estimate_target_profile(None, "NVDA", "2026-07-20", final_state, "Hold")

    assert profile["price_target"] is None
    assert profile["confidence_score"] is None


def test_explicit_structured_target_is_extracted_without_llm_call():
    llm = MagicMock()
    final_state = {
        "final_trade_decision": (
            "**Rating**: Buy\n\n**Price Target**: 145.5\n\n**Time Horizon**: 3 months"
        )
    }

    profile = estimate_target_profile(llm, "NVDA", "2026-07-20", final_state, "Buy")

    assert profile["price_target"] == 145.5
    assert profile["target_horizon"] == "3 months"
    assert profile["confidence_score"] is None
    llm.invoke.assert_not_called()


def test_unknown_currency_does_not_render_usd_symbol():
    assert format_price_target(145.5) == "145.50"
    assert format_price_target(145.5, currency="USD") == "$145.50"
