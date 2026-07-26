from types import SimpleNamespace
from unittest.mock import patch

from cli.main import (
    build_consolidated_report_html,
    estimate_target_profile,
    format_price_target,
)


def test_missing_target_and_confidence_remain_none():
    final_state = {"final_trade_decision": "**Rating**: Hold\n\n**Executive Summary**: Wait."}

    with patch("cli.main.fetch_reference_price", return_value=145.5):
        profile = estimate_target_profile(None, "NVDA", "2026-07-20", final_state, "Hold")

    assert profile["price_target"] is None
    assert profile["confidence_score"] is None


def test_explicit_structured_target_profile_is_extracted_without_fallback():
    class FailingLlm:
        def invoke(self, _messages):
            raise AssertionError("explicit decision metrics must not trigger a fallback call")

    final_state = {
        "final_trade_decision": (
            "**Rating**: Buy\n\n"
            "**Price Target**: 145.5\n\n"
            "**Time Horizon**: 3 months\n\n"
            "**Decision Confidence**: 78/100\n\n"
            "**Target Rationale**: Earnings growth supports the target."
        )
    }

    with patch("cli.main.fetch_reference_price", return_value=125.0):
        profile = estimate_target_profile(
            FailingLlm(), "NVDA", "2026-07-20", final_state, "Buy"
        )

    assert profile["price_target"] == 145.5
    assert profile["target_horizon"] == "3 months"
    assert profile["confidence_score"] == 78
    assert profile["target_summary"] == "Earnings growth supports the target."


def test_missing_structured_metrics_are_estimated_from_existing_analysis():
    class EvidenceLlm:
        def invoke(self, _messages):
            return SimpleNamespace(
                content=(
                    '{"price_target": 1.56, "confidence_score": 74, '
                    '"horizon": "0-3 months", '
                    '"summary": "Downside target is the cited 200-day support; '
                    'zero revenue and dilution support the Sell rating.", '
                    '"supporting_quote": "200 SMA: 1.56"}'
                )
            )

    final_state = {
        "final_trade_decision": (
            "**Rating**: Sell\n\n"
            "**Executive Summary**: Exit in tranches. Treat a break below the "
            "200 SMA (~1.56) as a hard exit level. Reconsider only after a "
            "sustained reclaim of ~2.05-2.17.\n\n"
            "**Time Horizon**: Immediate / next 0-3 months"
        ),
        "market_report": (
            "Verified close: 1.95. 200 SMA: 1.56. 10 EMA: 2.05. VWMA: 2.17."
        ),
        "fundamentals_report": "Zero revenue, negative cash flow, and dilution risk.",
    }

    with patch("cli.main.fetch_reference_price", return_value=1.95):
        profile = estimate_target_profile(
            EvidenceLlm(), "SPAIF", "2026-07-23", final_state, "Sell"
        )

    assert profile == {
        "price_target": 1.56,
        "confidence_score": 74,
        "target_horizon": "Immediate / next 0-3 months",
        "target_summary": (
            "Downside target is the cited 200-day support; zero revenue and "
            "dilution support the Sell rating."
        ),
        "reference_price": 1.95,
    }

    html = build_consolidated_report_html(
        [
            {
                "ticker": "SPAIF",
                "analysis_date": "2026-07-23",
                "decision": "Sell",
                "results_dir": "/tmp/spaif",
                "final_state": final_state,
                **profile,
            }
        ],
        "2026-07-23",
    )
    assert "<span class='metric-label'>Price Target</span><strong>1.56</strong>" in html
    assert "<span class='metric-label'>Target Gap</span><strong>-20.00%</strong>" in html
    assert "<strong>74/100</strong>" in html
    assert "Downside target is the cited 200-day support" in html


def test_uncited_fallback_target_is_rejected_as_unsupported():
    class FabricatingLlm:
        def invoke(self, _messages):
            return SimpleNamespace(
                content=(
                    '{"price_target": 999, "confidence_score": 88, '
                    '"horizon": "12 months", "summary": "Large upside.", '
                    '"supporting_quote": "Target 999"}'
                )
            )

    final_state = {
        "final_trade_decision": "**Rating**: Buy",
        "market_report": "Verified close: 100. Resistance: 120.",
    }

    with patch("cli.main.fetch_reference_price", return_value=100.0):
        profile = estimate_target_profile(
            FabricatingLlm(), "TEST", "2026-07-20", final_state, "Buy"
        )

    assert profile == {
        "price_target": None,
        "confidence_score": None,
        "target_horizon": None,
        "target_summary": None,
        "reference_price": 100.0,
    }


def test_non_price_number_near_generic_price_word_is_rejected():
    class MisreadingLlm:
        def invoke(self, _messages):
            return SimpleNamespace(
                content=(
                    '{"price_target": 999, "confidence_score": 88, '
                    '"horizon": "12 months", "summary": "Revenue momentum.", '
                    '"supporting_quote": '
                    '"High dilution risk; revenue increased 999%."}'
                )
            )

    final_state = {
        "final_trade_decision": "**Rating**: Buy",
        "fundamentals_report": "High dilution risk; revenue increased 999%.",
    }

    with patch("cli.main.fetch_reference_price", return_value=100.0):
        profile = estimate_target_profile(
            MisreadingLlm(), "TEST", "2026-07-20", final_state, "Buy"
        )

    assert profile == {
        "price_target": None,
        "confidence_score": None,
        "target_horizon": None,
        "target_summary": None,
        "reference_price": 100.0,
    }


def test_confidence_horizon_and_outlook_are_cleared_without_a_target():
    class TargetlessLlm:
        def invoke(self, _messages):
            return SimpleNamespace(
                content=(
                    '{"price_target": null, "confidence_score": 88, '
                    '"horizon": "12 months", "summary": "Evidence is mixed.", '
                    '"supporting_quote": null}'
                )
            )

    final_state = {"final_trade_decision": "**Rating**: Hold"}

    with patch("cli.main.fetch_reference_price", return_value=100.0):
        profile = estimate_target_profile(
            TargetlessLlm(), "TEST", "2026-07-20", final_state, "Hold"
        )

    assert profile == {
        "price_target": None,
        "confidence_score": None,
        "target_horizon": None,
        "target_summary": None,
        "reference_price": 100.0,
    }


def test_direction_inconsistent_target_clears_the_entire_profile():
    class ContradictingLlm:
        def invoke(self, _messages):
            return SimpleNamespace(
                content=(
                    '{"price_target": 150, "confidence_score": 80, '
                    '"horizon": "3 months", "summary": "Cited resistance.", '
                    '"supporting_quote": "Resistance: 150"}'
                )
            )

    final_state = {
        "final_trade_decision": "**Rating**: Sell",
        "market_report": "Verified close: 100. Resistance: 150.",
    }

    with patch("cli.main.fetch_reference_price", return_value=100.0):
        profile = estimate_target_profile(
            ContradictingLlm(), "TEST", "2026-07-20", final_state, "Sell"
        )

    assert profile == {
        "price_target": None,
        "confidence_score": None,
        "target_horizon": None,
        "target_summary": None,
        "reference_price": 100.0,
    }


def test_invalid_fallback_metrics_remain_unavailable_instead_of_using_defaults():
    class InvalidLlm:
        def invoke(self, _messages):
            return SimpleNamespace(
                content=(
                    '{"price_target": -10, "confidence_score": 140, '
                    '"horizon": "", "summary": ""}'
                )
            )

    final_state = {"final_trade_decision": "**Rating**: Hold"}

    with patch("cli.main.fetch_reference_price", return_value=145.5):
        profile = estimate_target_profile(
            InvalidLlm(), "NVDA", "2026-07-20", final_state, "Hold"
        )

    assert profile["price_target"] is None
    assert profile["confidence_score"] is None
    assert profile["target_horizon"] is None
    assert profile["target_summary"] is None


def test_unknown_currency_does_not_render_usd_symbol():
    assert format_price_target(145.5) == "145.50"
    assert format_price_target(145.5, currency="USD") == "$145.50"
