from parsel import Selector

from cli.main import (
    build_consolidated_report,
    build_consolidated_report_html,
    bullet_markdown_to_html,
    fallback_bullet_summary,
)


def test_consolidated_reports_mark_uncalibrated_confidence_and_missing_target_unavailable():
    result = {
        "ticker": "PENG",
        "analysis_date": "2026-07-19",
        "decision": "Hold",
        "price_target": None,
        "reference_price": None,
        "confidence_score": None,
        "target_horizon": None,
        "target_summary": None,
        "results_dir": "/tmp/peng",
        "final_state": {
            "final_trade_decision": "Hold.",
            "market_report": "Market report.",
            "sentiment_report": "Social report.",
            "news_report": "News report.",
            "fundamentals_report": "Fundamentals report.",
            "trader_investment_plan": "Trader plan.",
        },
    }

    markdown = build_consolidated_report([result], "2026-07-19")
    html = build_consolidated_report_html([result], "2026-07-19")

    assert "Recommendation confidence (uncalibrated)" in markdown
    assert "| PENG | Hold | - | - | - |" in markdown
    assert "Recommendation confidence (uncalibrated)" in html
    assert "Validated targets: 0 of 1 completed runs" in html
    assert "Average Price Target" not in html
    assert "<span class='metric-label'>Price Target</span>" in html


def test_consolidated_html_summary_renders_markdown_control_tokens():
    summary = fallback_bullet_summary(
        """
        FINAL TRANSACTION PROPOSAL: **HOLD**

        ### PENG technical read
        PENG is still in a **larger uptrend**, but momentum cooled sharply.
        """
    )

    html = bullet_markdown_to_html(summary)

    assert "**" not in html
    assert "###" not in html
    assert "<strong>HOLD</strong>" in html
    assert "<strong>larger uptrend</strong>" in html
    assert "PENG technical read" in html


def test_consolidated_html_summary_keeps_escaped_html_when_rendering_bold():
    html = bullet_markdown_to_html("- **Risk:** <script>alert('x')</script>")

    assert "<strong>Risk:</strong>" in html
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_trader_plan_html_renders_inline_markdown_without_raw_tokens():
    html = build_consolidated_report_html(
        [
            {
                "ticker": "PENG",
                "analysis_date": "2026-07-19",
                "decision": "Hold",
                "price_target": None,
                "reference_price": None,
                "confidence_score": 55,
                "target_horizon": "1 month",
                "target_summary": "Balanced setup.",
                "results_dir": "/tmp/peng",
                "final_state": {
                    "final_trade_decision": "Hold.",
                    "market_report": "Market report.",
                    "sentiment_report": "Social report.",
                    "news_report": "News report.",
                    "fundamentals_report": "Fundamentals report.",
                    "trader_investment_plan": (
                        "**Action**: Hold\n\n"
                        "**Reasoning**: Balanced risk/reward.\n\n"
                        "FINAL TRANSACTION PROPOSAL: **HOLD**"
                    ),
                },
            }
        ],
        "2026-07-19",
    )

    assert "**Action**" not in html
    assert "**HOLD**" not in html
    assert "<strong>Action</strong>: Hold" in html
    assert "FINAL TRANSACTION PROPOSAL: <strong>HOLD</strong>" in html


def test_fallback_summary_omits_tables_chatter_and_mid_sentence_ellipsis():
    summary = fallback_bullet_summary(
        """
        If you don\u2019t own it yet, I\u2019d prefer to wait for a better entry.

        Key points table
        | Category | What we learned | Why it matters |
        |---|---|---|
        | Operating | Revenue improved but working capital remains tight. |

        Operating performance improved as revenue and EPS guidance moved higher.
        Convertible refinancing risk remains the main overhang.
        """
    )

    assert "If you" not in summary
    assert "I\u2019d prefer" not in summary
    assert "Key points table" not in summary
    assert "|" not in summary
    assert "..." not in summary
    assert "Operating performance improved as revenue and EPS guidance moved higher." in summary
    assert "Convertible refinancing risk remains the main overhang." in summary


def test_market_fallback_prefers_the_investment_read_to_indicator_definitions():
    summary = fallback_bullet_summary(
        """AMD — Technical trading report
Indicator set selected
I selected eight indicators to cover trend and momentum.
- **close_200_sma:** strategic long-term trend benchmark.
- **close_50_sma:** medium-term trend context. Market read **AMD’s longer-term trend remains positive by moving-average positioning, but short-term price action has weakened notably.** The verified October 15 close was **$550.45**.
That is above the **50-day SMA of $536.11** and below the **10-day EMA of $595.59**.
""",
        section_name="Market",
    )

    assert "Indicator set selected" not in summary
    assert "I selected" not in summary
    assert "trend remains positive" in summary
    assert "$536.11" in summary


def test_fundamentals_fallback_skips_cover_page_before_executive_summary():
    summary = fallback_bullet_summary(
        "AMD — Fundamental research report **Analysis date: 2026-10-15** "
        "**Company: Advanced Micro Devices, Inc. (AMD)** Executive summary "
        "AMD’s reported fundamentals show revenue and profit growth. "
        "Revenue reached $11.54 billion, up from $10.25 billion.",
        section_name="Fundamentals",
    )

    assert "Fundamental research report" not in summary
    assert "Revenue reached $11.54 billion" in summary


def test_social_fallback_removes_report_header_but_keeps_caveated_sample():
    summary = fallback_bullet_summary(
        "**Overall Sentiment:** **Mixed** (Score: 5.0/10) **Confidence:** Low "
        "AMD sentiment report — 2026-10-08 to 2026-10-15 1. "
        "Source-by-source evidence **Yahoo Finance news — no headlines returned.** "
        "The supplied feed says no news was found. "
        "**StockTwits — 30 most-recent messages; retail tone is divided.** "
        "Of those, 13 were tagged bullish, 3 bearish, and 14 unlabeled.",
        section_name="Social",
    )

    assert "Mixed" in summary and "StockTwits" in summary
    assert "AMD sentiment report" not in summary
    assert "Source-by-source evidence" not in summary


def test_news_fallback_keeps_executive_read_without_stitched_headline_list():
    summary = fallback_bullet_summary(
        "AMD — Weekly News Brief Executive read "
        "The news feed returned no company-specific news for AMD. "
        "The macro backdrop remains difficult for growth stocks. "
        "News and market tone The global feed included headlines: - One headline - Another headline. "
        "Those headlines are only context.",
        section_name="News",
    )

    assert "no company-specific news" in summary
    assert "Weekly News Brief" not in summary
    assert "One headline" not in summary


def test_amd_consolidated_report_shows_nonactionable_scenario_without_fake_confidence():
    market_snapshot = (
        "- Latest trading row used: 2026-10-15\n"
        "### Latest verified OHLCV row\n| Close | 550.45 |\n"
        "### Verified technical indicators (latest row)\n"
        "| close_50_sma | 536.11 |\n| close_10_ema | 595.59 |"
    )
    result = {
        "ticker": "AMD", "analysis_date": "2026-10-15", "decision": "Abstain",
        "price_target": None, "reference_price": 550.45,
        "target_validation_status": "Not Proposed", "target_horizon": None,
        "recommendation_confidence_score": None, "confidence_score": None,
        "results_dir": "/tmp/amd", "run_id": "amd-run",
        "final_state": {
            "trade_date": "2026-10-15",
            "final_trade_decision": (
                "**Decision Status**: Abstain\n\n**Executive Summary**: "
                "No actionable recommendation: a defensible target was unavailable."
            ),
            "market_report": "Market read. The close was below the short-term average.",
            "sentiment_report": "Sentiment is mixed.",
            "news_report": "No company-specific news was found.",
            "fundamentals_report": "Revenue and profit grew.",
            "trader_investment_plan": "**Action**: Hold",
            "shared_evidence": [{
                "evidence_id": "ev-market", "source": "get_verified_market_snapshot",
                "status": "available", "coverage": "full",
                "published_at": "2026-10-15", "as_of_date": "2026-10-15",
                "content": market_snapshot,
            }],
        },
    }

    markdown = build_consolidated_report([result], "2026-10-15")
    html = build_consolidated_report_html([result], "2026-10-15")

    assert "Observed technical scenario (not a validated target): 536.11–595.59" in markdown
    assert "Observed technical scenario" in html
    assert "536.11–595.59" in html
    assert "Low confidence" in html
    assert "50-day SMA to 10-day EMA" in html
    assert "| close_50_sma |" not in html
    assert "width:6%" not in html
    assert "No target outlook generated." not in html
    rendered = Selector(text=html)
    headers = rendered.css(".summary-table th::text").getall()
    assert headers == [
        "Ticker", "Decision", "Already own", "New position", "Validated target",
        "Non-actionable scenario", "Status",
    ]
    assert "/tmp/amd" not in rendered.css(".summary-table").get()
    assert "/tmp/amd" not in rendered.css(".meta-line").get()
    assert "/tmp/amd" in rendered.css("details.audit-details").get()


def _position_aware_result(**overrides):
    result = {
        "ticker": "BE",
        "analysis_date": "2026-08-14",
        "decision": "Hold",
        "price_target": None,
        "reference_price": 236.22,
        "confidence_score": None,
        "recommendation_confidence_score": 78,
        "target_horizon": None,
        "target_summary": None,
        "target_validation_status": "Rejected",
        "target_rejection_reason": "supporting_quote_not_in_evidence",
        "thesis": "Bullish",
        "existing_position_action": "Hold",
        "existing_position_summary": "Keep a medium position.",
        "new_position_action": "Conditional Buy",
        "new_position_summary": "Wait for confirmation or a controlled pullback.",
        "conditional_confirmation": "Buy after a sustained move above 248-253.",
        "conditional_alternative": "Accumulate near 222, with 211 as deeper support.",
        "conditional_invalidation": "A sustained break below 210 weakens the setup.",
        "results_dir": "/tmp/be",
        "final_state": {
            "final_trade_decision": "Evidence-led portfolio decision.",
            "market_report": "Market report.",
            "sentiment_report": "Social report.",
            "news_report": "News report.",
            "fundamentals_report": "Fundamentals report.",
            "trader_investment_plan": "Trader plan.",
        },
    }
    result.update(overrides)
    return result


def test_consolidated_reports_render_position_plan_and_accessible_bull_icon():
    result = _position_aware_result(
        new_position_summary="Wait <script>alert('x')</script> for confirmation.",
    )

    markdown = build_consolidated_report([result], "2026-08-14")
    html = build_consolidated_report_html([result], "2026-08-14")

    assert "| Ticker | Decision | Thesis | Existing Position | New Position |" in markdown
    assert "### Position Plan" in markdown
    assert "Existing position: **Hold** — Keep a medium position." in markdown
    assert "New position: **Conditional Buy**" in markdown
    assert "Confirmation: Buy after a sustained move above 248-253." in markdown
    assert "Alternative: Accumulate near 222, with 211 as deeper support." in markdown
    assert "Invalidation: A sustained break below 210 weakens the setup." in markdown
    assert "Recommendation confidence (uncalibrated): 78/100" in markdown
    assert "Rejected: supporting quote not in evidence" in markdown
    assert "No validated target" in html
    assert "Recommendation confidence (uncalibrated)" in html
    assert "78/100" in html
    assert "aria-label='Bullish thesis'" in html
    assert "class='condition-row condition-confirmation'" in html
    assert "class='condition-row condition-alternative'" in html
    assert "class='condition-row condition-invalidation'" in html
    assert "<script>" not in html
    assert "&lt;script&gt;alert(&#x27;x&#x27;)&lt;/script&gt;" in html


def test_consolidated_reports_label_accepted_range_method_risk_and_both_anchors():
    result = _position_aware_result(
        ticker="PENG", decision="Hold", reference_price=75.74,
        target_validation_status="Accepted", target_rejection_reason=None,
        target_low=72.83, target_high=77.10,
        confidence_score=18, target_horizon="3 months",
        target_summary="Demand persistence may support a technical recovery.",
        target_method="Technical scenario",
        target_risks="A failed breakout would invalidate the range.",
        supporting_quote_low="| Low | 72.83 |",
        supporting_quote_high="| High | 77.10 |",
    )

    markdown = build_consolidated_report([result], "2026-10-09")
    html = build_consolidated_report_html([result], "2026-10-09")

    assert "72.83–77.10" in markdown and "72.83–77.10" in html
    assert "Target Method: Technical scenario" in markdown
    assert "Target Risks: A failed breakout would invalidate the range." in markdown
    assert "Technical scenario" in html
    assert "A failed breakout would invalidate the range." in html
    assert "| Low | 72.83 |" in markdown and "| High | 77.10 |" in markdown
    assert "| Low | 72.83 |" in html and "| High | 77.10 |" in html
    assert "Validated targets: 0 of 1 completed runs" in html


def test_consolidated_report_hides_rejected_range_method_risk_and_anchors():
    result = _position_aware_result(
        ticker="PENG", target_low=72.83, target_high=77.10,
        target_summary="Unsupported interpretation secret",
        target_method="Unsupported method secret",
        target_risks="Unsupported risk secret",
        supporting_quote_low="| Low | 72.83 |",
        supporting_quote_high="| High | 77.10 |",
    )

    markdown = build_consolidated_report([result], "2026-10-09")
    html = build_consolidated_report_html([result], "2026-10-09")

    assert "72.83–77.10" not in markdown + html
    assert "Unsupported method secret" not in markdown + html
    assert "Unsupported interpretation secret" not in markdown + html
    assert "Unsupported risk secret" not in markdown + html
    assert "| Low | 72.83 |" not in markdown + html


def test_consolidated_html_renders_bear_and_neutral_thesis_icons():
    bearish = _position_aware_result(
        ticker="DOWN",
        decision="Underweight",
        thesis="Bearish",
        existing_position_action="Trim",
        new_position_action="Conditional Sell",
        new_position_summary="Wait for downside confirmation.",
        conditional_confirmation="Sell after a sustained break below support.",
        conditional_alternative="Trim into a failed rally.",
        conditional_invalidation="A sustained reclaim above resistance cancels the setup.",
    )
    neutral = _position_aware_result(
        ticker="FLAT",
        thesis="Neutral",
        existing_position_action="Hold",
        new_position_action="Wait",
        new_position_summary="No favorable entry is available.",
        conditional_confirmation=None,
        conditional_alternative=None,
        conditional_invalidation=None,
    )

    html = build_consolidated_report_html([bearish, neutral], "2026-08-14")

    assert "aria-label='Bearish thesis'" in html
    assert "aria-label='Neutral thesis'" in html
    assert "Conditional Sell" in html


def test_batch_summary_counts_only_validated_targets_and_explains_rejections():
    accepted_be = _position_aware_result(
        ticker="BE",
        price_target=240.0,
        confidence_score=65,
        target_validation_status="Accepted",
        target_rejection_reason=None,
    )
    accepted_tln = _position_aware_result(
        ticker="TLN",
        decision="Underweight",
        thesis="Bearish",
        price_target=352.16,
        confidence_score=70,
        target_validation_status="Accepted",
        target_rejection_reason=None,
    )
    rejected_plug = _position_aware_result(
        ticker="PLUG",
        decision="Underweight",
        thesis="Bearish",
        recommendation_confidence_score=82,
    )

    markdown = build_consolidated_report(
        [accepted_be, accepted_tln, rejected_plug],
        "2026-08-14",
    )
    html = build_consolidated_report_html(
        [accepted_be, accepted_tln, rejected_plug],
        "2026-08-14",
    )

    assert "| Target Status |" in markdown
    assert "No validated target" in markdown
    assert "Rejected: supporting quote not in evidence" in markdown
    assert "Validated targets: 2 of 3 completed runs" in html
    assert "296.08" not in html
    assert "Rejected: supporting quote not in evidence" in html
    assert "82/100" in html


def test_current_day_snapshot_is_visibly_marked_as_live_and_reproducible_by_run_id():
    result = _position_aware_result(
        snapshot_mode="live_current_day",
        run_id="job-live-123",
        evidence_fingerprint="sha256:abc123",
    )

    markdown = build_consolidated_report([result], "2026-08-14")
    html = build_consolidated_report_html([result], "2026-08-14")

    warning = "Live current-day snapshot: market and source data can change between runs."
    assert warning in markdown
    assert warning in html
    assert "Run ID: job-live-123" in markdown
    assert "Evidence fingerprint: sha256:abc123" in markdown
