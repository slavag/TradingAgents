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
    assert "Average validated target across 0 of 1 completed runs: -" in html
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
    assert "Average validated target across 2 of 3 completed runs: 296.08" in html
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
