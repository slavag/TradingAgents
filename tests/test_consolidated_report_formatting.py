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

    assert "Model confidence (uncalibrated)" in markdown
    assert "| PENG | Hold | - | - | - |" in markdown
    assert "Model confidence (uncalibrated)" in html
    assert "Average target across completed runs: -" in html


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
