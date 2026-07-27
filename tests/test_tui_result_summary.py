from io import StringIO

from rich.console import Console

import cli.main as cli_main


def render_table(results: list[dict]) -> str:
    output = StringIO()
    console = Console(
        file=output,
        width=220,
        color_system=None,
        force_terminal=False,
    )
    console.print(cli_main.build_tui_result_summary(results))
    return output.getvalue()


def test_tui_result_summary_renders_successful_metrics():
    output = render_table(
        [
            {
                "ticker": "NVDA",
                "decision": "Buy",
                "price_target": 185.25,
                "confidence_score": 78,
                "target_horizon": "3 months",
                "target_summary": "Earnings growth supports the target.",
            }
        ]
    )

    assert "Ticker" in output
    assert "Decision" in output
    assert "Confidence (uncalibrated)" in output
    assert "NVDA" in output
    assert "Buy" in output
    assert "185.25" in output
    assert "78/100" in output
    assert "3 months" in output
    assert "Earnings growth supports the target." in output


def test_tui_result_summary_keeps_missing_metrics_unavailable():
    output = render_table(
        [
            {
                "ticker": "PENG",
                "decision": "Hold",
                "price_target": None,
                "confidence_score": None,
                "target_horizon": None,
                "target_summary": None,
            }
        ]
    )

    assert "PENG" in output
    assert "Hold" in output
    assert output.count("—") >= 4
    assert "50/100" not in output


def test_tui_result_summary_renders_every_ticker_and_failure():
    output = render_table(
        [
            {
                "ticker": "NVDA",
                "decision": "Buy",
                "price_target": 185.25,
                "confidence_score": 78,
                "target_horizon": "3 months",
                "target_summary": "Supported outlook.",
            },
            {
                "ticker": "BROKEN",
                "decision": None,
                "price_target": None,
                "confidence_score": None,
                "target_horizon": None,
                "target_summary": None,
                "error": RuntimeError("provider unavailable"),
            },
        ]
    )

    assert output.count("NVDA") == 1
    assert output.count("BROKEN") == 1
    assert "Failed" in output
    assert "provider unavailable" in output


def test_run_analysis_prints_summary_before_full_report_prompt(monkeypatch):
    result = {
        "ticker": "NVDA",
        "analysis_date": "2026-07-27",
        "decision": "Buy",
        "final_state": {"final_trade_decision": "**Rating**: Buy"},
        "results_dir": "/tmp/nvda",
        "price_target": 185.25,
        "confidence_score": 78,
        "target_horizon": "3 months",
        "target_summary": "Supported outlook.",
        "reference_price": 170.0,
    }
    events = []
    summary_table = object()

    monkeypatch.setattr(
        cli_main,
        "get_user_selections",
        lambda: {"tickers": ["NVDA"], "analysis_date": "2026-07-27"},
    )
    monkeypatch.setattr(
        cli_main,
        "get_save_preferences",
        lambda _selections: {"save_enabled": False, "save_path": None},
    )
    monkeypatch.setattr(
        cli_main,
        "run_single_analysis",
        lambda *_args, **_kwargs: result,
    )

    def build_summary(results):
        assert results == [result]
        events.append("summary-built")
        return summary_table

    def print_output(*args, **_kwargs):
        if args and args[0] is summary_table:
            events.append("summary-printed")

    def prompt(*_args, **_kwargs):
        events.append("prompt")
        return "N"

    monkeypatch.setattr(cli_main, "build_tui_result_summary", build_summary)
    monkeypatch.setattr(cli_main.console, "print", print_output)
    monkeypatch.setattr(cli_main.typer, "prompt", prompt)

    cli_main.run_analysis()

    assert events == ["summary-built", "summary-printed", "prompt"]
