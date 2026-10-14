"""User-facing report files share a safe UTC timestamp and selected ticker names."""

from unittest.mock import MagicMock

import pytest

from cli import main as cli_main
from tradingagents.investor_deck import DeckSlide, InvestorDeck
from tradingagents.report_names import report_stem


def _deck(ticker):
    return InvestorDeck(
        ticker=ticker, analysis_date="2026-10-08", headline=f"{ticker} company research",
        sources={}, slides=[DeckSlide(
            topic="business", title="Business model", takeaway="Source-backed view",
        )],
    )


@pytest.mark.unit
def test_batch_report_names_include_checked_tickers_and_utc_timestamp(tmp_path, monkeypatch):
    monkeypatch.setattr(cli_main, "build_consolidated_report", lambda *a, **k: "markdown")
    monkeypatch.setattr(cli_main, "build_consolidated_report_html", lambda *a, **k: "html")

    paths = cli_main.save_consolidated_report(
        [{"ticker": "AAPL"}], "2026-10-08", tmp_path,
        tickers=["AAPL", "NVDA"], report_timestamp="20261008T132500Z",
    )

    assert paths["markdown"].name == "consolidated_AAPL_NVDA_20261008T132500Z.md"
    assert paths["html"].name == "consolidated_AAPL_NVDA_20261008T132500Z.html"


@pytest.mark.unit
def test_batch_report_names_reject_unsafe_ticker_before_creating_directory(tmp_path):
    with pytest.raises(ValueError):
        cli_main.save_consolidated_report(
            [], "2026-10-08", tmp_path / "report",
            tickers=["../../outside"], report_timestamp="20261008T132500Z",
        )
    assert not (tmp_path / "report").exists()


@pytest.mark.unit
def test_report_names_reject_impossible_utc_timestamp():
    with pytest.raises(ValueError):
        report_stem("full", ["AAPL"], "20261399T999999Z")


@pytest.mark.unit
def test_cli_saves_one_new_deck_with_shared_report_timestamp(tmp_path, monkeypatch):
    llm = object()
    result = {
        "ticker": "AAPL", "analysis_date": "2026-10-08", "decision": "Hold",
        "final_state": {"market_report": "Market facts"},
        "results_dir": str(tmp_path), "_investor_llm": llm,
    }
    deck = _deck("AAPL")
    build_deck = MagicMock(return_value=deck)
    save_report = MagicMock(return_value=tmp_path / "AAPL_full_20261008T132500Z.md")
    monkeypatch.setattr(cli_main, "get_user_selections", lambda: {
        "tickers": ["AAPL"], "analysis_date": "2026-10-08",
    })
    monkeypatch.setattr(cli_main, "get_save_preferences", lambda _: {
        "save_enabled": True, "save_path": tmp_path,
    })
    monkeypatch.setattr(cli_main, "run_single_analysis", lambda *a, **k: result)
    monkeypatch.setattr(cli_main, "build_investor_deck", build_deck, raising=False)
    monkeypatch.setattr(cli_main, "save_report_to_disk", save_report)
    monkeypatch.setattr(cli_main, "utc_report_timestamp", lambda: "20261008T132500Z", raising=False)
    monkeypatch.setattr(cli_main, "build_tui_result_summary", lambda _: "summary")
    monkeypatch.setattr(cli_main.console, "print", lambda *a, **k: None)
    monkeypatch.setattr(cli_main.typer, "prompt", lambda *a, **k: "N")

    cli_main.run_analysis()

    build_deck.assert_called_once_with(result["final_state"], "AAPL", "2026-10-08", llm)
    assert save_report.call_args.kwargs["investor_deck"] is deck
    assert save_report.call_args.kwargs["report_timestamp"] == "20261008T132500Z"


@pytest.mark.unit
def test_cli_batch_reuses_stamp_for_each_ticker_and_both_batch_exports(tmp_path, monkeypatch):
    tickers = ["AAPL", "NVDA"]
    def build_ticker_deck(_state, ticker, _date, _llm):
        if ticker == "NVDA":
            raise RuntimeError("optional deck synthesis failed")
        return _deck(ticker)

    build_deck = MagicMock(side_effect=build_ticker_deck)
    export_root = tmp_path / "export"
    monkeypatch.setitem(cli_main.DEFAULT_CONFIG, "results_dir", str(tmp_path / "results"))
    monkeypatch.setattr(cli_main, "get_user_selections", lambda: {
        "tickers": tickers, "analysis_date": "2026-10-08",
    })
    monkeypatch.setattr(cli_main, "get_save_preferences", lambda _: {
        "save_enabled": True, "save_path": export_root,
    })
    monkeypatch.setattr(cli_main, "run_single_analysis", lambda _s, ticker, **_k: {
        "ticker": ticker, "analysis_date": "2026-10-08", "decision": "Hold",
        "final_state": {"market_report": f"Market for {ticker}"},
        "results_dir": str(tmp_path / ticker), "_investor_llm": object(),
    })
    monkeypatch.setattr(cli_main, "build_investor_deck", build_deck, raising=False)
    monkeypatch.setattr(cli_main, "utc_report_timestamp", lambda: "20261008T132500Z")
    monkeypatch.setattr(cli_main, "build_consolidated_report", lambda *a, **k: "batch md")
    monkeypatch.setattr(cli_main, "build_consolidated_report_html", lambda *a, **k: "batch html")
    monkeypatch.setattr(cli_main, "build_tui_result_summary", lambda _: "summary")
    monkeypatch.setattr(cli_main.console, "print", lambda *a, **k: None)
    monkeypatch.setattr(cli_main.typer, "prompt", lambda *a, **k: "N")

    cli_main.run_analysis()

    assert build_deck.call_count == 2
    for ticker in tickers:
        assert (export_root / ticker / f"{ticker}_full_20261008T132500Z.md").is_file()
        assert (export_root / ticker / f"{ticker}_investor_20261008T132500Z.html").is_file()
        assert (export_root / ticker / f"{ticker}_full_20261008T132500Z.html").is_file()
        investor_html = (
            export_root / ticker / f"{ticker}_investor_20261008T132500Z.html"
        ).read_text()
        if ticker == "AAPL":
            assert "AAPL company research" in investor_html
        else:
            assert "Research deck unavailable" in investor_html
    batch_name = "consolidated_AAPL_NVDA_20261008T132500Z"
    assert (export_root / f"{batch_name}.md").is_file()
    assert (export_root / f"{batch_name}.html").is_file()
    assert len(list((tmp_path / "results" / "batch").rglob(f"{batch_name}.md"))) == 1
