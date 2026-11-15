"""The optional reaction marker survives source-only reporting without repricing daily data."""
import json

from tradingagents.reporting import write_report_tree
from tradingagents.web.service import serialize_result


def reaction():
    return {
        "symbol": "CBRS", "analysis_date": "2026-11-13", "status": "available",
        "quote_type": "post_market", "price": 120, "regular_close": 100, "change_pct": 20,
        "observed_at": "2026-11-12T23:00:00+00:00", "observed_at_exchange": "2026-11-12T18:00:00-05:00",
        "regular_observed_at": "2026-11-12T21:00:00+00:00", "fetched_at": "2026-11-13T07:00:00+00:00",
        "age_seconds": 28800, "currency": "USD", "exchange_timezone": "America/New_York",
        "provider": "yfinance", "note": "Informational, not causal proof linking the move to news.",
    }


def test_marker_is_persisted_and_included_in_web_without_changing_daily_target(tmp_path):
    source = {"extended_hours_reaction": reaction(), "trade_date": "2026-11-13"}
    report = write_report_tree(source, "CBRS", tmp_path)
    assert json.loads((tmp_path / "market_reaction.json").read_text()) == reaction()
    assert "## Extended-hours market reaction" not in report.read_text()
    web = serialize_result({"ticker": "CBRS", "analysis_date": "2026-11-13", "final_state": source,
                            "price_target": 110, "target_validation_status": "Accepted",
                            "results_dir": str(tmp_path), "report_path": str(report)})
    assert web["extended_hours_reaction"] == reaction()
    assert web["price_target"] == 110



def test_market_evidence_remains_available_to_decisions_without_a_report_notice(monkeypatch):
    from types import SimpleNamespace

    from tradingagents.agents.analysts import market_analyst
    from tradingagents.agents.utils.agent_utils import get_agent_context_from_state

    prompts = []
    monkeypatch.setattr(market_analyst, "fetch_extended_hours_reaction", lambda *_: reaction())
    monkeypatch.setattr(market_analyst, "take_turn", lambda prompt, *_: prompts.append(prompt.format(messages=[])) or SimpleNamespace(content="Daily price view.", tool_calls=[]))
    monkeypatch.setattr(market_analyst, "verified_market_snapshot", lambda *_: SimpleNamespace(model_dump=lambda **_: {}))
    source = {"company_of_interest": "CBRS", "trade_date": "2026-11-13", "analysis_mode": "live", "messages": []}
    source.update(market_analyst.create_market_analyst(None)(source))
    assert "+20.00%" in prompts[0]
    assert "+20.00%" in get_agent_context_from_state(source, "portfolio_manager", include_reports=True)
    assert source["market_report"] == "Daily price view."


def test_available_and_unavailable_markers_do_not_create_standalone_slides():
    from tests.test_investor_deck_acceptance import (
        peng_state_from_public_sec_fixtures_and_saved_vendor_rows,
    )
    from tradingagents.investor_deck import build_investor_deck

    source = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    for record in (reaction(), {"status": "unavailable", "reason": "outside_supported_session"}):
        source["extended_hours_reaction"] = record
        deck = build_investor_deck(source, "PENG", "2026-10-09", None)
        assert deck is not None
        assert all(slide.topic != "market_reaction" for slide in deck.slides)


def test_consolidated_reports_do_not_add_a_standalone_marker_section():
    from cli.main import build_consolidated_report, build_consolidated_report_html
    source = {"extended_hours_reaction": reaction(), "trade_date": "2026-11-13"}
    result = {"ticker": "CBRS", "analysis_date": "2026-11-13", "final_state": source,
              "decision": "Hold", "results_dir": "/tmp", "report_path": "/tmp/report.md"}
    for rendered in (build_consolidated_report([result], "2026-11-13"),
                     build_consolidated_report_html([result], "2026-11-13")):
        assert "Extended-hours market reaction" not in rendered


def test_missing_saved_marker_uses_a_readable_explanation():
    from tradingagents.dataflows.market_reaction import render_market_reaction
    text = render_market_reaction({"status": "unavailable", "reason": "not_captured_in_saved_run"})
    assert "not recorded in the saved run" in text
    assert "not_captured_in_saved_run" not in text
