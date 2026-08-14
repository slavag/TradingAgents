"""Report parity: the shared writer produces the report tree for the CLI and the
programmatic API alike (#1037)."""

import json
from types import SimpleNamespace

import pytest

from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.reporting import (
    build_run_manifest,
    compare_run_manifests,
    write_report_tree,
)


def _state():
    return {
        "market_report": "MKT",
        "news_report": "NEWS",
        "investment_debate_state": {"judge_decision": "RM PLAN"},
        "trader_investment_plan": "TRADE",
        "risk_debate_state": {"judge_decision": "PM DECISION"},
    }


@pytest.mark.unit
def test_write_report_tree_creates_files(tmp_path):
    out = write_report_tree(_state(), "AAPL", tmp_path)
    assert out.name == "complete_report.md"
    assert (tmp_path / "1_analysts" / "market.md").read_text() == "MKT"
    assert (tmp_path / "1_analysts" / "news.md").read_text() == "NEWS"
    assert (tmp_path / "2_research" / "manager.md").read_text() == "RM PLAN"
    assert (tmp_path / "3_trading" / "trader.md").read_text() == "TRADE"
    assert (tmp_path / "5_portfolio" / "decision.md").read_text() == "PM DECISION"
    complete = out.read_text()
    assert "Trading Analysis Report: AAPL" in complete
    assert "MKT" in complete and "PM DECISION" in complete


@pytest.mark.unit
def test_write_report_tree_records_a_stable_auditable_run_manifest(tmp_path):
    metadata = {
        "run_id": "job-123",
        "analysis_date": "2026-08-14",
        "snapshot_mode": "live_current_day",
        "models": {"quick": "gpt-5.4-mini", "deep": "gpt-5.5"},
        "temperature": 0.0,
    }

    write_report_tree(_state(), "AAPL", tmp_path / "one", run_metadata=metadata)
    write_report_tree(_state(), "AAPL", tmp_path / "two", run_metadata=metadata)

    first = json.loads((tmp_path / "one" / "run_manifest.json").read_text())
    second = json.loads((tmp_path / "two" / "run_manifest.json").read_text())

    assert first["ticker"] == "AAPL"
    assert first["run_id"] == "job-123"
    assert first["snapshot_mode"] == "live_current_day"
    assert first["evidence_fingerprint"].startswith("sha256:")
    assert first["decision_fingerprint"].startswith("sha256:")
    assert first["evidence_fingerprint"] == second["evidence_fingerprint"]
    assert first["decision_fingerprint"] == second["decision_fingerprint"]


@pytest.mark.unit
@pytest.mark.parametrize(
    ("previous", "current", "expected"),
    [
        (None, {"evidence_fingerprint": "e1", "decision_fingerprint": "d1"}, "first_recorded_run"),
        (
            {"evidence_fingerprint": "e1", "decision_fingerprint": "d1"},
            {"evidence_fingerprint": "e1", "decision_fingerprint": "d1"},
            "reproduced",
        ),
        (
            {"evidence_fingerprint": "e1", "decision_fingerprint": "d1"},
            {"evidence_fingerprint": "e1", "decision_fingerprint": "d2"},
            "decision_changed_same_evidence",
        ),
        (
            {"evidence_fingerprint": "e1", "decision_fingerprint": "d1"},
            {"evidence_fingerprint": "e2", "decision_fingerprint": "d2"},
            "evidence_changed",
        ),
    ],
)
def test_compare_run_manifests_distinguishes_input_drift_from_model_drift(
    previous,
    current,
    expected,
):
    assert compare_run_manifests(current, previous) == expected


@pytest.mark.unit
def test_decision_fingerprint_tracks_outcome_not_prose_wording():
    first_state = _state() | {
        "final_trade_decision": (
            "**Decision Status**: Actionable\n\n"
            "**Rating**: Hold\n\n"
            "**Executive Summary**: First wording.\n\n"
            "**Thesis**: Neutral\n\n"
            "**Existing Position**: Hold\n\n"
            "**New Position**: Wait\n\n"
            "**Target Validation**: Rejected (target_bundle_incomplete)"
        )
    }
    rewritten_state = _state() | {
        "final_trade_decision": first_state["final_trade_decision"].replace(
            "First wording.",
            "Entirely different prose.",
        )
    }
    changed_outcome_state = _state() | {
        "final_trade_decision": first_state["final_trade_decision"].replace(
            "**Rating**: Hold",
            "**Rating**: Underweight",
        )
    }

    first = build_run_manifest(first_state, "AAPL")
    rewritten = build_run_manifest(rewritten_state, "AAPL")
    changed = build_run_manifest(changed_outcome_state, "AAPL")

    assert first["decision_fingerprint"] == rewritten["decision_fingerprint"]
    assert first["decision_content_fingerprint"] != rewritten["decision_content_fingerprint"]
    assert first["decision_fingerprint"] != changed["decision_fingerprint"]


@pytest.mark.unit
def test_save_reports_explicit_path(tmp_path):
    # Unbound: with an explicit save_path, the method doesn't touch self/config.
    out = TradingAgentsGraph.save_reports(None, _state(), "AAPL", save_path=tmp_path)
    assert (tmp_path / "complete_report.md").exists()
    assert out == tmp_path / "complete_report.md"


@pytest.mark.unit
def test_save_reports_defaults_under_results_dir(tmp_path):
    mock_self = SimpleNamespace(config={"results_dir": str(tmp_path)})
    out = TradingAgentsGraph.save_reports(mock_self, _state(), "AAPL")
    assert out.exists()
    assert out.parent.parent.name == "reports"  # results_dir/reports/AAPL_<stamp>/...
    assert out.parent.name.startswith("AAPL_")
