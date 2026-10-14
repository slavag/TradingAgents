"""Report parity: the shared writer produces the report tree for the CLI and the
programmatic API alike (#1037)."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tradingagents.forecasting.schemas import ForecastRecord
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.investor_brief import BriefCard, BriefClaim, InvestorBrief
from tradingagents.investor_deck import DeckSlide, InvestorDeck
from tradingagents.reporting import (
    build_run_manifest,
    compare_run_manifests,
    write_report_tree,
)
from tradingagents.research_ledger import ResearchSource


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
    assert out.name.startswith("AAPL_full_20") and out.name.endswith("Z.md")
    assert (tmp_path / "1_analysts" / "market.md").read_text() == "MKT"
    assert (tmp_path / "1_analysts" / "news.md").read_text() == "NEWS"
    assert (tmp_path / "2_research" / "manager.md").read_text() == "RM PLAN"
    assert (tmp_path / "3_trading" / "trader.md").read_text() == "TRADE"
    assert (tmp_path / "5_portfolio" / "decision.md").read_text() == "PM DECISION"
    complete = out.read_text()
    assert "Trading Analysis Report: AAPL" in complete
    assert "MKT" in complete and "PM DECISION" in complete


@pytest.mark.unit
def test_top_level_ticker_reports_share_one_utc_filename_timestamp(tmp_path):
    out = write_report_tree(
        _state(), "AAPL", tmp_path, report_timestamp="20261008T132500Z"
    )

    assert out.name == "AAPL_full_20261008T132500Z.md"
    assert (tmp_path / "AAPL_full_20261008T132500Z.html").is_file()
    assert (tmp_path / "1_analysts" / "market.md").is_file()


@pytest.mark.unit
def test_writer_uses_new_deck_and_preserves_shared_report_stamp(tmp_path):
    source = ResearchSource(
        "ev-business", "sec", "PENG annual filing",
        "https://www.sec.gov/Archives/example", "2026-10-06", "Business", "available",
    )
    deck = InvestorDeck(
        ticker="PENG", analysis_date="2026-10-09", headline="PENG company research",
        sources={"ev-business": source}, slides=[DeckSlide(
            topic="business", title="Business model", takeaway="Memory supports AI demand",
            source_ids=["ev-business"],
        )],
    )

    write_report_tree(
        _state(), "PENG", tmp_path, report_timestamp="20261009T154737Z",
        investor_deck=deck,
    )

    html = (tmp_path / "PENG_investor_20261009T154737Z.html").read_text()
    assert "Business model" in html and "source-1" in html
    assert (tmp_path / "PENG_full_20261009T154737Z.html").is_file()


@pytest.mark.unit
def test_writer_persists_source_only_reason_outside_investor_html(tmp_path):
    deck = InvestorDeck(
        ticker="PENG", analysis_date="2026-10-09", headline="PENG research",
        sources={}, synthesis_status="source_only",
        synthesis_reason="unsupported_interpretation",
        slides=[
            DeckSlide(topic="business", title="Business", takeaway="Source-backed view"),
            DeckSlide(topic="revenue", title="Revenue", takeaway="Source-backed facts"),
        ],
    )

    write_report_tree(
        _state(), "PENG", tmp_path, run_metadata={"run_id": "job-1"},
        report_timestamp="20261009T154737Z", investor_deck=deck,
    )

    manifest = json.loads((tmp_path / "run_manifest.json").read_text())
    html = (tmp_path / "PENG_investor_20261009T154737Z.html").read_text()
    assert manifest["investor_deck_synthesis_status"] == "source_only"
    assert manifest["investor_deck_synthesis_reason"] == "unsupported_interpretation"
    assert "unsupported_interpretation" not in html


@pytest.mark.unit
def test_investor_deck_unknown_reason_is_bounded_before_persistence(tmp_path):
    deck = InvestorDeck(
        ticker="PENG", analysis_date="2026-10-09", headline="PENG research",
        sources={}, synthesis_status="source_only",
        synthesis_reason="raw private model response",
        slides=[DeckSlide(topic="business", title="Business", takeaway="View")],
    )

    write_report_tree(
        _state(), "PENG", tmp_path, run_metadata={"run_id": "job-1"},
        investor_deck=deck,
    )

    manifest = (tmp_path / "run_manifest.json").read_text()
    assert "raw private model response" not in manifest
    assert json.loads(manifest)["investor_deck_synthesis_reason"] == "invalid_structured_response"


@pytest.mark.unit
def test_deck_render_failure_does_not_remove_full_report(tmp_path, monkeypatch):
    from tradingagents import reporting

    deck = InvestorDeck(
        ticker="PENG", analysis_date="2026-10-09", headline="PENG research",
        sources={}, slides=[DeckSlide(topic="business", title="Business", takeaway="View")],
    )
    monkeypatch.setattr(reporting, "render_investor_deck", lambda *a: (
        _ for _ in ()
    ).throw(ValueError("bad optional deck")))

    out = write_report_tree(
        _state(), "PENG", tmp_path, report_timestamp="20261009T154737Z",
        investor_deck=deck,
    )

    assert out.is_file()
    assert (tmp_path / "PENG_full_20261009T154737Z.html").is_file()
    assert "Research deck unavailable" in (
        tmp_path / "PENG_investor_20261009T154737Z.html"
    ).read_text()


@pytest.mark.unit
def test_writer_emits_safe_unavailable_investor_html_without_a_validated_deck(tmp_path):
    state = _state() | {"market_report": "<script>alert('unsafe')</script> Market analysis."}

    write_report_tree(state, "AAPL", tmp_path, report_timestamp="20261008T132500Z")

    html = (tmp_path / "AAPL_investor_20261008T132500Z.html").read_text()
    assert "Research deck unavailable" in html
    assert "AAPL" in html
    assert "default-src 'none'" in html
    assert "<script>" not in html
    assert 'href="AAPL_full_20261008T132500Z.html"' in html


@pytest.mark.unit
def test_writer_keeps_full_report_when_optional_source_list_is_missing(tmp_path):
    state = _state() | {"shared_evidence": None}

    out = write_report_tree(
        state, "AAPL", tmp_path, report_timestamp="20261008T132500Z"
    )

    assert out.is_file()
    assert (tmp_path / "AAPL_investor_20261008T132500Z.html").is_file()


@pytest.mark.unit
def test_investor_report_labels_metadata_only_sources(tmp_path):
    state = _state() | {"shared_evidence": [{
        "evidence_id": "ev-filing", "source": "sec", "title": "SEC 8-K filing",
        "content": "Filing metadata only; narrative not extracted.",
        "coverage": "metadata_only", "status": "available",
    }]}
    brief = InvestorBrief(
        headline="AAPL investor view",
        cards=[BriefCard(
            title="What remains unknown", takeaway="The filing narrative is unavailable.",
            claims=[BriefClaim(
                text="Only filing metadata was collected.",
                evidence_ids=["ev-filing"], classification="gap",
            )],
        )],
    )

    write_report_tree(
        state, "AAPL", tmp_path, report_timestamp="20261008T132500Z",
        investor_brief=brief,
    )

    html = (tmp_path / "AAPL_investor_20261008T132500Z.html").read_text()
    assert "SEC filing record (details unavailable)" in html
    assert "SEC 8-K filing (metadata only)" not in html


@pytest.mark.unit
def test_write_report_tree_creates_full_safe_html_beside_markdown(tmp_path):
    state = _state() | {
        "market_report": '<script>alert("unsafe")</script> Market analysis.',
        "final_trade_decision": "**Rating**: Hold",
    }

    out = write_report_tree(state, "AAPL", tmp_path)

    html = out.with_suffix(".html").read_text(encoding="utf-8")
    assert 'class="ticker">AAPL</span>' in html
    assert "Market analysis." in html
    assert "RM PLAN" in html
    assert "TRADE" in html
    assert "PM DECISION" in html
    assert "&lt;script&gt;" in html
    assert "<script>" not in html
    assert "default-src 'none'" in html


@pytest.mark.unit
def test_write_report_tree_records_a_stable_auditable_run_manifest(tmp_path):
    metadata = {
        "run_id": "job-123",
        "analysis_date": "2026-08-14",
        "snapshot_mode": "live_current_day",
        "models": {"quick": "gpt-5.4-mini", "deep": "gpt-5.5"},
        "temperature": 0.0,
        "generated_at": "2026-08-20T18:30:00+00:00",
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
def test_write_report_tree_persists_valid_forecast_record(tmp_path):
    state = _state() | {
        "asset_type": "stock",
        "trade_date": "2026-08-20",
        "final_trade_decision": "**Rating**: Hold",
    }
    metadata = {
        "generated_at": "2026-08-20T18:30:00+00:00",
        "models": {},
        "temperature": 0.0,
    }

    write_report_tree(state, "AAPL", tmp_path, run_metadata=metadata)

    payload = json.loads((tmp_path / "forecast_record.json").read_text())
    record = ForecastRecord.model_validate(payload)
    assert record.canonical_symbol == "AAPL"
    assert record.record_id.startswith("sha256:")


@pytest.mark.unit
def test_forecast_record_persistence_is_idempotent_for_same_record(tmp_path):
    state = _state() | {
        "asset_type": "stock",
        "trade_date": "2026-08-20",
        "final_trade_decision": "**Rating**: Hold",
    }
    metadata = {
        "generated_at": "2026-08-20T18:30:00+00:00",
        "models": {},
        "temperature": 0.0,
    }

    write_report_tree(state, "AAPL", tmp_path, run_metadata=metadata)
    first = (tmp_path / "forecast_record.json").read_text()
    write_report_tree(state, "AAPL", tmp_path, run_metadata=metadata)

    assert (tmp_path / "forecast_record.json").read_text() == first


@pytest.mark.unit
def test_forecast_record_persistence_rejects_overwrite_with_different_record(tmp_path):
    state = _state() | {
        "asset_type": "stock",
        "trade_date": "2026-08-20",
        "final_trade_decision": "**Rating**: Hold",
    }
    metadata = {
        "generated_at": "2026-08-20T18:30:00+00:00",
        "models": {},
        "temperature": 0.0,
    }
    write_report_tree(state, "AAPL", tmp_path, run_metadata=metadata)

    with pytest.raises(FileExistsError, match="immutable forecast record"):
        write_report_tree(
            state | {"final_trade_decision": "**Rating**: Sell"},
            "AAPL",
            tmp_path,
            run_metadata=metadata,
        )


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
    assert out.exists()
    assert out.parent == tmp_path
    assert out.name.startswith("AAPL_full_20") and out.name.endswith("Z.md")


@pytest.mark.unit
def test_programmatic_save_uses_final_report_model_for_investor_deck(tmp_path, monkeypatch):
    from tradingagents.graph import trading_graph

    llm = object()
    graph = SimpleNamespace(final_report_llm=llm)
    deck = InvestorDeck(
        ticker="AAPL", analysis_date="2026-10-08", headline="AAPL company research",
        sources={}, slides=[DeckSlide(
            topic="business", title="Business model", takeaway="A source-backed view",
        )],
    )
    build_deck = MagicMock(return_value=deck)
    monkeypatch.setattr(trading_graph, "build_investor_deck", build_deck, raising=False)
    state = _state() | {"trade_date": "2026-10-08"}

    TradingAgentsGraph.save_reports(graph, state, "AAPL", save_path=tmp_path)

    build_deck.assert_called_once_with(state, "AAPL", "2026-10-08", llm)
    investor = next(tmp_path.glob("AAPL_investor_*.html"))
    assert "AAPL company research" in investor.read_text()


@pytest.mark.unit
def test_save_reports_defaults_under_results_dir(tmp_path):
    mock_self = SimpleNamespace(config={"results_dir": str(tmp_path)})
    out = TradingAgentsGraph.save_reports(mock_self, _state(), "AAPL")
    assert out.exists()
    assert out.parent.parent.name == "reports"  # results_dir/reports/AAPL_<stamp>/...
    assert out.parent.name.startswith("AAPL_")
