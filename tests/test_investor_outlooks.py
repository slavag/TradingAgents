"""Independent, evidence-backed investment horizons without live providers."""

from datetime import date

import pytest

from tradingagents.agents.schemas import PortfolioOutlookDraft
from tradingagents.agents.utils.outlooks import add_months, validate_outlooks


def outlook(months=6, **changes):
    values = {
        "months": months,
        "rating": "Buy",
        "recommendation_confidence_score": 65,
        "summary": "Improving demand could support recovery, subject to cash conversion.",
        "drivers": ["Demand persistence and operating execution."],
        "upside_case": "Better margins could support a stronger rerating.",
        "downside_case": "Weaker demand and ongoing cash use could reverse the recovery.",
        "invalidation": "Reconsider if reported cash conversion deteriorates.",
        "source_ids": ["ev-market"],
        "price_target": 110 if months == 6 else 125,
        "target_basis": "Model estimate",
        "target_assumptions": "Assume demand supports a recovery from the verified close.",
        "supporting_quote": "| Close | 100.00 |",
        "confidence_score": 25,
        "target_summary": "A conditional recovery scenario based on the verified close.",
        "target_method": "Verified close and explicit forward growth assumptions.",
        "target_risks": "Demand or cash conversion may disappoint.",
    }
    values.update(changes)
    return PortfolioOutlookDraft(**values)


def state():
    return {
        "trade_date": "2026-11-13",
        "market_report": "Verified close: 100.",
        "verified_market_snapshot": {"observed_on": "2026-11-12", "quote_currency": "USD"},
        "shared_evidence": [
            {
                "evidence_id": "ev-market",
                "source": "get_verified_market_snapshot",
                "category": "market",
                "status": "available",
                "published_at": "2026-11-12",
                "as_of_date": "2026-11-13",
                "content": "Verified close: 100.\n| Close | 100.00 |",
            }
        ],
    }


def test_horizons_keep_independent_targets_recommendations_and_confidences():
    result = validate_outlooks(
        [
            outlook(),
            outlook(
                12,
                rating="Underweight",
                price_target=80,
                recommendation_confidence_score=45,
                confidence_score=10,
            ),
        ],
        state(),
    )
    assert [(r.months, r.end_date) for r in result] == [(6, "2027-05-13"), (12, "2027-11-13")]
    assert [r.rating for r in result] == ["Buy", "Underweight"]
    assert [r.target.price_target for r in result] == [110, 80]
    assert [r.recommendation_confidence_score for r in result] == [65, 45]
    assert [r.target.confidence_score for r in result] == [25, 10]
    assert result[0].reference_observed_on == "2026-11-12"


def test_missing_or_duplicate_horizons_are_diagnostic_not_copied_tactical_targets():
    source = state() | {"portfolio_decision": {"price_target": 999}}
    missing = validate_outlooks([], source)
    assert len(missing) == 2
    assert all(r.status == "unavailable" and r.target is None for r in missing)
    duplicate = validate_outlooks([outlook(), outlook()], source)
    assert duplicate[0].reason == "duplicate_horizon"
    assert duplicate[1].reason == "missing_horizon"


def test_invalid_outlook_target_preserves_its_recommendation_confidence():
    result = validate_outlooks([outlook(price_target=90)], state())[0]
    assert result.status == "target_unavailable"
    assert result.rating == "Buy" and result.recommendation_confidence_score == 65
    assert result.target is None
    assert result.reason == "target_direction_conflict"


def test_unadmitted_sources_cannot_support_an_outlook():
    result = validate_outlooks([outlook(source_ids=["fake-source"])], state())[0]
    assert result.status == "unavailable"
    assert result.reason == "outlook_sources_invalid"


@pytest.mark.parametrize(
    "start,months,expected",
    [
        ("2026-08-31", 6, "2027-02-28"),
        ("2023-08-31", 6, "2024-02-29"),
        ("2024-02-29", 12, "2025-02-28"),
    ],
)
def test_calendar_endpoints(start, months, expected):
    assert add_months(date.fromisoformat(start), months).isoformat() == expected


def pm_state():
    from tests.test_portfolio_manager_integrity import make_pm_state

    return make_pm_state() | state()


def test_pm_preserves_tactical_decision_and_publishes_two_horizons_without_extra_call():
    from unittest.mock import MagicMock

    from tests.test_portfolio_manager_integrity import actionable_draft
    from tradingagents.agents.managers.portfolio_manager import create_portfolio_manager

    draft = actionable_draft(
        rating="Hold",
        price_target=100,
        supporting_quote="| Close | 100.00 |",
        time_horizon="1 month",
        confidence_score=40,
        target_summary="Neutral central case.",
        target_method="Technical scenario",
        target_risks="Trend may change.",
        outlooks=[outlook(), outlook(12)],
    )
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = draft
    result = create_portfolio_manager(llm)(pm_state())
    assert result["portfolio_decision"]["price_target"] == 100
    assert [r["target"]["price_target"] for r in result["portfolio_outlooks"]] == [110, 125]
    assert llm.with_structured_output.return_value.invoke.call_count == 1


def test_missing_horizons_get_one_repair_without_changing_tactical_decision():
    from unittest.mock import MagicMock

    from tests.test_portfolio_manager_integrity import actionable_draft
    from tradingagents.agents.managers.portfolio_manager import create_portfolio_manager
    from tradingagents.agents.schemas import PortfolioDecisionDraft, PortfolioOutlookRepair

    initial, repair = MagicMock(), MagicMock()
    initial.invoke.return_value = actionable_draft(
        rating="Hold",
        price_target=100,
        supporting_quote="| Close | 100.00 |",
        time_horizon="1 month",
        confidence_score=40,
        target_summary="Neutral central case.",
        target_method="Technical scenario",
        target_risks="Trend may change.",
    )
    repair.invoke.return_value = PortfolioOutlookRepair(outlooks=[outlook(), outlook(12)])
    llm = MagicMock()
    llm.with_structured_output.side_effect = lambda schema: (
        initial if schema is PortfolioDecisionDraft else repair
    )
    result = create_portfolio_manager(llm)(pm_state())
    assert result["portfolio_decision"]["rating"] == "Hold"
    assert result["portfolio_decision"]["price_target"] == 100
    assert len(result["portfolio_outlooks"]) == 2
    assert initial.invoke.call_count == 1 and repair.invoke.call_count == 1


def test_outlooks_are_persisted_and_rendered_without_overwriting_tactical_forecast(tmp_path):
    import json

    from tradingagents.reporting import write_report_tree
    from tradingagents.web.service import serialize_result

    rows = [r.model_dump(mode="json") for r in validate_outlooks([outlook(), outlook(12)], state())]
    source = state() | {"portfolio_outlooks": rows}
    report = write_report_tree(source, "CBRS", tmp_path)
    assert json.loads((tmp_path / "outlooks.json").read_text())["outlooks"] == rows
    text = report.read_text()
    assert "6-month outlook" in text and "12-month outlook" in text
    assert "65/100" in text and "25/100" in text
    assert "2027-05-13" in text and "2027-11-13" in text
    assert "Model estimate" in text
    web = serialize_result(
        {
            "ticker": "CBRS",
            "analysis_date": "2026-11-13",
            "final_state": source,
            "results_dir": str(tmp_path),
            "report_path": str(report),
        }
    )
    assert web["portfolio_outlooks"] == rows


def test_malformed_horizon_does_not_destroy_valid_tactical_draft():
    from tests.test_portfolio_manager_integrity import actionable_draft
    from tradingagents.agents.schemas import PortfolioDecisionDraft

    payload = actionable_draft().model_dump()
    payload["outlooks"] = [{"months": 6, "rating": "not-a-rating"}]
    draft = PortfolioDecisionDraft.model_validate(payload)
    assert draft.rating.value == "Buy"
    assert draft.recommendation_confidence_score == 72
    assert draft.outlooks == []


def test_deck_outlook_sections_render_independent_confidence_targets_and_conditions():
    from tradingagents.investor_deck import InvestorDeck
    from tradingagents.investor_deck_html import render_investor_deck
    from tradingagents.investor_outlooks import build_outlook_slides
    from tradingagents.research_ledger import build_research_ledger

    source = state() | {
        "portfolio_outlooks": [
            r.model_dump(mode="json") for r in validate_outlooks([outlook(), outlook(12)], state())
        ]
    }
    ledger = build_research_ledger(source, "2026-11-13")
    slides = build_outlook_slides(source, ledger)
    assert [slide.topic for slide in slides] == ["outlook_6_month", "outlook_12_month"]
    html = render_investor_deck(
        InvestorDeck(
            ticker="CBRS",
            analysis_date="2026-11-13",
            headline="CBRS research",
            slides=slides,
            sources=dict(ledger.sources),
        ),
        "20261113T120000Z",
    )
    assert "2027-05-13" in html and "2027-11-13" in html
    assert "110" in html and "125" in html
    assert "65/100" in html and "25/100" in html
    assert "Model estimate" in html and "Upside" in html and "What changes the view" in html
    assert "2026-11-12" in html and "not company guidance" in html


def test_consolidated_reports_include_the_full_independent_outlooks():
    from cli.main import build_consolidated_report, build_consolidated_report_html

    source = state() | {
        "portfolio_outlooks": [
            r.model_dump(mode="json") for r in validate_outlooks([outlook(), outlook(12)], state())
        ]
    }
    result = {
        "ticker": "CBRS",
        "analysis_date": "2026-11-13",
        "decision": "Hold",
        "final_state": source,
        "results_dir": "/tmp",
        "report_path": "/tmp/report.md",
    }
    markdown = build_consolidated_report([result], "2026-11-13")
    html = build_consolidated_report_html([result], "2026-11-13")
    for text in (markdown, html):
        assert "6-month outlook" in text and "12-month outlook" in text
        assert "2027-05-13" in text and "2027-11-13" in text
        assert "65/100" in text and "25/100" in text


def test_outlook_artifact_cannot_be_overwritten_with_changed_forecasts(tmp_path):
    from tradingagents.reporting import write_report_tree

    rows = [r.model_dump(mode="json") for r in validate_outlooks([outlook(), outlook(12)], state())]
    source = state() | {"portfolio_outlooks": rows}
    write_report_tree(source, "CBRS", tmp_path)
    source["portfolio_outlooks"][0]["target"]["price_target"] = 999
    with pytest.raises(FileExistsError):
        write_report_tree(source, "CBRS", tmp_path)


def test_built_investor_deck_places_forward_views_before_tactical_decision():
    from tests.test_investor_deck_acceptance import (
        _peng_typed_portfolio_decision,
        peng_state_from_public_sec_fixtures_and_saved_vendor_rows,
    )
    from tradingagents.investor_deck import build_investor_deck

    source = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    source["shared_evidence"].extend(state()["shared_evidence"])
    source.update({k: v for k, v in state().items() if k != "shared_evidence"})
    for record in source["shared_evidence"]:
        record["as_of_date"] = "2026-11-13"
    source["portfolio_decision"] = _peng_typed_portfolio_decision()
    source["portfolio_outlooks"] = [
        r.model_dump(mode="json") for r in validate_outlooks([outlook(), outlook(12)], state())
    ]
    deck = build_investor_deck(source, "PENG", "2026-11-13", None)
    topics = [slide.topic for slide in deck.slides]
    assert topics[-3:] == ["outlook_6_month", "outlook_12_month", "decision"]


def test_outlook_report_discloses_daily_reference_excludes_extended_hours():
    from tradingagents.agents.utils.outlooks import render_outlooks

    rows = [r.model_dump(mode="json") for r in validate_outlooks([outlook(), outlook(12)], state())]
    assert "extended-hours prices are separate information" in render_outlooks(rows)


def test_real_snapshot_format_is_sufficient_without_magic_verified_close_sentence():
    source = state()
    source["market_report"] = "The daily session ended below the trend average."
    source["shared_evidence"][0]["content"] = (
        "## Verified market data snapshot for TEST\n- Latest trading row used: 2026-11-12\n"
        "### Latest verified OHLCV row\n| Field | Value |\n| Close | 100.00 |\n"
    )
    result = validate_outlooks([outlook(), outlook(12)], source)
    assert all(r.status == "available" for r in result)
    assert result[0].reference_price == 100


def test_source_point_outlook_requires_method_and_risks_too():
    result = validate_outlooks(
        [
            outlook(
                price_target=100, target_basis="Source level", target_method=None, target_risks=None
            )
        ],
        state(),
    )[0]
    assert result.status == "target_unavailable"
    assert result.reason == "target_method_missing"
    assert result.rating == "Buy" and result.recommendation_confidence_score == 65


def test_quote_currency_uses_resolved_instrument_context_and_is_never_assumed_usd():
    source = state()
    source["verified_market_snapshot"]["quote_currency"] = None
    source["instrument_context"] = "Resolved identity: Company: Test; Quote currency: CAD."
    result = validate_outlooks([outlook(), outlook(12)], source)
    assert result[0].quote_currency == "CAD"
    source["instrument_context"] = "Company name only."
    unknown = validate_outlooks([outlook(), outlook(12)], source)
    assert unknown[0].quote_currency is None
    assert "quote_currency_missing" in unknown[0].data_warnings


def test_model_prose_cannot_become_the_outlook_reference_close():
    source = state()
    source["market_report"] = "Verified close: 999."
    source.pop("verified_market_snapshot")
    source["shared_evidence"][0]["content"] = "- Latest trading row used: 2026-11-12\n| High | 100.00 |"
    result = validate_outlooks([outlook(rating="Sell", target_basis="Source level", price_target=100,
                                       supporting_quote="| High | 100.00 |")], source)[0]
    assert result.reference_price is None
    assert result.expected_return_pct is None
    assert "reference_price_missing" in result.data_warnings
