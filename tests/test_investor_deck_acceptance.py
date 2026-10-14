"""PENG fixture acceptance without a live model, market request, or reference deck."""

import re
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from parsel import Selector

from cli.main import estimate_target_profile
from tradingagents.agents.managers.portfolio_manager import create_portfolio_manager
from tradingagents.agents.schemas import (
    PortfolioDecision,
    PortfolioDecisionDraft,
    PortfolioTargetRepair,
    render_pm_decision,
)
from tradingagents.dataflows.sec_documents import extract_sections
from tradingagents.investor_deck import (
    DraftPoint,
    NarrativeBatch,
    NarrativeDraft,
    build_deck_outline,
    build_investor_deck,
)
from tradingagents.reporting import write_report_tree
from tradingagents.research_ledger import ResearchFact, ResearchLedger, build_research_ledger
from tradingagents.web.service import serialize_result

pytestmark = pytest.mark.unit
_FIXTURES = Path(__file__).parent / "fixtures" / "sec"


def peng_state_from_public_sec_fixtures_and_saved_vendor_rows():
    documents = [
        (
            "peng_q4_ex991.html", "EX-99.1", "earnings_release", "2026-10-06",
            "2026-08-28", "0001616533-26-000046",
            "https://www.sec.gov/Archives/edgar/data/1616533/"
            "000161653326000046/pengq4-26form8xkxex991.htm",
        ),
        (
            "peng_2025_10k.html", "10-K", "business", "2025-10-24",
            "2025-08-29", "0001616533-25-000061",
            "https://www.sec.gov/Archives/edgar/data/1616533/"
            "000161653325000061/peng-20250829.htm",
        ),
        (
            "peng_cfo_8k.html", "8-K", "governance", "2026-10-06",
            "2026-10-06", "0001193125-26-415645",
            "https://www.sec.gov/Archives/edgar/data/1616533/"
            "000119312526415645/d162527d8k.htm",
        ),
    ]
    records = []
    for filename, form, category, published, period, accession, url in documents:
        sections = extract_sections((_FIXTURES / filename).read_text(encoding="utf-8"), form)
        for index, section in enumerate(sections, 1):
            records.append({
                "evidence_id": f"ev-{category}-{index}", "source": "sec",
                "category": category, "title": f"{form} {section.name}",
                "content": section.content,
                "table_rows": [list(row) for row in section.table_rows],
                "section": section.name, "report_period": period,
                "form": "8-K" if form == "EX-99.1" else form,
                "document_type": form, "accession": accession,
                "url": url, "published_at": published,
                "as_of_date": "2026-10-09", "status": "available", "coverage": "partial",
            })
    records.extend([
        {
            "evidence_id": "ev-cash", "source": "get_cashflow", "category": "fundamentals",
            "title": "Quarterly cash flow",
            "content": ",2026-05-31\nOperating Cash Flow,-74788000\nFree Cash Flow,-77629000",
            "published_at": "2026-10-09", "as_of_date": "2026-10-09",
            "status": "available",
        },
        {
            "evidence_id": "ev-balance", "source": "get_balance_sheet",
            "category": "fundamentals", "title": "Quarterly balance",
            "content": ",2026-05-31\nCash And Cash Equivalents,440300000\nTotal Debt,502509000",
            "published_at": "2026-10-09", "as_of_date": "2026-10-09",
            "status": "available",
        },
        {
            "evidence_id": "ev-valuation", "source": "get_fundamentals",
            "category": "fundamentals", "title": "Valuation snapshot",
            "content": "Market Cap: 3745969408\nPE Ratio (TTM): 28.1\nForward PE: 13.9",
            "published_at": "2026-10-09", "as_of_date": "2026-10-09",
            "status": "available",
        },
    ])
    return {
        "trade_date": "2026-10-09", "shared_evidence": records,
        "final_trade_decision": "Hold while cash conversion is weak.",
        "investment_debate_state": {
            "bull_history": "AI deployments support growth.",
            "bear_history": "Working capital absorbs cash.",
        },
    }


def _peng_typed_portfolio_decision():
    return {
        "status": "Actionable", "rating": "Overweight",
        "executive_summary": "Hold existing exposure; stage new exposure only after confirmation.",
        "investment_thesis": "Reported growth supports the thesis, while cash conversion remains a risk.",
        "thesis": "Bullish",
        "existing_position_action": "Hold",
        "existing_position_summary": "Maintain a right-sized core; trim any oversized portion.",
        "new_position_action": "Conditional Buy",
        "new_position_summary": "Use a small staged entry after confirmation, not a full-size chase.",
        "conditional_plan": {
            "confirmation": "The breakout must hold above the verified session high.",
            "alternative": "Consider an orderly pullback toward a verified trend reference.",
            "invalidation": "Suspend purchases if cash flow stays negative and the trend breaks.",
        },
        "recommendation_confidence_score": 74,
        "price_target": 78.75,
        "time_horizon": "1-3 months",
        "confidence_score": 56,
        "target_summary": "A near-term retest of a verified high, not fundamental fair value.",
        "supporting_quote": "| High | 78.75 |",
        "target_validation_status": "Accepted",
    }


def test_validated_decision_is_last_slide_without_an_extra_model_topic():
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    state["portfolio_decision"] = _peng_typed_portfolio_decision()

    deck = build_investor_deck(state, "PENG", "2026-10-09", None)

    assert deck is not None and deck.synthesis_status == "source_only"
    assert deck.slides[-1].topic == "decision"
    assert [slide.topic for slide in deck.slides[-3:-1]] == ["bull_case", "bear_case"]
    block = next(item for item in deck.slides[-1].blocks if item.kind == "decision")
    assert (block.rating, block.existing_action, block.new_action) == (
        "Overweight", "Hold", "Conditional Buy",
    )
    assert (block.recommendation_confidence, block.target,
            block.target_confidence, block.target_horizon) == (
        74, 78.75, 56, "1-3 months",
    )
    assert "analysis:portfolio" in block.source_ids


def test_final_recommendation_keeps_complete_portfolio_manager_summary():
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    decision = _peng_typed_portfolio_decision()
    summary = (
        "Overweight on a three-to-six-month view because reported sales and operating "
        "income accelerated. Wait for a staged entry rather than chasing the current "
        "extension; the earnings improvement must also translate into cash conversion."
    )
    decision["executive_summary"] = summary
    state["portfolio_decision"] = decision

    deck = build_investor_deck(state, "PENG", "2026-10-09", None)

    assert deck is not None
    block = next(item for item in deck.slides[-1].blocks if item.kind == "decision")
    assert block.recommendation_summary == summary


def test_source_only_peng_range_reaches_final_slide_without_cut_text(tmp_path):
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    state["shared_evidence"].append({
        "evidence_id": "ev-market", "source": "get_verified_market_snapshot",
        "category": "market", "title": "Verified market snapshot",
        "content": "Verified close: 75.74.\n| Low | 72.83 |\n| High | 77.10 |",
        "published_at": "2026-10-09", "as_of_date": "2026-10-09",
        "status": "available",
    })
    decision = _peng_typed_portfolio_decision()
    decision.update({
        "rating": "Hold", "price_target": None, "supporting_quote": None,
        "target_low": 72.83, "target_high": 77.10,
        "supporting_quote_low": "| Low | 72.83 |",
        "supporting_quote_high": "| High | 77.10 |",
        "time_horizon": "3 months", "confidence_score": 18,
        "target_summary": (
            "If demand persists, verified support and resistance frame a "
            "three-month technical recovery scenario; these observations are not fair value."
        ),
        "target_method": "Technical scenario",
        "target_risks": (
            "A failed breakout or weaker demand would invalidate this range "
            "before the three-month horizon."
        ),
    })
    state["portfolio_decision"] = decision
    state["final_trade_decision"] = render_pm_decision(
        PortfolioDecision.model_validate(decision),
    )
    state["risk_debate_state"] = {"judge_decision": state["final_trade_decision"]}

    deck = build_investor_deck(state, "PENG", "2026-10-09", None)

    assert deck is not None and deck.synthesis_status == "source_only"
    block = next(item for item in deck.slides[-1].blocks if item.kind == "decision")
    assert block.target is None
    assert (block.target_low, block.target_high) == (Decimal("72.83"), Decimal("77.10"))
    write_report_tree(
        state, "PENG", tmp_path, investor_deck=deck,
        report_timestamp="20261009T154737Z",
    )
    html = (tmp_path / "PENG_investor_20261009T154737Z.html").read_text()
    full_md = (tmp_path / "PENG_full_20261009T154737Z.md").read_text()
    assert "72.83–77.10" in html
    assert "Technical scenario" in html
    assert "3 months" in html and "18/100" in html
    assert decision["target_summary"] in html
    assert decision["target_risks"] in html
    assert "| Low | 72.83 |" in html and "| High | 77.10 |" in html
    assert "No validated price target" not in html
    assert "Interpretation withheld" not in html
    assert "| Low | 72.83 |" in full_md and "| High | 77.10 |" in full_md


def test_offline_peng_repair_range_flows_through_deck_forecast_and_web(tmp_path):
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    state.update({
        "company_of_interest": "PENG",
        "asset_type": "stock",
        "market_report": "Verified close: 75.74.",
        "investment_plan": "Hold pending demand confirmation.",
        "trader_investment_plan": "Wait for the breakout.",
        "verified_market_snapshot": {
            "symbol": "PENG", "requested_date": "2026-10-09",
            "observed_on": "2026-10-09", "observed_at": None,
            "close": "75.74", "quote_currency": "USD",
            "adjustment_basis": "split_adjusted", "vendor": "yfinance",
        },
        "risk_debate_state": {
            "history": "Demand supports upside, but cash conversion remains a risk.",
            "aggressive_history": "", "conservative_history": "",
            "neutral_history": "", "current_aggressive_response": "",
            "current_conservative_response": "", "current_neutral_response": "",
            "latest_speaker": "Neutral", "count": 1,
        },
    })
    state["shared_evidence"].append({
        "evidence_id": "ev-market", "source": "get_verified_market_snapshot",
        "category": "market", "title": "Verified market snapshot",
        "content": "Verified close: 75.74.\n| Low | 72.83 |\n| Close | 75.74 |\n| High | 77.10 |",
        "published_at": "2026-10-09", "as_of_date": "2026-10-09",
        "status": "available",
    })
    first = MagicMock()
    first.invoke.return_value = PortfolioDecisionDraft(
        rating="Hold", executive_summary="Maintain exposure pending confirmation.",
        investment_thesis="Reported growth is offset by cash conversion risk.",
        thesis="Neutral", existing_position_action="Hold",
        existing_position_summary="Keep a measured position.",
        new_position_action="Avoid", new_position_summary="Wait for better risk/reward.",
        recommendation_confidence_score=78,
    )
    repair = MagicMock()
    repair.invoke.return_value = PortfolioTargetRepair(
        target_low=72.83, target_high=77.10,
        supporting_quote_low="| Low | 72.83 |",
        supporting_quote_high="| High | 77.10 |",
        time_horizon="3 months", confidence_score=18,
        target_summary="Persistent demand could support a three-month technical recovery.",
        target_method="Technical scenario",
        target_risks="A failed breakout or weak cash conversion would invalidate the range.",
    )
    llm = MagicMock()
    llm.with_structured_output.side_effect = lambda schema: (
        first if schema is PortfolioDecisionDraft else repair
    )

    state.update(create_portfolio_manager(llm)(state))
    deck = build_investor_deck(state, "PENG", "2026-10-09", None)
    assert deck is not None
    write_report_tree(
        state, "PENG", tmp_path, investor_deck=deck,
        run_metadata={"run_id": "offline-peng"},
        report_timestamp="20261009T154737Z",
    )
    profile = estimate_target_profile(None, "PENG", "2026-10-09", state, "Hold")
    web = serialize_result({
        "ticker": "PENG", "analysis_date": "2026-10-09", "decision": "Hold",
        "final_state": state, "results_dir": str(tmp_path),
        "report_path": str(tmp_path / "PENG_full_20261009T154737Z.md"),
        **profile,
    })
    html = (tmp_path / "PENG_investor_20261009T154737Z.html").read_text()
    forecast = (tmp_path / "forecast_record.json").read_text()

    assert first.invoke.call_count == 1 and repair.invoke.call_count == 1
    assert state["portfolio_decision"]["target_validation_status"] == "Accepted"
    assert "72.83–77.10" in html and "72.83" in web["price_target_label"]
    assert (web["target_low"], web["target_high"]) == (72.83, 77.10)
    assert '"target_low": "72.83"' in forecast
    assert '"target_high": "77.1"' in forecast
    assert '"central_target": null' in forecast


def test_rejected_target_never_becomes_an_investor_price_target():
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    decision = _peng_typed_portfolio_decision()
    for field in ("price_target", "time_horizon", "confidence_score",
                  "target_summary", "supporting_quote"):
        decision[field] = None
    decision["target_validation_status"] = "Rejected"
    decision["target_validation_reason"] = "supporting_quote_not_in_evidence"
    state["portfolio_decision"] = decision

    deck = build_investor_deck(state, "PENG", "2026-10-09", None)

    assert deck is not None and deck.slides[-1].topic == "decision"
    block = next(item for item in deck.slides[-1].blocks if item.kind == "decision")
    assert block.target is None and block.target_confidence is None
    assert block.recommendation_confidence == 74


def test_nonactionable_decision_has_no_investment_action_or_confidence():
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    state["portfolio_decision"] = {
        "status": "Unavailable", "rating": None,
        "executive_summary": "Final decision unavailable.",
        "investment_thesis": "The manager did not produce a validated decision.",
        "status_reason": "upstream_trader_unavailable",
    }

    deck = build_investor_deck(state, "PENG", "2026-10-09", None)

    assert deck is not None and deck.slides[-1].topic == "decision"
    block = next(item for item in deck.slides[-1].blocks if item.kind == "decision")
    assert block.rating is None
    assert block.existing_action is None and block.new_action is None
    assert block.recommendation_confidence is None and block.target is None


def test_decision_guidance_keeps_complete_reconsideration_and_rationale_sentences():
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    decision = _peng_typed_portfolio_decision()
    decision["new_position_summary"] = (
        "Avoid initiating a new PENG position at the latest close because the "
        "stock is above its upper Bollinger Band with elevated volatility, while "
        "cash conversion and next-year execution remain unconfirmed. "
        "Reconsider only after operating cash flow improves and margins hold. "
        "Any new position should still respect the investor's own risk budget "
        "rather than assume the team knows the size of an existing portfolio. "
        "This is a conditional action plan, not an instruction to trade immediately."
    )
    decision["target_summary"] = (
        "The target is a plausible near-term downside level because it is the "
        "latest verified session low and was identified by the analysts as a "
        "risk marker, not as guaranteed support, during a volatile session "
        "that included a wide intraday range and rapid repricing. "
        "It is a downside reference rather than a forecast of intrinsic value. "
        "A later filing may change the evidence and require this level to be reassessed. "
        "The reference level must be interpreted alongside the stated horizon and risk controls."
    )
    state["portfolio_decision"] = decision

    deck = build_investor_deck(state, "PENG", "2026-10-09", None)

    assert deck is not None
    block = next(item for item in deck.slides[-1].blocks if item.kind == "decision")
    assert block.new_guidance is not None
    assert block.new_guidance == decision["new_position_summary"]
    assert block.target_rationale is not None
    assert block.target_rationale == decision["target_summary"]
    assert "…" not in block.new_guidance + block.target_rationale


def complete_topic_fake_llm(state):
    ledger = build_research_ledger(state, "2026-10-09")
    outline = build_deck_outline(ledger, state)
    batches = []
    for offset in range(0, len(outline), 4):
        drafts = []
        for slide in outline[offset:offset + 4]:
            needle = {
                "products": "OriginAI", "leadership": "Stephen Cumming",
                "relationships": "Lektra",
            }.get(slide.topic)
            candidates = [
                item for items in ledger.excerpts.values() for item in items
                if item.evidence_id in slide.source_ids
            ]
            chosen = next(
                (item for item in candidates if needle and needle in item.text),
                candidates[0] if candidates else None,
            )
            cited = chosen.evidence_id if chosen else slide.source_ids[0]
            excerpt = chosen.text if chosen else ""
            sentence = next(
                (text.strip() for text in re.split(r"(?<=[.!?])\s+", excerpt)
                 if needle and needle in text),
                "",
            )
            points = [DraftPoint(text=sentence, source_id=cited, quote=sentence)] if sentence else []
            drafts.append(NarrativeDraft(
                topic=slide.topic, takeaway="Evidence informs the view.",
                context="The cited material describes the trade-off.",
                source_ids=[cited], points=points,
            ))
        batches.append(NarrativeBatch(slides=drafts))
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.side_effect = batches
    return llm


def test_peng_october_9_deck_is_company_research_not_four_trading_cards(tmp_path):
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    llm = complete_topic_fake_llm(state)
    deck = build_investor_deck(state, "PENG", "2026-10-09", llm)

    assert deck is not None
    topics = {slide.topic for slide in deck.slides}
    assert len(topics) >= 12
    assert {
        "business", "products", "segments", "relationships", "revenue",
        "earnings", "cash", "capital", "guidance", "bull_case", "bear_case",
    } <= topics
    guidance = next(slide for slide in deck.slides if slide.topic == "guidance")
    guidance_table = next(block for block in guidance.blocks if block.kind == "table")
    assert guidance.takeaway.startswith("FY2027 guidance:")
    assert "FY2027" in guidance_table.headings[1]
    assert guidance_table.rows[0].cells[1] == "40% YoY Growth +/-10%"
    assert guidance_table.rows[-1].cells[-1] == "$4.45 +/- $0.70"
    write_report_tree(
        state, "PENG", tmp_path, report_timestamp="20261009T154737Z",
        investor_deck=deck,
    )
    html = (tmp_path / "PENG_investor_20261009T154737Z.html").read_text()
    assert "$566.7 million" in html and "$478.7 million" in html
    assert "$3.75 billion" in html
    assert "Debt exceeded cash by $62.2 million" in html
    assert "earlier than the latest company-reported earnings period" in html
    assert "53.4%" in html and "32.3%" in html and "14.4%" in html
    assert "OriginAI" in html and "Stephen Cumming" in html and "Lektra" in html
    assert "pengq4-26form8xkxex991.htm" in html
    assert "60%+" not in html and "&lt;10%" not in html
    assert "68/100" not in html and "$150B" not in html
    assert sum(slide.topic == "technical" for slide in deck.slides) <= 1
    assert llm.with_structured_output.return_value.invoke.call_count <= 4
    for slide in deck.slides:
        specifics = sum(
            len(block.metrics) if block.kind == "metrics" else
            len(block.rows) if block.kind == "table" else
            len(block.points) if block.kind in {"text", "trend"} else
            len(block.events)
            for block in slide.blocks
        )
        assert specifics >= 2, f"{slide.topic} is a title-and-takeaway filler slide"
    rendered = Selector(text=html)
    marker_targets = set(rendered.css("a.source-ref::attr(href)").getall())
    source_ids = {f"#{source_id}" for source_id in rendered.css(".sources li::attr(id)").getall()}
    assert marker_targets and marker_targets <= source_ids
    assert "@media(max-width:650px)" in html
    assert "@media print" in html


def test_peng_rich_sources_still_render_when_optional_model_draft_is_invalid(tmp_path):
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = NarrativeBatch(slides=[])

    deck = build_investor_deck(state, "PENG", "2026-10-09", llm)

    assert deck is not None and deck.synthesis_status == "source_only"
    assert len(deck.slides) >= 12
    assert all(slide.takeaway != slide.title for slide in deck.slides)
    assert "18.4%" in next(slide.takeaway for slide in deck.slides
                           if slide.topic == "revenue")
    assert "Integrated Memory" in next(slide.takeaway for slide in deck.slides
                                       if slide.topic == "segments")
    write_report_tree(
        state, "PENG", tmp_path, report_timestamp="20261009T154737Z",
        investor_deck=deck,
    )
    html = (tmp_path / "PENG_investor_20261009T154737Z.html").read_text()
    assert "Interpretation withheld" not in html
    assert deck.synthesis_reason == "required_topics_missing"
    assert "Labeled team views are analyst opinions" not in html
    assert "$566.7 million" in html
    assert "53.4%" in html
    guidance = next(slide for slide in deck.slides if slide.topic == "guidance")
    guidance_points = [point.text for block in guidance.blocks if block.kind == "text"
                       for point in block.points]
    assert not any("Outlook GAAP Outlook Adjustments" in point for point in guidance_points)
    assert all(len(point) < 240 for point in guidance_points)
    business = next(slide for slide in deck.slides if slide.topic == "business")
    business_points = [point.text for block in business.blocks if block.kind == "text"
                       for point in block.points]
    assert all("Financial Statements and Supplementary Data" not in point
               for point in business_points)
    assert "Research deck unavailable" not in html


def test_peng_source_only_deck_has_distinct_evidence_led_bull_and_bear_cases(tmp_path):
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = NarrativeBatch(slides=[])

    deck = build_investor_deck(state, "PENG", "2026-10-09", llm)

    assert deck is not None and deck.synthesis_status == "source_only"
    cases = {slide.topic: slide for slide in deck.slides
             if slide.topic in {"bull_case", "bear_case", "bull_bear"}}
    assert set(cases) == {"bull_case", "bear_case"}
    assert cases["bull_case"].title == "Bull case"
    assert cases["bear_case"].title == "Bear case"
    for side, slide in cases.items():
        points = [point for block in slide.blocks if block.kind == "text"
                  for point in block.points]
        factual = [point for point in points if not point.text.startswith("Watch:") and any(
            deck.sources[key].provider != "analysis" for key in point.source_ids
        )]
        team = [point for point in points if any(
            deck.sources[key].provider == "analysis" for key in point.source_ids
        )]
        assert len(factual) >= 3, f"{side} has too few independent facts"
        assert team and any("Team interpretation:" in point.text for point in team)
        assert any("Watch:" in point.text for point in points)
        assert all(point.source_ids and all(key in deck.sources for key in point.source_ids)
                   for point in points)
        assert "Analyst:" not in slide.takeaway
        assert "##" not in slide.takeaway
    write_report_tree(
        state, "PENG", tmp_path, report_timestamp="20261009T154737Z",
        investor_deck=deck,
    )
    html = (tmp_path / "PENG_investor_20261009T154737Z.html").read_text()
    rendered = Selector(text=html)
    assert len(rendered.css('section.slide[data-topic="bull_case"]')) == 1
    assert len(rendered.css('section.slide[data-topic="bear_case"]')) == 1
    assert "Bull Analyst:" not in html and "Bear Analyst:" not in html


def test_bear_case_omits_undated_balance_comparison_and_unobserved_cash_metric():
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    original = build_research_ledger(state, "2026-10-09")
    undated = ResearchLedger(
        facts=tuple(
            replace(fact, period_end=None)
            if fact.metric in {"cash", "total_debt"} else fact
            for fact in original.facts
        ),
        sources=original.sources, excerpts=original.excerpts,
    )
    undated_bear = next(slide for slide in build_deck_outline(undated, state)
                        if slide.topic == "bear_case")
    assert not any("At None" in point.text for block in undated_bear.blocks
                   if block.kind == "text" for point in block.points)

    missing_fcf = ResearchLedger(
        facts=tuple(fact for fact in original.facts if fact.metric != "free_cash_flow"),
        sources=original.sources, excerpts=original.excerpts,
    )
    bear = next(slide for slide in build_deck_outline(missing_fcf, state)
                if slide.topic == "bear_case")
    points = [point.text for block in bear.blocks if block.kind == "text"
              for point in block.points]

    assert not any("Free cash flow was" in point for point in points)
    assert any(point.startswith("Watch:") and "free cash flow" not in point
               for point in points)


def test_bull_case_prefers_company_reported_quarter_over_conflicting_vendor_value():
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    original = build_research_ledger(state, "2026-10-09")
    conflicted = ResearchLedger(
        facts=(*original.facts, ResearchFact(
            "net_sales", Decimal("600000000"), "USD", "Q4-26", "2026-08-28",
            "quarterly", "gaap", ("ev-valuation",),
        )),
        sources=original.sources, excerpts=original.excerpts,
    )

    bull = next(slide for slide in build_deck_outline(conflicted, state)
                if slide.topic == "bull_case")
    points = [point.text for block in bull.blocks if block.kind == "text"
              for point in block.points]

    assert any("Reported net sales" in point and "$566.7 million" in point
               for point in points)
    assert not any("$600.0 million" in point for point in points)


def test_source_backed_cases_survive_when_team_excerpts_are_missing():
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    state["investment_debate_state"] = {}

    outline = build_deck_outline(build_research_ledger(state, "2026-10-09"), state)

    assert {"bull_case", "bear_case"} <= {slide.topic for slide in outline}
    for slide in outline:
        if slide.topic in {"bull_case", "bear_case"}:
            assert not any("Team interpretation:" in point.text
                           for block in slide.blocks if block.kind == "text"
                           for point in block.points)


def test_october_5_cutoff_excludes_q4_and_cfo_developments():
    state = peng_state_from_public_sec_fixtures_and_saved_vendor_rows()
    state["trade_date"] = "2026-10-05"
    for record in state["shared_evidence"]:
        record["as_of_date"] = "2026-10-05"

    ledger = build_research_ledger(state, "2026-10-05")
    outline = build_deck_outline(ledger, state)

    assert all(fact.value != 566_685_000 for fact in ledger.facts)
    assert all("Stephen Cumming" not in item.text for items in ledger.excerpts.values()
               for item in items)
    assert "leadership" not in {slide.topic for slide in outline}


def test_non_us_thin_source_omits_empty_company_sections():
    state = {"trade_date": "2026-10-09", "shared_evidence": [{
        "evidence_id": "ev-market", "source": "get_stock_data", "category": "market",
        "title": "Market history", "content": "Prices available",
        "published_at": "2026-10-09", "as_of_date": "2026-10-09",
        "status": "available",
    }]}

    assert build_deck_outline(build_research_ledger(state, "2026-10-09"), state) == []
