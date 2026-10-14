"""Source evidence survives handoffs and is admitted consistently to prompts/validation."""

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from tradingagents.agents.managers.research_manager import create_research_manager
from tradingagents.agents.schemas import PortfolioRating, ResearchPlan, TraderAction, TraderProposal
from tradingagents.agents.trader.trader import create_trader
from tradingagents.agents.utils.agent_utils import create_msg_delete
from tradingagents.agents.utils.decision_integrity import build_decision_evidence


def source_record(**overrides):
    record = {
        "source": "sec", "category": "fundamentals", "title": "Reported revenue",
        "content": "Revenue was USD 123456789 for the quarter ended 2025-03-31.",
        "url": "https://www.sec.gov/Archives/example", "published_at": "2025-05-01",
        "as_of_date": "2025-05-02", "status": "available",
    }
    record.update(overrides)
    return record


def state_with_evidence(**overrides):
    state = {
        "company_of_interest": "NVDA", "trade_date": "2025-05-02", "asset_type": "stock",
        "shared_evidence": [source_record()], "messages": [],
        "market_report": "ORIGINAL_MARKET_REPORT", "news_report": "ORIGINAL_NEWS_REPORT",
        "sentiment_report": "ORIGINAL_SENTIMENT_REPORT",
        "fundamentals_report": "ORIGINAL_FUNDAMENTALS_REPORT",
        "investment_plan": "Buy recommendation", "trader_investment_plan": "Buy proposal",
        "investment_debate_state": {"history": "Debate summary", "count": 1},
    }
    state.update(overrides)
    return state


@pytest.mark.parametrize("role", ["research", "trader"])
def test_managers_and_trader_receive_original_evidence_and_reports(role):
    captured = []
    result = (ResearchPlan(recommendation=PortfolioRating.BUY, rationale="x", strategic_actions="y")
              if role == "research" else TraderProposal(action=TraderAction.BUY, reasoning="x"))
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.side_effect = (
        lambda prompt: captured.append(prompt) or result
    )
    factory = create_research_manager if role == "research" else create_trader
    factory(llm)(state_with_evidence())
    prompt = str(captured[0])
    assert "123456789" in prompt
    assert "https://www.sec.gov/Archives/example" in prompt
    assert "ORIGINAL_MARKET_REPORT" in prompt
    assert "ORIGINAL_FUNDAMENTALS_REPORT" in prompt


def test_message_clearing_preserves_original_tool_output_not_model_claims():
    messages = [
        HumanMessage(content="NVDA", id="human"),
        AIMessage(content="invented model claim", id="ai", tool_calls=[
            {"name": "get_news", "args": {"ticker": "NVDA"}, "id": "call1"},
        ]),
        ToolMessage(content="ORIGINAL_SOURCE_NEWS", name="get_news", tool_call_id="call1", id="tool"),
    ]
    update = create_msg_delete()(state_with_evidence(messages=messages))
    assert any("ORIGINAL_SOURCE_NEWS" in row["content"] for row in update.get("shared_evidence", []))
    assert not any("invented model claim" in row["content"] for row in update["shared_evidence"])
    assert len(update["messages"]) == 4


def test_failed_tool_message_is_not_admissible_as_source_evidence():
    from tradingagents.agents.utils.evidence import tool_message_evidence

    state = state_with_evidence(messages=[
        ToolMessage(content="rate limited", name="get_news", tool_call_id="call1", status="error"),
    ])

    records = tool_message_evidence(state)

    assert records[0]["status"] == "unavailable"
    assert "rate limited" in records[0]["content"]


def test_tool_unavailable_sentinel_is_not_marked_available_on_success_status():
    from tradingagents.agents.utils.evidence import normalize_evidence, tool_message_evidence

    state = state_with_evidence(messages=[
        ToolMessage(
            content="DATA_UNAVAILABLE: vendor rate limited",
            name="get_news", tool_call_id="call1",
        ),
    ])

    record = tool_message_evidence(state)[0]
    normalized = normalize_evidence(source_record(
        status="available", content="NO_DATA_AVAILABLE: no usable prices"
    ), "2025-05-02")

    assert record["status"] == "unavailable"
    assert normalized["status"] == "unavailable"


def test_metadata_only_coverage_survives_normalization_and_is_visible_in_context():
    from tradingagents.agents.utils.evidence import normalize_evidence, render_shared_evidence

    record = normalize_evidence(source_record(coverage="metadata_only"), "2025-05-02")
    context = render_shared_evidence(state_with_evidence(shared_evidence=[record]), "research")

    assert record["coverage"] == "metadata_only"
    assert "Coverage: metadata only" in context


def test_final_target_corpus_includes_source_content_but_not_unavailable_claims():
    state = state_with_evidence(shared_evidence=[
        source_record(content="Analyst price target is $150."),
        source_record(status="unavailable", content="Unverified price target $999."),
        source_record(published_at="2025-05-03", content="Future price target $888."),
    ])
    evidence = build_decision_evidence(state)
    assert "Analyst price target is $150." in evidence
    assert "$999" not in evidence
    assert "$888" not in evidence


def test_normalization_deduplicates_and_marks_truncation():
    from tradingagents.agents.utils.evidence import merge_evidence

    row = source_record(content="Revenue " + "x" * 50000)
    first = merge_evidence([], [row], "2025-05-02")
    second = merge_evidence(first, [row], "2025-05-02")
    assert len(second) == 1
    assert second[0]["evidence_id"] == first[0]["evidence_id"]
    assert second[0]["truncated"] is True
    assert len(second[0]["content"]) <= 24000
    assert second[0]["retrieved_at"]


def test_rendered_context_has_budget_and_explicit_omissions():
    from tradingagents.agents.utils.evidence import render_shared_evidence

    state = state_with_evidence(shared_evidence=[
        source_record(title=f"Document {i}", content="facts " * 3000) for i in range(20)
    ])
    context = render_shared_evidence(state, role="trader", max_chars=4000)
    assert len(context) <= 4000
    assert "omitted" in context.lower() or "excerpt" in context.lower()
    assert "2025-05-01" in context


def test_shared_context_keeps_verified_snapshot_and_source_diversity_under_budget():
    from tradingagents.agents.utils.evidence import render_shared_evidence

    state = state_with_evidence(shared_evidence=[
        source_record(
            source="get_stock_data", category="market", title="Raw history",
            content="RAW_HISTORY " * 3000,
        ),
        source_record(
            source="sec", category="fundamentals", title="SEC fact",
            content="SEC_CASH_FLOW_FACT",
        ),
        source_record(
            source="get_verified_market_snapshot", category="market",
            title="Verified market snapshot", content="VERIFIED_CLOSE_123",
        ),
        source_record(
            source="news", category="news", title="Company news",
            content="NEWS_CATALYST_FACT",
        ),
    ])

    context = render_shared_evidence(state, role="portfolio_manager", max_chars=2500)

    assert len(context) <= 2500
    assert "VERIFIED_CLOSE_123" in context
    assert "SEC_CASH_FLOW_FACT" in context
    assert "NEWS_CATALYST_FACT" in context
    assert "omitted" in context.lower() or "excerpt" in context.lower()


def test_shared_context_reserves_room_across_categories_before_repeating_one():
    from tradingagents.agents.utils.evidence import render_shared_evidence

    state = state_with_evidence(shared_evidence=[
        source_record(source="get_verified_market_snapshot", category="market",
                      title="Verified snapshot", content="CANONICAL_CLOSE"),
        *(source_record(category="fundamentals", title=f"SEC fact {i}",
                        content=f"FACT_{i} " + "detail " * 150)
          for i in range(6)),
        source_record(category="news", title="Company announcement",
                      content="NEWS_EVENT"),
    ])

    context = render_shared_evidence(state, role="portfolio_manager", max_chars=1800)

    assert "CANONICAL_CLOSE" in context
    assert "FACT_0" in context
    assert "NEWS_EVENT" in context


def test_future_record_does_not_claim_a_source_category_slot():
    from tradingagents.agents.utils.evidence import render_shared_evidence

    state = state_with_evidence(shared_evidence=[
        *(source_record(category="fundamentals", title=f"SEC fact {i}",
                        content="detail " * 150)
          for i in range(6)),
        source_record(category="news", title="Future news", published_at="2025-05-03",
                      content="FUTURE_EVENT"),
        source_record(category="news", title="Current news", content="CURRENT_EVENT"),
    ])

    context = render_shared_evidence(state, role="portfolio_manager", max_chars=1800)

    assert "CURRENT_EVENT" in context
    assert "FUTURE_EVENT" not in context


def test_report_context_balances_all_analysts_instead_of_only_first():
    from tradingagents.agents.utils.evidence import render_original_reports

    state = state_with_evidence(**{
        f"{role}_report": f"{role.upper()}_SUMMARY " + "detail " * 2000
        + f" {role.upper()}_CONCLUSION"
        for role in ("market", "sentiment", "news", "fundamentals")
    })

    context = render_original_reports(state, max_chars=4000)

    assert len(context) <= 4000
    for role in ("market", "sentiment", "news", "fundamentals"):
        assert f"### {role}_report" in context
        assert f"{role.upper()}_SUMMARY" in context
        assert f"{role.upper()}_CONCLUSION" in context


def test_collector_retains_available_source_when_another_provider_fails(monkeypatch):
    from tradingagents.agents.utils import evidence

    def route(method, ticker, curr_date):
        if method == "get_company_filings":
            return [source_record()]
        raise RuntimeError("https://vendor.invalid?apikey=SECRET")

    monkeypatch.setattr(evidence, "route_to_vendor", route)
    monkeypatch.setattr(evidence, "get_config", lambda: {"enrichment_enabled": True})
    result = evidence.collect_shared_evidence(state_with_evidence(shared_evidence=[]))
    assert any(row["status"] == "available" for row in result["shared_evidence"])
    assert any(row["status"] == "unavailable" for row in result["shared_evidence"])
    assert "SECRET" not in str(result)
    assert result["evidence_collected"] is True


def test_issuer_without_sec_coverage_keeps_other_source_evidence(monkeypatch):
    from tradingagents.agents.utils import evidence

    def route(method, ticker, curr_date):
        if method == "get_company_filings":
            return [source_record(
                source="sec", category="filings", status="unavailable",
                content="No eligible SEC filing for this issuer.",
                published_at=None, as_of_date=curr_date,
            )]
        return []

    monkeypatch.setattr(evidence, "route_to_vendor", route)
    monkeypatch.setattr(evidence, "get_config", lambda: {"enrichment_enabled": True})
    market = source_record(
        source="get_stock_data", category="market", title="Market history",
        content="Valid historical prices", published_at="2026-10-09",
        as_of_date="2026-10-09",
    )
    result = evidence.collect_shared_evidence(state_with_evidence(
        company_of_interest="NONUS", trade_date="2026-10-09",
        shared_evidence=[market],
    ))

    assert result["evidence_collected"] is True
    assert any(row["source"] == "sec" and row["status"] == "unavailable"
               for row in result["shared_evidence"])
    assert any(row["source"] == "get_stock_data" and row["status"] == "available"
               and row["content"] == "Valid historical prices"
               for row in result["shared_evidence"])


def test_enrichment_methods_are_registered_as_optional_explicit_vendors(monkeypatch):
    from tradingagents.dataflows import interface

    assert interface.TOOLS_CATEGORIES["regulatory_filings"]["tools"] == [
        "get_company_filings"
    ]
    assert interface.TOOLS_CATEGORIES["earnings_data"]["tools"] == [
        "get_earnings_context"
    ]
    assert set(interface.VENDOR_METHODS["get_company_filings"]) == {"sec"}
    assert set(interface.VENDOR_METHODS["get_earnings_context"]) == {"fmp"}
    assert {"regulatory_filings", "earnings_data"} <= interface.OPTIONAL_CATEGORIES


@pytest.mark.parametrize("override", [{"evidence_collected": True}, {"asset_type": "crypto"}])
def test_collector_does_not_refetch_or_fetch_company_sources_for_crypto(monkeypatch, override):
    from tradingagents.agents.utils import evidence

    def no_network(*args, **kwargs):
        pytest.fail("provider must not be called")

    monkeypatch.setattr(evidence, "route_to_vendor", no_network)
    evidence.collect_shared_evidence(state_with_evidence(**override))


def test_sec_section_metadata_and_table_rows_survive_normalization():
    from tradingagents.agents.utils.evidence import normalize_evidence

    record = source_record(
        category="earnings_release", title="SEC EX-99.1 Table 1",
        content="(in thousands) Q4-26 and Q3-26 net sales",
        published_at="2026-10-06", as_of_date="2026-10-09",
        form="8-K", accession="0001616533-26-000046",
        document_type="EX-99.1", section="Table 1",
        report_period="2026-08-28",
        table_rows=[["Total net sales", "566,685", "478,713"]],
    )

    normalized = normalize_evidence(record, "2026-10-09")

    assert normalized["form"] == "8-K"
    assert normalized["document_type"] == "EX-99.1"
    assert normalized["section"] == "Table 1"
    assert normalized["report_period"] == "2026-08-28"
    assert normalized["table_rows"] == [["Total net sales", "566,685", "478,713"]]


def test_nested_sec_table_metadata_is_bounded_and_redacted():
    from tradingagents.agents.utils.evidence import normalize_evidence

    record = source_record(
        table_rows=[["apikey=SECRET" + "x" * 500] * 25 for _ in range(130)]
    )

    rows = normalize_evidence(record, "2025-05-02")["table_rows"]

    assert len(rows) == 120
    assert all(len(row) == 20 for row in rows)
    assert max(len(cell) for row in rows for cell in row) <= 300
    assert "SECRET" not in str(rows)


def test_two_sections_in_one_filing_keep_distinct_evidence_ids():
    from tradingagents.agents.utils.evidence import normalize_evidence

    base = source_record(
        category="earnings_release", content="The same filing contains two tables.",
        published_at="2026-10-06", as_of_date="2026-10-09",
    )
    first = normalize_evidence(
        base | {"title": "SEC EX-99.1 Table 1", "section": "Table 1"}, "2026-10-09"
    )
    second = normalize_evidence(
        base | {"title": "SEC EX-99.1 Table 2", "section": "Table 2"}, "2026-10-09"
    )

    assert first["evidence_id"] != second["evidence_id"]


def test_q4_release_and_business_excerpts_beat_indicator_flood_in_prompt():
    from tradingagents.agents.utils.evidence import render_shared_evidence

    records = [
        source_record(
            source="get_verified_market_snapshot", category="market",
            title="Verified snapshot", content="SNAPSHOT " + "bar " * 220,
            published_at="2026-10-09", as_of_date="2026-10-09",
        ),
        *[
            source_record(
                category="fundamentals", title=f"Older fact {index}",
                content=f"OLD_{index} " + "detail " * 150,
                published_at="2026-07-07", as_of_date="2026-10-09",
            )
            for index in range(39)
        ],
        source_record(
            category="earnings_release", title="Q4 earnings",
            content="Q4 FY2026 net sales 566,685", published_at="2026-10-06",
            as_of_date="2026-10-09", section="Quarterly highlights",
            report_period="2026-08-28",
        ),
        source_record(
            category="business", title="Annual business",
            content="OriginAI and Integrated Memory", published_at="2025-10-24",
            as_of_date="2026-10-09", section="Business",
            report_period="2025-08-29",
        ),
    ]
    state = state_with_evidence(trade_date="2026-10-09", shared_evidence=records)

    prompt = render_shared_evidence(state, "fundamentals", max_chars=1500)

    assert "566,685" in prompt
    assert "OriginAI" in prompt
    assert "Section: Quarterly highlights" in prompt
    assert "Report period: 2026-08-28" in prompt


def test_sec_table_values_are_visible_to_analysts_when_not_in_excerpt_text():
    from tradingagents.agents.utils.evidence import render_shared_evidence

    row = source_record(
        category="earnings_release", title="SEC EX-99.1 Table 1",
        content="(in thousands) Quarterly Financial Results",
        published_at="2026-10-06", as_of_date="2026-10-09",
        section="Table 1", report_period="2026-08-28",
        table_rows=[
            ["Metric", "Q4-26", "Q3-26"],
            ["Total net sales", "566,685", "478,713"],
        ],
    )

    context = render_shared_evidence(
        state_with_evidence(trade_date="2026-10-09", shared_evidence=[row]),
        role="fundamentals", max_chars=3000,
    )

    assert "Total net sales" in context
    assert "566,685" in context
    assert "478,713" in context


def test_extracted_peng_financial_tables_survive_normalization_and_prompt_handoff():
    from tradingagents.agents.utils.evidence import merge_evidence, render_shared_evidence
    from tradingagents.dataflows.sec_documents import extract_sections

    fixture = Path(__file__).parent / "fixtures" / "sec" / "peng_q4_ex991.html"
    sections = extract_sections(fixture.read_text(encoding="utf-8"), "EX-99.1")
    raw = [{
        "source": "sec", "category": "earnings_release", "title": section.name,
        "content": section.content, "table_rows": section.table_rows,
        "status": "available", "coverage": "partial",
        "published_at": "2026-10-06", "as_of_date": "2026-10-09",
        "section": section.name, "report_period": "2026-08-28",
    } for section in sections if section.table_rows]
    normalized = merge_evidence([], raw, "2026-10-09")

    context = render_shared_evidence(
        state_with_evidence(trade_date="2026-10-09", shared_evidence=normalized),
        role="fundamentals", max_chars=6000,
    )

    assert "566,685" in context
    assert "Integrated Memory" in context
    assert "924,001" in context


def test_future_or_wrong_run_sources_are_not_admitted():
    from tradingagents.agents.utils.evidence import render_shared_evidence

    state = state_with_evidence(shared_evidence=[
        source_record(published_at="2025-05-03", content="FUTURE_FACT"),
        source_record(as_of_date="2025-05-04", content="OTHER_RUN_FACT"),
        source_record(published_at=None, content="UNDATED_HISTORICAL_FACT"),
    ])
    context = render_shared_evidence(state, role="research")
    assert "FUTURE_FACT" not in context
    assert "OTHER_RUN_FACT" not in context
    assert "UNDATED_HISTORICAL_FACT" not in context


def test_live_retrieved_source_does_not_invent_publication_date():
    from tradingagents.agents.utils.evidence import render_shared_evidence

    today = date.today().isoformat()
    state = state_with_evidence(trade_date=today, shared_evidence=[
        source_record(as_of_date=today, published_at=None),
    ])
    context = render_shared_evidence(state, role="market")
    assert "123456789" in context
    assert "publication time unknown" in context.lower()


def test_source_evidence_is_persisted_separately_from_model_reports(tmp_path):
    from tradingagents.reporting import write_source_evidence

    write_source_evidence(state_with_evidence(), tmp_path)
    records = json.loads((tmp_path / "0_sources" / "evidence.json").read_text())
    manifest = (tmp_path / "0_sources" / "README.md").read_text()
    assert records[0]["source"] == "sec"
    assert records[0]["content"].startswith("Revenue was")
    assert "https://www.sec.gov/Archives/example" in manifest
    assert "model-authored" not in records[0]["content"]

    # Rewriting the identical artifact is idempotent; different evidence cannot
    # silently replace the provenance for an existing report tree.
    assert write_source_evidence(state_with_evidence(), tmp_path) == (
        tmp_path / "0_sources" / "evidence.json"
    )
    changed = state_with_evidence(shared_evidence=[source_record(content="DIFFERENT")])
    with pytest.raises(FileExistsError, match="immutable source evidence"):
        write_source_evidence(changed, tmp_path)
    assert json.loads((tmp_path / "0_sources" / "evidence.json").read_text()) == records


def test_run_fingerprint_tracks_source_facts_but_not_retrieval_time():
    from tradingagents.reporting import build_run_manifest

    base = source_record(retrieved_at="2026-09-01T10:00:00Z")
    later = source_record(retrieved_at="2026-09-01T11:00:00Z")
    changed = source_record(content="Different reported revenue")
    metadata_only = source_record(coverage="metadata_only")
    first = build_run_manifest(state_with_evidence(shared_evidence=[base]), "NVDA")
    same_facts = build_run_manifest(state_with_evidence(shared_evidence=[later]), "NVDA")
    changed_facts = build_run_manifest(state_with_evidence(shared_evidence=[changed]), "NVDA")
    changed_coverage = build_run_manifest(state_with_evidence(shared_evidence=[metadata_only]), "NVDA")
    assert first["evidence_fingerprint"] == same_facts["evidence_fingerprint"]
    assert first["evidence_fingerprint"] != changed_facts["evidence_fingerprint"]
    assert first["evidence_fingerprint"] != changed_coverage["evidence_fingerprint"]
