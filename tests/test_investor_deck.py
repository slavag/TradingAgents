"""Evidence-dependent research topics and strict narrative grounding."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from tradingagents.agents.utils.structured import StructuredOutputFailure
from tradingagents.investor_deck import (
    DraftPoint,
    NarrativeBatch,
    NarrativeDraft,
    TrendBlock,
    _source_packet,
    _team_case_point,
    _valid_point,
    build_deck_outline,
    build_investor_deck,
)
from tradingagents.investor_deck_html import render_investor_deck
from tradingagents.research_ledger import (
    ResearchExcerpt,
    ResearchFact,
    ResearchLedger,
    ResearchSource,
    build_research_ledger,
)

pytestmark = pytest.mark.unit


def peng_ledger_fixture():
    sources = {
        key: ResearchSource(
            key, "analysis" if key.startswith("analysis:") else "sec", key,
            None if key.startswith("analysis:") else "https://www.sec.gov/Archives/example",
            "2026-10-06", "Business", "available",
        ) for key in (
            "ev-business", "ev-release", "ev-governance", "analysis:bull", "analysis:bear"
        )
    }

    def fact(metric, value, period, source="ev-release"):
        return ResearchFact(
            metric, Decimal(value), "USD", period,
            "2026-08-28" if period in {"Q4-26", "FY26"} else "2026-05-31",
            "annual" if period == "FY26" else "quarterly", "gaap", (source,),
        )

    return ResearchLedger(
        facts=(
            fact("net_sales", "566685000", "Q4-26"),
            fact("net_sales", "478713000", "Q3-26"),
            fact("net_income", "93204000", "Q4-26"),
            fact("net_income", "44689000", "Q3-26"),
            fact("operating_cash_flow", "-74788000", "Q3-26"),
            fact("net_sales", "1731468000", "FY26"),
            fact("segment:Integrated Memory", "924001000", "FY26"),
            fact("segment:Advanced Computing", "558789000", "FY26"),
        ),
        sources=sources,
        excerpts={
            "business": (ResearchExcerpt(
                "business", "ev-business",
                "Penguin supplies AI computing systems. OriginAI products and solutions "
                "face competition. Integrated Memory develops specialized modules.",
            ),),
            "earnings_release": (ResearchExcerpt(
                "earnings_release", "ev-release",
                "Customer Lektra selected the platform. Other neocloud customers "
                "deployed systems. The management outlook calls for 40% sales growth. "
                "Management guidance gives diluted EPS of $3.50.",
            ),),
            "governance": (ResearchExcerpt(
                "governance", "ev-governance", "Stephen Cumming was appointed CFO.",
            ),),
            "analysis": (ResearchExcerpt(
                "analysis", "analysis:bull", "AI deployments may grow.",
            ), ResearchExcerpt(
                "analysis", "analysis:bear", "Working capital may absorb cash.",
            )),
        },
    )


def test_peng_outline_covers_company_and_financial_topics_not_just_portfolio():
    outline = build_deck_outline(peng_ledger_fixture(), {"trade_date": "2026-10-09"})
    topics = {slide.topic for slide in outline}

    assert {
        "business", "products", "segments", "relationships", "revenue",
        "earnings", "cash", "guidance", "bull_case",
    } <= topics
    assert "bull_bear" not in topics
    assert "missing_sources" not in topics


def test_revenue_trend_is_chronological_and_growth_uses_compatible_quarters():
    ledger = peng_ledger_fixture()
    source = ledger.sources["ev-release"]
    prior_year = ResearchFact(
        "net_sales", Decimal("337922000"), "USD", "Q4-25", None,
        "quarterly", "gaap", ("ev-release",),
    )
    ledger = ResearchLedger(
        (*(
            ResearchFact(
                fact.metric, fact.value, fact.unit, fact.period_label,
                None if fact.metric == "net_sales" and fact.period_label == "Q3-26"
                else fact.period_end,
                fact.period_kind, fact.basis, fact.source_ids,
            ) for fact in ledger.facts
        ), prior_year), ledger.sources | {"ev-release": source},
        ledger.excerpts,
    )
    slide = next(slide for slide in build_deck_outline(
        ledger, {"trade_date": "2026-10-09"}
    ) if slide.topic == "revenue")
    trend = next(block for block in slide.blocks if isinstance(block, TrendBlock))

    assert [point.period for point in trend.points] == ["Q4-25", "Q3-26", "Q4-26"]
    assert all(point.basis == "GAAP" for point in trend.points)
    assert any("18.4%" in point.text and "Q3-26" in point.text
               for block in slide.blocks if block.kind == "text" for point in block.points)


def test_unsupported_topic_is_omitted_without_placeholder_slide():
    full = peng_ledger_fixture()
    sparse = ResearchLedger(
        tuple(fact for fact in full.facts if fact.metric == "net_sales"),
        {"ev-release": full.sources["ev-release"]}, {},
    )
    topics = {slide.topic for slide in build_deck_outline(sparse, {})}

    assert "products" not in topics
    assert "missing_sources" not in topics


def test_model_missing_required_topic_keeps_only_validated_source_deck():
    state = _supported_business_state()
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = NarrativeBatch(slides=[])

    deck = build_investor_deck(state, "PENG", "2026-10-09", llm)
    assert deck is not None and deck.synthesis_status == "source_only"
    assert deck.synthesis_reason == "required_topics_missing"
    assert all(not slide.narrative for slide in deck.slides)
    assert llm.with_structured_output.return_value.invoke.call_count == 1


def _supported_business_state():
    return {"trade_date": "2026-10-09", "shared_evidence": [{
        "evidence_id": "ev-business", "source": "sec", "category": "business",
        "title": "Annual business",
        "content": (
            "Penguin supplies advanced computing systems. "
            "Integrated Memory develops specialized modules. "
            "Customer Lektra selected the platform."
        ),
        "published_at": "2026-10-06", "as_of_date": "2026-10-09",
        "status": "available", "coverage": "partial",
    }]}


@pytest.mark.parametrize("text, quote", [
    ("Customer Lektra contributed $9 billion", "Customer Lektra selected the platform."),
    ("Customer Lektra earned $12 million", "Customer Lektra earned $10 million."),
])
def test_model_fact_point_rejects_invented_numbers_even_with_valid_source(text, quote):
    ledger = peng_ledger_fixture()
    if quote not in ledger.excerpts["earnings_release"][0].text:
        ledger = ResearchLedger(
            ledger.facts, ledger.sources,
            ledger.excerpts | {"earnings_release": (
                ResearchExcerpt("earnings_release", "ev-release", quote),
            )},
        )
    point = DraftPoint(text=text, source_id="ev-release", quote=quote)

    assert not _valid_point(point, ledger)


def test_model_fact_point_rejects_uncited_customer_name():
    point = DraftPoint(
        text="Customer Invented selected the platform.", source_id="ev-release",
        quote="Customer Lektra selected the platform.",
    )
    assert not _valid_point(point, peng_ledger_fixture())


def test_model_prose_cannot_insert_an_unverified_number():
    state = _supported_business_state()
    topics = [slide.topic for slide in build_deck_outline(
        build_research_ledger(state, "2026-10-09"), state
    )]
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = NarrativeBatch(slides=[
        NarrativeDraft(
            topic=topic, takeaway="Revenue grew 900%", context="The result matters.",
            source_ids=["ev-business"],
        ) for topic in topics
    ])

    deck = build_investor_deck(state, "PENG", "2026-10-09", llm)
    assert deck is not None and deck.synthesis_status == "source_only"
    assert deck.synthesis_reason == "unsupported_interpretation"
    assert all("900%" not in slide.takeaway for slide in deck.slides)
    assert "900%" not in render_investor_deck(deck, "20261009T154737Z")
    assert llm.with_structured_output.return_value.invoke.call_count >= 1


def test_unknown_structured_failure_code_is_not_persisted(monkeypatch):
    llm = MagicMock()

    def fail_structured(*_args, **_kwargs):
        raise StructuredOutputFailure("secret-provider-text")

    monkeypatch.setattr(
        "tradingagents.investor_deck.invoke_structured_required", fail_structured,
    )

    deck = build_investor_deck(_supported_business_state(), "PENG", "2026-10-09", llm)

    assert deck is not None
    assert deck.synthesis_status == "source_only"
    assert deck.synthesis_reason == "invalid_structured_response"
    assert "secret-provider-text" not in deck.model_dump_json()


def test_optional_narration_failure_logs_reason_not_provider_text(caplog):
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.side_effect = RuntimeError(
        "private model response: buy at any price"
    )

    deck = build_investor_deck(_supported_business_state(), "PENG", "2026-10-09", llm)

    assert deck is not None and deck.synthesis_reason == "structured_invocation_failed"
    assert "structured_invocation_failed" in caplog.text
    assert "private model response" not in caplog.text


@pytest.mark.parametrize("bad_context,bad_point", [
    ("Microsoft is a customer.", None),
    ("The cited material describes the trade-off.",
     DraftPoint(
         text="Lektra contributed nine billion dollars.", source_id="ev-business",
         quote="Customer Lektra selected the platform.",
     )),
])
def test_full_builder_rejects_uncited_company_or_spelled_out_money(bad_context, bad_point):
    state = _supported_business_state()
    topics = [slide.topic for slide in build_deck_outline(
        build_research_ledger(state, "2026-10-09"), state
    )]
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = NarrativeBatch(slides=[
        NarrativeDraft(
            topic=topic, takeaway="Evidence informs the view.", context=bad_context,
            source_ids=["ev-business"], points=[bad_point] if bad_point else [],
        ) for topic in topics
    ])

    deck = build_investor_deck(state, "PENG", "2026-10-09", llm)
    assert deck is not None and deck.synthesis_status == "source_only"
    assert all(not slide.narrative for slide in deck.slides)
    assert llm.with_structured_output.return_value.invoke.call_count >= 1


def test_analysis_only_evidence_cannot_publish_a_research_deck():
    state = {
        "trade_date": "2026-10-09", "shared_evidence": [],
        "investment_debate_state": {
            "bull_history": "Growth may accelerate.",
            "bear_history": "Cash conversion may remain weak.",
        },
        "final_trade_decision": "Hold pending improved cash conversion.",
    }
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = NarrativeBatch(slides=[
        NarrativeDraft(
            topic="bull_bear", takeaway="The view could change.",
            context="Both cases remain conditional.", source_ids=["analysis:bull"],
        ),
    ])

    assert build_investor_deck(state, "THIN", "2026-10-09", llm) is None


def test_newer_vendor_quarter_remains_visible_beside_older_sec_history():
    sec_source = ResearchSource(
        "ev-sec", "sec", "Issuer quarterly filing", None,
        "2026-07-07", "Financial results", "available",
    )
    vendor_source = ResearchSource(
        "ev-vendor", "get_income_statement", "Vendor statement", None,
        "2026-10-09", None, "available",
    )
    ledger = ResearchLedger(
        facts=(
            ResearchFact("net_sales", Decimal("100000000"), "USD", "Q1-26",
                         "2026-02-28", "quarterly", "gaap", ("ev-sec",)),
            ResearchFact("net_sales", Decimal("120000000"), "USD", "Q2-26",
                         "2026-05-29", "quarterly", "gaap", ("ev-sec",)),
            ResearchFact("net_sales", Decimal("150000000"), "USD", "2026-08-31",
                         "2026-08-31", "quarterly", "gaap", ("ev-vendor",)),
        ),
        sources={"ev-sec": sec_source, "ev-vendor": vendor_source}, excerpts={},
    )
    slide = next(slide for slide in build_deck_outline(
        ledger, {"trade_date": "2026-10-09"}
    ) if slide.topic == "revenue")

    assert any(
        metric.period == "2026-08-31" and "ev-vendor" in metric.source_ids
        for block in slide.blocks
        for metric in (block.metrics if block.kind == "metrics" else
                       block.points if block.kind == "trend" else [])
    )


def test_confirmed_announced_dates_create_a_sourced_catalyst_topic():
    source = ResearchSource(
        "ev-events", "sec", "Issuer event filing", None,
        "2026-10-06", "Outlook", "available",
    )
    ledger = ResearchLedger(
        facts=(), sources={"ev-events": source}, excerpts={
            "earnings_release": (ResearchExcerpt(
                "earnings_release", "ev-events",
                "Management announced a product launch scheduled for 2026-12-05. "
                "A customer deployment was announced for 2026-12-20.",
            ),),
        },
    )

    outline = build_deck_outline(ledger, {"trade_date": "2026-10-09"})

    assert any(slide.topic == "confirmed_catalysts" for slide in outline)


def test_product_topic_prefers_named_product_detail_over_generic_annual_intro():
    ledger = ResearchLedger(
        facts=(),
        sources={
            "ev-overview": ResearchSource(
                "ev-overview", "sec", "Annual overview", None,
                "2025-10-24", "Business", "available",
            ),
            "ev-products": ResearchSource(
                "ev-products", "sec", "Annual products", None,
                "2025-10-24", "Products", "available",
            ),
        },
        excerpts={"business": (
            ResearchExcerpt(
                "business", "ev-overview",
                "The company sells computing products. Its product portfolio serves enterprises. "
                "Customers purchase several product categories.",
            ),
            ResearchExcerpt(
                "business", "ev-products",
                "Our solutions include OriginAI for AI factories. "
                "ClusterWareAI software manages computing clusters.",
            ),
        )},
    )

    outline = build_deck_outline(ledger, {"trade_date": "2026-10-09"})
    products = next(slide for slide in outline if slide.topic == "products")
    points = [point.text for block in products.blocks if block.kind == "text"
              for point in block.points]

    assert any("OriginAI" in point for point in points)
    assert any("ClusterWareAI" in point for point in points)


def test_named_solution_is_not_crowded_out_by_related_platform_details():
    ledger = ResearchLedger(
        facts=(),
        sources={
            "ev-products": ResearchSource(
                "ev-products", "sec", "Annual products", None,
                "2025-10-24", "Products", "available",
            ),
            "ev-release": ResearchSource(
                "ev-release", "sec", "Business highlights", None,
                "2026-10-06", "Business highlights", "available",
            ),
        },
        excerpts={
            "business": (ResearchExcerpt(
                "business", "ev-products",
                "Our solutions include OriginAI for AI factories. "
                "This is achieved through the ClusterWare software platform. "
                "ICE ClusterWare is a software platform for managing clusters.",
            ),),
            "earnings_release": (ResearchExcerpt(
                "earnings_release", "ev-release",
                "ClusterWareAI software supports AI infrastructure customers.",
            ),),
        },
    )

    products = next(slide for slide in build_deck_outline(
        ledger, {"trade_date": "2026-10-09"}
    ) if slide.topic == "products")
    points = [point.text for block in products.blocks if block.kind == "text"
              for point in block.points]

    assert any("OriginAI" in point for point in points)


def test_relationship_topic_retains_named_customer_after_generic_bullets():
    ledger = ResearchLedger(
        facts=(),
        sources={
            "ev-quarter": ResearchSource(
                "ev-quarter", "sec", "Quarter highlights", None,
                "2026-10-06", "Quarterly highlights", "available",
            ),
            "ev-business": ResearchSource(
                "ev-business", "sec", "Business highlights", None,
                "2026-10-06", "Business highlights", "available",
            ),
        },
        excerpts={"earnings_release": (
            ResearchExcerpt(
                "earnings_release", "ev-quarter",
                "We focus on customer execution. Customers are seeing stronger demand. "
                "Enterprise customers are adopting new solutions.",
            ),
            ResearchExcerpt(
                "earnings_release", "ev-business",
                "Won six new customers. • Continued to serve neocloud customers. "
                "• Won another neocloud customer, Lektra, which selected our platform. "
                "• A supplier arrangement improved availability.",
            ),
        )},
    )

    outline = build_deck_outline(ledger, {"trade_date": "2026-10-09"})
    relationships = next(slide for slide in outline if slide.topic == "relationships")
    points = [point.text for block in relationships.blocks if block.kind == "text"
              for point in block.points]

    assert any("Lektra" in point for point in points)


def test_team_interpretation_is_bounded_and_cannot_create_a_case_without_facts():
    ledger = ResearchLedger(
        facts=(),
        sources={
            key: ResearchSource(key, "analysis", key, None, None, None, "available")
            for key in ("analysis:bull", "analysis:bear")
        },
        excerpts={"analysis": (
            ResearchExcerpt(
                "analysis", "analysis:bull",
                "AI demand could support growth. " + "Additional debate detail. " * 500,
            ),
            ResearchExcerpt(
                "analysis", "analysis:bear",
                "Cash conversion could lag earnings. " + "Additional debate detail. " * 500,
            ),
        )},
    )

    assert not any(slide.topic in {"bull_case", "bear_case"} for slide in
                   build_deck_outline(ledger, {"trade_date": "2026-10-09"}))
    bull = _team_case_point(list(ledger.excerpts["analysis"]), "bull")
    bear = _team_case_point(list(ledger.excerpts["analysis"]), "bear")
    assert bull is not None and bear is not None
    assert max(len(bull.text), len(bear.text)) <= 245


def test_team_interpretation_selects_thesis_not_dialogue_or_markdown_header():
    keys = ("analysis:bull", "analysis:bear", "analysis:portfolio")
    ledger = ResearchLedger(
        facts=(),
        sources={key: ResearchSource(key, "analysis", key, None, None, None, "available")
                 for key in keys},
        excerpts={"analysis": (
            ResearchExcerpt(
                "analysis", "analysis:bull",
                "Bull Analyst: ## Bull Case for TEST: Headline\n\n"
                "Bear, I agree the latest price is extended.\n\n"
                "The core question is whether customer demand is durable. "
                "The latest evidence supports a stronger business.",
            ),
            ResearchExcerpt(
                "analysis", "analysis:bear",
                "Bear Analyst: ## Bear rebuttal\n\n"
                "Bull, I agree the quarter was strong.\n\n"
                "My conclusion remains bearish: cash conversion is not yet durable.",
            ),
            ResearchExcerpt(
                "analysis", "analysis:portfolio",
                "**Decision Status**: Actionable\n**Rating**: Hold\n"
                "**Executive Summary**: The business is improving but the entry is stretched. "
                "Maintain exposure while watching cash conversion.\n"
                "**Conditional Confirmation**: Reassess after operating cash flow improves.",
            ),
        )},
    )

    bull = _team_case_point(list(ledger.excerpts["analysis"]), "bull")
    bear = _team_case_point(list(ledger.excerpts["analysis"]), "bear")

    assert bull is not None and bear is not None
    assert "core question" in bull.text
    assert "cash conversion is not yet durable" in bear.text
    assert all("##" not in point.text and "**" not in point.text
               and "Analyst:" not in point.text for point in (bull, bear))


def test_team_interpretation_does_not_stitch_separate_emphasis_fragments():
    excerpt = ResearchExcerpt(
        "analysis", "analysis:bear",
        "Bear Analyst: They treat a **very strong quarter** as if **PENG** is a buy.\n\n"
        "Here's my bear case: **The stock has rerated sharply while cash conversion "
        "remains negative and execution risk persists.**",
    )

    point = _team_case_point([excerpt], "bear")

    assert point is not None
    assert "stock has rerated sharply" in point.text
    assert "very strong quarter as if" not in point.text


def test_team_interpretation_prefers_complete_view_over_text_between_bold_spans():
    excerpt = ResearchExcerpt(
        "analysis", "analysis:bull",
        "Bull Analyst: ## Bull case for PENG: debate\n\n"
        "Bear, your concerns describe **entry timing and execution risk**, not a "
        "broken business thesis. The evidence increasingly supports the latter.\n\n"
        "My view: **PENG is investable for growth-oriented investors**, although "
        "a staged entry is more prudent than a chase.",
    )

    point = _team_case_point([excerpt], "bull")

    assert point is not None
    assert point.text.startswith("Team interpretation: PENG is investable")
    assert "not a broken business thesis" not in point.text
    assert not point.text.endswith("My view:")


def test_long_team_view_is_never_displayed_as_a_cut_sentence():
    long_view = "The investment case depends on " + "durable execution and cash conversion " * 10
    excerpt = ResearchExcerpt("analysis", "analysis:bull", long_view + ".")

    point = _team_case_point([excerpt], "bull")

    assert point is None or "…" not in point.text


def test_long_selected_source_sentence_and_takeaway_are_not_clipped():
    source = ResearchSource(
        "ev-business", "sec", "Annual business", None,
        "2025-10-24", "Business", "available",
    )
    long_sentence = (
        "Penguin designs and manages enterprise computing systems "
        + "with specialized memory and deployment services " * 12 + "."
    )
    ledger = ResearchLedger(
        facts=(), sources={"ev-business": source}, excerpts={"business": (
            ResearchExcerpt(
                "business", "ev-business",
                long_sentence + " Today we have Advanced Computing, Integrated Memory "
                "and Optimized LED. Penguin builds high-availability systems.",
            ),
        )},
    )

    business = next(slide for slide in build_deck_outline(
        ledger, {"trade_date": "2026-10-09"}
    ) if slide.topic == "business")
    points = [point.text for block in business.blocks if block.kind == "text"
              for point in block.points]

    assert "…" not in business.takeaway
    assert all("…" not in point for point in points)
    assert not any(point.startswith("Penguin designs") and point != long_sentence
                   for point in points)


def test_leadership_takeaway_uses_a_complete_source_sentence():
    source = ResearchSource(
        "ev-governance", "sec", "Officer filing", None,
        "2026-10-06", "Officer appointment", "available",
    )
    appointment = (
        "On October 6, 2026, the board appointed a new chief financial officer "
        "who will also serve as principal accounting officer and oversee the "
        "company's financial reporting while the company transitions from its "
        "interim finance leadership arrangement, effective immediately."
    )
    ledger = ResearchLedger(
        facts=(), sources={"ev-governance": source}, excerpts={"governance": (
            ResearchExcerpt(
                "governance", "ev-governance",
                appointment + " The prior officer was succeeded on that date.",
            ),
        )},
    )

    slide = next(item for item in build_deck_outline(
        ledger, {"trade_date": "2026-10-09"}
    ) if item.topic == "leadership")

    assert slide.takeaway == appointment
    assert "…" not in slide.takeaway


def test_business_slide_prefers_operating_segments_over_rebranding_boilerplate():
    source = ResearchSource(
        "ev-business", "sec", "Annual business", None,
        "2025-10-24", "Business", "available",
    )
    ledger = ResearchLedger(
        facts=(), sources={"ev-business": source}, excerpts={"business": (
            ResearchExcerpt(
                "business", "ev-business",
                "Business Segments The most exciting technology is challenging to adopt. "
                "Over recent years we transformed our business structure. "
                "We revised how we describe our business segments. "
                "Today we have Advanced Computing, Integrated Memory and Optimized LED. "
                "Penguin designs, builds and manages high-performance enterprise solutions. "
                "Financial Statements and Supplementary Data – Notes to Consolidated "
                "Financial Statements – Intangible Assets and Goodwill” Integrated Memory "
                "solves specialty memory limitations.",
            ),
        )},
    )

    business = next(slide for slide in build_deck_outline(
        ledger, {"trade_date": "2026-10-09"}
    ) if slide.topic == "business")
    points = [point.text for block in business.blocks if block.kind == "text"
              for point in block.points]

    assert any("Advanced Computing, Integrated Memory and Optimized LED" in point
               for point in points)
    assert any("designs, builds and manages" in point for point in points)
    assert not any("most exciting technology" in point or "revised how we describe" in point
                   for point in points)
    assert not any("Financial Statements and Supplementary Data" in point
                   for point in points)


def test_management_outlook_uses_typed_source_table_not_cfo_reference():
    sources = {
        "ev-quarter": ResearchSource(
            "ev-quarter", "sec", "Quarter highlights", None,
            "2026-10-06", "Quarterly highlights", "available",
        ),
        "ev-outlook": ResearchSource(
            "ev-outlook", "sec", "Earnings release outlook", None,
            "2026-10-06", "Outlook", "available",
        ),
        "ev-table": ResearchSource(
            "ev-table", "sec", "EX-99.1 outlook table", None,
            "2026-10-06", "Table 3", "available",
        ),
        "ev-cfo": ResearchSource(
            "ev-cfo", "sec", "CFO filing", None,
            "2026-10-06", "Results of operations", "available",
        ),
        "ev-legal": ResearchSource(
            "ev-legal", "sec", "Forward-looking statement", None,
            "2026-10-06", "Outlook", "available",
        ),
    }
    ledger = ResearchLedger(
        facts=(), sources=sources, excerpts={"earnings_release": (
            ResearchExcerpt(
                "earnings_release", "ev-quarter",
                "Fiscal 2026 results show strong reported net sales and earnings.",
            ),
            ResearchExcerpt(
                "earnings_release", "ev-outlook",
                "Management issued an outlook for fiscal year 2027 with net-sales growth "
                "and diluted EPS guidance. The outlook is forward-looking.",
            ),
            ResearchExcerpt(
                "earnings_release", "ev-table", "Raising Fiscal 2027 Outlook",
                table_rows=(
                    ("Outlook", "GAAP Outlook", "Adjustments", "Non-GAAP Outlook"),
                    ("Net sales", "40% YoY Growth +/-10%", "—", "40% YoY Growth +/-10%"),
                    ("Gross margin", "27% +/- 2%", "1%", "(A)", "28% +/- 2%"),
                    ("Diluted earnings per share", "$3.50 +/- $0.70", "$0.95",
                     "(A)(B)", "$4.45 +/- $0.70"),
                ),
            ),
            ResearchExcerpt(
                "earnings_release", "ev-cfo",
                "The CFO appointment filing refers readers to a separate outlook release. "
                "The company's results and outlook appear elsewhere.",
            ),
            ResearchExcerpt(
                "earnings_release", "ev-legal",
                "The Business Outlook referenced above contains forward-looking statements. "
                "This outlook is subject to many legal risks and uncertainties.",
            ),
        )},
    )

    guidance = next(slide for slide in build_deck_outline(
        ledger, {"trade_date": "2026-10-09"}
    ) if slide.topic == "guidance")
    tables = [block for block in guidance.blocks if block.kind == "table"]

    assert len(tables) == 1
    assert "FY2027" in tables[0].headings[1]
    assert len(tables[0].rows) == 3
    assert tables[0].rows[0].cells[1] == "40% YoY Growth +/-10%"
    assert tables[0].rows[2].cells[-1] == "$4.45 +/- $0.70"
    assert any("not reported results" in point.text for block in guidance.blocks
               if block.kind == "text" for point in block.points)
    assert "ev-table" in guidance.source_ids
    assert "ev-cfo" not in guidance.source_ids
    assert "ev-legal" not in guidance.source_ids


def test_matching_sec_q3_fact_and_release_comparative_are_one_trend_point():
    sources = {
        "ev-release": ResearchSource(
            "ev-release", "sec", "Q4 release", None,
            "2026-10-06", "Table 2", "available",
        ),
        "ev-xbrl": ResearchSource(
            "ev-xbrl", "sec", "Q3 XBRL", None,
            "2026-07-07", None, "available",
        ),
    }
    ledger = ResearchLedger(
        facts=(
            ResearchFact("net_sales", Decimal("566685000"), "USD", "Q4-26",
                         "2026-08-28", "quarterly", "gaap", ("ev-release",)),
            ResearchFact("net_sales", Decimal("478713000"), "USD", "Q3-26",
                         None, "quarterly", "gaap", ("ev-release",)),
            ResearchFact("net_sales", Decimal("478713000"), "USD", "Q3-26",
                         "2026-05-29", "quarterly", "gaap", ("ev-xbrl",)),
        ),
        sources=sources, excerpts={},
    )

    slide = next(slide for slide in build_deck_outline(
        ledger, {"trade_date": "2026-10-09"}
    ) if slide.topic == "revenue")
    trend = next(block for block in slide.blocks if block.kind == "trend")

    assert [point.period for point in trend.points] == ["Q3-26", "Q4-26"]
    assert set(trend.points[0].source_ids) == {"ev-release", "ev-xbrl"}


def test_cash_and_capital_slides_use_latest_comparable_values_not_history_flood():
    sources = {
        key: ResearchSource(key, "get_balance_sheet", "Vendor statements", None,
                            "2026-10-09", None, "available")
        for key in ("ev-latest", "ev-old")
    }
    facts = tuple(
        ResearchFact(metric, Decimal(value), "USD", period, period, kind,
                     "gaap", ("ev-latest" if period == "2026-05-31" else "ev-old",))
        for period, values in (
            ("2026-05-31", {"cash": "440300000", "total_debt": "502509000",
                             "operating_cash_flow": "-74788000", "free_cash_flow": "-77629000"}),
            ("2026-02-28", {"cash": "489172000", "total_debt": "503528000",
                             "operating_cash_flow": "54952000", "free_cash_flow": "53349000"}),
        )
        for metric, value in values.items()
        for kind in ("instant" if metric in {"cash", "total_debt"} else "quarterly",)
    )
    ledger = ResearchLedger(facts=facts, sources=sources, excerpts={})
    outline = build_deck_outline(ledger, {"trade_date": "2026-10-09"})

    for topic in ("cash", "capital"):
        slide = next(slide for slide in outline if slide.topic == topic)
        metrics = [metric for block in slide.blocks if block.kind == "metrics"
                   for metric in block.metrics]
        assert len(metrics) == 2
        assert {metric.period for metric in metrics} == {"2026-05-31"}
        assert slide.source_ids == ["ev-latest"]


def test_source_packet_keeps_json_complete_under_budget():
    ledger = peng_ledger_fixture()
    many = ResearchLedger(
        ledger.facts, ledger.sources,
        ledger.excerpts | {"business": (
            ResearchExcerpt("business", "ev-business", "fact " * 10000),
        )},
    )
    slides = build_deck_outline(many, {"trade_date": "2026-10-09"})
    packet = _source_packet(slides[:4], many)

    assert len(packet) <= 24_000
    assert '"required_topics"' in packet
    assert '"no_new_numbers": true' in packet


def test_source_packet_labels_reported_periods_for_interpretation():
    ledger = peng_ledger_fixture()
    revenue = next(slide for slide in build_deck_outline(
        ledger, {"trade_date": "2026-10-09"}
    ) if slide.topic == "revenue")

    packet = _source_packet([revenue], ledger)

    assert '"metric": "net_sales"' in packet
    assert '"period": "Q4-26"' in packet
    assert '"basis": "gaap"' in packet
