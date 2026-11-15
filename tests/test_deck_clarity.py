"""Self-contained regressions for readable, attributed source-only slides."""
from decimal import Decimal

import pytest

from tradingagents.investor_deck import TableBlock, TextBlock, build_deck_outline
from tradingagents.research_ledger import (
    ResearchExcerpt,
    ResearchFact,
    ResearchLedger,
    ResearchSource,
)

pytestmark = pytest.mark.unit


def ledger_with(excerpts=(), facts=()):
    return ResearchLedger(tuple(facts), {
        item.evidence_id: ResearchSource(item.evidence_id, "sec", "Quarterly filing", None,
                                       "2026-11-12", "Management discussion", "available")
        for item in excerpts
    } | {"financial": ResearchSource("financial", "sec", "Earnings release", None,
                                     "2026-11-12", "Income statement", "available")},
        {"quarterly_filing": tuple(excerpts)})


def points(slide):
    return [point for block in slide.blocks if isinstance(block, TextBlock) for point in block.points]


def test_accounting_customer_terms_do_not_create_commercial_slide():
    rows = (ResearchExcerpt("quarterly_filing", "acct",
        "Customer warrant assets amortization 2026 2025 47,340 8,230. "
        "Customer warrant liabilities reconciliation 2026 2025 14,000 9,000.",
        (("Customer warrant assets", "47,340", "8,230"),)),)
    ledger = ledger_with(rows)
    ledger = ResearchLedger(ledger.facts, ledger.sources, {"earnings_release": rows})
    assert "relationships" not in {slide.topic for slide in build_deck_outline(ledger, {})}


def test_quarterly_business_and_commercial_disclosures_support_full_source_points():
    text = ("We operate cloud services for enterprise customers. "
            "Our platform provides dedicated compute capacity for training workloads. "
            "Our products include software tools that manage customer deployment. "
            "Customer Orion signed a capacity agreement for cloud services. "
            "Our partner Vega distributes the platform to enterprise buyers.")
    ledger = ledger_with([ResearchExcerpt("quarterly_filing", "business", text)])
    slides = {s.topic: s for s in build_deck_outline(ledger, {"trade_date": "2026-11-13"})}
    assert {"business", "products", "relationships"} <= slides.keys()
    for topic in ("business", "products", "relationships"):
        slide = slides[topic]
        assert slide.takeaway not in [p.text for p in points(slide)]
        assert len(points(slide)) >= 3
        assert any("Interpretation:" in p.text for p in points(slide))
        assert all(p.source_ids == ["business"] for p in points(slide))
    assert any("Customer Orion signed" in p.text for p in points(slides["relationships"]))


def test_financial_fallback_explains_revenue_profit_divergence_and_separates_ytd():
    def fact(metric, value, year, kind="quarterly"):
        return ResearchFact(metric, Decimal(value), "USD", f"Q3-{str(year)[2:]}",
                            f"{year}-09-30", kind, "gaap", ("financial",))
    facts = [fact("net_sales", "210809000", 2026), fact("net_sales", "135714000", 2025),
             fact("gross_profit", "42094000", 2026), fact("gross_profit", "55135000", 2025),
             fact("net_income", "-161218000", 2026), fact("net_income", "-33990000", 2025),
             fact("net_sales", "584324000", 2026, "ytd")]
    slides = {s.topic: s for s in build_deck_outline(ledger_with(facts=facts), {})}
    earnings = slides["earnings"]
    for topic in ("revenue", "earnings"):
        assert slides[topic].takeaway not in [p.text for p in points(slides[topic])]
        assert len(slides[topic].takeaway.split()) <= 25
    table = next(b for b in earnings.blocks if isinstance(b, TableBlock))
    assert any("Gross profit" in row.cells[0] for row in table.rows)
    assert "GAAP" in " ".join(table.headings)
    assert "quarterly" in " ".join(table.headings).lower()
    assert "584.3" not in str(table)
    body = " ".join(p.text for p in points(earnings))
    assert "revenue increased" in body.lower() and "gross profit declined" in body.lower()
    assert "cash" in body.lower()
    assert len(points(earnings)) >= 2
    assert all(p.source_ids == ["financial"] for p in points(earnings))


def test_market_slide_discloses_daily_session_limit():
    facts = [ResearchFact(metric, Decimal(value), unit, "2026-11-12", "2026-11-12", "instant",
                          "market", ("financial",))
             for metric, value, unit in (("close", "97.75", "USD_per_share"),
                                         ("rsi", "47", "score"))]
    slide = next(s for s in build_deck_outline(ledger_with(facts=facts), {}) if s.topic == "market")
    body = " ".join(p.text for p in points(slide))
    assert "regular-session" in body.lower() and "extended-hours" in body.lower()
    assert "2026-11-12" in body


def test_number_dense_release_without_structured_rows_is_not_a_relationship():
    excerpts = (ResearchExcerpt("earnings_release", "acct",
        "Customer balances 2026 2025 47,340 8,230 14,000 9,000. "
        "Partner balances 2026 2025 11,000 8,000 4,000 7,000."),)
    base = ledger_with(excerpts)
    ledger = ResearchLedger(base.facts, base.sources, {"earnings_release": excerpts})
    assert "relationships" not in {s.topic for s in build_deck_outline(ledger, {})}


def test_commercial_sentences_with_dates_and_values_remain_attributable():
    excerpts = (ResearchExcerpt("quarterly_filing", "commercial",
        "Customer Orion signed a $40 million capacity agreement in 2026. "
        "Partner Vega distributes the platform to enterprise customers.",
        (("Year", "Revenue"), ("2026", "40"))),)
    slides = {s.topic: s for s in build_deck_outline(ledger_with(excerpts), {})}
    relationship = slides["relationships"]
    assert any("$40 million" in p.text for p in points(relationship))
    assert all(p.source_ids == ["commercial"] for p in points(relationship))


def test_profitability_table_keeps_non_gaap_and_cumulative_figures_out():
    facts = []
    for year in (2025, 2026):
        for metric, value in (("net_income", "-1000000"), ("gross_profit", "2000000")):
            facts.append(ResearchFact(metric, Decimal(value), "USD", f"Q3-{str(year)[2:]}",
                                      f"{year}-09-30", "quarterly", "gaap", ("financial",)))
    facts += [ResearchFact("gross_profit", Decimal("999000000"), "USD", "Q3-26", "2026-09-30",
                           "quarterly", "non_gaap", ("financial",)),
              ResearchFact("gross_profit", Decimal("888000000"), "USD", "9M-26", "2026-09-30",
                           "ytd", "gaap", ("financial",))]
    earnings = next(s for s in build_deck_outline(ledger_with(facts=facts), {}) if s.topic == "earnings")
    table = next(b for b in earnings.blocks if isinstance(b, TableBlock))
    assert "999.0" not in str(table) and "888.0" not in str(table)
    assert len(table.headings) == 3


@pytest.mark.parametrize("topic", ["market", "revenue", "earnings", "cash", "capital"])
def test_financial_and_market_takeaways_are_concise_and_distinct_from_body(topic):
    def fact(metric, value, period="2026-09-30", kind="quarterly", unit="USD", basis="gaap"):
        return ResearchFact(metric, Decimal(value), unit, period, period, kind, basis, ("financial",))
    facts = [fact("net_sales", "1000000", "2026-06-30"), fact("net_sales", "2000000"),
             fact("net_income", "-1000000", "2026-06-30"), fact("net_income", "-2000000"),
             fact("gross_profit", "500000", "2026-06-30"), fact("gross_profit", "400000"),
             fact("operating_cash_flow", "-1000000"), fact("cash", "5000000", kind="instant"),
             fact("total_debt", "8000000", kind="instant"),
             fact("close", "97.75", kind="instant", unit="USD_per_share", basis="market"),
             fact("rsi", "47", kind="instant", unit="score", basis="market")]
    slide = next(s for s in build_deck_outline(ledger_with(facts=facts), {}) if s.topic == topic)
    assert slide.takeaway not in [p.text for p in points(slide)]
    assert len(slide.takeaway.split()) <= 25
    assert "Interpretation:" not in slide.takeaway


def test_customer_accounting_policies_are_not_commercial_relationship_evidence():
    excerpt = ResearchExcerpt("quarterly_filing", "accounting",
        "Customer acquisition costs are deferred and amortized over the contract term. "
        "Customer deposits are recognized as revenue when performance obligations are satisfied.")
    assert "relationships" not in {s.topic for s in build_deck_outline(ledger_with([excerpt]), {})}


def test_customer_concentration_and_real_contract_terms_are_preserved():
    excerpt = ResearchExcerpt("quarterly_filing", "commercial",
        "One customer accounted for 30% of revenue during the quarter. "
        "Our customer Orion signed a noncancelable capacity contract with a deposit payable at signing.")
    slide = next(s for s in build_deck_outline(ledger_with([excerpt]), {}) if s.topic == "relationships")
    assert any("30%" in p.text for p in points(slide))
    assert any("deposit payable" in p.text for p in points(slide))


def test_profitability_comparison_uses_distinct_economic_periods_and_sec_conflicts():
    base = ledger_with()
    sources = dict(base.sources) | {"vendor": ResearchSource(
        "vendor", "data provider", "Vendor financials", None, "2026-11-13", None, "available")}
    def fact(metric, value, label, end, source="financial"):
        return ResearchFact(metric, Decimal(value), "USD", label, end, "quarterly", "gaap", (source,))
    facts = []
    for metric, q2, q3, vendor in (("net_sales", "100000000", "110000000", "200000000"),
                                  ("gross_profit", "40000000", "45000000", "20000000"),
                                  ("net_income", "1000000", "2000000", "-9000000")):
        facts.extend([fact(metric, q2, "Q2-26", "2026-06-30"),
                      fact(metric, q3, "Q3-26", "2026-09-30"),
                      fact(metric, vendor, "2026-09-30", "2026-09-30", "vendor")])
    slide = next(s for s in build_deck_outline(ResearchLedger(tuple(facts), sources, {}), {})
                 if s.topic == "earnings")
    table = next(b for b in slide.blocks if isinstance(b, TableBlock))
    assert "2026-06-30" in table.headings[1] and "2026-09-30" in table.headings[2]
    revenue = next(row for row in table.rows if row.cells[0] == "Revenue")
    assert revenue.cells[1:] == ["$100.0 million", "$110.0 million"]
    gross = next(row for row in table.rows if row.cells[0] == "Gross profit")
    assert gross.cells[1:] == ["$40.0 million", "$45.0 million"]
    assert all(row.source_ids == ["financial"] for row in table.rows)
    assert not any("Revenue increased while gross profit declined" in p.text for p in points(slide))


def test_profitability_comparison_skips_latest_incomplete_period():
    def fact(metric, value, end):
        return ResearchFact(metric, Decimal(value), "USD", end, end, "quarterly", "gaap", ("financial",))
    facts = [fact(metric, value, end) for end in ("2026-03-31", "2026-06-30")
             for metric, value in (("net_income", "1000000"), ("gross_profit", "2000000"))]
    facts.append(fact("net_income", "3000000", "2026-09-30"))
    slide = next(s for s in build_deck_outline(ledger_with(facts=facts), {}) if s.topic == "earnings")
    table = next(b for b in slide.blocks if isinstance(b, TableBlock))
    assert "2026-03-31" in table.headings[1] and "2026-06-30" in table.headings[2]
    assert "2026-09-30" not in str(table)


@pytest.mark.parametrize("previous,latest,wording,percentage", [
    ("-450500000", "-161200000", "narrowed", "64.2%"),
    ("-100000000", "-150000000", "widened", "50.0%"),
])
def test_negative_earnings_describe_loss_change(previous, latest, wording, percentage):
    facts = [ResearchFact("net_income", Decimal(value), "USD", label, end, "quarterly", "gaap", ("financial",))
             for value, label, end in ((previous, "Q2-26", "2026-06-30"), (latest, "Q3-26", "2026-09-30"))]
    slide = next(s for s in build_deck_outline(ledger_with(facts=facts), {}) if s.topic == "earnings")
    body = " ".join(p.text for p in points(slide))
    assert f"Net loss {wording} {percentage}" in body
    assert f"net loss {wording}" in slide.takeaway.lower()
    assert percentage in slide.takeaway
    assert "Net income rose" not in body and "Net income fell" not in body


def test_generic_cloud_compute_delivery_supports_business_and_product_topics():
    excerpt = ResearchExcerpt("quarterly_filing", "commercial",
        "We deliver our solutions to customers in several different ways. "
        "Customers seeking cloud flexibility can access dedicated compute through "
        "consumption-based models on our cloud service or through partner clouds. "
        "We offer computing services for enterprises training large models.")
    slides = {s.topic: s for s in build_deck_outline(ledger_with([excerpt]), {})}
    assert {"business", "products"} <= slides.keys()
    for topic in ("business", "products"):
        assert len([p for p in points(slides[topic]) if not p.text.startswith("Interpretation:")]) >= 2
        assert any("consumption-based models" in p.text for p in points(slides[topic]))


def test_service_and_solution_accounting_boilerplate_does_not_create_company_topics():
    excerpt = ResearchExcerpt("quarterly_filing", "accounting",
        "Revenue from services is recognized when performance obligations are satisfied. "
        "Services contract assets are amortized over the expected customer relationship. "
        "Solutions revenue recognition follows the accounting policy for deferred revenue.")
    assert not {"business", "products", "relationships"} & {
        s.topic for s in build_deck_outline(ledger_with([excerpt]), {})}


@pytest.mark.parametrize("label,end,expected", [
    ("3M ended 2026-09-30", "2026-09-30", "3M ended 2026-09-30"),
    ("Q1-27", "2026-09-30", "Q1-27 (ended 2026-09-30)"),
])
def test_profitability_headings_preserve_fiscal_labels_without_repeating_dates(label, end, expected):
    facts = [ResearchFact(metric, Decimal("1000000"), "USD", period, date_end, "quarterly", "gaap", ("financial",))
             for period, date_end in (("3M ended 2026-06-30", "2026-06-30"), (label, end))
             for metric in ("net_income", "gross_profit")]
    slide = next(s for s in build_deck_outline(ledger_with(facts=facts), {}) if s.topic == "earnings")
    table = next(b for b in slide.blocks if isinstance(b, TableBlock))
    assert table.headings[2] == expected
    assert table.headings[2].count(end) == 1


def test_business_prose_uses_third_person_without_changing_source_evidence():
    import re

    text = ("Our customers use Cerebras solutions to run applications that demand speed. "
            "We deliver our solutions to customers in several different ways. "
            "We generate revenue primarily from hardware solutions and cloud services.")
    ledger = ledger_with([ResearchExcerpt("quarterly_filing", "business", text)])
    slide = next(s for s in build_deck_outline(ledger, {}) if s.topic == "business")
    body = " ".join(p.text for p in points(slide))
    assert not re.search(r"\b(?:we|our|ours|us|ourselves)\b", body, re.I)
    assert "Their customers use Cerebras" in body
    assert "They deliver their solutions" in body
    assert "They generate revenue" in body
    assert ledger.excerpts["quarterly_filing"][0].text == text
    assert all(p.source_ids == ["business"] for p in points(slide))


@pytest.mark.parametrize(('original', 'expected'), [
    ("We're expanding our US cloud services.", "They're expanding their US cloud services."),
    ("We’ve built these products ourselves; the platform is ours.",
     "They've built these products themselves; the platform is theirs."),
    ("Customers buy from us and we support their deployments.",
     "Customers buy from them and they support their deployments."),
    ("Cerebras serves enterprise customers.", "Cerebras serves enterprise customers."),
])
def test_third_person_prose_preserves_grammar_and_country_abbreviations(original, expected):
    from tradingagents.investor_deck import third_person_prose

    assert third_person_prose(original) == expected
