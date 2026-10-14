"""Dated, attributable figures for the investor research deck."""

from decimal import Decimal
from pathlib import Path

import pytest

from tradingagents.dataflows.sec_documents import extract_sections
from tradingagents.research_ledger import build_research_ledger

pytestmark = pytest.mark.unit


def sec_row(**overrides):
    row = {
        "evidence_id": "ev-peng-q4", "source": "sec", "category": "earnings_release",
        "title": "FY2026 results table", "content": "(in thousands) Q4-26 Q3-26",
        "coverage": "partial", "status": "available", "published_at": "2026-10-06",
        "as_of_date": "2026-10-09",
        "url": "https://www.sec.gov/Archives/edgar/data/1616533/000161653326000046/ex991.htm",
        "section": "Quarterly Financial Results", "report_period": "2026-08-28",
        "table_rows": [
            ["Metric", "Q4-26", "Q3-26"],
            ["Total net sales", "566,685", "478,713"],
        ],
    }
    return row | overrides


def vendor_row(**overrides):
    row = {
        "evidence_id": "ev-vendor-q3", "source": "get_income_statement",
        "category": "fundamentals", "title": "Quarterly income statement",
        "content": ",2026-05-31\nTotal Revenue,478713000", "coverage": "full",
        "status": "available", "published_at": "2026-10-09",
        "as_of_date": "2026-10-09", "url": None,
    }
    return row | overrides


def test_peng_release_beats_older_vendor_quarter_without_relabeling_it():
    ledger = build_research_ledger({"shared_evidence": [sec_row(), vendor_row()]}, "2026-10-09")

    assert ledger.latest("net_sales").value == Decimal("566685000")
    assert ledger.latest("net_sales").period_label == "Q4-26"
    assert any(f.value == Decimal("478713000") and f.period_label == "2026-05-31"
               for f in ledger.facts_for("net_sales"))
    assert ledger.sources["ev-peng-q4"].section == "Quarterly Financial Results"


def test_source_index_keeps_exact_accession_document_and_period_provenance():
    row = sec_row(
        accession="0001616533-26-000046", form="8-K", document_type="EX-99.1",
    )

    source = build_research_ledger(
        {"shared_evidence": [row]}, "2026-10-09"
    ).sources["ev-peng-q4"]

    assert source.evidence_id == "ev-peng-q4"
    assert source.accession == "0001616533-26-000046"
    assert source.document_type == "EX-99.1"
    assert source.report_period == "2026-08-28"


def test_peng_segment_mix_is_derived_from_same_annual_gaap_table():
    annual = sec_row(
        evidence_id="ev-peng-fy26", section="Annual Financial Results",
        content="(in thousands) FY26 FY25",
        table_rows=[
            ["Metric", "FY26", "FY25"],
            ["Advanced Computing", "558,789", "648,417"],
            ["Integrated Memory", "924,001", "464,249"],
            ["Optimized LED", "248,678", "256,128"],
            ["Total net sales", "1,731,468", "1,368,794"],
        ],
    )
    ledger = build_research_ledger({"shared_evidence": [annual]}, "2026-10-09")

    assert ledger.segment_mix("FY26") == {
        "Integrated Memory": Decimal("53.4"),
        "Advanced Computing": Decimal("32.3"),
        "Optimized LED": Decimal("14.4"),
    }


def test_future_and_metadata_only_records_cannot_supply_research_facts():
    ledger = build_research_ledger({"shared_evidence": [
        sec_row(evidence_id="future", published_at="2026-10-10"),
        sec_row(evidence_id="metadata", coverage="metadata_only"),
    ]}, "2026-10-09")
    assert ledger.facts == ()


def test_team_reports_are_interpretation_sources_not_facts():
    state = {"shared_evidence": [], "investment_debate_state": {
        "bull_history": "Growth case", "bear_history": "Cash risk",
    }}
    ledger = build_research_ledger(state, "2026-10-09")

    assert ledger.facts == ()
    assert {row.evidence_id for row in ledger.excerpts["analysis"]} == {
        "analysis:bull", "analysis:bear",
    }


def test_vendor_dollar_unit_is_not_scaled_as_thousands():
    ledger = build_research_ledger({"shared_evidence": [vendor_row()]}, "2026-10-09")
    assert ledger.latest("net_sales").value == Decimal("478713000")


def test_annual_vendor_csv_never_becomes_quarterly_trend_input():
    annual = vendor_row(
        title="Annual income statement",
        content=(
            "# Income Statement data for PENG (annual)\n"
            ",2026-08-31,2025-08-31\n"
            "Total Revenue,1731468000,1368794000"
        ),
    )
    ledger = build_research_ledger({"shared_evidence": [annual]}, "2026-10-09")

    assert ledger.latest("net_sales") is None
    assert all(fact.period_kind == "annual" for fact in ledger.facts_for("net_sales"))


def test_balance_sheet_cash_is_an_instant_not_a_quarter_flow():
    balance = vendor_row(
        source="get_balance_sheet", title="Quarterly balance sheet",
        content=(
            "# Balance Sheet data for PENG (quarterly)\n"
            ",2026-05-31\nCash And Cash Equivalents,440301000\nTotal Debt,502509000"
        ),
    )
    ledger = build_research_ledger({"shared_evidence": [balance]}, "2026-10-09")

    assert {fact.period_kind for fact in ledger.facts} == {"instant"}


def test_split_currency_cell_and_duplicate_non_gaap_headers_stay_separate():
    row = sec_row(table_rows=[
        ["Metric", "Q4-26", "", "Q3-26", "", "Q4-26", ""],
        ["", "GAAP", "", "GAAP", "", "Non-GAAP", ""],
        ["Total net sales", "$", "566,685", "$", "478,713", "$", "600,000"],
    ])
    ledger = build_research_ledger({"shared_evidence": [row]}, "2026-10-09")

    q4 = [fact for fact in ledger.facts_for("net_sales") if fact.period_label == "Q4-26"]
    assert any(f.value == Decimal("566685000") and f.basis == "gaap" for f in q4)
    assert any(f.value == Decimal("600000000") and f.basis == "non_gaap" for f in q4)
    assert ledger.latest("net_sales").value == Decimal("566685000")


def test_adjacent_gaap_and_non_gaap_headers_keep_both_bases():
    row = sec_row(table_rows=[
        ["Metric", "Q4-26", "Q4-26"],
        ["", "GAAP", "Non-GAAP"],
        ["Net income attributable to Penguin Solutions", "93,204", "100,000"],
    ])
    ledger = build_research_ledger({"shared_evidence": [row]}, "2026-10-09")

    values = {(fact.basis, fact.value) for fact in ledger.facts_for("net_income")}
    assert values == {
        ("gaap", Decimal("93204000")),
        ("non_gaap", Decimal("100000000")),
    }


def test_sparse_sec_release_table_pairs_numeric_cells_with_repeated_periods():
    row = sec_row(
        content="Quarterly Financial Results",
        table_rows=[
            ["", "GAAP (1)", "", "Non-GAAP (2)"],
            ["(in thousands, except per share amounts)", "Q4-26", "", "Q3-26", "",
             "Q4-25", "", "Q4-26", "", "Q3-26", "", "Q4-25"],
            ["Total net sales", "$", "566,685", "", "", "$", "478,713", "", "",
             "$", "337,922", "", "", "$", "566,685", "", "", "$", "478,713", ""],
            ["Net income attributable to Penguin Solutions", "93,204", "44,689",
             "9,431", "71,438", "52,246", "28,843"],
        ],
    )

    ledger = build_research_ledger({"shared_evidence": [row]}, "2026-10-09")

    sales = ledger.facts_for("net_sales")
    assert any(f.period_label == "Q4-26" and f.basis == "gaap"
               and f.value == Decimal("566685000") for f in sales)
    assert any(f.period_label == "Q3-26" and f.basis == "gaap"
               and f.value == Decimal("478713000") for f in sales)
    income = ledger.facts_for("net_income")
    assert any(f.period_label == "Q4-26" and f.basis == "non_gaap"
               and f.value == Decimal("71438000") for f in income)


def test_unknown_period_is_not_declared_newer_than_dated_vendor_quarter():
    row = sec_row(table_rows=[["Metric", "Unknown"], ["Total net sales", "999,999"]])
    ledger = build_research_ledger({"shared_evidence": [row, vendor_row()]}, "2026-10-09")

    assert ledger.latest("net_sales").value == Decimal("478713000")


def test_real_exhibit_fragment_table_parses_q4_and_annual_without_reference_deck():
    fixture = Path(__file__).parent / "fixtures" / "sec" / "peng_q4_ex991.html"
    sections = extract_sections(fixture.read_text(encoding="utf-8"), "EX-99.1")
    records = [sec_row(
        evidence_id=f"ev-section-{index}", title=section.name,
        content=section.content, section=section.name,
        table_rows=[list(cells) for cells in section.table_rows],
    ) for index, section in enumerate(sections, 1)]

    ledger = build_research_ledger({"shared_evidence": records}, "2026-10-09")

    assert ledger.latest("net_sales").value == Decimal("566685000")
    assert ledger.latest("diluted_eps").value == Decimal("1.29")
    assert ledger.segment_mix("FY26") == {
        "Integrated Memory": Decimal("53.4"),
        "Advanced Computing": Decimal("32.3"),
        "Optimized LED": Decimal("14.4"),
    }


def test_sec_companyfacts_are_typed_without_turning_ytd_cash_into_a_quarter():
    revenue = sec_row(
        evidence_id="ev-sec-q3-revenue", category="fundamentals",
        coverage="reported_fact", table_rows=[], published_at="2026-07-07",
        content=(
            "Metric: Revenue\nValue: 478713000 USD\n"
            "Period: 2026-02-28 to 2026-05-29\nFiscal year: 2026\n"
            "Fiscal period: Q3\nFiling date: 2026-07-07\nForm: 10-Q\n"
            "Accession number: 0001616533-26-000043\n"
            "Evidence type: SEC companyfacts reported XBRL fact."
        ),
    )
    cash_flow = revenue | {
        "evidence_id": "ev-sec-ytd-ocf",
        "content": revenue["content"].replace(
            "Metric: Revenue\nValue: 478713000 USD\nPeriod: 2026-02-28",
            "Metric: Operating cash flow\nValue: 11222000 USD\nPeriod: 2025-08-30",
        ),
    }

    ledger = build_research_ledger(
        {"shared_evidence": [revenue, cash_flow]}, "2026-10-09"
    )

    q3 = ledger.latest("net_sales")
    assert q3 is not None and q3.value == Decimal("478713000")
    assert q3.period_label == "Q3-26" and q3.period_end == "2026-05-29"
    assert ledger.sources["ev-sec-q3-revenue"].accession == "0001616533-26-000043"
    assert ledger.sources["ev-sec-q3-revenue"].report_period == "2026-05-29"
    ocf = ledger.facts_for("operating_cash_flow")[0]
    assert ocf.period_kind == "ytd"
    assert ocf.period_label == "FY26 Q3 YTD"
    assert ledger.latest("operating_cash_flow") is None
