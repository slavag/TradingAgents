"""Offline reporting: entity isolation, comparable inputs and honest coverage."""

from copy import deepcopy
from decimal import Decimal

from tradingagents.related_companies.presentation import build_related_slides, render_related_report
from tradingagents.research_ledger import ResearchFact, ResearchLedger, ResearchSource


def raw_source(key, name, title="Quarterly results", published="2026-11-10"):
    return {
        "evidence_id": key,
        "entity_name": name,
        "source": "sec",
        "title": title,
        "published_at": published,
        "url": "https://example.test/" + key,
        "status": "available",
    }


def fact(metric, value, source="peer", **changes):
    row = {
        "metric": metric,
        "value": str(value),
        "unit": "USD",
        "period_label": "Q3-26",
        "period_end": "2026-09-30",
        "period_kind": "quarterly",
        "basis": "gaap",
        "source_ids": [source],
    }
    row.update(changes)
    return row


def fixture():
    sources = {
        "main": ResearchSource("main", "sec", "MAIN results", None, "2026-11-10", None, "available")
    }
    ledger = ResearchLedger(
        tuple(
            ResearchFact(
                **{**f, "value": Decimal(f["value"]), "source_ids": tuple(f["source_ids"])}
            )
            for f in [
                fact("net_sales", 100, "main"),
                fact("close", 10, "main", period_kind="instant", basis="market", unit="price"),
            ]
        ),
        sources,
        {},
    )
    peer = {
        "name": "Peer Inc",
        "symbol": "PEER",
        "identity_status": "source_verified",
        "roles": [
            {
                "role": "competitor",
                "quote": "We compete with Peer Inc in cloud services.",
                "source_id": "proof",
                "relationship_date": "2026-11-10",
                "justification": "Cloud services overlap.",
            }
        ],
        "sources": [raw_source("peer", "Peer Inc")],
        "facts": [
            fact("net_sales", 9999),
            fact("close", 900, period_kind="instant", basis="market", unit="price"),
        ],
        "performance": [],
        "news": {"status": "unavailable", "latest_7_days": [], "material_30_days": []},
        "coverage": [],
    }
    bundle = {
        "target": "MAIN",
        "as_of": "2026-11-13",
        "status": "available",
        "entities": [peer],
        "sources": [raw_source("proof", "MAIN", "Competition disclosure")],
        "coverage": [],
    }
    return bundle, ledger


def test_main_and_peer_financials_are_isolated_and_sources_named():
    bundle, ledger = fixture()
    before = deepcopy(ledger)
    slides, sources = build_related_slides(
        {"related_company_research": bundle, "trade_date": bundle["as_of"]}, ledger
    )
    main = next(s for s in slides if s.topic == "related_financial_MAIN")
    peer = next(s for s in slides if s.topic == "related_financial_PEER")
    assert "100 USD" in main.model_dump_json() and "9999" not in main.model_dump_json()
    assert "9999 USD" in peer.model_dump_json() and "900" not in main.model_dump_json()
    comparison = next(s for s in slides if s.topic == "related_financial_comparison")
    rows = comparison.blocks[0].rows
    assert "100 USD" in rows[0].cells[1] and "9999" not in str(rows[0].cells)
    assert "9999 USD" in rows[1].cells[1]
    assert ledger == before
    assert sources["peer"].title.startswith("Related company — Peer Inc:")
    assert all(sid in sources or sid in ledger.sources for s in slides for sid in s.source_ids)
    assert "MAIN" in render_related_report(bundle)


def test_mixed_period_currency_and_future_facts_do_not_produce_ratios():
    bundle, ledger = fixture()
    e = bundle["entities"][0]
    e["facts"] += [
        fact("operating_income", 800, period_kind="ytd"),
        fact("net_income", -5),
        fact("net_sales", 12345, period_end="2026-12-31"),
        fact("net_sales", 5, period_label="Q3-25", period_end="2025-09-30", unit="EUR"),
    ]
    text = render_related_report(bundle)
    assert "12345" not in text and "8.0%" not in text
    assert "| P/E | N/A" in text
    assert "quarterly loss" in text
    assert "| Revenue growth | N/A" in text and "| Operating margin | N/A" in text


def test_counterparty_news_is_dated_conditional_and_clean_distinct_from_unavailable():
    bundle, _ = fixture()
    e = bundle["entities"][0]
    e["roles"] += [
        {
            "role": "supplier",
            "quote": "Peer Inc supplies cloud services to us.",
            "source_id": "proof",
            "relationship_date": "2026-11-10",
        },
        {
            "role": "customer",
            "quote": "Our customer is Peer Inc.",
            "source_id": "proof",
            "relationship_date": "2026-11-10",
        },
    ]
    e["sources"].append(raw_source("news", "Peer Inc", "Capacity outage", "2026-11-12"))
    e["news"] = {"status": "available", "latest_7_days": ["news"], "material_30_days": ["news"]}
    text = render_related_report(bundle)
    assert "## Suppliers" in text and "## Customers" in text
    assert "Capacity outage" in text and "https://example.test/news" in text
    assert "conditional" in text and "materiality unverified" in text
    assert "disclosed as of 2026-11-10" in text
    e["news"] = {"status": "clean", "latest_7_days": [], "material_30_days": []}
    assert "No reported news in available coverage" in render_related_report(bundle)
    e["news"]["status"] = "unavailable"
    assert "News coverage unavailable" in render_related_report(bundle)


def test_private_peer_stays_qualitative_and_performance_requires_comparability():
    bundle, ledger = fixture()
    e = bundle["entities"][0]
    e.update(symbol=None, identity_status="unresolved")
    text = render_related_report(bundle)
    assert "9999" not in text and "private or unresolved" in text
    e.update(symbol="PEER", identity_status="source_verified")
    e["performance"] = [
        {
            "months": 1,
            "return_pct": 20,
            "start_date": "2026-10-13",
            "end_date": "2026-11-13",
            "currency": "USD",
            "adjustment": "adjusted close",
            "source": "load_ohlcv",
        }
    ]
    slides, sources = build_related_slides(
        {"related_company_research": bundle, "trade_date": bundle["as_of"]}, ledger
    )
    performance = next(s for s in slides if s.topic == "related_performance")
    assert "MAIN" in performance.model_dump_json() and "20%" in performance.model_dump_json()
    assert "N/A" in performance.model_dump_json()
    assert any(s.provider == "load_ohlcv" for s in sources.values())
    bundle["primary_performance"] = [dict(e["performance"][0], adjustment="unadjusted close")]
    assert "incompatible adjustment" in render_related_report(bundle)


def test_markdown_primary_row_uses_explicit_ledger_and_duplicate_conflicts_fail_closed():
    bundle, ledger = fixture()
    report = render_related_report(bundle, ledger)
    main = report.split("## Business and financial comparison — MAIN")[1].split("## ")[0]
    peer = report.split("## Business and financial comparison — Peer Inc")[1].split("## ")[0]
    assert "100 USD" in main and "9999" not in main
    assert "9999 USD" in peer
    bundle["entities"][0]["facts"].append(fact("net_sales", 1111))
    report = render_related_report(bundle, ledger)
    peer = report.split("## Business and financial comparison — Peer Inc")[1].split("## ")[0]
    assert "| Revenue | N/A" in peer


def test_sources_and_performance_after_cutoff_are_rejected_without_mutation():
    bundle, ledger = fixture()
    peer = bundle["entities"][0]
    peer["sources"].append(raw_source("future-news", "Peer Inc", "Future expansion", "2026-11-20"))
    peer["news"] = {
        "status": "available",
        "latest_7_days": ["future-news"],
        "material_30_days": ["future-news"],
    }
    peer["performance"] = [
        {
            "months": 1,
            "return_pct": 999,
            "start_date": "2026-10-13",
            "end_date": "2026-11-20",
            "currency": "USD",
            "adjustment": "adjusted",
            "source": "load_ohlcv",
        }
    ]
    before = deepcopy(bundle)
    slides, sources = build_related_slides(
        {"related_company_research": bundle, "trade_date": bundle["as_of"]}, ledger
    )
    assert "future-news" not in sources
    assert "Future expansion" not in render_related_report(bundle)
    assert not any("999%" in slide.model_dump_json() for slide in slides)
    assert bundle == before
    assert build_related_slides(
        {"related_company_research": bundle, "trade_date": "2026-11-14"}, ledger
    ) == ([], {})


def test_compatible_quarterly_metrics_are_computed_without_using_ytd():
    bundle, ledger = fixture()
    peer = bundle["entities"][0]
    peer["facts"] = [
        fact("net_sales", 200),
        fact("net_sales", 100, period_end="2025-09-30", period_label="Q3-25"),
        fact("operating_income", 30),
        fact("operating_income", 1000, period_kind="ytd"),
    ]
    report = render_related_report(bundle, ledger)
    assert "100.0% YoY" in report and "15.0%; quarter ended" in report
    peer["facts"][2]["unit"] = "EUR"
    assert "| Operating margin | N/A" in render_related_report(bundle, ledger)


def test_large_news_sets_continue_without_clipping_quotes_or_source_references():
    bundle, ledger = fixture()
    peer = bundle["entities"][0]
    quote = "Peer Inc supplies cloud services to us, including the complete disclosed qualification about service scope and contractual limits."
    peer["roles"].append(
        {
            "role": "supplier",
            "quote": quote,
            "source_id": "proof",
            "relationship_date": "2026-11-10",
        }
    )
    keys = []
    for index in range(18):
        key = f"news-{index}"
        keys.append(key)
        peer["sources"].append(
            raw_source(
                key,
                "Peer Inc",
                f"Dated operational statement {index}: complete headline covering cloud capacity and service availability",
                "2026-11-12",
            )
        )
    peer["news"] = {"status": "available", "latest_7_days": keys, "material_30_days": keys}
    slides, sources = build_related_slides(
        {"related_company_research": bundle, "trade_date": bundle["as_of"]}, ledger
    )
    assert any("related_supplier_PEER_continuation" in slide.topic for slide in slides)
    assert quote in "\n".join(slide.model_dump_json() for slide in slides)
    assert all(
        key in sources or key in ledger.sources for slide in slides for key in slide.source_ids
    )
    assert all(key in "\n".join(slide.model_dump_json() for slide in slides) for key in keys)
    assert "..." not in render_related_report(bundle, ledger)


def test_disabled_research_does_not_add_sections():
    bundle, ledger = fixture()
    bundle["status"] = "disabled"
    assert render_related_report(bundle, ledger) == ""
    assert build_related_slides({"related_company_research": bundle}, ledger) == ([], {})


def test_quarterly_loss_does_not_override_positive_vendor_reported_ttm_pe():
    bundle, ledger = fixture()
    peer = bundle["entities"][0]
    peer["facts"] += [
        fact("net_income", -5),
        fact(
            "pe_ttm",
            25,
            unit="multiple",
            period_kind="ttm",
            basis="market",
            period_label="TTM",
            period_end="2026-11-10",
        ),
    ]
    text = render_related_report(bundle, ledger)
    detail = text.split("## Business and financial comparison — Peer Inc")[1].split("## ")[0]
    assert "| P/E | 25x;" in detail
    assert "vendor-reported TTM" in detail and "snapshot 2026-11-10" in detail
    assert "| P/E | N/M" not in detail


def test_quarterly_loss_without_ttm_information_is_a_coverage_gap():
    bundle, ledger = fixture()
    bundle["entities"][0]["facts"].append(fact("net_income", -5))
    text = render_related_report(bundle, ledger)
    detail = text.split("## Business and financial comparison — Peer Inc")[1].split("## ")[0]
    assert "| P/E | N/A" in detail
    assert "TTM coverage unavailable" in detail and "quarterly loss" in detail
    assert "N/M" not in detail


def test_nonpositive_ttm_eps_or_negative_reported_ttm_multiple_is_not_meaningful():
    for metric, value, unit, basis in [
        ("diluted_eps", -1, "USD_per_share", "gaap"),
        ("diluted_eps", 0, "USD_per_share", "gaap"),
        ("pe_ttm", -20, "multiple", "market"),
    ]:
        bundle, ledger = fixture()
        bundle["entities"][0]["facts"].append(
            fact(
                metric,
                value,
                unit=unit,
                period_kind="ttm",
                basis=basis,
                period_end="2026-11-10",
                period_label="TTM",
            )
        )
        detail = (
            render_related_report(bundle, ledger)
            .split("## Business and financial comparison — Peer Inc")[1]
            .split("## ")[0]
        )
        assert "| P/E | N/M" in detail
        assert "TTM" in detail


def test_own_ttm_multiple_requires_compatible_currency_and_future_vendor_multiple_is_rejected():
    bundle, ledger = fixture()
    peer = bundle["entities"][0]
    peer["facts"] = [
        fact("diluted_eps", 2, unit="USD_per_share", period_kind="ttm"),
        fact("close", 30, unit="USD", period_kind="instant", basis="market"),
    ]
    detail = (
        render_related_report(bundle, ledger)
        .split("## Business and financial comparison — Peer Inc")[1]
        .split("## ")[0]
    )
    assert "| P/E | 15.0x;" in detail
    assert "computed from compatible-currency inputs" in detail
    peer["facts"][1]["unit"] = "EUR"
    peer["facts"].append(
        fact(
            "pe_ttm",
            9999,
            unit="multiple",
            period_kind="ttm",
            basis="market",
            period_end="2026-11-20",
        )
    )
    detail = (
        render_related_report(bundle, ledger)
        .split("## Business and financial comparison — Peer Inc")[1]
        .split("## ")[0]
    )
    assert "| P/E | N/A" in detail and "9999x" not in detail


def test_unverified_discovery_justification_cannot_be_published_as_sourced_overlap():
    bundle, ledger = fixture()
    proof = bundle["entities"][0]["roles"][0]
    proof["justification"] = (
        "Peer Inc has 99% market share and will eliminate MAIN within six months."
    )
    slides, _ = build_related_slides(
        {"related_company_research": bundle, "trade_date": bundle["as_of"]}, ledger
    )
    markdown = render_related_report(bundle, ledger)
    deck = "\n".join(slide.model_dump_json() for slide in slides)
    assert "99%" not in markdown and "99%" not in deck
    assert "eliminate MAIN" not in markdown and "eliminate MAIN" not in deck
    assert proof["quote"] in markdown and proof["quote"] in deck
    assert proof["source_id"] in markdown and proof["source_id"] in deck
    assert proof["relationship_date"] in markdown and proof["relationship_date"] in deck


def test_missing_named_counterparties_do_not_create_placeholder_slides():
    bundle, ledger = fixture()
    before = deepcopy(bundle)
    slides, _ = build_related_slides(
        {"related_company_research": bundle, "trade_date": bundle["as_of"]}, ledger
    )
    assert not any(s.topic in {"related_supplier", "related_customer"} for s in slides)
    assert not any("Named relationship coverage unavailable" in s.takeaway for s in slides)
    # Detailed research still records missing coverage for audit and decision inputs.
    assert "Named relationship coverage unavailable" in render_related_report(bundle, ledger)
    assert bundle == before


def test_verified_counterparty_slide_remains_when_other_role_has_no_data():
    bundle, ledger = fixture()
    bundle['entities'][0]['roles'].append({
        'role': 'supplier', 'source_id': 'proof',
        'quote': 'Peer Inc supplies cloud services to MAIN.',
        'relationship_date': '2026-11-10',
    })
    slides, _ = build_related_slides(
        {"related_company_research": bundle, "trade_date": bundle["as_of"]}, ledger
    )
    assert any(s.topic == 'related_supplier_PEER' for s in slides)
    assert not any(s.topic == 'related_customer' for s in slides)
