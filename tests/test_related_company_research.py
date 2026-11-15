from tradingagents.related_companies import (
    create_related_company_researcher,
    render_related_context,
)
from tradingagents.related_companies.schemas import RelatedDiscovery


class FakeModel:
    def __init__(self, payload):
        self.payload = payload
        self.calls = 0

    def with_structured_output(self, schema):
        assert schema is RelatedDiscovery
        return self

    def invoke(self, prompt):
        self.calls += 1
        return RelatedDiscovery.model_validate(self.payload)


def evidence(text, **extra):
    return dict(
        evidence_id="ev-proof",
        source="sec",
        category="business",
        title="Business",
        content=text,
        status="available",
        coverage="full",
        as_of_date="2024-01-31",
        published_at="2024-01-20",
        **extra,
    )


def candidate(
    name="Peer Corp",
    symbol=None,
    role="competitor",
    quote="Peer Corp competes directly with us in satellite services.",
):
    return {
        "name": name,
        "symbol": symbol,
        "proofs": [
            {
                "role": role,
                "quote": quote,
                "source_id": "ev-proof",
                "justification": "Satellite service overlap",
                "relationship_date": "2024-01-20",
            }
        ],
    }


def run(payload, rows):
    model = FakeModel(payload)
    result = create_related_company_researcher(model)(
        {"company_of_interest": "TARGET", "trade_date": "2024-01-31", "shared_evidence": rows}
    )["related_company_research"]
    return result, model


def test_missing_admitted_evidence_skips_model():
    result, model = run({"candidates": []}, [])
    assert model.calls == 0
    assert result["coverage"]


def test_exact_direct_competition_quote_is_retained_without_inventing_symbol():
    c = candidate()
    result, model = run({"candidates": [c]}, [evidence(c["proofs"][0]["quote"])])
    assert model.calls == 1
    assert result["entities"][0]["symbol"] is None
    assert result["entities"][0]["roles"][0]["role"] == "competitor"
    assert "Peer Corp" in render_related_context(result)


def test_invented_or_sector_only_or_accounting_relationship_rejected():
    for c, text in [
        (candidate(), "Other text."),
        (candidate(quote="Peer Corp operates in our sector."), "Peer Corp operates in our sector."),
        (
            candidate(
                name="Customer warrant assets",
                role="customer",
                quote="Customer warrant assets were recorded.",
            ),
            "Customer warrant assets were recorded.",
        ),
    ]:
        result, _ = run({"candidates": [c]}, [evidence(text)])
        assert not result["entities"]


def test_historical_symbol_needs_source_verification_and_never_live_identity(monkeypatch):
    from tradingagents.related_companies import providers

    monkeypatch.setattr(
        providers,
        "resolve_instrument_identity",
        lambda *a: (_ for _ in ()).throw(AssertionError("live identity")),
    )
    c = candidate(symbol="FAKE")
    result, _ = run({"candidates": [c]}, [evidence(c["proofs"][0]["quote"])])
    assert result["entities"][0]["symbol"] is None


def test_multiple_proved_roles_preserved_and_future_proof_rejected():
    c = candidate()
    c["proofs"].append(
        {
            "role": "customer",
            "quote": "Peer Corp is our customer.",
            "source_id": "ev-proof",
            "justification": "buys services",
            "relationship_date": "2024-01-20",
        }
    )
    text = "Peer Corp competes directly with us in satellite services. Peer Corp is our customer."
    result, _ = run({"candidates": [c]}, [evidence(text)])
    assert len(result["entities"][0]["roles"]) == 2
    c["proofs"][0]["relationship_date"] = "2025-01-01"
    result, _ = run({"candidates": [c]}, [evidence(text)])
    assert [p["role"] for p in result["entities"][0]["roles"]] == ["customer"]


def test_counterparty_quote_must_establish_role_relative_to_target():
    c = candidate(role="customer", quote="Peer Corp serves many customers in Europe.")
    result, _ = run({"candidates": [c]}, [evidence(c["proofs"][0]["quote"])])
    assert not result["entities"]


def test_dated_vendor_news_normalization_rejects_future_and_untagged():
    from tradingagents.related_companies.providers import _records

    payload = {
        "feed": [
            {
                "title": "Peer expands",
                "summary": "Opening a plant.",
                "time_published": "20240129T120000",
                "url": "https://example.test/a",
                "ticker_sentiment": [{"ticker": "PEER"}],
            },
            {
                "title": "Wrong issuer",
                "time_published": "20240129T120000",
                "ticker_sentiment": [{"ticker": "OTHER"}],
            },
            {
                "title": "Future",
                "time_published": "20240201T120000",
                "ticker_sentiment": [{"ticker": "PEER"}],
            },
        ]
    }
    rows = _records(payload, "get_news", "PEER", "2024-01-31")
    assert [r["title"] for r in rows] == ["Peer expands"]


def test_historical_proved_symbol_retains_disclosure_without_current_symbol_retrieval(monkeypatch):
    from tradingagents.related_companies import providers

    calls = []

    def transport(method, *args):
        calls.append((method, args))
        if method == "get_news":
            return [
                {
                    "source": "wire",
                    "category": "news",
                    "title": "Peer expands",
                    "content": "Peer Corp opens a plant.",
                    "published_at": "2024-01-29",
                    "status": "available",
                }
            ]
        return [
            {
                "source": "sec",
                "category": "business",
                "title": "Peer filing",
                "content": "Peer Corp operates satellites.",
                "published_at": "2024-01-20",
                "status": "available",
            }
        ]

    monkeypatch.setattr(providers, "route_to_vendor", transport)
    monkeypatch.setattr(
        providers,
        "load_ohlcv",
        lambda *a: (_ for _ in ()).throw(AssertionError("historical current-adjusted OHLCV")),
    )
    providers._CACHE.clear()
    c = candidate(symbol="PEER")
    text = c["proofs"][0]["quote"] + " Peer Corp (NASDAQ: PEER)."
    result, _ = run({"candidates": [c]}, [evidence(text)])
    entity = result["entities"][0]
    assert entity["symbol"] == "PEER"
    assert not calls
    assert entity["news"]["status"] == "unavailable"
    assert all(r["entity_symbol"] == "PEER" for r in entity["sources"])
    # Transport and ledger products are per entity, never target shared_evidence.
    assert all(r["evidence_id"] == "ev-proof" for r in result["sources"])
    run({"candidates": [c]}, [evidence(text)])
    assert not calls


def test_role_limits_and_target_exclusion():
    candidates = []
    quotes = []
    for i in range(12):
        c = candidate(
            name=f"Peer {i} Corp",
            quote=f"Peer {i} Corp competes directly with us in satellite services.",
        )
        candidates.append(c)
        quotes.append(c["proofs"][0]["quote"])
    result, _ = run({"candidates": candidates}, [evidence(" ".join(quotes))])
    assert len(result["entities"]) == 3


def test_fail_open_and_strict_one_invocation():
    model = FakeModel({"candidates": []})

    def fail(prompt):
        model.calls += 1
        raise RuntimeError("secret provider credentials should not escape")

    model.invoke = fail
    result = create_related_company_researcher(model)(
        {
            "company_of_interest": "TARGET",
            "trade_date": "2024-01-31",
            "shared_evidence": [evidence("Our suppliers include Peer Corp.")],
        }
    )
    assert model.calls == 1
    assert "secret" not in str(result)
    assert result["related_company_research"]["status"] == "partial"


def test_news_yahoo_string_preserves_dates_and_links():
    from tradingagents.related_companies.providers import _records

    text = "## PEER News\n\n### Plant opened (published: 2024-01-29; source: Newswire)\nPeer opened a plant.\nLink: https://example.test/plant\n\n"
    rows = _records(text, "get_news", "PEER", "2024-01-31")
    assert rows[0]["published_at"] == "2024-01-29"
    assert rows[0]["url"] == "https://example.test/plant"


def test_admitted_sources_scope_and_cutoff():
    from tradingagents.related_companies import admitted_related_sources

    c = candidate()
    bundle, _ = run({"candidates": [c]}, [evidence(c["proofs"][0]["quote"])])
    assert (
        admitted_related_sources({"related_company_research": bundle}, "2024-01-31")[0][
            "evidence_id"
        ]
        == "ev-proof"
    )
    assert admitted_related_sources({"related_company_research": bundle}, "2025-01-31") == []


def test_live_returns_all_horizons_financials_and_failures_are_scoped(monkeypatch):
    from datetime import date

    import pandas as pd

    from tradingagents.related_companies import providers

    as_of = date.today().isoformat()
    providers._CACHE.clear()
    dates = pd.date_range(
        pd.Timestamp(as_of) - pd.DateOffset(months=13), pd.Timestamp(as_of) + pd.Timedelta(days=2)
    )
    monkeypatch.setattr(
        providers,
        "load_ohlcv",
        lambda *a: pd.DataFrame({"Date": dates, "Close": [100 + i for i in range(len(dates))]}),
    )

    def transport(method, *args):
        if method == "get_news":
            return {"status": "clean", "as_of_date": as_of}
        if method == "get_company_filings":
            raise RuntimeError("provider failure")
        if method == "get_income_statement":
            return ",2025-12-31\nTotal Revenue,478713000"
        return "DATA_UNAVAILABLE: no fundamentals"

    monkeypatch.setattr(providers, "route_to_vendor", transport)
    entity = {"name": "Peer Corp", "symbol": "PEER", "roles": [{"role": "competitor"}]}
    result = providers.collect_entity(
        entity, as_of, {"quote_currency": "USD", "financial_currency": "USD"}
    )
    assert len(result["performance"]) == 4
    assert all(r["end_date"] == as_of and r["currency"] == "USD" for r in result["performance"])
    assert result["news"]["status"] == "clean"
    assert result["facts"][0]["metric"] == "net_sales"
    assert all("entity_symbol" in r for r in result["sources"])
    assert any("retrieval unavailable" in gap for gap in result["coverage"])
    assert all("provider failure" not in gap for gap in result["coverage"])


def test_admission_rejects_unrelated_competition_and_customer_clauses():
    cases = [
        (
            "competitor",
            "Peer Corp sells satellite services to consumers; we compete in broadband markets.",
        ),
        (
            "competitor",
            "Peer Corp sells satellite services to consumers. We compete in broadband markets.",
        ),
        ("customer", "Our customers include Globex. Peer Corp serves consumers."),
        ("customer", "Our customer Globex competes with Peer Corp in satellite services."),
    ]
    for role, quote in cases:
        c = candidate(role=role, quote=quote)
        result, _ = run({"candidates": [c]}, [evidence(quote)])
        assert not result["entities"], quote


def test_competitor_and_counterparty_lists_remain_supported():
    for role, quote in [
        ("competitor", "We compete with Globex and Peer Corp in satellite services."),
        ("competitor", "Our satellite service competitors include Globex and Peer Corp."),
        ("customer", "Our customers include Globex and Peer Corp."),
        ("supplier", "Our suppliers include Globex and Peer Corp."),
    ]:
        c = candidate(role=role, quote=quote)
        result, _ = run({"candidates": [c]}, [evidence(quote)])
        assert len(result["entities"]) == 1, quote


def test_source_ticker_pairing_requires_exact_entity_boundary():
    from tradingagents.related_companies.providers import verify_symbol

    symbol, status, _ = verify_symbol(
        "Peer Corp",
        "OTHER",
        [
            {
                "content": "Peer Corp competes with us in satellite services. NotPeer Corp (NASDAQ: OTHER)."
            }
        ],
        "2024-01-31",
    )
    assert symbol is None


def test_source_verified_same_symbol_aliases_merge_without_losing_roles(monkeypatch):
    from tradingagents.related_companies import providers

    providers._CACHE.clear()
    monkeypatch.setattr(providers, "route_to_vendor", lambda *a: [])
    first = candidate(name="Peer Corp", symbol="PEER")
    second = candidate(
        name="Peer Corporation",
        symbol="PEER",
        role="customer",
        quote="Peer Corporation is our customer.",
    )
    text = (
        first["proofs"][0]["quote"]
        + " Peer Corp (PEER). "
        + second["proofs"][0]["quote"]
        + " Peer Corporation (PEER)."
    )
    result, _ = run({"candidates": [first, second]}, [evidence(text)])
    assert len(result["entities"]) == 1
    assert {p["role"] for p in result["entities"][0]["roles"]} == {"competitor", "customer"}


def test_relationship_date_is_disclosure_date_not_model_invented_history():
    c = candidate()
    c["proofs"][0]["relationship_date"] = "2001-01-01"
    result, _ = run({"candidates": [c]}, [evidence(c["proofs"][0]["quote"])])
    proof = result["entities"][0]["roles"][0]
    assert proof["relationship_date"] == "2024-01-20"
    assert proof["date_semantics"] == "disclosed_as_of"


def test_secondary_and_social_sources_cannot_confirm_relationships():
    c = candidate()
    for source, category in [("reddit", "social"), ("wire", "news"), ("fmp", "news")]:
        row = evidence(c["proofs"][0]["quote"]) | {"source": source, "category": category}
        result, _ = run({"candidates": [c]}, [row])
        assert not result["entities"]


def test_collection_cache_uses_verified_symbol_across_source_aliases(monkeypatch):
    from datetime import date

    from tradingagents.related_companies import providers

    providers._CACHE.clear()
    calls = []
    monkeypatch.setattr(providers, "route_to_vendor", lambda *a: calls.append(a) or [])
    base = {"symbol": "PEER", "roles": [{"role": "customer"}]}
    providers.collect_entity(base | {"name": "Peer Corp"}, date.today().isoformat())
    providers.collect_entity(base | {"name": "Peer Corporation"}, date.today().isoformat())
    assert len(calls) == 1


def test_discovery_packet_packs_complete_records_and_reports_omitted_sources():
    import json

    from tradingagents.related_companies.research import _discovery_packet

    rows = [
        evidence("First full disclosure. " + "A" * 22000) | {"evidence_id": "ev-first"},
        evidence("Second full disclosure. " + "B" * 22000) | {"evidence_id": "ev-second"},
        evidence("Decisive tail relationship: Peer Corp is our customer. " + "C" * 22000)
        | {"evidence_id": "ev-tail"},
    ]
    packet, admitted, omitted = _discovery_packet(rows, "TARGET", "2024-01-31")
    parsed = json.loads(packet)
    assert len(packet) <= 58000
    assert [r["evidence_id"] for r in parsed["sources"]] == ["ev-first", "ev-second"]
    assert parsed["sources"][1]["content"] == rows[1]["content"]
    assert "Decisive tail relationship" not in packet
    assert admitted == rows[:2]
    assert omitted == 1
    assert "1 source record(s) omitted" in parsed["coverage_note"]


def test_competitor_list_owned_by_customer_is_not_target_competition():
    quote = "Our customer Globex's satellite service competitors include Peer Corp."
    c = candidate(quote=quote)
    result, _ = run({"candidates": [c]}, [evidence(quote)])
    assert not result["entities"]


def test_wrong_owner_comma_and_relative_clauses_are_not_relationships():
    from tradingagents.related_companies.research import _relative_role

    for role, quote in [
        ("supplier", "Beta supplies Alpha, which provides services to us."),
        ("competitor", "We compete with Globex in satellite services, but Beta is a customer."),
        ("supplier", "Our suppliers include Alpha, and our customers include Beta."),
        ("competitor", "Our satellite competitors include Alpha, and our suppliers include Beta."),
        ("customer", "We sell products to Alpha, which supplies products to Beta."),
        ("customer", "We sell products to Alpha who supplies products to Beta."),
        ("customer", "We sell products to Alpha that supplies products to Beta."),
    ]:
        assert not _relative_role("Beta", role, quote, "TARGET"), quote
    for role, quote in [
        ("supplier", "Our suppliers include Alpha, Beta and Gamma."),
        ("competitor", "We compete with Alpha, Beta and Gamma in satellite services."),
        ("customer", "Our customers include Alpha, Beta and Gamma."),
    ]:
        assert _relative_role("Beta", role, quote, "TARGET"), quote


def test_context_budget_reserves_actual_dated_news_for_ninth_entity():
    entities = []
    for index in range(9):
        key = f"news-{index}"
        entities.append(
            {
                "name": f"Company {index}",
                "symbol": None,
                "roles": [
                    {
                        "role": "customer",
                        "relationship_date": "2024-01-20",
                        "source_id": f"proof-{index}",
                        "quote": "Our customers include this company. "
                        + ("Long relationship detail. " * 150),
                    }
                ],
                "facts": [
                    {
                        "metric": f"metric-{n}",
                        "value": "100",
                        "unit": "USD",
                        "period_label": "2023",
                        "basis": "gaap",
                        "period_kind": "annual",
                        "source_ids": [f"fact-{index}-{n}"],
                    }
                    for n in range(40)
                ],
                "sources": [
                    {
                        "evidence_id": key,
                        "category": "news",
                        "title": f"Company {index} factory closure",
                        "published_at": "2024-01-29",
                        "content": f"Company {index} closed its factory. No target impact is established.",
                        "source": "wire",
                    }
                ],
                "news": {"status": "available", "latest_7_days": [key], "material_30_days": [key]},
                "coverage": [],
            }
        )
    context = render_related_context(
        {"target": "TARGET", "as_of": "2024-01-31", "entities": entities, "coverage": []},
        max_chars=16000,
    )
    assert len(context) <= 16000
    for index in range(9):
        assert f"Company {index} factory closure" in context
        assert f"Company {index} closed its factory." in context
        assert f"[news-{index}]" in context
        assert f"[proof-{index}]" in context
    assert context.endswith("bundle for audit and full reporting.")


def test_historical_entity_never_uses_actual_sec_current_ticker_index(monkeypatch):
    from tradingagents.dataflows import sec
    from tradingagents.related_companies import providers

    requests = []
    monkeypatch.setattr(sec, "_user_agent", lambda: "Offline test test@example.test")

    def current_index(url, user_agent):
        requests.append(url)
        if url == sec.SEC_TICKERS_URL:
            return {"0": {"ticker": "PEER", "cik_str": 999, "title": "Other New Issuer"}}
        return {}

    monkeypatch.setattr(sec, "_request_json", current_index)
    monkeypatch.setattr(
        providers,
        "route_to_vendor",
        lambda method, *args: (
            sec.get_company_filings(*args) if method == "get_company_filings" else []
        ),
    )
    providers._CACHE.clear()
    result = providers.collect_entity(
        {"name": "Historical Peer Corp", "symbol": "PEER", "roles": [{"role": "competitor"}]},
        "2024-01-31",
    )
    assert not requests
    assert not result["sources"] and not result["facts"] and not result["performance"]
    assert result["news"]["status"] == "unavailable"
    assert any("stable as-of issuer identity" in gap for gap in result["coverage"])


def test_ambiguous_multi_predicate_clauses_are_not_direct_edges():
    from tradingagents.related_companies.research import _relative_role

    for role, quote in [
        ("supplier", "Beta supplies Alpha and Alpha provides services to us."),
        ("customer", "We sell products to Alpha and Alpha sells products to Beta."),
        ("competitor", "We compete with Alpha in satellite services and Beta is a customer."),
        ("supplier", "Beta supplies Alpha, a company providing services to us."),
    ]:
        assert not _relative_role("Beta", role, quote, "TARGET"), quote


def test_competitor_overlap_is_substantive_not_temporal_or_legal():
    for ending in ["in 2024", "in January", "in court"]:
        c = candidate(quote=f"Peer Corp competes directly with us {ending}.")
        result, _ = run({"candidates": [c]}, [evidence(c["proofs"][0]["quote"])])
        assert not result["entities"], ending
    c = candidate(quote="Peer Corp competes directly with us for enterprise inference workloads.")
    result, _ = run({"candidates": [c]}, [evidence(c["proofs"][0]["quote"])])
    assert result["entities"]


def test_live_entity_cache_expires_within_five_minutes(monkeypatch):
    from datetime import date

    from tradingagents.related_companies import providers

    providers._CACHE.clear()
    clock = [1000.0]
    monkeypatch.setattr(providers, "monotonic", lambda: clock[0])
    calls = []
    monkeypatch.setattr(providers, "route_to_vendor", lambda *a: calls.append(a) or [])
    entity = {"name": "Peer Corp", "symbol": "PEER", "roles": [{"role": "customer"}]}
    providers.collect_entity(entity, date.today().isoformat())
    clock[0] += 299
    providers.collect_entity(entity, date.today().isoformat())
    assert len(calls) == 1
    clock[0] += 2
    providers.collect_entity(entity, date.today().isoformat())
    assert len(calls) == 2


def test_peer_statement_uses_reporting_currency_not_quote_currency(monkeypatch):
    from datetime import date

    from tradingagents.related_companies import providers

    providers._CACHE.clear()
    monkeypatch.setattr(
        providers,
        "route_to_vendor",
        lambda method, *args: (
            ",2025-12-31\nTotal Revenue,777000000" if method == "get_income_statement" else []
        ),
    )
    monkeypatch.setattr(
        providers, "load_ohlcv", lambda *args: (_ for _ in ()).throw(RuntimeError("offline"))
    )
    entity = {"name": "Canadian Peer", "symbol": "PEER", "roles": [{"role": "competitor"}]}
    result = providers.collect_entity(
        entity, date.today().isoformat(), {"quote_currency": "HKD", "financial_currency": "CAD"}
    )
    assert result["facts"][0]["unit"] == "CAD"
    assert result["facts"][0]["value"] == "777000000"


def test_unknown_reporting_currency_omits_monetary_fact_but_preserves_raw_source(monkeypatch):
    from datetime import date

    from tradingagents.related_companies import providers

    providers._CACHE.clear()
    monkeypatch.setattr(
        providers,
        "route_to_vendor",
        lambda method, *args: (
            ",2025-12-31\nTotal Revenue,777000000" if method == "get_income_statement" else []
        ),
    )
    monkeypatch.setattr(
        providers, "load_ohlcv", lambda *args: (_ for _ in ()).throw(RuntimeError("offline"))
    )
    entity = {"name": "Unknown Peer", "symbol": "PEER", "roles": [{"role": "competitor"}]}
    result = providers.collect_entity(entity, date.today().isoformat(), {"quote_currency": "USD"})
    assert not result["facts"]
    assert any("777000000" in s["content"] for s in result["sources"])
    assert any("reporting currency" in gap for gap in result["coverage"])


def test_sec_table_currency_requires_agreement_and_keeps_dimensionless_metrics(monkeypatch):
    from datetime import date

    from tradingagents.related_companies import providers

    table = {
        "source": "sec",
        "category": "earnings_release",
        "title": "Issuer annual results",
        "content": "(in thousands of CAD) FY25 FY24",
        "published_at": "2026-01-20",
        "report_period": "2025-12-31",
        "status": "available",
        "table_rows": [["Metric", "FY25", "FY24"], ["Total net sales", "100", "90"]],
    }

    def transport(method, *args):
        if method == "get_company_filings":
            return [table]
        if method == "get_fundamentals":
            return "PE Ratio (TTM): 5"
        return []

    monkeypatch.setattr(providers, "route_to_vendor", transport)
    monkeypatch.setattr(
        providers, "load_ohlcv", lambda *args: (_ for _ in ()).throw(RuntimeError("offline"))
    )
    entity = {"name": "Canadian Peer", "symbol": "PEER", "roles": [{"role": "competitor"}]}
    for currency, expect_sales in [("CAD", True), ("USD", False), ("", False)]:
        providers._CACHE.clear()
        result = providers.collect_entity(
            entity,
            date.today().isoformat(),
            {"quote_currency": "HKD", "financial_currency": currency},
        )
        sales = [f for f in result["facts"] if f["metric"] == "net_sales"]
        assert bool(sales) is expect_sales
        assert all(f["unit"] == "CAD" for f in sales)
        assert any(f["metric"] == "pe_ttm" and f["unit"] == "multiple" for f in result["facts"])


def test_unverified_model_coverage_notes_do_not_become_decision_evidence():
    c = candidate()
    claim = "TARGET lost a customer order worth $999 million."
    result, _ = run({"candidates": [c], "coverage": [claim]}, [evidence(c["proofs"][0]["quote"])])
    assert claim not in render_related_context(result)
    assert all(claim not in gap for gap in result["coverage"])


def test_market_cap_major_currency_matches_minor_quote_without_conversion():
    from decimal import Decimal

    from tradingagents.related_companies.providers import _verified_fact_unit
    from tradingagents.research_ledger import ResearchFact

    fact = ResearchFact(
        "market_cap",
        Decimal("12000000"),
        "GBP",
        "2026-11-14",
        "2026-11-14",
        "instant",
        "market",
        ("cap",),
    )
    sources = {"cap": {"source": "get_fundamentals", "content": "Market Cap: 12000000 GBP"}}
    assert _verified_fact_unit(fact, sources, {"quote_currency": "GBp"}) == "GBP"
    assert fact.value == Decimal("12000000")
    assert _verified_fact_unit(fact, sources, {"quote_currency": "USD"}) is None


def test_live_gbp_market_cap_keeps_major_unit_and_source_value(monkeypatch):
    from datetime import date

    from tradingagents.related_companies import providers

    providers._CACHE.clear()
    monkeypatch.setattr(
        providers,
        "route_to_vendor",
        lambda method, *args: "Market Cap: 12000000 GBP" if method == "get_fundamentals" else [],
    )
    monkeypatch.setattr(
        providers, "load_ohlcv", lambda *args: (_ for _ in ()).throw(RuntimeError("offline"))
    )
    entity = {"name": "British Peer", "symbol": "PEER", "roles": [{"role": "competitor"}]}
    result = providers.collect_entity(
        entity, date.today().isoformat(), {"quote_currency": "GBp", "financial_currency": "USD"}
    )
    cap = next(f for f in result["facts"] if f["metric"] == "market_cap")
    assert (cap["value"], cap["unit"]) == ("12000000", "GBP")


def test_live_old_symbol_proof_without_current_identity_never_imports_reassigned_issuer(
    monkeypatch,
):
    from datetime import date

    from tradingagents.dataflows import sec
    from tradingagents.related_companies import providers

    today = date.today().isoformat()
    calls = []
    requests = []
    monkeypatch.setattr(providers, "resolve_instrument_identity", lambda symbol: {})
    monkeypatch.setattr(sec, "_user_agent", lambda: "Offline test test@example.test")

    def current_index(url, user_agent):
        requests.append(url)
        return (
            {"0": {"ticker": "PEER", "cik_str": 999, "title": "Other New Issuer"}}
            if url == sec.SEC_TICKERS_URL
            else {}
        )

    monkeypatch.setattr(sec, "_request_json", current_index)

    def transport(method, *args):
        calls.append(method)
        if method == "get_company_filings":
            return sec.get_company_filings(*args)
        if method == "get_news":
            return [
                {
                    "source": "wire",
                    "title": "Other New Issuer bankruptcy",
                    "content": "Other New Issuer filed for bankruptcy.",
                    "published_at": today,
                    "category": "news",
                    "status": "available",
                }
            ]
        return []

    monkeypatch.setattr(providers, "route_to_vendor", transport)
    providers._CACHE.clear()
    c = candidate(
        name="Old Peer Corp",
        symbol="PEER",
        quote="Old Peer Corp competes directly with us in satellite services.",
    )
    row = evidence(c["proofs"][0]["quote"] + " Old Peer Corp (PEER).") | {"as_of_date": today}
    bundle = create_related_company_researcher(FakeModel({"candidates": [c]}))(
        {"company_of_interest": "TARGET", "trade_date": today, "shared_evidence": [row]}
    )["related_company_research"]
    assert len(bundle["entities"]) == 1
    entity = bundle["entities"][0]
    assert entity["symbol"] is None
    assert entity["identity_status"] == "unresolved"
    assert not calls and not requests
    assert "bankruptcy" not in render_related_context(bundle)


def test_live_independently_verified_identity_still_collects_related_news(monkeypatch):
    from datetime import date

    from tradingagents.related_companies import providers

    today = date.today().isoformat()
    calls = []
    monkeypatch.setattr(
        providers,
        "resolve_instrument_identity",
        lambda symbol: {
            "company_name": "Peer Corp",
            "quote_currency": "USD",
            "financial_currency": "USD",
        },
    )
    monkeypatch.setattr(
        providers, "load_ohlcv", lambda *args: (_ for _ in ()).throw(RuntimeError("offline"))
    )

    def transport(method, *args):
        calls.append(method)
        return (
            [
                {
                    "source": "wire",
                    "title": "Peer Corp opens factory",
                    "content": "Peer Corp opened a new factory.",
                    "published_at": today,
                    "category": "news",
                    "status": "available",
                }
            ]
            if method == "get_news"
            else []
        )

    monkeypatch.setattr(providers, "route_to_vendor", transport)
    providers._CACHE.clear()
    c = candidate(symbol="PEER")
    row = evidence(c["proofs"][0]["quote"] + " Peer Corp (PEER).") | {"as_of_date": today}
    bundle = create_related_company_researcher(FakeModel({"candidates": [c]}))(
        {"company_of_interest": "TARGET", "trade_date": today, "shared_evidence": [row]}
    )["related_company_research"]
    assert bundle["entities"][0]["identity_status"] == "live_identity_verified"
    assert "get_news" in calls
    assert "Peer Corp opens factory" in render_related_context(bundle)


def test_live_identity_refresh_bypasses_global_cache_without_clearing_it(monkeypatch):
    from datetime import date
    from functools import lru_cache

    from tradingagents.related_companies import providers

    today = date.today().isoformat()
    clock = [1000.0]
    current_name = ["Old Peer Corp"]
    lookups = []

    @lru_cache(maxsize=32)
    def shared_resolver(symbol):
        lookups.append(current_name[0])
        return {
            "company_name": current_name[0],
            "quote_currency": "USD",
            "financial_currency": "USD",
        }

    # Seed the shared main-instrument cache; related verification must neither trust nor clear it.
    shared_resolver("PEER")
    monkeypatch.setattr(providers, "resolve_instrument_identity", shared_resolver)
    monkeypatch.setattr(providers, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        providers, "load_ohlcv", lambda *a: (_ for _ in ()).throw(RuntimeError("offline"))
    )
    calls = []
    monkeypatch.setattr(
        providers, "route_to_vendor", lambda method, *args: calls.append(method) or []
    )
    providers._CACHE.clear()
    c = candidate(
        name="Old Peer Corp",
        symbol="PEER",
        quote="Old Peer Corp competes directly with us in satellite services.",
    )
    row = evidence(c["proofs"][0]["quote"] + " Old Peer Corp (PEER).") | {"as_of_date": today}
    state = {"company_of_interest": "TARGET", "trade_date": today, "shared_evidence": [row]}
    stage = create_related_company_researcher(FakeModel({"candidates": [c]}))
    assert stage(state)["related_company_research"]["entities"][0]["symbol"] == "PEER"
    current_name[0] = "Other New Issuer"
    count = len(calls)
    clock[0] += 299
    assert stage(state)["related_company_research"]["entities"][0]["symbol"] == "PEER"
    clock[0] += 2
    result = stage(state)["related_company_research"]
    assert result["entities"][0]["symbol"] is None
    assert len(calls) == count
    assert shared_resolver("PEER")["company_name"] == "Old Peer Corp"
    assert lookups[-1] == "Other New Issuer"
