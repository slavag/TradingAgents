"""Investor presentations are readable without laundering unsupported claims."""

from unittest.mock import MagicMock

import pytest

from tradingagents import investor_brief


@pytest.mark.unit
def test_structured_brief_keeps_cited_facts_but_drops_unsupported_numbers():
    builder = getattr(investor_brief, "build_investor_brief", None)
    assert callable(builder), "the per-ticker investor brief builder is missing"

    draft = investor_brief.InvestorBrief(
        headline="AAPL has $999 billion in cash",
        cards=[investor_brief.BriefCard(
            title="Financial footing",
            takeaway="The latest recorded cash balance matters more than speculation.",
            claims=[
                investor_brief.BriefClaim(
                    text="Cash was $4.31 million. <script>bad()</script>",
                    evidence_ids=["ev-cash"], classification="fact",
                ),
                investor_brief.BriefClaim(
                    text="Debt was $99 million.", evidence_ids=["ev-cash"],
                    classification="fact",
                ),
                investor_brief.BriefClaim(
                    text="A monopoly is guaranteed.", evidence_ids=["ev-unknown"],
                    classification="fact",
                ),
                investor_brief.BriefClaim(
                    text="The company controls every market.", evidence_ids=["ev-cash"],
                    classification="fact",
                ),
            ],
        )],
    )
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = draft
    state = {
        "trade_date": "2026-10-08",
        "portfolio_decision": {"status": "Actionable", "rating": "Hold"},
        "shared_evidence": [{
            "evidence_id": "ev-cash", "source": "sec", "category": "fundamentals",
            "title": "SEC reported cash", "content": "Cash was 4.31 million USD.",
            "status": "available", "coverage": "reported_fact",
            "published_at": "2026-10-07", "as_of_date": "2026-10-08",
        }],
    }

    result = builder(state, "AAPL", "2026-10-08", llm)
    claims = result.cards[0].claims
    assert [claim.evidence_ids for claim in claims] == [["ev-cash"]]
    assert result.headline == "AAPL: investor brief"
    assert "4.31" in claims[0].text
    html = investor_brief.render_investor_brief(
        result, "AAPL", "2026-10-08", "20261008T132500Z",
        {"ev-cash": "SEC reported cash"},
    )
    assert "&lt;script&gt;" in html
    assert "<script>" not in html
    assert "$99" not in html
    assert "Run timestamp: 2026-10-08 13:25 UTC" in html


@pytest.mark.unit
def test_brief_falls_back_to_explicit_decision_when_model_call_fails():
    builder = getattr(investor_brief, "build_investor_brief", None)
    assert callable(builder), "the per-ticker investor brief builder is missing"

    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.side_effect = RuntimeError("provider down")
    state = {
        "trade_date": "2026-10-08",
        "portfolio_decision": {
            "status": "Actionable", "rating": "Hold",
            "existing_position_action": "Hold", "new_position_action": "Wait",
            "executive_summary": "Maintain only an existing position.",
        },
    }

    result = builder(state, "AAPL", "2026-10-08", llm)

    assert "AAPL" in result.headline
    assert "Maintain only an existing position." in result.cards[0].takeaway
    assert result.cards[0].claims[0].classification == "assessment"


@pytest.mark.unit
def test_optional_brief_tolerates_missing_or_malformed_source_containers():
    state = {
        "portfolio_decision": {
            "status": "Actionable", "rating": "Hold",
            "executive_summary": "Maintain the existing position.",
        },
        "shared_evidence": None,
        "risk_debate_state": "unavailable",
    }
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.side_effect = RuntimeError("provider down")

    brief = investor_brief.build_investor_brief(state, "AAPL", "2026-10-08", llm)

    assert brief.cards[0].takeaway == "Maintain the existing position."


@pytest.mark.unit
def test_numeric_assessment_cannot_rely_only_on_model_authored_team_text():
    draft = investor_brief.InvestorBrief(
        headline="AAPL investor view",
        cards=[investor_brief.BriefCard(
            title="Valuation", takeaway="This needs source support.",
            claims=[investor_brief.BriefClaim(
                text="The team estimates $999 million in cash.",
                evidence_ids=["analysis:portfolio"], classification="assessment",
            )],
        )],
    )
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = draft
    state = {
        "trade_date": "2026-10-08",
        "final_trade_decision": "The team estimates $999 million in cash.",
        "portfolio_decision": {
            "status": "Actionable", "rating": "Hold",
            "executive_summary": "Maintain only an existing position.",
        },
        "shared_evidence": [{
            "evidence_id": "ev-cash", "source": "sec", "category": "fundamentals",
            "title": "SEC reported cash", "content": "Cash was 4.31 million USD.",
            "status": "available", "published_at": "2026-10-07",
            "as_of_date": "2026-10-08",
        }],
    }

    result = investor_brief.build_investor_brief(state, "AAPL", "2026-10-08", llm)

    assert "Maintain only an existing position." in result.cards[0].takeaway
    assert all("$999" not in claim.text for card in result.cards for claim in card.claims)


@pytest.mark.unit
def test_brief_prompt_retains_late_fundamental_and_cashflow_sources():
    captured = []
    draft = investor_brief.InvestorBrief(
        headline="AEHR investment picture",
        cards=[investor_brief.BriefCard(
            title="At a glance", takeaway="Evidence is mixed.",
            claims=[investor_brief.BriefClaim(
                text="The portfolio view is cautious.", evidence_ids=["analysis:portfolio"],
                classification="assessment",
            )],
        )],
    )
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.side_effect = (
        lambda prompt: captured.append(prompt) or draft
    )
    sources = [
        {
            "evidence_id": f"ev-early-{index}", "source": "get_indicators",
            "category": "market", "title": f"Indicator {index}",
            "content": f"Indicator {index}: 1.0", "status": "available",
            "published_at": "2026-10-08", "as_of_date": "2026-10-08",
        }
        for index in range(30)
    ]
    sources.extend([
        {
            "evidence_id": "ev-fundamentals", "source": "get_fundamentals",
            "category": "fundamentals", "title": "Company fundamentals",
            "content": "Industry: Semiconductor Equipment & Materials\nRevenue (TTM): 50001000",
            "status": "available", "published_at": "2026-10-08", "as_of_date": "2026-10-08",
        },
        {
            "evidence_id": "ev-income", "source": "get_income_statement",
            "category": "fundamentals", "title": "Annual income statement",
            "content": ",2026-05-31,2025-05-31\nTotal Revenue,50001000,58968000",
            "status": "available", "published_at": "2026-10-08", "as_of_date": "2026-10-08",
        },
        {
            "evidence_id": "ev-cashflow", "source": "get_cashflow",
            "category": "fundamentals", "title": "Annual cash flow",
            "content": ",2026-05-31,2025-05-31\nIssuance Of Capital Stock,97395000,0",
            "status": "available", "published_at": "2026-10-08", "as_of_date": "2026-10-08",
        },
    ])
    state = {
        "trade_date": "2026-10-08", "shared_evidence": sources,
        "final_trade_decision": "The portfolio view is cautious.",
        "fundamentals_report": "Revenue fell, but the latest quarter rebounded. " * 150,
    }

    investor_brief.build_investor_brief(state, "AEHR", "2026-10-08", llm)

    prompt = captured[0]
    assert '"id": "ev-fundamentals"' in prompt
    assert '"id": "ev-income"' in prompt
    assert '"id": "ev-cashflow"' in prompt
    assert "At most one card on technical indicators" in prompt


@pytest.mark.unit
def test_brief_prompt_exposes_working_capital_drivers_from_long_statements():
    captured = []
    draft = investor_brief.InvestorBrief(
        headline="PENG investment picture",
        cards=[investor_brief.BriefCard(
            title="Decision", takeaway="Cash conversion needs proof.",
            claims=[investor_brief.BriefClaim(
                text="The portfolio view is Hold.", evidence_ids=["analysis:portfolio"],
                classification="assessment",
            )],
        )],
    )
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.side_effect = (
        lambda prompt: captured.append(prompt) or draft
    )
    filler = "Unrelated Row,1000000,1000000\n" * 50
    state = {
        "trade_date": "2026-10-09",
        "final_trade_decision": "The portfolio view is Hold.",
        "shared_evidence": [
            {
                "evidence_id": "ev-balance", "source": "get_balance_sheet",
                "category": "fundamentals", "title": "Balance Sheet",
                "content": ",2026-05-31,2026-02-28\n" + filler
                + "Accounts Receivable,702981000,369900000\n"
                + "Inventory,498318000,322400000\n" + filler,
                "status": "available", "published_at": "2026-10-09",
                "as_of_date": "2026-10-09",
            },
            {
                "evidence_id": "ev-cashflow", "source": "get_cashflow",
                "category": "fundamentals", "title": "Cash Flow",
                "content": ",2026-05-31,2026-02-28\n" + filler
                + "Change In Receivables,-333660000,-28641000\n"
                + "Change In Inventory,-175958000,-109155000\n" + filler,
                "status": "available", "published_at": "2026-10-09",
                "as_of_date": "2026-10-09",
            },
        ],
    }

    investor_brief.build_investor_brief(state, "PENG", "2026-10-09", llm)

    prompt = captured[0]
    assert "Accounts Receivable: 2026-05-31 $703.0 million" in prompt
    assert "Inventory: 2026-05-31 $498.3 million" in prompt
    assert "Change In Receivables: 2026-05-31 -$333.7 million" in prompt
    assert "Change In Inventory: 2026-05-31 -$176.0 million" in prompt


@pytest.mark.unit
def test_brief_accepts_deterministically_rounded_millions_from_sec_fact():
    draft = investor_brief.InvestorBrief(
        headline="AEHR investment picture",
        cards=[investor_brief.BriefCard(
            title="Annual sales", takeaway="Sales contracted.",
            claims=[investor_brief.BriefClaim(
                text="Annual revenue was $50.0 million.", evidence_ids=["ev-revenue"],
                classification="fact",
            )],
        )],
    )
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = draft
    state = {
        "trade_date": "2026-10-08",
        "shared_evidence": [{
            "evidence_id": "ev-revenue", "source": "sec", "category": "fundamentals",
            "title": "SEC reported Revenue", "content": "Metric: Revenue\nValue: 50001000 USD",
            "status": "available", "coverage": "reported_fact",
            "published_at": "2026-10-08", "as_of_date": "2026-10-08",
        }],
    }

    result = investor_brief.build_investor_brief(state, "AEHR", "2026-10-08", llm)

    assert result.cards[0].claims[0].text == "Annual revenue was $50.0 million."


@pytest.mark.unit
def test_brief_renders_raw_usd_amount_as_readable_millions():
    draft = investor_brief.InvestorBrief(
        headline="AEHR investment picture",
        cards=[investor_brief.BriefCard(
            title="Annual sales", takeaway="Sales contracted.",
            claims=[investor_brief.BriefClaim(
                text="Annual revenue was 50001000 USD.", evidence_ids=["ev-revenue"],
                classification="fact",
            )],
        )],
    )
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = draft
    state = {
        "trade_date": "2026-10-08",
        "shared_evidence": [{
            "evidence_id": "ev-revenue", "source": "sec", "category": "fundamentals",
            "title": "SEC reported Revenue", "content": "Metric: Revenue\nValue: 50001000 USD",
            "status": "available", "published_at": "2026-10-08",
            "as_of_date": "2026-10-08",
        }],
    }

    result = investor_brief.build_investor_brief(state, "AEHR", "2026-10-08", llm)
    html = investor_brief.render_investor_brief(
        result, "AEHR", "2026-10-08", "20261008T132500Z"
    )

    assert "$50.0 million" in html
    assert "50001000 USD" not in html


@pytest.mark.unit
def test_brief_does_not_reverse_the_sign_of_a_cited_loss():
    draft = investor_brief.InvestorBrief(
        headline="AEHR investment picture",
        cards=[investor_brief.BriefCard(
            title="Profitability", takeaway="The year ended with a loss.",
            claims=[
                investor_brief.BriefClaim(
                    text="Net income was a $7.1 million profit.",
                    evidence_ids=["ev-income"], classification="fact",
                ),
                investor_brief.BriefClaim(
                    text="Net income was -$7.1 million.",
                    evidence_ids=["ev-income"], classification="fact",
                ),
            ],
        )],
    )
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = draft
    state = {
        "trade_date": "2026-10-08",
        "shared_evidence": [{
            "evidence_id": "ev-income", "source": "sec", "category": "fundamentals",
            "title": "SEC reported Net income", "content": "Metric: Net income\nValue: -7126000 USD",
            "status": "available", "published_at": "2026-10-08",
            "as_of_date": "2026-10-08",
        }],
    }

    brief = investor_brief.build_investor_brief(state, "AEHR", "2026-10-08", llm)

    assert [claim.text for claim in brief.cards[0].claims] == [
        "Net income was -$7.1 million."
    ]


@pytest.mark.unit
def test_brief_omits_sparse_and_gap_only_slides_but_keeps_relevant_caveat():
    draft = investor_brief.InvestorBrief(
        headline="PENG investment picture",
        cards=[
            investor_brief.BriefCard(
                title="Decision overview", takeaway="Hold pending better cash conversion.",
                claims=[investor_brief.BriefClaim(
                    text="The portfolio view is Hold.",
                    evidence_ids=["analysis:portfolio"], classification="assessment",
                )],
            ),
            investor_brief.BriefCard(
                title="What PENG is", takeaway="PENG is a technology company.",
                claims=[
                    investor_brief.BriefClaim(
                        text="The sector is Technology and the industry is Information Technology Services.",
                        evidence_ids=["ev-profile"], classification="fact",
                    ),
                    investor_brief.BriefClaim(
                        text="The company is classified in the Information Technology Services industry.",
                        evidence_ids=["ev-profile"], classification="fact",
                    ),
                    investor_brief.BriefClaim(
                        text="The company has a $3.7 billion market cap.",
                        evidence_ids=["ev-profile"], classification="fact",
                    ),
                    investor_brief.BriefClaim(
                        text="A sector label alone does not explain products or customers.",
                        evidence_ids=["analysis:fundamentals"], classification="assessment",
                    ),
                ],
            ),
            investor_brief.BriefCard(
                title="Operating performance", takeaway="The recent quarter improved sharply.",
                claims=[
                    investor_brief.BriefClaim(
                        text="Revenue rose from $343.0 million to $478.7 million.",
                        evidence_ids=["ev-income"], classification="fact",
                    ),
                    investor_brief.BriefClaim(
                        text="Operating income rose from $25.7 million to $50.9 million.",
                        evidence_ids=["ev-income"], classification="fact",
                    ),
                    investor_brief.BriefClaim(
                        text="That improvement matters only if it converts into cash.",
                        evidence_ids=["analysis:fundamentals"], classification="assessment",
                    ),
                    investor_brief.BriefClaim(
                        text="Analyst estimates were unavailable for comparison.",
                        evidence_ids=["ev-estimates"], classification="gap",
                    ),
                ],
            ),
            investor_brief.BriefCard(
                title="Catalysts to watch", takeaway="A catalyst has not been verified.",
                claims=[
                    investor_brief.BriefClaim(
                        text="No company news was found.",
                        evidence_ids=["ev-news"], classification="fact",
                    ),
                    investor_brief.BriefClaim(
                        text="No company announcement was found.",
                        evidence_ids=["ev-news"], classification="fact",
                    ),
                    investor_brief.BriefClaim(
                        text="An empty news search is not a catalyst.",
                        evidence_ids=["analysis:news"], classification="assessment",
                    ),
                    investor_brief.BriefClaim(
                        text="The filing narrative was unavailable.",
                        evidence_ids=["ev-filing"], classification="gap",
                    ),
                ],
            ),
            investor_brief.BriefCard(
                title="What is missing", takeaway="Some feeds did not return data.",
                claims=[
                    investor_brief.BriefClaim(
                        text="Analyst estimates were unavailable for comparison.",
                        evidence_ids=["ev-estimates"], classification="gap",
                    ),
                    investor_brief.BriefClaim(
                        text="The filing narrative was unavailable.",
                        evidence_ids=["ev-filing"], classification="gap",
                    ),
                    investor_brief.BriefClaim(
                        text="Another filing narrative was unavailable.",
                        evidence_ids=["ev-filing"], classification="gap",
                    ),
                ],
            ),
        ],
    )
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = draft
    state = {
        "trade_date": "2026-10-09",
        "final_trade_decision": "The portfolio view is Hold.",
        "fundamentals_report": (
            "That improvement matters only if it converts into cash. "
            "A sector label alone does not explain products or customers."
        ),
        "news_report": "An empty news search is not a catalyst.",
        "shared_evidence": [
            {
                "evidence_id": "ev-profile", "source": "get_fundamentals",
                "category": "fundamentals", "title": "Company fundamentals",
                "content": "Sector: Technology\nIndustry: Information Technology Services\n"
                "Market Cap: 3667440128",
                "status": "available", "published_at": "2026-10-09",
                "as_of_date": "2026-10-09",
            },
            {
                "evidence_id": "ev-income", "source": "get_income_statement",
                "category": "fundamentals", "title": "Quarterly income statement",
                "content": "Revenue rose from $343.0 million to $478.7 million. "
                "Operating income rose from $25.7 million to $50.9 million.",
                "status": "available", "published_at": "2026-10-09",
                "as_of_date": "2026-10-09",
            },
            {
                "evidence_id": "ev-news", "source": "get_news", "category": "news",
                "title": "Company news", "content": (
                    "No company news was found. No company announcement was found."
                ),
                "status": "available", "published_at": "2026-10-09",
                "as_of_date": "2026-10-09",
            },
            {
                "evidence_id": "ev-filing", "source": "sec", "category": "filings",
                "title": "SEC filing", "content": "Filing narrative unavailable.",
                "coverage": "metadata_only", "status": "available",
                "published_at": "2026-10-09", "as_of_date": "2026-10-09",
            },
            {
                "evidence_id": "ev-estimates", "source": "fmp", "category": "earnings",
                "title": "Analyst estimates", "content": "Analyst estimates unavailable.",
                "status": "unavailable", "published_at": "2026-10-09",
                "as_of_date": "2026-10-09",
            },
        ],
    }

    brief = investor_brief.build_investor_brief(state, "PENG", "2026-10-09", llm)

    assert [card.title for card in brief.cards] == ["Decision overview", "Operating performance"]
    assert brief.cards[1].claims[-1].classification == "gap"


@pytest.mark.unit
def test_brief_combines_adjacent_financial_facts_and_their_interpretation():
    draft = investor_brief.InvestorBrief(
        headline="PENG investment picture",
        cards=[
            investor_brief.BriefCard(
                title="Decision overview", takeaway="Hold while cash conversion is weak.",
                claims=[investor_brief.BriefClaim(
                    text="The portfolio view is Hold.",
                    evidence_ids=["analysis:portfolio"], classification="assessment",
                )],
            ),
            investor_brief.BriefCard(
                title="Operating performance", takeaway="Revenue and operating income rose.",
                claims=[
                    investor_brief.BriefClaim(
                        text="Revenue rose to $478.7 million.",
                        evidence_ids=["ev-income"], classification="fact",
                    ),
                    investor_brief.BriefClaim(
                        text="Operating income rose to $50.9 million.",
                        evidence_ids=["ev-income"], classification="fact",
                    ),
                ],
            ),
            investor_brief.BriefCard(
                title="Why the financial trend matters",
                takeaway="Cash must confirm the improvement.",
                claims=[investor_brief.BriefClaim(
                    text="Working-capital use makes the earnings improvement less certain.",
                    evidence_ids=["analysis:fundamentals"], classification="assessment",
                )],
            ),
        ],
    )
    llm = MagicMock()
    llm.with_structured_output.return_value.invoke.return_value = draft
    state = {
        "trade_date": "2026-10-09",
        "final_trade_decision": "The portfolio view is Hold.",
        "fundamentals_report": (
            "Working-capital use makes the earnings improvement less certain."
        ),
        "shared_evidence": [{
            "evidence_id": "ev-income", "source": "get_income_statement",
            "category": "fundamentals", "title": "Quarterly income statement",
            "content": "Revenue rose to $478.7 million. Operating income rose to $50.9 million.",
            "status": "available", "published_at": "2026-10-09",
            "as_of_date": "2026-10-09",
        }],
    }

    brief = investor_brief.build_investor_brief(state, "PENG", "2026-10-09", llm)

    assert [card.title for card in brief.cards] == ["Decision overview", "Operating performance"]
    assert [claim.classification for claim in brief.cards[1].claims] == [
        "fact", "fact", "assessment",
    ]


@pytest.mark.unit
def test_brief_renders_source_notes_without_internal_tool_or_metric_titles():
    brief = investor_brief.InvestorBrief(
        headline="PENG investment picture",
        cards=[investor_brief.BriefCard(
            title="Operating performance", takeaway="Growth needs cash conversion.",
            claims=[
                investor_brief.BriefClaim(
                    text="Revenue rose to $478.7 million.",
                    evidence_ids=["income"], classification="fact",
                ),
                investor_brief.BriefClaim(
                    text="Net income was $44.7 million.",
                    evidence_ids=["sec-net-income"], classification="fact",
                ),
                investor_brief.BriefClaim(
                    text="Cash conversion remains the key test.",
                    evidence_ids=["analysis:portfolio"], classification="assessment",
                ),
            ],
        )],
    )

    html = investor_brief.render_investor_brief(
        brief, "PENG", "2026-10-09", "20261009T075756Z", {
            "income": "Executed tool response: get_income_statement",
            "sec-net-income": "SEC reported Net income for PENG",
            "analysis:portfolio": "Portfolio manager assessment",
        },
    )

    assert "Executed tool response" not in html
    assert "SEC reported Net income for PENG" not in html
    assert "Quarterly income statement" in html
    assert "SEC filing financial data" in html
    assert "Portfolio decision" in html
    assert "Sources and analysis basis" in html
    assert '<details class="source-notes">' in html
    assert "Why it matters" in html
    assert "Reported" in html
    assert '<span class="claim-kind">assessment</span>' not in html


@pytest.mark.unit
def test_substantive_slide_can_include_five_distinct_grounded_points():
    brief = investor_brief.InvestorBrief(
        headline="PENG investment picture",
        cards=[investor_brief.BriefCard(
            title="Earnings and cash", takeaway="The apparent improvement has a cash test.",
            claims=[
                investor_brief.BriefClaim(
                    text=point, evidence_ids=["ev-financials"], classification="fact",
                )
                for point in [
                    "Revenue increased.",
                    "Operating income increased.",
                    "Receivables increased.",
                    "Inventory increased.",
                    "Operating cash flow turned negative.",
                ]
            ],
        )],
    )

    html = investor_brief.render_investor_brief(
        brief, "PENG", "2026-10-09", "20261009T075756Z",
        {"ev-financials": "Quarterly statements"},
    )

    assert html.count('class="claim fact"') == 5
    assert "Operating cash flow turned negative." in html
