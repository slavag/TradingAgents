"""Offline, escaped investor research HTML with traceable source markers."""

from decimal import Decimal

import pytest
from parsel import Selector

from tradingagents.investor_deck import (
    DecisionBlock,
    DeckMetric,
    DeckSlide,
    DeckTableRow,
    InvestorDeck,
    MetricBlock,
    TableBlock,
    TrendBlock,
)
from tradingagents.investor_deck_html import (
    render_investor_deck,
    render_unavailable_investor_deck,
)
from tradingagents.research_ledger import ResearchSource

pytestmark = pytest.mark.unit


def peng_deck_fixture(title="FY2026 Q4", source_url=None):
    url = source_url or (
        "https://www.sec.gov/Archives/edgar/data/1616533/"
        "000161653326000046/pengq4-26form8xkxex991.htm"
    )
    source = ResearchSource(
        "ev-q4", "sec", "PENG FY2026 results", url,
        "2026-10-06", "Quarterly Financial Results", "available",
        accession="0001616533-26-000046", form="8-K",
        document_type="EX-99.1", report_period="2026-08-28",
        coverage="partial",
    )
    return InvestorDeck(
        ticker="PENG", analysis_date="2026-10-09", headline="PENG investor research",
        sources={"ev-q4": source}, slides=[
            DeckSlide(
                topic="revenue", title=title, takeaway="Revenue accelerated",
                source_ids=["ev-q4"], blocks=[MetricBlock(metrics=[DeckMetric(
                    label="Net sales", value="$566.7 million", raw_value=Decimal("566685000"),
                    period="FY2026 Q4", source_ids=["ev-q4"],
                )])],
            ),
            DeckSlide(
                topic="segments", title="Revenue mix",
                takeaway="Memory is the largest segment", source_ids=["ev-q4"],
                blocks=[TableBlock(headings=["Segment", "Share"], rows=[
                    DeckTableRow(cells=["Integrated Memory", "53.4%"], source_ids=["ev-q4"]),
                ])],
            ),
        ],
    )


def test_deck_renders_dated_sources_tables_and_trends_without_external_assets():
    deck = peng_deck_fixture()
    deck.slides.append(DeckSlide(
        topic="earnings", title="Profitability", takeaway="Income improved",
        source_ids=["ev-q4"], blocks=[TrendBlock(points=[
            DeckMetric(label="Net income", value="$44.7 million",
                       raw_value=Decimal("44689000"), period="Q3-26", source_ids=["ev-q4"]),
            DeckMetric(label="Net income", value="$93.2 million",
                       raw_value=Decimal("93204000"), period="Q4-26", source_ids=["ev-q4"]),
        ])],
    ))

    html = render_investor_deck(deck, "20261009T154737Z")

    assert "FY2026 Q4" in html and "$566.7 million" in html
    assert "Integrated Memory" in html and "53.4%" in html
    assert "Q3-26" in html and "Q4-26" in html
    assert Selector(text=html).css("svg.trend-line")
    assert "<polyline" in html
    assert 'class="bar-row"' not in html
    assert "pengq4-26form8xkxex991.htm" in html
    assert "<script" not in html and "<img" not in html


def test_source_only_deck_keeps_research_without_generator_banner():
    deck = peng_deck_fixture()
    deck.synthesis_status = "source_only"
    deck.synthesis_reason = "unsupported_interpretation"

    html = render_investor_deck(deck, "20261009T154737Z")

    assert "FY2026 Q4" in html
    assert "Interpretation withheld" not in html
    assert "Optional deck narration" not in html


def test_source_appendix_uses_reader_facing_labels_for_internal_tool_records():
    deck = peng_deck_fixture()
    deck.sources["ev-q4"] = ResearchSource(
        "ev-q4", "get_cashflow", "Executed tool response: get_cashflow", None,
        "2026-10-09", None, "available", coverage="full",
    )

    html = render_investor_deck(deck, "20261009T154737Z")

    appendix = Selector(text=html).css(".sources").get()
    assert appendix is not None
    assert "Cash-flow statement" in appendix
    assert "Executed tool response" not in appendix

    deck.sources["ev-q4"] = ResearchSource(
        "ev-q4", "sec", "SEC reported Net income for PENG", None,
        "2026-07-07", None, "available", form="10-Q",
        accession="0001616533-26-000043",
    )
    appendix = Selector(text=render_investor_deck(
        deck, "20261009T154737Z",
    )).css(".sources").get()
    assert appendix is not None
    assert "Company filing financial data" in appendix
    assert "SEC reported Net income for PENG" not in appendix


def test_decision_slide_renders_separate_actions_confidences_and_accepted_target():
    deck = peng_deck_fixture()
    deck.slides.append(DeckSlide(
        topic="decision", title="Final recommendation",
        takeaway="Overweight: Hold existing holdings; Conditional Buy for a new position.",
        source_ids=["ev-q4"], blocks=[DecisionBlock(
            status="Actionable", rating="Overweight", recommendation_confidence=74,
            existing_action="Hold", existing_guidance="Keep a right-sized core.",
            new_action="Conditional Buy", new_guidance="Stage a small entry.",
            confirmation="Breakout must hold.", alternative="Wait for a pullback.",
            invalidation="Suspend purchases if cash flow stays negative.",
            target=Decimal("78.75"), target_horizon="1-3 months",
            target_confidence=56,
            target_rationale="A near-term retest, not <script>fundamental</script> value.",
            source_ids=["ev-q4"],
        )],
    ))

    html = render_investor_deck(deck, "20261009T154737Z")
    slide = Selector(text=html).css('section.slide[data-topic="decision"]')

    assert slide.css(".decision-existing").get()
    assert slide.css(".decision-new").get()
    assert "Recommendation confidence" in slide.get()
    assert "Target confidence" in slide.get()
    assert "74/100" in slide.get() and "56/100" in slide.get()
    assert "78.75" in slide.get() and "1-3 months" in slide.get()
    assert "Quote currency not recorded" in slide.get()
    assert "Breakout must hold" in slide.get()
    assert "Suspend purchases" in slide.get()
    assert 'href="#source-1"' in slide.get()
    assert "03 / 03" in slide.get()
    assert "<script>" not in html
    assert "&lt;script&gt;" in html


def test_recommendation_card_shows_full_summary_and_balanced_layout():
    deck = peng_deck_fixture()
    summary = (
        "Reported growth supports an Overweight view, but do not chase the current "
        "extension. Stage entries only after confirmation, and reassess if cash "
        "conversion fails to improve. <script>untrusted</script>"
    )
    deck.slides.append(DeckSlide(
        topic="decision", title="Final recommendation", takeaway="Overweight.",
        blocks=[DecisionBlock(
            status="Actionable", rating="Overweight", recommendation_confidence=72,
            recommendation_summary=summary,
            target=Decimal("80.10"), target_horizon="3-6 months",
            target_rationale="The verified high is a checkpoint, not fair value.",
        )],
    ))

    html = render_investor_deck(deck, "20261012T180336Z")
    slide = Selector(text=html).css('section.slide[data-topic="decision"]')
    overview = slide.css(".decision-overview")

    assert len(overview) == 1
    assert "Reported growth supports an Overweight view" in overview.get()
    assert "conversion fails to improve." in overview.get()
    assert "&lt;script&gt;untrusted&lt;/script&gt;" in overview.get()
    assert "<script>untrusted</script>" not in html
    assert slide.css(".decision-target.decision-wide")


def test_sources_start_collapsed_with_count_and_keep_individual_anchors():
    deck = peng_deck_fixture()

    html = render_investor_deck(deck, "20261012T180336Z")
    selector = Selector(text=html)
    disclosure = selector.css("details.sources")

    assert len(disclosure) == 1
    assert "open" not in disclosure.attrib
    assert "Sources (1)" in disclosure.css("summary::text").getall()
    assert disclosure.css('li#source-1 strong::text').get() == "PENG FY2026 results"
    assert selector.css('a.source-ref[href="#source-1"]')


def test_nonactionable_decision_slide_hides_actions_confidence_and_target():
    deck = peng_deck_fixture()
    deck.slides.append(DeckSlide(
        topic="decision", title="Final recommendation",
        takeaway="No actionable recommendation was produced for this run.",
        source_ids=["ev-q4"], blocks=[DecisionBlock(
            status="Unavailable", status_reason="upstream_trader_unavailable",
            source_ids=["ev-q4"],
        )],
    ))

    html = render_investor_deck(deck, "20261009T154737Z")
    slide = Selector(text=html).css('section.slide[data-topic="decision"]')

    assert "No actionable recommendation" in slide.get()
    assert "upstream trader unavailable" in slide.get()
    assert "Target confidence" not in slide.get()
    assert "Recommendation confidence" not in slide.get()
    assert "decision-existing" not in slide.get()


def test_decision_without_conditions_omits_empty_conditions_divider():
    deck = peng_deck_fixture()
    deck.slides.append(DeckSlide(
        topic="decision", title="Final recommendation", takeaway="Hold.",
        source_ids=["ev-q4"], blocks=[DecisionBlock(
            status="Actionable", rating="Hold", recommendation_confidence=60,
            existing_action="Hold", new_action="Wait", source_ids=["ev-q4"],
        )],
    ))

    html = render_investor_deck(deck, "20261009T154737Z")

    assert not Selector(text=html).css('[data-topic="decision"] .decision-conditions')
    assert "http://fonts" not in html


def test_line_chart_orders_marks_by_time_and_preserves_readable_values():
    deck = peng_deck_fixture()
    deck.slides = [DeckSlide(
        topic="cash", title="Cash trend", takeaway="Cash has varied",
        source_ids=["ev-q4"], blocks=[TrendBlock(points=[
            DeckMetric(label="Cash flow", value="-$10.0 million",
                       raw_value=Decimal("-10000000"), period="Q1-26", source_ids=["ev-q4"]),
            DeckMetric(label="Cash flow", value="$20.0 million",
                       raw_value=Decimal("20000000"), period="Q2-26", source_ids=["ev-q4"]),
            DeckMetric(label="Cash flow", value="$5.0 million",
                       raw_value=Decimal("5000000"), period="Q3-26", source_ids=["ev-q4"]),
        ])],
    )]

    selector = Selector(text=render_investor_deck(deck, "20261009T154737Z"))
    chart = selector.css("svg.trend-line")
    coordinates = chart.css("polyline::attr(points)").get().split()
    ys = [Decimal(point.split(",")[1]) for point in coordinates]
    labels = selector.css("ol.trend-values").xpath(".//text()").getall()

    assert len(coordinates) == 3
    assert ys[1] < ys[2] < ys[0]
    assert chart.css("line.zero-line")
    zero_y = Decimal(chart.css("line.zero-line::attr(y1)").get())
    zero_pct = Decimal(selector.css(".trend-scale .zero-label::attr(style)").get()
                       .removeprefix("top:").removesuffix("%"))
    assert abs(zero_pct - zero_y / Decimal(220) * 100) < Decimal("0.5")
    assert "Q1-26" in " ".join(labels) and "$20.0 million" in " ".join(labels)
    assert selector.css("ol.trend-values li a.source-ref")


def test_line_chart_spacing_reflects_missing_fiscal_quarters():
    deck = peng_deck_fixture()
    deck.slides = [DeckSlide(
        topic="revenue", title="Revenue trend", takeaway="Sales increased",
        source_ids=["ev-q4"], blocks=[TrendBlock(points=[
            DeckMetric(label="Net sales", value="$337.9 million",
                       raw_value=Decimal("337922000"), period="Q4-25", source_ids=["ev-q4"]),
            DeckMetric(label="Net sales", value="$478.7 million",
                       raw_value=Decimal("478713000"), period="Q3-26", source_ids=["ev-q4"]),
            DeckMetric(label="Net sales", value="$566.7 million",
                       raw_value=Decimal("566685000"), period="Q4-26", source_ids=["ev-q4"]),
        ])],
    )]

    selector = Selector(text=render_investor_deck(deck, "20261009T154737Z"))
    points = selector.css("svg.trend-desktop polyline::attr(points)").get().split()
    xs = [Decimal(point.split(",")[0]) for point in points]

    assert (xs[1] - xs[0]) > (xs[2] - xs[1]) * 2


def test_slide_title_starts_left_and_number_is_in_footer_with_sources():
    selector = Selector(text=render_investor_deck(peng_deck_fixture(), "20261009T154737Z"))
    slide = selector.css("section.slide")[0]

    assert slide.css(".slide-header h2::text").get() == "FY2026 Q4"
    assert not slide.css(".slide-header .eyebrow")
    assert slide.css("footer .source-ref")
    assert slide.css("footer .slide-number::text").get() == "01 / 02"


@pytest.mark.parametrize("source_url", ["javascript:alert(1)", "https://example.org:bad/x"])
def test_untrusted_source_html_and_javascript_url_are_not_rendered(source_url):
    deck = peng_deck_fixture(
        title="<script>alert(1)</script>", source_url=source_url,
    )

    html = render_investor_deck(deck, "20261009T154737Z")

    assert "&lt;script&gt;" in html and "<script>" not in html
    assert 'href="javascript:' not in html
    assert 'href="https://example.org:bad' not in html


def test_source_marker_resolves_to_exact_document_metadata():
    html = render_investor_deck(peng_deck_fixture(), "20261009T154737Z")

    assert 'href="#source-1"' in html
    assert 'id="source-1"' in html
    assert "PENG FY2026 results" in html
    assert "Quarterly Financial Results" in html
    assert "2026-10-06" in html
    assert "ev-q4" in html
    assert "0001616533-26-000046" in html
    assert "EX-99.1" in html
    assert "2026-08-28" in html
    assert "https://www.sec.gov/Archives/edgar/data/1616533/" in html


def test_unavailable_page_points_only_to_job_scoped_or_same_directory_full_report():
    job_id = "a" * 32
    route = f"/api/jobs/{job_id}/reports/PENG/full"
    html = render_unavailable_investor_deck(
        "PENG", "2026-10-09", route, "20261009T154737Z",
    )
    unsafe = render_unavailable_investor_deck(
        "PENG", "2026-10-09", "https://evil.invalid/steal", "20261009T154737Z",
    )

    assert "Research deck unavailable" in html
    assert "Interpretation withheld" not in html
    assert f'href="{route}"' in html
    assert "evil.invalid" not in unsafe
