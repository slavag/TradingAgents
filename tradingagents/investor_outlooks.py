"""Readable forward outlook slides, separate from the tactical decision slide."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from tradingagents.agents.utils.outlooks import PortfolioOutlook
from tradingagents.investor_deck import DeckPoint, DeckSlide, DeckTableRow, TableBlock, TextBlock
from tradingagents.research_ledger import ResearchLedger


def build_outlook_slides(state: Mapping[str, Any], ledger: ResearchLedger) -> list[DeckSlide]:
    slides = []
    for raw in state.get("portfolio_outlooks") or []:
        row = PortfolioOutlook.model_validate(raw)
        ids = [key for key in row.source_ids if key in ledger.sources]
        if not ids and "analysis:portfolio" in ledger.sources:
            ids = ["analysis:portfolio"]
        context = (
            f"Forward model scenario, not company guidance. Analysis: {row.analysis_date}; "
            f"horizon endpoint: {row.end_date}. Verified reference close: {row.reference_price} "
            f"{row.quote_currency or 'quote currency unavailable'}, observed {row.reference_observed_on or 'date unavailable'}. "
            "Daily session only; extended-hours prices are separate information. "
            "Confidence scores measure evidence strength and are not calibrated probabilities."
        )
        if row.rating is None:
            slides.append(
                DeckSlide(
                    topic=f"outlook_{row.months}_month",
                    title=f"{row.months}-month investment outlook",
                    takeaway="No horizon-specific forecast was generated for this saved analysis.",
                    narrative=f"{context} Diagnostic: {row.reason}. The tactical target has not been reused.",
                    source_ids=ids,
                )
            )
            continue
        cells = [
            ("Horizon endpoint", row.end_date),
            ("Recommendation", row.rating.value),
            ("Recommendation confidence", f"{row.recommendation_confidence_score}/100"),
        ]
        points = []
        if row.target:
            target = row.target
            value = (
                f"{target.price_target:,.2f}"
                if target.price_target is not None
                else f"{target.target_low:,.2f}–{target.target_high:,.2f}"
            )
            cells.extend(
                [
                    ("Target", f"{value} {row.quote_currency or 'quote currency unavailable'}"),
                    ("Target confidence", f"{target.confidence_score}/100"),
                    ("Basis", target.target_basis),
                ]
            )
            if row.expected_return_pct is not None:
                cells.append(("Change from reference", f"{row.expected_return_pct:+.2f}%"))
            elif row.expected_return_low_pct is not None:
                cells.append(
                    (
                        "Change from reference",
                        f"{row.expected_return_low_pct:+.2f}% to {row.expected_return_high_pct:+.2f}%",
                    )
                )
            points.extend(
                [
                    DeckPoint(
                        text=f"Method and rationale: {target.target_method}. {target.target_summary}",
                        source_ids=ids,
                    ),
                    DeckPoint(
                        text=f"Assumptions: {target.target_assumptions or 'Sourced price-level scenario; no fundamental fair-value claim.'}",
                        source_ids=ids,
                    ),
                    DeckPoint(text=f"Risks: {target.target_risks}", source_ids=ids),
                ]
            )
        else:
            cells.append(("Target diagnostic", row.reason or "Target not validated"))
        points.extend(
            [
                DeckPoint(text="Drivers: " + "; ".join(row.drivers), source_ids=ids),
                DeckPoint(text=f"Upside: {row.upside_case}", source_ids=ids),
                DeckPoint(text=f"Downside: {row.downside_case}", source_ids=ids),
                DeckPoint(text=f"What changes the view: {row.invalidation}", source_ids=ids),
            ]
        )
        # Keep the target summary and the operating conditions on separate
        # slides when the complete explanation would crowd a single page.
        details = points
        continuation = None
        if sum(len(point.text.split()) for point in points) > 120:
            split = 3 if row.target else 0
            details = points[:split]
            continuation = DeckSlide(
                topic=f"outlook_{row.months}_month_conditions",
                title=f"{row.months}-month drivers and risks",
                takeaway="The forecast depends on operating progress and has explicit reconsideration conditions.",
                narrative="Model scenarios and assumptions, not announced company guidance.",
                source_ids=ids,
                blocks=[TextBlock(points=points[split:])],
            )
        slides.append(
            DeckSlide(
                topic=f"outlook_{row.months}_month",
                title=f"{row.months}-month investment outlook",
                takeaway=row.summary,
                narrative=context,
                source_ids=ids,
                blocks=[
                    TableBlock(
                        headings=["Measure", "Outlook"],
                        rows=[
                            DeckTableRow(cells=[label, value], source_ids=ids)
                            for label, value in cells
                        ],
                    ),
                    *([TextBlock(points=details)] if details else []),
                ],
            )
        )
        if continuation is not None:
            slides.append(continuation)
    return slides
