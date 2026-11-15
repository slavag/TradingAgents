"""Deterministic related-company views; primary and secondary facts stay isolated.

This module formats admitted research only. It never retrieves data, requests a
model, changes the primary ledger, or derives a target from a related price.
"""

from __future__ import annotations

from calendar import monthrange
from collections.abc import Mapping
from dataclasses import asdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from tradingagents.research_ledger import ResearchLedger, ResearchSource

if TYPE_CHECKING:
    from tradingagents.investor_deck import DeckSlide


def _day(value: Any) -> str:
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except ValueError:
        return ""


def _number(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except InvalidOperation:
        return None


def _ids(rows) -> list[str]:
    return list(dict.fromkeys(key for row in rows for key in row if key))


def _public(entity) -> bool:
    return bool(entity.get("symbol")) and entity.get("identity_status") in {
        "source_verified",
        "live_identity_verified",
        "primary",
    }


def _label(entity) -> str:
    return (
        f"{entity['name']} ({entity['symbol']})"
        if entity.get("symbol")
        else f"{entity['name']} (private or unresolved)"
    )


def _source_map(bundle) -> dict[str, ResearchSource]:
    result = {}
    as_of = _day(bundle.get("as_of"))
    groups = [(bundle.get("target", "Primary company"), bundle.get("sources", []))]
    groups += [
        (e.get("name", "Related company"), e.get("sources", [])) for e in bundle.get("entities", [])
    ]
    for name, rows in groups:
        for row in rows:
            key = row.get("evidence_id")
            published = _day(row.get("published_at"))
            if (
                not key
                or row.get("status", "available") not in {"available", "partial"}
                or (published and published > as_of)
            ):
                continue
            title = str(row.get("title") or row.get("category") or "Research source")
            result[key] = ResearchSource(
                evidence_id=key,
                provider=str(row.get("source") or "unknown"),
                title=f"Related company — {name}: {title}",
                url=row.get("url"),
                published_at=row.get("published_at"),
                section=row.get("section"),
                status=row.get("status", "available"),
                accession=row.get("accession"),
                form=row.get("form"),
                document_type=row.get("document_type"),
                report_period=row.get("report_period"),
                coverage=row.get("coverage"),
            )
    return result


def _eligible(entity, as_of, sources):
    if not _public(entity):
        return []
    admitted = []
    own = set(entity.get("_source_ids", []))
    for original in entity.get("facts", []):
        row = dict(original)
        value = _number(row.get("value"))
        end = _day(row.get("period_end"))
        refs = row.get("source_ids") or []
        if (
            value is not None
            and end
            and end <= as_of
            and refs
            and all(
                key in sources
                and key in own
                and (
                    not _day(sources[key].published_at) or _day(sources[key].published_at) <= as_of
                )
                for key in refs
            )
        ):
            row.update(value=value, period_end=end)
            admitted.append(row)
    return admitted


def _latest(facts, metric, kind="quarterly", basis="gaap"):
    candidates = [
        f
        for f in facts
        if f["metric"] == metric and f.get("period_kind") == kind and f.get("basis") == basis
    ]
    if not candidates:
        return None
    end = max(f["period_end"] for f in candidates)
    latest = [f for f in candidates if f["period_end"] == end]
    # Conflicting provider/filing copies do not establish a unique comparable fact.
    if len({(f["value"], f.get("unit")) for f in latest}) != 1:
        return None
    return latest[0]


def _money_unit(unit):
    return unit in {
        "USD",
        "EUR",
        "GBP",
        "JPY",
        "ILS",
        "CAD",
        "AUD",
        "CHF",
        "CNY",
        "HKD",
        "KRW",
        "INR",
        "SEK",
        "NOK",
        "DKK",
        "TWD",
        "SGD",
        "BRL",
        "MXN",
        "ZAR",
    }


def _compatible(a, b):
    return bool(
        a
        and b
        and a.get("unit")
        and a["unit"] == b["unit"]
        and a["period_end"] == b["period_end"]
        and a["period_kind"] == b["period_kind"]
        and a["basis"] == b["basis"]
        and _money_unit(a["unit"])
    )


def _shown(fact):
    return (
        f"{fact['value']} {fact['unit']}; {fact.get('period_label', '')}, ended {fact['period_end']}; "
        f"{fact['period_kind']}, {fact['basis'].upper()}"
    )


def _financial_rows(entity, as_of, sources):
    if not _public(entity):
        return [
            (
                "Financial comparison",
                "N/A — private or unresolved identity; qualitative relationship only.",
                [],
            )
        ]
    facts = _eligible(entity, as_of, sources)
    revenue = _latest(facts, "net_sales")
    income = _latest(facts, "operating_income")
    net = _latest(facts, "net_income")
    rows = [
        (
            "Revenue",
            _shown(revenue)
            if revenue
            else "N/A — dated, unambiguous quarterly GAAP revenue unavailable.",
            revenue["source_ids"] if revenue else [],
        )
    ]
    old = None
    if revenue:
        earlier = [
            f
            for f in facts
            if f["metric"] == "net_sales"
            and f.get("basis") == revenue["basis"]
            and f.get("period_kind") == "quarterly"
            and f.get("unit") == revenue["unit"]
            and 350
            <= (
                date.fromisoformat(revenue["period_end"]) - date.fromisoformat(f["period_end"])
            ).days
            <= 380
        ]
        old = _latest(earlier, "net_sales")
    growth = (
        None
        if not old or old["value"] <= 0 or not _money_unit(revenue["unit"])
        else (revenue["value"] / old["value"] - 1) * 100
    )
    rows.append(
        (
            "Revenue growth",
            f"{growth:.1f}% YoY; quarters ended {old['period_end']} / {revenue['period_end']}; {revenue['unit']}, GAAP"
            if growth is not None
            else "N/A — matched prior-year quarter, currency and unit required.",
            _ids([revenue["source_ids"], old["source_ids"]]) if growth is not None else [],
        )
    )
    margin = (
        (income["value"] / revenue["value"] * 100)
        if _compatible(revenue, income) and revenue["value"] > 0
        else None
    )
    rows.append(
        (
            "Operating margin",
            f"{margin:.1f}%; quarter ended {revenue['period_end']}, GAAP; compatible {revenue['unit']} inputs"
            if margin is not None
            else "N/A — same-quarter GAAP revenue and operating income with matching currency/units required.",
            _ids([revenue["source_ids"], income["source_ids"]]) if margin is not None else [],
        )
    )
    rows.append(
        (
            "Net income",
            _shown(net) if net else "N/A — comparable quarterly GAAP net income unavailable.",
            net["source_ids"] if net else [],
        )
    )
    # Quarterly losses cannot establish trailing-twelve-month earnings. Prefer
    # a reported dimensionless multiple; currency compatibility is needed only
    # when deriving our own ratio from dated price and EPS inputs.
    eps = _latest(facts, "diluted_eps", "ttm")
    reported_pe = _latest(facts, "pe_ttm", "ttm", "market")
    if reported_pe and reported_pe.get("unit") != "multiple":
        reported_pe = None
    price = _latest(facts, "close", "instant", "market")
    if (eps and eps["value"] <= 0) or (reported_pe and reported_pe["value"] <= 0):
        if eps and eps["value"] <= 0:
            valuation = f"N/M — nonpositive reported TTM GAAP EPS, period ended {eps['period_end']}; no meaningful earnings multiple."
            refs = eps["source_ids"]
            if reported_pe and reported_pe["value"] > 0:
                valuation += f" Conflicting positive vendor-reported TTM multiple at snapshot {reported_pe['period_end']} is not used."
                refs = _ids([refs, reported_pe["source_ids"]])
        else:
            valuation = f"N/M — nonpositive vendor-reported TTM P/E, snapshot {reported_pe['period_end']}; no meaningful earnings multiple."
            refs = reported_pe["source_ids"]
    elif reported_pe:
        valuation = f"{reported_pe['value']}x; vendor-reported TTM P/E, snapshot {reported_pe['period_end']}; dimensionless provider ratio, not an independently calculated currency conversion."
        refs = reported_pe["source_ids"]
    elif (
        eps
        and price
        and eps["value"] > 0
        and price["value"] > 0
        and eps["unit"].endswith("_per_share")
        and _money_unit(price["unit"])
        and price["unit"] == eps["unit"].removesuffix("_per_share")
        and price["period_end"] >= eps["period_end"]
    ):
        valuation = f"{price['value'] / eps['value']:.1f}x; computed from compatible-currency inputs; TTM GAAP EPS ended {eps['period_end']}; quote {price['period_end']}, {price['unit']}"
        refs = _ids([eps["source_ids"], price["source_ids"]])
    else:
        valuation = "N/A — TTM coverage unavailable or incomplete: a dated reported TTM multiple or compatible-currency dated price and TTM GAAP EPS is required; no quarterly annualization."
        refs = []
        if net and net["value"] < 0:
            valuation += (
                f" The reported quarterly loss ({_shown(net)}) does not establish a TTM loss."
            )
            refs = net["source_ids"]
    rows.append(("P/E", valuation, refs))
    strength = [_latest(facts, key, "instant") for key in ("cash", "total_debt")]
    visible = [f for f in strength if f]
    rows.append(
        (
            "Financial strength",
            "; ".join(f"{f['metric']}: {_shown(f)}" for f in visible)
            + "; liquidity adequacy and covenant headroom unverified."
            if visible
            else "N/A — dated cash, debt and liquidity coverage unavailable.",
            _ids([f["source_ids"] for f in visible]),
        )
    )
    return rows


def _news_lines(entity, bundle, sources):
    news = entity.get("news") or {}
    state = news.get("status", "unavailable")
    prefix = "News windows: latest 7 days; prior 30-day candidates; "
    lines = [(prefix + "materiality unverified.", [])]
    keys = list(dict.fromkeys(news.get("latest_7_days", []) + news.get("material_30_days", [])))
    cutoff = date.fromisoformat(bundle["as_of"]) - timedelta(days=30)
    own = set(entity.get("_source_ids", []))
    valid = [
        sources[key]
        for key in keys
        if key in sources
        and key in own
        and _day(sources[key].published_at)
        and cutoff.isoformat() <= _day(sources[key].published_at) <= bundle["as_of"]
    ]
    for source in sorted(valid, key=lambda item: item.published_at or "", reverse=True):
        recent = (
            _day(source.published_at)
            >= (date.fromisoformat(bundle["as_of"]) - timedelta(days=7)).isoformat()
        )
        lines.append(
            (
                f"{'Latest 7 days' if recent else '30-day candidate'}: {source.title}; published {_day(source.published_at)}. "
                + (f"Source: {source.url}" if source.url else "Source URL unavailable."),
                [source.evidence_id],
            )
        )
    if not valid:
        lines.append(
            (
                "No reported news in available coverage; this does not establish an absence of issues."
                if state == "clean"
                else "News coverage unavailable or incomplete; no clean-search conclusion is supported.",
                [],
            )
        )
    return lines


def _performance_rows(entities, bundle, sources, details):
    as_of = bundle["as_of"]
    primary = bundle.get("primary_performance") or []
    reference = {r.get("months"): r for r in primary}
    # With no primary series, use the first admitted peer only as a convention
    # reference, explicitly retaining the missing primary row.
    if not reference:
        reference = {
            r.get("months"): r for e in entities[1:] if _public(e) for r in e.get("performance", [])
        }
    rows = []
    for entity in entities:
        cells, refs = [_label(entity)], []
        series = (
            primary if entity.get("identity_status") == "primary" else entity.get("performance", [])
        )
        for months in (1, 3, 6, 12):
            candidates = [r for r in series if r.get("months") == months] if _public(entity) else []
            row = candidates[0] if len(candidates) == 1 else None
            ref = reference.get(months)
            reason = (
                "missing comparable dated closes"
                if _public(entity)
                else "private or unresolved identity"
            )
            valid = bool(
                row
                and _day(row.get("start_date"))
                and _day(row.get("end_date"))
                and row["start_date"] < row["end_date"] <= as_of
                and row.get("currency")
                and row.get("adjustment")
                and _number(row.get("return_pct")) is not None
                and row.get("source")
            )
            if valid:
                cutoff_day = date.fromisoformat(as_of)
                index = cutoff_day.year * 12 + cutoff_day.month - 1 - months
                year, month = divmod(index, 12)
                month += 1
                anchor = date(year, month, min(cutoff_day.day, monthrange(year, month)[1]))
                start = date.fromisoformat(row["start_date"])
                end = date.fromisoformat(row["end_date"])
                if not 0 <= (anchor - start).days <= 5 or not 0 <= (cutoff_day - end).days <= 5:
                    valid, reason = False, "stale or incompatible horizon dates"
            if valid and ref and row.get("adjustment") != ref.get("adjustment"):
                valid, reason = False, "incompatible adjustment convention"
            if valid and ref and row.get("end_date") != ref.get("end_date"):
                valid, reason = False, "incompatible cutoff date"
            if valid:
                key = f"related-performance:{entity.get('symbol')}:{months}:{row['end_date']}"
                sources[key] = ResearchSource(
                    key,
                    str(row["source"]),
                    f"Related company — {entity['name']}: {months}-month regular-session price return",
                    None,
                    row["end_date"],
                    "regular-session performance",
                    "available",
                )
                cells.append(f"{row['return_pct']}% ({row['currency']})")
                refs.append(key)
                details.append(
                    (
                        f"{_label(entity)} — {months}-month regular-session return: {row['return_pct']}%; {row['start_date']} to {row['end_date']}; local {row['currency']}; {row['adjustment']}.",
                        [key],
                    )
                )
            else:
                cells.append("N/A")
                details.append(
                    (f"{_label(entity)} — {months}-month return unavailable: {reason}.", [])
                )
        rows.append((cells, refs))
    return rows


def _sections(bundle, primary_ledger=None):
    """Shared content model keeps Markdown and slide coverage statements aligned."""
    if bundle.get("status") == "disabled" or not _day(bundle.get("as_of")):
        return [], {}
    as_of = bundle["as_of"]
    sources = _source_map(bundle)
    if primary_ledger is not None:
        sources.update(primary_ledger.sources)
    entities = []
    for original in bundle.get("entities", []):
        entity = dict(original)
        entity["_source_ids"] = [r.get("evidence_id") for r in original.get("sources", [])]
        entities.append(entity)
    peers = [
        e
        for e in entities
        if any(
            p.get("role") == "competitor" and p.get("source_id") in sources
            for p in e.get("roles", [])
        )
    ][:5]
    main = {
        "name": str(bundle.get("target") or "Primary company"),
        "symbol": bundle.get("target"),
        "identity_status": "primary",
        "facts": [asdict(f) for f in primary_ledger.facts] if primary_ledger else [],
        "_source_ids": list(primary_ledger.sources) if primary_ledger else [],
    }
    sections = []
    overview = []
    for entity in peers:
        for proof in entity.get("roles", []):
            if proof.get("role") == "competitor" and proof.get("source_id") in sources:
                overview.append(
                    (
                        f"{_label(entity)} — competitor; competitive overlap is described in the admitted disclosure. Disclosed as of {proof.get('relationship_date', 'unknown')}: “{proof.get('quote', '')}” [{proof['source_id']}]. Financial identity: {entity.get('identity_status', 'unresolved')}.",
                        [proof["source_id"]],
                    )
                )
    overview.append(
        (
            f"{len(peers)} source-verified competitor(s) shown; up to five are retained. Missing identities or metrics are coverage gaps. Primary-company facts and peer facts are separate. Different fiscal calendars remain visible; amounts are in reported currency with no inferred FX conversion. A peer's growth or share-price gain alone does not demonstrate lost primary-company sales or support a target change.",
            [],
        )
    )
    sections.append(
        {
            "topic": "related_competitors",
            "title": "Direct competitors",
            "takeaway": "Disclosed overlap defines the peer set.",
            "points": overview,
        }
    )
    comparison_rows = []
    for entity in [main] + peers:
        metrics = {
            metric: (value, refs) for metric, value, refs in _financial_rows(entity, as_of, sources)
        }
        facts = _eligible(entity, as_of, sources)
        revenue = _latest(facts, "net_sales")
        cells = [
            _label(entity),
            f"{revenue['value']} {revenue['unit']}; {revenue['period_end']}" if revenue else "N/A",
        ]
        refs = list(revenue["source_ids"]) if revenue else []
        for metric in ("Revenue growth", "Operating margin", "P/E"):
            value, source_ids = metrics.get(metric, ("N/A", []))
            cells.append(
                "N/A"
                if value.startswith("N/A")
                else "N/M"
                if value.startswith("N/M")
                else value.split(";", 1)[0]
            )
            refs.extend(source_ids)
        comparison_rows.append((cells, list(dict.fromkeys(refs))))
    sections.append(
        {
            "topic": "related_financial_comparison",
            "title": "Peer financial comparison",
            "takeaway": "Each issuer uses its own latest unambiguous reported GAAP quarter; unavailable figures are not inferred.",
            "headings": [
                "Company",
                "Revenue / quarter end",
                "Growth YoY",
                "Operating margin",
                "P/E",
            ],
            "rows": comparison_rows,
            "points": [
                (
                    "Different quarter-end dates and local currencies limit direct scale comparisons. Full period, basis, units, input compatibility and coverage appear in the issuer detail pages. Valuation premiums, relative financial resilience and competitive wins cannot be established where the required comparable facts are absent.",
                    [],
                )
            ],
        }
    )
    for entity in [main] + peers:
        rows = _financial_rows(entity, as_of, sources)
        sections.append(
            {
                "topic": f"related_financial_{entity.get('symbol') or entity['name']}",
                "title": f"Business and financial comparison — {_label(entity)}",
                "takeaway": "Reported periods, accounting basis and unavailable inputs remain explicit.",
                "rows": [([metric, value], refs) for metric, value, refs in rows],
                "headings": ["Metric", "Reported value / comparability"],
                "points": [
                    (
                        "Primary-company ledger only."
                        if entity is main
                        else "Related-company facts only; no peer price is a primary reference or forecast.",
                        [],
                    )
                ]
                + (
                    []
                    if entity is main
                    else _news_lines(entity, bundle, sources)
                    + [(f"Coverage gap: {gap}", []) for gap in entity.get("coverage", [])]
                ),
            }
        )
    performance_details = []
    performance_rows = _performance_rows([main] + peers, bundle, sources, performance_details)
    sections.append(
        {
            "topic": "related_performance",
            "title": "Regular-session market performance",
            "takeaway": "Local-currency historical returns describe price performance, not forecasts.",
            "headings": ["Company", "1 month", "3 months", "6 months", "12 months"],
            "rows": performance_rows,
            "points": [
                (
                    "Returns require comparable cutoff dates and adjustment conventions. N/A identifies missing primary or peer history; cross-currency returns are local-currency, not a common-currency investment comparison. No peer recommendation or price target is implied.",
                    [],
                )
            ]
            + performance_details
            + [(str(gap), []) for gap in bundle.get("primary_performance_coverage", [])],
        }
    )
    for role, title in [("supplier", "Suppliers"), ("customer", "Customers")]:
        found = False
        for entity in entities:
            proofs = [
                p
                for p in entity.get("roles", [])
                if p.get("role") == role and p.get("source_id") in sources
            ]
            if not proofs:
                continue
            found = True
            points = [
                (
                    f"{_label(entity)}; {role} disclosed as of {p.get('relationship_date', 'unknown')} (disclosure date, not relationship inception or proof it remains active): “{p.get('quote', '')}” [{p['source_id']}].",
                    [p["source_id"]],
                )
                for p in proofs
            ]
            points += _news_lines(entity, bundle, sources)
            mechanism = (
                "capacity, pricing, delivery or service continuity could transmit to primary-company costs or availability"
                if role == "supplier"
                else "purchasing budgets, renewals or deployments could transmit to primary-company demand"
            )
            points.append(
                (
                    f"Exposure is conditional: if the disclosed relationship continues and the reported development affects the relevant product or service, {mechanism}. The news candidates do not establish actual order changes, production disruption or a causal impact. Materiality, concentration and the six-/twelve-month earnings effect remain unverified unless separately quantified in primary evidence. Monitor issuer confirmation of the relationship, affected product, timing and magnitude before changing the outlook.",
                    [p["source_id"] for p in proofs],
                )
            )
            points += [(f"Coverage gap: {gap}", []) for gap in entity.get("coverage", [])]
            sections.append(
                {
                    "topic": f"related_{role}_{entity.get('symbol') or entity['name']}",
                    "title": f"{title} — {_label(entity)}",
                    "takeaway": "Dated relationship evidence supports conditional exposure analysis.",
                    "points": points,
                }
            )
        if not found:
            sections.append(
                {
                    "topic": f"related_{role}",
                    "title": title,
                    "takeaway": "Named relationship coverage unavailable.",
                    "coverage_only": True,
                    "points": [
                        (
                            "No admitted named relationship evidence in available coverage; anonymous counterparties are not assigned guessed names, symbols, orders or impacts.",
                            [],
                        )
                    ],
                }
            )
    sections.append(
        {
            "topic": "related_coverage",
            "title": "Related-company coverage",
            "takeaway": f"Research as of {as_of}; status {bundle.get('status', 'unavailable')}.",
            "points": [(str(gap), []) for gap in bundle.get("coverage", [])]
            or [
                (
                    "No additional collection gaps recorded; metric-specific and news limitations above still apply.",
                    [],
                )
            ],
        }
    )
    return sections, sources


def build_related_slides(
    state: Mapping[str, Any], primary_ledger: ResearchLedger
) -> tuple[list[DeckSlide], dict[str, ResearchSource]]:
    """Build evidence-preserving sections without mutating state or the ledger."""
    # Lazy import avoids coupling source parsing to the deck model on module load.
    from tradingagents.investor_deck import (
        DeckPoint,
        DeckSlide,
        DeckTableRow,
        TableBlock,
        TextBlock,
    )

    bundle = state.get("related_company_research") or {}
    cutoff = str(state.get("trade_date") or state.get("as_of_date") or bundle.get("as_of") or "")
    if not bundle or bundle.get("as_of") != cutoff:
        return [], {}
    sections, sources = _sections(bundle, primary_ledger)
    slides = []
    for section in sections:
        if section.get("coverage_only"):
            continue
        # One complete statement per block lets the HTML renderer paginate without
        # clipping relationship quotes, source statements or adverse qualifications.
        points = section.get("points", [])
        blocks = []
        if section.get("rows"):
            blocks.append(
                TableBlock(
                    headings=section["headings"],
                    rows=[
                        DeckTableRow(cells=cells, source_ids=refs or ["related-coverage"])
                        for cells, refs in section["rows"]
                    ],
                )
            )
        for text, refs in points:
            blocks.append(
                TextBlock(points=[DeckPoint(text=text, source_ids=refs or ["related-coverage"])])
            )
        # Soft page budget: split only between complete statements / tables.
        # A long exact disclosure remains intact instead of being prefix-clipped.
        pages, page, words = [], [], 0
        for block in blocks:
            block_words = len(block.model_dump_json().split())
            if page and words + block_words > 270:
                pages.append(page)
                page, words = [], 0
            page.append(block)
            words += block_words
        if page:
            pages.append(page)
        for index, page in enumerate(pages):
            refs = []
            for block in page:
                items = block.rows if block.kind == "table" else block.points
                refs.extend(key for item in items for key in item.source_ids)
            slides.append(
                DeckSlide(
                    topic=section["topic"] + (f"_continuation_{index + 1}" if index else ""),
                    title=section["title"] + (f" — continued {index + 1}" if index else ""),
                    takeaway=section["takeaway"],
                    source_ids=list(dict.fromkeys(refs)),
                    blocks=page,
                )
            )
    if slides:
        sources["related-coverage"] = ResearchSource(
            "related-coverage",
            "research_coverage",
            f"{bundle.get('target', 'Primary company')} — related-company retrieval and comparison limitations",
            None,
            cutoff,
            "coverage",
            str(bundle.get("status") or "partial"),
        )
    return slides, {
        key: source for key, source in sources.items() if key not in primary_ledger.sources
    }


def render_related_report(
    bundle: Mapping[str, Any], primary_ledger: ResearchLedger | None = None
) -> str:
    """Render detailed Markdown with original links and complete qualifications.

    Without a primary ledger, the primary financial row states the coverage gap.
    Integrations can pass the same isolated ledger used by the investor deck.
    """
    sections, sources = _sections(bundle, primary_ledger)
    if not sections:
        return ""
    lines = [
        f"# Related-company research — {bundle.get('target', 'Primary company')}",
        f"As of {bundle['as_of']}. Primary-company financial facts use only the primary research ledger; secondary facts are never substituted.",
    ]
    for section in sections:
        lines += ["", "## " + section["title"], "", section["takeaway"]]
        for text, refs in section.get("points", []):
            lines += ["", text + (" [" + ", ".join(refs) + "]" if refs else "")]
        if section.get("rows"):
            lines += [
                "",
                "| " + " | ".join(section["headings"]) + " |",
                "| " + " | ".join("---" for _ in section["headings"]) + " |",
            ]
            for cells, refs in section["rows"]:
                display_cells = list(cells)
                if refs:
                    display_cells[-1] += " [" + ", ".join(refs) + "]"
                lines.append(
                    "| "
                    + " | ".join(
                        str(c).replace("|", "\\|").replace("\n", " ") for c in display_cells
                    )
                    + " |"
                )

    lines += ["", "## Sources", ""]
    lines += [
        f"- [{key}] {source.title}; {source.published_at or 'publication date unavailable'}"
        + (
            f" — [{source.provider}]({source.url})"
            if source.url
            else f" — {source.provider}; URL unavailable"
        )
        for key, source in sources.items()
    ]
    return "\n".join(lines) + "\n"
