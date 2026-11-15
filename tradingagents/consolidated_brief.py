"""Concise, complete extractive briefs; detailed research is never character-sliced."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

_CRITICAL = re.compile(r"\b(?:if|unless|only|subject to|invalidat\w*|exit|stop|do not|reconsider)\b", re.I)
_QUALIFIER = re.compile(r"\b(?:unaudited|preliminary|estimated?|non.gaap|adjusted|unverified|unavailable|not verified|not comparable|year.to.date|cumulative|currency|fiscal|coverage|insufficient)\b", re.I)
_RELEVANCE = re.compile(r"\b(?:risk|cash|margin|growth|revenue|debt|valuation|demand|supplier|customer|competit\w*)\b", re.I)
_EVENT = re.compile(r"\b(?:shortage|outage|capacity|restrict\w*|cancel\w*|renew\w*|order\w*|budget|demand|credit|default|bankrupt\w*)\b", re.I)


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def complete_statements(value: Any) -> list[str]:
    """Select whole source statements, preserving decimals and common abbreviations."""
    raw = _text(value)
    lines = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith(("|", "#")) or line.lower().startswith("extended-hours market reaction:") or re.match(r"(?i)^(?:if you want|would you like|i can now|extended-hours market reaction:)\b", line):
            continue
        line = re.sub(r"^[-*]\s+", "", line).replace("**", "")
        lines.append(line)
    raw = " ".join(lines)
    for token in ("U.S.", "U.K.", "e.g.", "i.e.", "Dr.", "Mr.", "Ms.", "Inc.", "Ltd."):
        raw = raw.replace(token, token.replace(".", "\u2024"))
    result = []
    for part in re.split(r"(?<=[.!?])\s+(?=[A-Z0-9\"'])", raw):
        part = " ".join(part.replace("\u2024", ".").split())
        if not part or re.search(r"\.{3}|…", part):
            continue
        if part[-1:] in {",", ":", ";"} or re.search(r"\b(?:and|or|because|with|of|to|that)$", part, re.I):
            continue
        if part[-1:] not in ".!?":
            part += "."
        if part not in result:
            result.append(part)
    return result


def summarize_complete(value: Any, count: int = 2, *, preserve_conditions: bool = False) -> str:
    statements = complete_statements(value)
    chosen = set(range(min(count, len(statements))))
    if preserve_conditions:
        chosen.update(i for i, sentence in enumerate(statements) if _CRITICAL.search(sentence) or _DECISION_CONDITION.search(sentence))
    return " ".join(sentence for i, sentence in enumerate(statements) if i in chosen)


_DECISION_CONDITION = re.compile(
    r"^(?:exit|abandon|reconsider|avoid|do not)\b|\b(?:invalidates?|invalidating|only if|unless|contingent on|conditional on|depends? on|requires? approval)\b", re.I,
)
_QUALITY_GROUPS = (
    re.compile(r"\b(?:preliminary|unaudited)\b", re.I),
    re.compile(r"(?:different|not match|not comparable|not be mixed|do not match).{0,70}(?:period|quarter)|(?:period|quarter).{0,90}(?:not match|not be mixed|not comparable)", re.I),
    re.compile(r"(?:news|estimates?|coverage).{0,70}(?:unavailable|missing|not verified)", re.I),
    re.compile(r"(?:fair.value|valuation).{0,90}(?:unavailable|missing|cannot)|(?:missing|preventing).{0,90}(?:fair.value|valuation)", re.I),
)


def _standalone(sentence: str) -> bool:
    return not re.match(r"(?i)^(?:these|those|this|that|they|it)\s", sentence)


def _quality_summaries(texts: list[Any]) -> list[str]:
    statements = [sentence for text in texts for sentence in complete_statements(text)]
    result = []
    for pattern in _QUALITY_GROUPS:
        matched = [sentence for sentence in statements if pattern.search(sentence) and (_standalone(sentence) or re.match(r"(?i)^(?:these|those|this|that) (?:figures|values|financial statements|metrics|data)\b", sentence))]
        if matched:
            # One complete, concise expression of each distinct limitation.
            result.append(min(matched, key=lambda value: (len(value.split()), len(value))))
    return list(dict.fromkeys(result))


def _field(text: Any, label: str) -> str:
    match = re.search(rf"(?ms)^\*\*{re.escape(label)}\*\*:\s*(.*?)(?=^\*\*[^*]+\*\*:|\Z)", _text(text))
    return match.group(1).strip() if match else ""


def _safe_link(value: Any) -> str | None:
    if not isinstance(value, str) or any(c.isspace() for c in value):
        return None
    parsed = urlsplit(value)
    return value if parsed.scheme == "https" and parsed.hostname and not parsed.username and not parsed.password else None


@dataclass
class StockBrief:
    highlights: list[str] = field(default_factory=list)
    guidance: list[tuple[str, str]] = field(default_factory=list)
    outlooks: list[tuple[str, str, str]] = field(default_factory=list)
    links: list[tuple[str, str]] = field(default_factory=list)
    actions: dict[str, str] = field(default_factory=dict)
    horizon_notes: list[str] = field(default_factory=list)


def _related_highlights(bundle: dict) -> tuple[list[str], list[tuple[str, str]]]:
    if not bundle or bundle.get("status") == "disabled":
        return [], []
    statements, links = [], []
    for role, title in (("competitor", "Direct competitors"), ("supplier", "Suppliers"), ("customer", "Customers")):
        entities = [entity for entity in bundle.get("entities", []) if any(p.get("role") == role for p in entity.get("roles", []))]
        if not entities:
            statements.append(f"{title}: verified relationship coverage is unavailable.")
            continue
        names = ", ".join(str(entity["name"]) for entity in entities)
        events = []
        for entity in entities:
            ids = set((entity.get("news") or {}).get("material_30_days", []))
            for record in entity.get("sources", []):
                if record.get("evidence_id") in ids and _EVENT.search(str(record.get("title") or "")):
                    events.append((len(_EVENT.findall(record["title"])), str(record.get("published_at") or ""), entity, record))
        if events:
            _, published, entity, record = max(events, key=lambda row: (row[0], row[1]))
            headline = summarize_complete(record.get("title"), 1)
            statements.append(f"{title}: {names}. Reported development at {entity['name']} ({published[:10]}): {headline} Exposure depends on the disclosed relationship; the event alone does not establish an effect on the stock.")
            href = _safe_link(record.get("url"))
            if href:
                links.append((f"{title} news source", href))
        else:
            status = {(entity.get("news") or {}).get("status", "unavailable") for entity in entities}
            coverage = ("No relevant news was found in the searched coverage" if status == {"clean"}
                        else "Recent news coverage is incomplete" if "unavailable" in status
                        else "Dated news is available for review in the detailed research")
            statements.append(f"{title}: {names}. {coverage}; this does not establish that there are no risks.")
    return statements, links


def build_stock_brief(result: dict) -> StockBrief:
    state = result.get("final_state") or {}
    decision = state.get("portfolio_decision") or {}
    rendered = state.get("final_trade_decision") or ""
    brief = StockBrief()
    brief.actions = {"Existing position": str(result.get("existing_position_action") or ""),
                     "New position": str(result.get("new_position_action") or "")}
    summary = decision.get("executive_summary") or _field(rendered, "Executive Summary") or rendered
    thesis = decision.get("investment_thesis") or _field(rendered, "Investment Thesis")
    if selected := summarize_complete(summary, 4, preserve_conditions=True):
        brief.highlights.append(selected)
    if not thesis:
        trader_reasoning = _field(state.get("trader_investment_plan"), "Reasoning")
        if text := summarize_complete(trader_reasoning, 1, preserve_conditions=True):
            brief.highlights.append("Trader reasoning: " + text)
    candidates = complete_statements(thesis)
    ranked = sorted(((i, sentence) for i, sentence in enumerate(candidates) if _standalone(sentence)), key=lambda row: (-len(_RELEVANCE.findall(row[1])), row[0]))
    selected_indices = {i for i, _ in ranked[:2]}
    selected_indices.update(i for i, sentence in enumerate(candidates) if _DECISION_CONDITION.search(sentence))
    for i, sentence in enumerate(candidates):
        if i in selected_indices and not any(sentence in text for text in brief.highlights):
            brief.highlights.append(sentence)
    analyst_texts = [state.get(key) for key in ("fundamentals_report", "market_report", "news_report")]
    # The Portfolio Manager has already synthesized these reports. Use their
    # complete facts only when that synthesis is absent; keep material data caveats.
    if not thesis:
        for text in analyst_texts:
            statements = [sentence for sentence in complete_statements(text) if _standalone(sentence)]
            if statements:
                best = max(enumerate(statements), key=lambda row: (len(_RELEVANCE.findall(row[1])), -row[0]))[1]
                if not any(best in text for text in brief.highlights):
                    brief.highlights.append(best)
    for caveat in _quality_summaries([thesis, *analyst_texts]):
        if not any(caveat in text for text in brief.highlights):
            brief.highlights.append(caveat)
    accepted = str(result.get("target_validation_status") or "").lower() == "accepted"
    if accepted:
        if rationale := summarize_complete(result.get("target_summary"), 1, preserve_conditions=True):
            brief.highlights.append("Target outlook: " + rationale)
        if assumptions := " ".join(complete_statements(result.get("target_assumptions"))):
            brief.highlights.append("Target assumptions: " + assumptions)
        if result.get("target_method"):
            method = result["target_method"]
            if result.get("target_basis") == "Source level" and len(method.split()) > 20 and re.search(r"\b(?:bollinger|technical|SMA|EMA|support level|resistance level)\b", method, re.I):
                method = "Source-anchored technical scenario."
            brief.highlights.append("Target Method: " + summarize_complete(method, 1))
    risks = summarize_complete(result.get("target_risks"), 1, preserve_conditions=True) if accepted else ""
    if risks and not any(risks in text for text in brief.highlights):
        brief.highlights.append("Target Risks: " + risks)
    related_bundle = state.get("related_company_research") or {}
    if related_bundle.get("as_of") != str(result.get("analysis_date") or state.get("trade_date") or ""):
        related_bundle = {}
    related, links = _related_highlights(related_bundle)
    brief.highlights.extend(related)
    brief.links.extend(links)
    for label, key in (("Existing position", "existing_position_summary"), ("New position", "new_position_summary"),
                       ("Confirmation", "conditional_confirmation"), ("Alternative", "conditional_alternative"),
                       ("Invalidation", "conditional_invalidation")):
        value = result.get(key)
        if value is None and key in {"existing_position_summary", "new_position_summary"}:
            value = decision.get(key)
        if selected := summarize_complete(value, 1, preserve_conditions=True):
            brief.guidance.append((label, selected))
    for row in state.get("portfolio_outlooks") or []:
        horizon = f"{row['months']}-month outlook — {row['end_date']}"
        rating = row.get("rating") or "Unavailable"
        confidence = row.get("recommendation_confidence_score")
        view = f"{rating} ({confidence}/100)" if confidence is not None else rating
        target = row.get("target")
        if target:
            value = (f"{target['price_target']:,.2f}" if target.get("price_target") is not None
                     else f"{target['target_low']:,.2f}–{target['target_high']:,.2f}")
            target_text = f"{value} {row.get('quote_currency') or '(currency unavailable)'} ({target.get('confidence_score')}/100; {target.get('target_basis') or 'Source level'})"
        else:
            target_text = "Unavailable: " + str(row.get("reason") or "no validated target").replace("_", " ")
        brief.outlooks.append((horizon, view, target_text))
        note = summarize_complete(row.get("summary"), 1, preserve_conditions=True)
        condition = summarize_complete(row.get("invalidation"), 1, preserve_conditions=True)
        if note or condition:
            brief.horizon_notes.append(f"{row['months']}-month view: {note}" + (f" What changes it: {condition}" if condition else ""))
    run_id, ticker = result.get("run_id"), str(result.get("ticker") or "")
    if isinstance(run_id, str) and re.fullmatch(r"[a-f0-9]{32}", run_id):
        base = f"/api/jobs/{run_id}/reports/{quote(ticker, safe='')}"
        brief.links.extend([("Full report", base + "/full"), ("Investor report", base + "/investor")])
    else:
        full_path = result.get("report_path")
        investor_path = result.get("investor_report_path")
        if isinstance(full_path, str) and full_path:
            brief.links.append(("Full report", Path(full_path).expanduser().resolve().as_uri()))
            if not investor_path and "_full_" in Path(full_path).name:
                investor_path = str(Path(full_path).with_name(Path(full_path).name.replace("_full_", "_investor_", 1)).with_suffix(".html"))
        if isinstance(investor_path, str) and investor_path:
            brief.links.append(("Investor report", Path(investor_path).expanduser().resolve().as_uri()))
    return brief


def render_brief_markdown(brief: StockBrief) -> str:
    lines = ["### Decision summary", *["- " + text for text in brief.highlights]]
    if brief.guidance:
        lines.extend(["", "### Position Plan"])
        for label, text in brief.guidance:
            action = brief.actions.get(label)
            lines.append(f"- {label}: **{action}** — {text}" if action else f"- {label}: {text}")
    if brief.outlooks:
        lines.extend(["", "### Forward outlooks", "Confidence scores are uncalibrated evidence-strength scores.", "",
                      "| Horizon | Recommendation / confidence | Target / confidence |", "| --- | --- | --- |"])
        lines.extend("| " + " | ".join(cell.replace("|", "\\|") for cell in row) + " |" for row in brief.outlooks)
    lines.extend("- " + note for note in brief.horizon_notes)
    if brief.links:
        lines.extend(["", "Details and sources: " + " · ".join(f"[{label}]({href})" for label, href in brief.links)])
    return "\n".join(lines)


def render_brief_html(brief: StockBrief, *, include_guidance: bool = True) -> str:
    parts = ["<section class='narrative-block consolidated-brief'><h3>Decision summary</h3><ul>",
             *["<li>" + escape(text) + "</li>" for text in brief.highlights], "</ul>"]
    if brief.guidance and include_guidance:
        parts.extend(["<h3>Position plan</h3><ul>", *[f"<li><strong>{escape(label)}:</strong> {escape(text)}</li>" for label, text in brief.guidance], "</ul>"])
    if brief.outlooks:
        parts.append("<h3>Forward outlooks</h3><p>Confidence scores are uncalibrated evidence-strength scores.</p><div class='table-scroll'><table><thead><tr><th>Horizon</th><th>Recommendation / confidence</th><th>Target / confidence</th></tr></thead><tbody>")
        parts.extend("<tr>" + "".join("<td>" + escape(cell) + "</td>" for cell in row) + "</tr>" for row in brief.outlooks)
        parts.append("</tbody></table></div>")
    parts.extend("<p>" + escape(note) + "</p>" for note in brief.horizon_notes)
    if brief.links:
        parts.append("<p>Details and sources: " + " · ".join(f'<a href="{escape(href, quote=True)}">{escape(label)}</a>' for label, href in brief.links) + "</p>")
    return "".join(parts) + "</section>"
