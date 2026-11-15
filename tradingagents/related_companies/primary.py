"""Primary comparison returns share peer conventions but never replace price anchors."""

from datetime import date

from tradingagents.agents.utils.decision_integrity import verified_price_context

from .providers import _performance


def augment_primary_performance(state: dict, update: dict) -> dict:
    bundle = dict(update.get("related_company_research") or {})
    if not bundle:
        return update
    bundle["primary_performance"] = []
    bundle["primary_performance_coverage"] = []
    peers = any(
        any(proof.get("role") == "competitor" for proof in entity.get("roles", []))
        for entity in bundle.get("entities", [])
    )
    if not peers or bundle.get("status") == "disabled":
        return update | {"related_company_research": bundle}
    as_of = str(state.get("trade_date") or state.get("as_of_date") or "")
    if state.get("analysis_mode") != "live" or as_of != date.today().isoformat():
        bundle["primary_performance_coverage"].append(
            "Historical comparison returns unavailable without point-in-time adjustment verification."
        )
        return update | {"related_company_research": bundle}
    try:
        rows, coverage = _performance(
            str(state["company_of_interest"]),
            as_of,
            verified_price_context(state)["quote_currency"],
        )
        bundle["primary_performance"] = rows
        bundle["primary_performance_coverage"].extend(coverage)
    except Exception:
        bundle["primary_performance_coverage"].append("Primary comparison return data unavailable.")
    return update | {"related_company_research": bundle}
