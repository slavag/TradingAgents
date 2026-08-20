"""Reusable report-tree writer shared by the CLI and the programmatic API.

Writes a run's per-section markdown (analysts, research, trading, risk,
portfolio) plus a consolidated ``complete_report.md`` under ``save_path``. The
CLI and ``TradingAgentsGraph.save_reports`` both call this, so a headless / API
run produces the same on-disk report tree a CLI run does.
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from tradingagents.forecasting.record_factory import forecast_record_from_state

_EVIDENCE_KEYS = (
    "market_report",
    "sentiment_report",
    "news_report",
    "fundamentals_report",
    "investment_plan",
    "trader_investment_plan",
    "past_context",
    "instrument_context",
    "company_of_interest",
    "asset_type",
)
_DECISION_OUTCOME_LABELS = (
    "Decision Status",
    "Rating",
    "Thesis",
    "Existing Position",
    "New Position",
    "Price Target",
    "Target Validation",
)


def _fingerprint(payload) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _decision_outcome(final_trade_decision) -> dict:
    text = str(final_trade_decision or "")
    outcome = {}
    for label in _DECISION_OUTCOME_LABELS:
        match = re.search(
            rf"(?im)^\s*\*\*{re.escape(label)}\*\*\s*:\s*(.+?)\s*$",
            text,
        )
        if match:
            outcome[label] = match.group(1).strip()
    return outcome or {"legacy_decision": text.strip()}


def build_run_manifest(
    final_state: dict,
    ticker: str,
    run_metadata: dict | None = None,
) -> dict:
    """Build stable fingerprints that make two saved runs comparable."""
    manifest = dict(run_metadata or {})
    risk_state = final_state.get("risk_debate_state") or {}
    evidence = {key: final_state.get(key) for key in _EVIDENCE_KEYS}
    evidence["risk_debate_history"] = (
        risk_state.get("history") if isinstance(risk_state, dict) else None
    )
    manifest.setdefault("schema_version", 1)
    manifest["ticker"] = ticker
    manifest.setdefault("generated_at", datetime.now(timezone.utc).isoformat())
    manifest.setdefault("evidence_fingerprint", _fingerprint(evidence))
    decision = final_state.get("final_trade_decision")
    manifest.setdefault("decision_fingerprint", _fingerprint(_decision_outcome(decision)))
    manifest.setdefault("decision_content_fingerprint", _fingerprint(decision))
    return manifest


def compare_run_manifests(current: dict, previous: dict | None) -> str:
    """Classify whether a changed decision came from input drift or model drift."""
    if previous is None:
        return "first_recorded_run"
    if current.get("evidence_fingerprint") != previous.get("evidence_fingerprint"):
        return "evidence_changed"
    if current.get("decision_fingerprint") != previous.get("decision_fingerprint"):
        return "decision_changed_same_evidence"
    return "reproduced"


def _generated_at_from_manifest(manifest: dict | None) -> datetime | None:
    if not manifest:
        return None
    raw = manifest.get("generated_at")
    if not isinstance(raw, str):
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _write_immutable_forecast_record(path: Path, record) -> None:
    payload = record.model_dump(mode="json")
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise FileExistsError(
                f"refusing to overwrite invalid immutable forecast record: {path}"
            ) from exc
        if existing == payload:
            return
        raise FileExistsError(
            f"refusing to overwrite immutable forecast record with different content: {path}"
        )
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_report_tree(
    final_state: dict,
    ticker: str,
    save_path,
    *,
    run_metadata: dict | None = None,
) -> Path:
    """Save a completed run's reports to ``save_path``; return the complete-report path."""
    save_path = Path(save_path)
    save_path.mkdir(parents=True, exist_ok=True)
    sections = []
    manifest = (
        build_run_manifest(final_state, ticker, run_metadata)
        if run_metadata is not None
        else None
    )
    forecast_record = forecast_record_from_state(
        final_state,
        ticker,
        manifest or {},
        generated_at=_generated_at_from_manifest(manifest),
    )
    _write_immutable_forecast_record(
        save_path / "forecast_record.json",
        forecast_record,
    )
    if run_metadata is not None:
        (save_path / "run_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    # 1. Analysts
    analysts_dir = save_path / "1_analysts"
    analyst_parts = []
    if final_state.get("market_report"):
        analysts_dir.mkdir(exist_ok=True)
        (analysts_dir / "market.md").write_text(final_state["market_report"], encoding="utf-8")
        analyst_parts.append(("Market Analyst", final_state["market_report"]))
    if final_state.get("sentiment_report"):
        analysts_dir.mkdir(exist_ok=True)
        (analysts_dir / "sentiment.md").write_text(final_state["sentiment_report"], encoding="utf-8")
        analyst_parts.append(("Sentiment Analyst", final_state["sentiment_report"]))
    if final_state.get("news_report"):
        analysts_dir.mkdir(exist_ok=True)
        (analysts_dir / "news.md").write_text(final_state["news_report"], encoding="utf-8")
        analyst_parts.append(("News Analyst", final_state["news_report"]))
    if final_state.get("fundamentals_report"):
        analysts_dir.mkdir(exist_ok=True)
        (analysts_dir / "fundamentals.md").write_text(final_state["fundamentals_report"], encoding="utf-8")
        analyst_parts.append(("Fundamentals Analyst", final_state["fundamentals_report"]))
    if analyst_parts:
        content = "\n\n".join(f"### {name}\n{text}" for name, text in analyst_parts)
        sections.append(f"## I. Analyst Team Reports\n\n{content}")

    # 2. Research
    if final_state.get("investment_debate_state"):
        research_dir = save_path / "2_research"
        debate = final_state["investment_debate_state"]
        research_parts = []
        if debate.get("bull_history"):
            research_dir.mkdir(exist_ok=True)
            (research_dir / "bull.md").write_text(debate["bull_history"], encoding="utf-8")
            research_parts.append(("Bull Researcher", debate["bull_history"]))
        if debate.get("bear_history"):
            research_dir.mkdir(exist_ok=True)
            (research_dir / "bear.md").write_text(debate["bear_history"], encoding="utf-8")
            research_parts.append(("Bear Researcher", debate["bear_history"]))
        if debate.get("judge_decision"):
            research_dir.mkdir(exist_ok=True)
            (research_dir / "manager.md").write_text(debate["judge_decision"], encoding="utf-8")
            research_parts.append(("Research Manager", debate["judge_decision"]))
        if research_parts:
            content = "\n\n".join(f"### {name}\n{text}" for name, text in research_parts)
            sections.append(f"## II. Research Team Decision\n\n{content}")

    # 3. Trading
    if final_state.get("trader_investment_plan"):
        trading_dir = save_path / "3_trading"
        trading_dir.mkdir(exist_ok=True)
        (trading_dir / "trader.md").write_text(final_state["trader_investment_plan"], encoding="utf-8")
        sections.append(f"## III. Trading Team Plan\n\n### Trader\n{final_state['trader_investment_plan']}")

    # 4. Risk Management
    if final_state.get("risk_debate_state"):
        risk_dir = save_path / "4_risk"
        risk = final_state["risk_debate_state"]
        risk_parts = []
        if risk.get("aggressive_history"):
            risk_dir.mkdir(exist_ok=True)
            (risk_dir / "aggressive.md").write_text(risk["aggressive_history"], encoding="utf-8")
            risk_parts.append(("Aggressive Analyst", risk["aggressive_history"]))
        if risk.get("conservative_history"):
            risk_dir.mkdir(exist_ok=True)
            (risk_dir / "conservative.md").write_text(risk["conservative_history"], encoding="utf-8")
            risk_parts.append(("Conservative Analyst", risk["conservative_history"]))
        if risk.get("neutral_history"):
            risk_dir.mkdir(exist_ok=True)
            (risk_dir / "neutral.md").write_text(risk["neutral_history"], encoding="utf-8")
            risk_parts.append(("Neutral Analyst", risk["neutral_history"]))
        if risk_parts:
            content = "\n\n".join(f"### {name}\n{text}" for name, text in risk_parts)
            sections.append(f"## IV. Risk Management Team Decision\n\n{content}")

        # 5. Portfolio Manager
        if risk.get("judge_decision"):
            portfolio_dir = save_path / "5_portfolio"
            portfolio_dir.mkdir(exist_ok=True)
            (portfolio_dir / "decision.md").write_text(risk["judge_decision"], encoding="utf-8")
            sections.append(f"## V. Portfolio Manager Decision\n\n### Portfolio Manager\n{risk['judge_decision']}")

    # Write consolidated report
    header = f"# Trading Analysis Report: {ticker}\n\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    (save_path / "complete_report.md").write_text(header + "\n\n".join(sections), encoding="utf-8")
    return save_path / "complete_report.md"
