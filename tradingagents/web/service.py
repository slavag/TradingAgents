from __future__ import annotations

import datetime as dt
import importlib.util
import json
import logging
import os
import threading
import traceback
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from cli.main import (
    ANALYST_AGENT_NAMES,
    ANALYST_ORDER,
    ANALYST_REPORT_MAP,
    build_consolidated_report,
    build_consolidated_report_html,
    classify_message_type,
    compact_report_text,
    estimate_target_profile,
    extract_content_string,
    format_price_target,
    save_consolidated_report,
    save_report_to_disk,
)
from cli.stats_handler import StatsCallbackHandler
from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.web.speaking_sources import (
    SOURCE_LABELS,
)

logger = logging.getLogger("tradingagents.web.service")

RESULTS_ROOT = Path(DEFAULT_CONFIG["results_dir"]).resolve()
MYAGENT_SCRIPT = Path(
    "/Users/slava/Documents/Development/private/investment/MyAgent/social_topn_aw_stwt_only_with_ta_cli.py"
)
PROJECT_ROOT = Path(__file__).resolve().parents[2]

_JOB_LOCK = threading.Lock()
_JOBS: dict[str, dict[str, Any]] = {}
_SPEAKING_CACHE: dict[str, Any] = {
    "expires_at": None,
    "key": None,
    "data": None,
}
_MARKET_TICKER_CACHE: dict[str, Any] = {
    "expires_at": None,
    "key": None,
    "data": None,
}
_MYAGENT_MODULE = None
_TICKER_DETAIL_CACHE: dict[str, dict[str, Any]] = {}

MARKET_INDEXES = [
    {"symbol": "^GSPC", "label": "S&P 500"},
    {"symbol": "^DJI", "label": "Dow"},
    {"symbol": "^IXIC", "label": "Nasdaq"},
    {"symbol": "^STOXX50E", "label": "Euro Stoxx 50"},
    {"symbol": "^GDAXI", "label": "DAX"},
    {"symbol": "^FTSE", "label": "FTSE 100"},
    {"symbol": "^RUT", "label": "Russell 2000"},
    {"symbol": "^N225", "label": "Japan"},
    {"symbol": "^HSI", "label": "Hong Kong"},
    {"symbol": "^KS11", "label": "Seoul"},
    {"symbol": "000001.SS", "label": "Shanghai"},
    {"symbol": "EURUSD=X", "label": "EUR / USD"},
    {"symbol": "USDILS=X", "label": "USD / NIS"},
    {"symbol": "^VIX", "label": "VIX"},
]

TEAM_ORDER = [
    ("Analyst Team", ["Market Analyst", "Social Analyst", "News Analyst", "Fundamentals Analyst"]),
    ("Research Team", ["Bull Researcher", "Bear Researcher", "Research Manager"]),
    ("Trading Team", ["Trader"]),
    ("Risk Management", ["Aggressive Analyst", "Neutral Analyst", "Conservative Analyst"]),
    ("Portfolio Management", ["Portfolio Manager"]),
]

REPORT_TITLES = {
    "market_report": "Market Analysis",
    "sentiment_report": "Social Sentiment",
    "news_report": "News Analysis",
    "fundamentals_report": "Fundamentals Analysis",
    "investment_plan": "Research Team Decision",
    "trader_investment_plan": "Trading Team Plan",
    "final_trade_decision": "Portfolio Management Decision",
}


def classify_runtime_error(exc: Exception) -> dict[str, Any]:
    """Classify provider/runtime exceptions into user-facing error types."""
    raw_message = str(exc).strip()
    lowered = raw_message.lower()

    if (
        "insufficient_quota" in lowered
        or "exceeded your current quota" in lowered
        or "check your plan and billing details" in lowered
    ):
        return {
            "kind": "quota_exceeded",
            "fatal_for_batch": True,
            "user_message": (
                "Provider quota exceeded. The API account has no remaining quota or credits. "
                "Check billing, top up credits, or switch provider/model before retrying."
            ),
            "raw_message": raw_message,
        }

    if "error code: 429" in lowered or "rate limit" in lowered:
        return {
            "kind": "rate_limited",
            "fatal_for_batch": True,
            "user_message": (
                "Provider rate limit reached. Wait and retry, reduce concurrency, or use another provider."
            ),
            "raw_message": raw_message,
        }

    return {
        "kind": "runtime_error",
        "fatal_for_batch": False,
        "user_message": raw_message,
        "raw_message": raw_message,
    }


def _now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def _job_snapshot(job_id: str) -> dict[str, Any]:
    with _JOB_LOCK:
        return json.loads(json.dumps(_JOBS[job_id]))


def _update_job(job_id: str, **updates):
    with _JOB_LOCK:
        job = _JOBS[job_id]
        job.update(updates)
        job["updated_at"] = _now_iso()


class WebProgressTracker:
    def __init__(self, selected_analysts: list[str]):
        self.selected_analysts = [item.lower() for item in selected_analysts]
        self.agent_status: dict[str, str] = {}
        self.report_sections: dict[str, str | None] = {}
        self.current_report: str | None = None
        self.recent_events: list[dict[str, str]] = []
        self._last_message_id = None
        self._init_state()

    def _init_state(self):
        for team_name, agents in TEAM_ORDER:
            for agent in agents:
                if team_name == "Analyst Team":
                    analyst_key = next(
                        (
                            key
                            for key, label in ANALYST_AGENT_NAMES.items()
                            if label == agent
                        ),
                        None,
                    )
                    if analyst_key not in self.selected_analysts:
                        continue
                self.agent_status[agent] = "pending"

        for section, report_key in ANALYST_REPORT_MAP.items():
            if section in self.selected_analysts:
                self.report_sections[report_key] = None
        self.report_sections.update(
            {
                "investment_plan": None,
                "trader_investment_plan": None,
                "final_trade_decision": None,
            }
        )

        if self.selected_analysts:
            first_agent = ANALYST_AGENT_NAMES[self.selected_analysts[0]]
            self.agent_status[first_agent] = "in_progress"

    def add_event(self, event_type: str, content: str):
        if not content:
            return
        self.recent_events.insert(
            0,
            {
                "time": dt.datetime.now().strftime("%H:%M:%S"),
                "type": event_type,
                "content": content.strip(),
            },
        )
        self.recent_events = self.recent_events[:12]

    def set_status(self, agent: str, status: str):
        if agent in self.agent_status:
            self.agent_status[agent] = status

    def set_team_status(self, team_name: str, status: str):
        team_agents = dict(TEAM_ORDER).get(team_name, [])
        for agent in team_agents:
            if agent in self.agent_status:
                self.agent_status[agent] = status

    def update_report(self, section_name: str, content: Any):
        if section_name not in self.report_sections:
            return
        normalized = extract_content_string(content)
        self.report_sections[section_name] = normalized
        if normalized:
            title = REPORT_TITLES.get(section_name, section_name)
            self.current_report = f"{title}\n\n{normalized}"

    def process_message(self, message: Any):
        message_id = getattr(message, "id", None)
        if message_id and message_id == self._last_message_id:
            return
        if message_id:
            self._last_message_id = message_id

        message_type, content = classify_message_type(message)
        if content and content.strip():
            self.add_event(message_type, compact_report_text(content, max_chars=260))

        if hasattr(message, "tool_calls") and message.tool_calls:
            for tool_call in message.tool_calls:
                if isinstance(tool_call, dict):
                    tool_name = tool_call.get("name", "tool")
                    args = tool_call.get("args", {})
                else:
                    tool_name = getattr(tool_call, "name", "tool")
                    args = getattr(tool_call, "args", {})
                self.add_event("Tool", f"{tool_name}: {compact_report_text(str(args), max_chars=180)}")

    def update_analysts(self, chunk: dict[str, Any]):
        found_active = False
        for analyst_key in ANALYST_ORDER:
            if analyst_key not in self.selected_analysts:
                continue
            agent_name = ANALYST_AGENT_NAMES[analyst_key]
            report_key = ANALYST_REPORT_MAP[analyst_key]
            report_content = extract_content_string(chunk.get(report_key))
            has_report = bool(report_content)
            if has_report:
                self.set_status(agent_name, "completed")
                self.update_report(report_key, report_content)
            elif not found_active:
                self.set_status(agent_name, "in_progress")
                found_active = True
            else:
                self.set_status(agent_name, "pending")

        if not found_active and self.selected_analysts:
            if self.agent_status.get("Bull Researcher") == "pending":
                self.set_status("Bull Researcher", "in_progress")

    def update_research(self, chunk: dict[str, Any]):
        debate_state = chunk.get("investment_debate_state")
        if not debate_state:
            return

        bull_hist = extract_content_string(debate_state.get("bull_history")) or ""
        bear_hist = extract_content_string(debate_state.get("bear_history")) or ""
        judge = extract_content_string(debate_state.get("judge_decision")) or ""

        if bull_hist:
            self.set_status("Bull Researcher", "completed")
            self.update_report("investment_plan", f"### Bull Researcher Analysis\n{bull_hist}")
        elif self.agent_status.get("Bull Researcher") == "in_progress":
            self.set_status("Bull Researcher", "in_progress")

        if bear_hist:
            self.set_status("Bear Researcher", "completed")
            self.update_report("investment_plan", f"### Bear Researcher Analysis\n{bear_hist}")
            if self.agent_status.get("Research Manager") == "pending":
                self.set_status("Research Manager", "in_progress")
        elif bull_hist and self.agent_status.get("Bear Researcher") == "pending":
            self.set_status("Bear Researcher", "in_progress")

        if judge:
            self.set_status("Research Manager", "completed")
            self.update_report("investment_plan", f"### Research Manager Decision\n{judge}")
            self.set_status("Trader", "in_progress")

    def update_trader(self, chunk: dict[str, Any]):
        if chunk.get("trader_investment_plan"):
            self.update_report("trader_investment_plan", chunk["trader_investment_plan"])
            self.set_status("Trader", "completed")
            if self.agent_status.get("Aggressive Analyst") == "pending":
                self.set_status("Aggressive Analyst", "in_progress")

    def update_risk(self, chunk: dict[str, Any]):
        risk_state = chunk.get("risk_debate_state")
        if not risk_state:
            return

        agg_hist = extract_content_string(risk_state.get("aggressive_history")) or ""
        con_hist = extract_content_string(risk_state.get("conservative_history")) or ""
        neu_hist = extract_content_string(risk_state.get("neutral_history")) or ""
        judge = extract_content_string(risk_state.get("judge_decision")) or ""

        if agg_hist:
            self.set_status("Aggressive Analyst", "completed")
            self.update_report("final_trade_decision", f"### Aggressive Analyst Analysis\n{agg_hist}")
            if self.agent_status.get("Neutral Analyst") == "pending":
                self.set_status("Neutral Analyst", "in_progress")

        if con_hist:
            self.set_status("Conservative Analyst", "completed")
            self.update_report("final_trade_decision", f"### Conservative Analyst Analysis\n{con_hist}")
            if self.agent_status.get("Neutral Analyst") == "pending":
                self.set_status("Neutral Analyst", "in_progress")

        if neu_hist:
            self.set_status("Neutral Analyst", "completed")
            self.update_report("final_trade_decision", f"### Neutral Analyst Analysis\n{neu_hist}")
            if self.agent_status.get("Portfolio Manager") == "pending":
                self.set_status("Portfolio Manager", "in_progress")

        if judge:
            self.set_status("Portfolio Manager", "completed")
            self.update_report("final_trade_decision", f"### Portfolio Manager Decision\n{judge}")

    def finalize(self, final_state: dict[str, Any]):
        for agent in list(self.agent_status):
            self.agent_status[agent] = "completed"
        for section in self.report_sections:
            if section in final_state:
                self.update_report(section, final_state[section])

    def snapshot_rows(self) -> list[dict[str, str]]:
        rows = []
        for team_name, agents in TEAM_ORDER:
            for agent in agents:
                if agent not in self.agent_status:
                    continue
                rows.append(
                    {
                        "team": team_name,
                        "agent": agent,
                        "status": self.agent_status.get(agent, "pending"),
                    }
                )
        return rows

    def snapshot(self) -> dict[str, Any]:
        return {
            "progress_rows": self.snapshot_rows(),
            "recent_events": self.recent_events,
            "current_report": self.current_report,
        }


def normalize_tickers(raw_tickers: Any) -> list[str]:
    if isinstance(raw_tickers, str):
        parts = raw_tickers.replace(",", " ").split()
    elif isinstance(raw_tickers, list):
        parts = [str(item) for item in raw_tickers]
    else:
        parts = []

    seen = set()
    normalized = []
    for part in parts:
        ticker = part.strip().upper()
        if ticker and ticker not in seen:
            seen.add(ticker)
            normalized.append(ticker)
    return normalized


def build_graph_config(payload: dict[str, Any]) -> dict[str, Any]:
    config = DEFAULT_CONFIG.copy()
    config["max_debate_rounds"] = payload.get("research_depth", 3)
    config["max_risk_discuss_rounds"] = payload.get("research_depth", 3)
    config["quick_think_llm"] = payload.get("quick_thinker", "gpt-5.4")
    config["deep_think_llm"] = payload.get("deep_thinker", "gpt-5.4")
    config["final_report_llm"] = payload.get("final_report_model", config["quick_think_llm"])
    config["llm_provider"] = payload.get("llm_provider", "openai").lower()
    config["backend_url"] = payload.get("backend_url")
    config["quick_think_provider"] = (
        payload.get("quick_provider") or config["llm_provider"]
    ).lower()
    config["deep_think_provider"] = (
        payload.get("deep_provider") or config["llm_provider"]
    ).lower()
    config["final_report_provider"] = (
        payload.get("final_report_provider") or config["llm_provider"]
    ).lower()
    config["quick_backend_url"] = payload.get("quick_backend_url")
    config["deep_backend_url"] = payload.get("deep_backend_url")
    config["final_report_backend_url"] = payload.get("final_report_backend_url")
    config["google_thinking_level"] = payload.get("google_thinking_level")
    config["openai_reasoning_effort"] = payload.get("openai_reasoning_effort")
    return config


def serialize_result(result: dict[str, Any]) -> dict[str, Any]:
    if result.get("error"):
        return {
            "ticker": result["ticker"],
            "analysis_date": result["analysis_date"],
            "status": "failed",
            "decision": None,
            "price_target": None,
            "price_target_label": "-",
            "confidence_score": None,
            "target_horizon": None,
            "target_summary": None,
            "results_dir": result["results_dir"],
            "report_path": None,
            "custom_report_path": result.get("custom_report_path"),
            "error_kind": result.get("error_kind"),
            "error": result["error"],
        }

    final_state = result["final_state"]
    confidence = result.get("confidence_score")
    return {
        "ticker": result["ticker"],
        "analysis_date": result["analysis_date"],
        "status": "completed",
        "decision": result.get("decision"),
        "price_target": result.get("price_target"),
        "price_target_label": format_price_target(result.get("price_target")),
        "confidence_score": confidence,
        "confidence_label": f"{confidence}/100" if confidence is not None else "-",
        "target_horizon": result.get("target_horizon"),
        "target_summary": result.get("target_summary"),
        "results_dir": result["results_dir"],
        "report_path": result.get("report_path"),
        "custom_report_path": result.get("custom_report_path"),
        "executive_summary": compact_report_text(
            final_state.get("final_trade_decision"), max_chars=900
        ),
        "trader_plan": compact_report_text(
            final_state.get("trader_investment_plan"), max_chars=500
        ),
        "highlights": {
            "market": compact_report_text(final_state.get("market_report"), max_chars=220),
            "social": compact_report_text(final_state.get("sentiment_report"), max_chars=220),
            "news": compact_report_text(final_state.get("news_report"), max_chars=220),
            "fundamentals": compact_report_text(
                final_state.get("fundamentals_report"), max_chars=220
            ),
        },
    }


def _load_myagent_module():
    global _MYAGENT_MODULE
    if _MYAGENT_MODULE is not None:
        logger.warning("Speaking stocks: reusing cached MyAgent module")
        return _MYAGENT_MODULE

    if not MYAGENT_SCRIPT.exists():
        raise FileNotFoundError(f"MyAgent script not found: {MYAGENT_SCRIPT}")

    load_dotenv(PROJECT_ROOT / ".env", override=False)
    if not os.getenv("ALPHAVANTAGE_API_KEY") and os.getenv("ALPHA_VANTAGE_API_KEY"):
        os.environ["ALPHAVANTAGE_API_KEY"] = os.environ["ALPHA_VANTAGE_API_KEY"]
        logger.warning(
            "Speaking stocks: mapped ALPHA_VANTAGE_API_KEY to ALPHAVANTAGE_API_KEY for MyAgent compatibility"
        )

    started_at = dt.datetime.now()
    logger.warning("Speaking stocks: loading MyAgent module from %s", MYAGENT_SCRIPT)
    spec = importlib.util.spec_from_file_location("myagent_social", MYAGENT_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load MyAgent speaking stocks module.")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    elapsed = (dt.datetime.now() - started_at).total_seconds()
    logger.warning("Speaking stocks: MyAgent module loaded in %.2fs", elapsed)
    _MYAGENT_MODULE = module
    return module


def _fetch_market_index_snapshots(symbols: list[str]) -> pd.DataFrame:
    import yfinance as yf

    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        try:
            history = yf.Ticker(symbol).history(period="7d", interval="1d", auto_adjust=True)
        except Exception as exc:
            logger.warning("Market tickers: failed to fetch %s: %s", symbol, exc)
            continue

        if history.empty or "Close" not in history:
            logger.warning("Market tickers: empty history for %s", symbol)
            continue

        closes = pd.to_numeric(history["Close"], errors="coerce").dropna()
        if closes.empty:
            continue

        last_close = float(closes.iloc[-1])
        prev_close = float(closes.iloc[-2]) if len(closes) > 1 else None
        ret_1d = ((last_close / prev_close) - 1.0) if prev_close not in (None, 0) else None
        rows.append(
            {
                "symbol": symbol,
                "price": last_close,
                "ret_1d": ret_1d,
            }
        )

    return pd.DataFrame(rows)


def fetch_speaking_stocks(
    top_n: int = 10,
    lookback_days: int = 30,
    ape_pages: int = 2,
    w_ret5: float = 0.7,
    w_trend: float = 0.3,
) -> list[dict[str, Any]]:
    cache_key = (top_n, lookback_days, ape_pages, w_ret5, w_trend)
    now = dt.datetime.now()
    if (
        _SPEAKING_CACHE["data"] is not None
        and _SPEAKING_CACHE["key"] == cache_key
        and _SPEAKING_CACHE["expires_at"] is not None
        and now < _SPEAKING_CACHE["expires_at"]
    ):
        logger.warning("Speaking stocks: cache hit for key=%s", cache_key)
        return _SPEAKING_CACHE["data"]

    logger.warning(
        "Speaking stocks: refresh started top_n=%s lookback_days=%s ape_pages=%s",
        top_n,
        lookback_days,
        ape_pages,
    )
    started_at = dt.datetime.now()
    module = _load_myagent_module()

    step_started = dt.datetime.now()
    aw_set = module.aw_symbol_set(pages=ape_pages)
    logger.warning(
        "Speaking stocks: ApeWisdom returned %s symbols in %.2fs",
        len(aw_set),
        (dt.datetime.now() - step_started).total_seconds(),
    )

    step_started = dt.datetime.now()
    stwt_set = module.stwt_symbol_set()
    logger.warning(
        "Speaking stocks: StockTwits returned %s symbols in %.2fs",
        len(stwt_set),
        (dt.datetime.now() - step_started).total_seconds(),
    )

    source_sets: dict[str, set[str]] = {
        "apewisdom": set(aw_set),
        "stocktwits": set(stwt_set),
    }
    intersection = sorted(list(source_sets["apewisdom"] & source_sets["stocktwits"]))
    logger.warning(
        "Speaking stocks: intersection size=%s",
        len(intersection),
    )

    if not intersection:
        logger.warning("Speaking stocks: no symbols available from ApeWisdom ∩ StockTwits")
        return []

    source_membership = {
        symbol: sorted(
            SOURCE_LABELS.get(source_name, source_name)
            for source_name, symbols in source_sets.items()
            if symbol in symbols
        )
        for symbol in intersection
    }
    candidate_symbols = intersection

    step_started = dt.datetime.now()
    prices = module.fetch_prices(candidate_symbols, lookback=lookback_days)
    logger.warning(
        "Speaking stocks: yfinance fetched prices for %s tickers in %.2fs",
        len(candidate_symbols),
        (dt.datetime.now() - step_started).total_seconds(),
    )
    if prices.empty:
        logger.warning("Speaking stocks: price frame empty, refresh finished early")
        return []

    momentum_5d = prices.pct_change(5, fill_method=None).tail(1).T
    momentum_5d.columns = ["ret_5d"]
    momentum_1d = prices.pct_change(1, fill_method=None).tail(1).T
    momentum_1d.columns = ["ret_1d"]
    sma50 = prices.rolling(50, min_periods=10).mean().tail(1).T
    sma50.columns = ["sma50"]
    sma200 = prices.rolling(200, min_periods=30).mean().tail(1).T
    sma200.columns = ["sma200"]
    latest = prices.tail(1).T
    latest.columns = ["price"]

    tech = latest.join([momentum_1d, momentum_5d, sma50, sma200], how="outer")
    tech["source_priority"] = [1 if symbol in intersection else 0 for symbol in tech.index]
    tech["source_hits"] = [len(source_membership.get(symbol, [])) for symbol in tech.index]
    tech["sma_trend"] = (
        (tech["price"] > tech["sma50"]).astype("Int64").fillna(0).astype(int)
        + (tech["sma50"] > tech["sma200"]).astype("Int64").fillna(0).astype(int)
    )
    tech.index.name = "symbol"

    frame = tech.reset_index()
    frame["ret_1d"] = pd.to_numeric(frame["ret_1d"], errors="coerce").fillna(0.0)
    frame["ret_5d"] = pd.to_numeric(frame["ret_5d"], errors="coerce").fillna(0.0)
    frame["sma_trend"] = pd.to_numeric(frame["sma_trend"], errors="coerce").fillna(0).astype(int)
    frame["source_priority"] = pd.to_numeric(frame["source_priority"], errors="coerce").fillna(0).astype(int)
    frame["source_hits"] = pd.to_numeric(frame["source_hits"], errors="coerce").fillna(0).astype(int)
    frame["z_ret5"] = module._zscore(frame["ret_5d"])
    frame["score"] = (
        w_ret5 * frame["z_ret5"]
        + w_trend * frame["sma_trend"]
    )

    top = frame.sort_values("score", ascending=False).head(top_n).copy()
    logger.warning(
        "Speaking stocks: ranked top %s tickers=%s",
        len(top),
        top["symbol"].tolist(),
    )

    logger.warning(
        "Speaking stocks: skipping Alpha Vantage fundamentals for fast ticker-tape refresh"
    )

    records = []
    for row in top.itertuples():
        price = float(row.price) if pd.notna(row.price) else None
        ret_5d = float(row.ret_5d) * 100.0 if pd.notna(row.ret_5d) else None
        ret_1d = float(row.ret_1d) * 100.0 if pd.notna(row.ret_1d) else None
        records.append(
            {
                "ticker": row.symbol,
                "score": round(float(row.score), 2),
                "price": round(price, 2) if price is not None else None,
                "ret_1d_pct": round(ret_1d, 1) if ret_1d is not None else None,
                "ret_5d_pct": round(ret_5d, 1) if ret_5d is not None else None,
                "trend_score": int(row.sma_trend) if pd.notna(row.sma_trend) else 0,
                "z_ret5": round(float(row.z_ret5), 2) if pd.notna(row.z_ret5) else None,
                "lookback_days": lookback_days,
                "sources": source_membership.get(row.symbol, []),
                "source_count": len(source_membership.get(row.symbol, [])),
                "pe_ratio": None,
                "market_cap": None,
                "sector": None,
            }
        )

    _SPEAKING_CACHE["key"] = cache_key
    _SPEAKING_CACHE["data"] = records
    _SPEAKING_CACHE["expires_at"] = now + dt.timedelta(minutes=30)
    logger.warning(
        "Speaking stocks: refresh completed in %.2fs with %s records",
        (dt.datetime.now() - started_at).total_seconds(),
        len(records),
    )
    return records


def fetch_market_tickers(limit: int = 12) -> list[dict[str, Any]]:
    cache_key = (limit,)
    now = dt.datetime.now()
    if (
        _MARKET_TICKER_CACHE["data"] is not None
        and _MARKET_TICKER_CACHE["key"] == cache_key
        and _MARKET_TICKER_CACHE["expires_at"] is not None
        and now < _MARKET_TICKER_CACHE["expires_at"]
    ):
        logger.warning("Market tickers: cache hit for key=%s", cache_key)
        return _MARKET_TICKER_CACHE["data"]

    logger.warning("Market tickers: refresh started limit=%s", limit)
    index_symbols = [item["symbol"] for item in MARKET_INDEXES[:limit]]
    frame = _fetch_market_index_snapshots(index_symbols)
    if frame.empty:
        logger.warning("Market tickers: snapshot frame empty")
        return []
    frame = frame.set_index("symbol").reindex(index_symbols).dropna(how="all")

    records = []
    label_by_symbol = {item["symbol"]: item["label"] for item in MARKET_INDEXES}
    for symbol, row in frame.iterrows():
        price = float(row["price"]) if pd.notna(row["price"]) else None
        ret_1d = float(row["ret_1d"]) * 100.0 if pd.notna(row["ret_1d"]) else None
        records.append(
            {
                "ticker": label_by_symbol.get(symbol, symbol),
                "symbol": symbol,
                "price": round(price, 2) if price is not None else None,
                "ret_1d_pct": round(ret_1d, 1) if ret_1d is not None else None,
                "source": "Market Index",
            }
        )

    _MARKET_TICKER_CACHE["key"] = cache_key
    _MARKET_TICKER_CACHE["data"] = records[:limit]
    _MARKET_TICKER_CACHE["expires_at"] = now + dt.timedelta(minutes=10)
    logger.warning("Market tickers: refresh completed with %s records", len(records[:limit]))
    return records[:limit]


def fetch_ticker_detail(ticker: str) -> dict[str, Any]:
    symbol = ticker.strip().upper()
    now = dt.datetime.now()
    cached = _TICKER_DETAIL_CACHE.get(symbol)
    if cached and cached.get("expires_at") and now < cached["expires_at"]:
        logger.warning("Ticker detail: cache hit for %s", symbol)
        return cached["data"]

    logger.warning("Ticker detail: fetching company snapshot for %s", symbol)
    import yfinance as yf

    yf_ticker = yf.Ticker(symbol)
    fast_info = {}
    info = {}
    try:
        fast_info = dict(getattr(yf_ticker, "fast_info", {}) or {})
    except Exception:
        fast_info = {}
    try:
        info = yf_ticker.info or {}
    except Exception:
        info = {}

    long_name = (
        info.get("longName")
        or info.get("shortName")
        or info.get("displayName")
        or symbol
    )
    sector = info.get("sectorDisp") or info.get("sector") or "—"
    industry = info.get("industryDisp") or info.get("industry") or "—"
    market_cap = (
        info.get("marketCap")
        or fast_info.get("market_cap")
        or fast_info.get("marketCap")
    )
    current_price = (
        fast_info.get("lastPrice")
        or fast_info.get("last_price")
        or info.get("currentPrice")
        or info.get("regularMarketPrice")
    )
    pe_ratio = (
        info.get("trailingPE")
        or info.get("forwardPE")
        or "—"
    )
    fifty_two_week_high = (
        fast_info.get("yearHigh")
        or fast_info.get("year_high")
        or info.get("fiftyTwoWeekHigh")
    )
    fifty_two_week_low = (
        fast_info.get("yearLow")
        or fast_info.get("year_low")
        or info.get("fiftyTwoWeekLow")
    )
    average_volume = (
        info.get("averageVolume")
        or info.get("averageDailyVolume10Day")
        or fast_info.get("tenDayAverageVolume")
    )
    employees = info.get("fullTimeEmployees")
    website = info.get("website")
    summary = info.get("longBusinessSummary") or ""

    data = {
        "ticker": symbol,
        "company_name": long_name,
        "sector": sector,
        "industry": industry,
        "market_cap": market_cap,
        "current_price": current_price,
        "pe_ratio": pe_ratio,
        "fifty_two_week_high": fifty_two_week_high,
        "fifty_two_week_low": fifty_two_week_low,
        "average_volume": average_volume,
        "employees": employees,
        "website": website,
        "summary": summary[:900].strip() if summary else "Company profile summary unavailable.",
    }

    _TICKER_DETAIL_CACHE[symbol] = {
        "data": data,
        "expires_at": now + dt.timedelta(minutes=30),
    }
    logger.warning("Ticker detail: snapshot ready for %s", symbol)
    return data


def _run_job(job_id: str, payload: dict[str, Any]):
    tickers = normalize_tickers(payload.get("tickers"))
    analysis_date = payload["analysis_date"]
    selected_analysts = payload.get("analysts") or [
        "market",
        "social",
        "news",
        "fundamentals",
    ]
    config = build_graph_config(payload)
    export_root = Path(payload["export_path"]).expanduser() if payload.get("export_path") else None
    custom_save_enabled = bool(payload.get("save_reports")) and export_root is not None

    raw_results: list[dict[str, Any]] = []
    serialized_results: list[dict[str, Any]] = []
    _update_job(job_id, status="running", total=len(tickers), completed=0)
    logger.info(
        "Job %s started (provider=%s, tickers=%s, analysis_date=%s)",
        job_id,
        payload.get("llm_provider"),
        tickers,
        analysis_date,
    )

    try:
        for index, ticker in enumerate(tickers, start=1):
            tracker = WebProgressTracker(selected_analysts)
            stats_handler = StatsCallbackHandler()
            _update_job(
                job_id,
                current_ticker=ticker,
                progress_message=f"Analyzing {ticker} ({index}/{len(tickers)})",
                **tracker.snapshot(),
            )
            try:
                graph = TradingAgentsGraph(
                    selected_analysts=selected_analysts,
                    debug=False,
                    config=config,
                    callbacks=[stats_handler],
                )
                init_state = graph.propagator.create_initial_state(ticker, analysis_date)
                args = graph.propagator.get_graph_args(callbacks=[stats_handler])
                trace = []
                for chunk in graph.graph.stream(init_state, **args):
                    messages = chunk.get("messages") or []
                    if messages:
                        tracker.process_message(messages[-1])

                    tracker.update_analysts(chunk)
                    tracker.update_research(chunk)
                    tracker.update_trader(chunk)
                    tracker.update_risk(chunk)
                    trace.append(chunk)

                    _update_job(
                        job_id,
                        current_ticker=ticker,
                        progress_message=f"Analyzing {ticker} ({index}/{len(tickers)})",
                        **tracker.snapshot(),
                    )

                final_state = trace[-1]
                tracker.finalize(final_state)
                decision = graph.process_signal(
                    extract_content_string(final_state["final_trade_decision"]) or ""
                )
                decision_text = decision if isinstance(decision, str) else json.dumps(decision)
                target_profile = estimate_target_profile(
                    graph.final_report_llm,
                    ticker,
                    analysis_date,
                    final_state,
                    decision_text,
                )

                default_report_dir = RESULTS_ROOT / ticker / analysis_date / "web_report"
                default_report_path = save_report_to_disk(final_state, ticker, default_report_dir)
                custom_report_path = None
                if custom_save_enabled:
                    custom_dir = export_root if len(tickers) == 1 else export_root / ticker
                    custom_report_path = save_report_to_disk(final_state, ticker, custom_dir)

                result = {
                    "ticker": ticker,
                    "analysis_date": analysis_date,
                    "decision": decision_text,
                    "final_state": final_state,
                    "results_dir": str(default_report_dir.resolve()),
                    "report_path": str(default_report_path.resolve()),
                    "custom_report_path": str(custom_report_path.resolve())
                    if custom_report_path
                    else None,
                    **target_profile,
                }
                logger.info(
                    "Job %s completed ticker %s (%s/%s) decision=%s",
                    job_id,
                    ticker,
                    index,
                    len(tickers),
                    decision_text,
                )
            except Exception as exc:
                error_info = classify_runtime_error(exc)
                tracker.add_event("System", error_info["user_message"])
                tracker.current_report = error_info["user_message"]
                logger.warning(
                    "Job %s failed ticker %s (%s/%s) kind=%s message=%s",
                    job_id,
                    ticker,
                    index,
                    len(tickers),
                    error_info["kind"],
                    error_info["user_message"],
                )
                result = {
                    "ticker": ticker,
                    "analysis_date": analysis_date,
                    "decision": None,
                    "final_state": None,
                    "results_dir": str((RESULTS_ROOT / ticker / analysis_date).resolve()),
                    "report_path": None,
                    "custom_report_path": None,
                    "price_target": None,
                    "confidence_score": None,
                    "target_horizon": None,
                    "target_summary": None,
                    "reference_price": None,
                    "error_kind": error_info["kind"],
                    "error": error_info["user_message"],
                    "raw_error": error_info["raw_message"],
                }

            raw_results.append(result)
            serialized_results.append(serialize_result(result))
            _update_job(
                job_id,
                completed=index,
                results=serialized_results,
                progress_message=(
                    f"Completed {ticker} ({index}/{len(tickers)})"
                    if not result.get("error")
                    else result["error"]
                ),
                **tracker.snapshot(),
            )

            if result.get("error_kind") in {"quota_exceeded", "rate_limited"}:
                for remaining_ticker in tickers[index:]:
                    skipped = {
                        "ticker": remaining_ticker,
                        "analysis_date": analysis_date,
                        "decision": None,
                        "final_state": None,
                        "results_dir": str((RESULTS_ROOT / remaining_ticker / analysis_date).resolve()),
                        "report_path": None,
                        "custom_report_path": None,
                        "price_target": None,
                        "confidence_score": None,
                        "target_horizon": None,
                        "target_summary": None,
                        "reference_price": None,
                        "error_kind": "skipped_after_provider_error",
                        "error": (
                            f"Skipped because the batch stopped after {ticker} hit "
                            f"{result['error_kind']}."
                        ),
                    }
                    raw_results.append(skipped)
                    serialized_results.append(serialize_result(skipped))
                logger.warning(
                    "Job %s stopped early due to %s; skipped remaining tickers=%s",
                    job_id,
                    result["error_kind"],
                    tickers[index:],
                )

                _update_job(
                    job_id,
                    completed=len(raw_results),
                    results=serialized_results,
                    progress_message=result["error"],
                    current_ticker=None,
                    **tracker.snapshot(),
                )
                break

        consolidated_markdown = None
        consolidated_html = None
        consolidated_paths = None
        custom_consolidated_paths = None
        if raw_results:
            consolidated_markdown = build_consolidated_report(
                raw_results,
                analysis_date,
                summary_llm=graph.final_report_llm,
            )
            consolidated_html = build_consolidated_report_html(
                raw_results,
                analysis_date,
                summary_llm=graph.final_report_llm,
            )

            default_batch_dir = RESULTS_ROOT / "batch_web" / analysis_date / job_id
            consolidated_paths = save_consolidated_report(
                raw_results,
                analysis_date,
                default_batch_dir,
                summary_llm=graph.final_report_llm,
            )

            if custom_save_enabled:
                custom_consolidated_paths = save_consolidated_report(
                    raw_results,
                    analysis_date,
                    export_root,
                    summary_llm=graph.final_report_llm,
                )

        fatal_provider_error = next(
            (
                item
                for item in raw_results
                if item.get("error_kind") in {"quota_exceeded", "rate_limited"}
            ),
            None,
        )

        _update_job(
            job_id,
            status="failed" if fatal_provider_error else "completed",
            completed=len(raw_results),
            current_ticker=None,
            progress_message=(
                fatal_provider_error["error"]
                if fatal_provider_error
                else "Analysis complete."
            ),
            results=serialized_results,
            consolidated_markdown=consolidated_markdown,
            consolidated_html=consolidated_html,
            consolidated_paths={
                "default": {k: str(v.resolve()) for k, v in consolidated_paths.items()}
                if consolidated_paths
                else None,
                "custom": {k: str(v.resolve()) for k, v in custom_consolidated_paths.items()}
                if custom_consolidated_paths
                else None,
            },
            error_kind=fatal_provider_error.get("error_kind") if fatal_provider_error else None,
            error=fatal_provider_error.get("error") if fatal_provider_error else None,
        )
        logger.info(
            "Job %s finished status=%s completed=%s/%s",
            job_id,
            "failed" if fatal_provider_error else "completed",
            len(raw_results),
            len(tickers),
        )
    except Exception as exc:
        _update_job(
            job_id,
            status="failed",
            error=str(exc),
            traceback=traceback.format_exc(),
            current_ticker=None,
        )
        logger.exception("Job %s crashed", job_id)


def create_job(payload: dict[str, Any]) -> dict[str, Any]:
    tickers = normalize_tickers(payload.get("tickers"))
    if not tickers:
        raise ValueError("At least one ticker is required.")

    job_id = uuid.uuid4().hex
    job = {
        "id": job_id,
        "status": "queued",
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "total": len(tickers),
        "completed": 0,
        "tickers": tickers,
        "analysis_date": payload["analysis_date"],
        "current_ticker": None,
        "progress_message": "Queued.",
        "results": [],
        "progress_rows": [],
        "recent_events": [],
        "current_report": None,
        "consolidated_markdown": None,
        "consolidated_html": None,
        "consolidated_paths": None,
        "error": None,
    }
    with _JOB_LOCK:
        _JOBS[job_id] = job

    thread = threading.Thread(target=_run_job, args=(job_id, payload), daemon=True)
    thread.start()
    logger.info("Job %s queued", job_id)
    return _job_snapshot(job_id)


def get_job(job_id: str) -> dict[str, Any] | None:
    with _JOB_LOCK:
        if job_id not in _JOBS:
            return None
    return _job_snapshot(job_id)
