# TradingAgents/graph/trading_graph.py

import json
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf
from langgraph.prebuilt import ToolNode

# Import the abstract tool methods from agent_utils
from tradingagents.agents.utils.agent_utils import (
    build_instrument_context,
    get_balance_sheet,
    get_cashflow,
    get_fundamentals,
    get_global_news,
    get_income_statement,
    get_indicators,
    get_insider_transactions,
    get_macro_indicators,
    get_news,
    get_prediction_markets,
    get_stock_data,
    get_verified_market_snapshot,
    resolve_instrument_identity,
)
from tradingagents.agents.utils.memory import TradingMemoryLog
from tradingagents.dataflows.config import set_config
from tradingagents.dataflows.temporal import use_analysis_context
from tradingagents.dataflows.utils import safe_ticker_component
from tradingagents.default_config import DEFAULT_CONFIG, validate_outcome_holding_days
from tradingagents.llm_clients import create_llm_client
from tradingagents.reporting import write_report_tree

from .checkpointer import checkpoint_step, clear_checkpoint, get_checkpointer, thread_id
from .conditional_logic import ConditionalLogic
from .propagation import Propagator
from .reflection import Reflector
from .setup import GraphSetup
from .signal_processing import SignalProcessor

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RealizedOutcome:
    """Executable gross return over a common stock/benchmark session window."""

    raw_return: float
    benchmark_return: float
    excess_return: float
    holding_days: int
    entry_date: str
    exit_date: str
    return_basis: str = "gross"


def _coerce_max_retries(value):
    """Validate an ``llm_max_retries`` value to a non-negative int.

    Accepts an int or a numeric string (env vars arrive as strings). Rejects
    booleans and negatives loudly so a misconfiguration fails at startup rather
    than silently disabling retries.
    """
    if isinstance(value, bool):
        raise ValueError(f"llm_max_retries must be an integer, not a boolean: {value!r}")
    try:
        n = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"llm_max_retries must be an integer, got {value!r}") from exc
    if n < 0:
        raise ValueError(f"llm_max_retries must be >= 0, got {n}")
    return n


class TradingAgentsGraph:
    """Main class that orchestrates the trading agents framework."""

    def __init__(
        self,
        selected_analysts=("market", "social", "news", "fundamentals"),
        debug=False,
        config: dict[str, Any] = None,
        callbacks: list | None = None,
    ):
        """Initialize the trading agents graph and components.

        Args:
            selected_analysts: List of analyst types to include
            debug: Whether to run in debug mode
            config: Configuration dictionary. If None, uses default config
            callbacks: Optional list of callback handlers (e.g., for tracking LLM/tool stats)
        """
        self.debug = debug
        self.config = config or DEFAULT_CONFIG
        self.callbacks = callbacks or []

        # Update the interface's config
        set_config(self.config)

        # Create necessary directories
        os.makedirs(self.config["data_cache_dir"], exist_ok=True)
        os.makedirs(self.config["results_dir"], exist_ok=True)

        self.deep_thinking_llm = self._create_role_llm(
            provider_key="deep_think_provider",
            model_key="deep_think_llm",
            base_url_key="deep_backend_url",
        )
        self.quick_thinking_llm = self._create_role_llm(
            provider_key="quick_think_provider",
            model_key="quick_think_llm",
            base_url_key="quick_backend_url",
        )
        self.final_report_llm = self._create_role_llm(
            provider_key="final_report_provider",
            model_key="final_report_llm",
            base_url_key="final_report_backend_url",
        )

        self.memory_log = TradingMemoryLog(self.config)

        # Create tool nodes
        self.tool_nodes = self._create_tool_nodes()

        # Initialize components
        self.conditional_logic = ConditionalLogic(
            max_debate_rounds=self.config["max_debate_rounds"],
            max_risk_discuss_rounds=self.config["max_risk_discuss_rounds"],
        )
        self.graph_setup = GraphSetup(
            self.quick_thinking_llm,
            self.deep_thinking_llm,
            self.tool_nodes,
            self.conditional_logic,
        )

        self.propagator = Propagator(
            max_recur_limit=self.config.get("max_recur_limit", 100),
        )
        self.reflector = Reflector(self.final_report_llm)
        self.signal_processor = SignalProcessor(self.final_report_llm)

        # State tracking
        self.curr_state = None
        self.ticker = None
        self.log_states_dict = {}  # date to full state dict

        # Graph-shape-affecting run choices, kept for the checkpoint signature.
        self.selected_analysts = tuple(selected_analysts)

        # Set up the graph: keep the workflow for recompilation with a checkpointer.
        self.workflow = self.graph_setup.setup_graph(selected_analysts)
        self.graph = self.workflow.compile()

    def _get_provider_kwargs(self, provider: str | None = None) -> dict[str, Any]:
        """Get provider-specific kwargs for an individual LLM role."""
        kwargs = {}
        provider = (provider or self.config.get("llm_provider", "")).lower()

        if provider == "google":
            thinking_level = self.config.get("google_thinking_level")
            if thinking_level:
                kwargs["thinking_level"] = thinking_level

        elif provider == "openai":
            reasoning_effort = self.config.get("openai_reasoning_effort")
            if reasoning_effort:
                kwargs["reasoning_effort"] = reasoning_effort

        elif provider == "anthropic":
            effort = self.config.get("anthropic_effort")
            if effort:
                kwargs["effort"] = effort

        # Sampling temperature is cross-provider: forward it whenever set.
        # float() here so a value coming from a TRADINGAGENTS_TEMPERATURE env
        # string ("0.2") works the same as a programmatic float.
        temperature = self.config.get("temperature")
        if temperature is not None and temperature != "":
            kwargs["temperature"] = float(temperature)

        # SDK retry budget is cross-provider. Forward it only when explicitly set
        # so each provider keeps its own default (usually 2) otherwise (#1091).
        max_retries = self.config.get("llm_max_retries")
        if max_retries is not None and max_retries != "":
            kwargs["max_retries"] = _coerce_max_retries(max_retries)

        return kwargs

    def _create_role_llm(
        self,
        provider_key: str,
        model_key: str,
        base_url_key: str,
    ):
        provider = (
            self.config.get(provider_key)
            or self.config.get("llm_provider")
            or DEFAULT_CONFIG["llm_provider"]
        )
        model = self.config.get(model_key)
        base_url = self.config.get(base_url_key) or self.config.get("backend_url")

        llm_kwargs = self._get_provider_kwargs(provider)
        if self.callbacks:
            llm_kwargs["callbacks"] = self.callbacks

        client = create_llm_client(
            provider=provider,
            model=model,
            base_url=base_url,
            **llm_kwargs,
        )
        return client.get_llm()

    def _create_tool_nodes(self) -> dict[str, ToolNode]:
        """Create tool nodes for different data sources using abstract methods."""
        return {
            "market": ToolNode(
                [
                    # Core stock data tools
                    get_stock_data,
                    # Technical indicators
                    get_indicators,
                    # Deterministic verification snapshot (bound to the analyst
                    # LLM and required by its prompt; must be executable here or
                    # the call fails and the model reports it "unavailable").
                    get_verified_market_snapshot,
                ]
            ),
            "social": ToolNode(
                [
                    # News tools for social media analysis
                    get_news,
                ]
            ),
            "news": ToolNode(
                [
                    # News and insider information
                    get_news,
                    get_global_news,
                    get_insider_transactions,
                    get_macro_indicators,
                    get_prediction_markets,
                ]
            ),
            "fundamentals": ToolNode(
                [
                    # Fundamental analysis tools
                    get_fundamentals,
                    get_balance_sheet,
                    get_cashflow,
                    get_income_statement,
                ]
            ),
        }

    def _resolve_benchmark(self, ticker: str) -> str:
        """Pick the benchmark ticker for excess-return calculation against ``ticker``.

        ``config["benchmark_ticker"]`` overrides everything when set; otherwise
        the suffix map matches the ticker's exchange suffix (e.g. ``.T`` for
        Tokyo). US-listed tickers without a dotted suffix fall through to the
        empty-suffix entry (SPY by default). Unrecognised suffixes (including
        US tickers with dots like ``BRK.B``) also fall back to the empty-suffix
        entry, which is the right default because the excess-return calculation works
        in USD.
        """
        explicit = self.config.get("benchmark_ticker")
        if explicit:
            return explicit
        benchmark_map = self.config.get("benchmark_map", {})
        ticker_upper = ticker.upper()
        for suffix, benchmark in benchmark_map.items():
            if suffix and ticker_upper.endswith(suffix.upper()):
                return benchmark
        return benchmark_map.get("", "SPY")

    def _fetch_returns(
        self, ticker: str, trade_date: str, holding_days: int = 5,
        benchmark: str = "SPY",
        available_through: str | None = None,
    ) -> RealizedOutcome | None:
        """Fetch an executable gross outcome over common trading sessions.

        Entry is the next common session's adjusted open and exit is the
        ``holding_days``-th common session's adjusted close. ``available_through``
        caps both the vendor request and usable sessions for point-in-time runs.
        """
        from tradingagents.dataflows.symbol_utils import normalize_symbol

        try:
            signal = datetime.strptime(trade_date, "%Y-%m-%d")
            fetch_start = (signal + timedelta(days=1)).strftime("%Y-%m-%d")
            buffered_end = signal + timedelta(days=holding_days * 3 + 7)
            if available_through is not None:
                cutoff = datetime.strptime(available_through, "%Y-%m-%d")
                # yfinance's end is exclusive, so include the cutoff session by
                # requesting through the following calendar day.
                buffered_end = min(buffered_end, cutoff + timedelta(days=1))
            fetch_end = buffered_end.strftime("%Y-%m-%d")

            # Normalize so the realized-return lookup hits the same instrument
            # the analysis priced (e.g. XAUUSD -> GC=F) (#984). The benchmark is
            # already a canonical Yahoo symbol from ``_resolve_benchmark``.
            stock = yf.Ticker(normalize_symbol(ticker)).history(
                start=fetch_start,
                end=fetch_end,
                auto_adjust=True,
                actions=False,
            )
            bench = yf.Ticker(benchmark).history(
                start=fetch_start,
                end=fetch_end,
                auto_adjust=True,
                actions=False,
            )

            stock_prices = stock[["Open", "Close"]].rename(
                columns={"Open": "stock_open", "Close": "stock_close"}
            )
            benchmark_prices = bench[["Open", "Close"]].rename(
                columns={"Open": "benchmark_open", "Close": "benchmark_close"}
            )
            stock_prices.index = (
                pd.to_datetime(stock_prices.index).tz_localize(None).normalize()
            )
            benchmark_prices.index = (
                pd.to_datetime(benchmark_prices.index).tz_localize(None).normalize()
            )
            common = stock_prices.join(benchmark_prices, how="inner").dropna()
            common = common[common.index > pd.Timestamp(trade_date)]
            if available_through is not None:
                common = common[common.index <= pd.Timestamp(available_through)]
            if len(common) < holding_days:
                return None

            window = common.iloc[:holding_days]
            entry = window.iloc[0]
            exit_row = window.iloc[-1]
            raw_return = float(exit_row["stock_close"] / entry["stock_open"] - 1)
            benchmark_return = float(
                exit_row["benchmark_close"] / entry["benchmark_open"] - 1
            )
            return RealizedOutcome(
                raw_return=raw_return,
                benchmark_return=benchmark_return,
                excess_return=raw_return - benchmark_return,
                holding_days=holding_days,
                entry_date=window.index[0].date().isoformat(),
                exit_date=window.index[-1].date().isoformat(),
            )
        except Exception as e:
            logger.warning(
                "Could not resolve outcome for %s on %s vs %s (will retry next run): %s",
                ticker, trade_date, benchmark, e,
            )
            return None

    def _resolve_pending_entries(
        self, ticker: str, as_of_date: str | None = None,
    ) -> None:
        """Resolve pending log entries for ticker at the start of a new run.

        Fetches returns for each same-ticker pending entry, generates reflections,
        then writes all updates in a single atomic batch write to avoid redundant I/O.
        Skips entries whose price data is not yet available (too recent or delisted).

        Trade-off: only same-ticker entries are resolved per run.  Entries for
        other tickers accumulate until that ticker is run again.
        """
        pending = [e for e in self.memory_log.get_pending_entries() if e["ticker"] == ticker]
        if not pending:
            return

        benchmark = self._resolve_benchmark(ticker)
        holding_days = validate_outcome_holding_days(
            self.config["outcome_holding_days"]
        )
        updates = []
        for entry in pending:
            outcome = self._fetch_returns(
                ticker,
                entry["date"],
                holding_days=holding_days,
                benchmark=benchmark,
                available_through=as_of_date,
            )
            if outcome is None:
                continue  # price not available yet — try again next run
            reflection = self.reflector.reflect_on_final_decision(
                final_decision=entry.get("decision", ""),
                raw_return=outcome.raw_return,
                excess_return=outcome.excess_return,
                benchmark_name=benchmark,
            )
            updates.append({
                "ticker": ticker,
                "trade_date": entry["date"],
                "raw_return": outcome.raw_return,
                "benchmark_return": outcome.benchmark_return,
                "excess_return": outcome.excess_return,
                "holding_days": outcome.holding_days,
                "entry_date": outcome.entry_date,
                "exit_date": outcome.exit_date,
                "return_basis": outcome.return_basis,
                "reflection": reflection,
            })

        if updates:
            self.memory_log.batch_update_with_outcomes(updates)

    def resolve_instrument_context(self, ticker: str, asset_type: str = "stock") -> str:
        """Resolve ticker identity once and return the full instrument context.

        Deterministic yfinance lookup (cached, fail-open) injected into a
        context string so every agent anchors to the real company instead of
        hallucinating one from the price chart (#814). Both the propagate()
        path and the CLI call this so the resolved identity reaches the whole
        graph regardless of entry point.
        """
        identity = resolve_instrument_identity(ticker)
        return build_instrument_context(ticker, asset_type, identity)

    def _run_signature(self, asset_type: str) -> str:
        """Graph-shape inputs that must invalidate a checkpoint if changed.

        Keyed into the checkpoint thread ID so a resume under a different analyst
        selection, debate/risk depth, or asset mode starts fresh instead of
        silently continuing the previous graph (#1089).
        """
        return "|".join([
            "analysts=" + ",".join(self.selected_analysts),
            f"debate={self.config['max_debate_rounds']}",
            f"risk={self.config['max_risk_discuss_rounds']}",
            f"asset={asset_type}",
        ])

    @contextmanager
    def _execution_scope(
        self,
        company_name,
        trade_date,
        asset_type,
        callbacks=None,
    ):
        """Prepare and clean up every graph execution entry point."""
        with use_analysis_context(trade_date):
            self.ticker = company_name
            self._resolve_pending_entries(company_name, as_of_date=str(trade_date))
            checkpoint_context = None
            try:
                if self.config.get("checkpoint_enabled"):
                    checkpoint_context = get_checkpointer(
                        self.config["data_cache_dir"], company_name
                    )
                    saver = checkpoint_context.__enter__()
                    self.graph = self.workflow.compile(checkpointer=saver)

                    step = checkpoint_step(
                        self.config["data_cache_dir"],
                        company_name,
                        str(trade_date),
                        self._run_signature(asset_type),
                    )
                    if step is not None:
                        logger.info(
                            "Resuming from step %d for %s on %s",
                            step,
                            company_name,
                            trade_date,
                        )
                    else:
                        logger.info(
                            "Starting fresh for %s on %s", company_name, trade_date
                        )

                past_context = self.memory_log.get_past_context(
                    company_name, as_of_date=str(trade_date)
                )
                instrument_context = self.resolve_instrument_context(
                    company_name, asset_type
                )
                initial_state = self.propagator.create_initial_state(
                    company_name,
                    trade_date,
                    asset_type=asset_type,
                    past_context=past_context,
                    instrument_context=instrument_context,
                )
                args = self.propagator.get_graph_args(callbacks=callbacks)
                if self.config.get("checkpoint_enabled"):
                    run_thread_id = thread_id(
                        company_name,
                        str(trade_date),
                        self._run_signature(asset_type),
                    )
                    args.setdefault("config", {}).setdefault("configurable", {})[
                        "thread_id"
                    ] = run_thread_id
                yield initial_state, args
            finally:
                if checkpoint_context is not None:
                    checkpoint_context.__exit__(None, None, None)
                    self.graph = self.workflow.compile()

    def _complete_run(self, company_name, trade_date, asset_type, final_state):
        """Persist state and memory only after a graph run fully completes."""
        self.curr_state = final_state
        self._log_state(trade_date, final_state)
        self.memory_log.store_decision(
            ticker=company_name,
            trade_date=str(trade_date),
            final_trade_decision=final_state["final_trade_decision"],
        )
        if self.config.get("checkpoint_enabled"):
            clear_checkpoint(
                self.config["data_cache_dir"],
                company_name,
                str(trade_date),
                self._run_signature(asset_type),
            )

    def stream(
        self,
        company_name,
        trade_date,
        asset_type: str = "stock",
        callbacks=None,
    ):
        """Yield value chunks while owning the complete execution lifecycle."""
        with self._execution_scope(
            company_name, trade_date, asset_type, callbacks=callbacks
        ) as (initial_state, args):
            final_state = None
            for chunk in self.graph.stream(initial_state, **args):
                final_state = chunk
                yield chunk
            if final_state is None:
                raise RuntimeError(
                    "Graph execution produced no value chunks for "
                    f"{company_name} on {trade_date}."
                )
            self._complete_run(company_name, trade_date, asset_type, final_state)

    def propagate(self, company_name, trade_date, asset_type: str = "stock"):
        """Run the graph to completion and return its state and processed signal."""
        return self._run_graph(company_name, trade_date, asset_type=asset_type)

    def save_reports(self, final_state, ticker, save_path=None) -> Path:
        """Write the markdown report tree for a completed run, like the CLI does.

        Programmatic callers get the same on-disk reports the CLI produces. Pass
        an explicit ``save_path`` or let it default under ``results_dir``.
        """
        if save_path is None:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_path = (
                Path(self.config["results_dir"])
                / "reports"
                / f"{safe_ticker_component(ticker)}_{stamp}"
            )
        return write_report_tree(final_state, ticker, save_path)

    def _run_graph(self, company_name, trade_date, asset_type: str = "stock"):
        """Execute the non-streaming graph lifecycle."""
        with TradingAgentsGraph._execution_scope(
            self, company_name, trade_date, asset_type
        ) as (initial_state, args):
            if self.debug:
                final_state = {}
                last_printed = None
                produced_chunk = False
                for chunk in self.graph.stream(initial_state, **args):
                    produced_chunk = True
                    messages = chunk.get("messages") or []
                    if messages:
                        message = messages[-1]
                        signature = (
                            type(message).__name__,
                            getattr(message, "content", None),
                        )
                        if signature != last_printed:
                            message.pretty_print()
                            last_printed = signature
                    final_state.update(chunk)
                if not produced_chunk:
                    raise RuntimeError(
                        "Graph execution produced no value chunks for "
                        f"{company_name} on {trade_date}."
                    )
            else:
                final_state = self.graph.invoke(initial_state, **args)
            TradingAgentsGraph._complete_run(
                self, company_name, trade_date, asset_type, final_state
            )

        return final_state, self.process_signal(final_state["final_trade_decision"])

    def _log_state(self, trade_date, final_state):
        """Log the final state to a JSON file."""
        self.log_states_dict[str(trade_date)] = {
            "company_of_interest": final_state["company_of_interest"],
            "trade_date": final_state["trade_date"],
            "market_report": final_state["market_report"],
            "sentiment_report": final_state["sentiment_report"],
            "news_report": final_state["news_report"],
            "fundamentals_report": final_state["fundamentals_report"],
            "investment_debate_state": {
                "bull_history": final_state["investment_debate_state"]["bull_history"],
                "bear_history": final_state["investment_debate_state"]["bear_history"],
                "history": final_state["investment_debate_state"]["history"],
                "current_response": final_state["investment_debate_state"][
                    "current_response"
                ],
                "judge_decision": final_state["investment_debate_state"][
                    "judge_decision"
                ],
            },
            "trader_investment_decision": final_state["trader_investment_plan"],
            "risk_debate_state": {
                "aggressive_history": final_state["risk_debate_state"]["aggressive_history"],
                "conservative_history": final_state["risk_debate_state"]["conservative_history"],
                "neutral_history": final_state["risk_debate_state"]["neutral_history"],
                "history": final_state["risk_debate_state"]["history"],
                "judge_decision": final_state["risk_debate_state"]["judge_decision"],
            },
            "investment_plan": final_state["investment_plan"],
            "final_trade_decision": final_state["final_trade_decision"],
        }

        # Save to file. Reject ticker values that would escape the
        # results directory when joined as a path component.
        safe_ticker = safe_ticker_component(self.ticker)
        directory = Path(self.config["results_dir"]) / safe_ticker / "TradingAgentsStrategy_logs"
        directory.mkdir(parents=True, exist_ok=True)

        log_path = directory / f"full_states_log_{trade_date}.json"
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(self.log_states_dict[str(trade_date)], f, indent=4)

    def process_signal(self, full_signal):
        """Process a signal to extract the core decision."""
        return self.signal_processor.process_signal(full_signal)
