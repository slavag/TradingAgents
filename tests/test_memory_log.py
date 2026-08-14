"""Tests for TradingMemoryLog — storage, deferred reflection, PM injection, legacy removal."""

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import FrozenInstanceError
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import get_type_hints
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tradingagents import default_config as default_config_module
from tradingagents.agents.managers.portfolio_manager import create_portfolio_manager
from tradingagents.agents.schemas import (
    ConditionalActionPlan,
    ExistingPositionAction,
    NewPositionAction,
    PortfolioDecisionDraft,
    PortfolioRating,
    ThesisRating,
)
from tradingagents.agents.utils import memory as memory_module
from tradingagents.agents.utils.memory import TradingMemoryLog
from tradingagents.graph.propagation import Propagator
from tradingagents.graph.reflection import Reflector
from tradingagents.graph.trading_graph import TradingAgentsGraph

_SEP = TradingMemoryLog._SEPARATOR

DECISION_BUY = "Rating: Buy\nEnter at $189-192, 6% portfolio cap."
DECISION_OVERWEIGHT = (
    "Rating: Overweight\n"
    "Executive Summary: Moderate position, await confirmation.\n"
    "Investment Thesis: Strong fundamentals but near-term headwinds."
)
DECISION_SELL = "Rating: Sell\nExit position immediately."
DECISION_NO_RATING = (
    "Executive Summary: Complex situation with multiple competing factors.\n"
    "Investment Thesis: No clear directional signal at this time."
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def make_log(tmp_path, filename="trading_memory.md"):
    config = {"memory_log_path": str(tmp_path / filename)}
    return TradingMemoryLog(config)


def _seed_completed(tmp_path, ticker, date, decision_text, reflection_text, filename="trading_memory.md"):
    """Write a completed entry directly to file, bypassing the API."""
    entry = (
        f"[{date} | {ticker} | Buy | +1.0% | +0.5% | 5d]\n\n"
        f"DECISION:\n{decision_text}\n\n"
        f"REFLECTION:\n{reflection_text}"
        + _SEP
    )
    with open(tmp_path / filename, "a", encoding="utf-8") as f:
        f.write(entry)


def _resolve_entry(log, ticker, date, decision, reflection="Good call."):
    """Store a decision then immediately resolve it via the API."""
    log.store_decision(ticker, date, decision)
    log.update_with_outcome(ticker, date, 0.05, 0.02, 5, reflection)


def _fetch_with_frames(stock, benchmark, signal_date, holding_days, available_through="2026-12-31"):
    graph = MagicMock(spec=TradingAgentsGraph)
    with patch("yfinance.Ticker") as ticker_class:
        def make_ticker(symbol):
            ticker = MagicMock()
            ticker.history.return_value = benchmark if symbol == "SPY" else stock
            return ticker

        ticker_class.side_effect = make_ticker
        return TradingAgentsGraph._fetch_returns(
            graph,
            "NVDA",
            signal_date,
            holding_days=holding_days,
            benchmark="SPY",
            available_through=available_through,
        )


def _ohlc_frame(session_dates, base):
    count = len(session_dates)
    return pd.DataFrame(
        {
            "Open": [base + index for index in range(count)],
            "Close": [base + index + 1 for index in range(count)],
        },
        index=pd.to_datetime(session_dates),
    )


def _make_pm_state(past_context=""):
    """Minimal AgentState dict for portfolio_manager_node."""
    return {
        "company_of_interest": "NVDA",
        "past_context": past_context,
        "risk_debate_state": {
            "history": "Risk debate history.",
            "aggressive_history": "",
            "conservative_history": "",
            "neutral_history": "",
            "judge_decision": "",
            "current_aggressive_response": "",
            "current_conservative_response": "",
            "current_neutral_response": "",
            "count": 1,
        },
        "market_report": "Verified close: 190. Resistance: 215.",
        "sentiment_report": "Sentiment report.",
        "news_report": "News report.",
        "fundamentals_report": "Fundamentals report.",
        "investment_plan": "Research plan.",
        "trader_investment_plan": "Trader plan.",
    }


def _position_guidance():
    return {
        "thesis": ThesisRating.NEUTRAL,
        "existing_position_action": ExistingPositionAction.HOLD,
        "existing_position_summary": "Keep a medium position.",
        "new_position_action": NewPositionAction.WAIT,
        "new_position_summary": "Wait for a clearer setup.",
        "recommendation_confidence_score": 60,
        "conditional_plan": ConditionalActionPlan(
            confirmation="Reassess after fundamental and price confirmation.",
            alternative="Reassess after risk/reward improves.",
            invalidation="Avoid entry if the thesis weakens materially.",
        ),
    }


def _structured_pm_llm(captured: dict, decision: PortfolioDecisionDraft | None = None):
    """Build a MagicMock LLM whose with_structured_output binding captures the
    prompt and returns a real PortfolioDecisionDraft.
    """
    if decision is None:
        decision = PortfolioDecisionDraft(
            rating=PortfolioRating.HOLD,
            executive_summary="Hold the position; await catalyst.",
            investment_thesis="Balanced view; neither side carried the debate.",
            **_position_guidance(),
        )
    structured = MagicMock()
    structured.invoke.side_effect = lambda prompt: (
        captured.__setitem__("prompt", prompt) or decision
    )
    llm = MagicMock()
    llm.with_structured_output.return_value = structured
    return llm


# ---------------------------------------------------------------------------
# Core: storage and read path
# ---------------------------------------------------------------------------

class TestTradingMemoryLogCore:

    def test_store_creates_file(self, tmp_path):
        log = make_log(tmp_path)
        assert not (tmp_path / "trading_memory.md").exists()
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        assert (tmp_path / "trading_memory.md").exists()

    def test_store_appends_not_overwrites(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        log.store_decision("AAPL", "2026-01-11", DECISION_OVERWEIGHT)
        entries = log.load_entries()
        assert len(entries) == 2
        assert entries[0]["ticker"] == "NVDA"
        assert entries[1]["ticker"] == "AAPL"

    def test_store_decision_idempotent(self, tmp_path):
        """Calling store_decision twice with same (ticker, date) stores only one entry."""
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        assert len(log.load_entries()) == 1

    def test_concurrent_same_decision_appends_exactly_one_pending_entry(
        self, tmp_path, monkeypatch
    ):
        logs = [make_log(tmp_path), make_log(tmp_path)]
        logs[0]._log_path.write_text("", encoding="utf-8")
        barrier = threading.Barrier(2)
        original_parse_signal = memory_module.parse_decision_signal

        def synchronized_parse_signal(decision):
            rating = original_parse_signal(decision)
            barrier.wait(timeout=5)
            return rating

        monkeypatch.setattr(
            memory_module,
            "parse_decision_signal",
            synchronized_parse_signal,
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(
                    log.store_decision,
                    "NVDA",
                    "2026-01-10",
                    DECISION_BUY,
                )
                for log in logs
            ]
            for future in futures:
                future.result(timeout=5)

        entries = logs[0].load_entries()
        assert len(entries) == 1
        assert entries[0]["pending"] is True

    def test_concurrent_different_outcome_updates_both_survive(
        self, tmp_path, monkeypatch
    ):
        logs = [make_log(tmp_path), make_log(tmp_path)]
        logs[0].store_decision("NVDA", "2026-01-05", DECISION_BUY)
        logs[0].store_decision("AAPL", "2026-01-06", DECISION_SELL)
        barrier = threading.Barrier(2)
        original_read_text = Path.read_text

        def synchronized_read_text(path, *args, **kwargs):
            text = original_read_text(path, *args, **kwargs)
            if path == logs[0]._log_path and threading.current_thread().name.startswith(
                "memory-outcome"
            ):
                with suppress(threading.BrokenBarrierError):
                    barrier.wait(timeout=0.25)
            return text

        monkeypatch.setattr(Path, "read_text", synchronized_read_text)

        updates = [
            ("NVDA", "2026-01-05", "NVDA lesson."),
            ("AAPL", "2026-01-06", "AAPL lesson."),
        ]
        with ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="memory-outcome"
        ) as executor:
            futures = [
                executor.submit(
                    log.update_with_outcome,
                    ticker,
                    trade_date,
                    0.05,
                    0.02,
                    5,
                    reflection,
                )
                for log, (ticker, trade_date, reflection) in zip(
                    logs, updates, strict=True
                )
            ]
            for future in futures:
                future.result(timeout=5)

        entries = logs[0].load_entries()
        assert {(entry["ticker"], entry["reflection"]) for entry in entries} == {
            ("NVDA", "NVDA lesson."),
            ("AAPL", "AAPL lesson."),
        }
        assert all(not entry["pending"] for entry in entries)
        assert not list(tmp_path.glob("*.tmp"))
        assert not list(tmp_path.glob(".*.tmp"))

    def test_failed_atomic_update_cleans_temporary_file_without_corrupting_log(
        self, tmp_path
    ):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-05", DECISION_BUY)

        with (
            patch.object(os, "replace", side_effect=OSError("replace failed")),
            pytest.raises(OSError, match="replace failed"),
        ):
            log.update_with_outcome(
                "NVDA", "2026-01-05", 0.05, 0.02, 5, "NVDA lesson."
            )

        entries = log.load_entries()
        assert len(entries) == 1
        assert entries[0]["pending"] is True
        assert not list(tmp_path.glob("*.tmp"))
        assert not list(tmp_path.glob(".*.tmp"))

    def test_batch_update_resolves_multiple_entries(self, tmp_path):
        """batch_update_with_outcomes resolves multiple pending entries in one write."""
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-05", DECISION_BUY)
        log.store_decision("NVDA", "2026-01-12", DECISION_SELL)

        updates = [
            {"ticker": "NVDA", "trade_date": "2026-01-05",
             "raw_return": 0.05, "alpha_return": 0.02, "holding_days": 5,
             "reflection": "First correct."},
            {"ticker": "NVDA", "trade_date": "2026-01-12",
             "raw_return": -0.03, "alpha_return": -0.01, "holding_days": 5,
             "reflection": "Second correct."},
        ]
        log.batch_update_with_outcomes(updates)

        entries = log.load_entries()
        assert len(entries) == 2
        assert all(not e["pending"] for e in entries)
        assert entries[0]["reflection"] == "First correct."
        assert entries[1]["reflection"] == "Second correct."

    def test_batch_update_stores_execution_window_and_gross_basis(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-05", DECISION_BUY)
        log.batch_update_with_outcomes([
            {
                "ticker": "NVDA",
                "trade_date": "2026-01-05",
                "raw_return": 0.05,
                "benchmark_return": 0.03,
                "excess_return": 0.02,
                "holding_days": 5,
                "entry_date": "2026-01-06",
                "exit_date": "2026-01-12",
                "return_basis": "gross",
                "reflection": "Executable outcome recorded.",
            }
        ])

        raw_text = (tmp_path / "trading_memory.md").read_text(encoding="utf-8")
        assert (
            "[2026-01-05 | NVDA | Buy | +5.0% | +2.0% | 5d | "
            "2026-01-06 | 2026-01-12 | gross]"
        ) in raw_text
        entry = log.load_entries()[0]
        assert entry["excess"] == "+2.0%"
        assert entry["alpha"] == "+2.0%"
        assert entry["entry_date"] == "2026-01-06"
        assert entry["exit_date"] == "2026-01-12"
        assert entry["return_basis"] == "gross"

    def test_legacy_six_field_outcome_tag_remains_readable(self, tmp_path):
        _seed_completed(
            tmp_path,
            "NVDA",
            "2026-01-05",
            DECISION_BUY,
            "Legacy outcome.",
        )

        entry = make_log(tmp_path).load_entries()[0]

        assert entry["raw"] == "+1.0%"
        assert entry["excess"] == "+0.5%"
        assert entry["alpha"] == "+0.5%"
        assert entry["holding"] == "5d"
        assert entry["entry_date"] is None
        assert entry["exit_date"] is None
        assert entry["return_basis"] is None

    def test_pending_tag_format(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        text = (tmp_path / "trading_memory.md").read_text(encoding="utf-8")
        assert "[2026-01-10 | NVDA | Buy | pending]" in text

    # Rating parsing

    def test_rating_parsed_buy(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        assert log.load_entries()[0]["rating"] == "Buy"

    def test_rating_parsed_overweight(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("AAPL", "2026-01-11", DECISION_OVERWEIGHT)
        assert log.load_entries()[0]["rating"] == "Overweight"

    def test_unparseable_decision_is_unavailable_without_pending_outcome(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("MSFT", "2026-01-12", DECISION_NO_RATING)
        entry = log.load_entries()[0]
        assert entry["rating"] == "Unavailable"
        assert entry["pending"] is False
        assert entry["raw"] == "no-outcome"

    @pytest.mark.parametrize("status", ["Abstain", "Unavailable"])
    def test_non_actionable_decision_is_stored_without_pending_outcome(
        self,
        tmp_path,
        status,
    ):
        log = make_log(tmp_path)
        decision = (
            f"**Decision Status**: {status}\n\n"
            "**Executive Summary**: No executable trade."
        )

        log.store_decision("NVDA", "2026-08-14", decision)

        entry = log.load_entries()[0]
        assert entry["rating"] == status
        assert entry["pending"] is False
        assert entry["raw"] == "no-outcome"
        assert log.get_pending_entries() == []

    def test_rating_priority_over_prose(self, tmp_path):
        """'Rating: X' label wins even when an opposing rating word appears earlier in prose."""
        decision = (
            "The sell thesis is weak. The hold case is marginal.\n\n"
            "Rating: Buy\n\n"
            "Executive Summary: Strong fundamentals support the position."
        )
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", decision)
        assert log.load_entries()[0]["rating"] == "Buy"

    # Delimiter robustness

    def test_decision_with_markdown_separator(self, tmp_path):
        """LLM decision containing '---' must not corrupt the entry."""
        decision = "Rating: Buy\n\n---\n\nRisk: elevated volatility."
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", decision)
        entries = log.load_entries()
        assert len(entries) == 1
        assert "Risk: elevated volatility" in entries[0]["decision"]

    # load_entries

    def test_load_entries_empty_file(self, tmp_path):
        log = make_log(tmp_path)
        assert log.load_entries() == []

    def test_load_entries_single(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        entries = log.load_entries()
        assert len(entries) == 1
        e = entries[0]
        assert e["date"] == "2026-01-10"
        assert e["ticker"] == "NVDA"
        assert e["rating"] == "Buy"
        assert e["pending"] is True
        assert e["raw"] is None

    def test_load_entries_multiple(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        log.store_decision("AAPL", "2026-01-11", DECISION_OVERWEIGHT)
        log.store_decision("MSFT", "2026-01-12", DECISION_NO_RATING)
        entries = log.load_entries()
        assert len(entries) == 3
        assert [e["ticker"] for e in entries] == ["NVDA", "AAPL", "MSFT"]

    def test_decision_content_preserved(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        assert log.load_entries()[0]["decision"] == DECISION_BUY.strip()

    # get_pending_entries

    def test_get_pending_returns_pending_only(self, tmp_path):
        log = make_log(tmp_path)
        _seed_completed(tmp_path, "NVDA", "2026-01-05", "Buy NVDA.", "Correct.")
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        pending = log.get_pending_entries()
        assert len(pending) == 1
        assert pending[0]["ticker"] == "NVDA"
        assert pending[0]["date"] == "2026-01-10"

    # get_past_context

    def test_get_past_context_empty(self, tmp_path):
        log = make_log(tmp_path)
        assert log.get_past_context("NVDA") == ""

    def test_get_past_context_pending_excluded(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        assert log.get_past_context("NVDA") == ""

    def test_get_past_context_same_ticker(self, tmp_path):
        log = make_log(tmp_path)
        _seed_completed(tmp_path, "NVDA", "2026-01-05", "Buy NVDA — AI capex thesis intact.", "Directionally correct.")
        ctx = log.get_past_context("NVDA")
        assert "Past analyses of NVDA" in ctx
        assert "Buy NVDA" in ctx

    def test_get_past_context_cross_ticker(self, tmp_path):
        log = make_log(tmp_path)
        _seed_completed(tmp_path, "AAPL", "2026-01-05", "Buy AAPL — Services growth.", "Correct.")
        ctx = log.get_past_context("NVDA")
        assert "Recent cross-ticker lessons" in ctx
        assert "Past analyses of NVDA" not in ctx

    def test_past_context_excludes_same_day_and_future_entries(self, tmp_path):
        log = make_log(tmp_path)
        _seed_completed(tmp_path, "NVDA", "2026-01-05", "Old decision.", "Old lesson.")
        _seed_completed(tmp_path, "NVDA", "2026-01-10", "Same-day decision.", "Same-day lesson.")
        _seed_completed(tmp_path, "AAPL", "2026-01-12", "Future decision.", "Future lesson.")
        context = log.get_past_context("NVDA", as_of_date="2026-01-10")
        assert "Old lesson" in context
        assert "Same-day lesson" not in context
        assert "Future lesson" not in context

    def test_historical_context_excludes_malformed_legacy_date(self, tmp_path):
        log = make_log(tmp_path)
        _seed_completed(tmp_path, "NVDA", "unknown-date", "Legacy decision.", "Legacy lesson.")
        assert log.get_past_context("NVDA", as_of_date="2026-01-10") == ""

    def test_today_cutoff_excludes_same_day_and_malformed_legacy_entries(self, tmp_path):
        log = make_log(tmp_path)
        _seed_completed(tmp_path, "NVDA", "2020-01-01", "Earlier decision.", "Earlier lesson.")
        _seed_completed(
            tmp_path,
            "NVDA",
            date.today().isoformat(),
            "Same-day decision.",
            "Same-day lesson.",
        )
        _seed_completed(tmp_path, "NVDA", "unknown-date", "Legacy decision.", "Legacy lesson.")

        context = log.get_past_context("NVDA", as_of_date=date.today())

        assert "Earlier lesson." in context
        assert "Same-day lesson." not in context
        assert "Legacy lesson." not in context

    def test_past_context_without_cutoff_keeps_malformed_legacy_date(self, tmp_path):
        log = make_log(tmp_path)
        _seed_completed(tmp_path, "NVDA", "unknown-date", "Legacy decision.", "Legacy lesson.")
        assert "Legacy lesson." in log.get_past_context("NVDA")

    def test_cutoff_context_sorts_same_ticker_by_trade_date_before_quota(self, tmp_path):
        log = make_log(tmp_path)
        _seed_completed(tmp_path, "NVDA", "2026-01-09", "Newest decision.", "Newest lesson.")
        _seed_completed(tmp_path, "NVDA", "2026-01-01", "Oldest decision.", "Oldest lesson.")
        _seed_completed(tmp_path, "NVDA", "2026-01-05", "Middle decision.", "Middle lesson.")

        context = log.get_past_context(
            "NVDA", n_same=2, n_cross=0, as_of_date="2026-01-10"
        )

        assert "Newest lesson." in context
        assert "Middle lesson." in context
        assert "Oldest lesson." not in context
        assert context.index("Newest lesson.") < context.index("Middle lesson.")

    def test_cutoff_context_sorts_cross_ticker_by_trade_date_before_quota(self, tmp_path):
        log = make_log(tmp_path)
        _seed_completed(tmp_path, "AAPL", "2026-01-09", "AAPL decision.", "Newest cross.")
        _seed_completed(tmp_path, "MSFT", "2026-01-01", "MSFT decision.", "Oldest cross.")
        _seed_completed(tmp_path, "GOOG", "2026-01-05", "GOOG decision.", "Middle cross.")

        context = log.get_past_context(
            "NVDA", n_same=0, n_cross=2, as_of_date="2026-01-10"
        )

        assert "Newest cross." in context
        assert "Middle cross." in context
        assert "Oldest cross." not in context
        assert context.index("Newest cross.") < context.index("Middle cross.")

    def test_context_without_cutoff_preserves_reverse_append_order(self, tmp_path):
        log = make_log(tmp_path)
        _seed_completed(tmp_path, "NVDA", "2026-01-09", "Date-newer.", "Date-newer lesson.")
        _seed_completed(tmp_path, "NVDA", "2026-01-01", "Appended-later.", "Appended-later lesson.")

        context = log.get_past_context("NVDA", n_same=1, n_cross=0)

        assert "Appended-later lesson." in context
        assert "Date-newer lesson." not in context

    def test_get_past_context_accepts_date_annotation(self):
        hints = get_type_hints(TradingMemoryLog.get_past_context)
        assert hints["as_of_date"] == str | date | None

    def test_n_same_limit_respected(self, tmp_path):
        """Only the n_same most recent same-ticker entries are included."""
        log = make_log(tmp_path)
        for i in range(6):
            _seed_completed(tmp_path, "NVDA", f"2026-01-{i+1:02d}", f"Buy entry {i}.", "Correct.")
        ctx = log.get_past_context("NVDA", n_same=5)
        assert "Buy entry 0" not in ctx
        assert "Buy entry 5" in ctx

    def test_n_cross_limit_respected(self, tmp_path):
        """Only the n_cross most recent cross-ticker entries are included."""
        log = make_log(tmp_path)
        for i, ticker in enumerate(["AAPL", "MSFT", "GOOG", "META"]):
            _seed_completed(tmp_path, ticker, f"2026-01-{i+1:02d}", f"Buy {ticker}.", "Correct.")
        ctx = log.get_past_context("NVDA", n_cross=3)
        assert "AAPL" not in ctx
        assert "META" in ctx

    # No-op when config is None

    def test_no_log_path_is_noop(self):
        log = TradingMemoryLog(config=None)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        assert log.load_entries() == []
        assert log.get_past_context("NVDA") == ""

    # Rotation: opt-in cap on resolved entries

    def test_rotation_disabled_by_default(self, tmp_path):
        """Without max_entries, all resolved entries are kept."""
        log = make_log(tmp_path)
        for i in range(7):
            _resolve_entry(log, "NVDA", f"2026-01-{i+1:02d}", DECISION_BUY, f"Lesson {i}.")
        assert len(log.load_entries()) == 7

    def test_rotation_prunes_oldest_resolved(self, tmp_path):
        """When max_entries is set and exceeded, oldest resolved entries are pruned."""
        log = TradingMemoryLog({
            "memory_log_path": str(tmp_path / "trading_memory.md"),
            "memory_log_max_entries": 3,
        })
        # Resolve 5 entries; rotation should keep only the 3 most recent.
        for i in range(5):
            _resolve_entry(log, "NVDA", f"2026-01-{i+1:02d}", DECISION_BUY, f"Lesson {i}.")
        entries = log.load_entries()
        assert len(entries) == 3
        # Confirm the OLDEST were dropped, not the newest.
        dates = [e["date"] for e in entries]
        assert dates == ["2026-01-03", "2026-01-04", "2026-01-05"]

    def test_rotation_never_prunes_pending(self, tmp_path):
        """Pending entries (unresolved) are kept regardless of the cap."""
        log = TradingMemoryLog({
            "memory_log_path": str(tmp_path / "trading_memory.md"),
            "memory_log_max_entries": 2,
        })
        # 3 resolved + 2 pending. With cap=2, only 2 resolved survive; both pending stay.
        for i in range(3):
            _resolve_entry(log, "NVDA", f"2026-01-{i+1:02d}", DECISION_BUY, f"Resolved {i}.")
        log.store_decision("NVDA", "2026-02-01", DECISION_BUY)
        log.store_decision("NVDA", "2026-02-02", DECISION_OVERWEIGHT)
        # Trigger rotation by resolving one more entry — pending entries must stay.
        _resolve_entry(log, "NVDA", "2026-01-04", DECISION_BUY, "Resolved 3.")
        entries = log.load_entries()
        pending = [e for e in entries if e["pending"]]
        resolved = [e for e in entries if not e["pending"]]
        assert len(pending) == 2, "pending entries must never be pruned"
        assert len(resolved) == 2, f"expected 2 resolved after rotation, got {len(resolved)}"

    def test_rotation_under_cap_is_noop(self, tmp_path):
        """No rotation when resolved count <= max_entries."""
        log = TradingMemoryLog({
            "memory_log_path": str(tmp_path / "trading_memory.md"),
            "memory_log_max_entries": 10,
        })
        for i in range(3):
            _resolve_entry(log, "NVDA", f"2026-01-{i+1:02d}", DECISION_BUY, f"Lesson {i}.")
        assert len(log.load_entries()) == 3

    # Rating parsing: markdown bold and numbered list formats

    def test_rating_parsed_from_bold_markdown(self, tmp_path):
        """**Rating**: Buy — markdown bold around the label must not prevent parsing."""
        decision = "**Rating**: Buy\nEnter at $190."
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", decision)
        assert log.load_entries()[0]["rating"] == "Buy"

    def test_rating_parsed_from_bold_value(self, tmp_path):
        """Rating: **Sell** — markdown bold around the value must not prevent parsing."""
        decision = "Rating: **Sell**\nExit immediately."
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", decision)
        assert log.load_entries()[0]["rating"] == "Sell"

    def test_rating_label_wins_over_prose_with_markdown(self, tmp_path):
        """Rating: **Sell** must win even when prose contains a conflicting rating word."""
        decision = (
            "The buy thesis is weakened by guidance.\n"
            "Rating: **Sell**\n"
            "Exit before earnings."
        )
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", decision)
        assert log.load_entries()[0]["rating"] == "Sell"

    def test_rating_parsed_from_numbered_list(self, tmp_path):
        """1. Rating: Buy — numbered list prefix must not prevent parsing."""
        decision = "1. Rating: Buy\nEnter at $190."
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", decision)
        assert log.load_entries()[0]["rating"] == "Buy"


# ---------------------------------------------------------------------------
# Deferred reflection: update_with_outcome, Reflector, _fetch_returns
# ---------------------------------------------------------------------------

class TestDeferredReflection:

    # update_with_outcome

    def test_update_replaces_pending_tag(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        log.update_with_outcome("NVDA", "2026-01-10", 0.042, 0.021, 5, "Momentum confirmed.")
        text = (tmp_path / "trading_memory.md").read_text(encoding="utf-8")
        assert "[2026-01-10 | NVDA | Buy | pending]" not in text
        assert "+4.2%" in text
        assert "+2.1%" in text
        assert "5d" in text

    def test_update_appends_reflection(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        log.update_with_outcome("NVDA", "2026-01-10", 0.042, 0.021, 5, "Momentum confirmed.")
        entries = log.load_entries()
        assert len(entries) == 1
        e = entries[0]
        assert e["pending"] is False
        assert e["reflection"] == "Momentum confirmed."
        assert e["decision"] == DECISION_BUY.strip()

    def test_update_preserves_other_entries(self, tmp_path):
        """Only the matching entry is modified; all other entries remain unchanged."""
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        log.store_decision("AAPL", "2026-01-11", "Rating: Hold\nHold AAPL.")
        log.store_decision("MSFT", "2026-01-12", DECISION_SELL)
        log.update_with_outcome("AAPL", "2026-01-11", 0.01, -0.01, 5, "Neutral result.")
        entries = log.load_entries()
        assert len(entries) == 3
        nvda, aapl, msft = entries
        assert nvda["ticker"] == "NVDA" and nvda["pending"] is True
        assert aapl["ticker"] == "AAPL" and aapl["pending"] is False
        assert aapl["reflection"] == "Neutral result."
        assert msft["ticker"] == "MSFT" and msft["pending"] is True

    def test_update_atomic_write_does_not_reuse_fixed_temporary_path(self, tmp_path):
        """A stale legacy temp file is not shared with the atomic update."""
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        stale_tmp = tmp_path / "trading_memory.tmp"
        stale_tmp.write_text("GARBAGE CONTENT — should be overwritten", encoding="utf-8")
        log.update_with_outcome("NVDA", "2026-01-10", 0.042, 0.021, 5, "Correct.")
        assert stale_tmp.read_text(encoding="utf-8") == "GARBAGE CONTENT — should be overwritten"
        assert not list(tmp_path.glob(".trading_memory.md.*.tmp"))
        entries = log.load_entries()
        assert len(entries) == 1
        assert entries[0]["reflection"] == "Correct."
        assert entries[0]["pending"] is False

    def test_update_noop_when_no_log_path(self):
        log = TradingMemoryLog(config=None)
        log.update_with_outcome("NVDA", "2026-01-10", 0.05, 0.02, 5, "Reflection")

    def test_formatting_roundtrip_after_update(self, tmp_path):
        """All fields intact and blank line between tag and DECISION preserved after update."""
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-10", DECISION_BUY)
        log.update_with_outcome("NVDA", "2026-01-10", 0.042, 0.021, 5, "Momentum confirmed.")
        entries = log.load_entries()
        assert len(entries) == 1
        e = entries[0]
        assert e["pending"] is False
        assert e["decision"] == DECISION_BUY.strip()
        assert e["reflection"] == "Momentum confirmed."
        assert e["raw"] == "+4.2%"
        assert e["alpha"] == "+2.1%"
        assert e["holding"] == "5d"
        raw_text = (tmp_path / "trading_memory.md").read_text(encoding="utf-8")
        assert "[2026-01-10 | NVDA | Buy | +4.2% | +2.1% | 5d]\n\nDECISION:" in raw_text

    # Reflector.reflect_on_final_decision

    def test_reflect_on_final_decision_returns_llm_output(self):
        mock_llm = MagicMock()
        mock_llm.invoke.return_value.content = "Directionally correct. Thesis confirmed."
        reflector = Reflector(mock_llm)
        result = reflector.reflect_on_final_decision(
            final_decision=DECISION_BUY, raw_return=0.042, excess_return=0.021
        )
        assert result == "Directionally correct. Thesis confirmed."
        mock_llm.invoke.assert_called_once()

    def test_reflect_on_final_decision_includes_returns_in_prompt(self):
        """Return figures are present in the human message sent to the LLM."""
        mock_llm = MagicMock()
        mock_llm.invoke.return_value.content = "Incorrect call."
        reflector = Reflector(mock_llm)
        reflector.reflect_on_final_decision(
            final_decision=DECISION_SELL, raw_return=-0.08, excess_return=-0.05
        )
        messages = mock_llm.invoke.call_args[0][0]
        human_content = next(content for role, content in messages if role == "human")
        assert "-8.0%" in human_content
        assert "-5.0%" in human_content
        assert "Exit position immediately." in human_content

    # TradingAgentsGraph._fetch_returns

    def test_fetch_returns_enters_next_common_open_and_exits_fifth_close(self):
        stock = pd.DataFrame(
            {"Open": [100, 101, 102, 103, 104], "Close": [101, 102, 103, 104, 110]},
            index=pd.to_datetime(
                ["2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09", "2026-01-12"]
            ),
        )
        benchmark = pd.DataFrame(
            {"Open": [200, 201, 202, 203, 204], "Close": [201, 202, 203, 204, 210]},
            index=stock.index,
        )

        outcome = _fetch_with_frames(
            stock, benchmark, signal_date="2026-01-05", holding_days=5
        )

        assert type(outcome).__name__ == "RealizedOutcome"
        assert outcome.entry_date == "2026-01-06"
        assert outcome.exit_date == "2026-01-12"
        assert outcome.raw_return == pytest.approx(0.10)
        assert outcome.benchmark_return == pytest.approx(0.05)
        assert outcome.excess_return == pytest.approx(0.05)
        assert outcome.holding_days == 5
        assert outcome.return_basis == "gross"
        with pytest.raises(FrozenInstanceError):
            outcome.raw_return = 0.0

    def test_fetch_returns_inner_aligns_mismatched_calendars(self):
        stock = _ohlc_frame(["2026-01-06", "2026-01-07", "2026-01-08"], 100)
        benchmark = _ohlc_frame(["2026-01-06", "2026-01-08", "2026-01-09"], 200)

        outcome = _fetch_with_frames(
            stock, benchmark, signal_date="2026-01-05", holding_days=2
        )

        assert outcome.entry_date == "2026-01-06"
        assert outcome.exit_date == "2026-01-08"

    def test_fetch_returns_normalizes_timezone_aware_session_indexes(self):
        stock = _ohlc_frame(["2026-01-06", "2026-01-07"], 100)
        stock.index = stock.index.tz_localize("America/New_York")
        benchmark = _ohlc_frame(["2026-01-06", "2026-01-07"], 200)
        benchmark.index = benchmark.index.tz_localize("UTC")

        outcome = _fetch_with_frames(
            stock, benchmark, signal_date="2026-01-05", holding_days=2
        )

        assert outcome.entry_date == "2026-01-06"
        assert outcome.exit_date == "2026-01-07"

    def test_fetch_returns_preserves_positive_offset_exchange_session_dates(self):
        stock = pd.DataFrame(
            {"Open": [100, 105], "Close": [102, 110]},
            index=pd.to_datetime(["2026-01-06", "2026-01-07"]).tz_localize(
                "Asia/Tokyo"
            ),
        )
        benchmark = pd.DataFrame(
            {"Open": [200, 204], "Close": [202, 210]},
            index=stock.index,
        )

        outcome = _fetch_with_frames(
            stock, benchmark, signal_date="2026-01-05", holding_days=2
        )

        assert outcome.entry_date == "2026-01-06"
        assert outcome.exit_date == "2026-01-07"
        assert outcome.raw_return == pytest.approx(0.10)
        assert outcome.benchmark_return == pytest.approx(0.05)
        assert outcome.excess_return == pytest.approx(0.05)

    def test_fetch_returns_too_recent(self):
        """An incomplete common-session window stays pending."""
        one_session = _ohlc_frame(["2026-04-20"], 100)
        outcome = _fetch_with_frames(
            one_session,
            one_session,
            signal_date="2026-04-19",
            holding_days=5,
            available_through="2026-04-20",
        )
        assert outcome is None

    def test_fetch_returns_delisted(self):
        """Empty DataFrame → returns (None, None, None), no crash."""
        mock_graph = MagicMock(spec=TradingAgentsGraph)
        with patch("yfinance.Ticker") as mock_ticker_cls:
            m = MagicMock()
            m.history.return_value = pd.DataFrame(columns=["Open", "Close"])
            mock_ticker_cls.return_value = m
            outcome = TradingAgentsGraph._fetch_returns(
                mock_graph, "XXXXXFAKE", "2026-01-10"
            )
        assert outcome is None

    def test_fetch_returns_rejects_incomplete_common_session_window(self):
        stock = _ohlc_frame(
            ["2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09", "2026-01-12"],
            100,
        )
        benchmark = _ohlc_frame(["2026-01-06", "2026-01-08", "2026-01-12"], 200)

        outcome = _fetch_with_frames(
            stock, benchmark, signal_date="2026-01-05", holding_days=5
        )

        assert outcome is None

    # TradingAgentsGraph._resolve_benchmark — picks index for alpha calc

    def test_resolve_benchmark_explicit_override(self):
        """config['benchmark_ticker'] wins for every ticker."""
        mock_graph = MagicMock(spec=TradingAgentsGraph)
        mock_graph.config = {
            "benchmark_ticker": "QQQ",
            "benchmark_map": {"": "SPY", ".T": "^N225"},
        }
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "7203.T") == "QQQ"
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "NVDA") == "QQQ"

    def test_resolve_benchmark_suffix_map(self):
        """Known suffixes route to their regional index."""
        mock_graph = MagicMock(spec=TradingAgentsGraph)
        mock_graph.config = {
            "benchmark_ticker": None,
            "benchmark_map": {
                ".T": "^N225", ".HK": "^HSI", ".NS": "^NSEI",
                ".L": "^FTSE", ".TO": "^GSPTSE", ".AX": "^AXJO",
                ".BO": "^BSESN", "": "SPY",
            },
        }
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "7203.T") == "^N225"
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "0700.HK") == "^HSI"
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "RELIANCE.NS") == "^NSEI"
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "AZN.L") == "^FTSE"

    def test_resolve_benchmark_china_a_shares(self):
        """A-share tickers route to their exchange composite (uses the real
        default benchmark_map, since A-share support relies on it)."""
        from tradingagents.default_config import DEFAULT_CONFIG
        mock_graph = MagicMock(spec=TradingAgentsGraph)
        mock_graph.config = {"benchmark_ticker": None,
                             "benchmark_map": DEFAULT_CONFIG["benchmark_map"]}
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "600519.SS") == "000001.SS"
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "000001.SZ") == "399001.SZ"

    def test_resolve_benchmark_us_ticker_defaults_to_spy(self):
        """US tickers (no dotted suffix) take the empty-suffix entry."""
        mock_graph = MagicMock(spec=TradingAgentsGraph)
        mock_graph.config = {
            "benchmark_ticker": None,
            "benchmark_map": {"": "SPY", ".T": "^N225"},
        }
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "NVDA") == "SPY"
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "AAPL") == "SPY"

    def test_resolve_benchmark_unknown_suffix_falls_back(self):
        """Unrecognised suffix (BRK.B, FAKE.XX) falls back to SPY."""
        mock_graph = MagicMock(spec=TradingAgentsGraph)
        mock_graph.config = {
            "benchmark_ticker": None,
            "benchmark_map": {"": "SPY", ".T": "^N225"},
        }
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "FAKE.XX") == "SPY"
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "BRK.B") == "SPY"

    def test_resolve_benchmark_case_insensitive(self):
        """Suffix matching is case-insensitive so 7203.t resolves like 7203.T."""
        mock_graph = MagicMock(spec=TradingAgentsGraph)
        mock_graph.config = {
            "benchmark_ticker": None,
            "benchmark_map": {".T": "^N225", "": "SPY"},
        }
        assert TradingAgentsGraph._resolve_benchmark(mock_graph, "7203.t") == "^N225"

    def test_reflector_includes_benchmark_in_label(self):
        """benchmark_name appears in the prompt label, not 'SPY' hardcoded."""
        mock_llm = MagicMock()
        mock_llm.invoke.return_value.content = "Directionally correct."
        reflector = Reflector(mock_llm)
        reflector.reflect_on_final_decision(
            final_decision=DECISION_BUY,
            raw_return=0.05,
            excess_return=0.02,
            benchmark_name="^N225",
        )
        messages = mock_llm.invoke.call_args[0][0]
        human_content = next(content for role, content in messages if role == "human")
        assert "Excess return vs ^N225:" in human_content
        assert "Alpha" not in human_content

    def test_reflector_defaults_to_spy_for_unupdated_callers(self):
        """Default benchmark_name keeps the SPY label for legacy callers."""
        mock_llm = MagicMock()
        mock_llm.invoke.return_value.content = "ok"
        reflector = Reflector(mock_llm)
        reflector.reflect_on_final_decision(
            final_decision=DECISION_BUY,
            raw_return=0.05,
            excess_return=0.02,
        )
        messages = mock_llm.invoke.call_args[0][0]
        human_content = next(content for role, content in messages if role == "human")
        assert "Excess return vs SPY:" in human_content

    # TradingAgentsGraph._resolve_pending_entries

    def test_resolve_skips_other_tickers(self, tmp_path):
        """Pending AAPL entry is not resolved when the run is for NVDA."""
        log = make_log(tmp_path)
        log.store_decision("AAPL", "2026-01-10", DECISION_BUY)
        mock_graph = MagicMock(spec=TradingAgentsGraph)
        mock_graph.memory_log = log
        mock_graph._fetch_returns = MagicMock(return_value=None)
        TradingAgentsGraph._resolve_pending_entries(mock_graph, "NVDA")
        mock_graph._fetch_returns.assert_not_called()
        assert len(log.get_pending_entries()) == 1

    def test_resolve_marks_entry_completed(self, tmp_path):
        """After resolve, get_pending_entries() is empty and the entry has a REFLECTION."""
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-05", DECISION_BUY)
        mock_reflector = MagicMock()
        mock_reflector.reflect_on_final_decision.return_value = "Momentum confirmed."
        mock_graph = MagicMock(spec=TradingAgentsGraph)
        mock_graph.memory_log = log
        mock_graph.reflector = mock_reflector
        mock_graph.config = {"outcome_holding_days": 5}
        mock_graph._resolve_benchmark.return_value = "SPY"
        mock_graph._fetch_returns = MagicMock(return_value=SimpleNamespace(
            raw_return=0.05,
            benchmark_return=0.03,
            excess_return=0.02,
            holding_days=5,
            entry_date="2026-01-06",
            exit_date="2026-01-12",
            return_basis="gross",
        ))
        TradingAgentsGraph._resolve_pending_entries(
            mock_graph, "NVDA", as_of_date="2026-01-12"
        )
        assert log.get_pending_entries() == []
        entries = log.load_entries()
        assert len(entries) == 1
        assert entries[0]["pending"] is False
        assert entries[0]["reflection"] == "Momentum confirmed."
        assert "+5.0%" in entries[0]["raw"]
        assert "+2.0%" in entries[0]["alpha"]
        assert entries[0]["excess"] == "+2.0%"
        assert entries[0]["entry_date"] == "2026-01-06"
        assert entries[0]["exit_date"] == "2026-01-12"
        assert entries[0]["return_basis"] == "gross"
        mock_reflector.reflect_on_final_decision.assert_called_once_with(
            final_decision=DECISION_BUY,
            raw_return=0.05,
            excess_return=0.02,
            benchmark_name="SPY",
        )

    def test_pending_outcome_cannot_read_beyond_current_run_date(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-05", DECISION_BUY)
        graph = MagicMock(spec=TradingAgentsGraph)
        graph.memory_log = log
        graph.config = {"outcome_holding_days": 5}
        graph._resolve_benchmark.return_value = "SPY"
        graph._fetch_returns.return_value = None

        TradingAgentsGraph._resolve_pending_entries(
            graph, "NVDA", as_of_date="2026-01-07"
        )

        graph._fetch_returns.assert_called_once_with(
            "NVDA",
            "2026-01-05",
            holding_days=5,
            benchmark="SPY",
            available_through="2026-01-07",
        )

    @pytest.mark.parametrize("invalid_value", [0, -3, True, "not-a-number", 1.5])
    def test_resolve_rejects_invalid_outcome_holding_days(
        self, tmp_path, invalid_value
    ):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-05", DECISION_BUY)
        graph = MagicMock(spec=TradingAgentsGraph)
        graph.memory_log = log
        graph.config = {"outcome_holding_days": invalid_value}
        graph._resolve_benchmark.return_value = "SPY"
        graph._fetch_returns.return_value = None

        with pytest.raises(
            ValueError, match="outcome_holding_days must be a positive integer"
        ):
            TradingAgentsGraph._resolve_pending_entries(graph, "NVDA")

        graph._fetch_returns.assert_not_called()

    def test_resolve_accepts_numeric_string_outcome_holding_days(self, tmp_path):
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-05", DECISION_BUY)
        graph = MagicMock(spec=TradingAgentsGraph)
        graph.memory_log = log
        graph.config = {"outcome_holding_days": "7"}
        graph._resolve_benchmark.return_value = "SPY"
        graph._fetch_returns.return_value = None

        TradingAgentsGraph._resolve_pending_entries(graph, "NVDA")

        graph._fetch_returns.assert_called_once_with(
            "NVDA",
            "2026-01-05",
            holding_days=7,
            benchmark="SPY",
            available_through=None,
        )

    def test_outcome_holding_days_default_and_env_override(self, monkeypatch):
        assert default_config_module.DEFAULT_CONFIG["outcome_holding_days"] == 5
        monkeypatch.setenv("TRADINGAGENTS_OUTCOME_HOLDING_DAYS", "7")
        config = {"outcome_holding_days": 5}

        default_config_module._apply_env_overrides(config)

        assert config["outcome_holding_days"] == 7
        assert isinstance(config["outcome_holding_days"], int)


# ---------------------------------------------------------------------------
# Portfolio Manager injection: past_context in state and prompt
# ---------------------------------------------------------------------------

class TestPortfolioManagerInjection:

    # past_context in initial state

    def test_past_context_in_initial_state(self):
        propagator = Propagator()
        state = propagator.create_initial_state("NVDA", "2026-01-10", past_context="some context")
        assert "past_context" in state
        assert state["past_context"] == "some context"

    def test_past_context_defaults_to_empty(self):
        propagator = Propagator()
        state = propagator.create_initial_state("NVDA", "2026-01-10")
        assert state["past_context"] == ""

    # PM prompt

    def test_pm_prompt_includes_past_context(self):
        captured = {}
        llm = _structured_pm_llm(captured)
        pm_node = create_portfolio_manager(llm)
        state = _make_pm_state(past_context="[2026-01-05 | NVDA | Buy | +5.0% | +2.0% | 5d]\nGreat call.")
        pm_node(state)
        assert "Lessons from prior decisions and outcomes" in captured["prompt"]
        assert "Great call." in captured["prompt"]

    def test_pm_no_past_context_no_section(self):
        """PM prompt omits the lessons section entirely when past_context is empty."""
        captured = {}
        llm = _structured_pm_llm(captured)
        pm_node = create_portfolio_manager(llm)
        state = _make_pm_state(past_context="")
        pm_node(state)
        assert "Lessons from prior decisions" not in captured["prompt"]

    def test_pm_prompt_builds_independent_thesis_without_assuming_ownership(self):
        captured = {}
        llm = _structured_pm_llm(captured)
        pm_node = create_portfolio_manager(llm)
        pm_node(_make_pm_state())

        prompt = captured["prompt"]
        assert "Determine the thesis from evidence before choosing position actions" in prompt
        assert "Treat upstream recommendations as arguments, not independent votes" in prompt
        assert "Do not assume whether the user already owns the instrument" in prompt
        assert "Do not move more than one tier" in prompt

    def test_pm_returns_rendered_markdown_with_rating(self):
        """The structured PortfolioDecision is rendered to markdown that
        downstream consumers (memory log, signal processor, CLI display)
        can parse without any extra LLM call."""
        captured = {}
        decision = PortfolioDecisionDraft(
            rating=PortfolioRating.OVERWEIGHT,
            executive_summary="Build position gradually over the next two weeks.",
            investment_thesis="AI capex cycle remains intact; institutional flows constructive.",
            price_target=215.0,
            time_horizon="3-6 months",
            confidence_score=82,
            target_summary="Earnings revisions and trend support the central-case target.",
            supporting_quote="Verified close: 190. Resistance: 215.",
            **_position_guidance(),
        )
        llm = _structured_pm_llm(captured, decision)
        pm_node = create_portfolio_manager(llm)
        result = pm_node(_make_pm_state())
        md = result["final_trade_decision"]
        assert "**Rating**: Overweight" in md
        assert "**Executive Summary**: Build position gradually" in md
        assert "**Investment Thesis**: AI capex cycle" in md
        assert "**Thesis**: Neutral" in md
        assert "**Existing Position**: Hold" in md
        assert "**New Position**: Wait" in md
        assert "**Price Target**: 215.0" in md
        assert "**Time Horizon**: 3-6 months" in md
        assert "**Recommendation Confidence**: 60/100" in md
        assert "**Target Confidence**: 82/100" in md
        assert (
            "**Target Rationale**: Earnings revisions and trend support the "
            "central-case target."
        ) in md
        assert (
            "**Target Supporting Quote**: Verified close: 190. Resistance: 215."
            in md
        )

    def test_pm_is_unavailable_when_structured_output_is_unsupported(self):
        """Provider incompatibility must never become an unchecked Sell signal."""
        plain_response = "**Rating**: Sell\n\nExit ahead of guidance."
        llm = MagicMock()
        llm.with_structured_output.side_effect = NotImplementedError("provider unsupported")
        llm.invoke.return_value = MagicMock(content=plain_response)
        pm_node = create_portfolio_manager(llm)
        result = pm_node(_make_pm_state())
        decision = result["final_trade_decision"]
        assert "**Decision Status**: Unavailable" in decision
        assert "structured_binding_unsupported" in decision
        assert "**Rating**" not in decision
        llm.invoke.assert_not_called()

    # get_past_context ordering and limits

    def test_same_ticker_prioritised(self, tmp_path):
        """Same-ticker entries in same-ticker section; cross-ticker entries in cross-ticker section."""
        log = make_log(tmp_path)
        _resolve_entry(log, "NVDA", "2026-01-05", DECISION_BUY, "Momentum confirmed.")
        _resolve_entry(log, "AAPL", "2026-01-06", DECISION_SELL, "Overvalued.")
        result = log.get_past_context("NVDA")
        assert "Past analyses of NVDA" in result
        assert "Recent cross-ticker lessons" in result
        same_block, cross_block = result.split("Recent cross-ticker lessons")
        assert "NVDA" in same_block
        assert "AAPL" in cross_block

    def test_cross_ticker_reflection_only(self, tmp_path):
        """Cross-ticker entries show only the REFLECTION text, not the full DECISION."""
        log = make_log(tmp_path)
        _resolve_entry(log, "AAPL", "2026-01-06", DECISION_SELL, "Overvalued correction.")
        result = log.get_past_context("NVDA")
        assert "Overvalued correction." in result
        assert "Exit position immediately." not in result

    def test_n_same_limit_respected(self, tmp_path):
        """More than 5 same-ticker completed entries → only 5 injected."""
        log = make_log(tmp_path)
        for i in range(7):
            _resolve_entry(log, "NVDA", f"2026-01-{i+1:02d}", DECISION_BUY, f"Lesson {i}.")
        result = log.get_past_context("NVDA", n_same=5)
        lessons_present = sum(1 for i in range(7) if f"Lesson {i}." in result)
        assert lessons_present == 5

    def test_n_cross_limit_respected(self, tmp_path):
        """More than 3 cross-ticker completed entries → only 3 injected."""
        log = make_log(tmp_path)
        tickers = ["AAPL", "MSFT", "TSLA", "AMZN", "GOOG"]
        for i, ticker in enumerate(tickers):
            _resolve_entry(log, ticker, f"2026-01-{i+1:02d}", DECISION_BUY, f"{ticker} lesson.")
        result = log.get_past_context("NVDA", n_cross=3)
        cross_count = sum(result.count(f"{t} lesson.") for t in tickers)
        assert cross_count == 3

    # Full A→B→C integration cycle

    def test_full_cycle_store_resolve_inject(self, tmp_path):
        """store pending → resolve with outcome → past_context non-empty for PM."""
        log = make_log(tmp_path)
        log.store_decision("NVDA", "2026-01-05", DECISION_BUY)
        assert len(log.get_pending_entries()) == 1
        assert log.get_past_context("NVDA") == ""
        log.update_with_outcome("NVDA", "2026-01-05", 0.05, 0.02, 5, "Correct call.")
        assert log.get_pending_entries() == []
        past_ctx = log.get_past_context("NVDA")
        assert past_ctx != ""
        assert "NVDA" in past_ctx
        assert "Correct call." in past_ctx
        assert "DECISION:" in past_ctx
        assert "REFLECTION:" in past_ctx


# ---------------------------------------------------------------------------
# Legacy removal: BM25 / FinancialSituationMemory fully gone
# ---------------------------------------------------------------------------

class TestLegacyRemoval:

    def test_financial_situation_memory_removed(self):
        """FinancialSituationMemory must not be importable from the memory module."""
        import tradingagents.agents.utils.memory as m
        assert not hasattr(m, "FinancialSituationMemory")

    def test_bm25_not_imported(self):
        """rank_bm25 must not be present in the memory module namespace."""
        import tradingagents.agents.utils.memory as m
        assert not hasattr(m, "BM25Okapi")

    def test_reflect_and_remember_removed(self):
        """TradingAgentsGraph must not expose reflect_and_remember."""
        assert not hasattr(TradingAgentsGraph, "reflect_and_remember")

    def test_portfolio_manager_no_memory_param(self):
        """create_portfolio_manager accepts only llm; passing memory= raises TypeError."""
        mock_llm = MagicMock()
        create_portfolio_manager(mock_llm)
        with pytest.raises(TypeError):
            create_portfolio_manager(mock_llm, memory=MagicMock())

    def test_full_pipeline_no_regression(self, tmp_path):
        """propagate() completes and stores the decision after the redesign."""
        import functools

        fake_state = {
            "final_trade_decision": "Rating: Buy\nBuy NVDA.",
            "company_of_interest": "NVDA",
            "trade_date": "2026-01-10",
            "market_report": "",
            "sentiment_report": "",
            "news_report": "",
            "fundamentals_report": "",
            "investment_debate_state": {
                "bull_history": "", "bear_history": "", "history": "",
                "current_response": "", "judge_decision": "",
            },
            "investment_plan": "",
            "trader_investment_plan": "",
            "risk_debate_state": {
                "aggressive_history": "", "conservative_history": "",
                "neutral_history": "", "history": "", "judge_decision": "",
                "current_aggressive_response": "", "current_conservative_response": "",
                "current_neutral_response": "", "count": 1, "latest_speaker": "",
            },
        }
        mock_graph = MagicMock()
        mock_graph.memory_log = MagicMock(wraps=TradingMemoryLog({
            "memory_log_path": str(tmp_path / "mem.md")
        }))
        mock_graph.log_states_dict = {}
        mock_graph.debug = False
        mock_graph.config = {"results_dir": str(tmp_path)}
        mock_graph.graph.invoke.return_value = fake_state
        mock_graph.propagator.create_initial_state.return_value = fake_state
        mock_graph.propagator.get_graph_args.return_value = {}
        mock_graph.signal_processor.process_signal.return_value = "Buy"
        # Bind the real _run_graph so propagate's call to self._run_graph executes
        # the actual write path instead of the auto-MagicMock.
        mock_graph._run_graph = functools.partial(
            TradingAgentsGraph._run_graph, mock_graph
        )
        TradingAgentsGraph.propagate(mock_graph, "NVDA", "2026-01-10")
        mock_graph.memory_log.get_past_context.assert_called_once_with(
            "NVDA", as_of_date="2026-01-10"
        )
        entries = mock_graph.memory_log.load_entries()
        assert len(entries) == 1
        assert entries[0]["ticker"] == "NVDA"
        assert entries[0]["pending"] is True
