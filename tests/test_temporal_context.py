from datetime import date

import pytest

from tradingagents.dataflows.temporal import (
    AnalysisMode,
    build_analysis_context,
    get_analysis_context,
    historical_unavailable,
    use_analysis_context,
)
from tradingagents.graph.propagation import Propagator


def test_past_date_is_historical():
    context = build_analysis_context("2026-07-19", today=date(2026, 7, 20))
    assert context.mode is AnalysisMode.HISTORICAL


def test_today_is_live():
    context = build_analysis_context("2026-07-20", today=date(2026, 7, 20))
    assert context.mode is AnalysisMode.LIVE


def test_future_date_is_rejected():
    with pytest.raises(ValueError, match="cannot be in the future"):
        build_analysis_context("2026-07-21", today=date(2026, 7, 20))


def test_context_is_restored_after_scope():
    assert get_analysis_context() is None
    with use_analysis_context("2026-07-19", today=date(2026, 7, 20)):
        assert get_analysis_context().mode is AnalysisMode.HISTORICAL
        message = historical_unavailable("StockTwits")
        assert "StockTwits" in message
        assert "2026-07-19" in message
        assert "do not treat missing data as neutral" in message
    assert get_analysis_context() is None


def test_initial_state_exposes_temporal_contract():
    state = Propagator().create_initial_state("NVDA", "2020-01-02")
    assert state["as_of_date"] == "2020-01-02"
    assert state["analysis_mode"] == "historical"
