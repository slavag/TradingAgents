"""Offline contracts for US-equity momentum discovery."""

from dataclasses import replace
from datetime import date
from importlib import import_module

import pandas as pd
import pytest


def screen_module():
    return import_module("tradingagents.web.momentum_screen")


def bars(*, end="2026-10-15", count=51, unadjusted=15.0, volume=1_000_000,
         flatten_last_five=False):
    module = screen_module()
    dates = pd.bdate_range(end=end, periods=count)
    values = [10.0 + index * 0.1 for index in range(count)]
    if flatten_last_five:
        values[-5:] = [values[-6]] * 5
    return tuple(module.DailyBar(
        date=stamp.date(), adjusted_close=value,
        unadjusted_close=unadjusted, volume=volume,
    ) for stamp, value in zip(dates, values, strict=True))


def equity(symbol="NEW", **overrides):
    module = screen_module()
    values = {
        "symbol": symbol, "name": f"{symbol} Inc.", "exchange": "NASDAQ",
        "currency": "USD", "quote_type": "EQUITY", "active": True,
    }
    values.update(overrides)
    return module.EquityIdentity(**values)


def screen_one(*, symbol="NEW", history=None, identity=None, sources=None):
    module = screen_module()
    return module.screen_candidates(
        {symbol: sources or {"alpha_vantage_gainers"}},
        {symbol: history if history is not None else bars()},
        {symbol: identity if identity is not None else equity(symbol)},
        as_of=date(2026, 10, 15), analyzed=set(),
    )


def test_new_research_name_precedes_higher_corrobated_old_name():
    module = screen_module()
    same = bars()
    ranked = module.screen_candidates(
        {
            "NEW": {"alpha_vantage_gainers"},
            "OLD": {"alpha_vantage_gainers", "apewisdom"},
        },
        {"NEW": same, "OLD": same},
        {"NEW": equity("NEW"), "OLD": equity("OLD")},
        as_of=date(2026, 10, 15), analyzed={"OLD"},
    )

    assert [item.ticker for item in ranked] == ["NEW", "OLD"]
    assert ranked[0].already_analyzed is False
    assert ranked[1].already_analyzed is True
    assert ranked[1].score == round(ranked[0].score + 2, 2)
    assert ranked[0].close == 15.0
    assert ranked[0].quote_date == date(2026, 10, 15)
    assert ranked[0].ret_5d_pct > 0
    assert "+3.4%" in ranked[0].reason and "volume" in ranked[0].reason


@pytest.mark.parametrize("overrides", [
    {"quote_type": "ETF"},
    {"quote_type": ""},
    {"exchange": "OTC"},
    {"currency": "CAD"},
    {"active": False},
])
def test_only_verified_active_us_equities_qualify(overrides):
    assert screen_one(identity=equity(**overrides)) == []


def test_price_liquidity_return_history_and_freshness_are_hard_gates():
    assert screen_one(history=bars(unadjusted=4.99)) == []
    assert screen_one(history=bars(volume=100_000)) == []
    assert screen_one(history=bars(flatten_last_five=True)) == []
    assert screen_one(history=bars(count=49)) == []
    assert screen_one(history=bars(end="2026-10-09")) == []


def test_liquidity_uses_unadjusted_close_not_high_adjusted_close():
    history = bars(unadjusted=15.0, volume=1_000_000)
    low_dollar_volume = tuple(replace(
        bar, adjusted_close=bar.adjusted_close * 20,
        unadjusted_close=7.0, volume=1_000_000,
    ) for bar in history)

    assert screen_one(history=low_dollar_volume) == []
    assert screen_one(history=tuple(replace(bar, unadjusted_close=None)
                                    for bar in history)) == []


def test_nyse_holiday_and_weekend_do_not_age_a_quote_as_calendar_days():
    module = screen_module()

    assert module.quote_is_fresh(date(2026, 4, 2), date(2026, 4, 6))
    assert not module.quote_is_fresh(date(2026, 3, 31), date(2026, 4, 6))
    assert not module.quote_is_fresh(date(2026, 4, 4), date(2026, 4, 6))


def test_extreme_one_day_spike_is_flagged_and_score_is_bounded():
    history = list(bars())
    latest = history[-1]
    history[-1] = replace(latest, adjusted_close=latest.adjusted_close * 1.3)

    found = screen_one(history=tuple(history), sources={"apewisdom"})

    assert len(found) == 1
    assert "high_volatility_spike" in found[0].risk_flags
    assert 0 <= found[0].score <= 14
