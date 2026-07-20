"""Point-in-time boundaries for fundamental data vendors."""

import pytest

from tradingagents.dataflows import alpha_vantage_fundamentals, y_finance
from tradingagents.dataflows.temporal import use_analysis_context


def _vendor_must_not_be_called(*_args, **_kwargs):
    raise AssertionError("vendor must not be called for historical fundamentals")


@pytest.mark.unit
@pytest.mark.parametrize(
    "method,args",
    [
        (y_finance.get_fundamentals, ("NVDA", "2020-01-02")),
        (y_finance.get_balance_sheet, ("NVDA", "quarterly", "2020-01-02")),
        (y_finance.get_cashflow, ("NVDA", "quarterly", "2020-01-02")),
        (y_finance.get_income_statement, ("NVDA", "quarterly", "2020-01-02")),
    ],
)
def test_yfinance_fundamentals_are_unavailable_historically(monkeypatch, method, args):
    monkeypatch.setattr(y_finance.yf, "Ticker", _vendor_must_not_be_called)

    result = method(*args)

    assert result.startswith("DATA_UNAVAILABLE:")


@pytest.mark.unit
def test_yfinance_insider_transactions_are_unavailable_historically(monkeypatch):
    monkeypatch.setattr(y_finance.yf, "Ticker", _vendor_must_not_be_called)

    with use_analysis_context("2020-01-02"):
        result = y_finance.get_insider_transactions("NVDA")

    assert result.startswith("DATA_UNAVAILABLE:")


@pytest.mark.unit
@pytest.mark.parametrize(
    "method",
    [
        alpha_vantage_fundamentals.get_fundamentals,
        alpha_vantage_fundamentals.get_balance_sheet,
        alpha_vantage_fundamentals.get_cashflow,
        alpha_vantage_fundamentals.get_income_statement,
    ],
)
def test_alpha_vantage_fundamentals_are_unavailable_historically(monkeypatch, method):
    monkeypatch.setattr(
        alpha_vantage_fundamentals,
        "_make_api_request",
        _vendor_must_not_be_called,
    )

    result = method("NVDA", curr_date="2020-01-02")

    assert result.startswith("DATA_UNAVAILABLE:")
