"""Yahoo percentage-valued fundamentals must state their units."""

from types import SimpleNamespace

from tradingagents.dataflows import y_finance


def test_yahoo_dividend_yield_and_debt_ratio_keep_percent_units(monkeypatch):
    info = {
        "longName": "Example Corp",
        "dividendYield": 0.41,
        "debtToEquity": 78.4,
    }
    monkeypatch.setattr(
        y_finance.yf, "Ticker", lambda _ticker: SimpleNamespace(info=info)
    )
    result = y_finance.get_fundamentals("EXAMPLE")
    assert "Dividend Yield: 0.41%" in result
    assert "Debt to Equity: 78.4% (0.78x)" in result


def test_yahoo_money_fields_name_quote_main_and_reporting_currencies(monkeypatch):
    info = {
        "longName": "Example PLC",
        "currency": "GBp",
        "financialCurrency": "TWD",
        "marketCap": 1000000,
        "trailingEps": 1.2,
        "fiftyTwoWeekHigh": 250.5,
        "totalRevenue": 9000000,
        "freeCashflow": 700000,
    }
    monkeypatch.setattr(
        y_finance.yf, "Ticker", lambda _ticker: SimpleNamespace(info=info)
    )
    result = y_finance.get_fundamentals("EXAMPLE")
    assert "Market Cap: 1000000 GBP" in result
    assert "EPS (TTM): 1.2 GBP" in result
    assert "52 Week High: 250.5 GBp" in result
    assert "Revenue (TTM): 9000000 TWD" in result
    assert "Free Cash Flow: 700000 TWD" in result
