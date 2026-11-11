"""Sub-cent and currency-pair prices retain useful significant digits."""

import pandas as pd
import pytest

from tradingagents.dataflows import y_finance
from tradingagents.dataflows.market_data_validator import _fmt


@pytest.mark.parametrize("value,expected", [
    (0.0001234, "0.0001234"),
    (1.08736, "1.08736"),
    (123.45, "123.45"),
    (0.0, "0.00"),
])
def test_verified_snapshot_numeric_format_keeps_significant_price_digits(value, expected):
    assert _fmt(value) == expected


@pytest.mark.parametrize("price,expected", [
    (0.0001234, "0.0001234"),
    (1.08736, "1.08736"),
])
def test_yahoo_stock_csv_keeps_subcent_and_pip_prices(monkeypatch, price, expected):
    class FakeTicker:
        def history(self, **_kwargs):
            return pd.DataFrame(
                {"Open": [price], "High": [price], "Low": [price],
                 "Close": [price], "Volume": [1000]},
                index=pd.to_datetime(["2026-10-16"]),
            )

    monkeypatch.setattr(y_finance.yf, "Ticker", lambda _ticker: FakeTicker())
    text = y_finance.get_YFin_data_online(
        "EURUSD=X", "2026-10-16", "2026-10-16"
    )
    assert expected in text
