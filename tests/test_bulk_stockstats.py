import unittest
from unittest.mock import patch

import pandas as pd

from tradingagents.dataflows.y_finance import _get_stock_stats_bulk


class BulkStockstatsTests(unittest.TestCase):
    @patch("stockstats.wrap")
    @patch("tradingagents.dataflows.y_finance.load_ohlcv")
    def test_bulk_stockstats_handles_nan_without_name_error(self, mock_load_ohlcv, mock_wrap):
        base_df = pd.DataFrame(
            {
                "Date": pd.to_datetime(["2026-03-30", "2026-03-31"]),
                "Close": [100.0, 101.0],
                "rsi": [float("nan"), 57.5],
            }
        )
        mock_load_ohlcv.return_value = base_df.copy()
        mock_wrap.return_value = base_df.copy()

        result = _get_stock_stats_bulk("AAPL", "rsi", "2026-03-31")

        self.assertEqual(result["2026-03-30"], "N/A")
        self.assertEqual(result["2026-03-31"], "57.5")


if __name__ == "__main__":
    unittest.main()
