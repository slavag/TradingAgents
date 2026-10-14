"""Tests for tolerating a non-`Date` index column in stockstats_utils (#890).

Guards against a download frame whose date column is `index` or `Datetime`
instead of `Date`, which would otherwise silently drop every indicator.
"""

from __future__ import annotations

import warnings

import pandas as pd
import pytest

from tradingagents.dataflows import stockstats_utils as su


def _ohlcv(date_col: str) -> pd.DataFrame:
    """OHLCV frame whose date column is named `date_col`."""
    dates = pd.bdate_range("2026-04-01", periods=10)
    return pd.DataFrame({
        date_col: dates,
        "Open": [100.0 + i for i in range(10)],
        "High": [101.0 + i for i in range(10)],
        "Low": [99.0 + i for i in range(10)],
        "Close": [100.5 + i for i in range(10)],
        "Volume": [1_000_000 + i for i in range(10)],
    })


@pytest.mark.unit
class TestEnsureDateColumn:
    def test_renames_index_column(self):
        out = su._ensure_date_column(_ohlcv("index"))
        assert "Date" in out.columns and "index" not in out.columns

    def test_renames_datetime_and_date_variants(self):
        assert "Date" in su._ensure_date_column(_ohlcv("Datetime")).columns
        assert "Date" in su._ensure_date_column(_ohlcv("date")).columns

    def test_leaves_existing_date_untouched(self):
        df = _ohlcv("Date")
        assert su._ensure_date_column(df) is df  # no-op short-circuit

    def test_no_datelike_column_is_left_alone(self):
        df = pd.DataFrame({"Close": [1, 2, 3]})
        out = su._ensure_date_column(df)
        assert "Date" not in out.columns  # nothing to rename; caller handles


@pytest.mark.unit
class TestCleanDataframeAcrossVersions:
    @pytest.mark.parametrize("sliced_input", [False, True])
    @pytest.mark.parametrize("copy_on_write", [False, True])
    def test_cleaning_invalid_rows_owns_its_data_without_copy_warnings(
        self, sliced_input, copy_on_write
    ):
        source = pd.DataFrame({
            "Date": ["2026-04-01", "invalid", "2026-04-03", "2026-04-06", "2026-04-07"],
            "Open": ["100", "101", None, "104", "106"],
            "High": ["101", "102", "103", None, "107"],
            "Low": ["99", "100", "101", "103", "105"],
            "Close": ["100.5", "101.5", "bad", "104.5", "106.5"],
            "Volume": ["1000", "1001", "1002", None, "1004"],
        })
        original = source.copy(deep=True)
        data = source.iloc[:-1] if sliced_input else source
        input_original = data.copy(deep=True)

        with (
            pd.option_context("mode.copy_on_write", copy_on_write, "mode.chained_assignment", "warn"),
            warnings.catch_warnings(),
        ):
            warnings.simplefilter("error", pd.errors.SettingWithCopyWarning)
            cleaned = su._clean_dataframe(data)

        expected_indices = [0, 3] if sliced_input else [0, 3, 4]
        assert cleaned.index.tolist() == expected_indices
        assert cleaned.loc[3, "Close"] == 104.5
        assert cleaned.loc[3, "High"] == 101.0
        assert cleaned.loc[3, "Volume"] == 1000.0
        assert all(pd.api.types.is_numeric_dtype(cleaned[column]) for column in (
            "Open", "High", "Low", "Close", "Volume"
        ))
        pd.testing.assert_frame_equal(data, input_original)
        pd.testing.assert_frame_equal(source, original)

    def test_clean_handles_index_column(self):
        """A frame with `index` instead of `Date` must still clean to a
        usable, date-parsed frame (was KeyError: 'Date')."""
        cleaned = su._clean_dataframe(_ohlcv("index"))
        assert "Date" in cleaned.columns
        assert pd.api.types.is_datetime64_any_dtype(cleaned["Date"])
        assert len(cleaned) == 10

    def test_clean_handles_legacy_date_column(self):
        cleaned = su._clean_dataframe(_ohlcv("Date"))
        assert len(cleaned) == 10

    def test_indicators_compute_after_index_rename(self):
        """stockstats must compute indicators on a frame whose date column
        arrived as `index`, instead of erroring per indicator."""
        from stockstats import wrap
        cleaned = su._clean_dataframe(_ohlcv("index"))
        df = wrap(cleaned)
        df["close_5_sma"]  # triggers calculation
        assert "close_5_sma" in df.columns
        assert df["close_5_sma"].notna().any()
