from pathlib import Path


def test_readme_documents_historical_data_boundaries():
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "Historical analysis boundaries" in readme
    assert "StockTwits, Reddit, and Polymarket are unavailable" in readme
    assert "next common trading session's adjusted open" in readme
    assert "Model confidence is not a calibrated probability" in readme
