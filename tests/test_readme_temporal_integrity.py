from pathlib import Path


def test_readme_documents_historical_data_boundaries():
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "Historical analysis boundaries" in readme
    assert "StockTwits, Reddit, and Polymarket are unavailable" in readme
    assert "next common trading session's adjusted open" in readme
    assert "Model confidence is not a calibrated probability" in readme
    assert (
        "Memory for any supplied analysis date, including today, excludes same-day, "
        "future, and malformed entries." in readme
    )
    assert (
        "Historical social and prediction sources without point-in-time archives are unavailable."
        in readme
    )
    assert "Live runs remain variable." in readme
    assert "Dated news is filtered to the requested cutoff." in readme
    assert "gross raw return and excess return versus the configured benchmark" in readme
