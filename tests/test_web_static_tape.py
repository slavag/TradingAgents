from pathlib import Path


WEB_STATIC_ROOT = Path(__file__).resolve().parents[1] / "tradingagents" / "web" / "static"


def test_bottom_chatter_tape_shows_why_each_ticker_is_present():
    app_js = (WEB_STATIC_ROOT / "app.js").read_text()
    styles_css = (WEB_STATIC_ROOT / "styles.css").read_text()
    index_html = (WEB_STATIC_ROOT / "index.html").read_text()

    assert "function sourceSummary" in app_js
    assert "function trendSummary" in app_js
    assert "function currentMoveSummary" in app_js
    assert "source_count" in app_js
    assert "ticker-source-line" in app_js
    assert "ticker-divider" in app_js
    assert "ticker-bottom-block" in app_js
    assert "ticker-signal-line" in app_js
    assert "ticker-rank-block" in app_js
    assert "ticker-helper ticker-rank-help" in app_js
    assert "ticker-detail-row" in app_js
    assert "ticker-avg-block" in app_js
    assert "ticker-avg-help-row" in app_js
    assert "ticker-helper ticker-avg-help" in app_js
    assert "ticker-current-change" in app_js
    assert "market_open" in app_js
    assert "Rank score" in app_js
    assert "5D move + avg checks" in app_js
    assert "not a buy/sell signal" in app_js
    assert "Moving-average checks" in app_js
    assert "price > 50D, 50D > 200D" in app_js
    assert "ticker-signal" in app_js
    assert "ticker-move" in app_js
    assert "Today" in app_js
    assert "Latest close" in app_js
    assert "market is not trading" in app_js
    assert "Top chatter candidates" in app_js
    assert "Sources " not in app_js

    assert ".ticker-pane .ticker-group" in styles_css
    assert ".ticker-pane .ticker-track" in styles_css
    assert "ticker-scroll-from-right" in styles_css
    assert "minmax(256px, 256px)" in styles_css
    assert ".ticker-source-line" in styles_css
    assert ".ticker-divider" in styles_css
    assert ".ticker-bottom-block" in styles_css
    assert ".ticker-rank-block" in styles_css
    assert ".ticker-avg-block" in styles_css
    assert ".ticker-avg-help-row" in styles_css
    assert ".ticker-helper" in styles_css
    assert "overflow: visible" in styles_css
    assert "text-overflow: clip" in styles_css
    assert "align-self: end" in styles_css
    assert "font-smoothing" in styles_css
    assert "-webkit-font-smoothing: subpixel-antialiased" in styles_css
    assert "font-family: -apple-system" in styles_css
    assert "letter-spacing: 0" in styles_css
    assert "font-size: 18px" in styles_css
    assert "font-size: 12px" in styles_css
    assert "animation-duration: 68s" in styles_css
    assert "color: #f7f9ff" in styles_css
    assert ".ticker-current-change" in styles_css
    assert ".ticker-signal" in styles_css
    assert ".ticker-move" in styles_css
    assert "margin-left: auto" in styles_css
    assert "text-align: right" in styles_css
    assert ".ticker-source-count" not in styles_css

    assert "Chatter Candidates" in index_html
    assert "Rank Score" in index_html
    assert "Avg Checks" in index_html
