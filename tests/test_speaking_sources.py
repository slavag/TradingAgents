import unittest

from tradingagents.web.speaking_sources import (
    MOTLEY_FOOL_GAINERS_URL,
    MOTLEY_FOOL_MARKETS_URL,
    MOTLEY_FOOL_MOST_ACTIVE_URL,
    extract_symbols_from_market_page_html,
    extract_symbols_from_stock_page_html,
    fetch_external_market_symbols,
    parse_apewisdom_results,
    parse_stocktwits_results,
)


class SpeakingSourcesTests(unittest.TestCase):
    def test_extract_symbols_from_stock_table_html(self):
        html = """
        <table>
          <thead>
            <tr><th>Company</th><th>Symbol</th><th>Price</th></tr>
          </thead>
          <tbody>
            <tr><td>NVIDIA</td><td>NVDA</td><td>$120</td></tr>
            <tr><td>Palantir</td><td>PLTR</td><td>$24</td></tr>
          </tbody>
        </table>
        """

        self.assertEqual(extract_symbols_from_stock_page_html(html), {"NVDA", "PLTR"})

    def test_extract_symbols_from_market_quote_links(self):
        html = """
        <section>
          <a href="/quote/nasdaq/amzn/">AMZN</a>
          <a href="/quote/nasdaq/goog/">GOOG</a>
          <a href="/quote/nyse/brk.b/">BRK.B</a>
          <a href="/markets/">Markets</a>
        </section>
        """

        self.assertEqual(
            extract_symbols_from_market_page_html(html),
            {"AMZN", "GOOG", "BRK.B"},
        )

    def test_fetch_external_market_symbols_filters_to_allowed_universe(self):
        html_by_url = {
            MOTLEY_FOOL_GAINERS_URL: """
                <table>
                  <tr><th>Symbol</th></tr>
                  <tr><td>NVDA</td></tr>
                  <tr><td>PLTR</td></tr>
                </table>
            """,
            MOTLEY_FOOL_MOST_ACTIVE_URL: """
                <table>
                  <tr><th>Symbol</th></tr>
                  <tr><td>AMZN</td></tr>
                  <tr><td>GOOG</td></tr>
                </table>
            """,
            MOTLEY_FOOL_MARKETS_URL: """
                <a href="/quote/nasdaq/msft/">MSFT</a>
                <a href="/quote/nasdaq/nvda/">NVDA</a>
                <a href="/quote/index/dji/">DJI</a>
            """,
        }

        def fake_fetcher(url: str) -> str:
            return html_by_url[url]

        result = fetch_external_market_symbols(
            allowed_symbols={"NVDA", "AMZN", "MSFT"},
            html_fetcher=fake_fetcher,
        )

        self.assertEqual(result["motley_fool_gainers"], {"NVDA"})
        self.assertEqual(result["motley_fool_most_active"], {"AMZN"})
        self.assertEqual(result["motley_fool_markets"], {"MSFT", "NVDA"})

    def test_parse_apewisdom_results_keeps_mentions(self):
        payload = {
            "results": [
                {"ticker": "NVDA", "mentions": 82, "rank": 1},
                {"ticker": "PLTR", "mentions": 37, "rank": 2},
            ]
        }

        result = parse_apewisdom_results(payload, allowed_symbols={"NVDA", "PLTR"})

        self.assertEqual(result["NVDA"]["mentions"], 82)
        self.assertEqual(result["PLTR"]["rank"], 2)

    def test_parse_stocktwits_results_uses_trending_score_or_rank_fallback(self):
        payload = {
            "symbols": [
                {"symbol": "NVDA", "trending_score": 91.5},
                {"symbol": "PLTR"},
            ]
        }

        result = parse_stocktwits_results(payload, allowed_symbols={"NVDA", "PLTR"})

        self.assertEqual(result["NVDA"]["trending_score"], 91.5)
        self.assertEqual(result["PLTR"]["trending_score"], 1.0)


if __name__ == "__main__":
    unittest.main()
