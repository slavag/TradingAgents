import json
from datetime import date, datetime

from .alpha_vantage_common import _make_api_request, format_datetime_for_api
from .config import get_config
from .symbol_utils import normalize_symbol
from .temporal import historical_unavailable


def triage_news_articles(ticker: str, as_of_date: str, articles: list[dict]):
    """Load the optional classifier only after the dated ticker feed is filtered."""
    from tradingagents.agents.news_triage import triage_news_articles as triage

    return triage(ticker, as_of_date, articles)


def _published_on(value) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value[:8], "%Y%m%d").date()
    except ValueError:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None


def get_news(ticker, start_date, end_date) -> dict[str, str] | str:
    """Returns live and historical market news & sentiment data from premier news outlets worldwide.

    Covers stocks, cryptocurrencies, forex, and topics like fiscal policy, mergers & acquisitions, IPOs.

    Args:
        ticker: Stock symbol for news articles.
        start_date: Start date for news search.
        end_date: End date for news search.

    Returns:
        Dictionary containing news sentiment data or JSON string.
    """

    params = {
        "tickers": ticker,
        "time_from": format_datetime_for_api(start_date),
        "time_to": date.fromisoformat(end_date).strftime("%Y%m%dT2359"),
    }

    response = _make_api_request("NEWS_SENTIMENT", params)
    if not get_config().get("jev_news_triage_enabled", False) or historical_unavailable(
        "Jev ticker-news triage", end_date
    ):
        return response
    try:
        payload = json.loads(response) if isinstance(response, str) else dict(response)
        feed = payload.get("feed")
        start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    except (ValueError, TypeError, AttributeError):
        return response
    if not isinstance(feed, list):
        return response

    canonical = normalize_symbol(ticker)
    retained = []
    eligible = []
    eligible_indexes = []
    uncertain = 0
    for article in feed:
        if not isinstance(article, dict):
            continue
        published = _published_on(article.get("time_published"))
        if published is not None and not start <= published <= end:
            continue
        tags = article.get("ticker_sentiment")
        tagged = {
            normalize_symbol(str(entry.get("ticker")))
            for entry in tags if isinstance(entry, dict) and entry.get("ticker")
        } if isinstance(tags, list) else set()
        if tagged and canonical not in tagged:
            continue
        retained.append(article)
        if published is not None and canonical in tagged:
            eligible_indexes.append(len(retained) - 1)
            eligible.append({
                "title": article.get("title"),
                "summary": article.get("summary"),
            })
        else:
            uncertain += 1

    if eligible:
        keep, note = triage_news_articles(ticker, end_date, eligible)
        if len(keep) != len(eligible):
            keep = [True] * len(eligible)
            note = "Jev news triage unavailable; articles are unscreened."
        for index, kept in reversed(list(zip(eligible_indexes, keep, strict=True))):
            if not kept:
                del retained[index]
    else:
        note = "Jev news triage skipped: no dated, ticker-tagged articles were verified."
    if uncertain:
        note += f" {uncertain} article(s) without verified date or ticker tag shown unscreened."
    payload["feed"] = retained
    if "items" in payload:
        payload["items"] = len(retained)
    payload["tradingagents_news_triage"] = note
    return json.dumps(payload, ensure_ascii=False)

def get_global_news(curr_date, look_back_days: int = 7, limit: int = 50) -> dict[str, str] | str:
    """Returns global market news & sentiment data without ticker-specific filtering.

    Covers broad market topics like financial markets, economy, and more.

    Args:
        curr_date: Current date in yyyy-mm-dd format.
        look_back_days: Number of days to look back (default 7).
        limit: Maximum number of articles (default 50).

    Returns:
        Dictionary containing global news sentiment data or JSON string.
    """
    from datetime import datetime, timedelta

    # Calculate start date
    curr_dt = datetime.strptime(curr_date, "%Y-%m-%d")
    start_dt = curr_dt - timedelta(days=look_back_days)
    start_date = start_dt.strftime("%Y-%m-%d")

    params = {
        "topics": "financial_markets,economy_macro,economy_monetary",
        "time_from": format_datetime_for_api(start_date),
        "time_to": format_datetime_for_api(curr_date),
        "limit": str(limit),
    }

    return _make_api_request("NEWS_SENTIMENT", params)


def get_insider_transactions(symbol: str) -> dict[str, str] | str:
    """Returns latest and historical insider transactions by key stakeholders.

    Covers transactions by founders, executives, board members, etc.

    Args:
        symbol: Ticker symbol. Example: "IBM".

    Returns:
        Dictionary containing insider transaction data or JSON string.
    """

    unavailable = historical_unavailable("Alpha Vantage insider transactions")
    if unavailable:
        return unavailable

    params = {
        "symbol": symbol,
    }

    return _make_api_request("INSIDER_TRANSACTIONS", params)
