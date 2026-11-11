"""yfinance-based news data fetching functions."""

import contextlib
from datetime import datetime, timedelta, timezone

import yfinance as yf
from dateutil.relativedelta import relativedelta

from .config import get_config
from .errors import VendorUnavailableError
from .stockstats_utils import yf_retry
from .symbol_utils import normalize_symbol


def triage_news_articles(ticker: str, as_of_date: str, articles: list[dict]):
    """Load the optional Jev adapter only after deterministic vendor filtering."""
    from tradingagents.agents.news_triage import triage_news_articles as triage

    return triage(ticker, as_of_date, articles)


def _as_utc(dt: datetime) -> datetime:
    """Normalize a datetime to UTC-aware; a naive value is assumed to be UTC.

    Window bounds arrive naive (parsed from ``yyyy-mm-dd``) while article
    timestamps may be offset-aware, so every operand is normalized before
    comparison. Without this the filter depends on the host timezone (#1126).
    """
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _extract_article_data(article: dict) -> dict:
    """Extract article data from yfinance news format (handles nested 'content' structure)."""
    # Handle nested content structure
    if "content" in article:
        content = article["content"]
        title = content.get("title", "No title")
        summary = content.get("summary", "")
        provider = content.get("provider", {})
        publisher = provider.get("displayName", "Unknown")

        # Get URL from canonicalUrl or clickThroughUrl
        url_obj = content.get("canonicalUrl") or content.get("clickThroughUrl") or {}
        link = url_obj.get("url", "")

        # Get publish date
        pub_date_str = content.get("pubDate", "")
        pub_date = None
        if pub_date_str:
            with contextlib.suppress(ValueError, AttributeError):
                pub_date = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))

        return {
            "title": title,
            "summary": summary,
            "publisher": publisher,
            "link": link,
            "pub_date": pub_date,
        }
    else:
        # Fallback for flat structure. Parse the epoch publish time so flat
        # articles are date-filterable too (otherwise they bypass the
        # historical window and leak future news, #992/#1007).
        pub_date = None
        ts = article.get("providerPublishTime")
        if ts:
            # Epoch seconds are UTC; parse them as UTC-aware so filtering does
            # not shift with the host timezone (#1126).
            with contextlib.suppress(ValueError, OSError, TypeError):
                pub_date = datetime.fromtimestamp(ts, tz=timezone.utc)
        return {
            "title": article.get("title", "No title"),
            "summary": article.get("summary", ""),
            "publisher": article.get("publisher", "Unknown"),
            "link": article.get("link", ""),
            "pub_date": pub_date,
        }


def _in_news_window(pub_date, start_dt, end_dt) -> bool:
    """Whether an article belongs in the half-open window ``[start, end + 1 day)``.

    Every operand is normalized to UTC, and the upper bound is exclusive so an
    article stamped exactly at midnight after ``end_dt`` cannot leak into a
    historical run (#1126). An undated article is kept only when the window
    reaches the present (live run) — in a historical/backtest window it's
    excluded, since we can't prove it isn't future news (#992/#1007).
    """
    end = _as_utc(end_dt)
    if pub_date is not None:
        return _as_utc(start_dt) <= _as_utc(pub_date) < end + timedelta(days=1)
    return end >= datetime.now(timezone.utc) - timedelta(days=1)


def _search_news(canonical: str, limit: int) -> list[dict]:
    """Keep only Yahoo search articles explicitly tagged with this symbol."""
    search = yf_retry(lambda: yf.Search(query=canonical, news_count=limit))
    news = getattr(search, "news", None) or []
    return [
        article for article in news[:limit]
        if isinstance(article, dict)
        and canonical in (str(value).upper() for value in article.get("relatedTickers") or [])
    ]


def get_news_yfinance(
    ticker: str,
    start_date: str,
    end_date: str,
) -> str:
    """
    Retrieve news for a specific stock ticker using yfinance.

    Args:
        ticker: Stock ticker symbol (e.g., "AAPL")
        start_date: Start date in yyyy-mm-dd format
        end_date: End date in yyyy-mm-dd format

    Returns:
        Formatted string containing news articles
    """
    article_limit = get_config()["news_article_limit"]
    # Query Yahoo with the canonical symbol, like every other yfinance path —
    # a raw broker/forex/crypto alias (XAUUSD, BTCUSD) otherwise silently
    # returns no news. Keep the user's ticker in the report header.
    canonical = normalize_symbol(ticker)
    resolved = "" if canonical == ticker else f" (resolved to {canonical})"
    try:
        stock = yf.Ticker(canonical)
        news = yf_retry(lambda: stock.get_news(count=article_limit)) or []
        if not news:
            news = _search_news(canonical, article_limit)

        if not news:
            raise VendorUnavailableError(
                f"Yahoo Finance news has no verified articles tagged {canonical}"
            )

        # Parse date range for filtering
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        dated_articles = []
        for article in news:
            related = article.get("relatedTickers") if isinstance(article, dict) else None
            if related is None and isinstance(article, dict):
                content = article.get("content")
                related = content.get("relatedTickers") if isinstance(content, dict) else None
            if (
                isinstance(related, list) and related
                and canonical not in {normalize_symbol(str(item)) for item in related}
            ):
                continue
            data = _extract_article_data(article)

            # Keep only articles within the requested window (look-ahead safe).
            if not _in_news_window(data["pub_date"], start_dt, end_dt):
                continue
            dated_articles.append(data)

        if not dated_articles:
            raise VendorUnavailableError(
                f"Yahoo Finance news sample for {canonical} does not establish "
                f"coverage between {start_date} and {end_date}"
            )

        eligible_indexes = [
            index for index, data in enumerate(dated_articles)
            if data["pub_date"] is not None
        ]
        eligible = [dated_articles[index] for index in eligible_indexes]
        if eligible:
            screened_keep, triage_note = triage_news_articles(
                ticker, end_date, eligible
            )
            if len(screened_keep) != len(eligible):
                screened_keep = [True] * len(eligible)
                triage_note = "Jev news triage unavailable; articles are unscreened."
        else:
            screened_keep, triage_note = [], ""
        keep = [True] * len(dated_articles)
        for index, retained in zip(eligible_indexes, screened_keep, strict=True):
            keep[index] = retained
        undated_count = len(dated_articles) - len(eligible)
        if undated_count and (
            triage_note or get_config().get("jev_news_triage_enabled", False)
        ):
            triage_note += (
                f" {undated_count} undated article(s) shown unscreened."
            )
        news_str = ""
        for data, retained in zip(dated_articles, keep, strict=True):
            if not retained:
                continue
            published = (
                _as_utc(data["pub_date"]).date().isoformat()
                if data["pub_date"] is not None else "date unavailable"
            )
            news_str += f"### {data['title']} (published: {published}; source: {data['publisher']})\n"
            if data["summary"]:
                news_str += f"{data['summary']}\n"
            if data["link"]:
                news_str += f"Link: {data['link']}\n"
            news_str += "\n"
        if not news_str:
            news_str = "No verified articles retained after relevance triage.\n"
        note = f"{triage_note}\n\n" if triage_note else ""
        return f"## {ticker}{resolved} News, from {start_date} to {end_date}:\n\n{note}{news_str}"

    except VendorUnavailableError:
        raise
    except Exception as exc:
        raise VendorUnavailableError(
            f"Yahoo Finance news request failed for {canonical}: {type(exc).__name__}"
        ) from None


def get_global_news_yfinance(
    curr_date: str,
    look_back_days: int | None = None,
    limit: int | None = None,
) -> str:
    """
    Retrieve global/macro economic news using yfinance Search.

    Args:
        curr_date: Current date in yyyy-mm-dd format
        look_back_days: Number of days to look back. ``None`` falls back to
            ``global_news_lookback_days`` from the active config.
        limit: Maximum number of articles to return. ``None`` falls back to
            ``global_news_article_limit`` from the active config.

    Returns:
        Formatted string containing global news articles
    """
    config = get_config()
    if look_back_days is None:
        look_back_days = config["global_news_lookback_days"]
    if limit is None:
        limit = config["global_news_article_limit"]
    search_queries = config["global_news_queries"]

    all_news = []
    seen_titles = set()

    try:
        for query in search_queries:
            search = yf_retry(lambda q=query: yf.Search(
                query=q,
                news_count=limit,
                enable_fuzzy_query=True,
            ))

            if search.news:
                for article in search.news:
                    # Handle both flat and nested structures
                    if "content" in article:
                        data = _extract_article_data(article)
                        title = data["title"]
                    else:
                        title = article.get("title", "")

                    # Deduplicate by title
                    if title and title not in seen_titles:
                        seen_titles.add(title)
                        all_news.append(article)

            if len(all_news) >= limit:
                break

        if not all_news:
            return f"No global news found for {curr_date}"

        # Calculate date range
        curr_dt = datetime.strptime(curr_date, "%Y-%m-%d")
        start_dt = curr_dt - relativedelta(days=look_back_days)
        start_date = start_dt.strftime("%Y-%m-%d")

        news_str = ""
        kept = 0
        for article in all_news[:limit]:
            # Extract uniformly (flat + nested) and apply the same look-ahead-safe
            # window filter, so flat articles can't leak future news (#1007).
            data = _extract_article_data(article)
            if not _in_news_window(data["pub_date"], start_dt, curr_dt):
                continue
            news_str += f"### {data['title']} (source: {data['publisher']})\n"
            if data["summary"]:
                news_str += f"{data['summary']}\n"
            if data["link"]:
                news_str += f"Link: {data['link']}\n"
            news_str += "\n"
            kept += 1

        # All candidates fell outside the window -> say so rather than return an
        # empty-bodied report (#993).
        if kept == 0:
            return f"No global news found between {start_date} and {curr_date}"

        return f"## Global Market News, from {start_date} to {curr_date}:\n\n{news_str}"

    except Exception as e:
        return f"Error fetching global news: {str(e)}"
