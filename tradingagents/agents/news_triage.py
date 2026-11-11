"""Optional Jev relevance triage for verified, ticker-specific news articles."""

from __future__ import annotations

import math
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

from tradingagents.agents.post_screen import system_one
from tradingagents.dataflows.config import get_config
from tradingagents.dataflows.temporal import is_historical

_MAX_ARTICLES = 20
_WORKERS = 4
_DROP_BELOW = 0.10
_MAX_TEXT = 1600
_BATCH_TIMEOUT = 10.0
_REQUEST_TIMEOUT = 5.0

_QUESTION = {
    "relevant": {
        "type": "noul",
        "instructions": (
            "Is this news article primarily about `instrument` as an investable "
            "company or asset, rather than only mentioning its ticker in passing?"
        ),
        "criteria": {
            "true": "The article materially discusses the instrument, its business, products, results, or outlook.",
            "false": "The article is about another instrument, generic market news, or merely lists this ticker.",
        },
    }
}


def _article_text(article: dict) -> str:
    title = str(article.get("title") or "")
    summary = str(article.get("summary") or "")
    text = re.sub(r"(?:https?://|www\.)\S+", "", f"{title}\n{summary}")
    return text[:_MAX_TEXT]


def _score(response: dict) -> float:
    value = response["relevant"]["noul"]
    if (
        isinstance(value, bool) or not isinstance(value, (int, float))
        or not math.isfinite(value) or not 0 <= value <= 1
    ):
        raise ValueError("malformed Jev relevance score")
    return float(value)


def triage_news_articles(
    ticker: str, as_of_date: str, articles: list[dict]
) -> tuple[list[bool], str]:
    """Keep uncertain items; fail open if Jev cannot classify the bounded set."""
    keep_all = [True] * len(articles)
    if not get_config().get("jev_news_triage_enabled", False) or is_historical(as_of_date):
        return keep_all, ""
    if not os.environ.get("TYPESAFE_API_KEY"):
        return keep_all, "Jev news triage unavailable (key missing); articles are unscreened."
    if not articles:
        return keep_all, ""

    screened = articles[:_MAX_ARTICLES]
    scores: list[float | None] = [None] * len(screened)
    try:
        pool = ThreadPoolExecutor(max_workers=_WORKERS)
        try:
            futures = {
                pool.submit(
                    system_one,
                    {"instrument": ticker, "article": _article_text(article)},
                    _QUESTION, attempts=1, timeout=_REQUEST_TIMEOUT,
                ): index
                for index, article in enumerate(screened)
            }
            for future in as_completed(futures, timeout=_BATCH_TIMEOUT):
                scores[futures[future]] = _score(future.result())
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
        if any(score is None for score in scores):
            raise ValueError("missing Jev relevance score")
    except Exception:
        return keep_all, "Jev news triage unavailable; articles are unscreened."

    kept = [score >= _DROP_BELOW for score in scores] + [True] * (len(articles) - len(screened))
    note = f"Jev news relevance triage: {sum(kept[:len(screened)])} of {len(screened)} screened articles kept."
    if len(articles) > len(screened):
        note += f" {len(articles) - len(screened)} additional articles shown unscreened."
    return kept, note
