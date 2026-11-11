"""Explicit, live-only Jev screening for fetched social posts.

Posts leave this process when TYPESAFE_API_KEY is configured for a live run.
Historical analyses never send posts. A failed screen keeps
the original posts and labels them unscreened.
"""

from __future__ import annotations

import logging
import math
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from ipaddress import ip_address
from urllib.parse import urlsplit

import requests

from tradingagents.agents.utils.agent_utils import resolve_instrument_identity
from tradingagents.dataflows.temporal import is_historical

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://api.typesafe.ai"
_DEFAULT_MODEL = "jev-latest"
_RETRY_STATUSES = (429, 529)
_ATTEMPTS = 3
_MAX_WAIT = 30.0
_TIMEOUT = 15.0
_WORKERS = 16
_OFF_TOPIC_BELOW = 0.3
_STANCE_CONFIDENCE = 0.5

QUESTIONS = {
    "about": {
        "type": "noul",
        "instructions": "Is `post` about `instrument`: the company, its stock, its products or its outlook?",
        "criteria": {
            "true": "`post` discusses `instrument` itself.",
            "false": "`post` names `instrument` only in passing or in a list of tickers, is spam or promotion, or is about a different company.",
        },
    },
    "stance": {
        "type": "choice",
        "instructions": "What does the author of `post` expect for the stock price of `instrument`?",
        "criteria": {
            "bullish": "The author expects `instrument`'s stock to rise, or is buying or holding it long.",
            "bearish": "The author expects `instrument`'s stock to fall, or is selling or shorting it.",
            "neutral": "The author gives no view of their own on `instrument`'s stock: a question, news without opinion, or someone else's view quoted.",
        },
    },
}


class TypeSafeError(Exception):
    """A Jev request did not produce usable answers."""


def _url() -> str:
    base = (os.environ.get("TYPESAFE_BASE_URL") or _DEFAULT_BASE_URL).rstrip("/")
    try:
        parsed = urlsplit(base)
        host = parsed.hostname
        _ = parsed.port  # validate a supplied port before sending credentials
    except ValueError:
        raise TypeSafeError("TYPESAFE_BASE_URL must use HTTPS or loopback HTTP") from None
    try:
        loopback = host is not None and ip_address(host).is_loopback
    except (ValueError, TypeError):
        loopback = False
    if (
        not host or parsed.username or parsed.password or parsed.query or parsed.fragment
        or not (parsed.scheme == "https" or parsed.scheme == "http" and loopback)
    ):
        raise TypeSafeError("TYPESAFE_BASE_URL must use HTTPS or loopback HTTP")
    return base if base.endswith("/v1/systemone") else base + "/v1/systemone"


def _retry_after(response) -> float | None:
    try:
        return min(max(0.0, float(response.headers.get("retry-after"))), _MAX_WAIT)
    except (TypeError, ValueError):
        return None


def _answers(response, questions: dict) -> dict[str, dict]:
    try:
        payload = response.json()
        answers = payload["answers"]
        if all(answers[name]["type"] == spec["type"] for name, spec in questions.items()):
            return answers
    except (ValueError, KeyError, TypeError):
        pass
    raise TypeSafeError("malformed response")


def system_one(state, questions: dict, *, attempts: int = _ATTEMPTS,
               timeout: float = _TIMEOUT) -> dict[str, dict]:
    """Ask typed questions with bounded retry and credential-safe errors."""
    key = os.environ.get("TYPESAFE_API_KEY")
    if not key:
        raise TypeSafeError("TYPESAFE_API_KEY not set")
    endpoint = _url()
    body = {
        "state": state,
        "model": os.environ.get("TYPESAFE_DEFAULT_MODEL") or _DEFAULT_MODEL,
        "questions": questions,
    }
    headers = {"Authorization": f"Bearer {key}"}
    backoff, retry_after = 1.0, None
    failure = "request failed"
    for attempt in range(attempts):
        if attempt:
            time.sleep(retry_after if retry_after is not None else backoff * random.uniform(0.8, 1.2))
            backoff *= 2
        try:
            response = requests.post(endpoint, json=body, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            failure, retry_after = type(exc).__name__, None
            if isinstance(exc, (requests.ConnectionError, requests.Timeout,
                                requests.exceptions.ChunkedEncodingError)):
                continue
            break
        if response.status_code == 200:
            return _answers(response, questions)
        failure, retry_after = f"HTTP {response.status_code}", _retry_after(response)
        if response.status_code not in _RETRY_STATUSES:
            break
    raise TypeSafeError(failure)


def _stance(answer: dict) -> str:
    choice = answer["choice"]
    if choice not in QUESTIONS["stance"]["criteria"] or answer["confidence"] < _STANCE_CONFIDENCE:
        return "unclear"
    return choice


def _score(value) -> float:
    if (
        isinstance(value, bool) or not isinstance(value, (int, float))
        or not math.isfinite(value) or not 0 <= value <= 1
    ):
        raise TypeSafeError("malformed response")
    return float(value)


def _unscreened(instrument: str, posts: list[str], failure: str) -> tuple[list[bool], str]:
    logger.warning("Jev screening failed for %s: %s", instrument, failure)
    return [True] * len(posts), f"<Jev screening unavailable ({failure}); posts are unscreened>"


def _ask_each(instrument: str, posts: list[str]) -> list[dict]:
    answers: list[dict | None] = [None] * len(posts)
    pool = ThreadPoolExecutor(max_workers=_WORKERS)
    try:
        futures = {
            pool.submit(system_one, {"instrument": instrument, "post": post}, QUESTIONS): index
            for index, post in enumerate(posts)
        }
        for future in as_completed(futures):
            answers[futures[future]] = future.result()
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return answers


def jev_screen(ticker: str, as_of_date: str | None = None):
    """Return a post screen when a key is configured for a live analysis."""
    if is_historical(as_of_date):
        return None
    if not os.environ.get("TYPESAFE_API_KEY"):
        return None
    name = resolve_instrument_identity(ticker).get("company_name")
    instrument = f"{name} ({ticker})" if name else ticker

    def screen(posts: list[str]) -> tuple[list[bool], str]:
        try:
            answers = _ask_each(instrument, posts)
            relevance = [_score(answer["about"]["noul"]) for answer in answers]
            for answer in answers:
                _score(answer["stance"]["confidence"])
            keep = [score >= _OFF_TOPIC_BELOW for score in relevance]
            stances = [
                _stance(answer["stance"])
                for answer, kept in zip(answers, keep, strict=True) if kept
            ]
        except (KeyError, TypeError):
            return _unscreened(instrument, posts, "malformed response")
        except TypeSafeError as exc:
            return _unscreened(instrument, posts, str(exc))
        counts = ", ".join(
            f"{stances.count(stance)} {stance}"
            for stance in ("bullish", "bearish", "neutral", "unclear")
        )
        return keep, (
            f"Screened by Jev: {len(stances)} of the {len(posts)} posts fetched are about "
            f"{instrument}; their stance on its stock: {counts}."
        )

    return screen
