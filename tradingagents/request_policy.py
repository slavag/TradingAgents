"""Finite defaults for OpenAI-compatible network requests."""

import math

DEFAULT_REQUEST_TIMEOUT_SECONDS = 300.0
DEFAULT_REQUEST_RETRIES = 1


def request_timeout_seconds(value=None) -> float:
    if value is None:
        return DEFAULT_REQUEST_TIMEOUT_SECONDS
    if isinstance(value, bool):
        raise ValueError("request timeout must be a positive finite number of seconds")
    try:
        seconds = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("request timeout must be a positive finite number of seconds") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("request timeout must be a positive finite number of seconds")
    return seconds
