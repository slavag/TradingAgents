"""Safe, UTC-stamped filenames for user-facing reports."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from datetime import datetime, timezone

from tradingagents.dataflows.utils import safe_ticker_component

_STAMP = re.compile(r"\d{8}T\d{6}Z\Z")
_MAX_STEM = 190


def utc_report_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def report_stem(kind: str, tickers: Iterable[str], timestamp: str | None = None) -> str:
    """Build a bounded filename stem without accepting path-shaped symbols."""
    if kind not in {"full", "investor", "consolidated"}:
        raise ValueError("unknown report type")
    stamp = timestamp or utc_report_timestamp()
    if not _STAMP.fullmatch(stamp):
        raise ValueError("report timestamp must be UTC YYYYMMDDTHHMMSSZ")
    datetime.strptime(stamp, "%Y%m%dT%H%M%SZ")

    symbols = []
    for raw in tickers:
        symbol = safe_ticker_component(str(raw).strip().upper())
        if symbol not in symbols:
            symbols.append(symbol)
    if kind != "consolidated" and len(symbols) != 1:
        raise ValueError("a per-ticker report requires exactly one ticker")
    if not symbols:
        symbols = ["batch"]

    if kind != "consolidated":
        return f"{symbols[0]}_{kind}_{stamp}"

    prefix = "consolidated_"
    full = f"{prefix}{'_'.join(symbols)}_{stamp}"
    if len(full) <= _MAX_STEM:
        return full

    digest = hashlib.sha256("|".join(symbols).encode()).hexdigest()[:8]
    for count in range(len(symbols) - 1, 0, -1):
        omitted = len(symbols) - count
        stem = f"{prefix}{'_'.join(symbols[:count])}_plus{omitted}_{digest}_{stamp}"
        if len(stem) <= _MAX_STEM:
            return stem
    return f"{prefix}{symbols[0]}_plus{len(symbols) - 1}_{digest}_{stamp}"
