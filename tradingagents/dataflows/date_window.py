"""Canonical, run-bounded dates for model-callable data tools."""

from __future__ import annotations

import re
from datetime import date, timedelta

_DATE = re.compile(r"[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}\Z")


def _parse(value: str | None) -> date | None:
    if not isinstance(value, str) or not _DATE.fullmatch(value):
        return None
    try:
        year, month, day = (int(part) for part in value.split("-"))
        return date(year, month, day)
    except ValueError:
        return None


def as_of(requested: str | None, trade_date: str) -> str | None:
    """Return a canonical requested date, never after the run's trade date."""
    parsed = _parse(requested)
    if requested is not None and parsed is None:
        raise ValueError("curr_date is not a date; give it as YYYY-MM-DD")
    if not trade_date:
        return parsed.isoformat() if parsed is not None else None
    cutoff = _parse(trade_date)
    if cutoff is None:
        raise ValueError("trade_date must be YYYY-MM-DD")
    return parsed.isoformat() if parsed is not None and parsed <= cutoff else cutoff.isoformat()


def as_of_window(start_date: str, end_date: str, trade_date: str) -> tuple[str, str]:
    """Canonicalize a model's window and keep its end no later than the run."""
    start, requested_end = _parse(start_date), _parse(end_date)
    if start is None:
        raise ValueError(f"start_date {start_date!r} is not a date; give it as YYYY-MM-DD")
    if end_date is not None and requested_end is None:
        raise ValueError("end_date is not a date; give it as YYYY-MM-DD")
    if requested_end is not None and start > requested_end:
        raise ValueError(
            f"start_date {start_date} is after end_date {end_date}; give the earlier date first"
        )
    end = _parse(as_of(end_date, trade_date))
    if end is None:
        raise ValueError(f"end_date {end_date!r} is not a date; give it as YYYY-MM-DD")
    if start <= end:
        return start.isoformat(), end.isoformat()
    span = requested_end - start if requested_end is not None else timedelta(0)
    return (end - span).isoformat(), end.isoformat()
