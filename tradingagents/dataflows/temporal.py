from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Iterator


class AnalysisMode(str, Enum):
    LIVE = "live"
    HISTORICAL = "historical"


@dataclass(frozen=True)
class AnalysisContext:
    as_of_date: date
    mode: AnalysisMode


_CURRENT_CONTEXT: ContextVar[AnalysisContext | None] = ContextVar(
    "tradingagents_analysis_context", default=None
)


def build_analysis_context(
    as_of_date: str | date, today: date | None = None
) -> AnalysisContext:
    parsed = as_of_date if isinstance(as_of_date, date) else date.fromisoformat(str(as_of_date))
    current = today or date.today()
    if parsed > current:
        raise ValueError(f"analysis date {parsed.isoformat()} cannot be in the future")
    mode = AnalysisMode.HISTORICAL if parsed < current else AnalysisMode.LIVE
    return AnalysisContext(parsed, mode)


@contextmanager
def use_analysis_context(
    as_of_date: str | date, today: date | None = None
) -> Iterator[AnalysisContext]:
    context = build_analysis_context(as_of_date, today=today)
    token = _CURRENT_CONTEXT.set(context)
    try:
        yield context
    finally:
        _CURRENT_CONTEXT.reset(token)


def get_analysis_context() -> AnalysisContext | None:
    return _CURRENT_CONTEXT.get()


def is_historical(as_of_date: str | date | None = None) -> bool:
    context = build_analysis_context(as_of_date) if as_of_date is not None else get_analysis_context()
    return bool(context and context.mode is AnalysisMode.HISTORICAL)


def historical_unavailable(
    source: str, as_of_date: str | date | None = None
) -> str | None:
    context = build_analysis_context(as_of_date) if as_of_date is not None else get_analysis_context()
    if context is None or context.mode is AnalysisMode.LIVE:
        return None
    cutoff = context.as_of_date.isoformat()
    return (
        f"DATA_UNAVAILABLE: {source} has no point-in-time archive for historical "
        f"analysis as of {cutoff}. Proceed without it; do not treat missing data "
        "as neutral and do not fabricate values."
    )
