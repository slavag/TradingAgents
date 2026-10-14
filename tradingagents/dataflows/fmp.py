"""Optional Financial Modeling Prep earnings evidence.

FMP's analyst estimates and transcript APIs expose current snapshots, but they
do not expose the point-in-time revision history needed for historical analysis.
This module therefore fails closed for historical dates before making a request.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from urllib.parse import urlencode

import requests

from tradingagents.settings import get_data_source_setting

from .temporal import historical_unavailable

FMP_API_BASE = "https://financialmodelingprep.com/stable"
FMP_DOCS_URL = "https://site.financialmodelingprep.com/developer/docs/stable"
REQUEST_TIMEOUT = 15
MAX_ROWS = 4
MAX_TRANSCRIPT_CHARS = 8_000

_SYMBOL_RE = re.compile(r"^[A-Z0-9.^_-]{1,32}$")


class FMPRequestError(RuntimeError):
    """A credential-safe error raised by the FMP HTTP boundary."""


def _public_url(path: str, params: dict | None = None) -> str:
    url = f"{FMP_API_BASE}/{path}"
    return f"{url}?{urlencode(params)}" if params else url


def _record(
    *,
    title: str,
    content: str,
    url: str,
    as_of_date: str,
    status: str,
    published_at: str | None = None,
) -> dict:
    return {
        "source": "fmp",
        "category": "earnings",
        "title": title,
        "content": content,
        "url": url,
        "published_at": published_at,
        "as_of_date": as_of_date,
        "status": status,
    }


def _unavailable(title: str, reason: str, url: str, as_of_date: str) -> dict:
    return _record(
        title=title,
        content=f"DATA_UNAVAILABLE: {reason}",
        url=url,
        as_of_date=as_of_date,
        status="unavailable",
    )


def _request_list(
    path: str,
    params: dict,
    api_key: str,
    endpoint_name: str,
) -> list:
    """Fetch one FMP list endpoint without exposing credentials in its URL/errors."""
    try:
        response = requests.get(
            f"{FMP_API_BASE}/{path}",
            params={**params, "apikey": api_key},
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException:
        raise FMPRequestError(f"FMP {endpoint_name} network request failed; retry later.") from None

    status_code = getattr(response, "status_code", 0)
    if status_code != 200:
        if status_code in {401, 403}:
            guidance = "verify FMP_API_KEY and plan entitlement"
        elif status_code == 402:
            guidance = "verify FMP plan entitlement for this endpoint"
        elif status_code == 429:
            guidance = "the FMP rate limit was reached; retry later"
        else:
            guidance = "verify endpoint availability and retry later"
        raise FMPRequestError(f"FMP {endpoint_name} HTTP {status_code}; {guidance}.")

    try:
        payload = response.json()
    except (TypeError, ValueError):
        raise FMPRequestError(f"FMP {endpoint_name} returned a malformed JSON payload.") from None
    if not isinstance(payload, list):
        raise FMPRequestError(f"FMP {endpoint_name} returned a malformed JSON payload.")
    return payload


def _parse_date(value) -> date | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _normalize_iso(value: str) -> str:
    text = value.strip()
    try:
        if len(text) == 10:
            return date.fromisoformat(text).isoformat()
        return datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except ValueError:
        parsed = _parse_date(text)
        return parsed.isoformat() if parsed else text


def _format_value(value) -> str:
    return "not supplied" if value is None else str(value)


def _matching_rows(payload: list, ticker: str) -> list[dict]:
    return [
        row
        for row in payload
        if isinstance(row, dict) and str(row.get("symbol", ticker)).strip().upper() == ticker
    ]


def _collect_estimates(ticker: str, as_of_date: str, api_key: str) -> dict:
    params = {
        "symbol": ticker,
        "period": "annual",
        "page": 0,
        "limit": MAX_ROWS,
    }
    url = _public_url("analyst-estimates", params)
    try:
        payload = _request_list("analyst-estimates", params, api_key, "analyst estimates")
    except FMPRequestError as exc:
        return _unavailable(f"{ticker} analyst estimates", str(exc), url, as_of_date)

    rows = [
        row for row in _matching_rows(payload, ticker) if _parse_date(row.get("date")) is not None
    ][:MAX_ROWS]
    if not rows:
        return _unavailable(
            f"{ticker} analyst estimates",
            "No matching analyst-estimate rows were returned by FMP.",
            url,
            as_of_date,
        )

    blocks = []
    for row in rows:
        blocks.append(
            "\n".join(
                [
                    f"Fiscal period end: {row['date']} (annual)",
                    "EPS consensus (currency/share): "
                    f"{_format_value(row.get('epsAvg'))} "
                    f"[low {_format_value(row.get('epsLow'))}; "
                    f"high {_format_value(row.get('epsHigh'))}; "
                    f"analysts {_format_value(row.get('numAnalystsEps'))}]",
                    "Revenue consensus (currency units): "
                    f"{_format_value(row.get('revenueAvg'))} "
                    f"[low {_format_value(row.get('revenueLow'))}; "
                    f"high {_format_value(row.get('revenueHigh'))}; "
                    f"analysts {_format_value(row.get('numAnalystsRevenue'))}]",
                ]
            )
        )

    content = (
        f"FMP analyst consensus for {ticker}. This is a current snapshot as of "
        f"retrieval for analysis date {as_of_date}; FMP does not provide a "
        "point-in-time snapshot timestamp in these rows. Currency is not supplied "
        "by this endpoint, so monetary values are labeled as currency units.\n\n"
        + "\n\n".join(blocks)
        + "\n\nSource facts: FMP stable analyst-estimates endpoint; annual period; "
        f"at most {MAX_ROWS} rows. Fiscal dates are forecast periods, not "
        "publication dates."
    )
    return _record(
        title=f"{ticker} annual analyst earnings and revenue estimates",
        content=content,
        url=url,
        as_of_date=as_of_date,
        status="available",
    )


def _has_actual(row: dict) -> bool:
    return row.get("epsActual") is not None or row.get("revenueActual") is not None


def _collect_earnings(ticker: str, as_of_date: str, api_key: str) -> dict:
    params = {"symbol": ticker, "limit": MAX_ROWS}
    url = _public_url("earnings", params)
    try:
        payload = _request_list("earnings", params, api_key, "earnings reports")
    except FMPRequestError as exc:
        return _unavailable(f"{ticker} earnings reports", str(exc), url, as_of_date)

    cutoff = date.fromisoformat(as_of_date)
    rows = []
    for row in _matching_rows(payload, ticker):
        report_date = _parse_date(row.get("date"))
        if report_date is None or report_date > cutoff or not _has_actual(row):
            continue
        rows.append((report_date, row))
    rows.sort(key=lambda item: item[0], reverse=True)
    rows = rows[:MAX_ROWS]
    if not rows:
        return _unavailable(
            f"{ticker} earnings reports",
            "No matching completed earnings reports on or before the analysis date "
            "were returned by FMP.",
            url,
            as_of_date,
        )

    blocks = []
    for _, row in rows:
        blocks.append(
            "\n".join(
                [
                    f"FMP report date: {row['date']}",
                    f"Fiscal period end: {_format_value(row.get('fiscalDateEnding'))}",
                    f"EPS actual (currency/share): {_format_value(row.get('epsActual'))}",
                    f"EPS estimate (currency/share): {_format_value(row.get('epsEstimated'))}",
                    f"Revenue actual (currency units): {_format_value(row.get('revenueActual'))}",
                    "Revenue estimate (currency units): "
                    f"{_format_value(row.get('revenueEstimated'))}",
                ]
            )
        )

    content = (
        f"FMP actual-versus-estimate earnings reports for {ticker}. Currency is not "
        "supplied by this endpoint, so monetary values are labeled as currency "
        "units.\n\n"
        + "\n\n".join(blocks)
        + "\n\nSource facts: FMP stable earnings endpoint; completed reports only; "
        "future calendar events are excluded. FMP's report date is retained in "
        "the content and is not represented as a publication timestamp."
    )
    return _record(
        title=f"{ticker} recent actual versus estimated earnings",
        content=content,
        url=url,
        as_of_date=as_of_date,
        status="available",
    )


def _quarter_number(value) -> int | None:
    text = str(value).strip().upper()
    if text.startswith("Q"):
        text = text[1:]
    try:
        quarter = int(text)
    except ValueError:
        return None
    return quarter if quarter in {1, 2, 3, 4} else None


def _collect_transcript(ticker: str, as_of_date: str, api_key: str) -> dict:
    date_params = {"symbol": ticker}
    dates_url = _public_url("earning-call-transcript-dates", date_params)
    try:
        payload = _request_list(
            "earning-call-transcript-dates",
            date_params,
            api_key,
            "transcript date discovery",
        )
    except FMPRequestError as exc:
        return _unavailable(f"{ticker} earnings call transcript", str(exc), dates_url, as_of_date)

    cutoff = date.fromisoformat(as_of_date)
    eligible = []
    for row in _matching_rows(payload, ticker):
        call_date = _parse_date(row.get("date"))
        quarter = _quarter_number(row.get("quarter"))
        year = row.get("fiscalYear", row.get("year"))
        try:
            year = int(year)
        except (TypeError, ValueError):
            continue
        if call_date is None or call_date > cutoff or quarter is None:
            continue
        eligible.append((call_date, year, quarter, row))

    if not eligible:
        return _unavailable(
            f"{ticker} earnings call transcript",
            "No eligible transcript date on or before the analysis date was returned by FMP.",
            dates_url,
            as_of_date,
        )

    eligible.sort(key=lambda item: item[0], reverse=True)
    _, year, quarter, date_row = eligible[0]
    transcript_params = {
        "symbol": ticker,
        "year": year,
        "quarter": quarter,
        "limit": 1,
    }
    transcript_url = _public_url("earning-call-transcript", transcript_params)
    try:
        transcript_payload = _request_list(
            "earning-call-transcript",
            transcript_params,
            api_key,
            "earnings transcript",
        )
    except FMPRequestError as exc:
        return _unavailable(
            f"{ticker} earnings call transcript", str(exc), transcript_url, as_of_date
        )

    transcript_rows = [
        row
        for row in _matching_rows(transcript_payload, ticker)
        if isinstance(row.get("content"), str) and row["content"].strip()
    ]
    if not transcript_rows:
        return _unavailable(
            f"{ticker} earnings call transcript",
            "FMP returned no matching transcript content for the discovered fiscal period.",
            transcript_url,
            as_of_date,
        )

    row = transcript_rows[0]
    full_text = row["content"].strip()
    excerpt = full_text[:MAX_TRANSCRIPT_CHARS]
    if len(full_text) > MAX_TRANSCRIPT_CHARS:
        excerpt_label = (
            f"Transcript excerpt (truncated to {MAX_TRANSCRIPT_CHARS} characters "
            f"from {len(full_text)}):"
        )
    else:
        excerpt_label = f"Transcript excerpt (complete, {len(full_text)} characters):"
    call_date = row.get("date", date_row.get("date", "not supplied"))
    content = (
        f"FMP earnings call transcript for {ticker}.\n"
        f"Fiscal period: FY{year} Q{quarter}.\n"
        f"Call date: {call_date}. This is the call/event date; a separate transcript "
        "publication timestamp is not supplied by FMP, so published_at is null.\n\n"
        f"{excerpt_label}\n{excerpt}\n\n"
        "Source facts: FMP stable earning-call-transcript endpoint, selected through "
        "the transcript-date discovery endpoint. Retrieval time is added by the "
        "parent evidence collector."
    )
    return _record(
        title=f"{ticker} earnings call transcript FY{year} Q{quarter}",
        content=content,
        url=transcript_url,
        as_of_date=as_of_date,
        status="available",
    )


def get_earnings_context(ticker: str, curr_date: str) -> list[dict]:
    """Return current FMP earnings evidence records for ``ticker``.

    Missing credentials and unsupported historical dates are represented as
    unavailable evidence so the caller can preserve source completeness without
    failing an analysis run.
    """
    symbol = str(ticker).strip().upper()
    if not _SYMBOL_RE.fullmatch(symbol):
        raise ValueError(f"invalid FMP ticker symbol: {ticker!r}")

    # This also validates curr_date and rejects future analysis dates. It must run
    # before credential lookup or any live endpoint access.
    historical_reason = historical_unavailable("FMP earnings evidence", curr_date)
    if historical_reason:
        return [
            _record(
                title=f"{symbol} FMP earnings evidence",
                content=historical_reason,
                url=FMP_DOCS_URL,
                as_of_date=curr_date,
                status="unavailable",
            )
        ]

    api_key = get_data_source_setting("FMP_API_KEY")
    if not api_key:
        return [
            _unavailable(
                f"{symbol} FMP earnings evidence",
                "FMP_API_KEY is not configured; set it to enable optional FMP earnings evidence.",
                FMP_DOCS_URL,
                curr_date,
            )
        ]

    return [
        _collect_estimates(symbol, curr_date, api_key),
        _collect_earnings(symbol, curr_date, api_key),
        _collect_transcript(symbol, curr_date, api_key),
    ]
