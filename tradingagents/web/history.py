"""Local run metadata and read-only discovery of existing report trees."""

from __future__ import annotations

import copy
import json
import logging
import os
import re
import tempfile
from datetime import date
from pathlib import Path
from urllib.parse import quote

logger = logging.getLogger(__name__)
_ID = re.compile(r"[a-f0-9]{32}")
_SYMBOL = re.compile(r"[A-Za-z0-9^][A-Za-z0-9._^=+\-]{0,63}")
_STAMP = re.compile(r"_(\d{8}T\d{6}Z)\.(?:html|md)$")
_PRIVATE = {"traceback", "consolidated_html", "consolidated_markdown"}


def valid_job_id(value: object) -> bool:
    return isinstance(value, str) and _ID.fullmatch(value) is not None


def _valid_date(value: object) -> bool:
    try:
        return isinstance(value, str) and date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def _safe_file(path: Path, root: Path) -> bool:
    return path.resolve().is_relative_to(root.resolve()) and path.is_file()


def _json(path: Path, root: Path) -> dict:
    try:
        if not _safe_file(path, root) or path.stat().st_size > 8_000_000:
            return {}
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError, UnicodeError):
        logger.debug("Skipping unreadable saved metadata")
        return {}


def _valid_job(value: dict) -> bool:
    tickers = value.get("tickers")
    return (
        valid_job_id(value.get("id"))
        and _valid_date(value.get("analysis_date"))
        and isinstance(tickers, list)
        and bool(tickers)
        and all(isinstance(t, str) and _SYMBOL.fullmatch(t) for t in tickers)
        and isinstance(value.get("results", []), list)
        and all(
            isinstance(r, dict) and r.get("ticker") in tickers for r in value.get("results", [])
        )
        and isinstance(value.get("created_at", ""), str)
        and isinstance(value.get("status", ""), str)
    )


def save_job(root: Path, job: dict) -> None:
    """Atomically save a snapshot without duplicating report bodies or raw errors."""
    if not _valid_job(job):
        raise ValueError("Invalid saved run metadata")
    value = {k: v for k, v in job.items() if k not in _PRIVATE}
    folder = root / ".web_jobs"
    folder.mkdir(parents=True, exist_ok=True)
    if not folder.resolve().is_relative_to(root.resolve()):
        raise ValueError("Saved run folder is outside the results root")
    fd, temporary = tempfile.mkstemp(prefix=".run-", suffix=".tmp", dir=folder)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, folder / f"{job['id']}.json")
    finally:
        Path(temporary).unlink(missing_ok=True)


def report_path(
    root: Path, job: dict, ticker: str | None, kind: str, extension: str = "html"
) -> Path | None:
    """Resolve only recognized artifacts inside the exact selected run."""
    if not valid_job_id(job.get("id")) or not _valid_date(job.get("analysis_date")):
        return None
    if extension not in {"html", "md"}:
        return None
    if kind == "consolidated":
        folder = root / "batch_web" / job["analysis_date"] / job["id"]
        pattern = f"consolidated_*.{extension}"
    elif kind in {"investor", "full"} and ticker in job.get("tickers", []):
        if not isinstance(ticker, str) or not _SYMBOL.fullmatch(ticker):
            return None
        if not any(
            r.get("ticker") == ticker and r.get("status") == "completed"
            for r in job.get("results", [])
            if isinstance(r, dict)
        ):
            return None
        folder = root / ticker / job["analysis_date"] / "runs" / job["id"] / "web_report"
        pattern = f"{ticker}_{kind}_*.{extension}"
    else:
        return None
    if not folder.resolve().is_relative_to(root.resolve()):
        return None
    candidates = [p for p in folder.glob(pattern) if _STAMP.search(p.name) and _safe_file(p, root)]
    if not candidates and kind == "full" and extension == "html":
        candidates = [
            p
            for p in folder.glob(f"{ticker}_full_*.md")
            if _STAMP.search(p.name) and _safe_file(p, root)
        ]
        legacy = folder / "complete_report.md"
        if not candidates and _safe_file(legacy, root):
            candidates = [legacy]
    if not candidates and kind == "consolidated":
        markdown = [
            p
            for p in folder.glob("consolidated_*.md")
            if (_STAMP.search(p.name) or p.name == "consolidated_report.md") and _safe_file(p, root)
        ]
        if extension in {"html", "md"}:
            candidates = markdown
    return max(candidates, key=lambda p: p.name) if candidates else None


def decorate_job(root: Path, job: dict) -> dict:
    """Attach stable same-origin URLs only for artifacts that actually exist."""
    value = copy.deepcopy(job)
    value.pop("traceback", None)
    base = f"/api/jobs/{quote(str(job['id']), safe='')}"
    for result in value.get("results", []):
        ticker = result.get("ticker")
        result["reports"] = {
            kind: f"{base}/reports/{quote(ticker, safe='')}/{kind}"
            for kind in ("investor", "full")
            if report_path(root, job, ticker, kind) is not None
        }
    value["consolidated_reports"] = {
        ext: f"{base}/reports/consolidated?format={ext}"
        for ext in ("html", "md")
        if report_path(root, job, None, "consolidated", ext) is not None
    }
    return value


def _decision_fields(folder: Path, root: Path) -> dict:
    fields = {}
    names = {
        "Rating": "decision",
        "Executive Summary": "executive_summary",
        "Thesis": "thesis",
        "Existing Position": "existing_position_action",
        "Existing Position Guidance": "existing_position_summary",
        "New Position": "new_position_action",
        "New Position Guidance": "new_position_summary",
        "Conditional Confirmation": "conditional_confirmation",
        "Conditional Alternative": "conditional_alternative",
        "Conditional Invalidation": "conditional_invalidation",
        "Target Rationale": "target_summary",
    }
    for path in (folder / "5_portfolio").glob("*.md"):
        if not _safe_file(path, root) or path.stat().st_size > 2_000_000:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            continue
        for match in re.finditer(r"^\*\*([^*\n]+)\*\*\s*:\s*(.*?)(?=^\*\*|\Z)", text, re.M | re.S):
            if match[1] in names:
                fields[names[match[1]]] = match[2].strip()
    return fields


def _legacy_jobs(root: Path) -> dict[str, dict]:
    jobs: dict[str, dict] = {}
    for path in root.glob("*/????-??-??/runs/*/web_report/run_manifest.json"):
        folder = path.parent
        job_id, day, ticker = folder.parent.name, folder.parents[2].name, folder.parents[3].name
        meta = _json(path, root)
        if (
            not meta
            or not valid_job_id(job_id)
            or not _valid_date(day)
            or not _SYMBOL.fullmatch(ticker)
            or meta.get("ticker") != ticker
            or meta.get("run_id") != job_id
            or meta.get("analysis_date") != day
        ):
            continue
        generated = meta.get("generated_at") if isinstance(meta.get("generated_at"), str) else day
        job = jobs.setdefault(
            job_id,
            {
                "id": job_id,
                "analysis_date": day,
                "created_at": generated,
                "updated_at": generated,
                "status": "completed",
                "tickers": [],
                "results": [],
                "completed": 0,
                "total": 0,
                "progress_message": "Imported saved research.",
                "imported": True,
            },
        )
        if job["analysis_date"] != day or ticker in job["tickers"]:
            continue
        job["tickers"].append(ticker)
        forecast = _json(folder / "forecast_record.json", root)
        accepted = str(forecast.get("target_validation_status", "")).lower() == "accepted"
        low, high = forecast.get("target_low"), forecast.get("target_high")
        point = forecast.get("central_target")
        label = (
            f"{low:g}–{high:g}"
            if isinstance(low, (int, float)) and isinstance(high, (int, float))
            else f"{point:g}"
            if isinstance(point, (int, float))
            else "—"
        )
        result = {
            "ticker": ticker,
            "analysis_date": day,
            "status": "completed",
            "run_id": job_id,
            "decision": forecast.get("rating"),
            "results_dir": str(folder),
            "price_target_label": label if accepted else "—",
            "target_horizon": forecast.get("horizon_text") if accepted else None,
            "snapshot_mode": meta.get("snapshot_mode"),
            "evidence_fingerprint": meta.get("evidence_fingerprint"),
            "comparison_status": meta.get("comparison_status"),
            "recommendation_confidence_score": forecast.get("recommendation_confidence_score"),
            **_decision_fields(folder, root),
        }
        job["results"].append(result)
        job["completed"] = job["total"] = len(job["results"])
        reports = list(folder.glob(f"{ticker}_full_*.html"))
        if reports and (match := _STAMP.search(max(reports).name)):
            job["report_timestamp"] = match[1]
    _merge_batch_summaries(root, jobs)
    return jobs


def _batch_rows(path: Path, root: Path) -> list[dict]:
    """Recover batch membership, including failures without a ticker manifest."""
    if not _safe_file(path, root) or path.stat().st_size > 8_000_000:
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return []
    headers, rows = {}, []
    for line in lines:
        if not line.startswith("|"):
            if rows:
                break
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if "Ticker" in cells and "Status" in cells:
            headers = {name: index for index, name in enumerate(cells)}
            continue
        if not headers or len(cells) <= max(headers.values()):
            continue
        ticker = cells[headers["Ticker"]]
        status = cells[headers["Status"]].casefold()
        if not _SYMBOL.fullmatch(ticker) or status not in {"completed", "failed"}:
            continue
        rows.append(
            {
                "ticker": ticker,
                "status": status,
                "decision": cells[headers["Decision"]] if "Decision" in headers else None,
            }
        )
    return rows


def _merge_batch_summaries(root: Path, jobs: dict[str, dict]) -> None:
    for path in sorted(root.glob("batch_web/????-??-??/*/consolidated_*.md")):
        job_id, day = path.parent.name, path.parent.parent.name
        if not valid_job_id(job_id) or not _valid_date(day):
            continue
        records = _batch_rows(path, root)
        if not records:
            continue
        stamp = _STAMP.search(path.name)
        job = jobs.setdefault(
            job_id,
            {
                "id": job_id,
                "analysis_date": day,
                "created_at": day,
                "updated_at": day,
                "tickers": [],
                "results": [],
                "imported": True,
                "progress_message": "Imported saved batch.",
            },
        )
        if job["analysis_date"] != day:
            continue
        by_ticker = {result["ticker"]: result for result in job["results"]}
        for record in records:
            if record["ticker"] not in by_ticker:
                by_ticker[record["ticker"]] = {
                    **record,
                    "analysis_date": day,
                    "run_id": job_id,
                    "error": "This ticker failed in the saved batch. Open the consolidated report for details."
                    if record["status"] == "failed"
                    else None,
                    "results_dir": str(
                        root / record["ticker"] / day / "runs" / job_id / "web_report"
                    ),
                }
        job["tickers"] = list(dict.fromkeys([r["ticker"] for r in records] + job["tickers"]))
        job["results"] = [by_ticker[ticker] for ticker in job["tickers"]]
        failures = sum(r.get("status") == "failed" for r in job["results"])
        job["status"] = (
            "failed" if failures == len(job["results"]) else "partial" if failures else "completed"
        )
        job["completed"] = job["total"] = len(job["tickers"])
        if stamp:
            job["report_timestamp"] = stamp[1]


def saved_jobs(root: Path) -> dict[str, dict]:
    """Merge the durable catalog over legacy files; a failed attempt stays failed."""
    jobs = _legacy_jobs(root)
    for path in (root / ".web_jobs").glob("*.json"):
        value = _json(path, root)
        if not _valid_job(value) or path.stem != value["id"]:
            continue
        if value.get("status") in {"queued", "running"}:
            value["status"] = "interrupted"
            value["current_ticker"] = None
            value["progress_message"] = (
                "This run was interrupted. Saved reports are still available."
            )
            progress = value.get("progress_rows", [])
            if isinstance(progress, list):
                for row in progress:
                    if isinstance(row, dict) and str(row.get("status", "")).lower().replace(
                        " ", "_"
                    ) in {"pending", "queued", "running", "in_progress"}:
                        row["status"] = "interrupted"
        jobs[value["id"]] = value
    return jobs
