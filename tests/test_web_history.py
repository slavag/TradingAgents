"""Research history must outlive workers and keep reports bound to their batch."""

import json

import pytest
from fastapi.testclient import TestClient

from tradingagents.web import service
from tradingagents.web.app import app


@pytest.fixture
def local(tmp_path, monkeypatch):
    monkeypatch.setattr(service, "RESULTS_ROOT", tmp_path)
    with service._JOB_LOCK:
        previous = dict(service._JOBS)
        service._JOBS.clear()
    yield TestClient(app, base_url="http://127.0.0.1:8000", client=("127.0.0.1", 50001))
    with service._JOB_LOCK:
        service._JOBS.clear()
        service._JOBS.update(previous)


def legacy_report(root, job_id="a" * 32, ticker="PENG", stamp="20261012T180336Z"):
    folder = root / ticker / "2026-10-12" / "runs" / job_id / "web_report"
    folder.mkdir(parents=True)
    (folder / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": job_id,
                "ticker": ticker,
                "analysis_date": "2026-10-12",
                "generated_at": "2026-10-12T18:03:36Z",
                "snapshot_mode": "live_current_day",
            }
        )
    )
    (folder / "forecast_record.json").write_text(
        json.dumps(
            {
                "rating": "overweight",
                "target_validation_status": "Accepted",
                "central_target": 80.1,
                "horizon_text": "3–6 months",
            }
        )
    )
    (folder / f"{ticker}_investor_{stamp}.html").write_text("<title>Investor report</title>")
    (folder / f"{ticker}_full_{stamp}.html").write_text("<title>Full research</title>")
    batch = root / "batch_web" / "2026-10-12" / job_id
    batch.mkdir(parents=True, exist_ok=True)
    (batch / f"consolidated_{ticker}_{stamp}.html").write_text("<title>Exact batch</title>")
    (batch / f"consolidated_{ticker}_{stamp}.md").write_text("# Exact batch")
    return folder


def test_history_imports_existing_reports_without_a_live_job(local, tmp_path):
    legacy_report(tmp_path)
    response = local.get("/api/jobs")
    assert response.status_code == 200
    job = response.json()["jobs"][0]
    assert job["tickers"] == ["PENG"]
    assert job["results"][0]["decision"].lower() == "overweight"
    assert job["results"][0]["reports"]["investor"].endswith("/reports/PENG/investor")
    assert local.get(job["consolidated_reports"]["html"]).text == "<title>Exact batch</title>"
    assert local.get(f"/api/jobs/{'a' * 32}/reports/PENG/full").status_code == 200


def test_saved_jobs_recover_as_interrupted_and_preserve_completed_results(local, tmp_path):
    from tradingagents.web.history import save_job

    legacy_report(tmp_path)
    save_job(
        tmp_path,
        {
            "id": "a" * 32,
            "analysis_date": "2026-10-12",
            "status": "running",
            "tickers": ["PENG", "NVDA"],
            "results": [{"ticker": "PENG", "status": "completed"}],
            "created_at": "2026-10-12T18:00:00Z",
            "report_timestamp": "20261012T180336Z",
            "progress_rows": [
                {"agent": "Market analyst", "status": "completed"},
                {"agent": "News analyst", "status": "in_progress"},
            ],
        },
    )
    response = local.get(f"/api/jobs/{'a' * 32}")
    assert response.status_code == 200
    job = response.json()
    assert job["status"] == "interrupted"
    assert job["results"][0]["reports"]["investor"]
    assert job["tickers"] == ["PENG", "NVDA"]
    assert job["progress_rows"][0]["status"] == "completed"
    assert job["progress_rows"][1]["status"] == "interrupted"


def test_history_skips_corrupt_siblings_and_checks_missing_artifacts(local, tmp_path):
    folder = legacy_report(tmp_path)
    bad = tmp_path / "NVDA" / "2026-10-12" / "runs" / ("b" * 32) / "web_report"
    bad.mkdir(parents=True)
    (bad / "run_manifest.json").write_text("broken JSON")
    (folder / "PENG_investor_20261012T180336Z.html").unlink()
    data = local.get("/api/jobs").json()
    assert data["total"] == 1
    assert data["jobs"][0]["results"][0]["reports"].get("investor") is None
    assert local.get(f"/api/jobs/{'a' * 32}/reports/PENG/investor").status_code == 404


def test_history_and_consolidated_reports_remain_local_and_contained(local, tmp_path):
    folder = legacy_report(tmp_path)
    remote = TestClient(app, base_url="http://127.0.0.1:8000", client=("10.0.0.2", 50002))
    assert remote.get("/api/jobs").status_code == 403
    assert remote.get(f"/api/jobs/{'a' * 32}/reports/consolidated").status_code == 403
    assert local.get(f"/api/jobs/{'a' * 32}/reports/NVDA/full").status_code == 404
    outside = tmp_path.parent / "private-report.html"
    outside.write_text("must not be exposed")
    target = folder / "PENG_full_20261012T180336Z.html"
    target.unlink()
    target.symlink_to(outside)
    assert local.get(f"/api/jobs/{'a' * 32}/reports/PENG/full").status_code == 404


def test_history_paginates_and_filters_on_tickers(local, tmp_path):
    legacy_report(tmp_path, ticker="PENG")
    legacy_report(tmp_path, job_id="b" * 32, ticker="NVDA")
    data = local.get("/api/jobs", params={"q": "NVDA", "limit": 1}).json()
    assert data["total"] == 1
    assert data["jobs"][0]["tickers"] == ["NVDA"]
    assert local.get("/api/jobs", params={"limit": 1, "offset": 1}).json()["total"] == 2


def test_job_updates_are_durable_without_persisting_raw_tracebacks(local, tmp_path):
    job_id = "c" * 32
    with service._JOB_LOCK:
        service._JOBS[job_id] = {
            "id": job_id,
            "tickers": ["PENG"],
            "analysis_date": "2026-10-12",
            "created_at": "2026-10-12T18:00:00Z",
            "results": [],
            "status": "running",
        }
    service._update_job(
        job_id, status="failed", error="Analysis failed", traceback="private detail"
    )
    with service._JOB_LOCK:
        service._JOBS.pop(job_id)
    response = local.get(f"/api/jobs/{job_id}")
    assert response.status_code == 200
    assert response.json()["status"] == "failed"
    assert "private detail" not in response.text


def test_retrying_the_same_submission_returns_the_same_job(local, tmp_path, monkeypatch):
    class DormantThread:
        def __init__(self, **kwargs):
            pass

        def start(self):
            pass

    payload = {
        "tickers": "PENG",
        "analysis_date": "2026-10-12",
        "request_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
    }
    with monkeypatch.context() as worker_patch:
        worker_patch.setattr(service.threading, "Thread", DormantThread)
        first = service.create_job(payload)
        second = service.create_job(payload)
    assert first["id"] == second["id"]
    assert local.get("/api/jobs").json()["total"] == 1
    with pytest.raises(ValueError, match="different analysis"):
        service.create_job({**payload, "tickers": "NVDA"})


def test_legacy_batch_keeps_failed_tickers_in_consolidated_scope(local, tmp_path):
    legacy_report(tmp_path)
    batch = tmp_path / "batch_web" / "2026-10-12" / ("a" * 32)
    (batch / "consolidated_PENG_20261012T180336Z.md").write_text(
        "# Consolidated\n\n## Batch Summary\n\n"
        "| Ticker | Decision | Status | Default Results |\n"
        "| --- | --- | --- | --- |\n"
        "| PENG | Overweight | Completed | saved |\n"
        "| NVDA | - | Failed | missing |\n\n---\n"
    )
    job = local.get("/api/jobs").json()["jobs"][0]
    assert job["tickers"] == ["PENG", "NVDA"]
    assert job["status"] == "partial"
    assert job["results"][1]["status"] == "failed"
    assert job["results"][1]["reports"] == {}


def test_older_markdown_research_is_readable_without_regenerating(local, tmp_path):
    folder = legacy_report(tmp_path)
    (folder / "PENG_full_20261012T180336Z.html").unlink()
    (folder / "complete_report.md").write_text(
        "# Original research\n\nKeep this **complete**.\n\n<script>bad()</script>"
    )
    response = local.get(f"/api/jobs/{'a' * 32}/reports/PENG/full")
    assert response.status_code == 200
    assert "Original research" in response.text
    assert "<strong>complete</strong>" in response.text
    assert "<script>bad()" not in response.text


def test_valid_json_with_invalid_metadata_types_does_not_break_history(local, tmp_path):
    legacy_report(tmp_path)
    catalog = tmp_path / ".web_jobs"
    catalog.mkdir()
    (catalog / f"{'d' * 32}.json").write_text(
        json.dumps(
            {
                "id": "d" * 32,
                "tickers": ["NVDA"],
                "analysis_date": "2026-10-12",
                "results": [],
                "created_at": {"corrupt": True},
                "status": "completed",
            }
        )
    )
    response = local.get("/api/jobs")
    assert response.status_code == 200
    assert response.json()["total"] == 1
