"""Saved investor HTML is served only for a matching local web job."""

from fastapi.testclient import TestClient

from tradingagents.web import service as web_service
from tradingagents.web.app import app


def test_investor_report_endpoint_is_job_scoped_and_local(tmp_path, monkeypatch):
    job_id = "a" * 32
    report_dir = tmp_path / "AAPL" / "2026-10-08" / "runs" / job_id / "web_report"
    report_dir.mkdir(parents=True)
    report = report_dir / "AAPL_investor_20261008T132500Z.html"
    report.write_text("<!doctype html><title>AAPL investor brief</title>", encoding="utf-8")
    monkeypatch.setattr(web_service, "RESULTS_ROOT", tmp_path)
    with web_service._JOB_LOCK:
        web_service._JOBS[job_id] = {
            "id": job_id, "tickers": ["AAPL"], "analysis_date": "2026-10-08",
            "report_timestamp": "20261008T132500Z",
            "results": [{"ticker": "AAPL", "status": "completed"}],
        }
    try:
        local = TestClient(
            app, base_url="http://127.0.0.1:8000", client=("127.0.0.1", 50000)
        )
        response = local.get(f"/api/jobs/{job_id}/reports/AAPL/investor")
        assert response.status_code == 200
        assert "AAPL investor brief" in response.text
        assert response.headers["content-security-policy"].startswith("default-src 'none'")
        assert response.headers["cache-control"] == "no-store"
        assert "AAPL_investor_20261008T132500Z.html" in response.headers["content-disposition"]
        assert local.get(f"/api/jobs/{job_id}/reports/MSFT/investor").status_code == 404

        remote = TestClient(
            app, base_url="http://127.0.0.1:8000", client=("10.0.0.8", 50000)
        )
        assert remote.get(f"/api/jobs/{job_id}/reports/AAPL/investor").status_code == 403
    finally:
        with web_service._JOB_LOCK:
            web_service._JOBS.pop(job_id, None)


def test_full_report_endpoint_is_job_scoped_and_local(tmp_path, monkeypatch):
    job_id = "b" * 32
    report_dir = tmp_path / "PENG" / "2026-10-09" / "runs" / job_id / "web_report"
    report_dir.mkdir(parents=True)
    report = report_dir / "PENG_full_20261009T154737Z.html"
    report.write_text("<!doctype html><title>PENG full</title>", encoding="utf-8")
    monkeypatch.setattr(web_service, "RESULTS_ROOT", tmp_path)
    with web_service._JOB_LOCK:
        web_service._JOBS[job_id] = {
            "id": job_id, "tickers": ["PENG"], "analysis_date": "2026-10-09",
            "report_timestamp": "20261009T154737Z",
            "results": [{"ticker": "PENG", "status": "completed"}],
        }
    try:
        local = TestClient(
            app, base_url="http://127.0.0.1:8000", client=("127.0.0.1", 50000)
        )
        remote = TestClient(
            app, base_url="http://127.0.0.1:8000", client=("10.0.0.8", 50000)
        )
        response = local.get(f"/api/jobs/{job_id}/reports/PENG/full")
        assert response.status_code == 200
        assert "PENG full" in response.text
        assert response.headers["cache-control"] == "no-store"
        assert local.get(f"/api/jobs/{job_id}/reports/AAPL/full").status_code == 404
        assert remote.get(f"/api/jobs/{job_id}/reports/PENG/full").status_code == 403
    finally:
        with web_service._JOB_LOCK:
            web_service._JOBS.pop(job_id, None)
