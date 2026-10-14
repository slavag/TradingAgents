from __future__ import annotations

import hashlib
import ipaddress
import json
import logging
import logging.config
import os
import threading
import webbrowser
from dataclasses import asdict
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from tradingagents.llm_clients.api_key_env import PROVIDER_API_KEY_ENV
from tradingagents.llm_clients.model_catalog import (
    get_catalog_metadata,
    get_model_capabilities,
    get_role_model_options,
    get_web_model_capabilities,
    get_web_model_options,
)
from tradingagents.llm_clients.model_discovery import DISCOVERY_ADAPTERS, discover_models
from tradingagents.report_html import render_report
from tradingagents.settings import (
    SettingsStorageError,
    SettingsValidationError,
    credential_status,
    update_credentials,
)
from tradingagents.web import service as web_service
from tradingagents.web.history import report_path
from tradingagents.web.service import (
    create_job,
    evaluate_saved_forecasts,
    fetch_market_tickers,
    fetch_speaking_stocks,
    fetch_ticker_detail,
    get_job,
    get_role_model_status,
    optimize_portfolio_payload,
)

logger = logging.getLogger("tradingagents.web")
WEB_BUILD = "research-workspace-v1"

STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _compute_asset_version() -> str:
    digest = hashlib.sha256()
    for filename in ("index.html", "styles.css", "model_picker.js", "app.js", "workspace.js", "research.js"):
        path = STATIC_DIR / filename
        if path.exists():
            digest.update(path.read_bytes())
    return digest.hexdigest()[:12]


ASSET_VERSION = _compute_asset_version()


class NoCacheStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response


def _clear_site_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
        "Clear-Site-Data": '"cache", "storage"',
        "X-TradingAgents-Web-Build": WEB_BUILD,
        "X-TradingAgents-Asset-Version": ASSET_VERSION,
    }


def _normalize_log_level(level: str | None) -> str:
    value = (level or "WARNING").upper()
    allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
    return value if value in allowed else "WARNING"


def configure_logging(log_file: str | None = None, log_level: str | None = None):
    level = _normalize_log_level(log_level or os.getenv("TRADINGAGENTS_WEB_LOG_LEVEL"))
    target = Path(
        log_file
        or os.getenv("TRADINGAGENTS_WEB_LOG_FILE")
        or (PROJECT_ROOT / "logs" / "tradingagents-web.log")
    )
    target.parent.mkdir(parents=True, exist_ok=True)

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                }
            },
            "handlers": {
                "file": {
                    "class": "logging.FileHandler",
                    "filename": str(target),
                    "mode": "a",
                    "formatter": "standard",
                    "level": level,
                    "encoding": "utf-8",
                }
            },
            "root": {
                "handlers": ["file"],
                "level": level,
            },
            "loggers": {
                "tradingagents": {
                    "handlers": ["file"],
                    "level": level,
                    "propagate": False,
                },
                "uvicorn": {
                    "handlers": ["file"],
                    "level": level,
                    "propagate": False,
                },
                "uvicorn.error": {
                    "handlers": ["file"],
                    "level": level,
                    "propagate": False,
                },
                "uvicorn.access": {
                    "handlers": ["file"],
                    "level": level,
                    "propagate": False,
                },
            },
        }
    )
    return str(target), level


app = FastAPI(title="TradingAgents Web", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", NoCacheStaticFiles(directory=STATIC_DIR), name="static")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path == "/" or path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    response.headers["X-TradingAgents-Web-Build"] = WEB_BUILD
    response.headers.setdefault("X-TradingAgents-Asset-Version", ASSET_VERSION)
    if not (path.startswith("/static") or path == "/api/health" or path.startswith("/api/jobs/")):
        logger.info(
            "%s %s -> %s",
            request.method,
            path,
            response.status_code,
        )
    return response


class AnalysisRequest(BaseModel):
    request_id: str | None = Field(default=None, pattern=r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$")
    tickers: list[str] | str
    analysis_date: str
    analysts: list[str] = Field(
        default_factory=lambda: ["market", "social", "news", "fundamentals"]
    )
    research_depth: int = 3
    llm_provider: str = "openai"
    backend_url: str | None = None
    quick_provider: str = "openai"
    quick_thinker: str = "gpt-5.4"
    deep_provider: str = "openai"
    deep_thinker: str = "gpt-5.4"
    final_report_provider: str = "openai"
    final_report_model: str = "gpt-5.4-mini"
    google_thinking_level: str | None = None
    openai_reasoning_effort: str | None = "medium"
    temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    save_reports: bool = True
    export_path: str | None = None


class EvaluationRunRequest(BaseModel):
    tickers: list[str] = Field(default_factory=list)
    date_from: str | None = None
    date_to: str | None = None
    pending_only: bool = False
    report_paths: list[str] = Field(default_factory=list)


def _render_index_response(promotion_root: Path | None = None) -> HTMLResponse:
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        role_defaults = get_role_model_status(promotion_root=promotion_root)
        capabilities = get_web_model_capabilities()
        for entry in role_defaults.values():
            provider, model = entry.get("provider"), entry.get("model")
            if provider and model:
                capabilities.setdefault(provider, {})[model] = get_model_capabilities(provider, model)
        html = (
            index_path.read_text(encoding="utf-8")
            .replace("__ASSET_VERSION__", ASSET_VERSION)
            .replace(
                "__MODEL_OPTIONS_JSON__",
                json.dumps(get_web_model_options()),
            )
            .replace(
                "__ROLE_MODEL_OPTIONS_JSON__",
                json.dumps(get_role_model_options()),
            )
            .replace(
                "__MODEL_CAPABILITIES_JSON__",
                json.dumps(capabilities),
            )
            .replace(
                "__CATALOG_METADATA_JSON__",
                json.dumps(get_catalog_metadata()),
            )
            .replace(
                "__DISCOVERY_PROVIDERS_JSON__",
                json.dumps(sorted(DISCOVERY_ADAPTERS)),
            )
            .replace(
                "__ROLE_DEFAULTS_JSON__",
                json.dumps(role_defaults),
            )
        )
        return HTMLResponse(
            html,
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
                "X-TradingAgents-Web-Build": WEB_BUILD,
                "X-TradingAgents-Asset-Version": ASSET_VERSION,
            },
        )
    raise HTTPException(status_code=503, detail="Frontend assets are not available yet.")


@app.get("/")
def index():
    return _render_index_response()


@app.get("/app/{build_id}")
def versioned_index(build_id: str):
    if build_id != ASSET_VERSION:
        return _render_index_response()
    return _render_index_response()


@app.get("/__clear-site-data__")
def clear_site_data():
    logger.info("Serving explicit cache reset endpoint")
    return PlainTextResponse(
        "TradingAgents requested browser cache/storage reset for this origin. You can now reload / .",
        headers=_clear_site_headers(),
    )


@app.get("/api/health")
def health():
    return {"status": "ok"}


def _require_local_settings_request(request: Request) -> None:
    try:
        client_is_local = ipaddress.ip_address(request.client.host).is_loopback
    except (ValueError, AttributeError):
        client_is_local = False
    hostname = request.url.hostname or ""
    try:
        host_is_local = hostname == "localhost" or ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        host_is_local = False
    origin = request.headers.get("origin")
    expected_origin = f"{request.url.scheme}://{request.url.netloc}"
    if (
        not client_is_local
        or not host_is_local
        or (origin is not None and origin != expected_origin)
        or request.headers.get("sec-fetch-site") == "cross-site"
    ):
        raise HTTPException(status_code=403, detail="Settings are available from the local app only.")


@app.get("/api/models/{provider}/refresh")
def refresh_available_models(provider: str, request: Request):
    """Return only bounded account-visible IDs to the local web picker."""
    _require_local_settings_request(request)
    if provider not in PROVIDER_API_KEY_ENV:
        raise HTTPException(status_code=400, detail="Unknown model provider.")
    result = discover_models(provider, force=True)
    return JSONResponse(asdict(result), headers={"Cache-Control": "no-store"})


@app.get("/api/settings/data-sources")
def data_source_settings(request: Request):
    _require_local_settings_request(request)
    try:
        return JSONResponse({"fields": credential_status()}, headers={"Cache-Control": "no-store"})
    except SettingsStorageError as exc:
        raise HTTPException(status_code=503, detail="Could not load data-source settings.") from exc


@app.put("/api/settings/data-sources")
async def save_data_source_settings(request: Request):
    _require_local_settings_request(request)
    if request.headers.get("content-type", "").split(";", 1)[0] != "application/json":
        raise HTTPException(status_code=415, detail="Settings must be submitted as JSON.")
    try:
        body = await request.body()
        if len(body) > 20_000:
            raise SettingsValidationError("Settings request is too large.")
        payload = json.loads(body)
        if not isinstance(payload, dict) or set(payload) - {"values", "remove"}:
            raise SettingsValidationError("Invalid settings update.")
        fields = await run_in_threadpool(
            update_credentials, payload.get("values", {}), payload.get("remove", [])
        )
        return JSONResponse({"fields": fields}, headers={"Cache-Control": "no-store"})
    except (json.JSONDecodeError, SettingsValidationError) as exc:
        raise HTTPException(status_code=400, detail="Invalid settings. Check the fields and SEC contact email.") from exc
    except SettingsStorageError as exc:
        raise HTTPException(status_code=503, detail="Could not save data-source settings.") from exc


@app.get("/api/speaking-stocks")
def speaking_stocks(
    top_n: int = Query(default=10, ge=1, le=50),
    lookback_days: int = Query(default=30, ge=5, le=120),
):
    try:
        logger.info(
            "Fetching speaking stocks (top_n=%s, lookback_days=%s)",
            top_n,
            lookback_days,
        )
        return {
            "items": fetch_speaking_stocks(top_n=top_n, lookback_days=lookback_days),
        }
    except Exception as exc:
        logger.exception("Speaking stocks fetch failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/market-tickers")
def market_tickers(limit: int = Query(default=12, ge=4, le=24)):
    try:
        logger.info("Fetching market tickers (limit=%s)", limit)
        return {"items": fetch_market_tickers(limit=limit)}
    except Exception as exc:
        logger.exception("Market tickers fetch failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/speaking-stocks/{ticker}")
def speaking_stock_detail(ticker: str):
    try:
        logger.info("Fetching ticker detail for %s", ticker)
        return fetch_ticker_detail(ticker)
    except Exception as exc:
        logger.exception("Ticker detail fetch failed for %s", ticker)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/api/jobs")
def job_history(request: Request, q: str = Query(default="", max_length=100),
                offset: int = Query(default=0, ge=0), limit: int = Query(default=50, ge=1, le=200)):
    _require_local_settings_request(request)
    return JSONResponse(web_service.list_jobs(q=q, offset=offset, limit=limit),
                        headers={"Cache-Control": "no-store"})


@app.post("/api/jobs")
def create_analysis_job(request: AnalysisRequest):
    try:
        payload = request.model_dump()
        logger.info(
            "Creating analysis job (provider=%s, tickers=%s, date=%s)",
            payload.get("llm_provider"),
            payload.get("tickers"),
            payload.get("analysis_date"),
        )
        return create_job(payload)
    except Exception as exc:
        logger.exception("Job creation failed")
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job


@app.get("/api/jobs/{job_id}/reports/{ticker}/investor")
def investor_report(job_id: str, ticker: str, request: Request):
    return _job_report_response(job_id, ticker, request, "investor")


@app.get("/api/jobs/{job_id}/reports/{ticker}/full")
def full_report(job_id: str, ticker: str, request: Request):
    return _job_report_response(job_id, ticker, request, "full")


@app.get("/api/jobs/{job_id}/reports/consolidated")
def consolidated_report(job_id: str, request: Request,
                        format: str = Query(default="html", pattern="^(html|md)$")):
    _require_local_settings_request(request)
    job = get_job(job_id)
    path = report_path(web_service.RESULTS_ROOT, job, None, "consolidated", format) if job else None
    if path is None:
        raise HTTPException(status_code=404, detail="Report not found.")
    if path.suffix == ".md" and format == "html":
        return _markdown_report_response(path, ", ".join(job["tickers"]), job["analysis_date"])
    return FileResponse(path, media_type="text/html" if format == "html" else "text/markdown",
                        filename=path.name,
                        content_disposition_type="inline" if format == "html" else "attachment",
                        headers=_report_headers())


def _report_headers() -> dict[str, str]:
    return {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
            "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'"}


def _markdown_report_response(path: Path, ticker: str, analysis_date: str) -> HTMLResponse:
    try:
        if path.stat().st_size > 8_000_000:
            raise ValueError("Report is too large")
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError, ValueError):
        raise HTTPException(status_code=404, detail="Report not found.") from None
    html = render_report(ticker, {"trade_date": analysis_date}, None,
                         [("1. Saved research", "", [("Full research", path.name, text)])])
    return HTMLResponse(html, headers=_report_headers())


def _job_report_response(job_id: str, ticker: str, request: Request, kind: str):
    try:
        _require_local_settings_request(request)
    except HTTPException as exc:
        raise HTTPException(status_code=403, detail="Reports are available from the local app only.") from exc
    job = get_job(job_id)
    if job is None or ticker not in job.get("tickers", []):
        raise HTTPException(status_code=404, detail="Report not found.")
    if not any(
        result.get("ticker") == ticker and result.get("status") == "completed"
        for result in job.get("results", [])
    ):
        raise HTTPException(status_code=404, detail="Report not found.")
    path = report_path(web_service.RESULTS_ROOT, job, ticker, kind)
    if path is None:
        raise HTTPException(status_code=404, detail="Report not found.") from None
    if path.suffix == ".md":
        return _markdown_report_response(path, ticker, job["analysis_date"])
    return FileResponse(
        path,
        media_type="text/html",
        filename=path.name,
        content_disposition_type="inline",
        headers=_report_headers(),
    )


@app.post("/api/evaluations/run")
def run_saved_forecast_evaluations(request: EvaluationRunRequest | None = None):
    try:
        request = request or EvaluationRunRequest()
        return evaluate_saved_forecasts(
            tickers=tuple(request.tickers),
            date_from=request.date_from,
            date_to=request.date_to,
            pending_only=request.pending_only,
            report_roots=tuple(Path(path) for path in request.report_paths),
        )
    except Exception as exc:
        logger.exception("Forecast evaluation failed")
        raise HTTPException(status_code=500, detail="Forecast evaluation failed.") from exc


@app.post("/api/portfolio/optimize")
def optimize_portfolio_request(payload: dict):
    try:
        return optimize_portfolio_payload(payload)
    except Exception as exc:
        logger.exception("Portfolio optimization failed")
        raise HTTPException(
            status_code=400,
            detail="Invalid portfolio optimization request.",
        ) from exc


@app.on_event("startup")
def on_startup():
    logger.info(
        "TradingAgents web application initialized (build=%s, assets=%s)",
        WEB_BUILD,
        ASSET_VERSION,
    )


@app.on_event("shutdown")
def on_shutdown():
    logger.info("TradingAgents web application stopped")


def _open_browser_when_ready(url: str):
    try:
        logger.info("Opening browser at %s", url)
        webbrowser.open_new_tab(url)
    except Exception:
        logger.exception("Failed to open browser automatically")


def run(
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = False,
    open_browser: bool = True,
    log_level: str = "WARNING",
    log_file: str | None = None,
):
    log_path, normalized_level = configure_logging(log_file=log_file, log_level=log_level)
    url = f"http://{host}:{port}"
    logger.warning(
        "TradingAgents web server starting url=%s build=%s assets=%s log_file=%s log_level=%s",
        url,
        WEB_BUILD,
        ASSET_VERSION,
        log_path,
        normalized_level,
    )
    logger.info("Starting TradingAgents web server at %s", url)
    if open_browser:
        threading.Timer(1.0, _open_browser_when_ready, args=(url,)).start()
    uvicorn.run(
        "tradingagents.web.app:app",
        host=host,
        port=port,
        reload=reload,
        log_level=normalized_level.lower(),
        log_config=None,
        access_log=True,
    )
