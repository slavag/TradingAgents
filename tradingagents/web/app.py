from __future__ import annotations

import json
import hashlib
import logging
import logging.config
import os
import threading
import webbrowser
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from tradingagents.llm_clients.model_catalog import get_web_model_options
from tradingagents.web.service import (
    create_job,
    fetch_market_tickers,
    fetch_speaking_stocks,
    fetch_ticker_detail,
    get_job,
)

logger = logging.getLogger("tradingagents.web")
WEB_BUILD = "sidebar-accordion-v2"

STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _compute_asset_version() -> str:
    digest = hashlib.sha256()
    for filename in ("index.html", "styles.css", "app.js"):
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
    save_reports: bool = True
    export_path: str | None = None


def _render_index_response() -> HTMLResponse:
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        html = (
            index_path.read_text(encoding="utf-8")
            .replace("__ASSET_VERSION__", ASSET_VERSION)
            .replace(
                "__MODEL_OPTIONS_JSON__",
                json.dumps(get_web_model_options()),
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
