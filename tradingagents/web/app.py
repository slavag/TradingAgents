from __future__ import annotations

from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from tradingagents.web.service import create_job, fetch_speaking_stocks, get_job

STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="TradingAgents Web", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class AnalysisRequest(BaseModel):
    tickers: list[str] | str
    analysis_date: str
    analysts: list[str] = Field(
        default_factory=lambda: ["market", "social", "news", "fundamentals"]
    )
    research_depth: int = 3
    llm_provider: str = "openai"
    backend_url: str | None = None
    quick_thinker: str = "gpt-5.4"
    deep_thinker: str = "gpt-5.4"
    google_thinking_level: str | None = None
    openai_reasoning_effort: str | None = "medium"
    save_reports: bool = True
    export_path: str | None = None


@app.get("/")
def index():
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    raise HTTPException(status_code=503, detail="Frontend assets are not available yet.")


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/speaking-stocks")
def speaking_stocks(
    top_n: int = Query(default=10, ge=1, le=50),
    lookback_days: int = Query(default=30, ge=5, le=120),
):
    try:
        return {
            "items": fetch_speaking_stocks(top_n=top_n, lookback_days=lookback_days),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/jobs")
def create_analysis_job(request: AnalysisRequest):
    try:
        return create_job(request.model_dump())
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job


def run():
    uvicorn.run(
        "tradingagents.web.app:app",
        host="127.0.0.1",
        port=8000,
        reload=False,
    )
