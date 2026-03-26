"""
FastAPI backend for Process Mining — Directum RX.
Parses logs at startup, builds DuckDB, serves REST API + static frontend.
"""

import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

import ai
import db
import metadata as meta_loader
import parser as log_parser

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

logger = logging.getLogger("uvicorn.error")

LOGS_DIR     = os.getenv("LOGS_DIR",     "/data/logs")
METADATA_DIR = os.getenv("METADATA_DIR", "/data/metadata")

_startup: dict = {
    "status": "loading",
    "started_at": None,
    "ready_at": None,
    "parse_seconds": None,
    "build_seconds": None,
    "meta": {},
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    _startup["started_at"] = time.time()

    logger.info("Loading entity metadata…")
    process_names = meta_loader.load_process_names(METADATA_DIR)

    logger.info(f"Parsing logs from {LOGS_DIR}…")
    t1 = time.time()
    events, parse_meta = log_parser.parse_logs(LOGS_DIR)
    _startup["parse_seconds"] = round(time.time() - t1, 1)
    _startup["meta"] = parse_meta
    logger.info(f"Parsed {parse_meta['parsed_events']} events in {_startup['parse_seconds']}s")

    logger.info("Building DuckDB tables…")
    t2 = time.time()
    db.build_db(events, process_names)
    _startup["build_seconds"] = round(time.time() - t2, 1)
    _startup["status"] = "ready"
    _startup["ready_at"] = time.time()
    logger.info(f"Ready in {round(_startup['ready_at'] - _startup['started_at'], 1)}s total")
    yield


app = FastAPI(title="Process Mining – Directum RX", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"],
)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", include_in_schema=False)
async def root():
    return FileResponse(str(STATIC_DIR / "index.html"))


def _ready():
    if _startup["status"] != "ready":
        raise HTTPException(503, "Data is still loading…")


# ── Status ────────────────────────────────────────────────────────────────────
@app.get("/api/status")
async def api_status():
    info = dict(_startup)
    if info.get("ready_at") and info.get("started_at"):
        info["total_seconds"] = round(info["ready_at"] - info["started_at"], 1)
    info["ai_enabled"] = ai.is_enabled()
    info["ai_model"]   = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini") if ai.is_enabled() else None
    return info


# ── Core analytics ─────────────────────────────────────────────────────────────
@app.get("/api/overview")
async def api_overview():
    _ready()
    return db.query_overview()


@app.get("/api/processes")
async def api_processes():
    _ready()
    return db.query_processes()


@app.get("/api/process/{process_id}")
async def api_process_detail(process_id: str):
    _ready()
    return db.query_process_detail(process_id)


@app.get("/api/process/{process_id}/timeline")
async def api_process_timeline(process_id: str):
    _ready()
    return db.query_process_timeline(process_id)


@app.get("/api/blocks")
async def api_blocks(
    process_id: Optional[str] = Query(None),
    scheme_id:  Optional[int] = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    _ready()
    return db.query_blocks(process_id=process_id, scheme_id=scheme_id, limit=limit)


@app.get("/api/blocks/{scheme_id}/{block_id}/results")
async def api_block_results(scheme_id: int, block_id: str):
    _ready()
    return db.query_block_results(scheme_id, block_id)


@app.get("/api/timeline")
async def api_timeline(
    granularity: str = Query("hour", pattern="^(hour|day)$"),
):
    _ready()
    return db.query_timeline(granularity)


@app.get("/api/flow")
async def api_flow(
    scheme_id:  Optional[int] = Query(None),
    process_id: Optional[str] = Query(None),
    top_n: int = Query(30, ge=1, le=100),
):
    _ready()
    return db.query_flow(scheme_id=scheme_id, process_id=process_id, top_n=top_n)


@app.get("/api/bottlenecks")
async def api_bottlenecks():
    _ready()
    return db.query_bottlenecks()


@app.get("/api/issues")
async def api_issues():
    _ready()
    return db.query_issues()


@app.get("/api/filters")
async def api_filters():
    _ready()
    return db.query_filters()


@app.get("/api/heatmap")
async def api_heatmap():
    _ready()
    return db.query_heatmap()


@app.get("/api/histogram")
async def api_histogram(process_id: Optional[str] = Query(None)):
    _ready()
    return db.query_duration_histogram(process_id=process_id)


# ── AI insights (streaming SSE) ───────────────────────────────────────────────
@app.get("/api/ai-insights")
async def api_ai_insights():
    _ready()
    data = db.query_summary_for_ai()

    def generate():
        for chunk in ai.stream_analysis(data):
            payload = json.dumps({"text": chunk}, ensure_ascii=False)
            yield f"data: {payload}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":   "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
