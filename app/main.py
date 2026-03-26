"""
FastAPI backend for Process Mining — Directum RX.
Parses logs at startup, builds DuckDB, serves REST API + static frontend.
"""

import asyncio
import io
import json
import logging
import os
import time
import zipfile
import tarfile
import gzip
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, File, UploadFile
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
    CORSMiddleware, allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", include_in_schema=False)
async def root():
    return FileResponse(str(STATIC_DIR / "index.html"))


def _ready():
    if _startup["status"] != "ready":
        raise HTTPException(503, "Data is still loading…")


def _rebuild_sync():
    """Synchronous full re-parse + DB rebuild. Run in thread executor."""
    process_names = meta_loader.load_process_names(METADATA_DIR)
    t1 = time.time()
    events, parse_meta = log_parser.parse_logs(LOGS_DIR)
    _startup["parse_seconds"] = round(time.time() - t1, 1)
    _startup["meta"] = parse_meta
    logger.info(f"Rebuilt: {parse_meta['parsed_events']} events in {_startup['parse_seconds']}s")
    t2 = time.time()
    db.build_db(events, process_names)
    _startup["build_seconds"] = round(time.time() - t2, 1)
    _startup["status"] = "ready"
    _startup["ready_at"] = time.time()


# ── Status ────────────────────────────────────────────────────────────────────
@app.get("/api/status")
async def api_status():
    info = dict(_startup)
    if info.get("ready_at") and info.get("started_at"):
        info["total_seconds"] = round(info["ready_at"] - info["started_at"], 1)
    info["ai_enabled"] = ai.is_enabled()
    info["ai_model"]   = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini") if ai.is_enabled() else None
    return info


# ── Log file management ───────────────────────────────────────────────────────
@app.get("/api/logs")
async def api_list_logs():
    """List log files currently in LOGS_DIR."""
    logs_path = Path(LOGS_DIR)
    if not logs_path.exists():
        return []
    files = []
    for f in sorted(logs_path.iterdir()):
        if f.is_file():
            stat = f.stat()
            files.append({
                "name": f.name,
                "size_bytes": stat.st_size,
                "size_mb": round(stat.st_size / 1024 / 1024, 2),
                "modified": round(stat.st_mtime),
            })
    return files


@app.post("/api/upload")
async def api_upload(files: list[UploadFile] = File(...)):
    """
    Upload log files (.log, .gz, .zip, .tar.gz) and rebuild the analytics DB.
    ZIP archives are extracted; only .log files inside are kept.
    """
    logs_path = Path(LOGS_DIR)
    logs_path.mkdir(parents=True, exist_ok=True)

    saved, errors = [], []

    for upload in files:
        raw_name = Path(upload.filename or "upload.log").name
        content = await upload.read()
        low = raw_name.lower()

        try:
            if low.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    for member in zf.namelist():
                        mname = Path(member).name
                        if not mname or mname.startswith("."):
                            continue
                        if mname.lower().endswith((".log", ".log.gz")):
                            data = zf.read(member)
                            (logs_path / mname).write_bytes(data)
                            saved.append(mname)

            elif low.endswith((".tar.gz", ".tgz")):
                with tarfile.open(fileobj=io.BytesIO(content)) as tf:
                    for member in tf.getmembers():
                        mname = Path(member.name).name
                        if not mname or not mname.lower().endswith(".log"):
                            continue
                        fobj = tf.extractfile(member)
                        if fobj:
                            (logs_path / mname).write_bytes(fobj.read())
                            saved.append(mname)

            elif low.endswith(".gz"):
                # single gzipped log: strip .gz suffix
                dest_name = raw_name[:-3] if raw_name.endswith(".gz") else raw_name + ".log"
                (logs_path / dest_name).write_bytes(gzip.decompress(content))
                saved.append(dest_name)

            else:
                (logs_path / raw_name).write_bytes(content)
                saved.append(raw_name)

        except Exception as exc:
            errors.append(f"{raw_name}: {exc}")
            logger.warning(f"Upload error for {raw_name}: {exc}")

    if not saved:
        raise HTTPException(400, f"No valid log files extracted. Errors: {errors}")

    _startup["status"] = "reloading"
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _rebuild_sync)

    return {
        "saved": saved,
        "errors": errors,
        "parsed_events": _startup["meta"].get("parsed_events", 0),
        "total_files": len(_startup["meta"].get("files", [])),
        "parse_seconds": _startup["parse_seconds"],
    }


@app.delete("/api/logs/{filename}")
async def api_delete_log(filename: str):
    """Delete a log file and rebuild the DB."""
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(400, "Invalid filename")
    filepath = Path(LOGS_DIR) / filename
    if not filepath.exists():
        raise HTTPException(404, "File not found")
    filepath.unlink()
    logger.info(f"Deleted log file: {filename}")

    _startup["status"] = "reloading"
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _rebuild_sync)

    return {"deleted": filename, "parsed_events": _startup["meta"].get("parsed_events", 0)}


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
