import os
import time
import logging
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from validator import run_validation

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Simple FastAPI + React App")

_errors_cache: dict = {"data": None, "ts": 0.0}
_CACHE_TTL = 300  # seconds


def _get_errors():
    now = time.time()
    if _errors_cache["data"] is None or (now - _errors_cache["ts"]) > _CACHE_TTL:
        _errors_cache["data"] = run_validation()
        _errors_cache["ts"] = now
    return _errors_cache["data"]

# --- API Routes ---
@app.get("/api/hello")
async def hello():
    logger.info("Accessed /api/hello")
    return {"message": "Hello from FastAPI!"}

@app.get("/api/health")
async def health_check():
    logger.info("Health check at /api/health")
    return {"status": "healthy"}

@app.get("/api/data")
async def get_data():
    logger.info("Data requested at /api/data")
    data = [{"x": x, "y": 2 ** x} for x in range(30)]
    return {
        "data": data,
        "title": "Hello world!",
        "x_title": "Apps",
        "y_title": "Fun with data"
    }

@app.get("/api/errors")
def get_errors(
    source: str = Query(default=None),
    error_type: str = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
):
    logger.info(f"Errors requested — source={source} type={error_type} page={page}")
    errors = _get_errors()

    if source:
        errors = [e for e in errors if e["source"] == source]
    if error_type:
        errors = [e for e in errors if e["error_type"] == error_type]

    summary: dict[str, int] = {}
    for e in errors:
        summary[e["source"]] = summary.get(e["source"], 0) + 1

    total = len(errors)
    start = (page - 1) * page_size
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "errors": errors[start : start + page_size],
        "summary": summary,
    }


@app.post("/api/errors/refresh")
def refresh_errors():
    _errors_cache["data"] = None
    _get_errors()
    logger.info("Error cache refreshed")
    return {"status": "refreshed", "total": len(_errors_cache["data"])}


# --- Static Files Setup ---
static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
os.makedirs(static_dir, exist_ok=True)

app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")

# --- Catch-all for React Routes ---
@app.get("/{full_path:path}")
async def serve_react(full_path: str):
    index_html = os.path.join(static_dir, "index.html")
    if os.path.exists(index_html):
        logger.info(f"Serving React frontend for path: /{full_path}")
        return FileResponse(index_html)
    logger.error("Frontend not built. index.html missing.")
    raise HTTPException(
        status_code=404,
        detail="Frontend not built. Please run 'npm run build' first."
    )