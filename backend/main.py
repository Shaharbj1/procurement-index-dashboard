"""
main.py — FastAPI entry point
Serves the REST API at /api/* and the static frontend at /.
APScheduler runs auto-fetch on the 15th of each month (UTC).
"""
import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from backend.database import init_db
from backend.routers import indices, timeseries, calculator, upload, export, review

logger = logging.getLogger(__name__)

# Initialise database on import (before lifespan, so migrations run even in tests)
init_db()

# ── APScheduler ───────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    from backend.fetchers import run_all_fetchers

    fetch_day  = int(os.getenv("FETCH_DAY",  "15"))
    fetch_hour = int(os.getenv("FETCH_HOUR", "8"))

    scheduler.add_job(
        run_all_fetchers,
        CronTrigger(day=fetch_day, hour=fetch_hour, timezone="UTC"),
        id="monthly_fetch",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(
        "Scheduler started — auto-fetch on day=%d hour=%d UTC",
        fetch_day, fetch_hour,
    )
    yield
    scheduler.shutdown()
    logger.info("Scheduler shut down")


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Global Procurement Index Dashboard",
    description="Track procurement-relevant price indices across packaging, labour, logistics, and API/chemicals.",
    version="3.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)

# ── API routers ───────────────────────────────────────────────────────────────
app.include_router(indices.router,    prefix="/api", tags=["Indices"])
app.include_router(timeseries.router, prefix="/api", tags=["Time Series"])
app.include_router(calculator.router, prefix="/api", tags=["Calculator"])
app.include_router(upload.router,     prefix="/api", tags=["Upload"])
app.include_router(export.router,     prefix="/api", tags=["Export"])
app.include_router(review.router,     prefix="/api", tags=["Review"])

from backend.routers.admin   import router as admin_router
from backend.routers.regional import router as regional_router

app.include_router(admin_router,    prefix="/api/admin",    tags=["Admin"])
app.include_router(regional_router, prefix="/api/regional", tags=["Regional"])


@app.get("/api/health", tags=["Health"])
def health():
    from backend.database import get_connection
    with get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM index_values").fetchone()[0]
    return {"status": "ok", "db_records": count}


# ── Static frontend ───────────────────────────────────────────────────────────
_frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
_frontend_dir = os.path.abspath(_frontend_dir)

if os.path.isdir(_frontend_dir):
    app.mount("/css", StaticFiles(directory=os.path.join(_frontend_dir, "css")), name="css")
    app.mount("/js",  StaticFiles(directory=os.path.join(_frontend_dir, "js")),  name="js")

    @app.get("/", include_in_schema=False)
    def root():
        return FileResponse(os.path.join(_frontend_dir, "index.html"))

    @app.get("/calculator.html", include_in_schema=False)
    def calc_page():
        return FileResponse(os.path.join(_frontend_dir, "calculator.html"))

    @app.get("/upload.html", include_in_schema=False)
    def upload_page():
        return FileResponse(os.path.join(_frontend_dir, "upload.html"))

    @app.get("/executive-review.html", include_in_schema=False)
    def review_page():
        return FileResponse(os.path.join(_frontend_dir, "executive-review.html"))

    @app.get("/admin.html", include_in_schema=False)
    def admin_page():
        return FileResponse(os.path.join(_frontend_dir, "admin.html"))

    @app.get("/regional.html", include_in_schema=False)
    def regional_page():
        return FileResponse(os.path.join(_frontend_dir, "regional.html"))
