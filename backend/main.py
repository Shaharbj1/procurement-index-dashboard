"""
main.py — FastAPI entry point
Serves the REST API at /api/* and the static frontend at /.
"""
import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from backend.database import init_db
from backend.routers import indices, timeseries, calculator, upload, export, review

# Initialise database on startup
init_db()

app = FastAPI(
    title="Global Procurement Index Dashboard",
    description="Track procurement-relevant price indices across packaging, labour, logistics, and API/chemicals.",
    version="3.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# ── API routers ───────────────────────────────────────────────────────────────
app.include_router(indices.router,    prefix="/api", tags=["Indices"])
app.include_router(timeseries.router, prefix="/api", tags=["Time Series"])
app.include_router(calculator.router, prefix="/api", tags=["Calculator"])
app.include_router(upload.router,     prefix="/api", tags=["Upload"])
app.include_router(export.router,     prefix="/api", tags=["Export"])
app.include_router(review.router,     prefix="/api", tags=["Review"])


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
    # Mount sub-directories explicitly so /api routes are not shadowed
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
