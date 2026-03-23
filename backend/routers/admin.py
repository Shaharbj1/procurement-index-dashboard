"""
routers/admin.py — Admin endpoints.
POST /api/admin/refresh — triggers run_all_fetchers() in background.
GET  /api/admin/fetch-log — last 50 fetch_log rows + scheduler info + env status.
"""
import os
from fastapi import APIRouter, BackgroundTasks
from backend.database import get_connection

router = APIRouter()

PAID_INDEX_IDS = [
    "sec_pkg_nbsk", "sec_pkg_euwid", "sec_pkg_icis", "sec_pkg_pet5050",
    "sec_pkg_risi", "sec_pkg_wmc_bopet", "prim_bopet", "prim_ldpe",
]


@router.post("/refresh")
async def trigger_refresh(background_tasks: BackgroundTasks):
    """Trigger run_all_fetchers() in background. Returns immediately."""
    from backend.fetchers import run_all_fetchers
    background_tasks.add_task(run_all_fetchers)
    return {"started": True, "message": "Fetch job queued in background"}


@router.get("/fetch-log")
def get_fetch_log():
    """Return last 50 fetch_log rows, scheduler info, env variable status."""
    from backend.main import scheduler

    with get_connection() as conn:
        rows = conn.execute(
            """SELECT id, run_at, index_id, status, rows_added, rows_updated,
                      error_msg, duration_ms
               FROM fetch_log
               ORDER BY id DESC
               LIMIT 50"""
        ).fetchall()
        log_entries = [dict(r) for r in rows]

    # Scheduler next/last run
    next_run = None
    last_run = None
    try:
        job = scheduler.get_job("monthly_fetch")
        if job:
            next_run = job.next_run_time.isoformat() if job.next_run_time else None
    except Exception:
        pass

    if log_entries:
        last_run = log_entries[0]["run_at"]

    # Env variable presence (never expose actual values)
    env_status = {
        "BLS_API_KEY":    bool(os.getenv("BLS_API_KEY")),
        "FRED_API_KEY":   bool(os.getenv("FRED_API_KEY")),
        "DESTATIS_USER":  bool(os.getenv("DESTATIS_USER")),
        "DESTATIS_PASS":  bool(os.getenv("DESTATIS_PASS")),
        "IEA_API_KEY":    bool(os.getenv("IEA_API_KEY")),
    }

    overall_status = "NEVER_RUN"
    if log_entries:
        has_error = any(e["status"] == "error" for e in log_entries[:10])
        overall_status = "ERROR" if has_error else "OK"

    return {
        "fetch_log":      log_entries,
        "next_scheduled_run": next_run,
        "last_run":       last_run,
        "overall_status": overall_status,
        "env_status":     env_status,
        "paid_indices":   PAID_INDEX_IDS,
    }
