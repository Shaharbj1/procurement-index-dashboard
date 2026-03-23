"""
fetchers/__init__.py — Orchestrator for all auto-fetch functions.
Called by APScheduler on the 15th of each month.
Never raises exceptions — all errors are caught and logged to fetch_log.
"""
import asyncio
import logging
import time
from datetime import datetime
from typing import List, Dict

from backend.database import get_connection, upsert_index_value_full

logger = logging.getLogger(__name__)


async def run_all_fetchers():
    """
    Run all fetcher functions, upsert results into the DB, and write one
    fetch_log row per fetcher (not per row). Never raises.
    Returns summary dict.
    """
    logger.info("run_all_fetchers: starting monthly auto-fetch")

    from backend.fetchers import eurostat, bls, others

    fetcher_groups = [
        (eurostat.fetch_ppi_eu,              "Eurostat PPI EU"),
        (eurostat.fetch_lci_eu,              "Eurostat LCI EU"),
        (eurostat.fetch_hicp_eu,             "Eurostat HICP EU"),
        (eurostat.fetch_emn_de,              "Eurostat EMN Germany"),
        (eurostat.fetch_extra_singles,       "Eurostat extra singles"),
        (eurostat.fetch_regional_ppi,        "Eurostat Regional PPI"),
        (eurostat.fetch_regional_cpi,        "Eurostat Regional CPI"),
        (eurostat.fetch_regional_lci,        "Eurostat Regional LCI"),
        (eurostat.fetch_regional_energy,     "Eurostat Regional Energy"),
        (bls.fetch_bls,                      "BLS indices"),
        (others.fetch_scb_sweden,            "SCB Sweden"),
        (others.fetch_destatis,              "Destatis Germany"),
        (others.fetch_istat_italy,           "ISTAT Italy"),
        (others.fetch_ons_uk,                "ONS UK"),
        (others.fetch_fred,                  "FRED USA"),
        (others.fetch_oecd,                  "OECD"),
        (others.fetch_eex_scrapers,          "EEX/PEG scrapers"),
        (others.fetch_ine_chile,             "INE Chile"),
        (others.fetch_india_labour,          "India Labour Bureau"),
        (others.fetch_salary_explorer,       "Salary Explorer"),
        (others.fetch_switzerland_regional,  "FSO Switzerland Regional"),
        (others.fetch_israel_regional,       "CBS Israel Regional"),
    ]

    total_added = 0
    total_updated = 0
    total_errors = 0

    for fn, desc in fetcher_groups:
        t0 = time.monotonic()
        rows: List[Dict] = []
        error_msg = None
        try:
            rows = await fn()
        except Exception as exc:
            error_msg = str(exc)[:500]
            logger.error("Fetcher %s raised: %s", desc, error_msg)
            total_errors += 1

        duration_ms = int((time.monotonic() - t0) * 1000)

        if not rows and error_msg is None:
            # Skipped (e.g. missing env var, returned empty)
            logger.info("Fetcher %s: skipped/empty (took %dms)", desc, duration_ms)
            _log_entry(desc, "skipped", 0, 0, None, duration_ms)
            continue

        if error_msg and not rows:
            _log_entry(desc, "error", 0, 0, error_msg, duration_ms)
            continue

        # Upsert all rows in one DB pass
        added_here = 0
        updated_here = 0
        row_errors = []
        for row in rows:
            try:
                with get_connection() as conn:
                    result = upsert_index_value_full(
                        conn, row["index_id"], row["period"], float(row["value"])
                    )
                    if result["action"] == "added":
                        added_here += 1
                    else:
                        updated_here += 1
            except Exception as exc:
                row_errors.append(str(exc)[:200])
                total_errors += 1

        # One log entry per fetcher group
        status = "ok" if not row_errors else "error"
        err_summary = "; ".join(row_errors[:3]) if row_errors else None
        _log_entry(desc, status, added_here, updated_here, err_summary, duration_ms)

        logger.info(
            "Fetcher %s: added=%d updated=%d errors=%d took=%dms",
            desc, added_here, updated_here, len(row_errors), duration_ms,
        )

        total_added   += added_here
        total_updated += updated_here

    logger.info(
        "run_all_fetchers: done — added=%d updated=%d errors=%d",
        total_added, total_updated, total_errors,
    )
    return {"added": total_added, "updated": total_updated, "errors": total_errors}


def _log_entry(index_id, status, rows_added, rows_updated, error_msg, duration_ms):
    """Write a single row to fetch_log."""
    try:
        with get_connection() as conn:
            conn.execute(
                """INSERT INTO fetch_log
                   (run_at, index_id, status, rows_added, rows_updated, error_msg, duration_ms)
                   VALUES (datetime('now'), ?, ?, ?, ?, ?, ?)""",
                (str(index_id or "unknown")[:100], status,
                 rows_added, rows_updated, error_msg, duration_ms),
            )
    except Exception as exc:
        logger.warning("Failed to write fetch_log: %s", exc)
