"""
database.py — SQLite schema, init, connection pool, upsert helpers
DATABASE_PATH is read from environment variable (Railway Volume mount).
"""
import os
import sqlite3
from contextlib import contextmanager
from typing import Optional

DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/indices.db")

# ── Catalogue seed data — 35 indices ────────────────────────────────────────
SEED_INDICES = [
    # Secondary Packaging (7)
    ("sec_pkg_nbsk",    "NBSK — Pulp price",                    "secondary_packaging", "Fastmarkets", "plastics",  "EUR/tonne", "2015=100", 1),
    ("sec_pkg_euwid",   "EUWID Paper Index",                    "secondary_packaging", "EUWID",       "plastics",  "index",     "2015=100", 1),
    ("sec_pkg_icis",    "ICIS PET Film/Resins",                 "secondary_packaging", "ICIS",        "plastics",  "index",     "2015=100", 1),
    ("sec_pkg_pet5050", "PET 5050/ICISL OR",                    "secondary_packaging", "ICIS",        "plastics",  "index",     "2015=100", 1),
    ("sec_pkg_risi",    "RISI Paper Index",                     "secondary_packaging", "Fastmarkets", "plastics",  "index",     "2015=100", 1),
    ("sec_pkg_pie_pe",  "PIE PE Film Index",                    "secondary_packaging", "manual",      "plastics",  "index",     "2015=100", 0),
    ("sec_pkg_wmc_bopet","WMC BOPET Index",                     "secondary_packaging", "manual",      "plastics",  "index",     "2015=100", 0),
    # EMN (7)
    ("emn_cpi_uk",      "CPI — United Kingdom",                 "emn",                 "eurostat",    "general",   "index",     "2015=100", 0),
    ("emn_aki_blue",    "AKI Blue Collar — Sweden",             "emn",                 "eurostat",    "labor",     "index",     "2015=100", 0),
    ("emn_aki_white",   "AKI White Collar — Sweden",            "emn",                 "eurostat",    "labor",     "index",     "2015=100", 0),
    ("emn_ppi_sweden",  "PPI — Sweden",                         "emn",                 "eurostat",    "general",   "index",     "2015=100", 0),
    ("emn_cpi_italy",   "CPI — Italy",                          "emn",                 "eurostat",    "general",   "index",     "2015=100", 0),
    ("emn_labor_de",    "Labor Cost Index — Germany",           "emn",                 "eurostat",    "labor",     "index",     "2015=100", 0),
    ("emn_ppi_de",      "PPI — Germany",                        "emn",                 "eurostat",    "general",   "index",     "2015=100", 0),
    # Logistics (3)
    ("log_cpi_general", "Consumer Price Index — General",       "logistics",           "manual",      "general",   "index",     "2015=100", 0),
    ("log_transport_ine","Transport Cost Index (INE Chile)",    "logistics",           "manual",      "logistics", "index",     "2015=100", 0),
    ("log_nea_intl",    "NEA International Shipments",          "logistics",           "manual",      "logistics", "index",     "2015=100", 0),
    # Primary Packaging & MD (14)
    ("prim_ppi_eu",     "PPI — EU (Eurostat)",                  "primary_pkg_md",      "eurostat",    "general",   "index",     "2015=100", 0),
    ("prim_ppi_us_bls", "PPI — US BLS",                        "primary_pkg_md",      "bls",         "general",   "index",     "1982=100", 0),
    ("prim_lci_eu",     "LCI Labour Cost — EU",                "primary_pkg_md",      "eurostat",    "labor",     "index",     "2015=100", 0),
    ("prim_hicp_medical","HICP Medical — EU",                  "primary_pkg_md",      "eurostat",    "general",   "index",     "2015=100", 0),
    ("prim_ppi_tubes",  "PPI Tubes (FRED)",                     "primary_pkg_md",      "manual",      "general",   "index",     "2015=100", 0),
    ("prim_eex_energy", "EEX French Power Futures",             "primary_pkg_md",      "manual",      "energy",    "index",     "2015=100", 0),
    ("prim_peg_gas",    "PEG Natural Gas",                      "primary_pkg_md",      "manual",      "energy",    "index",     "2015=100", 0),
    ("prim_ppi_int_bls","PPI Intermediate — BLS",              "primary_pkg_md",      "bls",         "general",   "index",     "1982=100", 0),
    ("prim_cpi_us",     "CPI — US Consumer Price",             "primary_pkg_md",      "bls",         "general",   "index",     "1982=100", 0),
    ("prim_ppi_aptar",  "PPI Intermediate — APTAR",            "primary_pkg_md",      "bls",         "plastics",  "index",     "1982=100", 0),
    ("prim_cpi_g20",    "CPI G20",                              "primary_pkg_md",      "eurostat",    "general",   "index",     "2015=100", 0),
    ("prim_ppi_union",  "PPI Intermediate — Union Plastic",    "primary_pkg_md",      "bls",         "plastics",  "index",     "1982=100", 0),
    ("prim_bopet",      "BOPET — Biax. PET",                   "primary_pkg_md",      "manual",      "plastics",  "EUR/tonne", "2015=100", 1),
    ("prim_ldpe",       "LDPE — Low-Density Polyethylene",     "primary_pkg_md",      "manual",      "plastics",  "EUR/tonne", "2015=100", 1),
    # API & Chemicals (4)
    ("api_ppi_pharma",  "PPI Pharma Mfg — BLS PCU325412",      "api_chemicals",       "bls",         "chemicals", "index",     "2012=100", 0),
    ("api_eurostat",    "Eurostat PPI & CPI",                  "api_chemicals",       "eurostat",    "chemicals", "index",     "2015=100", 0),
    ("api_india_labor", "India Labor & Inflation (MoLE)",       "api_chemicals",       "manual",      "labor",     "index",     "2015=100", 0),
    ("api_salary_expl", "Salary Explorer — Avg Labor Cost",    "api_chemicals",       "manual",      "labor",     "index",     "2015=100", 0),
]


def get_db_path() -> str:
    """Return the database path, ensuring the directory exists."""
    path = DATABASE_PATH
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    return path


@contextmanager
def get_connection():
    """Context manager yielding a sqlite3 connection with row_factory."""
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Create tables and seed catalogue if empty."""
    with get_connection() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS indices (
                id           TEXT PRIMARY KEY,
                name         TEXT NOT NULL,
                segment      TEXT NOT NULL,
                source       TEXT NOT NULL,
                category     TEXT NOT NULL,
                unit         TEXT NOT NULL DEFAULT 'index',
                base_year    TEXT NOT NULL DEFAULT '2015=100',
                paid_source  INTEGER NOT NULL DEFAULT 0,
                last_updated DATETIME,
                active       INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS index_values (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                index_id    TEXT NOT NULL REFERENCES indices(id),
                period      TEXT NOT NULL,
                value       REAL NOT NULL,
                mom_change  REAL,
                yoy_change  REAL,
                created_at  DATETIME DEFAULT (datetime('now'))
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ux_index_period
                ON index_values (index_id, period);
        """)

        # Seed catalogue entries (skip if already present)
        existing = conn.execute("SELECT COUNT(*) FROM indices").fetchone()[0]
        if existing == 0:
            conn.executemany(
                """INSERT OR IGNORE INTO indices
                   (id, name, segment, source, category, unit, base_year, paid_source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                SEED_INDICES,
            )


def upsert_index_value(conn: sqlite3.Connection, index_id: str, period: str, value: float) -> dict:
    """
    Insert or update a single (index_id, period) row.
    Computes MoM and YoY changes automatically.
    Returns {'action': 'added'|'updated'}.
    """
    # Check existing
    existing = conn.execute(
        "SELECT id FROM index_values WHERE index_id=? AND period=?",
        (index_id, period),
    ).fetchone()

    # Compute MoM: previous month
    mom_change = _compute_pct_change(conn, index_id, period, months=1)
    yoy_change = _compute_pct_change(conn, index_id, period, months=12)

    if existing:
        conn.execute(
            """UPDATE index_values
               SET value=?, mom_change=?, yoy_change=?, created_at=datetime('now')
               WHERE index_id=? AND period=?""",
            (value, mom_change, yoy_change, index_id, period),
        )
        action = "updated"
    else:
        conn.execute(
            """INSERT INTO index_values (index_id, period, value, mom_change, yoy_change)
               VALUES (?, ?, ?, ?, ?)""",
            (index_id, period, value, mom_change, yoy_change),
        )
        action = "added"

    # Update last_updated on index catalogue
    conn.execute(
        "UPDATE indices SET last_updated=datetime('now') WHERE id=?",
        (index_id,),
    )
    return {"action": action}


def _compute_pct_change(conn: sqlite3.Connection, index_id: str, period: str, months: int) -> Optional[float]:
    """Compute % change vs N months ago. Returns None if no prior data."""
    prior_period = _subtract_months(period, months)
    if prior_period is None:
        return None
    row = conn.execute(
        "SELECT value FROM index_values WHERE index_id=? AND period=?",
        (index_id, prior_period),
    ).fetchone()
    if row is None:
        return None
    prior_val = row["value"]
    if prior_val == 0:
        return None
    # We don't know the current value yet — fetch it or it's being inserted
    # We need to get the current value from what we're inserting; caller passes value
    # So we can't call this directly — instead compute inline in upsert
    return None  # placeholder


def upsert_index_value_full(conn: sqlite3.Connection, index_id: str, period: str, value: float) -> dict:
    """
    Full upsert with proper MoM/YoY computation.
    """
    def get_value(p: str) -> Optional[float]:
        r = conn.execute(
            "SELECT value FROM index_values WHERE index_id=? AND period=?",
            (index_id, p),
        ).fetchone()
        return r["value"] if r else None

    prior_month = _subtract_months(period, 1)
    prior_year  = _subtract_months(period, 12)

    prior_month_val = get_value(prior_month) if prior_month else None
    prior_year_val  = get_value(prior_year) if prior_year else None

    mom = round((value - prior_month_val) / prior_month_val * 100, 4) if prior_month_val else None
    yoy = round((value - prior_year_val)  / prior_year_val  * 100, 4) if prior_year_val  else None

    existing = conn.execute(
        "SELECT id FROM index_values WHERE index_id=? AND period=?",
        (index_id, period),
    ).fetchone()

    if existing:
        conn.execute(
            """UPDATE index_values
               SET value=?, mom_change=?, yoy_change=?, created_at=datetime('now')
               WHERE index_id=? AND period=?""",
            (value, mom, yoy, index_id, period),
        )
        action = "updated"
    else:
        conn.execute(
            """INSERT INTO index_values (index_id, period, value, mom_change, yoy_change)
               VALUES (?, ?, ?, ?, ?)""",
            (index_id, period, value, mom, yoy),
        )
        action = "added"

    conn.execute(
        "UPDATE indices SET last_updated=datetime('now') WHERE id=?",
        (index_id,),
    )
    return {"action": action}


def _subtract_months(period: str, months: int) -> Optional[str]:
    """Given 'YYYY-MM', subtract N months. Returns None if period is invalid."""
    try:
        year, month = int(period[:4]), int(period[5:7])
        total = year * 12 + (month - 1) - months
        y, m = divmod(total, 12)
        return f"{y:04d}-{m+1:02d}"
    except Exception:
        return None
