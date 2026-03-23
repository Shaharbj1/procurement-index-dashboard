"""
database.py — SQLite schema, init, connection pool, upsert helpers
DATABASE_PATH is read from environment variable (Railway Volume mount).
"""
import os
import sqlite3
from contextlib import contextmanager
from typing import Optional

DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/indices.db")

# ── Catalogue seed data — 35 original indices ────────────────────────────────
# Each dict: id, name, segment, source, category, unit, base_year, paid_source,
#            source_url (None for paid/manual), period_type, country_iso
SEED_INDICES = [
    # Secondary Packaging (7) — all paid/manual, no source_url
    {"id": "sec_pkg_nbsk",     "name": "NBSK — Pulp price",              "segment": "secondary_packaging", "source": "Fastmarkets", "category": "plastics",  "unit": "EUR/tonne", "base_year": "2015=100", "paid_source": 1, "source_url": None,                                                                "period_type": "monthly", "country_iso": None},
    {"id": "sec_pkg_euwid",    "name": "EUWID Paper Index",              "segment": "secondary_packaging", "source": "EUWID",       "category": "plastics",  "unit": "index",     "base_year": "2015=100", "paid_source": 1, "source_url": None,                                                                "period_type": "monthly", "country_iso": None},
    {"id": "sec_pkg_icis",     "name": "ICIS PET Film/Resins",           "segment": "secondary_packaging", "source": "ICIS",        "category": "plastics",  "unit": "index",     "base_year": "2015=100", "paid_source": 1, "source_url": None,                                                                "period_type": "monthly", "country_iso": None},
    {"id": "sec_pkg_pet5050",  "name": "PET 5050/ICISL OR",              "segment": "secondary_packaging", "source": "ICIS",        "category": "plastics",  "unit": "index",     "base_year": "2015=100", "paid_source": 1, "source_url": None,                                                                "period_type": "monthly", "country_iso": None},
    {"id": "sec_pkg_risi",     "name": "RISI Paper Index",               "segment": "secondary_packaging", "source": "Fastmarkets", "category": "plastics",  "unit": "index",     "base_year": "2015=100", "paid_source": 1, "source_url": None,                                                                "period_type": "monthly", "country_iso": None},
    {"id": "sec_pkg_pie_pe",   "name": "PIE PE Film Index",              "segment": "secondary_packaging", "source": "manual",      "category": "plastics",  "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": None,                                                                "period_type": "monthly", "country_iso": None},
    {"id": "sec_pkg_wmc_bopet","name": "WMC BOPET Index",               "segment": "secondary_packaging", "source": "manual",      "category": "plastics",  "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": None,                                                                "period_type": "monthly", "country_iso": None},
    # EMN (7)
    {"id": "emn_cpi_uk",       "name": "CPI — United Kingdom",           "segment": "emn",                 "source": "eurostat",    "category": "general",   "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://www.ons.gov.uk/economy/inflationandpriceindices",           "period_type": "monthly", "country_iso": "GB"},
    {"id": "emn_aki_blue",     "name": "AKI Blue Collar — Sweden",       "segment": "emn",                 "source": "eurostat",    "category": "labor",     "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://www.scb.se/en/finding-statistics/statistics-by-subject-area/labour-market/wages-salaries-and-labour-costs/", "period_type": "monthly", "country_iso": "SE"},
    {"id": "emn_aki_white",    "name": "AKI White Collar — Sweden",      "segment": "emn",                 "source": "eurostat",    "category": "labor",     "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://www.scb.se/en/finding-statistics/statistics-by-subject-area/labour-market/wages-salaries-and-labour-costs/", "period_type": "monthly", "country_iso": "SE"},
    {"id": "emn_ppi_sweden",   "name": "PPI — Sweden",                   "segment": "emn",                 "source": "eurostat",    "category": "general",   "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://www.scb.se/en/finding-statistics/statistics-by-subject-area/trade-in-goods-and-services/price-statistics/producer-price-index-ppi/", "period_type": "monthly", "country_iso": "SE"},
    {"id": "emn_cpi_italy",    "name": "CPI — Italy",                    "segment": "emn",                 "source": "eurostat",    "category": "general",   "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://ec.europa.eu/eurostat/databrowser/view/prc_hicp_midx",    "period_type": "monthly", "country_iso": "IT"},
    {"id": "emn_labor_de",     "name": "Labor Cost Index — Germany",     "segment": "emn",                 "source": "eurostat",    "category": "labor",     "unit": "index",     "base_year": "2016=100", "paid_source": 0, "source_url": "https://ec.europa.eu/eurostat/databrowser/view/lc_lci_lev",       "period_type": "quarterly", "country_iso": "DE"},
    {"id": "emn_ppi_de",       "name": "PPI — Germany",                  "segment": "emn",                 "source": "eurostat",    "category": "general",   "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://www-genesis.destatis.de/genesis/online",                    "period_type": "monthly", "country_iso": "DE"},
    # Logistics (3)
    {"id": "log_cpi_general",  "name": "Consumer Price Index — General", "segment": "logistics",           "source": "manual",      "category": "general",   "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://data.oecd.org/price/inflation-cpi.htm",                     "period_type": "monthly", "country_iso": None},
    {"id": "log_transport_ine","name": "Transport Cost Index (INE Chile)","segment": "logistics",           "source": "manual",      "category": "logistics", "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://www.ine.cl/estadisticas/economia/indices-de-precios-y-costos/indice-de-precios-de-transporte", "period_type": "monthly", "country_iso": "CL"},
    {"id": "log_nea_intl",     "name": "NEA International Shipments",    "segment": "logistics",           "source": "manual",      "category": "logistics", "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": None,                                                                "period_type": "monthly", "country_iso": None},
    # Primary Packaging & MD (14)
    {"id": "prim_ppi_eu",      "name": "PPI — EU (Eurostat)",            "segment": "primary_pkg_md",      "source": "eurostat",    "category": "general",   "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://ec.europa.eu/eurostat/databrowser/view/sts_inppd_m",      "period_type": "monthly", "country_iso": None},
    {"id": "prim_ppi_us_bls",  "name": "PPI — US BLS",                  "segment": "primary_pkg_md",      "source": "bls",         "category": "general",   "unit": "index",     "base_year": "1982=100", "paid_source": 0, "source_url": "https://www.bls.gov/ppi/",                                          "period_type": "monthly", "country_iso": "US"},
    {"id": "prim_lci_eu",      "name": "LCI Labour Cost — EU",           "segment": "primary_pkg_md",      "source": "eurostat",    "category": "labor",     "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://ec.europa.eu/eurostat/databrowser/view/lc_lci_lev",       "period_type": "monthly", "country_iso": None},
    {"id": "prim_hicp_medical","name": "HICP Medical — EU",              "segment": "primary_pkg_md",      "source": "eurostat",    "category": "general",   "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://ec.europa.eu/eurostat/databrowser/view/prc_hicp_midx",    "period_type": "monthly", "country_iso": None},
    {"id": "prim_ppi_tubes",   "name": "PPI Tubes (FRED)",               "segment": "primary_pkg_md",      "source": "manual",      "category": "general",   "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://fred.stlouisfed.org/series/PCU3321103321105",               "period_type": "monthly", "country_iso": "US"},
    {"id": "prim_eex_energy",  "name": "EEX French Power Futures",       "segment": "primary_pkg_md",      "source": "manual",      "category": "energy",    "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://www.eex.com/en/market-data/power/futures",                  "period_type": "monthly", "country_iso": None},
    {"id": "prim_peg_gas",     "name": "PEG Natural Gas",                "segment": "primary_pkg_md",      "source": "manual",      "category": "energy",    "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://www.powernext.com/spot-market-data",                       "period_type": "monthly", "country_iso": None},
    {"id": "prim_ppi_int_bls", "name": "PPI Intermediate — BLS",        "segment": "primary_pkg_md",      "source": "bls",         "category": "general",   "unit": "index",     "base_year": "1982=100", "paid_source": 0, "source_url": "https://www.bls.gov/ppi/",                                          "period_type": "monthly", "country_iso": "US"},
    {"id": "prim_cpi_us",      "name": "CPI — US Consumer Price",        "segment": "primary_pkg_md",      "source": "bls",         "category": "general",   "unit": "index",     "base_year": "1982=100", "paid_source": 0, "source_url": "https://www.bls.gov/cpi/",                                          "period_type": "monthly", "country_iso": "US"},
    {"id": "prim_ppi_aptar",   "name": "PPI Intermediate — APTAR",      "segment": "primary_pkg_md",      "source": "bls",         "category": "plastics",  "unit": "index",     "base_year": "1982=100", "paid_source": 0, "source_url": "https://www.bls.gov/ppi/",                                          "period_type": "monthly", "country_iso": "US"},
    {"id": "prim_cpi_g20",     "name": "CPI G20",                        "segment": "primary_pkg_md",      "source": "eurostat",    "category": "general",   "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://data.oecd.org/price/inflation-cpi.htm",                     "period_type": "monthly", "country_iso": None},
    {"id": "prim_ppi_union",   "name": "PPI Intermediate — Union Plastic","segment": "primary_pkg_md",     "source": "bls",         "category": "plastics",  "unit": "index",     "base_year": "1982=100", "paid_source": 0, "source_url": "https://www.bls.gov/ppi/",                                          "period_type": "monthly", "country_iso": "US"},
    {"id": "prim_bopet",       "name": "BOPET — Biax. PET",             "segment": "primary_pkg_md",      "source": "manual",      "category": "plastics",  "unit": "EUR/tonne", "base_year": "2015=100", "paid_source": 1, "source_url": None,                                                                "period_type": "monthly", "country_iso": None},
    {"id": "prim_ldpe",        "name": "LDPE — Low-Density Polyethylene","segment": "primary_pkg_md",      "source": "manual",      "category": "plastics",  "unit": "EUR/tonne", "base_year": "2015=100", "paid_source": 1, "source_url": None,                                                                "period_type": "monthly", "country_iso": None},
    # API & Chemicals (4)
    {"id": "api_ppi_pharma",   "name": "PPI Pharma Mfg — BLS PCU325412","segment": "api_chemicals",       "source": "bls",         "category": "chemicals", "unit": "index",     "base_year": "2012=100", "paid_source": 0, "source_url": "https://www.bls.gov/ppi/",                                          "period_type": "monthly", "country_iso": "US"},
    {"id": "api_eurostat",     "name": "Eurostat PPI & CPI",             "segment": "api_chemicals",       "source": "eurostat",    "category": "chemicals", "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://ec.europa.eu/eurostat/databrowser/view/sts_inppd_m",      "period_type": "monthly", "country_iso": None},
    {"id": "api_india_labor",  "name": "India Labor & Inflation (MoLE)", "segment": "api_chemicals",       "source": "manual",      "category": "labor",     "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://labourbureau.gov.in/",                                      "period_type": "monthly", "country_iso": "IN"},
    {"id": "api_salary_expl",  "name": "Salary Explorer — Avg Labor Cost","segment": "api_chemicals",      "source": "manual",      "category": "labor",     "unit": "index",     "base_year": "2015=100", "paid_source": 0, "source_url": "https://www.salaryexplorer.com/",                                   "period_type": "monthly", "country_iso": None},
]

# ── Regional seed data — 52 new indices ──────────────────────────────────────
_EU_SOURCE_PPI   = "https://ec.europa.eu/eurostat/databrowser/view/sts_inppd_m"
_EU_SOURCE_CPI   = "https://ec.europa.eu/eurostat/databrowser/view/prc_hicp_midx"
_EU_SOURCE_LCI   = "https://ec.europa.eu/eurostat/databrowser/view/lc_lci_lev"
_EU_SOURCE_NRG   = "https://ec.europa.eu/eurostat/databrowser/view/nrg_pc_205"
_FSO_PPI         = "https://www.pxweb.bfs.admin.ch/pxweb/en/px-x-0602020000_105"
_FSO_CPI         = "https://www.pxweb.bfs.admin.ch/pxweb/en/px-x-0602020000_101"
_FSO_LCI         = "https://www.pxweb.bfs.admin.ch/pxweb/en/px-x-0302010000_101"
_IEA_NRG         = "https://www.iea.org/data-and-statistics"
_CBS_PPI         = "https://api.cbs.gov.il/v1/OData/13798/TypedDataSet"
_CBS_CPI         = "https://api.cbs.gov.il/v1/OData/78420/TypedDataSet"
_CBS_LCI         = "https://api.cbs.gov.il/v1/OData/14/TypedDataSet"
_CBS_NRG         = "https://api.cbs.gov.il/v1/OData"

_EU_COUNTRIES = [
    ("de", "Germany", "DE"), ("fr", "France", "FR"), ("it", "Italy", "IT"),
    ("es", "Spain", "ES"), ("nl", "Netherlands", "NL"), ("be", "Belgium", "BE"),
    ("se", "Sweden", "SE"), ("pl", "Poland", "PL"), ("at", "Austria", "AT"),
    ("pt", "Portugal", "PT"), ("cz", "Czechia", "CZ"),
]

SEED_REGIONAL = []

# PPI — monthly
for iso_lower, country_name, iso_upper in _EU_COUNTRIES:
    SEED_REGIONAL.append({
        "id": f"reg_ppi_{iso_lower}", "name": f"PPI — {country_name}",
        "segment": "regional", "source": "eurostat", "category": "producer_prices",
        "unit": "index", "base_year": "2015=100", "paid_source": 0,
        "source_url": _EU_SOURCE_PPI, "period_type": "monthly", "country_iso": iso_upper,
    })
SEED_REGIONAL.append({"id": "reg_ppi_ch", "name": "PPI — Switzerland",
    "segment": "regional", "source": "fso", "category": "producer_prices",
    "unit": "index", "base_year": "2015=100", "paid_source": 0,
    "source_url": _FSO_PPI, "period_type": "monthly", "country_iso": "CH"})
SEED_REGIONAL.append({"id": "reg_ppi_il", "name": "PPI — Israel",
    "segment": "regional", "source": "cbs_israel", "category": "producer_prices",
    "unit": "index", "base_year": "2015=100", "paid_source": 0,
    "source_url": _CBS_PPI, "period_type": "monthly", "country_iso": "IL"})

# CPI — monthly
for iso_lower, country_name, iso_upper in _EU_COUNTRIES:
    SEED_REGIONAL.append({
        "id": f"reg_cpi_{iso_lower}", "name": f"CPI — {country_name}",
        "segment": "regional", "source": "eurostat", "category": "consumer_prices",
        "unit": "index", "base_year": "2015=100", "paid_source": 0,
        "source_url": _EU_SOURCE_CPI, "period_type": "monthly", "country_iso": iso_upper,
    })
SEED_REGIONAL.append({"id": "reg_cpi_ch", "name": "CPI — Switzerland",
    "segment": "regional", "source": "fso", "category": "consumer_prices",
    "unit": "index", "base_year": "2020=100", "paid_source": 0,
    "source_url": _FSO_CPI, "period_type": "monthly", "country_iso": "CH"})
SEED_REGIONAL.append({"id": "reg_cpi_il", "name": "CPI — Israel",
    "segment": "regional", "source": "cbs_israel", "category": "consumer_prices",
    "unit": "index", "base_year": "2022=100", "paid_source": 0,
    "source_url": _CBS_CPI, "period_type": "monthly", "country_iso": "IL"})

# LCI — quarterly
for iso_lower, country_name, iso_upper in _EU_COUNTRIES:
    SEED_REGIONAL.append({
        "id": f"reg_lci_{iso_lower}", "name": f"LCI — {country_name}",
        "segment": "regional", "source": "eurostat", "category": "labor_cost",
        "unit": "index", "base_year": "2016=100", "paid_source": 0,
        "source_url": _EU_SOURCE_LCI, "period_type": "quarterly", "country_iso": iso_upper,
    })
SEED_REGIONAL.append({"id": "reg_lci_ch", "name": "LCI — Switzerland",
    "segment": "regional", "source": "fso", "category": "labor_cost",
    "unit": "index", "base_year": "2016=100", "paid_source": 0,
    "source_url": _FSO_LCI, "period_type": "quarterly", "country_iso": "CH"})
SEED_REGIONAL.append({"id": "reg_lci_il", "name": "LCI — Israel",
    "segment": "regional", "source": "cbs_israel", "category": "labor_cost",
    "unit": "index", "base_year": "2016=100", "paid_source": 0,
    "source_url": _CBS_LCI, "period_type": "quarterly", "country_iso": "IL"})

# Energy — semi-annual
for iso_lower, country_name, iso_upper in _EU_COUNTRIES:
    SEED_REGIONAL.append({
        "id": f"reg_energy_{iso_lower}", "name": f"Industrial Energy — {country_name}",
        "segment": "regional", "source": "eurostat", "category": "energy",
        "unit": "EUR/kWh", "base_year": "excl. tax", "paid_source": 0,
        "source_url": _EU_SOURCE_NRG, "period_type": "semi-annual", "country_iso": iso_upper,
    })
SEED_REGIONAL.append({"id": "reg_energy_ch", "name": "Industrial Energy — Switzerland",
    "segment": "regional", "source": "iea", "category": "energy",
    "unit": "EUR/kWh", "base_year": "excl. tax", "paid_source": 0,
    "source_url": _IEA_NRG, "period_type": "semi-annual", "country_iso": "CH"})
SEED_REGIONAL.append({"id": "reg_energy_il", "name": "Industrial Energy — Israel",
    "segment": "regional", "source": "cbs_israel", "category": "energy",
    "unit": "EUR/kWh", "base_year": "excl. tax", "paid_source": 0,
    "source_url": _CBS_NRG, "period_type": "semi-annual", "country_iso": "IL"})


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


def add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, definition: str):
    """Add a column to a table only if it doesn't exist yet (SQLite-safe migration)."""
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {definition}")


def init_db():
    """Create tables, run migrations, and seed catalogue if empty."""
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
                active       INTEGER NOT NULL DEFAULT 1,
                source_url   TEXT,
                period_type  TEXT NOT NULL DEFAULT 'monthly',
                country_iso  TEXT
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

            CREATE TABLE IF NOT EXISTS fetch_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at       DATETIME NOT NULL,
                index_id     TEXT NOT NULL,
                status       TEXT NOT NULL,
                rows_added   INTEGER DEFAULT 0,
                rows_updated INTEGER DEFAULT 0,
                error_msg    TEXT,
                duration_ms  INTEGER
            );
        """)

        # Safe migrations for existing databases (e.g. Railway Volume)
        add_column_if_missing(conn, "indices", "source_url",  "TEXT")
        add_column_if_missing(conn, "indices", "period_type", "TEXT NOT NULL DEFAULT 'monthly'")
        add_column_if_missing(conn, "indices", "country_iso", "TEXT")

        # Seed original 35 indices if table is empty
        existing = conn.execute("SELECT COUNT(*) FROM indices").fetchone()[0]
        if existing == 0:
            _seed_original(conn)

        # Seed 52 regional indices (idempotent — uses INSERT OR IGNORE)
        _seed_regional(conn)

        # Migrate emn_labor_de to quarterly (Eurostat LCI source)
        conn.execute(
            "UPDATE indices SET period_type='quarterly', source_url=? WHERE id='emn_labor_de'",
            ("https://ec.europa.eu/eurostat/databrowser/view/lc_lci_lev",),
        )


def _seed_original(conn: sqlite3.Connection):
    """Insert the 35 original indices."""
    conn.executemany(
        """INSERT OR IGNORE INTO indices
           (id, name, segment, source, category, unit, base_year, paid_source,
            source_url, period_type, country_iso)
           VALUES (:id, :name, :segment, :source, :category, :unit, :base_year,
                   :paid_source, :source_url, :period_type, :country_iso)""",
        SEED_INDICES,
    )
    # Back-fill source_url for rows that exist but have NULL source_url
    for idx in SEED_INDICES:
        if idx["source_url"]:
            conn.execute(
                "UPDATE indices SET source_url=? WHERE id=? AND source_url IS NULL",
                (idx["source_url"], idx["id"]),
            )


def _seed_regional(conn: sqlite3.Connection):
    """Insert 52 regional indices (safe to call multiple times)."""
    conn.executemany(
        """INSERT OR IGNORE INTO indices
           (id, name, segment, source, category, unit, base_year, paid_source,
            source_url, period_type, country_iso)
           VALUES (:id, :name, :segment, :source, :category, :unit, :base_year,
                   :paid_source, :source_url, :period_type, :country_iso)""",
        SEED_REGIONAL,
    )


def upsert_index_value_full(conn: sqlite3.Connection, index_id: str, period: str, value: float) -> dict:
    """
    Full upsert with proper MoM/YoY computation.
    Returns {'action': 'added'|'updated'}.
    """
    def get_value(p: str) -> Optional[float]:
        r = conn.execute(
            "SELECT value FROM index_values WHERE index_id=? AND period=?",
            (index_id, p),
        ).fetchone()
        return r["value"] if r else None

    prior_month = _subtract_periods(period, 1)
    prior_year  = _subtract_periods(period, 12)

    prior_month_val = get_value(prior_month) if prior_month else None
    prior_year_val  = get_value(prior_year)  if prior_year  else None

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


# Keep backwards-compatible alias
upsert_index_value = upsert_index_value_full


def _subtract_periods(period: str, steps: int) -> Optional[str]:
    """
    Subtract N periods from a period string.
    Supports YYYY-MM (monthly), YYYY-QN (quarterly, step=quarters),
    and YYYY-SN (semi-annual, step=halves).
    """
    import re
    try:
        if re.match(r"\d{4}-\d{2}$", period):
            year, month = int(period[:4]), int(period[5:7])
            total = year * 12 + (month - 1) - steps
            y, m = divmod(total, 12)
            return f"{y:04d}-{m+1:02d}"
        elif re.match(r"\d{4}-Q[1-4]$", period):
            year, q = int(period[:4]), int(period[6])
            total = (year * 4 + (q - 1)) - steps
            y, qq = divmod(total, 4)
            return f"{y:04d}-Q{qq+1}"
        elif re.match(r"\d{4}-S[12]$", period):
            year, s = int(period[:4]), int(period[6])
            total = (year * 2 + (s - 1)) - steps
            y, ss = divmod(total, 2)
            return f"{y:04d}-S{ss+1}"
    except Exception:
        pass
    return None
