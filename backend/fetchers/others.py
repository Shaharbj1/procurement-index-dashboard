"""
fetchers/others.py — All non-Eurostat, non-BLS fetchers.
Each function: async def fetch_X() -> list[dict]
Rules: never raise, catch all exceptions, return [] with log on error.
Missing env var → return [] immediately (logged as 'skipped' by orchestrator).
"""
import os
import logging
import re
from typing import List, Dict, Optional

import httpx

logger = logging.getLogger(__name__)

_TIMEOUT = 60
_BACKOFF = [5, 25, 125]


async def _get(url: str, params: dict = None, headers: dict = None) -> Optional[httpx.Response]:
    import asyncio
    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
        for attempt, delay in enumerate(_BACKOFF):
            try:
                r = await client.get(url, params=params, headers=headers or {})
                r.raise_for_status()
                return r
            except Exception as exc:
                if attempt < len(_BACKOFF) - 1:
                    logger.warning("GET %s attempt %d failed: %s", url, attempt+1, exc)
                    await asyncio.sleep(delay)
                else:
                    logger.error("GET %s failed: %s", url, exc)
    return None


# ── SCB Sweden (PxWeb API) ────────────────────────────────────────────────────

async def fetch_scb_sweden() -> List[Dict]:
    """
    emn_aki_blue, emn_aki_white — SCB PxWeb AKI (labour cost index).
    emn_ppi_sweden is now fetched via Eurostat; AKI only here.
    """
    results: List[Dict] = []
    # SCB AKI tables — filter to last 60 time periods to avoid payload limit
    tasks = [
        (
            "https://api.scb.se/OV0104/v1/doris/en/ssd/AM/AM0110/AM0110A/AKIKvMan",
            {
                "query": [
                    {"code": "Tid", "selection": {"filter": "top", "values": ["60"]}}
                ],
                "response": {"format": "JSON"},
            },
            "emn_aki_blue",
        ),
        (
            "https://api.scb.se/OV0104/v1/doris/en/ssd/AM/AM0110/AM0110A/AKIKvTjm",
            {
                "query": [
                    {"code": "Tid", "selection": {"filter": "top", "values": ["60"]}}
                ],
                "response": {"format": "JSON"},
            },
            "emn_aki_white",
        ),
    ]
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        for url, payload, idx_id in tasks:
            try:
                r = await client.post(url, json=payload)
                r.raise_for_status()
                data = r.json()
                rows = _parse_scb(data, idx_id)
                logger.info("SCB %s: parsed %d rows", idx_id, len(rows))
                results.extend(rows)
            except Exception as exc:
                logger.error("SCB fetch for %s failed: %s", idx_id, exc)
    return results


def _parse_scb(data: dict, idx_id: str) -> List[Dict]:
    results = []
    try:
        keys = data.get("columns", [])
        time_col = next((i for i, k in enumerate(keys) if k.get("type") == "t"), None)
        val_col  = next((i for i, k in enumerate(keys) if k.get("type") == "c"), None)
        if time_col is None or val_col is None:
            return []
        for row in data.get("data", []):
            try:
                period_raw = row["key"][time_col]
                value_str  = row["values"][0]
                period = _scb_period(period_raw)
                if period and value_str not in ("..", ""):
                    results.append({"index_id": idx_id, "period": period, "value": float(value_str)})
            except Exception:
                pass
    except Exception as exc:
        logger.error("_parse_scb error: %s", exc)
    return results


def _scb_period(raw: str) -> Optional[str]:
    """Convert SCB period like '2023M01' or '2023Q1' to YYYY-MM or YYYY-QN."""
    m = re.match(r"(\d{4})M(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.match(r"(\d{4})Q(\d)", raw)
    if m:
        return f"{m.group(1)}-Q{m.group(2)}"
    return None


# ── Destatis GENESIS (Germany) ────────────────────────────────────────────────

async def fetch_destatis() -> List[Dict]:
    """emn_labor_de, emn_ppi_de — Destatis GENESIS-Online API."""
    user = os.getenv("DESTATIS_USER")
    pwd  = os.getenv("DESTATIS_PASS")
    if not user or not pwd:
        logger.info("DESTATIS_USER or DESTATIS_PASS not set — skipping Destatis fetch")
        return []

    results: List[Dict] = []
    base = "https://www-genesis.destatis.de/genesisWS/rest/2020"
    pairs = [
        ("62321BJ001", "emn_ppi_de"),    # PPI Germany
        ("62461BJ001", "emn_labor_de"),  # Labour Cost Index Germany
    ]
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        for table, idx_id in pairs:
            try:
                r = await client.get(
                    f"{base}/data/tablefile",
                    params={"username": user, "password": pwd, "name": table,
                            "area": "all", "compress": "false", "transpose": "false",
                            "startyear": "2015", "format": "ffcsv"},
                )
                r.raise_for_status()
                rows = _parse_destatis_csv(r.text, idx_id)
                results.extend(rows)
            except Exception as exc:
                logger.error("Destatis fetch for %s failed: %s", idx_id, exc)
    return results


def _parse_destatis_csv(text: str, idx_id: str) -> List[Dict]:
    results = []
    try:
        lines = text.strip().splitlines()
        for line in lines[1:]:
            parts = line.split(";")
            if len(parts) < 2:
                continue
            try:
                period_raw = parts[0].strip().strip('"')
                value_raw  = parts[-1].strip().strip('"').replace(",", ".")
                period = _destatis_period(period_raw)
                if period and value_raw not in ("", ".", "-"):
                    results.append({"index_id": idx_id, "period": period, "value": float(value_raw)})
            except Exception:
                pass
    except Exception as exc:
        logger.error("_parse_destatis_csv error: %s", exc)
    return results


def _destatis_period(raw: str) -> Optional[str]:
    m = re.match(r"(\d{4}) (\w{3})", raw)
    months = {"Jan":"01","Feb":"02","Mär":"03","Mar":"03","Apr":"04","Mai":"05","May":"05",
              "Jun":"06","Jul":"07","Aug":"08","Sep":"09","Okt":"10","Oct":"10","Nov":"11","Dez":"12","Dec":"12"}
    if m:
        mon = months.get(m.group(2))
        if mon:
            return f"{m.group(1)}-{mon}"
    return None


# ── ISTAT Italy ───────────────────────────────────────────────────────────────

async def fetch_istat_italy() -> List[Dict]:
    """emn_cpi_italy — ISTAT SDMX REST API."""
    url = "https://sdmx.istat.it/SDMXWS/rest/data/IT1,DF_DCSP_NICINDAGG,1.0/M.CPI.IT...../"
    r = await _get(url, headers={"Accept": "application/json"})
    if not r:
        return []
    try:
        data = r.json()
        # Parse similar to Eurostat SDMX
        obs = data.get("dataSets", [{}])[0].get("observations", {})
        time_vals = list(data["structure"]["dimensions"]["observation"][0]["values"])
        results = []
        for k, vals in obs.items():
            t_idx = int(k.split(":")[0])
            if t_idx < len(time_vals) and vals[0] is not None:
                period_raw = time_vals[t_idx]["id"]
                m = re.match(r"(\d{4})-(\d{2})", period_raw)
                if m:
                    results.append({"index_id": "emn_cpi_italy",
                                    "period": f"{m.group(1)}-{m.group(2)}",
                                    "value": float(vals[0])})
        return results
    except Exception as exc:
        logger.error("ISTAT parse error: %s", exc)
        return []


# ── ONS UK ────────────────────────────────────────────────────────────────────

async def fetch_ons_uk() -> List[Dict]:
    """emn_cpi_uk — UK ONS CPI All Items (D7G7, MM23 dataset)."""
    # D7G7 = CPI All Items Index (2015=100) from MM23 dataset
    url = "https://api.ons.gov.uk/v1/timeseries/D7G7/dataset/MM23/data"
    r = await _get(url)
    if not r:
        return []
    try:
        data = r.json()
        results = []
        _month_map = {
            "jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
            "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12",
        }
        for m in data.get("months", []):
            try:
                year  = str(m.get("year", "")).strip()
                month = str(m.get("month", "")).strip()
                # ONS month can be "01" (numeric) or "Jan" (abbreviated)
                if month.isdigit():
                    mo = month.zfill(2)
                else:
                    mo = _month_map.get(month[:3].lower(), "")
                if year and mo:
                    results.append({"index_id": "emn_cpi_uk",
                                    "period": f"{year}-{mo}",
                                    "value": float(m["value"])})
            except Exception:
                pass
        logger.info("ONS UK: parsed %d rows", len(results))
        return results
    except Exception as exc:
        logger.error("ONS parse error: %s", exc)
        return []


# ── FRED API ──────────────────────────────────────────────────────────────────

async def fetch_fred() -> List[Dict]:
    """prim_ppi_tubes — FRED API (requires FRED_API_KEY)."""
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        logger.info("FRED_API_KEY not set — skipping FRED fetch")
        return []

    series_map = {"PCU3321103321105": "prim_ppi_tubes"}
    results = []
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        for series_id, idx_id in series_map.items():
            try:
                r = await client.get(
                    "https://api.stlouisfed.org/fred/series/observations",
                    params={"series_id": series_id, "api_key": api_key,
                            "file_type": "json", "observation_start": "2015-01-01"},
                )
                r.raise_for_status()
                data = r.json()
                for obs in data.get("observations", []):
                    try:
                        val = obs["value"]
                        if val == ".":
                            continue
                        period = obs["date"][:7]   # YYYY-MM
                        results.append({"index_id": idx_id, "period": period, "value": float(val)})
                    except Exception:
                        pass
            except Exception as exc:
                logger.error("FRED fetch for %s failed: %s", series_id, exc)
    return results


# ── OECD API ──────────────────────────────────────────────────────────────────

async def fetch_oecd() -> List[Dict]:
    """
    prim_cpi_g20, log_cpi_general — OECD Data Explorer API v2.
    Uses the new sdmx.oecd.org endpoint (stats.oecd.org was retired).
    """
    results = []
    # OECD SDMX v2 format: CPI index 2015=100, monthly
    tasks = [
        (
            "https://sdmx.oecd.org/v2/data/OECD.SDD.STES,DP_LIVE,/"
            "G20.CPI.TOT.IDX2015.M/all"
            "?startPeriod=2015-01&format=jsondata&dimensionAtObservation=AllDimensions",
            "prim_cpi_g20",
        ),
        (
            "https://sdmx.oecd.org/v2/data/OECD.SDD.STES,DP_LIVE,/"
            "OECD.CPI.TOT.IDX2015.M/all"
            "?startPeriod=2015-01&format=jsondata&dimensionAtObservation=AllDimensions",
            "log_cpi_general",
        ),
    ]
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        for url, idx_id in tasks:
            try:
                r = await client.get(url, headers={"Accept": "application/vnd.sdmx.data+json;version=2"})
                r.raise_for_status()
                data = r.json()
                rows = _parse_oecd_v2(data, idx_id)
                logger.info("OECD %s: parsed %d rows", idx_id, len(rows))
                results.extend(rows)
            except Exception as exc:
                logger.error("OECD fetch for %s failed: %s", idx_id, exc)
    return results


def _parse_oecd_v2(data: dict, idx_id: str) -> List[Dict]:
    """Parse OECD SDMX-JSON v2 (sdmx.oecd.org) format."""
    results = []
    try:
        datasets = data.get("data", {}).get("dataSets", data.get("dataSets", []))
        structure = data.get("data", {}).get("structures", data.get("structures", []))
        if not datasets or not structure:
            return []
        obs = datasets[0].get("observations", {})
        dims = structure[0].get("dimensions", {}).get("observation", [])
        # Find the TIME_PERIOD dimension
        time_dim = next((d for d in dims if d.get("id") in ("TIME_PERIOD", "time")), None)
        if not time_dim:
            return []
        time_values = {i: v.get("id","") for i, v in enumerate(time_dim.get("values", []))}
        for key, vals in obs.items():
            try:
                indices = [int(x) for x in key.split(":")]
                time_pos = dims.index(time_dim)
                t_idx = indices[time_pos]
                period_raw = time_values.get(t_idx, "")
                m = re.match(r"(\d{4})-(\d{2})", period_raw)
                if m and vals and vals[0] is not None:
                    results.append({"index_id": idx_id,
                                    "period": f"{m.group(1)}-{m.group(2)}",
                                    "value": float(vals[0])})
            except Exception:
                pass
    except Exception as exc:
        logger.error("_parse_oecd_v2 error for %s: %s", idx_id, exc)
    return results


def _parse_oecd(data: dict, idx_id: str) -> List[Dict]:
    results = []
    try:
        ds = data["dataSets"][0]["series"]
        time_periods = data["structure"]["dimensions"]["observation"][0]["values"]
        for _, series_data in ds.items():
            for t_str, obs in series_data.get("observations", {}).items():
                t_idx = int(t_str)
                period_raw = time_periods[t_idx]["id"]
                m = re.match(r"(\d{4})-(\d{2})", period_raw)
                if m and obs[0] is not None:
                    results.append({"index_id": idx_id,
                                    "period": f"{m.group(1)}-{m.group(2)}",
                                    "value": float(obs[0])})
    except Exception as exc:
        logger.error("_parse_oecd error for %s: %s", idx_id, exc)
    return results


# ── EEX / Powernext scrapers ──────────────────────────────────────────────────

async def fetch_eex_scrapers() -> List[Dict]:
    """
    prim_eex_energy, prim_peg_gas — HTML scrape (fragile).
    Falls back gracefully with log entry.
    """
    results = []
    try:
        from bs4 import BeautifulSoup
        async with httpx.AsyncClient(timeout=30, follow_redirects=True,
                                     headers={"User-Agent": "Mozilla/5.0"}) as client:
            # EEX publishes market data via their website; try public download endpoint
            # This is fragile — log a warning and return empty if structure changes
            r = await client.get("https://www.eex.com/en/market-data/power/futures")
            if r.status_code == 200:
                logger.info("EEX page fetched (%d bytes) — scrape logic needed for production", len(r.text))
            # Returning empty — actual scrape logic requires parsing EEX's dynamic table
            # which needs Selenium or their data API subscription
    except Exception as exc:
        logger.warning("EEX scraper failed (expected if no paid access): %s", exc)
    return results


# ── INE Chile ─────────────────────────────────────────────────────────────────

async def fetch_ine_chile() -> List[Dict]:
    """log_transport_ine — INE Chile Transport Cost Index JSON API."""
    url = "https://stat.ine.cl/Index.aspx/Producto/series/P/38"
    # INE Chile publishes data via a REST-like JSON endpoint
    api_url = "https://api.ine.cl/v2/CatalogoIndicadores/datos/38/8/M"
    r = await _get(api_url)
    if not r:
        return []
    try:
        data = r.json()
        results = []
        for item in data.get("Series", [{}])[0].get("Datos", []):
            try:
                period_raw = item.get("Periodo", "")
                value      = float(item.get("Valor", 0))
                # Period format: "202301" → "2023-01"
                if re.match(r"\d{6}$", period_raw):
                    period = f"{period_raw[:4]}-{period_raw[4:6]}"
                    results.append({"index_id": "log_transport_ine", "period": period, "value": value})
            except Exception:
                pass
        return results
    except Exception as exc:
        logger.error("INE Chile parse error: %s", exc)
        return []


# ── India Labour Bureau ───────────────────────────────────────────────────────

async def fetch_india_labour() -> List[Dict]:
    """api_india_labor — India Labour Bureau CPI-IW (CSV download)."""
    url = "https://labourbureau.gov.in/wp-content/uploads/2024/10/CPI-IW.pdf"
    # PDF download is not easily parseable without pdfplumber; skip and log
    logger.info("api_india_labor: PDF source requires pdfplumber — returning empty. Manual upload recommended.")
    return []


# ── Salary Explorer ───────────────────────────────────────────────────────────

async def fetch_salary_explorer() -> List[Dict]:
    """api_salary_expl — Salary Explorer annual scrape (January only)."""
    import datetime
    if datetime.datetime.utcnow().month != 1:
        logger.info("api_salary_expl: annual-only, skipping non-January run")
        return []
    # Salary Explorer is HTML-only, no public API. Returning empty with log.
    logger.info("api_salary_expl: HTML scrape not automated — manual upload recommended")
    return []


# ── FSO Switzerland Regional ──────────────────────────────────────────────────

async def fetch_switzerland_regional() -> List[Dict]:
    """
    reg_ppi_ch, reg_cpi_ch, reg_lci_ch — FSO PxWeb API.
    reg_energy_ch — IEA API (requires IEA_API_KEY; skipped if absent).
    """
    results = []

    fso_tasks = [
        ("https://www.pxweb.bfs.admin.ch/api/v1/de/px-x-0602020000_105",
         "reg_ppi_ch", "monthly"),
        ("https://www.pxweb.bfs.admin.ch/api/v1/de/px-x-0602020000_101",
         "reg_cpi_ch", "monthly"),
        ("https://www.pxweb.bfs.admin.ch/api/v1/de/px-x-0302010000_101",
         "reg_lci_ch", "quarterly"),
    ]

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        for base_url, idx_id, period_type in fso_tasks:
            try:
                # First get table metadata to know the correct table name
                r = await client.get(base_url)
                if r.status_code != 200:
                    logger.warning("FSO %s returned %d", idx_id, r.status_code)
                    continue
                # PxWeb returns JSON with table list; use first entry
                tables = r.json()
                if not tables:
                    continue
                table_id = tables[0].get("id", "")
                if not table_id:
                    continue
                r2 = await client.post(
                    f"{base_url}/{table_id}",
                    json={"query": [], "response": {"format": "JSON"}},
                )
                if r2.status_code != 200:
                    continue
                rows = _parse_scb_fso(r2.json(), idx_id)
                results.extend(rows)
            except Exception as exc:
                logger.error("FSO fetch for %s failed: %s", idx_id, exc)

    # IEA for reg_energy_ch
    iea_key = os.getenv("IEA_API_KEY")
    if not iea_key:
        logger.info("IEA_API_KEY not set — skipping reg_energy_ch (will be logged as skipped)")
    else:
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                r = await client.get(
                    "https://api.iea.org/v1/data",
                    params={"countries": "CHE", "products": "electricity",
                            "flows": "industry", "apikey": iea_key},
                    headers={"Accept": "application/json"},
                )
                r.raise_for_status()
                data = r.json()
                for item in data.get("data", []):
                    try:
                        year  = str(item.get("year", ""))
                        half  = item.get("period", "S1")
                        value = float(item.get("value", 0))
                        if year and value:
                            results.append({"index_id": "reg_energy_ch",
                                           "period": f"{year}-{half}",
                                           "value": value})
                    except Exception:
                        pass
        except Exception as exc:
            logger.error("IEA fetch for reg_energy_ch failed: %s", exc)

    return results


def _parse_scb_fso(data: dict, idx_id: str) -> List[Dict]:
    """Parse PxWeb JSON-stat2 format (FSO uses same as SCB)."""
    results = []
    try:
        for row in data.get("data", []):
            try:
                key   = row.get("key", [])
                vals  = row.get("values", [])
                if not key or not vals or vals[0] in ("..", "", None):
                    continue
                period_raw = key[-1]  # Last key dimension is time
                period = _scb_period(period_raw)
                if period:
                    results.append({"index_id": idx_id, "period": period, "value": float(vals[0])})
            except Exception:
                pass
    except Exception as exc:
        logger.error("_parse_scb_fso error: %s", exc)
    return results


def _scb_period(raw: str) -> Optional[str]:
    m = re.match(r"(\d{4})M(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.match(r"(\d{4})Q(\d)", raw)
    if m:
        return f"{m.group(1)}-Q{m.group(2)}"
    m = re.match(r"(\d{4})-(\d{2})$", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None


# ── CBS Israel Regional ────────────────────────────────────────────────────────

async def fetch_israel_regional() -> List[Dict]:
    """
    reg_ppi_il, reg_cpi_il, reg_lci_il, reg_energy_il — CBS Israel OData API.
    No API key required.
    """
    results = []

    cbs_tasks = [
        ("https://api.cbs.gov.il/v1/OData/13798/TypedDataSet",  "reg_ppi_il",    "monthly"),
        ("https://api.cbs.gov.il/v1/OData/78420/TypedDataSet",  "reg_cpi_il",    "monthly"),
        ("https://api.cbs.gov.il/v1/OData/14/TypedDataSet",     "reg_lci_il",    "quarterly"),
    ]

    async with httpx.AsyncClient(timeout=_TIMEOUT,
                                  headers={"Accept": "application/json"}) as client:
        for url, idx_id, period_type in cbs_tasks:
            try:
                r = await client.get(url, params={"$top": 200, "$orderby": "Period desc"})
                r.raise_for_status()
                data = r.json()
                for item in data.get("value", []):
                    try:
                        period_raw = str(item.get("Period", "") or item.get("Year", ""))
                        value      = float(item.get("DataValue", 0) or item.get("Value", 0))
                        period = _cbs_period(period_raw, period_type)
                        if period and value:
                            results.append({"index_id": idx_id, "period": period, "value": value})
                    except Exception:
                        pass
            except Exception as exc:
                logger.error("CBS Israel fetch for %s failed: %s", idx_id, exc)

    # Energy: CBS Industrial Electricity
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT,
                                      headers={"Accept": "application/json"}) as client:
            r = await client.get(
                "https://api.cbs.gov.il/v1/OData/14116/TypedDataSet",
                params={"$top": 100},
            )
            r.raise_for_status()
            data = r.json()
            for item in data.get("value", []):
                try:
                    year = str(item.get("Year", ""))
                    half = "S1" if int(item.get("Period", 1)) <= 6 else "S2"
                    value = float(item.get("DataValue", 0) or 0)
                    if year and value:
                        results.append({"index_id": "reg_energy_il",
                                        "period": f"{year}-{half}", "value": value})
                except Exception:
                    pass
    except Exception as exc:
        logger.error("CBS Israel energy fetch failed: %s", exc)

    return results


def _cbs_period(raw: str, period_type: str) -> Optional[str]:
    """Convert CBS Israel period string to standard format."""
    # Monthly: YYYYMM or YYYY-MM
    m = re.match(r"(\d{4})(\d{2})$", raw)
    if m and period_type == "monthly":
        return f"{m.group(1)}-{m.group(2)}"
    m = re.match(r"(\d{4})-(\d{2})$", raw)
    if m and period_type == "monthly":
        return f"{m.group(1)}-{m.group(2)}"
    # Quarterly: YYYYQ1 or 2023Q1
    m = re.match(r"(\d{4})Q(\d)$", raw)
    if m:
        return f"{m.group(1)}-Q{m.group(2)}"
    # Year only → Q1 as default
    m = re.match(r"(\d{4})$", raw)
    if m:
        return f"{m.group(1)}-01" if period_type == "monthly" else None
    return None
