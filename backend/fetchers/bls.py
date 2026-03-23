"""
fetchers/bls.py — US BLS Public Data API v2 fetcher.
Fetches all BLS-sourced indices in one batch request.
Never raises — returns [] on any error.
"""
import os
import logging
from typing import List, Dict

import httpx

logger = logging.getLogger(__name__)

_BLS_API = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
_TIMEOUT  = 60
_MAX_RETRIES = 3
_BACKOFF  = [5, 25, 125]

# BLS series → our index_id
BLS_SERIES = {
    "PCU31-33--31-33--":  "prim_ppi_eu",       # proxy — EU PPI not from BLS; skip
    "WPS00000000":        "prim_ppi_us_bls",    # PPI finished goods
    "EIUIR000000000000000": "prim_ppi_int_bls", # PPI intermediate
    "CUUR0000SA0":        "prim_cpi_us",        # CPI all urban consumers
    "PCU325412325412":    "api_ppi_pharma",     # PPI pharma mfg
    # Plastics
    "PCU3261--3261--":    "prim_ppi_aptar",     # PPI plastics products (APTAR proxy)
    "PCU3261--3261--1":   "prim_ppi_union",     # PPI plastics (Union proxy)
}

# Remove ones that don't map to BLS
VALID_BLS_SERIES = {k: v for k, v in BLS_SERIES.items()
                    if v not in ("prim_ppi_eu",)}  # prim_ppi_eu comes from Eurostat

# Confirmed BLS series IDs (verified format)
REAL_BLS_SERIES = {
    "WPSFD4111":       "prim_ppi_us_bls",    # PPI Finished Goods (replaces invalid WPS00000000)
    "WPUID61":         "prim_ppi_int_bls",   # PPI Intermediate Materials (replaces invalid EIUIR)
    "CUUR0000SA0":     "prim_cpi_us",         # CPI All Urban Consumers
    "PCU325412325412": "api_ppi_pharma",      # PPI Pharmaceutical Manufacturing
    "PCU3261--3261--": "prim_ppi_aptar",      # PPI Plastics product mfg (Aptar proxy)
    "WPU00000000":     "prim_ppi_union",      # PPI All Commodities (Union proxy; PCU3261--3261--1 invalid)
}


async def fetch_bls() -> List[Dict]:
    """Fetch all BLS indices in one API call. Returns list of {index_id, period, value}."""
    import asyncio

    api_key = os.getenv("BLS_API_KEY", "")
    series_ids = list(REAL_BLS_SERIES.keys())

    payload = {
        "seriesid": series_ids,
        "startyear": "2015",
        "endyear": "2026",
    }
    if api_key:
        payload["registrationkey"] = api_key

    results: List[Dict] = []

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        for attempt, delay in enumerate(_BACKOFF):
            try:
                r = await client.post(_BLS_API, json=payload)
                r.raise_for_status()
                data = r.json()

                if data.get("status") != "REQUEST_SUCCEEDED":
                    logger.warning("BLS API status: %s — %s", data.get("status"), data.get("message"))

                for series_obj in data.get("Results", {}).get("series", []):
                    sid = series_obj.get("seriesID", "")
                    idx_id = REAL_BLS_SERIES.get(sid)
                    if not idx_id:
                        continue
                    for dp in series_obj.get("data", []):
                        try:
                            year  = dp["year"]
                            period = dp["period"]   # e.g. "M01"
                            value  = float(dp["value"])
                            if not period.startswith("M"):
                                continue
                            month = period[1:].lstrip("0") or "0"
                            period_str = f"{year}-{int(month):02d}"
                            results.append({"index_id": idx_id, "period": period_str, "value": value})
                        except Exception:
                            pass
                break

            except Exception as exc:
                if attempt < len(_BACKOFF) - 1:
                    logger.warning("BLS attempt %d failed: %s — retrying in %ds", attempt+1, exc, delay)
                    await asyncio.sleep(delay)
                else:
                    logger.error("BLS fetch failed after %d attempts: %s", _MAX_RETRIES, exc)

    return results
