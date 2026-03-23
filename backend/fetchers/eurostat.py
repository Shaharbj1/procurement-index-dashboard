"""
fetchers/eurostat.py — Eurostat SDMX-JSON fetchers.
All functions return list[dict] with keys: index_id, period, value.
Never raise exceptions — catch, log, return [].
"""
import logging
import re
from typing import List, Dict, Optional

import httpx

logger = logging.getLogger(__name__)

_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
_TIMEOUT = 60
_MAX_RETRIES = 3
_BACKOFF = [5, 25, 125]

EU_GEOS = "DE+FR+IT+ES+NL+BE+SE+PL+AT+PT+CZ"

# Map Eurostat geo code → our country_iso for reg_ IDs
_GEO_TO_ISO = {
    "DE": "de", "FR": "fr", "IT": "it", "ES": "es", "NL": "nl",
    "BE": "be", "SE": "se", "PL": "pl", "AT": "at", "PT": "pt", "CZ": "cz",
}


async def _get_json(url: str, params: dict) -> Optional[dict]:
    """GET with retry + exponential backoff. Returns None on failure."""
    import asyncio
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        for attempt, delay in enumerate(_BACKOFF):
            try:
                r = await client.get(url, params=params)
                r.raise_for_status()
                return r.json()
            except Exception as exc:
                if attempt < len(_BACKOFF) - 1:
                    logger.warning("Eurostat GET attempt %d failed: %s — retrying in %ds", attempt+1, exc, delay)
                    await asyncio.sleep(delay)
                else:
                    logger.error("Eurostat GET failed after %d attempts: %s", _MAX_RETRIES, exc)
    return None


def _parse_sdmx_single(data: dict, index_id: str, period_prefix: str = "") -> List[Dict]:
    """Parse Eurostat SDMX-JSON for a single-geo response."""
    results = []
    try:
        dim = data["dimension"]
        time_dim = dim.get("time", {})
        values_dim = data.get("value", {})
        time_labels = list(time_dim.get("category", {}).get("index", {}).keys())

        for str_idx, value in values_dim.items():
            t_idx = int(str_idx)
            if t_idx < len(time_labels):
                period_raw = time_labels[t_idx]
                period = _normalize_period(period_raw, period_prefix)
                if period and value is not None:
                    results.append({"index_id": index_id, "period": period, "value": float(value)})
    except Exception as exc:
        logger.error("_parse_sdmx_single error for %s: %s", index_id, exc)
    return results


def _parse_sdmx_multi_geo(data: dict, id_prefix: str, type_suffix: str) -> List[Dict]:
    """
    Parse Eurostat SDMX-JSON for multi-geo response.
    Returns rows with index_id = f'reg_{type_suffix}_{geo_lower}'.
    """
    results = []
    try:
        dim = data["dimension"]
        geo_dim  = dim.get("geo", {})
        time_dim = dim.get("time", {})
        geo_labels  = list(geo_dim.get("category", {}).get("index", {}).keys())   # e.g. ['DE','FR',...]
        time_labels = list(time_dim.get("category", {}).get("index", {}).keys())  # e.g. ['2010-01',...]
        n_geo  = len(geo_labels)
        n_time = len(time_labels)

        for str_idx, value in data.get("value", {}).items():
            flat = int(str_idx)
            geo_i  = flat // n_time
            time_i = flat  % n_time
            if geo_i >= n_geo or time_i >= n_time or value is None:
                continue
            geo_code   = geo_labels[geo_i]
            period_raw = time_labels[time_i]
            iso_lower  = _GEO_TO_ISO.get(geo_code)
            if not iso_lower:
                continue
            period = _normalize_period(period_raw, type_suffix)
            if period:
                results.append({
                    "index_id": f"reg_{type_suffix}_{iso_lower}",
                    "period":   period,
                    "value":    float(value),
                })
    except Exception as exc:
        logger.error("_parse_sdmx_multi_geo error for %s: %s", type_suffix, exc)
    return results


def _normalize_period(raw: str, hint: str = "") -> Optional[str]:
    """Convert Eurostat period notation to our standard format."""
    raw = raw.strip()
    # YYYY-MM → keep as is
    if re.match(r"\d{4}-\d{2}$", raw):
        return raw
    # YYYY → monthly not applicable; skip unless quarterly
    # YYYY-QX or YYYY-QN
    m = re.match(r"(\d{4})-Q(\d)$", raw)
    if m:
        return f"{m.group(1)}-Q{m.group(2)}"
    # Eurostat semi-annual: YYYY-S1 / YYYY-S2
    m = re.match(r"(\d{4})-S(\d)$", raw)
    if m:
        return f"{m.group(1)}-S{m.group(2)}"
    # Eurostat energy uses YYYY-SXX notation (e.g. 2023-S1)
    return None


# ── Single-series fetchers ────────────────────────────────────────────────────

async def fetch_ppi_eu() -> List[Dict]:
    """prim_ppi_eu — EU PPI manufacturing monthly."""
    data = await _get_json(
        f"{_BASE}/sts_inppd_m",
        {"geo": "EU27_2020", "nace_r2": "MIG_ING", "s_adj": "NSA", "unit": "I15",
         "format": "JSON", "lang": "EN"},
    )
    if not data:
        return []
    return _parse_sdmx_single(data, "prim_ppi_eu")


async def fetch_lci_eu() -> List[Dict]:
    """prim_lci_eu — EU Labour Cost Index quarterly."""
    data = await _get_json(
        f"{_BASE}/lc_lci_lev",
        {"geo": "EU27_2020", "indic_lc": "LCI", "nace_r2": "B-N", "s_adj": "NSA",
         "unit": "I16", "format": "JSON", "lang": "EN"},
    )
    if not data:
        return []
    return _parse_sdmx_single(data, "prim_lci_eu")


async def fetch_hicp_eu() -> List[Dict]:
    """prim_hicp_medical + emn_cpi_italy + prim_cpi_g20 (approximated via Eurostat HICP)."""
    results = []
    pairs = [
        ("EU27_2020", "prim_hicp_medical"),
        ("IT",        "emn_cpi_italy"),
    ]
    for geo, idx_id in pairs:
        data = await _get_json(
            f"{_BASE}/prc_hicp_midx",
            {"geo": geo, "coicop": "CP00", "unit": "I15", "format": "JSON", "lang": "EN"},
        )
        if data:
            results.extend(_parse_sdmx_single(data, idx_id))
    return results


# ── Regional multi-country fetchers ───────────────────────────────────────────

async def fetch_regional_ppi() -> List[Dict]:
    """reg_ppi_{de|fr|it|es|nl|be|se|pl|at|pt|cz} — Eurostat PPI monthly."""
    data = await _get_json(
        f"{_BASE}/sts_inppd_m",
        {"geo": EU_GEOS, "nace_r2": "MIG_ING", "s_adj": "NSA", "unit": "I15",
         "format": "JSON", "lang": "EN"},
    )
    if not data:
        return []
    return _parse_sdmx_multi_geo(data, "reg", "ppi")


async def fetch_regional_cpi() -> List[Dict]:
    """reg_cpi_{de|fr|...} — Eurostat HICP monthly."""
    data = await _get_json(
        f"{_BASE}/prc_hicp_midx",
        {"geo": EU_GEOS, "coicop": "CP00", "unit": "I15",
         "format": "JSON", "lang": "EN"},
    )
    if not data:
        return []
    return _parse_sdmx_multi_geo(data, "reg", "cpi")


async def fetch_regional_lci() -> List[Dict]:
    """reg_lci_{de|fr|...} — Eurostat Labour Cost Index quarterly."""
    data = await _get_json(
        f"{_BASE}/lc_lci_lev",
        {"geo": EU_GEOS, "indic_lc": "LCI", "nace_r2": "B-N", "s_adj": "NSA",
         "unit": "I16", "format": "JSON", "lang": "EN"},
    )
    if not data:
        return []
    return _parse_sdmx_multi_geo(data, "reg", "lci")


async def fetch_regional_energy() -> List[Dict]:
    """reg_energy_{de|fr|...} — Eurostat industrial electricity price semi-annual."""
    data = await _get_json(
        f"{_BASE}/nrg_pc_205",
        {"geo": EU_GEOS, "unit": "KWH", "nrg_cons": "MWH20-499", "tax": "X_TAX",
         "format": "JSON", "lang": "EN"},
    )
    if not data:
        return []
    return _parse_sdmx_multi_geo(data, "reg", "energy")
