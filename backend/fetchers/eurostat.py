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

# Map Eurostat geo code → our country_iso (lower) for reg_ IDs
_GEO_TO_ISO = {
    "DE": "de", "FR": "fr", "IT": "it", "ES": "es", "NL": "nl",
    "BE": "be", "SE": "se", "PL": "pl", "AT": "at", "PT": "pt", "CZ": "cz",
}


async def _get_json(url: str, params: dict) -> Optional[dict]:
    """
    GET with retry + exponential backoff. Returns None on failure.
    Params values that contain '+' are split and sent as repeated query params
    (Eurostat requires geo=DE&geo=SE, NOT geo=DE%2BSE).
    """
    import asyncio
    # Build list-of-tuples so repeated params are handled correctly
    param_list = []
    for k, v in params.items():
        if isinstance(v, str) and "+" in v and k in ("geo",):
            for part in v.split("+"):
                param_list.append((k, part))
        else:
            param_list.append((k, v))

    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
        for attempt, delay in enumerate(_BACKOFF):
            try:
                r = await client.get(url, params=param_list)
                r.raise_for_status()
                return r.json()
            except Exception as exc:
                if attempt < len(_BACKOFF) - 1:
                    logger.warning("Eurostat GET attempt %d failed: %s — retrying in %ds", attempt+1, exc, delay)
                    await asyncio.sleep(delay)
                else:
                    logger.error("Eurostat GET failed after %d attempts: %s", _MAX_RETRIES, exc)
    return None


def _pos_to_label(category: dict) -> dict:
    """
    Build {integer_position: label_string} from SDMX category.index dict
    which is {label_string: integer_position}.
    """
    return {v: k for k, v in category.get("index", {}).items()}


def _parse_sdmx_single(data: dict, index_id: str) -> List[Dict]:
    """
    Parse Eurostat SDMX-JSON for a single-geo / single-filter response
    where all non-time dimensions have size 1.
    Uses position-based lookup to avoid dict-key-order assumptions.
    """
    results = []
    try:
        dim        = data.get("dimension", {})
        time_cat   = dim.get("time", {}).get("category", {})
        pos_to_period = _pos_to_label(time_cat)   # {0: "2024-11", 1: "2024-10", ...}

        for str_idx, value in data.get("value", {}).items():
            flat = int(str_idx)
            period_raw = pos_to_period.get(flat)
            if period_raw and value is not None:
                period = _normalize_period(period_raw)
                if period:
                    results.append({"index_id": index_id, "period": period, "value": float(value)})
    except Exception as exc:
        logger.error("_parse_sdmx_single error for %s: %s", index_id, exc)
    return results


def _parse_sdmx_multi_geo(data: dict, type_suffix: str) -> List[Dict]:
    """
    Parse Eurostat SDMX-JSON for multi-geo response.
    Flat index = geo_position * n_time + time_position.
    Returns rows with index_id = 'reg_{type_suffix}_{geo_lower}'.
    """
    results = []
    try:
        dim      = data.get("dimension", {})
        sizes    = data.get("size", [])
        dim_ids  = data.get("id", [])

        geo_pos  = next((i for i, d in enumerate(dim_ids) if d.lower() == "geo"),  -1)
        time_pos = next((i for i, d in enumerate(dim_ids) if d.lower() == "time"), -1)
        if geo_pos == -1 or time_pos == -1:
            logger.error("_parse_sdmx_multi_geo: cannot find geo/time in id list: %s", dim_ids)
            return []

        n_geo  = sizes[geo_pos]  if sizes and geo_pos  < len(sizes) else 1
        n_time = sizes[time_pos] if sizes and time_pos < len(sizes) else 1

        geo_cat    = dim.get("geo",  {}).get("category", {})
        time_cat   = dim.get("time", {}).get("category", {})
        pos_to_geo  = _pos_to_label(geo_cat)    # {0: "DE", 1: "FR", ...}
        pos_to_time = _pos_to_label(time_cat)   # {0: "2024-11", ...}

        # Stride: how many elements per unit of the geo dimension
        # For dims ordered [..., geo, time], stride_geo = n_time
        geo_stride  = 1
        for s in sizes[geo_pos + 1:]:
            geo_stride *= s
        time_stride = 1
        for s in sizes[time_pos + 1:]:
            time_stride *= s

        for str_idx, value in data.get("value", {}).items():
            if value is None:
                continue
            flat    = int(str_idx)
            geo_i   = (flat // geo_stride)  % n_geo
            time_i  = (flat // time_stride) % n_time

            geo_code   = pos_to_geo.get(geo_i)
            period_raw = pos_to_time.get(time_i)
            iso_lower  = _GEO_TO_ISO.get(geo_code) if geo_code else None

            if not iso_lower or not period_raw:
                continue
            period = _normalize_period(period_raw)
            if period:
                results.append({
                    "index_id": f"reg_{type_suffix}_{iso_lower}",
                    "period":   period,
                    "value":    float(value),
                })
    except Exception as exc:
        logger.error("_parse_sdmx_multi_geo error for %s: %s", type_suffix, exc)
    return results


def _normalize_period(raw: str) -> Optional[str]:
    """Convert Eurostat period notation to our standard format."""
    raw = raw.strip()
    if re.match(r"\d{4}-\d{2}$", raw):
        return raw
    m = re.match(r"(\d{4})-Q(\d)$", raw)
    if m:
        return f"{m.group(1)}-Q{m.group(2)}"
    m = re.match(r"(\d{4})-S(\d)$", raw)
    if m:
        return f"{m.group(1)}-S{m.group(2)}"
    return None


# ── Single-series fetchers ────────────────────────────────────────────────────

async def fetch_ppi_eu() -> List[Dict]:
    """prim_ppi_eu — EU PPI total industry monthly."""
    data = await _get_json(
        f"{_BASE}/sts_inppd_m",
        {"geo": "EU27_2020", "nace_r2": "B-E36", "s_adj": "NSA", "unit": "I15",
         "format": "JSON", "lang": "EN"},
    )
    if not data:
        return []
    return _parse_sdmx_single(data, "prim_ppi_eu")


async def fetch_lci_eu() -> List[Dict]:
    """prim_lci_eu — EU Labour Cost Index quarterly (lc_lci_r2_q, 2020=100)."""
    data = await _get_json(
        f"{_BASE}/lc_lci_r2_q",
        {"geo": "EU27_2020", "nace_r2": "B-N", "s_adj": "NSA",
         "unit": "I20", "format": "JSON", "lang": "EN"},
    )
    if not data:
        return []
    return _parse_sdmx_single(data, "prim_lci_eu")


async def fetch_hicp_eu() -> List[Dict]:
    """prim_hicp_medical + emn_cpi_italy — Eurostat HICP."""
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


async def fetch_emn_de() -> List[Dict]:
    """
    emn_ppi_de — Germany PPI monthly (Eurostat sts_inppd_m).
    emn_labor_de — Germany Labour Cost Index quarterly (Eurostat lc_lci_lev).
    """
    results = []

    # emn_ppi_de — monthly
    data_ppi = await _get_json(
        f"{_BASE}/sts_inppd_m",
        {"geo": "DE", "nace_r2": "B-E36", "s_adj": "NSA", "unit": "I15",
         "format": "JSON", "lang": "EN"},
    )
    if data_ppi:
        results.extend(_parse_sdmx_single(data_ppi, "emn_ppi_de"))

    # emn_labor_de — quarterly (Eurostat LCI lc_lci_r2_q, base 2020=100)
    data_lci = await _get_json(
        f"{_BASE}/lc_lci_r2_q",
        {"geo": "DE", "nace_r2": "B-N", "s_adj": "NSA",
         "unit": "I20", "format": "JSON", "lang": "EN"},
    )
    if data_lci:
        results.extend(_parse_sdmx_single(data_lci, "emn_labor_de"))

    return results


# ── Regional multi-country fetchers ───────────────────────────────────────────

async def fetch_regional_ppi() -> List[Dict]:
    """reg_ppi_{de|fr|it|es|nl|be|se|pl|at|pt|cz} — Eurostat PPI monthly."""
    data = await _get_json(
        f"{_BASE}/sts_inppd_m",
        {"geo": EU_GEOS, "nace_r2": "B-E36", "s_adj": "NSA", "unit": "I15",
         "sinceTimePeriod": "2015-01", "format": "JSON", "lang": "EN"},
    )
    if not data:
        return []
    return _parse_sdmx_multi_geo(data, "ppi")


async def fetch_regional_cpi() -> List[Dict]:
    """reg_cpi_{de|fr|...} — Eurostat HICP monthly."""
    data = await _get_json(
        f"{_BASE}/prc_hicp_midx",
        {"geo": EU_GEOS, "coicop": "CP00", "unit": "I15",
         "sinceTimePeriod": "2015-01", "format": "JSON", "lang": "EN"},
    )
    if not data:
        return []
    return _parse_sdmx_multi_geo(data, "cpi")


async def fetch_regional_lci() -> List[Dict]:
    """reg_lci_{de|fr|...} — Eurostat Labour Cost Index quarterly (lc_lci_r2_q, 2020=100)."""
    data = await _get_json(
        f"{_BASE}/lc_lci_r2_q",
        {"geo": EU_GEOS, "nace_r2": "B-N", "s_adj": "NSA",
         "unit": "I20", "sinceTimePeriod": "2015-Q1", "format": "JSON", "lang": "EN"},
    )
    if not data:
        return []
    return _parse_sdmx_multi_geo(data, "lci")


async def fetch_regional_energy() -> List[Dict]:
    """reg_energy_{de|fr|...} — Eurostat industrial electricity price semi-annual."""
    data = await _get_json(
        f"{_BASE}/nrg_pc_205",
        {"geo": EU_GEOS, "unit": "KWH", "nrg_cons": "MWH20-499", "tax": "X_TAX",
         "sinceTimePeriod": "2015-S1", "format": "JSON", "lang": "EN"},
    )
    if not data:
        return []
    return _parse_sdmx_multi_geo(data, "energy")


async def fetch_extra_singles() -> List[Dict]:
    """
    Fetches indices not covered by other calls:
    - api_eurostat  : EU PPI (same as prim_ppi_eu but different index_id)
    - emn_ppi_sweden: Sweden PPI monthly
    """
    results = []
    pairs = [
        ("EU27_2020", "api_eurostat"),
        ("SE",        "emn_ppi_sweden"),
    ]
    for geo, idx_id in pairs:
        data = await _get_json(
            f"{_BASE}/sts_inppd_m",
            {"geo": geo, "nace_r2": "B-E36", "s_adj": "NSA", "unit": "I15",
             "format": "JSON", "lang": "EN"},
        )
        if data:
            results.extend(_parse_sdmx_single(data, idx_id))
    return results
