"""
routers/upload.py — POST /api/upload (preview) + POST /api/upload/confirm (commit)

Supported formats:
  1. Standard CSV:    columns index_id, period, value
  2. Eurostat CSV:    detected by TIME_PERIOD + OBS_VALUE columns
  3. BLS Excel:       detected by 'Series Id', 'Year', 'Period', 'Value' columns
  4. Excel multi-idx: each sheet = one index (sheet name = index_id)
"""
import io
import re
import uuid
import json
import tempfile
import os
from typing import List, Dict, Any

from fastapi import APIRouter, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse

from backend.database import get_connection, upsert_index_value_full

router = APIRouter()

# In-memory pending session store  {session_id: [{"index_id", "period", "value"}, ...]}
_pending: Dict[str, List[Dict[str, Any]]] = {}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _validate_period(period: str) -> bool:
    return bool(re.match(r"^\d{4}-(0[1-9]|1[0-2])$", str(period)))


def _normalize_value(val) -> float:
    """Strip % signs, thousands separators, convert comma-decimal."""
    s = str(val).strip().replace("%", "").replace(" ", "")
    s = s.replace(",", ".")
    return float(s)


def _bls_period_map(year: Any, period_code: str) -> str:
    """Convert BLS Year + Period (M01-M12) → YYYY-MM."""
    m = re.match(r"M(\d{2})", str(period_code).strip())
    if not m:
        raise ValueError(f"Unknown BLS period code: {period_code}")
    month = m.group(1)
    return f"{int(year):04d}-{month}"


def _parse_standard_csv(content: bytes) -> List[Dict]:
    import csv
    text = content.decode("utf-8-sig").strip()
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for i, row in enumerate(reader, start=2):
        index_id = row.get("index_id", "").strip()
        period   = row.get("period", "").strip()
        value_s  = row.get("value", "").strip()
        if not index_id or not period or not value_s:
            continue
        if not _validate_period(period):
            raise ValueError(f"Row {i}: period '{period}' must be YYYY-MM")
        try:
            value = _normalize_value(value_s)
        except ValueError:
            raise ValueError(f"Row {i}: value '{value_s}' is not numeric")
        rows.append({"index_id": index_id, "period": period, "value": value})
    return rows


def _parse_eurostat_csv(content: bytes) -> List[Dict]:
    """Eurostat export: TIME_PERIOD + OBS_VALUE columns; geo/nace_r2 → index_id."""
    import csv
    text = content.decode("utf-8-sig").strip()
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    rows = []
    for i, row in enumerate(reader, start=2):
        period_raw = (row.get("TIME_PERIOD") or row.get("time_period") or "").strip()
        value_raw  = (row.get("OBS_VALUE")  or row.get("obs_value")  or "").strip()
        # Build index_id from geo or nace_r2 if present
        geo    = (row.get("geo") or row.get("GEO") or "").strip()
        nace   = (row.get("nace_r2") or row.get("NACE_R2") or "").strip()
        freq   = (row.get("freq") or row.get("FREQ") or "").strip()

        if not period_raw or not value_raw or value_raw in (":", "N/A", ""):
            continue

        # Convert Eurostat YYYY-MM period
        period = period_raw[:7]  # handles YYYY-MM or YYYY-MM-DD
        if not _validate_period(period):
            continue

        try:
            value = _normalize_value(value_raw)
        except ValueError:
            continue

        index_id = geo or nace or "eurostat_import"
        index_id = re.sub(r"[^a-z0-9_]", "_", index_id.lower()).strip("_")
        rows.append({"index_id": index_id, "period": period, "value": value})
    return rows


def _parse_bls_excel(file_bytes: bytes) -> List[Dict]:
    """BLS Excel: 'Series Id', 'Year', 'Period', 'Value' columns."""
    import pandas as pd
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=0)
    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]
    required = {"Series Id", "Year", "Period", "Value"}
    if not required.issubset(set(df.columns)):
        raise ValueError("BLS format requires columns: Series Id, Year, Period, Value")

    rows = []
    for _, row in df.iterrows():
        series_id = str(row["Series Id"]).strip()
        year      = row["Year"]
        period_c  = str(row["Period"]).strip()
        val       = row["Value"]

        if str(period_c).upper() == "M13":  # annual average — skip
            continue
        try:
            period = _bls_period_map(year, period_c)
            value  = float(val)
            index_id = re.sub(r"[^a-z0-9_]", "_", series_id.lower()).strip("_")
            rows.append({"index_id": index_id, "period": period, "value": value})
        except (ValueError, TypeError):
            continue
    return rows


def _parse_excel_multi_sheet(file_bytes: bytes) -> List[Dict]:
    """Each sheet = one index; sheet name becomes index_id."""
    import pandas as pd
    xl = pd.ExcelFile(io.BytesIO(file_bytes))
    rows = []
    for sheet_name in xl.sheet_names:
        df = xl.parse(sheet_name)
        df.columns = [str(c).strip() for c in df.columns]
        # Must have at least period and value columns
        period_col = next((c for c in df.columns if "period" in c.lower() or "date" in c.lower() or "time" in c.lower()), None)
        value_col  = next((c for c in df.columns if "value" in c.lower() or "obs" in c.lower()), None)
        if period_col is None or value_col is None:
            continue
        index_id = re.sub(r"[^a-z0-9_]", "_", sheet_name.lower()).strip("_")
        for _, row in df.iterrows():
            p = str(row[period_col]).strip()[:7]
            v = row[value_col]
            if not _validate_period(p):
                continue
            try:
                rows.append({"index_id": index_id, "period": p, "value": float(v)})
            except (ValueError, TypeError):
                continue
    return rows


def _detect_and_parse(filename: str, content: bytes):
    """Auto-detect format and parse. Returns (rows, detected_format_label)."""
    name_lower = filename.lower()

    if name_lower.endswith(".xlsx") or name_lower.endswith(".xls"):
        # Try BLS format first
        try:
            rows = _parse_bls_excel(content)
            if rows:
                return rows, "BLS Excel"
        except Exception:
            pass
        # Fall back to multi-sheet
        rows = _parse_excel_multi_sheet(content)
        return rows, "Excel (multi-sheet)"

    if name_lower.endswith(".csv"):
        text_sample = content.decode("utf-8-sig", errors="replace")
        # Detect Eurostat by header keywords
        if "TIME_PERIOD" in text_sample or "OBS_VALUE" in text_sample or "time_period" in text_sample:
            rows = _parse_eurostat_csv(content)
            return rows, "Eurostat CSV"
        # Standard CSV
        rows = _parse_standard_csv(content)
        return rows, "Standard CSV"

    raise ValueError(f"Unsupported file type: {filename}. Use .csv or .xlsx")


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/upload")
async def upload_preview(file: UploadFile = File(...)):
    """
    Parse file, validate, return preview (first 10 rows).
    Does NOT write to database yet.
    """
    content = await file.read()
    try:
        rows, fmt = _detect_and_parse(file.filename or "upload.csv", content)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    if not rows:
        raise HTTPException(status_code=422, detail="No valid rows found in file")

    session_id = str(uuid.uuid4())
    _pending[session_id] = rows

    return {
        "rows": rows[:10],
        "total_rows": len(rows),
        "detected_format": fmt,
        "session_id": session_id,
    }


@router.post("/upload/confirm")
def upload_confirm(body: dict):
    """
    Commit the pending upload identified by session_id.
    Body: { "session_id": "...", "index_meta": {...optional overrides...} }
    """
    session_id = body.get("session_id")
    if not session_id or session_id not in _pending:
        raise HTTPException(status_code=400, detail="Invalid or expired session_id")

    rows = _pending.pop(session_id)
    added = updated = 0

    with get_connection() as conn:
        for row in rows:
            index_id = row["index_id"]
            period   = row["period"]
            value    = row["value"]

            # Auto-create index catalogue entry if unknown
            exists = conn.execute(
                "SELECT id FROM indices WHERE id=?", (index_id,)
            ).fetchone()
            if not exists:
                conn.execute(
                    """INSERT OR IGNORE INTO indices
                       (id, name, segment, source, category, unit, base_year, paid_source, active)
                       VALUES (?, ?, 'secondary_packaging', 'manual', 'general', 'index', '2015=100', 0, 1)""",
                    (index_id, index_id.replace("_", " ").title()),
                )

            result = upsert_index_value_full(conn, index_id, period, value)
            if result["action"] == "added":
                added += 1
            else:
                updated += 1

    return {
        "rows_added": added,
        "rows_updated": updated,
        "message": f"{added} rows added, {updated} rows updated",
    }
