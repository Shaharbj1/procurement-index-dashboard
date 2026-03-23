"""
routers/calculator.py — POST /api/calculate
Supports monthly (YYYY-MM), quarterly (YYYY-QN), and semi-annual (YYYY-SN) periods.
"""
import re
from fastapi import APIRouter, HTTPException
from backend.database import get_connection
from backend.models import CalculateRequest, CalculateResponse

router = APIRouter()


def parse_period(p: str) -> tuple:
    """Convert any period format to a comparable (year, sub-period) tuple."""
    if re.match(r"\d{4}-\d{2}$", p):
        y, m = p.split("-")
        return int(y), int(m)
    if re.match(r"\d{4}-Q[1-4]$", p):
        y, q = p.split("-Q")
        return int(y), int(q) * 3
    if re.match(r"\d{4}-S[12]$", p):
        y, s = p.split("-S")
        return int(y), int(s) * 6
    raise ValueError(f"Unknown period format: {p!r}")


def period_format(p: str) -> str:
    """Return the format name of a period string."""
    if re.match(r"\d{4}-\d{2}$", p):
        return "monthly"
    if re.match(r"\d{4}-Q[1-4]$", p):
        return "quarterly"
    if re.match(r"\d{4}-S[12]$", p):
        return "semi-annual"
    return "unknown"


@router.post("/calculate", response_model=CalculateResponse)
def calculate(req: CalculateRequest):
    """
    Compute % change between two periods.
    Formula: pct_change = ((end - start) / start) * 100
    Supports YYYY-MM, YYYY-QN, YYYY-SN period formats.
    """
    start_fmt = period_format(req.start_period)
    end_fmt   = period_format(req.end_period)

    if start_fmt == "unknown":
        raise HTTPException(status_code=400, detail=f"Invalid start_period format: {req.start_period!r}")
    if end_fmt == "unknown":
        raise HTTPException(status_code=400, detail=f"Invalid end_period format: {req.end_period!r}")
    if start_fmt != end_fmt:
        raise HTTPException(
            status_code=400,
            detail=f"Period formats must match (start: {start_fmt}, end: {end_fmt})"
        )

    try:
        start_tuple = parse_period(req.start_period)
        end_tuple   = parse_period(req.end_period)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if end_tuple <= start_tuple:
        raise HTTPException(status_code=400, detail="end_period must be after start_period")

    with get_connection() as conn:
        # Fetch index metadata (including period_type and source_url)
        meta = conn.execute(
            "SELECT name, period_type, source_url FROM indices WHERE id = ?",
            (req.index_id,)
        ).fetchone()
        if not meta:
            raise HTTPException(status_code=404, detail=f"Index '{req.index_id}' not found")

        # Fetch start value
        start_row = conn.execute(
            "SELECT value FROM index_values WHERE index_id=? AND period=?",
            (req.index_id, req.start_period),
        ).fetchone()
        if not start_row:
            raise HTTPException(
                status_code=404,
                detail=f"No data for period {req.start_period} in index {req.index_id}",
            )

        # Fetch end value
        end_row = conn.execute(
            "SELECT value FROM index_values WHERE index_id=? AND period=?",
            (req.index_id, req.end_period),
        ).fetchone()
        if not end_row:
            raise HTTPException(
                status_code=404,
                detail=f"No data for period {req.end_period} in index {req.index_id}",
            )

        start_val  = start_row["value"]
        end_val    = end_row["value"]
        pct_change = round((end_val - start_val) / start_val * 100, 2) if start_val != 0 else 0.0
        abs_change = round(end_val - start_val, 4)

        # Series for the selected range (string comparison works for all our formats)
        series_rows = conn.execute(
            """SELECT period, value, mom_change, yoy_change
               FROM index_values
               WHERE index_id=? AND period >= ? AND period <= ?
               ORDER BY period ASC""",
            (req.index_id, req.start_period, req.end_period),
        ).fetchall()

        return CalculateResponse(
            index_name=meta["name"],
            start_period=req.start_period,
            start_value=start_val,
            end_period=req.end_period,
            end_value=end_val,
            pct_change=pct_change,
            abs_change=abs_change,
            monthly_series=[dict(r) for r in series_rows],
        )
