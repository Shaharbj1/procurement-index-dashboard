"""
routers/calculator.py — POST /api/calculate
"""
from fastapi import APIRouter, HTTPException
from backend.database import get_connection
from backend.models import CalculateRequest, CalculateResponse

router = APIRouter()


@router.post("/calculate", response_model=CalculateResponse)
def calculate(req: CalculateRequest):
    """
    Compute % change between two periods.
    Formula: pct_change = ((end - start) / start) * 100
    """
    if req.end_period <= req.start_period:
        raise HTTPException(status_code=400, detail="end_period must be after start_period")

    with get_connection() as conn:
        # Fetch index name
        meta = conn.execute(
            "SELECT name FROM indices WHERE id = ?", (req.index_id,)
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

        start_val = start_row["value"]
        end_val   = end_row["value"]
        pct_change = round((end_val - start_val) / start_val * 100, 2) if start_val != 0 else 0.0
        abs_change = round(end_val - start_val, 4)

        # Monthly series for the selected range
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
