"""
routers/timeseries.py — GET /api/timeseries/{id}
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from backend.database import get_connection

router = APIRouter()


@router.get("/timeseries/{index_id}")
def get_timeseries(
    index_id: str,
    from_period: Optional[str] = Query(None, alias="from"),
    to_period:   Optional[str] = Query(None, alias="to"),
):
    """Return full time-series for an index. Optional ?from=YYYY-MM&to=YYYY-MM."""
    with get_connection() as conn:
        exists = conn.execute(
            "SELECT id FROM indices WHERE id = ?", (index_id,)
        ).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail=f"Index '{index_id}' not found")

        sql = """
            SELECT period, value, mom_change, yoy_change
            FROM index_values
            WHERE index_id = ?
        """
        params = [index_id]
        if from_period:
            sql += " AND period >= ?"
            params.append(from_period)
        if to_period:
            sql += " AND period <= ?"
            params.append(to_period)
        sql += " ORDER BY period ASC"

        rows = conn.execute(sql, params).fetchall()
        return {
            "index_id": index_id,
            "series": [dict(r) for r in rows],
        }
