"""
routers/indices.py — GET /api/indices, /api/indices/{id}
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from backend.database import get_connection

router = APIRouter()

SEGMENT_LABELS = {
    "secondary_packaging": "Secondary packaging",
    "emn":                 "EMN",
    "logistics":           "Logistics",
    "primary_pkg_md":      "Primary pkg & MD",
    "api_chemicals":       "API & chemicals",
    "regional":            "Regional",
}


def _row_to_dict(row) -> dict:
    return dict(row)


@router.get("/indices")
def list_indices(
    segment:     Optional[str] = Query(None),
    source:      Optional[str] = Query(None),
    category:    Optional[str] = Query(None),
    q:           Optional[str] = Query(None),
    country_iso: Optional[str] = Query(None),
    period_type: Optional[str] = Query(None),
):
    """Return all active indices with latest value, MoM %, YoY %, last period."""
    with get_connection() as conn:
        sql = """
            SELECT
                i.id, i.name, i.segment, i.source, i.category,
                i.unit, i.base_year, i.paid_source, i.last_updated, i.active,
                i.source_url, i.period_type, i.country_iso,
                iv.value       AS latest_value,
                iv.period      AS latest_period,
                iv.mom_change,
                iv.yoy_change
            FROM indices i
            LEFT JOIN index_values iv
                ON iv.index_id = i.id
                AND iv.period = (
                    SELECT MAX(period) FROM index_values WHERE index_id = i.id
                )
            WHERE i.active = 1
        """
        params = []
        if segment:
            sql += " AND i.segment = ?"
            params.append(segment)
        if source:
            sql += " AND i.source = ?"
            params.append(source)
        if category:
            sql += " AND i.category = ?"
            params.append(category)
        if q:
            sql += " AND LOWER(i.name) LIKE ?"
            params.append(f"%{q.lower()}%")
        if country_iso:
            sql += " AND i.country_iso = ?"
            params.append(country_iso)
        if period_type:
            sql += " AND i.period_type = ?"
            params.append(period_type)
        sql += " ORDER BY i.segment, i.name"
        rows = conn.execute(sql, params).fetchall()
        return [_row_to_dict(r) for r in rows]


@router.get("/indices/{index_id}")
def get_index(index_id: str):
    """Return single index metadata + last 36 periods of values."""
    with get_connection() as conn:
        meta = conn.execute(
            """SELECT id, name, segment, source, category, unit, base_year,
                      paid_source, last_updated, active, source_url, period_type, country_iso
               FROM indices WHERE id = ?""",
            (index_id,),
        ).fetchone()
        if not meta:
            raise HTTPException(status_code=404, detail=f"Index '{index_id}' not found")

        series = conn.execute(
            """SELECT period, value, mom_change, yoy_change
               FROM index_values
               WHERE index_id = ?
               ORDER BY period DESC
               LIMIT 36""",
            (index_id,),
        ).fetchall()

        result = _row_to_dict(meta)
        result["series"] = [_row_to_dict(r) for r in reversed(series)]
        return result
