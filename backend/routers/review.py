"""
routers/review.py — GET /api/review/summary
Returns per-segment aggregates + 24-month series for the Executive Review page.
"""
from fastapi import APIRouter
from backend.database import get_connection

router = APIRouter()

SEGMENT_META = [
    {"key": "secondary_packaging", "label": "Secondary packaging", "color": "#1565C0"},
    {"key": "emn",                 "label": "EMN",                 "color": "#2E7D32"},
    {"key": "logistics",           "label": "Logistics",           "color": "#E65100"},
    {"key": "primary_pkg_md",      "label": "Primary pkg & MD",    "color": "#6A1B9A"},
    {"key": "api_chemicals",       "label": "API & chemicals",     "color": "#00695C"},
]


def _avg(values):
    clean = [v for v in values if v is not None]
    return round(sum(clean) / len(clean), 4) if clean else None


def _trend(mom_3, mom_6) -> str:
    if mom_3 is None or mom_6 is None:
        return "flat"
    if mom_3 > mom_6 + 0.1:
        return "up"
    if mom_3 < mom_6 - 0.1:
        return "down"
    return "flat"


@router.get("/review/summary")
def review_summary():
    with get_connection() as conn:
        # Latest period across all data
        latest_row = conn.execute(
            "SELECT MAX(period) AS p FROM index_values"
        ).fetchone()
        as_of = latest_row["p"] or "N/A"

        result_segments = []
        for seg in SEGMENT_META:
            seg_key = seg["key"]

            # All active indices in this segment
            indices_rows = conn.execute(
                "SELECT id, name, paid_source FROM indices WHERE segment=? AND active=1",
                (seg_key,),
            ).fetchall()

            segment_indices = []
            all_mom = []
            all_yoy = []

            for idx in indices_rows:
                idx_id   = idx["id"]
                idx_name = idx["name"]

                # Latest value
                latest = conn.execute(
                    """SELECT period, value, mom_change, yoy_change
                       FROM index_values WHERE index_id=?
                       ORDER BY period DESC LIMIT 1""",
                    (idx_id,),
                ).fetchone()

                # 24-month series
                series_rows = conn.execute(
                    """SELECT period, value FROM index_values
                       WHERE index_id=?
                       ORDER BY period DESC LIMIT 24""",
                    (idx_id,),
                ).fetchall()
                series = [{"period": r["period"], "value": r["value"]}
                          for r in reversed(series_rows)]

                mom = latest["mom_change"] if latest else None
                yoy = latest["yoy_change"] if latest else None

                if mom is not None:
                    all_mom.append(mom)
                if yoy is not None:
                    all_yoy.append(yoy)

                # Trend: compare 3-month vs 6-month avg MoM
                recent_mom = conn.execute(
                    """SELECT mom_change FROM index_values
                       WHERE index_id=? AND mom_change IS NOT NULL
                       ORDER BY period DESC LIMIT 6""",
                    (idx_id,),
                ).fetchall()
                mom_vals = [r["mom_change"] for r in recent_mom]
                mom_3 = _avg(mom_vals[:3])
                mom_6 = _avg(mom_vals[:6])

                segment_indices.append({
                    "id":             idx_id,
                    "name":           idx_name,
                    "latest_value":   latest["value"]      if latest else None,
                    "latest_period":  latest["period"]     if latest else None,
                    "mom":            mom,
                    "yoy":            yoy,
                    "series":         series,
                })

            # Segment-level 3-month and 6-month avg MoM for trend
            all_recent_mom_rows = conn.execute(
                """SELECT iv.mom_change
                   FROM index_values iv
                   JOIN indices i ON i.id = iv.index_id
                   WHERE i.segment=? AND iv.mom_change IS NOT NULL
                   ORDER BY iv.period DESC LIMIT 6""",
                (seg_key,),
            ).fetchall()
            seg_mom_vals = [r["mom_change"] for r in all_recent_mom_rows]
            seg_mom_3 = _avg(seg_mom_vals[:3])
            seg_mom_6 = _avg(seg_mom_vals)
            trend = _trend(seg_mom_3, seg_mom_6)

            result_segments.append({
                "segment":    seg["label"],
                "badge_color": seg["color"],
                "count":      len(segment_indices),
                "avg_mom":    _avg(all_mom),
                "avg_yoy":    _avg(all_yoy),
                "trend":      trend,
                "indices":    segment_indices,
            })

    return {"as_of": as_of, "segments": result_segments}
