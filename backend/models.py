"""
models.py — Pydantic request/response schemas
"""
from typing import List, Optional
from pydantic import BaseModel


# ── Upload ───────────────────────────────────────────────────────────────────

class UploadPreviewRow(BaseModel):
    index_id: str
    period: str
    value: float


class UploadPreviewResponse(BaseModel):
    rows: List[UploadPreviewRow]
    total_rows: int
    detected_format: str
    session_id: str


class UploadConfirmResponse(BaseModel):
    rows_added: int
    rows_updated: int
    message: str


# ── Calculator ───────────────────────────────────────────────────────────────

class CalculateRequest(BaseModel):
    index_id: str
    start_period: str
    end_period: str


class CalculateResponse(BaseModel):
    index_name: str
    start_period: str
    start_value: float
    end_period: str
    end_value: float
    pct_change: float
    abs_change: float
    monthly_series: Optional[List[dict]] = None


# ── Index catalogue ──────────────────────────────────────────────────────────

class IndexMeta(BaseModel):
    id: str
    name: str
    segment: str
    source: str
    category: str
    unit: str
    base_year: str
    paid_source: int
    last_updated: Optional[str]
    active: int
    latest_value: Optional[float] = None
    latest_period: Optional[str] = None
    mom_change: Optional[float] = None
    yoy_change: Optional[float] = None


class IndexDetail(IndexMeta):
    series: List[dict] = []


# ── Review ───────────────────────────────────────────────────────────────────

class ReviewIndex(BaseModel):
    id: str
    name: str
    latest_value: Optional[float]
    latest_period: Optional[str]
    mom: Optional[float]
    yoy: Optional[float]
    series: List[dict]


class ReviewSegment(BaseModel):
    segment: str
    badge_color: str
    count: int
    avg_mom: Optional[float]
    avg_yoy: Optional[float]
    trend: str
    indices: List[ReviewIndex]


class ReviewSummaryResponse(BaseModel):
    as_of: str
    segments: List[ReviewSegment]
