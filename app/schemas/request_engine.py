"""
Pydantic Schemas for Request Engine
====================================

Request/response models for the BQ-VZ-REQUEST-ENGINE API.

Phase: BQ-VZ-REQUEST-ENGINE Slice B
Created: 2026-04-02
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Cached Request
# ---------------------------------------------------------------------------

class CachedRequestResponse(BaseModel):
    id: str
    marketplace_request_id: str
    title: str
    description: str
    categories: List[str] = Field(default_factory=list)
    urgency: Optional[str] = None
    status: str
    published_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    updated_at: datetime
    synced_at: datetime
    match_run_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Match Engine
# ---------------------------------------------------------------------------

class RequestMatchSummary(BaseModel):
    dataset_id: str
    dataset_title: str
    score: float = Field(..., ge=0.0, le=1.0)
    score_reasons: Dict[str, Any] = Field(default_factory=dict)
    row_count_range: str = Field(
        default="unknown",
        description="Human-readable row count bucket (e.g. '1K-10K')",
    )
    freshness_category: str = Field(
        default="unknown",
        description="fresh / recent / stale / unknown",
    )
    require_review: bool = True


# ---------------------------------------------------------------------------
# Response Draft
# ---------------------------------------------------------------------------

class ResponseDraftCreate(BaseModel):
    matched_dataset_id: str
    title: str = Field(..., max_length=512)
    description: str = ""
    score: float = Field(0.0, ge=0.0, le=1.0)
    score_reasons: Dict[str, Any] = Field(default_factory=dict)
    require_review: bool = True
    internal_notes: Optional[str] = None


class ResponseDraftResponse(BaseModel):
    id: str
    cached_request_id: str
    matched_dataset_id: str
    title: str
    description: str
    score: float
    score_reasons: Dict[str, Any] = Field(default_factory=dict)
    require_review: bool
    status: str
    internal_notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

class SyncResult(BaseModel):
    synced: int = 0
    new: int = 0
    updated: int = 0
    cursor: Optional[str] = None
