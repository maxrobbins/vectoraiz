"""
Request Engine Router
=====================

API endpoints for the local request matching engine.
Syncs buyer requests from ai.market and matches against local datasets.

Phase: BQ-VZ-REQUEST-ENGINE Slice B
Created: 2026-04-02
"""

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, select

from app.core.database import get_session
from app.models.cached_requests import CachedRequest, ResponseDraft
from app.models.dataset import DatasetRecord
from app.schemas.request_engine import (
    CachedRequestResponse,
    RequestMatchSummary,
    ResponseDraftCreate,
    ResponseDraftResponse,
    SyncResult,
)
from app.services.request_match_service import match_request
from app.services.request_sync_service import full_sync

logger = logging.getLogger(__name__)

AIMARKET_API_BASE_URL = os.environ.get("AIMARKET_API_BASE_URL", "https://api.ai.market")
AIMARKET_SYNC_TOKEN = os.environ.get("AIMARKET_SYNC_TOKEN")

router = APIRouter()


# ---------------------------------------------------------------------------
# Cached Requests
# ---------------------------------------------------------------------------

@router.get(
    "/cached-requests",
    response_model=List[CachedRequestResponse],
    summary="List cached buyer requests",
)
def list_cached_requests(
    status: Optional[str] = Query(None, description="Filter by status (open, closed, expired)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_session),
):
    query = select(CachedRequest)
    if status:
        query = query.where(CachedRequest.status == status)
    query = query.order_by(CachedRequest.synced_at.desc()).offset(offset).limit(limit)  # type: ignore[union-attr]
    rows = db.exec(query).all()
    return [_to_cached_response(r) for r in rows]


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

@router.post(
    "/sync",
    response_model=SyncResult,
    summary="Trigger a sync from ai.market",
)
async def trigger_sync():
    try:
        result = await full_sync(AIMARKET_API_BASE_URL, AIMARKET_SYNC_TOKEN)
        return SyncResult(**result)
    except Exception as exc:
        logger.error("Sync failed: %s", exc)
        raise HTTPException(status_code=502, detail=f"Sync failed: {exc}")


# ---------------------------------------------------------------------------
# Match Engine
# ---------------------------------------------------------------------------

@router.get(
    "/cached-requests/{request_id}/matches",
    response_model=List[RequestMatchSummary],
    summary="Run match engine for a cached request",
)
def get_matches(
    request_id: str,
    db: Session = Depends(get_session),
):
    cached = db.exec(
        select(CachedRequest).where(CachedRequest.id == request_id)
    ).first()
    if not cached:
        raise HTTPException(status_code=404, detail="Cached request not found")

    # Load all ready datasets
    datasets = db.exec(
        select(DatasetRecord).where(DatasetRecord.status == "ready")
    ).all()

    matches = match_request(cached, list(datasets))

    # Update match_run_at
    cached.match_run_at = datetime.now(timezone.utc)
    db.add(cached)
    db.commit()

    return matches


# ---------------------------------------------------------------------------
# Response Drafts
# ---------------------------------------------------------------------------

@router.post(
    "/cached-requests/{request_id}/draft",
    response_model=ResponseDraftResponse,
    status_code=201,
    summary="Create a response draft from a match",
)
def create_draft(
    request_id: str,
    body: ResponseDraftCreate,
    db: Session = Depends(get_session),
):
    cached = db.exec(
        select(CachedRequest).where(CachedRequest.id == request_id)
    ).first()
    if not cached:
        raise HTTPException(status_code=404, detail="Cached request not found")

    dataset = db.exec(
        select(DatasetRecord).where(DatasetRecord.id == body.matched_dataset_id)
    ).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if dataset.status != "ready":
        raise HTTPException(status_code=409, detail="Dataset is not ready")

    draft = ResponseDraft(
        id=str(uuid.uuid4()),
        cached_request_id=request_id,
        matched_dataset_id=body.matched_dataset_id,
        title=body.title,
        description=body.description,
        score=body.score,
        score_reasons=json.dumps(body.score_reasons),
        require_review=body.require_review,
        status="draft",
        internal_notes=body.internal_notes,
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    db.add(draft)
    db.commit()
    db.refresh(draft)

    return _to_draft_response(draft)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_cached_response(row: CachedRequest) -> CachedRequestResponse:
    categories = []
    try:
        categories = json.loads(row.categories) if row.categories else []
    except (json.JSONDecodeError, TypeError):
        pass

    return CachedRequestResponse(
        id=row.id,
        marketplace_request_id=row.marketplace_request_id,
        title=row.title,
        description=row.description,
        categories=categories,
        urgency=row.urgency,
        status=row.status,
        published_at=row.published_at,
        expires_at=row.expires_at,
        updated_at=row.updated_at,
        synced_at=row.synced_at,
        match_run_at=row.match_run_at,
    )


def _to_draft_response(draft: ResponseDraft) -> ResponseDraftResponse:
    reasons = {}
    try:
        reasons = json.loads(draft.score_reasons) if draft.score_reasons else {}
    except (json.JSONDecodeError, TypeError):
        pass

    return ResponseDraftResponse(
        id=draft.id,
        cached_request_id=draft.cached_request_id,
        matched_dataset_id=draft.matched_dataset_id,
        title=draft.title,
        description=draft.description,
        score=draft.score,
        score_reasons=reasons,
        require_review=draft.require_review,
        status=draft.status,
        internal_notes=draft.internal_notes,
        created_at=draft.created_at,
        updated_at=draft.updated_at,
    )
