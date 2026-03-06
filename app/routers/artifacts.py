"""
Artifacts API Router — CRUD for allAI output files.

Endpoints:
  GET    /api/artifacts              — list artifacts (pagination, sort, filter)
  GET    /api/artifacts/{id}         — get artifact metadata
  GET    /api/artifacts/{id}/download — download file
  DELETE /api/artifacts/{id}         — delete artifact
  PATCH  /api/artifacts/{id}/star    — toggle star

All endpoints enforce user_id scoping.

PHASE: BQ-VZ-ARTIFACTS Phase 1
CREATED: 2026-03-06
"""

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from app.services.artifacts_service import get_artifacts_service

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_user_id(request: Request) -> str:
    """Extract user_id from auth context. Falls back to 'local' for single-user."""
    user = getattr(request.state, "user", None)
    if user and hasattr(user, "user_id") and user.user_id:
        return user.user_id
    return "local"


class StarRequest(BaseModel):
    starred: bool


@router.get("")
async def list_artifacts(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    include_expired: bool = Query(False),
    format_filter: Optional[str] = Query(None),
):
    """List artifacts for the current user."""
    user_id = _get_user_id(request)
    svc = get_artifacts_service()
    artifacts = await asyncio.to_thread(
        svc.list_artifacts, user_id, include_expired, offset, limit
    )
    result = [a.to_dict() for a in artifacts]
    if format_filter:
        result = [a for a in result if a["format"] == format_filter]
    return {"artifacts": result, "total": len(result)}


@router.get("/{artifact_id}")
async def get_artifact(artifact_id: str, request: Request):
    """Get artifact metadata."""
    user_id = _get_user_id(request)
    svc = get_artifacts_service()
    try:
        artifact = await asyncio.to_thread(svc.get_artifact, artifact_id, user_id)
        return artifact.to_dict()
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Artifact not found")


@router.get("/{artifact_id}/download")
async def download_artifact(artifact_id: str, request: Request):
    """Download artifact file with Content-Disposition: attachment."""
    user_id = _get_user_id(request)
    svc = get_artifacts_service()
    try:
        content_path, display_filename, mime_type = await asyncio.to_thread(
            svc.download_artifact, artifact_id, user_id
        )
        return FileResponse(
            path=str(content_path),
            media_type=mime_type,
            filename=display_filename,
            headers={
                "Content-Disposition": f'attachment; filename="{display_filename}"',
            },
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Artifact not found")


@router.delete("/{artifact_id}")
async def delete_artifact(artifact_id: str, request: Request):
    """Delete an artifact."""
    user_id = _get_user_id(request)
    svc = get_artifacts_service()
    try:
        await asyncio.to_thread(svc.delete_artifact, artifact_id, user_id)
        return {"status": "deleted"}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Artifact not found")


@router.patch("/{artifact_id}/star")
async def star_artifact(artifact_id: str, body: StarRequest, request: Request):
    """Toggle star status on an artifact."""
    user_id = _get_user_id(request)
    svc = get_artifacts_service()
    try:
        artifact = await asyncio.to_thread(
            svc.star_artifact, artifact_id, user_id, body.starred
        )
        return artifact.to_dict()
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Artifact not found")
