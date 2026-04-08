"""
Raw Listings Router
===================

Endpoints for raw file registration, listing CRUD, publish/delist,
and entitlement-gated file download.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-03
Updated: 2026-03-05 — Refactored to use RawListingService, added list/delete files
"""

import logging
import os
import uuid
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import FileResponse

from app.config import settings
from app.schemas.raw_listings import (
    MetadataResponse,
    RawFileRegisterRequest,
    RawFileResponse,
    RawListingCreateRequest,
    RawListingListResponse,
    RawListingResponse,
    RawListingUpdateRequest,
)
from app.services.raw_file_service import RawFileService, get_raw_file_service
from app.services.raw_listing_service import RawListingService, get_raw_listing_service
from app.services.entitlement_service import EntitlementService, get_entitlement_service

logger = logging.getLogger(__name__)

router = APIRouter()

# Separate router for download — registered without admin auth in main.py
download_router = APIRouter()


def _file_response(raw_file) -> RawFileResponse:
    return RawFileResponse(
        id=raw_file.id,
        filename=raw_file.filename,
        file_path=raw_file.file_path,
        file_size_bytes=raw_file.file_size_bytes,
        content_hash=raw_file.content_hash,
        mime_type=raw_file.mime_type,
        metadata=raw_file.metadata_,
        created_at=raw_file.created_at,
        updated_at=raw_file.updated_at,
    )


def _listing_response(listing) -> RawListingResponse:
    return RawListingResponse(
        id=listing.id,
        raw_file_id=listing.raw_file_id,
        marketplace_listing_id=listing.marketplace_listing_id,
        title=listing.title,
        description=listing.description,
        tags=listing.tags or [],
        auto_metadata=listing.auto_metadata,
        price_cents=listing.price_cents,
        status=listing.status,
        published_at=listing.published_at,
        created_at=listing.created_at,
        updated_at=listing.updated_at,
    )


# --- Raw File Endpoints ---

@router.post(
    "/files",
    response_model=RawFileResponse,
    status_code=201,
    summary="Register a raw file",
    description="Hash and register a raw file from disk for marketplace listing.",
)
async def register_raw_file(
    req: RawFileRegisterRequest,
    svc: RawFileService = Depends(get_raw_file_service),
):
    try:
        raw_file = svc.register_file(req.file_path)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return _file_response(raw_file)


@router.post(
    "/files/upload",
    response_model=RawFileResponse,
    status_code=201,
    summary="Upload and register a raw file",
    description="Accept a browser multipart upload, save it into the raw import directory, and register it.",
)
async def upload_raw_file(
    file: UploadFile = File(...),
    svc: RawFileService = Depends(get_raw_file_service),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Uploaded file must include a filename")

    import_dir = svc.get_import_directory()
    import_dir.mkdir(parents=True, exist_ok=True)

    original_name = os.path.basename(file.filename)
    suffix = Path(original_name).suffix
    stored_path = import_dir / f"{uuid.uuid4().hex}{suffix}"
    bytes_written = 0
    max_size_bytes = settings.raw_file_upload_max_size_mb * 1024 * 1024

    try:
        with stored_path.open("wb") as output:
            while chunk := await file.read(settings.chunk_size):
                bytes_written += len(chunk)
                if bytes_written > max_size_bytes:
                    raise HTTPException(
                        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                        detail=f"File exceeds max upload size of {settings.raw_file_upload_max_size_mb}MB",
                    )
                output.write(chunk)

        raw_file = svc.register_file(str(stored_path), filename=original_name)
        return _file_response(raw_file)
    except HTTPException:
        if stored_path.exists():
            stored_path.unlink()
        raise
    except Exception:
        if stored_path.exists():
            stored_path.unlink()
        raise
    finally:
        await file.close()


@router.get(
    "/files",
    response_model=List[RawFileResponse],
    summary="List all registered raw files",
)
async def list_raw_files(
    svc: RawFileService = Depends(get_raw_file_service),
):
    files = svc.list_files()
    return [_file_response(f) for f in files]


@router.get(
    "/files/{file_id}",
    response_model=RawFileResponse,
    summary="Get raw file metadata",
)
async def get_raw_file(
    file_id: str,
    svc: RawFileService = Depends(get_raw_file_service),
):
    raw_file = svc.get_file(file_id)
    if raw_file is None:
        raise HTTPException(status_code=404, detail="Raw file not found")
    return _file_response(raw_file)


@router.delete(
    "/files/{file_id}",
    status_code=204,
    summary="Delete a raw file and its metadata",
)
async def delete_raw_file(
    file_id: str,
    svc: RawFileService = Depends(get_raw_file_service),
):
    if not svc.delete_file(file_id):
        raise HTTPException(status_code=404, detail="Raw file not found")


@router.post(
    "/files/{file_id}/metadata",
    response_model=MetadataResponse,
    summary="Generate allAI auto-describe metadata",
)
async def generate_file_metadata(
    file_id: str,
    svc: RawFileService = Depends(get_raw_file_service),
):
    try:
        metadata = await svc.generate_metadata(file_id)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return MetadataResponse(file_id=file_id, auto_metadata=metadata)


# --- Raw Listing Endpoints ---

@router.get(
    "/listings",
    response_model=RawListingListResponse,
    summary="List raw listings (paginated)",
)
async def list_raw_listings(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: Optional[str] = Query(default=None, description="Filter by status"),
    svc: RawListingService = Depends(get_raw_listing_service),
):
    listings, total = svc.list_listings(status_filter=status, limit=limit, offset=offset)
    return RawListingListResponse(
        listings=[_listing_response(l) for l in listings],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.post(
    "/listings",
    response_model=RawListingResponse,
    status_code=201,
    summary="Create a draft raw listing",
)
async def create_raw_listing(
    req: RawListingCreateRequest,
    file_svc: RawFileService = Depends(get_raw_file_service),
    listing_svc: RawListingService = Depends(get_raw_listing_service),
):
    if file_svc.get_file(req.raw_file_id) is None:
        raise HTTPException(status_code=404, detail="Raw file not found")

    listing = listing_svc.create_listing(
        raw_file_id=req.raw_file_id,
        title=req.title,
        description=req.description,
        tags=req.tags,
        price_cents=req.price_cents,
    )
    return _listing_response(listing)


@router.put(
    "/listings/{listing_id}",
    response_model=RawListingResponse,
    summary="Update listing metadata",
)
async def update_raw_listing(
    listing_id: str,
    req: RawListingUpdateRequest,
    svc: RawListingService = Depends(get_raw_listing_service),
):
    listing = svc.update_listing(
        listing_id=listing_id,
        title=req.title,
        description=req.description,
        tags=req.tags,
        price_cents=req.price_cents,
    )
    if listing is None:
        raise HTTPException(status_code=404, detail="Listing not found")
    return _listing_response(listing)


@router.post(
    "/listings/{listing_id}/publish",
    response_model=RawListingResponse,
    summary="Publish listing (status -> listed)",
)
async def publish_raw_listing(
    listing_id: str,
    svc: RawListingService = Depends(get_raw_listing_service),
):
    try:
        listing = svc.publish_listing(listing_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Listing not found")
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return _listing_response(listing)


@router.post(
    "/listings/{listing_id}/delist",
    response_model=RawListingResponse,
    summary="Delist a listing (status -> delisted)",
)
async def delist_raw_listing(
    listing_id: str,
    svc: RawListingService = Depends(get_raw_listing_service),
):
    try:
        listing = svc.delist_listing(listing_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Listing not found")
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return _listing_response(listing)


# --- Download Endpoint (Entitlement-gated, no admin auth) ---

@download_router.get(
    "/download/{file_id}",
    summary="Download raw file (entitlement required)",
    description="Validates entitlement token, verifies file hash, and streams the file.",
)
async def download_raw_file(
    file_id: str,
    request: Request,
    file_svc: RawFileService = Depends(get_raw_file_service),
    ent_svc: EntitlementService = Depends(get_entitlement_service),
):
    auth_header = request.headers.get("Authorization", "")
    try:
        payload = ent_svc.validate_entitlement(auth_header)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))

    expected_hash = payload.get("file_hash", "")

    try:
        raw_file = file_svc.serve_file(file_id, expected_hash)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    logger.info(
        "Serving raw file %s to buyer %s (order=%s)",
        file_id, payload.get("buyer_id"), payload.get("order_id"),
    )

    return FileResponse(
        path=raw_file.file_path,
        filename=raw_file.filename,
        media_type=raw_file.mime_type or "application/octet-stream",
    )
