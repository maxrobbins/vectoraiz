"""
Raw Listings Router
===================

Endpoints for raw file registration, listing CRUD, publish/delist,
and entitlement-gated file download.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-03
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse
from sqlmodel import select, func

from app.models.raw_file import RawFile
from app.models.raw_listing import RawListing
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
from app.services.entitlement_service import EntitlementService, get_entitlement_service

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_db_session():
    from app.core.database import get_session_context
    return get_session_context()


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
    return RawFileResponse(
        id=raw_file.id,
        filename=raw_file.filename,
        file_path=raw_file.file_path,
        file_size_bytes=raw_file.file_size_bytes,
        content_hash=raw_file.content_hash,
        mime_type=raw_file.mime_type,
        created_at=raw_file.created_at,
        updated_at=raw_file.updated_at,
    )


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
    return RawFileResponse(
        id=raw_file.id,
        filename=raw_file.filename,
        file_path=raw_file.file_path,
        file_size_bytes=raw_file.file_size_bytes,
        content_hash=raw_file.content_hash,
        mime_type=raw_file.mime_type,
        created_at=raw_file.created_at,
        updated_at=raw_file.updated_at,
    )


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
        metadata = svc.generate_metadata(file_id)
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
):
    with _get_db_session() as session:
        query = select(RawListing)
        count_query = select(func.count()).select_from(RawListing)

        if status:
            query = query.where(RawListing.status == status)
            count_query = count_query.where(RawListing.status == status)

        total = session.exec(count_query).one()
        listings = session.exec(
            query.order_by(RawListing.created_at.desc()).offset(offset).limit(limit)
        ).all()

        return RawListingListResponse(
            listings=[
                RawListingResponse(
                    id=l.id,
                    raw_file_id=l.raw_file_id,
                    marketplace_listing_id=l.marketplace_listing_id,
                    title=l.title,
                    description=l.description,
                    tags=l.tags or [],
                    auto_metadata=l.auto_metadata,
                    price_cents=l.price_cents,
                    status=l.status,
                    published_at=l.published_at,
                    created_at=l.created_at,
                    updated_at=l.updated_at,
                )
                for l in listings
            ],
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
):
    # Verify raw file exists
    raw_file = file_svc.get_file(req.raw_file_id)
    if raw_file is None:
        raise HTTPException(status_code=404, detail="Raw file not found")

    import uuid
    listing = RawListing(
        id=str(uuid.uuid4()),
        raw_file_id=req.raw_file_id,
        title=req.title,
        description=req.description,
        tags=req.tags,
        price_cents=req.price_cents,
        status="draft",
    )

    with _get_db_session() as session:
        session.add(listing)
        session.commit()
        session.refresh(listing)

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


@router.put(
    "/listings/{listing_id}",
    response_model=RawListingResponse,
    summary="Update listing metadata",
)
async def update_raw_listing(
    listing_id: str,
    req: RawListingUpdateRequest,
):
    with _get_db_session() as session:
        listing = session.exec(
            select(RawListing).where(RawListing.id == listing_id)
        ).first()
        if listing is None:
            raise HTTPException(status_code=404, detail="Listing not found")

        if req.title is not None:
            listing.title = req.title
        if req.description is not None:
            listing.description = req.description
        if req.tags is not None:
            listing.tags = req.tags
        if req.price_cents is not None:
            listing.price_cents = req.price_cents

        listing.updated_at = datetime.now(timezone.utc)
        session.add(listing)
        session.commit()
        session.refresh(listing)

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


@router.post(
    "/listings/{listing_id}/publish",
    response_model=RawListingResponse,
    summary="Publish listing (status → listed)",
)
async def publish_raw_listing(listing_id: str):
    with _get_db_session() as session:
        listing = session.exec(
            select(RawListing).where(RawListing.id == listing_id)
        ).first()
        if listing is None:
            raise HTTPException(status_code=404, detail="Listing not found")

        if listing.status == "listed":
            raise HTTPException(status_code=409, detail="Listing is already published")

        if listing.status == "delisted":
            raise HTTPException(status_code=409, detail="Cannot publish a delisted listing")

        listing.status = "listed"
        listing.published_at = datetime.now(timezone.utc)
        listing.updated_at = datetime.now(timezone.utc)
        session.add(listing)
        session.commit()
        session.refresh(listing)

        logger.info("Published raw listing %s", listing_id)

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


@router.post(
    "/listings/{listing_id}/delist",
    response_model=RawListingResponse,
    summary="Delist a listing (status → delisted)",
)
async def delist_raw_listing(listing_id: str):
    with _get_db_session() as session:
        listing = session.exec(
            select(RawListing).where(RawListing.id == listing_id)
        ).first()
        if listing is None:
            raise HTTPException(status_code=404, detail="Listing not found")

        if listing.status != "listed":
            raise HTTPException(status_code=409, detail="Only listed listings can be delisted")

        listing.status = "delisted"
        listing.updated_at = datetime.now(timezone.utc)
        session.add(listing)
        session.commit()
        session.refresh(listing)

        logger.info("Delisted raw listing %s", listing_id)

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


# --- Download Endpoint (Entitlement-gated) ---

@router.get(
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
    # Validate entitlement from Authorization header
    auth_header = request.headers.get("Authorization", "")
    try:
        payload = ent_svc.validate_entitlement(auth_header)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))

    expected_hash = payload.get("file_hash", "")

    # Verify hash and get file
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
