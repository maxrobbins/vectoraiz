"""
Raw Listings Schemas
====================

Pydantic request/response schemas for raw file and listing endpoints.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-03
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# --- Raw File Schemas ---

class RawFileRegisterRequest(BaseModel):
    file_path: str = Field(..., max_length=1024, description="Absolute path to raw file on disk")


class RawFileUpdateRequest(BaseModel):
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Structured metadata fields")


class RawFileResponse(BaseModel):
    id: str
    filename: str
    file_path: str
    file_size_bytes: int
    content_hash: str
    mime_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    listing_status: Optional[str] = Field(default=None, description="Listing status: draft/listed/none")
    price_cents: Optional[int] = Field(default=None, description="Listing price in cents (from associated listing)")
    created_at: datetime
    updated_at: datetime


# --- Raw Listing Schemas ---

class RawListingCreateRequest(BaseModel):
    raw_file_id: str = Field(..., description="UUID of the registered raw file")
    title: str = Field(..., max_length=256)
    description: str = Field(..., min_length=1)
    tags: List[str] = Field(default_factory=list, max_length=20)
    price_cents: Optional[int] = Field(default=None, ge=0)


class RawListingUpdateRequest(BaseModel):
    title: Optional[str] = Field(default=None, max_length=256)
    description: Optional[str] = Field(default=None, min_length=1)
    tags: Optional[List[str]] = None
    price_cents: Optional[int] = Field(default=None, ge=0)


class RawListingResponse(BaseModel):
    id: str
    raw_file_id: str
    marketplace_listing_id: Optional[str] = None
    title: str
    description: str
    tags: List[Any]
    auto_metadata: Optional[Dict[str, Any]] = None
    price_cents: Optional[int] = None
    status: str
    published_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class RawListingListResponse(BaseModel):
    listings: List[RawListingResponse]
    total: int
    limit: int
    offset: int


class MetadataResponse(BaseModel):
    file_id: str
    auto_metadata: Dict[str, Any]
