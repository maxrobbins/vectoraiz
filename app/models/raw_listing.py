"""
Raw Listing Model
=================

SQLModel table for raw file marketplace listings.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-03
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlmodel import Field, SQLModel, Column, Text, JSON


class RawListing(SQLModel, table=True):
    """
    Marketplace listing backed by a raw file.

    Lifecycle: draft → listed → delisted
    """

    # TODO: BQ-VZ-DATA-CHANNEL — Ownership is inherited from the raw_file's
    # (future) user_id. Until user_id is added to raw_files, all listings are
    # effectively shared across authenticated admins. Must be resolved before
    # multi-tenant support.
    __tablename__ = "raw_listings"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True, max_length=36)
    raw_file_id: str = Field(foreign_key="raw_files.id", max_length=36)
    marketplace_listing_id: Optional[str] = Field(default=None, nullable=True, max_length=36)
    title: str = Field(max_length=256)
    description: str = Field(sa_column=Column(Text, nullable=False))
    tags: List[Any] = Field(default_factory=list, sa_column=Column(JSON, nullable=False, server_default="[]"))
    auto_metadata: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON, nullable=True))
    price_cents: Optional[int] = Field(default=None, nullable=True)
    status: str = Field(default="draft", index=True, max_length=32)
    published_at: Optional[datetime] = Field(default=None, nullable=True)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
