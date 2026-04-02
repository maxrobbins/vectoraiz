"""
Cached Request & Response Draft Models
=======================================

Local SQLite models for the BQ-VZ-REQUEST-ENGINE.
CachedRequest stores buyer requests synced from ai.market.
ResponseDraft stores seller match/response drafts.

Phase: BQ-VZ-REQUEST-ENGINE Slice B
Created: 2026-04-02
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from sqlmodel import Field, SQLModel, Column, Text


class DraftStatus(str, Enum):
    """Response draft lifecycle states."""
    DRAFT = "draft"
    SUBMITTED = "submitted"
    REJECTED = "rejected"


class CachedRequest(SQLModel, table=True):
    """Buyer data request cached locally from ai.market."""

    __tablename__ = "cached_requests"

    id: str = Field(primary_key=True, max_length=36)
    marketplace_request_id: str = Field(
        max_length=36, unique=True, index=True,
        description="UUID from ai.market — dedup key",
    )
    title: str = Field(max_length=512)
    description: str = Field(default="", sa_column=Column(Text, default=""))
    categories: str = Field(default="[]", sa_column=Column(Text, default="[]"))
    urgency: Optional[str] = Field(default=None, nullable=True, max_length=32)
    status: str = Field(default="open", index=True, max_length=32)
    published_at: Optional[datetime] = Field(default=None, nullable=True)
    expires_at: Optional[datetime] = Field(default=None, nullable=True)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    synced_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    match_run_at: Optional[datetime] = Field(default=None, nullable=True)


class ResponseDraft(SQLModel, table=True):
    """Seller's draft response matching a cached request to a local dataset."""

    __tablename__ = "response_drafts"

    id: str = Field(primary_key=True, max_length=36)
    cached_request_id: str = Field(
        max_length=36, index=True, foreign_key="cached_requests.id",
    )
    matched_dataset_id: str = Field(max_length=36, index=True)
    title: str = Field(max_length=512)
    description: str = Field(default="", sa_column=Column(Text, default=""))
    score: float = Field(default=0.0)
    score_reasons: str = Field(default="{}", sa_column=Column(Text, default="{}"))
    require_review: bool = Field(default=True)
    status: str = Field(default="draft", index=True, max_length=16)
    internal_notes: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
