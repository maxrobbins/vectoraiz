"""
Dataset Record Model
====================

SQLModel table for persistent dataset metadata storage.
Replaces the in-memory dict + datasets.json approach.

Phase: BQ-111 — Persistent State
Created: 2026-02-12
Updated: BQ-108+109 — Enhanced Upload Pipeline (batch, preview, confirm)
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from sqlalchemy import BigInteger
from sqlmodel import Field, SQLModel, Column, Text


class DatasetStatus(str, Enum):
    """Dataset lifecycle states (BQ-108+109)."""
    UPLOADED = "uploaded"            # File saved, extraction not started
    EXTRACTING = "extracting"       # Text/metadata extraction running
    PREVIEW_READY = "preview_ready" # Extraction done, awaiting user confirm
    INDEXING = "indexing"            # Chunking + embedding + Qdrant storage
    READY = "ready"                 # Fully indexed and searchable
    CANCELLED = "cancelled"         # User cancelled before indexing
    ERROR = "error"                 # Any stage failed


class DatasetRecord(SQLModel, table=True):
    """
    Persistent record of an uploaded/processed dataset.

    Stores metadata about the file, its processing status, and paths.
    The actual data lives in Parquet files on disk; this table tracks
    the catalog of known datasets.
    """

    __tablename__ = "dataset_records"

    id: str = Field(primary_key=True, max_length=36)
    original_filename: str = Field(max_length=512)
    storage_filename: str = Field(max_length=512)
    file_type: str = Field(max_length=32)
    file_size_bytes: int = Field(default=0, sa_column=Column(BigInteger, default=0))
    status: str = Field(default="uploaded", index=True)
    processed_path: Optional[str] = Field(default=None, nullable=True, max_length=1024)
    metadata_json: str = Field(default="{}", sa_column=Column(Text, default="{}"))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # BQ-108+109: Batch upload + preview fields
    batch_id: Optional[str] = Field(default=None, nullable=True, index=True, max_length=64)
    relative_path: Optional[str] = Field(default=None, nullable=True, max_length=1024)
    preview_text: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    preview_metadata: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    confirmed_at: Optional[datetime] = Field(default=None, nullable=True)
    confirmed_by: Optional[str] = Field(default=None, nullable=True, max_length=64)

    # BQ-MCP-RAG: External LLM connectivity (M20 — least privilege, default FALSE)
    externally_queryable: bool = Field(default=True)

    # BQ-D1: Marketplace listing ID (set when published to ai.market)
    listing_id: Optional[str] = Field(default=None, nullable=True, index=True, max_length=255)
