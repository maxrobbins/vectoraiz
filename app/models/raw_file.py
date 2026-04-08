"""
Raw File Model
==============

SQLModel table for raw file metadata storage.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-03
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import BigInteger, JSON
from sqlmodel import Field, SQLModel, Column


class RawFile(SQLModel, table=True):
    """
    Metadata record for a raw file registered for marketplace listing.

    The actual file lives on disk at file_path; this table tracks
    the catalog entry including content hash for integrity verification.
    """

    __tablename__ = "raw_files"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True, max_length=36)
    filename: str = Field(max_length=512)
    file_path: str = Field(max_length=1024)
    file_size_bytes: int = Field(sa_column=Column(BigInteger, nullable=False))
    content_hash: str = Field(max_length=64)  # SHA256 hex digest
    mime_type: Optional[str] = Field(default=None, nullable=True, max_length=128)
    metadata_: Optional[Dict[str, Any]] = Field(
        default=None,
        sa_column=Column("metadata", JSON, nullable=True),
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
