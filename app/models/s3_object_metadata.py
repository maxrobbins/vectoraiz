"""
S3 Object Metadata Model
========================

One row per S3 object discovered during scan.
"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Column, ForeignKey, String
from sqlmodel import Field, SQLModel, Text


class S3ObjectMetadata(SQLModel, table=True):
    """Metadata record for a single S3 object."""

    __tablename__ = "s3_object_metadata"

    id: str = Field(primary_key=True, max_length=36)
    connection_id: str = Field(
        sa_column=Column(String(36), ForeignKey("s3_connection.id", ondelete="CASCADE"), nullable=False)
    )
    scan_job_id: str = Field(
        sa_column=Column(String(36), ForeignKey("s3_scan_job.id", ondelete="CASCADE"), nullable=False)
    )
    object_key: str = Field(max_length=1024)
    size_bytes: int
    content_type: str = Field(max_length=128)
    last_modified: datetime
    etag: str = Field(max_length=128)
    dataset_id: Optional[str] = Field(
        default=None,
        sa_column=Column(String(36), ForeignKey("dataset_records.id", ondelete="SET NULL"), nullable=True),
    )
    metadata_extracted_at: Optional[datetime] = Field(default=None, nullable=True)
    extraction_status: Optional[str] = Field(default=None, max_length=32, nullable=True)
    extraction_skip_reason: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
