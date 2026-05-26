"""
S3 Scan Job Model
=================

Tracks paginated S3 object enumeration progress for a connection.
"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Column, ForeignKey, String
from sqlmodel import Field, SQLModel, Text


class S3ScanJob(SQLModel, table=True):
    """Bucket scan progress for an S3 connection."""

    __tablename__ = "s3_scan_job"

    id: str = Field(primary_key=True, max_length=36)
    connection_id: str = Field(
        sa_column=Column(String(36), ForeignKey("s3_connection.id", ondelete="CASCADE"), nullable=False)
    )
    status: str = Field(default="pending", max_length=32, nullable=False)
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = Field(default=None, nullable=True)
    continuation_token: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    error_message: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    objects_enumerated: int = Field(default=0, nullable=False)

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
