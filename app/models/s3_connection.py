"""
S3 Connection Model
===================

SQLModel table for seller-owned S3 bucket connection metadata.
STS role ARN and ExternalId are stored locally; no secret material is stored.
"""

from datetime import datetime, timezone
from typing import Optional

from pydantic import model_validator
from sqlalchemy import CheckConstraint
from sqlmodel import Column, Field, SQLModel, String, Text


class S3Connection(SQLModel, table=True):
    """Persistent record of an S3 STS connection."""

    __tablename__ = "s3_connection"
    __table_args__ = (
        CheckConstraint(
            "(status != 'configured') OR (role_arn IS NOT NULL AND external_id IS NOT NULL)",
            name="ck_s3_connection_configured_creds_required",
        ),
    )

    id: str = Field(primary_key=True, max_length=36)
    name: str = Field(max_length=255)
    bucket: str = Field(max_length=255)
    region: str = Field(max_length=64)
    role_arn: Optional[str] = Field(
        default=None,
        sa_column=Column(String(512), nullable=True),
    )
    external_id: Optional[str] = Field(
        default=None,
        sa_column=Column(String(128), nullable=True),
    )
    prefix: Optional[str] = Field(default=None, max_length=512, nullable=True)
    status: str = Field(default="configured", max_length=32, nullable=False)
    error_message: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    last_scanned_at: Optional[datetime] = Field(default=None, nullable=True)
    continuation_token: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))

    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def configured_connections_require_credentials(self) -> "S3Connection":
        if self.status == "configured" and (self.role_arn is None or self.external_id is None):
            raise ValueError("configured S3 connections require role_arn and external_id")
        return self
