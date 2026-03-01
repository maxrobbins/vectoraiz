"""
Fulfillment Log Model
=====================

SQLModel table for tracking fulfillment state locally.
Records every fulfillment request and its outcome.

Phase: BQ-D1 — Fulfillment Listener
Created: 2026-02-23

IMPORTANT: Excludes tokens, auth headers, chunk payloads, and buyer PII (MP-M23).
"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import BigInteger
from sqlmodel import Field, SQLModel, Column, Text


class FulfillmentLog(SQLModel, table=True):
    """
    Local log of fulfillment transfers.

    Each row tracks a single fulfillment deliver request from ai.market,
    from receipt through completion or failure.
    """

    __tablename__ = "fulfillment_log"

    id: str = Field(primary_key=True, max_length=36)
    transfer_id: str = Field(unique=True, index=True, max_length=36)
    order_id: str = Field(max_length=255)
    listing_id: str = Field(max_length=255, index=True)
    request_id: str = Field(max_length=255)
    status: str = Field(default="received", index=True)  # received → uploading → completed | failed | timed_out
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = Field(default=None, nullable=True)
    file_size_bytes: Optional[int] = Field(default=None, sa_column=Column(BigInteger, nullable=True))
    chunks_sent: Optional[int] = Field(default=None, nullable=True)
    error_code: Optional[str] = Field(default=None, nullable=True, max_length=64)
    error_message: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
