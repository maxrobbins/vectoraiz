"""
Notification Model
==================

SQLModel table for persistent notification storage.
Supports typed, categorized notifications with read/unread state
and optional batch grouping for related events.

Phase: BQ-VZ-NOTIFICATIONS — Persistent Notification System
Created: 2026-03-04
"""

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Field, SQLModel, Column, Text


class Notification(SQLModel, table=True):
    """
    Persistent notification record.

    Supports multiple types (info/success/warning/error/action_required),
    categories (upload/processing/system/diagnostic), and optional batch
    grouping for related notifications (e.g. batch uploads).
    """

    __tablename__ = "notifications"

    id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        primary_key=True,
        max_length=36,
    )
    type: str = Field(max_length=32, index=True)  # info, success, warning, error, action_required
    category: str = Field(max_length=32, index=True)  # upload, processing, system, diagnostic
    title: str = Field(max_length=255)
    message: str = Field(sa_column=Column(Text))
    metadata_json: Optional[str] = Field(default=None, sa_column=Column(Text, nullable=True))
    read: bool = Field(default=False, index=True)
    batch_id: Optional[str] = Field(default=None, nullable=True, index=True, max_length=64)
    source: str = Field(default="system", max_length=32)  # system, allai, upload
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
