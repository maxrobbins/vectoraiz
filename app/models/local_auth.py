"""
BQ-127: Local Auth Models
=========================

SQLModel tables for standalone-mode authentication.
Used when VECTORAIZ_MODE=standalone (air-gapped, no ai.market dependency).

Tables:
    local_users     — Admin/user accounts with bcrypt password hashes.
    local_api_keys  — API keys with HMAC-SHA256 hashed secrets and scoped permissions.

Phase: BQ-127 — Air-Gap Architecture
Created: 2026-02-13
"""

from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from sqlmodel import Field, SQLModel


class LocalUser(SQLModel, table=True):
    """BQ-127: Local user account for standalone auth.

    Password is stored as a bcrypt hash. The ``role`` field controls
    admin vs. regular user permissions (currently only 'admin' in Phase 1).
    """

    __tablename__ = "local_users"

    id: str = Field(
        default_factory=lambda: str(uuid4()),
        primary_key=True,
        max_length=36,
    )
    username: str = Field(index=True, unique=True, max_length=255)
    password_hash: str = Field(max_length=255)
    role: str = Field(default="admin", max_length=50)
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class LocalAPIKey(SQLModel, table=True):
    """BQ-127: Local API key for standalone auth (C2).

    Key format: ``vz_<key_id>_<secret>``
    - ``key_id``: 8-char alphanumeric, stored in DB, indexed for O(1) lookup.
    - ``key_hash``: HMAC-SHA256 of the secret portion using VECTORAIZ_APIKEY_HMAC_SECRET.
    - The raw secret is NEVER stored — only shown once at creation time.
    """

    __tablename__ = "local_api_keys"

    id: str = Field(
        default_factory=lambda: str(uuid4()),
        primary_key=True,
        max_length=36,
    )
    user_id: str = Field(index=True, max_length=36, foreign_key="local_users.id")
    key_id: str = Field(index=True, unique=True, max_length=16)
    key_hash: str = Field(max_length=255)
    label: Optional[str] = Field(default=None, max_length=255)
    scopes: str = Field(default='["read","write"]')
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_used_at: Optional[datetime] = Field(default=None, nullable=True)
    revoked_at: Optional[datetime] = Field(default=None, nullable=True)
