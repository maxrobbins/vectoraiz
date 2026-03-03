"""
BQ-VZ-MULTI-USER: User Model for Multi-User Authentication
============================================================

SQLModel table for JWT-based multi-user authentication.
Separate from BQ-127's local_users/local_api_keys tables which handle
the X-API-Key auth system.

Table:
    users — Admin/user accounts with Argon2id password hashes and role-based access.

Phase: BQ-VZ-MULTI-USER — Admin/User Role Split
Created: 2026-03-03
"""

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Field, SQLModel


class User(SQLModel, table=True):
    """Multi-user account with Argon2id password hashing and role-based access."""

    __tablename__ = "users"

    id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        primary_key=True,
        max_length=36,
        description="UUID primary key",
    )
    username: str = Field(
        index=True,
        unique=True,
        max_length=64,
        description="Unique username (3-64 chars)",
    )
    display_name: Optional[str] = Field(
        default=None,
        max_length=128,
        description="Human-readable display name",
    )
    pw_hash: str = Field(
        max_length=255,
        description="Argon2id password hash",
    )
    role: str = Field(
        default="user",
        max_length=16,
        description="User role: 'admin' or 'user'",
    )
    is_active: bool = Field(
        default=True,
        description="Whether the user account is active",
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Account creation timestamp",
    )
    last_login_at: Optional[datetime] = Field(
        default=None,
        nullable=True,
        description="Last successful login timestamp",
    )
