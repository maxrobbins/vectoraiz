"""
BQ-VZ-MULTI-USER: Authentication Service
==========================================

Core auth logic for the multi-user system:
    - User CRUD (create, get, list, deactivate)
    - Password hashing (Argon2id via argon2-cffi)
    - Authentication (username + password → User)
    - Password reset

Phase: BQ-VZ-MULTI-USER — Admin/User Role Split
Created: 2026-03-03
"""

import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError

logger = logging.getLogger(__name__)

# Argon2id hasher with secure defaults
_ph = PasswordHasher()


def hash_password(password: str) -> str:
    """Hash a password using Argon2id."""
    return _ph.hash(password)


def verify_password(password: str, pw_hash: str) -> bool:
    """Verify a password against an Argon2id hash."""
    try:
        return _ph.verify(pw_hash, password)
    except VerifyMismatchError:
        return False


class AuthService:
    """Service for multi-user authentication and user management."""

    async def create_user(
        self,
        username: str,
        password: str,
        role: str = "user",
        display_name: Optional[str] = None,
    ):
        """Create a new user with Argon2id-hashed password.

        Returns the created User object.
        Raises ValueError if username already exists.
        """
        from app.core.database import get_session_context
        from app.models.user import User
        from sqlmodel import select

        username = username.strip().lower()

        with get_session_context() as session:
            existing = session.exec(
                select(User).where(User.username == username)
            ).first()
            if existing:
                raise ValueError(f"Username '{username}' already exists")

            user = User(
                id=str(uuid4()),
                username=username,
                display_name=display_name or username,
                pw_hash=hash_password(password),
                role=role,
                is_active=True,
            )
            session.add(user)
            session.commit()
            session.refresh(user)

            logger.info("User created: username=%s role=%s", username, role)
            return user

    async def authenticate(self, username: str, password: str):
        """Authenticate a user by username and password.

        Returns the User on success, None on failure.
        Updates last_login_at on successful authentication.
        """
        from app.core.database import get_session_context
        from app.models.user import User
        from sqlmodel import select

        username = username.strip().lower()

        with get_session_context() as session:
            user = session.exec(
                select(User).where(User.username == username)
            ).first()

            if not user or not user.is_active:
                return None

            if not verify_password(password, user.pw_hash):
                return None

            # Update last_login_at
            user.last_login_at = datetime.now(timezone.utc)
            session.add(user)
            session.commit()
            session.refresh(user)

            return user

    async def get_user_by_id(self, user_id: str):
        """Look up a user by their UUID."""
        from app.core.database import get_session_context
        from app.models.user import User

        with get_session_context() as session:
            user = session.get(User, user_id)
            if user:
                # Detach from session by accessing all fields
                _ = user.id, user.username, user.display_name, user.role
                _ = user.is_active, user.created_at, user.last_login_at, user.pw_hash
            return user

    async def list_users(self):
        """List all users (admin only)."""
        from app.core.database import get_session_context
        from app.models.user import User
        from sqlmodel import select

        with get_session_context() as session:
            users = session.exec(select(User)).all()
            return list(users)

    async def deactivate_user(self, user_id: str) -> bool:
        """Deactivate a user account. Returns True if successful."""
        from app.core.database import get_session_context
        from app.models.user import User

        with get_session_context() as session:
            user = session.get(User, user_id)
            if not user:
                return False

            user.is_active = False
            session.add(user)
            session.commit()

            logger.info("User deactivated: user_id=%s username=%s", user_id, user.username)
            return True

    async def reset_password(self, user_id: str, new_password: str) -> bool:
        """Reset a user's password. Returns True if successful."""
        from app.core.database import get_session_context
        from app.models.user import User

        with get_session_context() as session:
            user = session.get(User, user_id)
            if not user:
                return False

            user.pw_hash = hash_password(new_password)
            session.add(user)
            session.commit()

            logger.info("Password reset: user_id=%s username=%s", user_id, user.username)
            return True

    async def needs_setup(self) -> bool:
        """Check if the users table is empty (needs first admin setup)."""
        from app.core.database import get_session_context
        from app.models.user import User
        from sqlmodel import select, func

        with get_session_context() as session:
            count = session.exec(select(func.count()).select_from(User)).one()
            return count == 0


# Module-level singleton
_auth_service: Optional[AuthService] = None


def get_auth_service() -> AuthService:
    """Return the singleton AuthService instance."""
    global _auth_service
    if _auth_service is None:
        _auth_service = AuthService()
    return _auth_service
