"""
BQ-VZ-MULTI-USER: JWT Middleware & Role-Based Access Control
=============================================================

Provides:
    - JWT secret management (auto-generated, persisted to /data/jwt_secret.key)
    - JWT token creation and decoding
    - role_required(*roles) → FastAPI Depends() for endpoint-level role enforcement
    - require_admin, require_any convenience shortcuts

JWT auth runs IN PARALLEL with the existing X-API-Key system:
    1. Check vz_session cookie → decode JWT → verify role
    2. Fall back to X-API-Key header → existing BQ-127 flow → default role="admin"
    3. Neither present → 401

Phase: BQ-VZ-MULTI-USER — Admin/User Role Split
Created: 2026-03-03
"""

import logging
import secrets
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import jwt
from fastapi import HTTPException, Request, status

from app.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# JWT Secret Management
# ---------------------------------------------------------------------------
_JWT_SECRET_PATH = Path(settings.data_directory) / "jwt_secret.key"
_jwt_secret: Optional[str] = None

JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24
JWT_COOKIE_NAME = "vz_session"


def get_jwt_secret() -> str:
    """Return the JWT signing secret, generating and persisting if needed."""
    global _jwt_secret
    if _jwt_secret is not None:
        return _jwt_secret

    # Try reading from persisted file
    if _JWT_SECRET_PATH.exists():
        stored = _JWT_SECRET_PATH.read_text().strip()
        if stored:
            _jwt_secret = stored
            logger.info("Loaded JWT secret from %s", _JWT_SECRET_PATH)
            return _jwt_secret

    # Auto-generate
    generated = secrets.token_hex(32)
    try:
        _JWT_SECRET_PATH.parent.mkdir(parents=True, exist_ok=True)
        _JWT_SECRET_PATH.write_text(generated)
        _JWT_SECRET_PATH.chmod(0o600)
        logger.info("Generated and persisted JWT secret to %s", _JWT_SECRET_PATH)
    except OSError as exc:
        logger.warning("Could not persist JWT secret to %s: %s", _JWT_SECRET_PATH, exc)

    _jwt_secret = generated
    return _jwt_secret


def create_jwt_token(user_id: str, role: str) -> str:
    """Create a signed JWT token with user claims."""
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "role": role,
        "iat": now,
        "exp": now + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    return jwt.encode(payload, get_jwt_secret(), algorithm=JWT_ALGORITHM)


def decode_jwt_token(token: str) -> Optional[dict]:
    """Decode and validate a JWT token. Returns claims dict or None."""
    try:
        return jwt.decode(token, get_jwt_secret(), algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        logger.debug("JWT token expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.debug("Invalid JWT token: %s", e)
        return None


# ---------------------------------------------------------------------------
# Role-Based Access Control
# ---------------------------------------------------------------------------

def role_required(*roles: str):
    """FastAPI dependency factory for endpoint-level role enforcement.

    Checks authentication via JWT cookie or X-API-Key, then verifies the
    user's role is in the allowed set.

    Usage::

        @router.get("/admin-only")
        async def admin_endpoint(user = Depends(role_required("admin"))):
            ...

    Or at router level::

        app.include_router(router, dependencies=[Depends(role_required("admin"))])
    """
    async def _dependency(request: Request):
        from app.auth.api_key_auth import get_current_user, _is_auth_enabled, AuthenticatedUser

        # If auth is disabled (dev mode), allow everything
        if not _is_auth_enabled():
            request.state.user_role = "admin"
            # Still call get_current_user to set request.state.user
            user = await get_current_user(request)
            return user

        # 1. Try JWT cookie first
        token = request.cookies.get(JWT_COOKIE_NAME)
        if token:
            claims = decode_jwt_token(token)
            if claims:
                user_role = claims.get("role", "user")
                user_id = claims.get("sub")

                if user_role not in roles:
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail="Insufficient permissions",
                    )

                # Look up user to verify they're still active
                from app.services.auth_service import get_auth_service
                auth_svc = get_auth_service()
                user = await auth_svc.get_user_by_id(user_id)
                if not user or not user.is_active:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="User account is inactive or not found",
                    )

                # Set request state for downstream use
                request.state.user_role = user_role
                request.state.user = AuthenticatedUser(
                    user_id=user.id,
                    key_id="jwt_session",
                    scopes=_role_to_scopes(user_role),
                    valid=True,
                )
                return request.state.user

        # 2. Try X-API-Key (existing flow)
        api_key = request.headers.get("X-API-Key")
        if api_key:
            user = await get_current_user(request)
            # API key users default to "admin" role for backward compatibility
            request.state.user_role = "admin"
            if "admin" not in roles:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Insufficient permissions",
                )
            return user

        # 3. Neither present
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    return _dependency


def _role_to_scopes(role: str) -> list:
    """Map a user role to API scopes for backward compatibility."""
    if role == "admin":
        return ["read", "write", "admin"]
    return ["read", "write"]


# Convenience shortcuts
require_admin = role_required("admin")
require_any = role_required("admin", "user")
