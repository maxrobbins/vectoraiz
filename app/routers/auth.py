"""
BQ-127 + BQ-VZ-MULTI-USER: Auth Router
========================================

Provides local authentication endpoints:
    GET  /api/auth/setup   — Check if first-run setup is available
    POST /api/auth/setup   — First-run admin creation (creates in both tables + sets JWT cookie)
    POST /api/auth/login   — Username/password → API key + JWT cookie
    POST /api/auth/logout  — Clears JWT cookie
    GET  /api/auth/me      — Current user info
    POST /api/auth/keys    — Create new API key for authenticated user
    GET  /api/auth/keys    — List user's API keys (masked)
    DELETE /api/auth/keys/{key_id} — Revoke a key
    GET  /api/auth/users   — List all users (admin only)
    POST /api/auth/users   — Create user (admin only)
    DELETE /api/auth/users/{user_id} — Deactivate user (admin only)
    POST /api/auth/users/{user_id}/reset-password — Reset password (admin only)

Phase: BQ-127 + BQ-VZ-MULTI-USER
"""

import json
import logging
import secrets
import string
from datetime import datetime, timezone
from typing import List, Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from app.config import settings
from app.auth.api_key_auth import (
    AuthenticatedUser,
    get_current_user,
    hmac_hash_secret,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Rate limiting (simple in-memory, per spec: 3/min for setup)
# ---------------------------------------------------------------------------
_setup_attempts: dict[str, list[float]] = {}
_SETUP_RATE_LIMIT = 3
_SETUP_RATE_WINDOW = 60  # seconds


def _check_rate_limit(client_ip: str) -> None:
    """BQ-127 (C10): Rate limit setup endpoint to 3 attempts/min per IP."""
    import time

    now = time.time()
    attempts = _setup_attempts.get(client_ip, [])
    # Prune old attempts
    attempts = [t for t in attempts if now - t < _SETUP_RATE_WINDOW]
    if len(attempts) >= _SETUP_RATE_LIMIT:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many setup attempts. Try again in 60 seconds.",
        )
    attempts.append(now)
    _setup_attempts[client_ip] = attempts


# ---------------------------------------------------------------------------
# Rate limiting for login (5 attempts per IP per 5 minutes)
# ---------------------------------------------------------------------------
_login_attempts: dict[str, list[float]] = {}
_LOGIN_RATE_LIMIT = 5
_LOGIN_RATE_WINDOW = 300  # 5 minutes


def _check_login_rate_limit(client_ip: str) -> None:
    """Rate limit login endpoint to 5 attempts per IP per 5 minutes."""
    import time

    now = time.time()
    attempts = _login_attempts.get(client_ip, [])
    attempts = [t for t in attempts if now - t < _LOGIN_RATE_WINDOW]
    if len(attempts) >= _LOGIN_RATE_LIMIT:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts. Try again later.",
            headers={"Retry-After": str(_LOGIN_RATE_WINDOW)},
        )
    attempts.append(now)
    _login_attempts[client_ip] = attempts


# ---------------------------------------------------------------------------
# BQ-127 Helpers (bcrypt for local_users backward compat)
# ---------------------------------------------------------------------------

def _prepare_password(password: str) -> bytes:
    """Pre-hash password with SHA-256 to handle bcrypt's 72-byte limit (bcrypt >= 5.0)."""
    import base64
    import hashlib
    return base64.b64encode(hashlib.sha256(password.encode()).digest())


def _hash_password(password: str) -> str:
    """Hash a password with bcrypt (SHA-256 pre-hash for long password safety)."""
    import bcrypt
    return bcrypt.hashpw(_prepare_password(password), bcrypt.gensalt()).decode()


def _verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against a bcrypt hash."""
    import bcrypt
    return bcrypt.checkpw(_prepare_password(password), password_hash.encode())


def _generate_key_id() -> str:
    """Generate an 8-char alphanumeric key_id for local API keys (C2)."""
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))


def _generate_key_secret() -> str:
    """Generate a 32-char random secret for local API keys."""
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(32))


def _create_api_key_for_user(
    user_id: str,
    label: str = "Default",
    scopes: Optional[List[str]] = None,
) -> dict:
    """BQ-127: Create a local API key, store HMAC hash in DB, return full key ONCE.

    Returns dict with: key_id, full_key, label, scopes, created_at
    """
    from app.core.database import get_session_context
    from app.models.local_auth import LocalAPIKey

    if scopes is None:
        scopes = ["read", "write", "admin"]

    key_id = _generate_key_id()
    secret = _generate_key_secret()
    full_key = f"vz_{key_id}_{secret}"
    key_hash = hmac_hash_secret(secret)

    now = datetime.now(timezone.utc)
    record = LocalAPIKey(
        id=str(uuid4()),
        user_id=user_id,
        key_id=key_id,
        key_hash=key_hash,
        label=label,
        scopes=json.dumps(scopes),
        created_at=now,
    )

    with get_session_context() as session:
        session.add(record)
        session.commit()

    logger.info("API key created: key_id=%s user_id=%s label=%s", key_id, user_id, label)

    return {
        "key_id": key_id,
        "full_key": full_key,
        "label": label,
        "scopes": scopes,
        "created_at": now.isoformat(),
    }


def _set_jwt_cookie(response: Response, user_id: str, role: str) -> None:
    """Set the vz_session JWT cookie on the response."""
    from app.middleware.auth import create_jwt_token, JWT_COOKIE_NAME, JWT_EXPIRY_HOURS

    token = create_jwt_token(user_id, role)
    response.set_cookie(
        key=JWT_COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        secure=False,  # LAN HTTP is fine
        max_age=JWT_EXPIRY_HOURS * 3600,
        path="/",
    )


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class SetupRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=64, description="Admin username")
    password: str = Field(..., min_length=8, max_length=255, description="Admin password (min 8 chars)")


class SetupResponse(BaseModel):
    user_id: str
    username: str
    api_key: str = Field(..., description="Full API key — shown ONCE, store it safely")
    message: str = "Admin account created. Save your API key — it cannot be retrieved later."


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    user_id: str
    username: str
    role: str
    api_key: Optional[str] = Field(default=None, description="Full API key — shown ONCE (BQ-127 compat)")
    message: str = "Login successful."


class CreateKeyRequest(BaseModel):
    label: str = Field(default="Untitled", max_length=255, description="Human-readable key label")
    scopes: List[str] = Field(default=["read", "write"], description="Scopes for this key")


class CreateKeyResponse(BaseModel):
    key_id: str
    full_key: str = Field(..., description="Full API key — shown ONCE")
    label: str
    scopes: List[str]
    created_at: str


class KeyInfo(BaseModel):
    key_id: str
    label: Optional[str]
    scopes: List[str]
    created_at: str
    last_used_at: Optional[str]
    revoked: bool


class UserInfo(BaseModel):
    user_id: str
    username: str
    display_name: Optional[str] = None
    role: str
    is_active: bool
    created_at: str
    last_login_at: Optional[str] = None


class CreateUserRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=64, description="Username")
    password: str = Field(..., min_length=8, max_length=255, description="Password (min 8 chars)")
    role: str = Field(default="user", description="Role: 'admin' or 'user'")
    display_name: Optional[str] = Field(default=None, max_length=128, description="Display name")


class ResetPasswordRequest(BaseModel):
    new_password: str = Field(..., min_length=8, max_length=255, description="New password (min 8 chars)")


# ---------------------------------------------------------------------------
# GET /api/auth/setup — Check if first-run setup is still available
# ---------------------------------------------------------------------------

@router.get(
    "/setup",
    summary="Check setup availability",
    description="Returns whether first-run setup is available (no users exist yet).",
)
async def check_setup():
    """Returns {needs_setup: bool} so the frontend can decide whether to show the setup form."""
    from app.services.auth_service import get_auth_service

    auth_svc = get_auth_service()
    needs = await auth_svc.needs_setup()

    # Also check BQ-127 local_users for backward compat
    from app.core.database import get_session_context
    from app.models.local_auth import LocalUser
    from sqlmodel import select, func

    with get_session_context() as session:
        local_count = session.exec(select(func.count()).select_from(LocalUser)).one()

    return {
        "needs_setup": needs and local_count == 0,
        "available": needs and local_count == 0,  # BQ-127 compat
    }


# ---------------------------------------------------------------------------
# POST /api/auth/setup — First-run admin creation
# ---------------------------------------------------------------------------

@router.post(
    "/setup",
    response_model=SetupResponse,
    status_code=status.HTTP_201_CREATED,
    summary="First-run setup — create admin account",
    description=(
        "Creates the first admin user. Only available when no users exist. "
        "Creates user in both users table (JWT auth) and local_users table (API key auth). "
        "Sets JWT cookie and returns API key for backward compatibility."
    ),
)
async def setup(body: SetupRequest, request: Request, response: Response):
    """First-run setup: create admin in both auth systems, set JWT cookie, return API key."""
    _check_rate_limit(request.client.host if request.client else "unknown")

    from app.services.auth_service import get_auth_service

    auth_svc = get_auth_service()

    # Check if setup is still available (users table)
    if not await auth_svc.needs_setup():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Setup is no longer available.",
        )

    # Also check BQ-127 local_users
    from app.core.database import get_session_context
    from app.models.local_auth import LocalUser
    from sqlmodel import select, func

    with get_session_context() as session:
        local_count = session.exec(select(func.count()).select_from(LocalUser)).one()
        if local_count > 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Setup is no longer available.",
            )

    # Create user in users table (Argon2id — BQ-VZ-MULTI-USER)
    try:
        user = await auth_svc.create_user(
            username=body.username,
            password=body.password,
            role="admin",
            display_name=body.username.strip(),
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        )

    # Create user in local_users table (bcrypt — BQ-127 backward compat)
    with get_session_context() as session:
        local_user = LocalUser(
            id=user.id,  # Same UUID for cross-table mapping
            username=body.username.strip().lower(),
            password_hash=_hash_password(body.password),
            role="admin",
            is_active=True,
        )
        session.add(local_user)
        session.commit()

    # Generate API key (BQ-127 backward compat)
    key_info = _create_api_key_for_user(
        user_id=user.id,
        label="Admin (setup)",
        scopes=["read", "write", "admin"],
    )

    # Set JWT cookie (BQ-VZ-MULTI-USER)
    _set_jwt_cookie(response, user.id, user.role)

    logger.info("First-run setup complete: user=%s (dual auth)", user.username)

    return SetupResponse(
        user_id=user.id,
        username=user.username,
        api_key=key_info["full_key"],
    )


# ---------------------------------------------------------------------------
# POST /api/auth/login — Username/password → JWT cookie + API key
# ---------------------------------------------------------------------------

@router.post(
    "/login",
    response_model=LoginResponse,
    summary="Login with username/password",
    description="Validates credentials, sets JWT cookie, and optionally returns API key.",
)
async def login(body: LoginRequest, request: Request, response: Response):
    """Authenticate against users table (Argon2id) first, fall back to local_users (bcrypt)."""
    _check_login_rate_limit(request.client.host if request.client else "unknown")

    from app.services.auth_service import get_auth_service

    auth_svc = get_auth_service()

    # Try users table first (Argon2id — BQ-VZ-MULTI-USER)
    user = await auth_svc.authenticate(body.username, body.password)
    if user:
        # Set JWT cookie
        _set_jwt_cookie(response, user.id, user.role)

        # Also generate API key for backward compat with existing frontend
        key_info = _create_api_key_for_user(
            user_id=user.id,
            label=f"Login ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})",
            scopes=["read", "write", "admin"] if user.role == "admin" else ["read", "write"],
        )

        return LoginResponse(
            user_id=user.id,
            username=user.username,
            role=user.role,
            api_key=key_info["full_key"],
        )

    # Fall back to local_users table (bcrypt — BQ-127)
    from app.core.database import get_session_context
    from app.models.local_auth import LocalUser
    from sqlmodel import select

    with get_session_context() as session:
        stmt = select(LocalUser).where(LocalUser.username == body.username.strip())
        local_user = session.exec(stmt).first()

    if local_user and local_user.is_active and _verify_password(body.password, local_user.password_hash):
        # Set JWT cookie using local_user info
        _set_jwt_cookie(response, local_user.id, local_user.role)

        # Generate API key
        key_info = _create_api_key_for_user(
            user_id=local_user.id,
            label=f"Login ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')})",
            scopes=["read", "write", "admin"],
        )

        return LoginResponse(
            user_id=local_user.id,
            username=local_user.username,
            role=local_user.role,
            api_key=key_info["full_key"],
        )

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid username or password.",
    )


# ---------------------------------------------------------------------------
# POST /api/auth/logout — Clear JWT cookie
# ---------------------------------------------------------------------------

@router.post(
    "/logout",
    summary="Logout",
    description="Clears the JWT session cookie.",
)
async def logout(response: Response):
    """Clear the vz_session cookie."""
    from app.middleware.auth import JWT_COOKIE_NAME

    response.delete_cookie(
        key=JWT_COOKIE_NAME,
        path="/",
        httponly=True,
        samesite="lax",
        secure=False,
    )
    return {"detail": "Logged out."}


# ---------------------------------------------------------------------------
# GET /api/auth/me — Current user info
# ---------------------------------------------------------------------------

@router.get(
    "/me",
    response_model=UserInfo,
    summary="Current user info",
    description="Returns information about the currently authenticated user.",
)
async def get_me(user: AuthenticatedUser = Depends(get_current_user)):
    """Return current user info — checks users table first, then local_users."""
    # Try users table first (BQ-VZ-MULTI-USER)
    from app.core.database import get_session_context
    from app.models.user import User

    with get_session_context() as session:
        mu_user = session.get(User, user.user_id)

    if mu_user:
        return UserInfo(
            user_id=mu_user.id,
            username=mu_user.username,
            display_name=mu_user.display_name,
            role=mu_user.role,
            is_active=mu_user.is_active,
            created_at=mu_user.created_at.isoformat() if mu_user.created_at else "",
            last_login_at=mu_user.last_login_at.isoformat() if mu_user.last_login_at else None,
        )

    # Fall back to local_users (BQ-127)
    from app.models.local_auth import LocalUser

    with get_session_context() as session:
        local_user = session.get(LocalUser, user.user_id)

    if local_user:
        return UserInfo(
            user_id=local_user.id,
            username=local_user.username,
            role=local_user.role,
            is_active=local_user.is_active,
            created_at=local_user.created_at.isoformat() if local_user.created_at else "",
        )

    # Fallback for ai.market users in connected mode
    return UserInfo(
        user_id=user.user_id,
        username=user.user_id,
        role="user",
        is_active=True,
        created_at="",
    )


# ---------------------------------------------------------------------------
# POST /api/auth/keys — Create new API key
# ---------------------------------------------------------------------------

@router.post(
    "/keys",
    response_model=CreateKeyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new API key",
    description="Creates a new API key for the authenticated user. The full key is returned ONCE.",
)
async def create_key(
    body: CreateKeyRequest,
    user: AuthenticatedUser = Depends(get_current_user),
):
    """BQ-127: Create a new API key for the authenticated user."""
    key_info = _create_api_key_for_user(
        user_id=user.user_id,
        label=body.label,
        scopes=body.scopes,
    )

    return CreateKeyResponse(**key_info)


# ---------------------------------------------------------------------------
# GET /api/auth/keys — List user's API keys (masked)
# ---------------------------------------------------------------------------

@router.get(
    "/keys",
    response_model=List[KeyInfo],
    summary="List API keys",
    description="Returns all API keys for the authenticated user with masked secrets.",
)
async def list_keys(user: AuthenticatedUser = Depends(get_current_user)):
    """BQ-127: List API keys for the authenticated user (secrets masked)."""
    from app.core.database import get_session_context
    from app.models.local_auth import LocalAPIKey
    from sqlmodel import select

    with get_session_context() as session:
        stmt = select(LocalAPIKey).where(LocalAPIKey.user_id == user.user_id)
        keys = session.exec(stmt).all()

    result = []
    for k in keys:
        try:
            scopes = json.loads(k.scopes)
        except (json.JSONDecodeError, TypeError):
            scopes = ["read", "write"]

        result.append(KeyInfo(
            key_id=k.key_id,
            label=k.label,
            scopes=scopes,
            created_at=k.created_at.isoformat() if k.created_at else "",
            last_used_at=k.last_used_at.isoformat() if k.last_used_at else None,
            revoked=k.revoked_at is not None,
        ))

    return result


# ---------------------------------------------------------------------------
# DELETE /api/auth/keys/{key_id} — Revoke a key
# ---------------------------------------------------------------------------

@router.delete(
    "/keys/{key_id}",
    status_code=status.HTTP_200_OK,
    summary="Revoke an API key",
    description="Soft-revokes an API key by setting revoked_at timestamp.",
)
async def revoke_key(
    key_id: str,
    user: AuthenticatedUser = Depends(get_current_user),
):
    """BQ-127: Revoke an API key by key_id."""
    from app.core.database import get_session_context
    from app.models.local_auth import LocalAPIKey
    from sqlmodel import select

    with get_session_context() as session:
        stmt = select(LocalAPIKey).where(
            LocalAPIKey.key_id == key_id,
            LocalAPIKey.user_id == user.user_id,
        )
        key_record = session.exec(stmt).first()
        if not key_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Key '{key_id}' not found.",
            )

        if key_record.revoked_at is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Key '{key_id}' is already revoked.",
            )

        key_record.revoked_at = datetime.now(timezone.utc)
        session.add(key_record)
        session.commit()

    # Invalidate cache for this key
    from app.auth.api_key_auth import api_key_cache
    keys_to_remove = [k for k, v in api_key_cache.items() if v.key_id == key_id]
    for k in keys_to_remove:
        api_key_cache.pop(k, None)

    logger.info("API key revoked: key_id=%s by user=%s", key_id, user.user_id)
    return {"detail": f"Key '{key_id}' revoked."}


# ---------------------------------------------------------------------------
# BQ-VZ-MULTI-USER: User management endpoints (admin only)
# ---------------------------------------------------------------------------

@router.get(
    "/users",
    response_model=List[UserInfo],
    summary="List all users (admin only)",
    description="Returns all users from the multi-user auth system.",
)
async def list_users(user: AuthenticatedUser = Depends(get_current_user)):
    """List all users. Requires admin role."""
    from app.middleware.auth import require_admin
    # Manual role check since we can't easily use Depends inside Depends
    _role = getattr(getattr(user, '_request', None), 'state', None)
    # Check via request state or JWT claims
    from app.services.auth_service import get_auth_service
    auth_svc = get_auth_service()

    # Verify caller is admin
    caller = await auth_svc.get_user_by_id(user.user_id)
    if caller and caller.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required.",
        )

    users = await auth_svc.list_users()
    return [
        UserInfo(
            user_id=u.id,
            username=u.username,
            display_name=u.display_name,
            role=u.role,
            is_active=u.is_active,
            created_at=u.created_at.isoformat() if u.created_at else "",
            last_login_at=u.last_login_at.isoformat() if u.last_login_at else None,
        )
        for u in users
    ]


@router.post(
    "/users",
    response_model=UserInfo,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new user (admin only)",
    description="Creates a new user account. Requires admin role.",
)
async def create_user(
    body: CreateUserRequest,
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Create a new user. Requires admin role."""
    from app.services.auth_service import get_auth_service

    auth_svc = get_auth_service()

    # Verify caller is admin
    caller = await auth_svc.get_user_by_id(user.user_id)
    if caller and caller.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required.",
        )

    if body.role not in ("admin", "user"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role must be 'admin' or 'user'.",
        )

    try:
        new_user = await auth_svc.create_user(
            username=body.username,
            password=body.password,
            role=body.role,
            display_name=body.display_name,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        )

    # Also create in local_users for API key backward compat
    from app.core.database import get_session_context
    from app.models.local_auth import LocalUser

    with get_session_context() as session:
        local_user = LocalUser(
            id=new_user.id,
            username=new_user.username,
            password_hash=_hash_password(body.password),
            role=body.role,
            is_active=True,
        )
        session.add(local_user)
        session.commit()

    return UserInfo(
        user_id=new_user.id,
        username=new_user.username,
        display_name=new_user.display_name,
        role=new_user.role,
        is_active=new_user.is_active,
        created_at=new_user.created_at.isoformat() if new_user.created_at else "",
    )


@router.delete(
    "/users/{user_id}",
    summary="Deactivate a user (admin only)",
    description="Deactivates a user account. The user can no longer log in.",
)
async def deactivate_user(
    user_id: str,
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Deactivate a user. Requires admin role."""
    from app.services.auth_service import get_auth_service

    auth_svc = get_auth_service()

    # Verify caller is admin
    caller = await auth_svc.get_user_by_id(user.user_id)
    if caller and caller.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required.",
        )

    # Prevent self-deactivation
    if user_id == user.user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot deactivate your own account.",
        )

    success = await auth_svc.deactivate_user(user_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found.",
        )

    # Also deactivate in local_users if exists
    from app.core.database import get_session_context
    from app.models.local_auth import LocalUser

    with get_session_context() as session:
        local_user = session.get(LocalUser, user_id)
        if local_user:
            local_user.is_active = False
            session.add(local_user)
            session.commit()

    return {"detail": "User deactivated."}


@router.post(
    "/users/{user_id}/reset-password",
    summary="Reset user password (admin only)",
    description="Resets a user's password. Requires admin role.",
)
async def admin_reset_password(
    user_id: str,
    body: ResetPasswordRequest,
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Reset a user's password. Requires admin role."""
    from app.services.auth_service import get_auth_service

    auth_svc = get_auth_service()

    # Verify caller is admin
    caller = await auth_svc.get_user_by_id(user.user_id)
    if caller and caller.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required.",
        )

    success = await auth_svc.reset_password(user_id, body.new_password)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found.",
        )

    # Also update in local_users if exists
    from app.core.database import get_session_context
    from app.models.local_auth import LocalUser

    with get_session_context() as session:
        local_user = session.get(LocalUser, user_id)
        if local_user:
            local_user.password_hash = _hash_password(body.new_password)
            session.add(local_user)
            session.commit()

    return {"detail": "Password reset successfully."}
