"""
BQ-127: Dual-Mode API Key Authentication
==========================================

Dispatches authentication by VECTORAIZ_MODE:
    standalone — validate against local key store (HMAC-SHA256 by key_id)
    connected  — if key starts with ``vz_``, validate locally;
                 if key starts with ``aim_``, validate against ai.market.

Local key format (C2): ``vz_<key_id>_<secret>``
    - key_id: 8-char alphanumeric, indexed for O(1) lookup
    - secret: 32-char random, NEVER stored — only HMAC hash stored
    - HMAC uses VECTORAIZ_APIKEY_HMAC_SECRET (C1)

HMAC secret auto-generation: if VECTORAIZ_APIKEY_HMAC_SECRET is not set,
generate one, persist to /data/.vectoraiz_hmac_secret, and log WARNING.

Updated: S130 (2026-02-13) — BQ-127 Air-Gap Architecture
"""

import hashlib
import hmac
import json
import logging
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import httpx
from cachetools import TTLCache
from fastapi import Depends, HTTPException, Request, WebSocket, status
from pydantic import BaseModel, Field

from app.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AuthenticatedUser(BaseModel):
    """BQ-127: Unified user model returned by both local and ai.market auth paths."""

    user_id: str
    key_id: str
    scopes: List[str]
    is_valid: bool = Field(alias="valid", default=True)
    balance_cents: Optional[int] = 0
    free_trial_remaining_cents: Optional[int] = 0

    model_config = {"populate_by_name": True}


# In-memory cache for validated API keys
api_key_cache = TTLCache(maxsize=1000, ttl=settings.auth_cache_ttl)


def _is_auth_enabled() -> bool:
    """Check if auth is enabled.

    Auth can only be disabled when BOTH conditions are met:
      1. settings.debug is True
      2. ENVIRONMENT is 'development' (or VECTORAIZ_AUTH_ENABLED explicitly false)

    This prevents a single env var from killing auth in production.
    """
    env_value = os.environ.get("VECTORAIZ_AUTH_ENABLED", "").lower()
    if env_value in ("true", "1", "yes"):
        return True
    if env_value in ("false", "0", "no"):
        environment = os.environ.get("ENVIRONMENT", "production").lower()
        if settings.debug and environment == "development":
            logger.warning(
                "AUTH DISABLED: VECTORAIZ_AUTH_ENABLED=false with debug=True and ENVIRONMENT=development. "
                "Do NOT use this in production."
            )
            return False
        logger.warning(
            "Ignoring VECTORAIZ_AUTH_ENABLED=false because debug=%s and ENVIRONMENT=%s. "
            "Auth disable requires debug=True AND ENVIRONMENT=development.",
            settings.debug,
            environment,
        )
        return True
    return settings.auth_enabled


# ---------------------------------------------------------------------------
# BQ-127: HMAC secret management (C1)
# ---------------------------------------------------------------------------
_HMAC_SECRET_FILE = Path(settings.data_directory) / ".vectoraiz_hmac_secret"


def _get_hmac_secret() -> str:
    """Return the HMAC secret for local API key hashing.

    Priority:
        1. VECTORAIZ_APIKEY_HMAC_SECRET env var / settings
        2. Persisted file at /data/.vectoraiz_hmac_secret
        3. Auto-generate, persist, and log WARNING
    """
    if settings.apikey_hmac_secret:
        return settings.apikey_hmac_secret

    # Try reading from persisted file
    if _HMAC_SECRET_FILE.exists():
        stored = _HMAC_SECRET_FILE.read_text().strip()
        if stored:
            settings.apikey_hmac_secret = stored
            logger.info("Loaded HMAC secret from %s", _HMAC_SECRET_FILE)
            return stored

    # Auto-generate
    generated = secrets.token_hex(32)
    try:
        _HMAC_SECRET_FILE.parent.mkdir(parents=True, exist_ok=True)
        _HMAC_SECRET_FILE.write_text(generated)
        _HMAC_SECRET_FILE.chmod(0o600)
    except OSError as exc:
        logger.warning("Could not persist HMAC secret to %s: %s", _HMAC_SECRET_FILE, exc)

    settings.apikey_hmac_secret = generated
    logger.warning(
        "VECTORAIZ_APIKEY_HMAC_SECRET not set — auto-generated and persisted to %s. "
        "Set VECTORAIZ_APIKEY_HMAC_SECRET in production for stability across restarts.",
        _HMAC_SECRET_FILE,
    )
    return generated


def hmac_hash_secret(secret: str) -> str:
    """HMAC-SHA256 hash a key secret using the configured HMAC secret."""
    hmac_key = _get_hmac_secret().encode()
    return hmac.new(hmac_key, secret.encode(), hashlib.sha256).hexdigest()


# ---------------------------------------------------------------------------
# BQ-127: Local key validation (standalone mode)
# ---------------------------------------------------------------------------

def _parse_local_key(api_key: str) -> Optional[tuple]:
    """Parse a ``vz_<key_id>_<secret>`` key. Returns (key_id, secret) or None."""
    if not api_key.startswith("vz_"):
        return None
    parts = api_key.split("_", 2)  # ["vz", key_id, secret]
    if len(parts) != 3 or not parts[1] or not parts[2]:
        return None
    return parts[1], parts[2]


async def _validate_local_key(api_key: str) -> Optional[AuthenticatedUser]:
    """Validate a local ``vz_`` API key against the local_api_keys table.

    Performs O(1) lookup by key_id, then compares HMAC hashes.
    Updates last_used_at on successful validation.
    """
    parsed = _parse_local_key(api_key)
    if not parsed:
        return None
    key_id, secret = parsed

    # Lazy import to avoid circular deps
    from app.core.database import get_session_context
    from app.models.local_auth import LocalAPIKey, LocalUser
    from sqlmodel import select

    with get_session_context() as session:
        stmt = select(LocalAPIKey).where(
            LocalAPIKey.key_id == key_id,
            LocalAPIKey.revoked_at.is_(None),  # type: ignore[union-attr]
        )
        key_record = session.exec(stmt).first()
        if not key_record:
            return None

        # Verify HMAC
        expected_hash = hmac_hash_secret(secret)
        if not hmac.compare_digest(key_record.key_hash, expected_hash):
            return None

        # Load associated user
        user = session.get(LocalUser, key_record.user_id)
        if not user or not user.is_active:
            return None

        # Update last_used_at
        key_record.last_used_at = datetime.now(timezone.utc)
        session.add(key_record)
        session.commit()

        # Parse scopes from JSON string
        try:
            scopes = json.loads(key_record.scopes)
        except (json.JSONDecodeError, TypeError):
            scopes = ["read", "write"]

        return AuthenticatedUser(
            user_id=user.id,
            key_id=key_record.key_id,
            scopes=scopes,
            valid=True,
            balance_cents=0,
            free_trial_remaining_cents=0,
        )


# ---------------------------------------------------------------------------
# ai.market validation (connected mode — existing flow)
# ---------------------------------------------------------------------------

_http_client: Optional[httpx.AsyncClient] = None


def _get_http_client() -> httpx.AsyncClient:
    """Return shared httpx.AsyncClient, creating on first use."""
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=5.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
    return _http_client


async def _validate_key_against_aimarket(api_key: str) -> Optional[AuthenticatedUser]:
    """Makes an API call to ai.market to validate the key."""
    headers = {"X-API-Key": api_key}
    validation_url = f"{settings.ai_market_url}/api/v1/gateway/validate"
    client = _get_http_client()

    try:
        response = await client.post(validation_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            if data.get("valid"):
                return AuthenticatedUser(**data)
        elif response.status_code == 401:
            logger.warning(f"Invalid API key received: {api_key[:7]}...")
            return None
        else:
            logger.error(
                f"Error validating API key. ai.market returned status {response.status_code}. "
                f"Response: {response.text}"
            )
            return None
    except httpx.RequestError as exc:
        logger.error(f"HTTP request to ai.market failed: {exc}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service is currently unavailable.",
        )
    return None


# ---------------------------------------------------------------------------
# BQ-127: Unified get_current_user — dispatches by mode + key prefix
# ---------------------------------------------------------------------------

async def get_current_user(request: Request) -> AuthenticatedUser:
    """BQ-127 + BQ-VZ-MULTI-USER: Authenticate requests via JWT cookie or X-API-Key.

    Auth priority:
        1. vz_session httpOnly cookie (JWT) — BQ-VZ-MULTI-USER
        2. X-API-Key header — BQ-127 (standalone/connected)
    Dispatches by mode for X-API-Key:
        standalone: validate against local key store only
        connected:  vz_ prefix → local; aim_ prefix → ai.market
    """
    if not _is_auth_enabled():
        mock_user = AuthenticatedUser(
            user_id="mock_user_auth_disabled",
            key_id="mock_key_auth_disabled",
            scopes=["read"],
            valid=True,
            balance_cents=10000,
            free_trial_remaining_cents=0,
        )
        request.state.user = mock_user
        request.state.user_role = "admin"
        return mock_user

    # BQ-VZ-MULTI-USER: Try JWT cookie first
    from app.middleware.auth import JWT_COOKIE_NAME, decode_jwt_token
    token = request.cookies.get(JWT_COOKIE_NAME)
    if token:
        claims = decode_jwt_token(token)
        if claims:
            user_id = claims.get("sub")
            user_role = claims.get("role", "user")
            from app.services.auth_service import get_auth_service
            auth_svc = get_auth_service()
            user_obj = await auth_svc.get_user_by_id(user_id)
            if user_obj and user_obj.is_active:
                jwt_user = AuthenticatedUser(
                    user_id=user_obj.id,
                    key_id="jwt_session",
                    scopes=["read", "write", "admin"] if user_role == "admin" else ["read", "write"],
                    valid=True,
                )
                request.state.user = jwt_user
                request.state.user_role = user_role
                return jwt_user

    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated. Provide X-API-Key header or vz_session cookie.",
        )

    # Check cache first
    cached_user = api_key_cache.get(api_key)
    if cached_user:
        request.state.user = cached_user
        return cached_user

    validated_user: Optional[AuthenticatedUser] = None

    if settings.mode == "standalone":
        # Standalone: local keys only
        validated_user = await _validate_local_key(api_key)
    else:
        # Connected: dispatch by prefix — only vz_ (local) and aim_ (ai.market) accepted
        if api_key.startswith("vz_"):
            validated_user = await _validate_local_key(api_key)
        elif api_key.startswith("aim_"):
            validated_user = await _validate_key_against_aimarket(api_key)
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key prefix. Keys must start with 'vz_' (local) or 'aim_' (ai.market).",
            )

    if not validated_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key.",
        )

    # Store in cache and request state
    api_key_cache[api_key] = validated_user
    request.state.user = validated_user
    request.state.user_role = "admin"  # BQ-VZ-MULTI-USER: API key users default to admin
    return validated_user


async def get_current_user_ws(websocket: WebSocket) -> Optional[AuthenticatedUser]:
    """BQ-127: WebSocket variant of get_current_user.

    Reads API key from ?token= query parameter.
    Returns None if auth fails — caller must close the WebSocket.
    """
    if not _is_auth_enabled():
        return AuthenticatedUser(
            user_id="mock_user_auth_disabled",
            key_id="mock_key_auth_disabled",
            scopes=["read"],
            valid=True,
            balance_cents=10000,
            free_trial_remaining_cents=0,
        )

    api_key = websocket.query_params.get("token")
    if not api_key:
        return None

    # Check cache
    cached_user = api_key_cache.get(api_key)
    if cached_user:
        return cached_user

    validated_user: Optional[AuthenticatedUser] = None

    if settings.mode == "standalone":
        validated_user = await _validate_local_key(api_key)
    else:
        if api_key.startswith("vz_"):
            validated_user = await _validate_local_key(api_key)
        elif api_key.startswith("aim_"):
            try:
                validated_user = await _validate_key_against_aimarket(api_key)
            except HTTPException:
                return None
        else:
            return None  # Invalid prefix

    if not validated_user:
        return None

    api_key_cache[api_key] = validated_user
    return validated_user


# ---------------------------------------------------------------------------
# BQ-127: Scope enforcement dependency (C11)
# ---------------------------------------------------------------------------

def require_scope(scope: str):
    """FastAPI dependency factory that enforces a required scope on the authenticated user.

    Usage::

        @router.post("/generate")
        async def generate(user: AuthenticatedUser = Depends(require_scope("allai"))):
            ...
    """
    async def checker(user: AuthenticatedUser = Depends(get_current_user)):
        if scope not in user.scopes:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Scope '{scope}' required",
            )
        return user
    return checker


# ---------------------------------------------------------------------------
# API Key Management Helpers (BQ-098 — kept for backward compat)
# ---------------------------------------------------------------------------

def lookup_api_key_by_hash(key_hash: str) -> Optional[dict]:
    """Look up an API key record by its SHA-256 hash (legacy BQ-098)."""
    try:
        from app.routers.billing import _key_hash_index, _api_key_store
        key_id = _key_hash_index.get(key_hash)
        if key_id:
            return _api_key_store.get(key_id)
    except ImportError:
        pass
    return None


async def close_http_client():
    """Gracefully close the shared httpx client at shutdown."""
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None
