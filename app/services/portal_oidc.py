"""
BQ-VZ-SHARED-SEARCH Phase 2: OIDC Service — Discovery, Token Exchange, ID Token Validation
============================================================================================

Implements standard OIDC Authorization Code flow for portal SSO tier.
Uses httpx for async HTTP, PyJWT for RS256 ID token verification with JWKS.

All OIDC state is in-memory (local VZ instance, not cloud).
"""

import hashlib
import logging
import os
import secrets
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
import jwt
from jwt import PyJWKClient

from app.config import settings
from app.models.portal import get_portal_config
from app.schemas.portal import PortalAccessLog, PortalConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OIDC Discovery cache (1 hour TTL)
# ---------------------------------------------------------------------------
_discovery_cache: Dict[str, Tuple[Dict[str, Any], float]] = {}
DISCOVERY_TTL_SECONDS = 3600  # 1 hour

# ---------------------------------------------------------------------------
# OIDC nonce / state tracking (in-memory, prevent replay)
# ---------------------------------------------------------------------------
_pending_states: Dict[str, Dict[str, Any]] = {}  # state -> {nonce, created_at, redirect_uri}

# ---------------------------------------------------------------------------
# Refresh tokens (in-memory, keyed by session_id)
# ---------------------------------------------------------------------------
_refresh_tokens: Dict[str, str] = {}  # session_id -> refresh_token

# ---------------------------------------------------------------------------
# ID tokens (in-memory, keyed by session_id) — for id_token_hint on logout
# ---------------------------------------------------------------------------
_id_tokens: Dict[str, str] = {}  # session_id -> raw id_token string

# ---------------------------------------------------------------------------
# Access logs (in-memory, capped at 1000)
# ---------------------------------------------------------------------------
_access_logs: deque = deque(maxlen=1000)

# ---------------------------------------------------------------------------
# Enterprise gate
# ---------------------------------------------------------------------------
PORTAL_SSO_ENABLED = os.environ.get("PORTAL_SSO_ENABLED", "true").lower() == "true"


def is_sso_enabled() -> bool:
    """Check if SSO is available (enterprise gate)."""
    return PORTAL_SSO_ENABLED


# ---------------------------------------------------------------------------
# OIDC Secret Encryption (Fernet)
# ---------------------------------------------------------------------------

def _get_fernet():
    """Get Fernet instance for OIDC client secret encryption."""
    from cryptography.fernet import Fernet
    key = settings.get_secret_key()
    # Fernet needs a 32-byte url-safe base64-encoded key.
    # settings.get_secret_key() already returns a valid Fernet key.
    return Fernet(key.encode() if isinstance(key, str) else key)


def encrypt_client_secret(plaintext: str) -> str:
    """Encrypt OIDC client secret for storage."""
    f = _get_fernet()
    return f.encrypt(plaintext.encode()).decode()


def decrypt_client_secret(ciphertext: str) -> str:
    """Decrypt OIDC client secret."""
    f = _get_fernet()
    return f.decrypt(ciphertext.encode()).decode()


# ---------------------------------------------------------------------------
# OIDC Discovery
# ---------------------------------------------------------------------------

async def fetch_oidc_discovery(issuer: str) -> Dict[str, Any]:
    """Fetch and cache OIDC discovery document from {issuer}/.well-known/openid-configuration."""
    now = time.time()
    cached = _discovery_cache.get(issuer)
    if cached and (now - cached[1]) < DISCOVERY_TTL_SECONDS:
        return cached[0]

    url = f"{issuer.rstrip('/')}/.well-known/openid-configuration"
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        doc = resp.json()

    _discovery_cache[issuer] = (doc, now)
    logger.info("Fetched OIDC discovery from %s", url)
    return doc


def clear_discovery_cache():
    """Clear OIDC discovery cache (for testing)."""
    _discovery_cache.clear()


# ---------------------------------------------------------------------------
# Authorization URL
# ---------------------------------------------------------------------------

def build_authorize_url(config: PortalConfig, redirect_uri: str) -> str:
    """Build OIDC authorize URL with state and nonce for CSRF/replay protection."""
    import urllib.parse

    state = secrets.token_urlsafe(32)
    nonce = secrets.token_urlsafe(32)

    _pending_states[state] = {
        "nonce": nonce,
        "created_at": time.time(),
        "redirect_uri": redirect_uri,
    }
    # Clean up old states (> 10 min)
    _cleanup_pending_states()

    # Discovery must have been fetched already (caller handles async)
    cached = _discovery_cache.get(config.oidc_issuer or "")
    if not cached:
        raise ValueError("OIDC discovery not fetched — call fetch_oidc_discovery first")

    doc = cached[0]
    authorize_endpoint = doc["authorization_endpoint"]

    params = {
        "response_type": "code",
        "client_id": config.oidc_client_id,
        "redirect_uri": redirect_uri,
        "scope": "openid email profile",
        "state": state,
        "nonce": nonce,
    }
    return f"{authorize_endpoint}?{urllib.parse.urlencode(params)}"


def _cleanup_pending_states():
    """Remove states older than 10 minutes."""
    cutoff = time.time() - 600
    expired = [k for k, v in _pending_states.items() if v["created_at"] < cutoff]
    for k in expired:
        del _pending_states[k]


def validate_state(state: str) -> Optional[Dict[str, Any]]:
    """Validate and consume OIDC state parameter. Returns state data or None."""
    data = _pending_states.pop(state, None)
    if not data:
        return None
    # Check not expired (10 min)
    if time.time() - data["created_at"] > 600:
        return None
    return data


# ---------------------------------------------------------------------------
# Token Exchange
# ---------------------------------------------------------------------------

async def exchange_code_for_tokens(
    config: PortalConfig,
    code: str,
    redirect_uri: str,
) -> Dict[str, Any]:
    """Exchange authorization code for tokens (standard OIDC code flow)."""
    doc = await fetch_oidc_discovery(config.oidc_issuer)
    token_endpoint = doc["token_endpoint"]

    # Decrypt client secret
    client_secret = config.oidc_client_secret
    if client_secret and client_secret.startswith("gAAAAA"):
        # Encrypted (Fernet ciphertext starts with gAAAAA)
        client_secret = decrypt_client_secret(client_secret)

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(
            token_endpoint,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": config.oidc_client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# ID Token Validation
# ---------------------------------------------------------------------------

async def validate_id_token(
    config: PortalConfig,
    id_token_str: str,
    expected_nonce: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate OIDC ID token: signature (RS256 via JWKS), iss, aud, exp, nonce."""
    doc = await fetch_oidc_discovery(config.oidc_issuer)
    jwks_uri = doc["jwks_uri"]

    # Fetch JWKS and get signing key
    jwk_client = PyJWKClient(jwks_uri)
    signing_key = jwk_client.get_signing_key_from_jwt(id_token_str)

    claims = jwt.decode(
        id_token_str,
        signing_key.key,
        algorithms=["RS256", "RS384", "RS512"],
        audience=config.oidc_client_id,
        issuer=config.oidc_issuer,
    )

    # Verify nonce if provided
    if expected_nonce and claims.get("nonce") != expected_nonce:
        raise jwt.InvalidTokenError("Nonce mismatch")

    return claims


# ---------------------------------------------------------------------------
# Refresh Token
# ---------------------------------------------------------------------------

def _cleanup_expired_refresh_tokens():
    """Prune refresh/id tokens whose sessions have expired from active_sessions."""
    config = get_portal_config()
    expired = [sid for sid in _refresh_tokens if sid not in config.active_sessions]
    for sid in expired:
        _refresh_tokens.pop(sid, None)
        _id_tokens.pop(sid, None)


def store_refresh_token(session_id: str, refresh_token: str):
    """Store refresh token for a portal session (in-memory)."""
    _refresh_tokens[session_id] = refresh_token
    _cleanup_expired_refresh_tokens()


def get_refresh_token(session_id: str) -> Optional[str]:
    """Get stored refresh token for a portal session."""
    return _refresh_tokens.get(session_id)


def clear_session_tokens(session_id: str):
    """Clear refresh token and id token for a session."""
    _refresh_tokens.pop(session_id, None)
    _id_tokens.pop(session_id, None)


# Keep old name as alias for backwards compat in existing call sites
clear_refresh_token = clear_session_tokens


def store_id_token(session_id: str, id_token: str):
    """Store raw id_token string for id_token_hint on logout."""
    _id_tokens[session_id] = id_token


def get_id_token(session_id: str) -> Optional[str]:
    """Get stored id_token for a session."""
    return _id_tokens.get(session_id)


# ---------------------------------------------------------------------------
# Access Logging
# ---------------------------------------------------------------------------

def log_portal_access(
    session_id: str,
    oidc_subject: str,
    oidc_email: Optional[str],
    action: str,
    detail: Optional[str] = None,
):
    """Log a portal access event for SSO users."""
    entry = PortalAccessLog(
        timestamp=datetime.now(timezone.utc),
        session_id=session_id,
        oidc_subject=oidc_subject,
        oidc_email=oidc_email,
        action=action,
        detail=detail,
    )
    _access_logs.append(entry)
    logger.debug("Portal access log: %s %s %s", oidc_subject, action, detail or "")


def get_access_logs(limit: int = 100, offset: int = 0) -> List[PortalAccessLog]:
    """Get recent portal access logs."""
    logs = list(_access_logs)
    logs.reverse()  # Most recent first
    return logs[offset : offset + limit]


def clear_access_logs():
    """Clear all access logs (for testing)."""
    _access_logs.clear()


# ---------------------------------------------------------------------------
# Session cleanup helpers
# ---------------------------------------------------------------------------

def clear_sessions_only():
    """Clear session-related state but preserve audit logs (Gate 3)."""
    _discovery_cache.clear()
    _pending_states.clear()
    _refresh_tokens.clear()
    _id_tokens.clear()


def clear_all_sso_state():
    """Clear all in-memory SSO state (for testing)."""
    _discovery_cache.clear()
    _pending_states.clear()
    _refresh_tokens.clear()
    _id_tokens.clear()
    _access_logs.clear()


# ---------------------------------------------------------------------------
# IdP Logout
# ---------------------------------------------------------------------------

async def get_end_session_url(config: PortalConfig, id_token_hint: Optional[str] = None) -> Optional[str]:
    """Get IdP end_session_endpoint URL if available."""
    try:
        doc = await fetch_oidc_discovery(config.oidc_issuer)
        end_session = doc.get("end_session_endpoint")
        if end_session and id_token_hint:
            return f"{end_session}?id_token_hint={id_token_hint}"
        return end_session
    except Exception:
        return None
