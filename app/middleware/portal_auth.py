"""
BQ-VZ-SHARED-SEARCH: Portal Auth — JWT & ACL for the Portal Trust Zone
========================================================================

Mandate M2: Separate trust zone — portal JWT uses different signing key prefix,
            different iss/aud claims than admin JWT. Zero shared auth state.
Mandate M1: ACL enforcement on every portal endpoint.

Portal JWT claims:
    - iss: "vectoraiz-portal"
    - aud: "portal"
    - sub: session_id
    - tier: open|code|sso
    - psv: portal_session_version (must match config or token is stale)
"""

import logging
import secrets
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import jwt
from fastapi import Depends, HTTPException, Request, status

from app.config import settings
from app.models.portal import get_portal_config
from app.schemas.portal import PortalSession, PortalTier

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Portal JWT — SEPARATE signing key from admin JWT (Mandate M2)
# ---------------------------------------------------------------------------
_PORTAL_JWT_SECRET_PATH = Path(settings.data_directory) / "portal_jwt_secret.key"
_portal_jwt_secret: Optional[str] = None

PORTAL_JWT_ALGORITHM = "HS256"
PORTAL_JWT_ISSUER = "vectoraiz-portal"
PORTAL_JWT_AUDIENCE = "portal"


def get_portal_jwt_secret() -> str:
    """Return the portal JWT signing secret, generating and persisting if needed."""
    global _portal_jwt_secret
    if _portal_jwt_secret is not None:
        return _portal_jwt_secret

    if _PORTAL_JWT_SECRET_PATH.exists():
        stored = _PORTAL_JWT_SECRET_PATH.read_text().strip()
        if stored:
            _portal_jwt_secret = stored
            return _portal_jwt_secret

    generated = secrets.token_hex(32)
    try:
        _PORTAL_JWT_SECRET_PATH.parent.mkdir(parents=True, exist_ok=True)
        _PORTAL_JWT_SECRET_PATH.write_text(generated)
        _PORTAL_JWT_SECRET_PATH.chmod(0o600)
        logger.info("Generated portal JWT secret at %s", _PORTAL_JWT_SECRET_PATH)
    except OSError as exc:
        logger.warning("Could not persist portal JWT secret: %s", exc)

    _portal_jwt_secret = generated
    return _portal_jwt_secret


def create_portal_jwt(session: PortalSession) -> str:
    """Create a signed portal JWT token."""
    now = datetime.now(timezone.utc)
    payload = {
        "iss": PORTAL_JWT_ISSUER,
        "aud": PORTAL_JWT_AUDIENCE,
        "sub": session.session_id,
        "tier": session.tier.value,
        "psv": session.portal_session_version,
        "ip": session.ip_address,
        "iat": now,
        "exp": session.expires_at,
    }
    # Phase 2: Include OIDC claims in JWT for SSO sessions
    if session.oidc_subject:
        payload["oidc_sub"] = session.oidc_subject
    if session.oidc_email:
        payload["oidc_email"] = session.oidc_email
    if session.oidc_name:
        payload["oidc_name"] = session.oidc_name
    return jwt.encode(payload, get_portal_jwt_secret(), algorithm=PORTAL_JWT_ALGORITHM)


def decode_portal_jwt(token: str) -> Optional[dict]:
    """Decode and validate a portal JWT. Returns claims or None."""
    try:
        return jwt.decode(
            token,
            get_portal_jwt_secret(),
            algorithms=[PORTAL_JWT_ALGORITHM],
            issuer=PORTAL_JWT_ISSUER,
            audience=PORTAL_JWT_AUDIENCE,
        )
    except jwt.ExpiredSignatureError:
        logger.debug("Portal JWT expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.debug("Invalid portal JWT: %s", e)
        return None


# ---------------------------------------------------------------------------
# Portal Session Dependency — used on all portal endpoints
# ---------------------------------------------------------------------------

async def get_portal_session(request: Request) -> PortalSession:
    """FastAPI dependency: resolve portal session from tier.

    Open tier: anonymous session, no auth required (SS-C2).
    Code tier: requires valid portal JWT with matching portal_session_version.
    SSO tier: Phase 2 — not implemented.

    CRITICAL: This handles AUTH only. ACL checks happen per-endpoint.
    """
    config = get_portal_config()

    if not config.enabled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Portal is not enabled",
        )

    if config.tier == PortalTier.open:
        # Open tier: anonymous session, no auth needed (SS-C2)
        return PortalSession(
            session_id=secrets.token_hex(16),
            tier=PortalTier.open,
            ip_address=request.client.host if request.client else "unknown",
            created_at=datetime.now(timezone.utc),
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=config.session_ttl_minutes),
            portal_session_version=config.portal_session_version,
        )

    if config.tier == PortalTier.code:
        # Extract bearer token
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Portal authentication required",
            )

        token = auth_header[7:]
        claims = decode_portal_jwt(token)
        if not claims:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired portal session",
            )

        # Check portal_session_version matches (SS-C1)
        if claims.get("psv") != config.portal_session_version:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Session expired, please re-authenticate",
            )

        return PortalSession(
            session_id=claims["sub"],
            tier=PortalTier(claims["tier"]),
            ip_address=claims.get("ip", "unknown"),
            created_at=datetime.fromtimestamp(claims["iat"], tz=timezone.utc),
            expires_at=datetime.fromtimestamp(claims["exp"], tz=timezone.utc),
            portal_session_version=claims["psv"],
        )

    if config.tier == PortalTier.sso:
        # Phase 2: SSO tier — requires valid portal JWT with OIDC claims
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="SSO authentication required",
            )

        token = auth_header[7:]
        claims = decode_portal_jwt(token)
        if not claims:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired SSO session",
            )

        # Check portal_session_version matches (SS-C1 applies to SSO too)
        if claims.get("psv") != config.portal_session_version:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Session expired, please re-authenticate",
            )

        # Verify this is an SSO-tier token
        if claims.get("tier") != "sso":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid session tier",
            )

        return PortalSession(
            session_id=claims["sub"],
            tier=PortalTier.sso,
            ip_address=claims.get("ip", "unknown"),
            created_at=datetime.fromtimestamp(claims["iat"], tz=timezone.utc),
            expires_at=datetime.fromtimestamp(claims["exp"], tz=timezone.utc),
            portal_session_version=claims["psv"],
            oidc_subject=claims.get("oidc_sub"),
            oidc_email=claims.get("oidc_email"),
            oidc_name=claims.get("oidc_name"),
        )

    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Unknown portal tier",
    )


def check_dataset_acl(dataset_id: str) -> None:
    """Check that a dataset is portal-visible. Raises 403 if not (M1)."""
    config = get_portal_config()
    ds_config = config.datasets.get(dataset_id)
    if not ds_config or not ds_config.portal_visible:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Dataset is not available on this portal",
        )
