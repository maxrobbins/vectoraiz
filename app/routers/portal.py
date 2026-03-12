"""
BQ-VZ-SHARED-SEARCH: Portal Router — /api/portal/* endpoints
==============================================================

All portal endpoints live here. Completely separate from admin routes (M2).
ACL enforced on every dataset access (M1).
"""

import logging
import secrets
from datetime import datetime, timezone, timedelta

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Query, status
from fastapi.responses import RedirectResponse

from app.core.async_utils import run_sync
from app.middleware.portal_auth import (
    get_portal_session,
    create_portal_jwt,
    check_dataset_acl,
)
from app.models.portal import (
    AccessCodeValidator,
    get_portal_config,
    save_portal_config,
)
from app.schemas.portal import (
    DatasetPortalConfig,
    PortalAuthRequest,
    PortalAuthResponse,
    PortalConfigUpdate,
    PortalPublicConfig,
    PortalSearchQuery,
    PortalSession,
    PortalTier,
)
from app.services.portal_service import get_portal_service
from app.services.portal_oidc import (
    is_sso_enabled,
    fetch_oidc_discovery,
    build_authorize_url,
    validate_state,
    exchange_code_for_tokens,
    validate_id_token,
    store_refresh_token,
    store_id_token,
    get_id_token,
    clear_refresh_token,
    log_portal_access,
    get_access_logs,
    get_end_session_url,
    encrypt_client_secret,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Public portal endpoints (no admin auth)
# ---------------------------------------------------------------------------

@router.get("/config")
async def get_portal_public_config():
    """Public: returns tier type, portal name, branding. No secrets."""
    config = get_portal_config()
    return PortalPublicConfig(
        enabled=config.enabled,
        tier=config.tier,
        name="Search Portal",
    )


@router.post("/auth/code", response_model=PortalAuthResponse)
async def authenticate_with_code(body: PortalAuthRequest, request: Request):
    """Shared access code authentication. Rate-limited per IP (M5)."""
    config = get_portal_config()

    if not config.enabled:
        raise HTTPException(status_code=404, detail="Portal is not enabled")

    if config.tier != PortalTier.code:
        raise HTTPException(status_code=400, detail="Access code auth not available for this portal tier")

    client_ip = request.client.host if request.client else "unknown"

    # Rate limit check (M5)
    if not AccessCodeValidator.check_rate_limit(client_ip):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many attempts. Try again later.",
        )

    # Record attempt before verification
    AccessCodeValidator.record_attempt(client_ip)

    # Verify code
    if not config.access_code_hash:
        raise HTTPException(status_code=500, detail="Portal access code not configured")

    if not AccessCodeValidator.verify_code(body.code, config.access_code_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access code",
        )

    # Issue portal JWT (SS-C1)
    now = datetime.now(timezone.utc)
    session = PortalSession(
        session_id=secrets.token_hex(16),
        tier=PortalTier.code,
        ip_address=client_ip,
        created_at=now,
        expires_at=now + timedelta(minutes=config.session_ttl_minutes),
        portal_session_version=config.portal_session_version,
    )

    token = create_portal_jwt(session)

    # Track session (SS-C3)
    config.active_sessions[session.session_id] = session.expires_at.isoformat()
    save_portal_config(config)

    return PortalAuthResponse(
        token=token,
        expires_at=session.expires_at,
        tier=session.tier.value,
    )


# ---------------------------------------------------------------------------
# SSO OIDC endpoints (Phase 2)
# ---------------------------------------------------------------------------

@router.get("/auth/sso/authorize")
async def sso_authorize(request: Request):
    """Redirect to IdP authorize endpoint (OIDC Authorization Code flow)."""
    config = get_portal_config()

    if not config.enabled:
        raise HTTPException(status_code=404, detail="Portal is not enabled")

    if config.tier != PortalTier.sso:
        raise HTTPException(status_code=400, detail="SSO auth not available for this portal tier")

    if not is_sso_enabled():
        raise HTTPException(status_code=403, detail="SSO requires Enterprise license")

    if not config.oidc_issuer or not config.oidc_client_id or not config.oidc_client_secret:
        raise HTTPException(status_code=400, detail="OIDC is not configured")

    # Fetch discovery document
    try:
        await fetch_oidc_discovery(config.oidc_issuer)
    except Exception as e:
        logger.error("OIDC discovery failed: %s", e)
        raise HTTPException(status_code=502, detail="Failed to reach identity provider")

    # Build callback URL
    callback_url = f"{config.base_url.rstrip('/')}/api/portal/auth/sso/callback"

    authorize_url = build_authorize_url(config, callback_url)
    return RedirectResponse(url=authorize_url, status_code=302)


@router.get("/auth/sso/callback")
async def sso_callback(
    request: Request,
    code: str = Query(...),
    state: str = Query(...),
):
    """Handle IdP callback: exchange code for tokens, create portal session."""
    config = get_portal_config()

    if not config.enabled or config.tier != PortalTier.sso:
        raise HTTPException(status_code=400, detail="SSO not active")

    # Validate state (CSRF protection)
    state_data = validate_state(state)
    if not state_data:
        raise HTTPException(status_code=400, detail="Invalid or expired state parameter")

    redirect_uri = state_data["redirect_uri"]
    expected_nonce = state_data["nonce"]

    # Exchange code for tokens
    try:
        tokens = await exchange_code_for_tokens(config, code, redirect_uri)
    except httpx.HTTPStatusError as e:
        logger.error("Token exchange failed: %s", e)
        raise HTTPException(status_code=401, detail="Token exchange failed")
    except Exception as e:
        logger.error("Token exchange error: %s", e)
        raise HTTPException(status_code=502, detail="Failed to exchange authorization code")

    id_token_str = tokens.get("id_token")
    if not id_token_str:
        raise HTTPException(status_code=401, detail="No ID token in response")

    # Validate ID token
    try:
        claims = await validate_id_token(config, id_token_str, expected_nonce)
    except Exception as e:
        logger.error("ID token validation failed: %s", e)
        raise HTTPException(status_code=401, detail="Invalid ID token")

    # Extract user info
    oidc_subject = claims.get("sub")
    oidc_email = claims.get("email")
    oidc_name = claims.get("name")

    if not oidc_subject:
        raise HTTPException(status_code=401, detail="ID token missing sub claim")

    # Create portal session
    now = datetime.now(timezone.utc)
    session = PortalSession(
        session_id=secrets.token_hex(16),
        tier=PortalTier.sso,
        ip_address=request.client.host if request.client else "unknown",
        created_at=now,
        expires_at=now + timedelta(minutes=config.session_ttl_minutes),
        portal_session_version=config.portal_session_version,
        oidc_subject=oidc_subject,
        oidc_email=oidc_email,
        oidc_name=oidc_name,
    )

    portal_token = create_portal_jwt(session)

    # Track session
    config.active_sessions[session.session_id] = session.expires_at.isoformat()
    save_portal_config(config)

    # Store refresh token if provided
    if tokens.get("refresh_token"):
        store_refresh_token(session.session_id, tokens["refresh_token"])

    # Store id_token for id_token_hint on logout (Gate 3)
    store_id_token(session.session_id, id_token_str)

    # Log access
    log_portal_access(session.session_id, oidc_subject, oidc_email, "login")

    # Redirect to portal with token as fragment (client picks it up)
    portal_url = f"{config.base_url.rstrip('/')}/portal/search"
    return RedirectResponse(
        url=f"{portal_url}#sso_token={portal_token}",
        status_code=302,
    )


@router.post("/auth/sso/logout")
async def sso_logout(session: PortalSession = Depends(get_portal_session)):
    """Logout SSO session: clear portal session, optionally redirect to IdP logout."""
    config = get_portal_config()

    # Remove from active sessions
    config.active_sessions.pop(session.session_id, None)
    save_portal_config(config)

    # Clear refresh token
    clear_refresh_token(session.session_id)

    # Log access
    if session.oidc_subject:
        log_portal_access(session.session_id, session.oidc_subject, session.oidc_email, "logout")

    # Try to get IdP end_session_endpoint with id_token_hint (Gate 3)
    end_session_url = None
    if config.oidc_issuer:
        try:
            id_token_hint = get_id_token(session.session_id)
            end_session_url = await get_end_session_url(config, id_token_hint=id_token_hint)
        except Exception:
            pass

    return {"message": "Logged out", "end_session_url": end_session_url}


@router.get("/auth/sso/userinfo")
async def sso_userinfo(session: PortalSession = Depends(get_portal_session)):
    """Return current SSO user info from the session JWT."""
    return {
        "email": session.oidc_email,
        "name": session.oidc_name,
        "subject": session.oidc_subject,
    }


@router.get("/datasets")
async def list_portal_datasets(session: PortalSession = Depends(get_portal_session)):
    """List datasets visible to portal users. ACL-enforced (M1)."""
    portal_svc = get_portal_service()
    datasets = await run_sync(portal_svc.get_visible_datasets)
    return {"datasets": [d.model_dump() for d in datasets]}


@router.post("/search")
async def search_portal(
    query: PortalSearchQuery,
    session: PortalSession = Depends(get_portal_session),
):
    """Search a portal-visible dataset. ACL-enforced (M1)."""
    # ACL check
    check_dataset_acl(query.dataset_id)

    # Log SSO access
    if session.tier == PortalTier.sso and session.oidc_subject:
        log_portal_access(
            session.session_id, session.oidc_subject, session.oidc_email,
            "search", f"dataset={query.dataset_id} q={query.query[:50]}",
        )

    portal_svc = get_portal_service()
    result = await run_sync(
        portal_svc.search_dataset,
        query.dataset_id,
        query.query,
        query.limit,
        query.offset,
    )
    return result.model_dump()


@router.get("/search/{dataset_id}")
async def search_dataset(
    dataset_id: str,
    q: str = Query(..., description="Search query"),
    limit: int = Query(20, ge=1, le=100),
    session: PortalSession = Depends(get_portal_session),
):
    """Search single dataset. ACL-enforced (M1)."""
    check_dataset_acl(dataset_id)

    portal_svc = get_portal_service()
    result = await run_sync(
        portal_svc.search_dataset,
        dataset_id,
        q,
        limit,
    )
    return result.model_dump()


# ---------------------------------------------------------------------------
# Admin portal management endpoints (require admin auth)
# ---------------------------------------------------------------------------

admin_router = APIRouter()


@admin_router.get("/settings/portal")
async def get_portal_settings():
    """Get current portal configuration (admin only)."""
    config = get_portal_config()
    # Return config but redact secrets
    data = config.model_dump()
    data["access_code_hash"] = "***" if config.access_code_hash else None
    data["oidc_client_secret"] = "***" if config.oidc_client_secret else None
    data["active_session_count"] = len(config.active_sessions)
    data["sso_enabled"] = is_sso_enabled()
    return data


@admin_router.put("/settings/portal")
async def update_portal_settings(body: PortalConfigUpdate):
    """Enable/disable portal, set tier, set access code, set base URL."""
    config = get_portal_config()

    if body.tier is not None:
        config.tier = body.tier

    if body.base_url is not None:
        config.base_url = body.base_url.rstrip("/")

    if body.session_ttl_minutes is not None:
        config.session_ttl_minutes = body.session_ttl_minutes

    # Handle OIDC config update (Phase 2)
    if body.oidc_issuer is not None:
        config.oidc_issuer = body.oidc_issuer
    if body.oidc_client_id is not None:
        config.oidc_client_id = body.oidc_client_id
    if body.oidc_client_secret is not None:
        if body.oidc_client_secret:
            config.oidc_client_secret = encrypt_client_secret(body.oidc_client_secret)
        else:
            config.oidc_client_secret = None

    # Validate SSO tier has OIDC configured
    if config.tier == PortalTier.sso:
        if not config.oidc_issuer or not config.oidc_client_id or not config.oidc_client_secret:
            raise HTTPException(
                status_code=400,
                detail="SSO tier requires oidc_issuer, oidc_client_id, and oidc_client_secret",
            )

    # Handle access code update (M5)
    if body.access_code is not None:
        if body.access_code == "":
            # Clear access code
            config.access_code_hash = None
        else:
            if not AccessCodeValidator.validate_strength(body.access_code):
                raise HTTPException(
                    status_code=400,
                    detail="Access code must be at least 6 alphanumeric characters and not purely numeric",
                )
            config.access_code_hash = AccessCodeValidator.hash_code(body.access_code)
            # Invalidate existing sessions on code rotation (SS-C1)
            config = AccessCodeValidator.invalidate_sessions_on_rotation(config)

    # Handle enable/disable
    if body.enabled is not None:
        if body.enabled:
            # M6: Reject enable without base_url
            if not config.base_url:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot enable portal without setting a base URL (Mandate M6)",
                )
            # Validate tier=code has access code set
            if config.tier == PortalTier.code and not config.access_code_hash:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot enable portal with 'code' tier without setting an access code",
                )
            # Validate tier=sso has OIDC configured
            if config.tier == PortalTier.sso:
                if not config.oidc_issuer or not config.oidc_client_id or not config.oidc_client_secret:
                    raise HTTPException(
                        status_code=400,
                        detail="Cannot enable portal with 'sso' tier without OIDC configuration",
                    )
        config.enabled = body.enabled

    save_portal_config(config)

    data = config.model_dump()
    data["access_code_hash"] = "***" if config.access_code_hash else None
    data["oidc_client_secret"] = "***" if config.oidc_client_secret else None
    data["active_session_count"] = len(config.active_sessions)
    return data


@admin_router.put("/settings/portal/datasets/{dataset_id}")
async def update_dataset_portal_config(dataset_id: str, body: DatasetPortalConfig):
    """Set per-dataset portal visibility and column restrictions."""
    # Verify dataset exists
    from app.services.processing_service import get_processing_service
    processing_svc = get_processing_service()
    record = processing_svc.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    config = get_portal_config()
    config.datasets[dataset_id] = body
    save_portal_config(config)

    return {"dataset_id": dataset_id, **body.model_dump()}


@admin_router.post("/settings/portal/revoke-sessions")
async def revoke_all_portal_sessions():
    """Revoke all active portal sessions by incrementing portal_session_version."""
    config = get_portal_config()
    config = AccessCodeValidator.invalidate_sessions_on_rotation(config)
    save_portal_config(config)
    return {"message": "All portal sessions revoked", "portal_session_version": config.portal_session_version}


# ---------------------------------------------------------------------------
# Phase 2: SSO Admin endpoints
# ---------------------------------------------------------------------------

@admin_router.post("/settings/portal/test-oidc")
async def test_oidc_connection():
    """Test OIDC connection by fetching the discovery document from the IdP."""
    config = get_portal_config()

    if not config.oidc_issuer:
        raise HTTPException(status_code=400, detail="OIDC issuer not configured")

    try:
        from app.services.portal_oidc import clear_discovery_cache
        clear_discovery_cache()  # Force fresh fetch
        doc = await fetch_oidc_discovery(config.oidc_issuer)
        return {
            "success": True,
            "issuer": doc.get("issuer"),
            "authorization_endpoint": doc.get("authorization_endpoint"),
            "token_endpoint": doc.get("token_endpoint"),
            "jwks_uri": doc.get("jwks_uri"),
            "end_session_endpoint": doc.get("end_session_endpoint"),
        }
    except Exception as e:
        logger.error("OIDC test connection failed: %s", e)
        return {
            "success": False,
            "error": "Failed to connect to identity provider",
        }


@admin_router.get("/admin/portal/access-logs")
async def get_portal_access_logs(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Return recent portal access logs for SSO users."""
    logs = get_access_logs(limit=limit, offset=offset)
    return {"logs": [log.model_dump() for log in logs], "total": len(logs)}


@admin_router.delete("/admin/portal/sessions/{session_id}")
async def revoke_portal_session(session_id: str):
    """Revoke a specific portal session."""
    config = get_portal_config()
    if session_id not in config.active_sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    del config.active_sessions[session_id]
    save_portal_config(config)
    # Clear refresh token if any
    clear_refresh_token(session_id)
    return {"message": f"Session {session_id} revoked"}


@admin_router.delete("/admin/portal/sessions")
async def revoke_all_portal_sessions_v2():
    """Revoke all portal sessions (increments portal_session_version)."""
    config = get_portal_config()
    config = AccessCodeValidator.invalidate_sessions_on_rotation(config)
    save_portal_config(config)
    # Clear session tokens but preserve audit logs (Gate 3)
    from app.services.portal_oidc import clear_sessions_only
    clear_sessions_only()
    return {"message": "All portal sessions revoked", "portal_session_version": config.portal_session_version}
