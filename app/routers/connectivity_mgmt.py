"""
Connectivity Management REST API — Settings UI endpoints.

Mounted at /api/connectivity (internal, session-auth protected).
Wraps existing service layer for the frontend Settings > Connectivity page.

Phase: BQ-MCP-RAG Phase 3 — Connection Hub UI
Created: S136
"""

import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.auth.api_key_auth import AuthenticatedUser, get_current_user
from app.config import settings
from app.services.serial_metering import metered, MeterDecision
from app.services.connectivity_metrics import get_connectivity_metrics
from app.services.connectivity_setup_generator import ConnectivitySetupGenerator, SUPPORTED_PLATFORMS
from app.services.connectivity_token_service import (
    ConnectivityTokenError,
    create_token,
    list_tokens,
    revoke_token,
    verify_token,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Connectivity Management"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class TokenCreateRequest(BaseModel):
    label: str = Field(..., min_length=1, max_length=255, description="Human label for the token")
    scopes: Optional[List[str]] = Field(None, description="Scopes to grant. Defaults to all.")


class TokenCreateResponse(BaseModel):
    token: str = Field(..., description="Full token — shown ONCE")
    token_id: str
    label: str
    scopes: List[str]
    secret_last4: str
    warning: str = "Save this token now — it cannot be retrieved later."


class TokenInfo(BaseModel):
    id: str
    label: str
    scopes: List[str]
    secret_last4: str
    created_at: Optional[str] = None
    expires_at: Optional[str] = None
    last_used_at: Optional[str] = None
    request_count: int = 0
    is_revoked: bool = False


class TokenListResponse(BaseModel):
    tokens: List[TokenInfo]
    count: int


class ConnectivityStatusResponse(BaseModel):
    enabled: bool
    bind_host: str
    tokens: List[TokenInfo]
    token_count: int
    active_token_count: int
    metrics: Dict[str, Any]


class SetupRequest(BaseModel):
    platform: str = Field(..., description="Target platform identifier")
    token: str = Field("", description="Token to embed in config (optional)")
    base_url: str = Field("http://localhost:8100", description="Base URL of vectorAIz instance")


class TestRequest(BaseModel):
    token: str = Field(..., description="Full token to test")


class TestResponse(BaseModel):
    connectivity_enabled: bool
    token_valid: bool
    token_label: Optional[str] = None
    token_scopes: List[str] = []
    datasets_accessible: int = 0
    sample_query_ok: bool = False
    latency_ms: Optional[int] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _token_to_info(t) -> TokenInfo:
    return TokenInfo(
        id=t.id,
        label=t.label,
        scopes=t.scopes,
        secret_last4=t.secret_last4,
        created_at=t.created_at.isoformat() if t.created_at else None,
        expires_at=t.expires_at.isoformat() if t.expires_at else None,
        last_used_at=t.last_used_at.isoformat() if t.last_used_at else None,
        request_count=t.request_count,
        is_revoked=getattr(t, "is_revoked", False),
    )


def _safe_error_category(e: Exception) -> str:
    """Return a safe error category string (no class names, no secrets)."""
    if isinstance(e, TimeoutError):
        return "timeout"
    if isinstance(e, PermissionError):
        return "permission_denied"
    if isinstance(e, ConnectionError):
        return "connection_error"
    return "internal_error"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/status",
    response_model=ConnectivityStatusResponse,
    summary="Full connectivity status for Settings UI",
)
async def connectivity_status(user: AuthenticatedUser = Depends(get_current_user)):
    tokens_raw = list_tokens()
    tokens = [_token_to_info(t) for t in tokens_raw]

    # Active = not revoked and not expired
    active_count = 0
    for t in tokens_raw:
        if not getattr(t, "is_revoked", False):
            if t.expires_at is None or t.expires_at.timestamp() > time.time():
                active_count += 1

    metrics = {}
    try:
        metrics = get_connectivity_metrics().get_snapshot()
    except Exception as e:
        logger.warning("Failed to fetch connectivity metrics: %s", _safe_error_category(e))

    return ConnectivityStatusResponse(
        enabled=settings.connectivity_enabled,
        bind_host=settings.connectivity_bind_host,
        tokens=tokens,
        token_count=len(tokens),
        active_token_count=active_count,
        metrics=metrics,
    )


@router.post(
    "/enable",
    summary="Enable external connectivity",
)
async def connectivity_enable(user: AuthenticatedUser = Depends(get_current_user), _meter: MeterDecision = Depends(metered("setup"))):
    was_enabled = settings.connectivity_enabled
    settings.connectivity_enabled = True
    return {
        "enabled": True,
        "changed": not was_enabled,
        "note": (
            "Connectivity enabled in memory. Set VECTORAIZ_CONNECTIVITY_ENABLED=true "
            "to persist across restarts."
        ) if not was_enabled else None,
    }


@router.post(
    "/disable",
    summary="Disable external connectivity",
)
async def connectivity_disable(user: AuthenticatedUser = Depends(get_current_user)):
    was_enabled = settings.connectivity_enabled
    settings.connectivity_enabled = False
    return {
        "enabled": False,
        "changed": was_enabled,
        "note": (
            "Connectivity disabled. Existing tokens preserved."
        ) if was_enabled else None,
    }


@router.get(
    "/tokens",
    response_model=TokenListResponse,
    summary="List all connectivity tokens",
)
async def connectivity_list_tokens(user: AuthenticatedUser = Depends(get_current_user)):
    tokens_raw = list_tokens()
    tokens = [_token_to_info(t) for t in tokens_raw]
    return TokenListResponse(tokens=tokens, count=len(tokens))


@router.post(
    "/tokens",
    response_model=TokenCreateResponse,
    summary="Create a new connectivity token",
    status_code=201,
)
async def connectivity_create_token(
    body: TokenCreateRequest,
    user: AuthenticatedUser = Depends(get_current_user),
):
    try:
        raw_token, token_info = create_token(label=body.label, scopes=body.scopes)
    except ConnectivityTokenError as e:
        raise HTTPException(status_code=400, detail=e.message)

    return TokenCreateResponse(
        token=raw_token,
        token_id=token_info.id,
        label=token_info.label,
        scopes=token_info.scopes,
        secret_last4=token_info.secret_last4,
    )


@router.delete(
    "/tokens/{token_id}",
    summary="Revoke a connectivity token",
)
async def connectivity_revoke_token(
    token_id: str,
    user: AuthenticatedUser = Depends(get_current_user),
):
    try:
        token_info = revoke_token(token_id)
    except ConnectivityTokenError as e:
        status = 404 if e.code == "auth_invalid" else 400
        raise HTTPException(status_code=status, detail=e.message)

    return {
        "revoked": True,
        "token_id": token_info.id,
        "label": token_info.label,
    }


@router.post(
    "/test",
    response_model=TestResponse,
    summary="Test a connectivity token",
)
async def connectivity_test_token(
    body: TestRequest,
    user: AuthenticatedUser = Depends(get_current_user),
    _meter: MeterDecision = Depends(metered("setup")),
):
    # NOTE: This endpoint is admin-only, protected by session auth (get_current_user).
    # On self-hosted instances, only authenticated admin users can invoke token testing.
    logger.debug("Token test requested", extra={"has_token": bool(body.token)})

    results = TestResponse(
        connectivity_enabled=settings.connectivity_enabled,
        token_valid=False,
    )

    if not settings.connectivity_enabled:
        results.error = "External connectivity is disabled"
        return results

    # Validate token
    try:
        token = verify_token(body.token)
        results.token_valid = True
        results.token_label = token.label
        results.token_scopes = token.scopes
    except ConnectivityTokenError:
        results.error = "Token validation failed"
        return results

    # Count accessible datasets
    try:
        from app.services.query_orchestrator import get_query_orchestrator
        orchestrator = get_query_orchestrator()
        start = time.time()
        ds_response = await orchestrator.list_datasets(token)
        results.datasets_accessible = ds_response.count
    except Exception as e:
        results.error = _safe_error_category(e)
        return results

    # Sample query
    if results.datasets_accessible > 0:
        try:
            from app.models.connectivity import VectorSearchRequest
            start = time.time()
            search_req = VectorSearchRequest(query="test", top_k=1)
            await orchestrator.search_vectors(token, search_req)
            results.sample_query_ok = True
            results.latency_ms = int((time.time() - start) * 1000)
        except Exception as e:
            logger.debug("Sample query failed during token test: %s", _safe_error_category(e))
            results.error = f"Token valid but sample query failed: {_safe_error_category(e)}"

    return results


@router.post(
    "/setup",
    summary="Generate platform-specific setup config",
)
async def connectivity_generate_setup(
    body: SetupRequest,
    user: AuthenticatedUser = Depends(get_current_user),
    _meter: MeterDecision = Depends(metered("setup")),
):
    if body.platform not in SUPPORTED_PLATFORMS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported platform: '{body.platform}'. "
                   f"Supported: {', '.join(sorted(SUPPORTED_PLATFORMS))}",
        )

    # For generic_llm, fetch dataset info
    datasets: List[Dict[str, Any]] = []
    if body.platform == "generic_llm":
        try:
            from app.services.processing_service import get_processing_service
            svc = get_processing_service()
            records = svc.list_datasets()
            for r in records:
                status_val = r.status.value if hasattr(r.status, "value") else str(r.status)
                if status_val == "ready":
                    datasets.append({
                        "id": r.id,
                        "name": r.original_filename,
                        "rows": r.metadata.get("row_count"),
                        "columns": r.metadata.get("column_count"),
                        "description": r.metadata.get("description", ""),
                    })
        except Exception as e:
            logger.debug("Failed to fetch datasets for setup config: %s", _safe_error_category(e))

    generator = ConnectivitySetupGenerator()
    result = generator.generate(
        platform=body.platform,
        token=body.token,
        base_url=body.base_url,
        datasets=datasets,
    )
    return result
