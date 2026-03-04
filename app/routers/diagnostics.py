"""
BQ-123B: Diagnostic bundle API endpoint.

POST /api/diagnostics/bundle — generates a ZIP diagnostic bundle.
POST /api/diagnostics/transmit — transmit bundle to ai.market support (Phase 4).
GET  /api/diagnostics/threads — faulthandler thread dump for debugging.
Requires authentication. Rate limited.
"""
import asyncio
import faulthandler
import io
import logging
import time
import uuid
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse

from app.auth.api_key_auth import AuthenticatedUser, get_current_user
from app.services.diagnostic_service import DiagnosticService

logger = logging.getLogger(__name__)

router = APIRouter()

# ── Simple in-memory rate limiter (1 bundle/min) ────────────────────
_last_bundle_time: float = 0.0
_RATE_LIMIT_SECONDS = 60.0

# ── Transmit rate limiter (1 transmission/hour) — Phase 4 ───────────
_last_transmit_time: float = 0.0
_TRANSMIT_RATE_LIMIT_SECONDS = 3600.0
_TRANSMIT_SIZE_CAP_BYTES = 50 * 1024 * 1024  # 50 MB
_TRANSMIT_ENDPOINT = "https://api.ai.market/api/v1/support/upload-diagnostic"
_ALLOWED_HOST = "api.ai.market"


@router.post("/diagnostics/bundle")
async def download_diagnostic_bundle(
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Generate and download a diagnostic ZIP bundle.

    Rate limited: 1 bundle per minute (bundle generation is expensive).
    Returns a streaming ZIP download.
    """
    global _last_bundle_time

    # Rate limit check
    now = time.monotonic()
    if now - _last_bundle_time < _RATE_LIMIT_SECONDS:
        remaining = int(_RATE_LIMIT_SECONDS - (now - _last_bundle_time))
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limited. Try again in {remaining}s.",
            headers={"Retry-After": str(remaining)},
        )

    _last_bundle_time = now

    logger.info(
        "diagnostic_bundle_requested",
        extra={"user_id": user.user_id},
    )

    try:
        service = DiagnosticService()
        buf = await service.generate_bundle()
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Bundle generation timed out (30s limit).",
        )
    except Exception as e:
        logger.error("diagnostic_bundle_failed", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate diagnostic bundle.",
        )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"vectoraiz-diagnostic-{timestamp}.zip"

    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )


# ── Phase 4: Diagnostic Transmission ────────────────────────────────


@router.post("/diagnostics/transmit")
async def transmit_diagnostic_bundle(
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Transmit a diagnostic bundle to ai.market support.

    Security mandates:
    - M1: PII/secret scrubbing (via DiagnosticService collectors)
    - M2: User-initiated only (requires auth + explicit POST)
    - M3: Fixed allowlisted endpoint (hardcoded, no user input)
    - M4: Rate limited: 1 transmission per hour
    - M5: Size cap: 50 MB
    """
    global _last_transmit_time

    # Rate limit check (M4)
    now = time.monotonic()
    if now - _last_transmit_time < _TRANSMIT_RATE_LIMIT_SECONDS:
        remaining = int(_TRANSMIT_RATE_LIMIT_SECONDS - (now - _last_transmit_time))
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Diagnostic transmission rate limited. Try again in {remaining}s.",
            headers={"Retry-After": str(remaining)},
        )

    logger.info(
        "diagnostic_transmit_requested",
        extra={"user_id": user.user_id},
    )

    # Generate bundle (includes PII scrubbing via collectors — M1)
    try:
        service = DiagnosticService()
        buf = await service.generate_bundle()
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Bundle generation timed out (30s limit).",
        )
    except Exception as e:
        logger.error("diagnostic_transmit_generate_failed", extra={"error": str(e)})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to generate diagnostic bundle.",
        )

    # Size cap check (M5)
    bundle_bytes = buf.getvalue()
    if len(bundle_bytes) > _TRANSMIT_SIZE_CAP_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Bundle size ({len(bundle_bytes)} bytes) exceeds 50 MB cap.",
        )

    # Transmit to fixed allowlisted endpoint (M3)
    transmission_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                _TRANSMIT_ENDPOINT,
                content=bundle_bytes,
                headers={
                    "Content-Type": "application/zip",
                    "X-Transmission-ID": transmission_id,
                    "X-VZ-Source": "vectoraiz-diagnostic",
                },
            )
            response.raise_for_status()
    except httpx.HTTPStatusError as e:
        logger.error(
            "diagnostic_transmit_http_error",
            extra={"status": e.response.status_code, "transmission_id": transmission_id},
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Support endpoint returned {e.response.status_code}.",
        )
    except Exception as e:
        logger.error(
            "diagnostic_transmit_failed",
            extra={"error": str(e), "transmission_id": transmission_id},
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Failed to transmit diagnostic bundle to support.",
        )

    # Update rate limit timestamp only on success
    _last_transmit_time = now

    # Log transmission locally
    logger.info(
        "diagnostic_transmit_success",
        extra={
            "user_id": user.user_id,
            "transmission_id": transmission_id,
            "bundle_size_bytes": len(bundle_bytes),
        },
    )

    # Create success notification
    try:
        from app.services.notification_service import get_notification_service
        import json

        svc = get_notification_service()
        svc.create(
            type="success",
            category="diagnostic",
            title="Diagnostic bundle sent",
            message=f"Diagnostic bundle ({round(len(bundle_bytes) / 1024, 1)} KB) transmitted to ai.market support.",
            metadata_json=json.dumps({
                "transmission_id": transmission_id,
                "size_bytes": len(bundle_bytes),
                "timestamp": timestamp,
            }),
            source="system",
        )
    except Exception:
        pass  # Non-critical

    return {
        "success": True,
        "transmission_id": transmission_id,
        "timestamp": timestamp,
        "size_bytes": len(bundle_bytes),
    }


@router.get("/diagnostics/threads")
async def dump_threads(
    _user: AuthenticatedUser = Depends(get_current_user),
):
    """Dump tracebacks for all threads via faulthandler.

    Useful for diagnosing deadlocks and blocked event loops.
    Requires authentication.
    """
    buffer = io.StringIO()
    faulthandler.dump_traceback(file=buffer, all_threads=True)
    return {"thread_dump": buffer.getvalue()}
