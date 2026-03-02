"""
BQ-123B: Diagnostic bundle API endpoint.

POST /api/diagnostics/bundle — generates a ZIP diagnostic bundle.
GET  /api/diagnostics/threads — faulthandler thread dump for debugging.
Requires authentication. Rate limited to 1 per minute.
"""
import asyncio
import faulthandler
import io
import logging
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse

from app.auth.api_key_auth import AuthenticatedUser, get_current_user
from app.services.diagnostic_service import DiagnosticService

logger = logging.getLogger(__name__)

router = APIRouter()

# ── Simple in-memory rate limiter (1 bundle/min) ────────────────────
_last_bundle_time: float = 0.0
_RATE_LIMIT_SECONDS = 60.0


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
