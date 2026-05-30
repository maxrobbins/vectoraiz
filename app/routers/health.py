"""
BQ-123A: Health check endpoints with deep component checks.

- GET /api/health          — cheap: process alive, version, uptime
- GET /api/health/deep     — bounded checks for 6 components (2s timeout each)
- GET /api/health/issues   — active non-critical issues from the ring buffer
"""
import asyncio
import logging
import os
import time
from datetime import datetime, timezone

import psutil
from fastapi import APIRouter, Depends
from qdrant_client import QdrantClient

from app.config import settings
from app.core.structured_logging import APP_VERSION, get_uptime_s
from app.auth.api_key_auth import get_current_user, AuthenticatedUser
from app.services.duckdb_service import ephemeral_duckdb_service

logger = logging.getLogger(__name__)

router = APIRouter()

COMPONENT_TIMEOUT = 2.0  # seconds


# ── Cheap health ─────────────────────────────────────────────────────
@router.get("/health")
async def health_check():
    """Cheap health check — no network calls."""
    import time
    start = time.perf_counter()
    data = {
        "status": "ok",
        "version": APP_VERSION,
        "service": "vectoraiz-backend",
        "uptime_s": round(get_uptime_s(), 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    duration = time.perf_counter() - start
    if duration > 0.5:
        logger.warning(f"EVENT LOOP LAG: /health took {duration:.3f}s. Possible blocking call.")
    return data


# ── Deep health (auth required — exposes infrastructure details) ─────
@router.get("/health/deep")
async def deep_health_check(
    _user: AuthenticatedUser = Depends(get_current_user),
):
    """Deep health check with bounded component checks."""
    components = {}

    checks = [
        ("qdrant", _check_qdrant()),
        ("duckdb", _check_duckdb()),
        ("llm", _check_llm()),
        ("trust_channel", _check_trust_channel()),
        ("disk", _check_disk()),
        ("memory", _check_memory()),
    ]

    results = await asyncio.gather(
        *[_bounded_check(name, coro) for name, coro in checks],
        return_exceptions=True,
    )

    for name_result in results:
        if isinstance(name_result, Exception):
            continue
        name, result = name_result
        components[name] = result

    # Overall status = worst component
    statuses = [c.get("status", "down") for c in components.values()]
    if "down" in statuses:
        overall = "down"
    elif "degraded" in statuses:
        overall = "degraded"
    else:
        overall = "ok"

    return {
        "status": overall,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "version": APP_VERSION,
        "uptime_s": round(get_uptime_s(), 1),
        "components": components,
    }


async def _bounded_check(name: str, coro) -> tuple[str, dict]:
    """Run a component check with a 2-second timeout."""
    try:
        result = await asyncio.wait_for(coro, timeout=COMPONENT_TIMEOUT)
        return name, result
    except asyncio.TimeoutError:
        return name, {"status": "down", "detail_safe": "Health check timed out"}
    except Exception as e:
        logger.warning("health_check_error", extra={"component": name, "error": str(e)})
        return name, {"status": "down", "detail_safe": f"Check failed: {type(e).__name__}"}


async def _check_qdrant() -> dict:
    """Check Qdrant connectivity and latency."""
    start = time.perf_counter()
    try:
        client = QdrantClient(host=settings.qdrant_host, port=settings.qdrant_port, timeout=2.0)
        collections = client.get_collections()
        latency_ms = round((time.perf_counter() - start) * 1000, 1)

        col_count = len(collections.collections)
        total_vectors = sum(
            getattr(c, "points_count", 0) or 0 for c in collections.collections
        )

        if latency_ms > 250:
            status = "degraded"
        else:
            status = "ok"

        return {
            "status": status,
            "latency_ms": latency_ms,
            "detail_safe": f"{col_count} collections, {total_vectors} vectors",
        }
    except Exception as e:
        latency_ms = round((time.perf_counter() - start) * 1000, 1)
        return {
            "status": "down",
            "latency_ms": latency_ms,
            "detail_safe": f"Connection failed: {type(e).__name__}",
        }


async def _check_duckdb() -> dict:
    """Check DuckDB with SELECT 1."""
    start = time.perf_counter()
    try:
        with ephemeral_duckdb_service() as duckdb:
            result = duckdb.connection.execute("SELECT 1").fetchone()
        latency_ms = round((time.perf_counter() - start) * 1000, 1)

        if result and result[0] == 1:
            status = "degraded" if latency_ms > 250 else "ok"
        else:
            status = "down"

        return {"status": status, "latency_ms": latency_ms}
    except Exception as e:
        latency_ms = round((time.perf_counter() - start) * 1000, 1)
        return {
            "status": "down",
            "latency_ms": latency_ms,
            "detail_safe": f"Query failed: {type(e).__name__}",
        }


async def _check_llm() -> dict:
    """Check Allie LLM provider configuration."""
    try:
        from app.services.allie_provider import get_allie_provider

        provider = get_allie_provider()
        return {"status": "ok", "detail_safe": f"Provider: {type(provider).__name__}"}
    except Exception as e:
        return {"status": "down", "detail_safe": f"LLM check failed: {type(e).__name__}"}


async def _check_trust_channel() -> dict:
    """Check device crypto keys."""
    try:
        from app.core.crypto import DeviceCrypto

        if not settings.keystore_passphrase:
            return {"status": "down", "detail_safe": "Keystore passphrase not set"}

        crypto = DeviceCrypto(
            keystore_path=settings.keystore_path,
            passphrase=settings.keystore_passphrase,
        )
        keypairs = crypto.get_or_create_keypairs()
        if keypairs:
            return {"status": "ok"}
        return {"status": "down", "detail_safe": "No keypairs available"}
    except Exception as e:
        return {"status": "down", "detail_safe": f"Key check failed: {type(e).__name__}"}


async def _check_disk() -> dict:
    """Check free disk space."""
    try:
        usage = psutil.disk_usage("/")
        free_pct = round(100.0 - usage.percent, 1)

        if free_pct < 5:
            status = "down"
        elif free_pct < 15:
            status = "degraded"
        else:
            status = "ok"

        return {"status": status, "free_pct": free_pct}
    except Exception as e:
        return {"status": "down", "detail_safe": f"Disk check failed: {type(e).__name__}"}


async def _check_memory() -> dict:
    """Check available memory."""
    try:
        mem = psutil.virtual_memory()
        avail_pct = round(100.0 - mem.percent, 1)

        if avail_pct < 3:
            status = "down"
        elif avail_pct < 10:
            status = "degraded"
        else:
            status = "ok"

        return {"status": status, "avail_pct": avail_pct}
    except Exception as e:
        return {"status": "down", "detail_safe": f"Memory check failed: {type(e).__name__}"}


# ── System info (public, no auth) ────────────────────────────────────
@router.get("/system/info")
async def system_info():
    """Public endpoint (no auth) returning system mode, feature flags, and system capabilities."""
    from app.core.channel_config import CHANNEL

    mem = psutil.virtual_memory()
    cores = os.cpu_count() or 4
    mem_gb = round(mem.total / (1024**3), 1)
    # Recommended concurrent uploads: floor(cores/4), clamped 2-6
    # 6 is the browser's per-origin connection limit
    recommended_concurrent = min(max(cores // 4, 2), 6)

    return {
        "mode": settings.mode,
        "version": APP_VERSION,
        "channel": CHANNEL.value,
        "features": {
            "allai": settings.allai_enabled and settings.mode != "standalone",
            "marketplace": settings.marketplace_enabled,
            "earnings": settings.marketplace_enabled,
            "local_auth": True,
        },
        "system": {
            "cpu_cores": cores,
            "memory_gb": mem_gb,
            "recommended_concurrent_uploads": recommended_concurrent,
        },
        "marketplace_api_url": settings.ai_market_url if settings.mode != "standalone" else None,
    }


@router.get("/system/mode")
async def system_mode():
    """Public endpoint (no auth) returning system mode and feature flags.

    Alias for /system/info with the shape requested by the frontend.
    """
    return {
        "mode": settings.mode,
        "features": {
            "marketplace": settings.marketplace_enabled,
            "allai": settings.allai_enabled and settings.mode != "standalone",
            "earnings": settings.marketplace_enabled,
        },
    }


# ── Issues endpoint (auth required — exposes infrastructure details) ─
@router.get("/health/issues")
async def get_issues(_user: AuthenticatedUser = Depends(get_current_user)):
    """Return active non-critical issues from the issue tracker."""
    from app.core.issue_tracker import issue_tracker

    return {
        "issues": issue_tracker.get_active_issues(),
        "count": len(issue_tracker.get_active_issues()),
    }
