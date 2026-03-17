"""
Serial Client — HTTP client for ai-market serial endpoints.
=============================================================

Wraps POST /serials/{serial}/activate, /meter, /status, /refresh
with retry + backoff.

BQ-VZ-SERIAL-CLIENT
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

RETRY_DELAYS = [1.0, 3.0]  # Exponential backoff: 1s, 3s
DEFAULT_TIMEOUT = 10.0


@dataclass(frozen=True)
class ActivateResult:
    success: bool
    install_token: Optional[str] = None
    error: Optional[str] = None
    status_code: int = 0


@dataclass(frozen=True)
class MeterResult:
    allowed: bool
    category: str = ""
    cost_usd: str = "0.00"
    remaining_usd: str = "0.00"
    reason: Optional[str] = None
    payment_enabled: bool = False
    migrated: bool = False
    error: Optional[str] = None
    status_code: int = 0


@dataclass(frozen=True)
class StatusResult:
    success: bool
    data: Optional[dict] = None
    migrated: bool = False
    error: Optional[str] = None
    status_code: int = 0


@dataclass(frozen=True)
class RefreshResult:
    success: bool
    install_token: Optional[str] = None
    error: Optional[str] = None
    status_code: int = 0


class SerialClient:
    """Async HTTP client for ai-market serial authority endpoints."""

    def __init__(self, base_url: Optional[str] = None, timeout: float = DEFAULT_TIMEOUT):
        self._base_url = (base_url or settings.aimarket_url).rstrip("/")
        self._timeout = timeout

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict] = None,
        headers: Optional[dict] = None,
        retries: int = 2,
    ) -> tuple[int, Optional[dict]]:
        """Make an HTTP request with retries and backoff."""
        import asyncio

        url = f"{self._base_url}{path}"
        last_exc: Optional[Exception] = None
        last_status = 0

        for attempt in range(1 + retries):
            try:
                async with httpx.AsyncClient(timeout=self._timeout) as client:
                    resp = await client.request(method, url, json=json, headers=headers or {})
                last_status = resp.status_code
                try:
                    data = resp.json()
                except (ValueError, Exception):
                    data = None
                return last_status, data
            except (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPError) as e:
                last_exc = e
                if attempt < retries:
                    delay = RETRY_DELAYS[attempt] if attempt < len(RETRY_DELAYS) else RETRY_DELAYS[-1]
                    logger.warning(
                        "Serial API retry %d/%d for %s %s: %s (wait %.1fs)",
                        attempt + 1, retries, method, path, e, delay,
                    )
                    await asyncio.sleep(delay)

        logger.error("Serial API failed after %d attempts: %s %s: %s", 1 + retries, method, path, last_exc)
        return last_status, None

    async def activate(
        self,
        serial: str,
        bootstrap_token: str,
        instance_id: str,
        hostname: str,
        version: str,
    ) -> ActivateResult:
        """POST /serials/{serial}/activate"""
        status_code, data = await self._request(
            "POST",
            f"/api/v1/serials/{serial}/activate",
            json={
                "instance_id": instance_id,
                "hostname": hostname,
                "version": version,
            },
            headers={"Authorization": f"Bearer {bootstrap_token}"},
        )
        if status_code == 200 and data:
            return ActivateResult(
                success=True,
                install_token=data.get("install_token"),
                status_code=status_code,
            )
        error = data.get("detail", str(data)) if data else f"HTTP {status_code}"
        return ActivateResult(success=False, error=error, status_code=status_code)

    async def meter(
        self,
        serial: str,
        install_token: str,
        category: str,
        cost_usd: Decimal,
        request_id: str,
        description: str = "",
    ) -> MeterResult:
        """POST /api/v1/serials/{serial}/meter"""
        status_code, data = await self._request(
            "POST",
            f"/api/v1/serials/{serial}/meter",
            json={
                "category": category,
                "cost_usd": str(cost_usd),
                "request_id": request_id,
                "description": description,
            },
            headers={"Authorization": f"Bearer {install_token}"},
        )
        if data and status_code in (200, 402):
            return MeterResult(
                allowed=data.get("allowed", False),
                category=data.get("category", category),
                cost_usd=data.get("cost_usd", "0.00"),
                remaining_usd=data.get("remaining_usd", "0.00"),
                reason=data.get("reason"),
                payment_enabled=data.get("payment_enabled", False),
                migrated=data.get("migrated", False),
                status_code=status_code,
            )
        error = data.get("detail", str(data)) if data else f"HTTP {status_code}"
        return MeterResult(
            allowed=False, error=error, status_code=status_code,
        )

    async def status(self, serial: str, install_token: str) -> StatusResult:
        """GET /api/v1/serials/{serial}/status"""
        status_code, data = await self._request(
            "GET",
            f"/api/v1/serials/{serial}/status",
            headers={"Authorization": f"Bearer {install_token}"},
        )
        if status_code == 200 and data:
            return StatusResult(
                success=True,
                data=data,
                migrated=data.get("migrated", False),
                status_code=status_code,
            )
        error = data.get("detail", str(data)) if data else f"HTTP {status_code}"
        return StatusResult(success=False, error=error, status_code=status_code)

    async def refresh(
        self,
        serial: str,
        install_token: str,
        instance_id: str,
    ) -> RefreshResult:
        """POST /api/v1/serials/{serial}/refresh"""
        status_code, data = await self._request(
            "POST",
            f"/api/v1/serials/{serial}/refresh",
            json={
                "instance_id": instance_id,
            },
            headers={"Authorization": f"Bearer {install_token}"},
        )
        if status_code == 200 and data:
            return RefreshResult(
                success=True,
                install_token=data.get("install_token"),
                status_code=status_code,
            )
        error = data.get("detail", str(data)) if data else f"HTTP {status_code}"
        return RefreshResult(success=False, error=error, status_code=status_code)

    async def credits_checkout(self, serial: str, install_token: str, amount_usd: float = 25.0) -> dict:
        """POST /api/v1/serials/{serial}/credits/checkout"""
        import os
        # Return URL must point to the FRONTEND (8080), not the backend (8100)
        frontend_port = os.environ.get("VECTORAIZ_FRONTEND_PORT", "8080")
        return_url = os.environ.get("VECTORAIZ_RETURN_URL", f"http://localhost:{frontend_port}")
        status_code, data = await self._request(
            "POST",
            f"/api/v1/serials/{serial}/credits/checkout",
            json={"amount_usd": amount_usd},
            headers={
                "Authorization": f"Bearer {install_token}",
                "X-VZ-Return-URL": return_url,
            },
        )
        if status_code == 200 and data:
            return {"success": True, **data}
        error = data.get("detail", str(data)) if data else f"HTTP {status_code}"
        return {"success": False, "error": error, "status_code": status_code}

    async def credits_usage(self, serial: str, install_token: str) -> dict:
        """GET /api/v1/serials/{serial}/credits/usage"""
        status_code, data = await self._request(
            "GET",
            f"/api/v1/serials/{serial}/credits/usage",
            headers={"Authorization": f"Bearer {install_token}"},
        )
        if status_code == 200 and data:
            return {"success": True, **data}
        error = data.get("detail", str(data)) if data else f"HTTP {status_code}"
        return {"success": False, "error": error, "status_code": status_code}
