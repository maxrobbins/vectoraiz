"""
Serial Metering — Strategy pattern for serial vs ledger metering.
==================================================================

Provides:
- MeteringStrategy protocol
- SerialMeteringStrategy (pre-registration: meters via ai-market serial)
- LedgerMeteringStrategy (post-migration: uses existing metering_service)
- FastAPI dependency `metered(category)` for endpoint protection
- CreditExhaustedException for the $4 wall

BQ-VZ-SERIAL-CLIENT
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Protocol

from fastapi import Depends, Request

from app.config import settings
from app.services.serial_store import (
    ACTIVE,
    DEGRADED,
    MIGRATED,
    PROVISIONED,
    UNPROVISIONED,
    SerialStore,
    get_serial_store,
)
from app.services.serial_client import SerialClient, MeterResult
from app.services.offline_queue import OfflineQueue, get_offline_queue
from app.services.auto_reload_service import check_auto_reload

logger = logging.getLogger(__name__)

# Default costs per metered operation (USD)
DEFAULT_SETUP_COST = Decimal("0.01")
DEFAULT_DATA_COST = Decimal("0.03")

# Views classified as "setup" for copilot dual-category metering
SETUP_VIEWS = frozenset({"onboarding", "setup", "connectivity", "metadata_builder", "publish"})


def classify_copilot_category(active_view: str | None) -> str:
    """Classify copilot message category based on the active UI view."""
    if active_view and active_view in SETUP_VIEWS:
        return "setup"
    return "data"


class CreditExhaustedException(Exception):
    """Raised when metering is denied (data credits exhausted, $4 wall)."""

    def __init__(
        self,
        category: str,
        reason: str,
        remaining_usd: str = "0.00",
        setup_remaining_usd: str = "0.00",
        payment_enabled: bool = False,
        serial: str = "",
    ):
        self.category = category
        self.reason = reason
        self.remaining_usd = remaining_usd
        self.setup_remaining_usd = setup_remaining_usd
        self.payment_enabled = payment_enabled
        self.serial = serial
        super().__init__(f"Credit exhausted: {reason}")


class ActivationRequiredException(Exception):
    """Raised when metering fails because the serial isn't activated."""

    def __init__(self, message: str = "Activation required"):
        super().__init__(message)


class UnprovisionedException(Exception):
    """Raised when no serial is provisioned."""

    def __init__(self, message: str = "allAI is not yet activated. Please reinstall in connected mode or contact support."):
        super().__init__(message)


@dataclass(frozen=True)
class MeterDecision:
    allowed: bool
    category: str
    offline: bool = False
    reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Strategy protocol
# ---------------------------------------------------------------------------

class MeteringStrategy(Protocol):
    async def check_and_meter(
        self, category: str, estimated_cost: Decimal, request_id: str,
    ) -> MeterDecision: ...


# ---------------------------------------------------------------------------
# SerialMeteringStrategy — pre-registration
# ---------------------------------------------------------------------------

class SerialMeteringStrategy:
    """Meters against serial credit pools via ai-market."""

    def __init__(
        self,
        store: SerialStore,
        client: Optional[SerialClient] = None,
        offline_queue: Optional[OfflineQueue] = None,
    ):
        self._store = store
        self._client = client or SerialClient()
        self._queue = offline_queue or get_offline_queue()

    async def check_and_meter(
        self, category: str, estimated_cost: Decimal, request_id: str,
    ) -> MeterDecision:
        state = self._store.state

        # UNPROVISIONED: allow setup offline, block data
        if state.state == UNPROVISIONED:
            if category == "setup":
                self._queue.append({
                    "category": category,
                    "cost_usd": str(estimated_cost),
                    "request_id": request_id,
                    "description": "unprovisioned-offline",
                    "timestamp": time.time(),
                })
                return MeterDecision(allowed=True, category=category, offline=True)
            raise UnprovisionedException()

        # PROVISIONED: allow setup offline, block data
        if state.state == PROVISIONED:
            if category == "setup":
                self._queue.append({
                    "category": category,
                    "cost_usd": str(estimated_cost),
                    "request_id": request_id,
                    "description": "provisioned-offline",
                    "timestamp": time.time(),
                })
                return MeterDecision(allowed=True, category=category, offline=True)
            raise ActivationRequiredException()

        # DEGRADED: allow setup offline, block data
        if state.state == DEGRADED:
            if category == "setup":
                self._queue.append({
                    "category": category,
                    "cost_usd": str(estimated_cost),
                    "request_id": request_id,
                    "description": "degraded-offline",
                    "timestamp": time.time(),
                })
                return MeterDecision(allowed=True, category=category, offline=True)
            raise CreditExhaustedException(
                category=category,
                reason="offline_data_blocked",
                serial=state.serial,
            )

        # ACTIVE: meter normally
        if not state.install_token:
            raise ActivationRequiredException()

        result = await self._client.meter(
            serial=state.serial,
            install_token=state.install_token,
            category=category,
            cost_usd=estimated_cost,
            request_id=request_id,
        )

        # Check migration
        if result.migrated:
            gw_user_id = None
            self._store.transition_to_migrated(gw_user_id)
            return MeterDecision(allowed=True, category=category)

        # Success
        if result.allowed:
            self._store.record_success()
            # Fire-and-forget: check if auto-reload should trigger
            asyncio.create_task(check_auto_reload())
            return MeterDecision(allowed=True, category=category)

        # Denied by server
        if result.status_code in (200, 402) and not result.allowed:
            self._store.record_success()
            cached = self._store.state.last_status_cache or {}
            raise CreditExhaustedException(
                category=category,
                reason=result.reason or f"insufficient_{category}_credits",
                remaining_usd=result.remaining_usd,
                setup_remaining_usd=cached.get("setup_remaining_usd", "0.00"),
                payment_enabled=result.payment_enabled,
                serial=state.serial,
            )

        # 401 → token revoked
        if result.status_code == 401:
            self._store.transition_to_unprovisioned()
            raise ActivationRequiredException("Token revoked — re-activation required")

        # Network failure
        self._store.record_failure()

        # Offline policy
        if category == "setup":
            self._queue.append({
                "category": category,
                "cost_usd": str(estimated_cost),
                "request_id": request_id,
                "description": "network-failure-offline",
                "timestamp": time.time(),
            })
            return MeterDecision(allowed=True, category=category, offline=True)

        # Data: allow if < 3 consecutive failures, block otherwise
        if self._store.state.consecutive_failures < 3:
            return MeterDecision(allowed=True, category=category, offline=True, reason="transient_offline")
        raise CreditExhaustedException(
            category=category,
            reason="offline_data_blocked",
            serial=state.serial,
        )


# ---------------------------------------------------------------------------
# LedgerMeteringStrategy — post-migration (BQ-073)
# ---------------------------------------------------------------------------

class LedgerMeteringStrategy:
    """Delegates to existing metering_service after migration."""

    async def check_and_meter(
        self, category: str, estimated_cost: Decimal, request_id: str,
    ) -> MeterDecision:
        # Post-migration: always allow — BQ-073 handles billing at proxy level
        return MeterDecision(allowed=True, category=category)


# ---------------------------------------------------------------------------
# Request ID generation (idempotent)
# ---------------------------------------------------------------------------

def _make_request_id(serial: str, endpoint: str) -> str:
    """Generate idempotent request ID: vz:{serial_short}:{endpoint_hash}:{timestamp_ms}"""
    serial_short = serial[3:11] if serial.startswith("VZ-") else serial[:8]
    endpoint_hash = hashlib.md5(endpoint.encode()).hexdigest()[:8]
    ts_ms = int(time.time() * 1000)
    return f"vz:{serial_short}:{endpoint_hash}:{ts_ms}"


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

def metered(category: str):
    """FastAPI dependency factory for metered endpoints.

    Usage:
        @router.post("/generate")
        async def generate(..., _meter: MeterDecision = Depends(metered("data"))):
    """
    async def _dependency(
        request: Request,
        store: SerialStore = Depends(get_serial_store),
    ) -> MeterDecision:
        # Standalone mode: no metering — all operations are free
        if settings.mode == "standalone":
            return MeterDecision(allowed=True, category=category)

        state = store.state

        # If MIGRATED, use ledger strategy
        if state.state == MIGRATED:
            strategy: MeteringStrategy = LedgerMeteringStrategy()
        else:
            strategy = SerialMeteringStrategy(store)

        cost = DEFAULT_DATA_COST if category == "data" else DEFAULT_SETUP_COST
        endpoint = f"{request.method}:{request.url.path}"
        request_id = _make_request_id(state.serial, endpoint)

        return await strategy.check_and_meter(category, cost, request_id)

    return _dependency
