"""
Activation Manager — Startup lifespan hook for serial activation.
=================================================================

Runs during FastAPI lifespan:
1. Load serial state
2. Activate if PROVISIONED
3. Refresh token if version changed
4. Background: poll status every 5min

BQ-VZ-SERIAL-CLIENT
"""

from __future__ import annotations

import asyncio
import logging
import platform
from datetime import datetime, timezone
from typing import Optional

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
from app.services.serial_client import SerialClient

logger = logging.getLogger(__name__)

STATUS_POLL_INTERVAL = 300  # 5 minutes
ACTIVATION_RETRY_INTERVAL = 30  # 30 seconds


class ActivationManager:
    """Manages serial activation lifecycle at startup and in background."""

    def __init__(
        self,
        store: Optional[SerialStore] = None,
        client: Optional[SerialClient] = None,
    ):
        self._store = store or get_serial_store()
        self._client = client or SerialClient()
        self._instance_id = self._get_instance_id()
        self._background_task: Optional[asyncio.Task] = None

    def _get_instance_id(self) -> str:
        """Stable instance ID (hostname-based)."""
        return f"vz-{platform.node()}"

    async def startup(self) -> None:
        """Run during FastAPI lifespan startup."""
        state = self._store.state

        if state.state == UNPROVISIONED:
            if state.serial and state.bootstrap_token:
                logger.info("Serial: UNPROVISIONED but serial+bootstrap_token present — transitioning to PROVISIONED")
                self._store.state.state = PROVISIONED
                self._store.save()
            else:
                # Auto-provision in connected mode
                await self._auto_provision()
                if self._store.state.state == UNPROVISIONED:
                    # Auto-provision didn't work — start background loop which will retry
                    logger.info("Serial: UNPROVISIONED — will retry auto-provision in background")

        if state.state == MIGRATED:
            logger.info("Serial: MIGRATED — using BQ-073 ledger metering")
            return

        if state.state == PROVISIONED:
            await self._attempt_activation()

        if state.state == ACTIVE:
            # Check for version change → refresh
            current_version = settings.app_version
            if state.last_app_version and state.last_app_version != current_version:
                logger.info(
                    "Version changed: %s → %s — refreshing token",
                    state.last_app_version, current_version,
                )
                await self._attempt_refresh()
            self._store.update_app_version(current_version)

        # Start background tasks
        self._background_task = asyncio.create_task(self._background_loop())

    async def shutdown(self) -> None:
        """Cancel background tasks."""
        if self._background_task:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass

    async def _auto_provision(self) -> None:
        """Auto-generate a serial from ai.market on first boot."""
        if settings.mode == "standalone":
            return  # Don't auto-provision in standalone mode

        logger.info("Auto-provisioning serial from ai.market...")
        base_url = settings.aimarket_url.rstrip("/") if settings.aimarket_url else "https://ai-market-backend-production.up.railway.app"
        url = f"{base_url}/api/v1/serials/generate"

        import httpx
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
                resp = await client.post(url, json={
                    "source": "auto_provision",
                })
                if resp.status_code == 201:
                    data = resp.json()
                    serial = data["serial"]
                    bootstrap_token = data["bootstrap_token"]

                    # Store and transition to PROVISIONED
                    self._store.state.serial = serial
                    self._store.state.bootstrap_token = bootstrap_token
                    self._store.state.state = PROVISIONED
                    self._store.save()

                    logger.info("Auto-provisioned serial: %s — transitioning to PROVISIONED", serial[:16])

                    # Immediately attempt activation
                    await self._attempt_activation()
                else:
                    logger.warning("Auto-provision failed: HTTP %d — %s", resp.status_code, resp.text[:200])
        except Exception as e:
            logger.warning("Auto-provision failed (network): %s — will retry in background", e)

    async def _attempt_activation(self) -> None:
        """Try to activate with bootstrap token."""
        state = self._store.state
        if not state.serial or not state.bootstrap_token:
            logger.warning("Cannot activate: missing serial or bootstrap_token")
            return

        logger.info("Attempting serial activation: serial=%s", state.serial[:16])
        result = await self._client.activate(
            serial=state.serial,
            bootstrap_token=state.bootstrap_token,
            instance_id=self._instance_id,
            hostname=platform.node(),
            version=settings.app_version,
        )
        if result.success and result.install_token:
            self._store.transition_to_active(result.install_token)
            self._store.update_app_version(settings.app_version)
            # Auto-configure allAI copilot with activation credentials
            try:
                from app.services.allie_provider import write_allie_config, reset_provider
                write_allie_config(
                    serial_number=state.serial,
                    install_token=result.install_token,
                    ai_market_url=settings.aimarket_url,
                )
                reset_provider()
                logger.info("Serial activated successfully — allAI copilot configured")
            except Exception:
                logger.warning("Serial activated but failed to write allAI config", exc_info=True)
        else:
            logger.warning("Activation failed: %s (status=%d)", result.error, result.status_code)
            if result.status_code == 401:
                self._store.transition_to_unprovisioned()

    async def _attempt_refresh(self) -> None:
        """Refresh install token after version change."""
        state = self._store.state
        if not state.install_token:
            return

        result = await self._client.refresh(
            serial=state.serial,
            install_token=state.install_token,
            instance_id=self._instance_id,
        )
        if result.success and result.install_token:
            self._store.state.install_token = result.install_token
            self._store.save()
            # Update allAI config with refreshed token
            try:
                from app.services.allie_provider import write_allie_config, reset_provider
                write_allie_config(
                    serial_number=state.serial,
                    install_token=result.install_token,
                    ai_market_url=settings.aimarket_url,
                )
                reset_provider()
                logger.info("Token refreshed successfully — allAI copilot updated")
            except Exception:
                logger.warning("Token refreshed but failed to write allAI config", exc_info=True)
        elif result.status_code == 401:
            logger.warning("Refresh returned 401 — falling back to PROVISIONED")
            self._store.state.state = PROVISIONED
            self._store.state.install_token = None
            self._store.save()
        else:
            logger.warning("Token refresh failed (network): %s — keeping existing token", result.error)

    async def _poll_status(self) -> None:
        """Poll serial status and update cache."""
        state = self._store.state
        if state.state not in (ACTIVE, DEGRADED) or not state.install_token:
            return

        result = await self._client.status(state.serial, state.install_token)
        if result.success and result.data:
            self._store.record_success()
            now = datetime.now(timezone.utc).isoformat()
            self._store.update_status_cache(result.data, now)
            if result.migrated:
                gw_user_id = result.data.get("gateway_user_id")
                self._store.transition_to_migrated(gw_user_id)
        elif result.status_code == 401:
            self._store.transition_to_unprovisioned()
        else:
            self._store.record_failure()

    async def _background_loop(self) -> None:
        """Background loop: retry activation if PROVISIONED, poll status if ACTIVE."""
        try:
            while True:
                try:
                    state = self._store.state
                    if state.state == PROVISIONED:
                        await self._attempt_activation()
                        await asyncio.sleep(ACTIVATION_RETRY_INTERVAL)
                    elif state.state in (ACTIVE, DEGRADED):
                        await self._poll_status()
                        await asyncio.sleep(STATUS_POLL_INTERVAL)
                    elif state.state == MIGRATED:
                        # No serial ops needed
                        await asyncio.sleep(STATUS_POLL_INTERVAL)
                    else:
                        # UNPROVISIONED — try auto-provision
                        await self._auto_provision()
                        await asyncio.sleep(ACTIVATION_RETRY_INTERVAL)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Activation background loop iteration failed")
                    await asyncio.sleep(STATUS_POLL_INTERVAL)
        except asyncio.CancelledError:
            logger.info("Activation manager background loop cancelled")


# Module-level singleton
_manager: Optional[ActivationManager] = None


def get_activation_manager() -> ActivationManager:
    global _manager
    if _manager is None:
        _manager = ActivationManager()
    return _manager
