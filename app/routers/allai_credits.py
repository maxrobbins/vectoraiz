"""
allAI Credits Router — Balance + Stripe Checkout proxy + Auto-Reload config
============================================================================

Proxies credit balance/usage and checkout requests to ai.market
via the SerialClient.  Auto-reload config stored in /data/auto_reload.json.
"""

import json
import logging
import os

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from app.auth.api_key_auth import get_current_user, AuthenticatedUser
from app.config import settings
from app.services.serial_client import SerialClient
from app.services.serial_store import get_serial_store, ACTIVE, DEGRADED

logger = logging.getLogger(__name__)

router = APIRouter()

AUTO_RELOAD_PATH = os.path.join(settings.data_directory, "auto_reload.json")

_DEFAULT_AUTO_RELOAD = {"enabled": False, "threshold_usd": 5.0, "reload_amount_usd": 25.0}


class AutoReloadBody(BaseModel):
    enabled: bool = False
    threshold_usd: float = Field(default=5.0, ge=1.0)
    reload_amount_usd: float = 25.0


def _read_auto_reload() -> dict:
    try:
        with open(AUTO_RELOAD_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(_DEFAULT_AUTO_RELOAD)


def _write_auto_reload(data: dict) -> None:
    os.makedirs(os.path.dirname(AUTO_RELOAD_PATH), exist_ok=True)
    with open(AUTO_RELOAD_PATH, "w") as f:
        json.dump(data, f, indent=2)


def _require_serial() -> tuple[str, str]:
    """Return (serial, install_token) or raise 409."""
    store = get_serial_store()
    state = store.state
    if state.state not in (ACTIVE, DEGRADED):
        raise HTTPException(
            status_code=409,
            detail="Serial not active. Connect to ai.market first.",
        )
    if not state.serial or not state.install_token:
        raise HTTPException(
            status_code=409,
            detail="Missing serial credentials.",
        )
    return state.serial, state.install_token


@router.get("/credits")
async def get_credits(user: AuthenticatedUser = Depends(get_current_user)):
    """Return allAI credit balance and recent usage."""
    serial, install_token = _require_serial()
    client = SerialClient()
    result = await client.credits_usage(serial, install_token)
    if not result.get("success"):
        raise HTTPException(
            status_code=result.get("status_code", 502),
            detail=result.get("error", "Failed to fetch credits"),
        )
    return result


@router.post("/credits/purchase")
async def purchase_credits(user: AuthenticatedUser = Depends(get_current_user)):
    """Create a Stripe Checkout session and return the checkout URL."""
    serial, install_token = _require_serial()
    client = SerialClient()
    result = await client.credits_checkout(serial, install_token)
    if not result.get("success"):
        raise HTTPException(
            status_code=result.get("status_code", 502),
            detail=result.get("error", "Failed to create checkout session"),
        )
    return result


@router.get("/credits/auto-reload")
async def get_auto_reload(user: AuthenticatedUser = Depends(get_current_user)):
    """Get auto-reload configuration."""
    return _read_auto_reload()


@router.post("/credits/auto-reload")
async def set_auto_reload(
    body: AutoReloadBody,
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Save auto-reload configuration."""
    data = {
        "enabled": body.enabled,
        "threshold_usd": body.threshold_usd,
        "reload_amount_usd": 25.0,  # Fixed for now
    }
    _write_auto_reload(data)
    return data
