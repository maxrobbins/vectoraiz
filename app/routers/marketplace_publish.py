"""
Marketplace Publish Router
==========================

BQ-VZ-PUBLISH Phase 3: Proxies listing publish requests from VZ frontend
to ai.market backend, signing with Ed25519 JWT.

Flow: VZ Frontend -> VZ Backend (this router) -> ai.market Backend
The Ed25519 private key lives on VZ backend only.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Literal, Optional
from uuid import uuid4

import httpx
import jwt
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.auth.api_key_auth import get_current_user
from app.config import settings
from app.core.channel_config import CHANNEL
from app.core.crypto import DeviceCrypto
from app.services.registration_service import _get_device_id
from app.services.serial_store import get_serial_store

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class MarketplacePublishRequest(BaseModel):
    title: str = Field(..., max_length=200)
    description: str = Field(..., max_length=5000)
    tags: list[str] = Field(default_factory=list)
    category: Optional[str] = None
    pricing_type: Literal["one_time", "subscription"] = "one_time"
    price_cents: int = Field(..., ge=0)
    row_count: Optional[int] = None
    column_names: Optional[list[str]] = None
    column_types: Optional[list[str]] = None
    file_format: Optional[str] = None
    file_size_bytes: Optional[int] = None
    vz_dataset_id: str  # local VZ dataset ID, becomes vz_raw_listing_id


class MarketplacePublishResponse(BaseModel):
    status: str
    listing_id: Optional[str] = None
    marketplace_url: Optional[str] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _jcs_hash(body: dict) -> str:
    """RFC 8785 JCS-style canonical hash (sorted keys, compact separators)."""
    canonical = json.dumps(
        body, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _build_jwt(seller_id: str, device_id: str, metadata_hash: str, ed_priv) -> str:
    """Create a short-lived EdDSA JWT for the publish action."""
    now = datetime.now(timezone.utc)
    claims = {
        "sub": seller_id,
        "iss": device_id,
        "action": "publish_listing",
        "metadata_hash": metadata_hash,
        "exp": now.timestamp() + 300,
        "iat": now.timestamp(),
        "jti": str(uuid4()),
    }
    return jwt.encode(claims, ed_priv, algorithm="EdDSA")


def _get_crypto() -> DeviceCrypto:
    """Get an initialized DeviceCrypto instance."""
    if not settings.keystore_passphrase:
        raise HTTPException(
            status_code=503,
            detail="Keystore passphrase not configured — cannot sign marketplace requests",
        )
    return DeviceCrypto(
        keystore_path=settings.keystore_path,
        passphrase=settings.keystore_passphrase,
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/marketplace/publish", response_model=MarketplacePublishResponse)
async def publish_to_marketplace(
    body: MarketplacePublishRequest,
    user=Depends(get_current_user),
):
    """Publish a dataset listing to ai.market via signed JWT proxy."""
    # 1. Load crypto + keypairs
    crypto = _get_crypto()
    ed_priv, _ed_pub, _x_priv, _x_pub = crypto.get_or_create_keypairs()

    # 2. Resolve install_id (iss) and seller_id (sub)
    store = get_serial_store()
    device_id = _get_device_id()  # deterministic device hash = install_id
    cached = store.state.last_status_cache or {}
    seller_id = cached.get("gateway_user_id")
    if not seller_id:
        raise HTTPException(
            status_code=409,
            detail="Seller identity not available — ensure this VZ instance has completed activation and status sync with ai.market",
        )

    # 3. Build payload for ai.market
    payload = body.model_dump(exclude_none=True)
    # Map vz_dataset_id -> vz_raw_listing_id for ai.market
    payload["vz_raw_listing_id"] = payload.pop("vz_dataset_id")
    # Attribution: informational only, tells ai.market how VZ was installed
    payload["download_channel"] = CHANNEL.value

    # 4. JCS hash + JWT
    metadata_hash = _jcs_hash(payload)
    token = _build_jwt(seller_id, device_id, metadata_hash, ed_priv)

    # 5. POST to ai.market
    url = f"{settings.ai_market_url}/api/v1/vz/publish"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                url,
                json=payload,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            )
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail="Cannot reach ai.market — check network connectivity")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="ai.market request timed out")

    # 6. Return response
    if resp.status_code in (200, 201):
        data = resp.json()
        return MarketplacePublishResponse(
            status="published",
            listing_id=data.get("listing_id"),
            marketplace_url=data.get("marketplace_url"),
        )

    # Error passthrough
    try:
        err_data = resp.json()
        detail = err_data.get("detail") or err_data.get("error") or str(err_data)
    except Exception:
        detail = resp.text or f"ai.market returned {resp.status_code}"

    logger.warning("ai.market publish failed (%d): %s", resp.status_code, detail)
    raise HTTPException(status_code=resp.status_code, detail=detail)


@router.get("/marketplace/publish-status")
async def publish_status(user=Depends(get_current_user)):
    """Check if this VZ instance is ready to publish to ai.market."""
    # Must be in connected mode
    if settings.mode != "connected":
        return {"can_publish": False, "reason": "VZ is in standalone mode — connect to ai.market to publish"}

    # Must have keystore passphrase
    if not settings.keystore_passphrase:
        return {"can_publish": False, "reason": "Keystore passphrase not configured"}

    # Must have keypairs
    try:
        crypto = _get_crypto()
        crypto.get_or_create_keypairs()
    except Exception as e:
        return {"can_publish": False, "reason": f"Keypair error: {e}"}

    # Must have device registration (platform keys)
    if not crypto.has_platform_keys():
        return {"can_publish": False, "reason": "Device not registered with ai.market"}

    return {"can_publish": True, "reason": None}
