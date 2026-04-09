"""
vectorAIz Device Registration Service
======================================

PURPOSE:
    Registers this vectorAIz instance with ai.market by sending
    Ed25519 + X25519 public keys, and stores the returned platform
    public keys + certificate locally for Trust Channel handshake.

BQ-102 ST-3: Registration client
- POST /api/v1/trust/register with key_type="ed25519"
- Exponential backoff retry (max 3 attempts, AG Council review)
- 409 recovery: re-fetch platform keys for already-registered devices
- Atomic keystore storage via DeviceCrypto
- Non-blocking: logs warning on failure, app continues
"""

import hashlib
import logging
import platform
from typing import Optional

import httpx

from app.config import settings
from app.core.crypto import DeviceCrypto

logger = logging.getLogger(__name__)

# Retry config
MAX_RETRIES = 3
INITIAL_BACKOFF_S = 2.0  # doubles each retry


def _get_device_id() -> str:
    """
    Generate a deterministic device ID from machine characteristics.
    Uses platform node (MAC-based) + machine type as a stable fingerprint.
    In Docker, this should be stable across container restarts if the
    hostname/MAC is preserved.
    """
    raw = f"{platform.node()}:{platform.machine()}:{platform.system()}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _get_os_type() -> str:
    """Map platform.system() to OSType enum values."""
    system = platform.system().lower()
    if system == "darwin":
        return "macos"
    elif system == "linux":
        # Check if running in Docker
        try:
            with open("/proc/1/cgroup", "r") as f:
                if "docker" in f.read():
                    return "docker"
        except (FileNotFoundError, PermissionError):
            pass
        return "linux"
    elif system == "windows":
        return "windows"
    return "linux"


async def register_with_marketplace(
    crypto: DeviceCrypto,
    api_key: Optional[str] = None,
) -> bool:
    """
    Register this device with ai.market's Trust Channel.

    Sends Ed25519 + X25519 public keys to POST /api/v1/trust/register
    and stores the returned platform keys + certificate.

    Args:
        crypto: Initialized DeviceCrypto with keypairs generated.
        api_key: API key for authentication. If None, uses internal_api_key.

    Returns:
        True if registration succeeded or device already registered with keys.
        False if registration failed (non-fatal).
    """
    # Skip if already have platform keys
    if crypto.has_platform_keys():
        logger.info("Platform keys already present in keystore — skipping registration")
        return True

    ed25519_pub_b64, x25519_pub_b64 = crypto.get_public_keys_b64()
    device_id = _get_device_id()
    os_type = _get_os_type()

    payload = {
        "device_id": device_id,
        "vectoraiz_version": "2.0.0",  # TODO: read from package
        "os_type": os_type,
        "key_type": "ed25519",
        "ed25519_public_key": ed25519_pub_b64,
        "x25519_public_key": x25519_pub_b64,
    }

    auth_key = api_key or settings.internal_api_key
    if not auth_key:
        logger.warning("No API key available for device registration — skipping")
        return False

    headers = {
        "X-API-Key": auth_key,
        "Content-Type": "application/json",
    }

    url = f"{settings.ai_market_url}/api/v1/trust/register"
    backoff = INITIAL_BACKOFF_S

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, json=payload, headers=headers)

            if resp.status_code == 200:
                data = resp.json()
                crypto.store_platform_keys(
                    platform_ed25519_pub=data.get("ai_market_ed25519_public_key", ""),
                    platform_x25519_pub=data.get("ai_market_x25519_public_key", ""),
                    certificate=data.get("certificate", ""),
                )
                logger.info(f"Device registered with ai.market (device_id={device_id[:16]}...)")
                return True

            elif resp.status_code == 409:
                # AG Council: handle "already registered" — response may include platform keys
                data = resp.json()
                if data.get("ai_market_ed25519_public_key"):
                    # 409 with platform keys = already registered, just store them
                    crypto.store_platform_keys(
                        platform_ed25519_pub=data.get("ai_market_ed25519_public_key", ""),
                        platform_x25519_pub=data.get("ai_market_x25519_public_key", ""),
                        certificate=data.get("certificate", ""),
                    )
                    logger.info("Device already registered — platform keys recovered from 409")
                    return True
                else:
                    logger.warning(f"Device already registered but no platform keys in response: {data}")
                    return False

            elif resp.status_code in (401, 403):
                logger.error(f"Registration auth failed ({resp.status_code}): {resp.text}")
                return False  # Don't retry auth failures

            else:
                logger.warning(
                    f"Registration attempt {attempt}/{MAX_RETRIES} failed: "
                    f"{resp.status_code} {resp.text}"
                )

        except httpx.TimeoutException:
            logger.warning(f"Registration attempt {attempt}/{MAX_RETRIES} timed out")
        except httpx.ConnectError as e:
            logger.warning(f"Registration attempt {attempt}/{MAX_RETRIES} connection error: {e}")
        except Exception as e:
            logger.error(f"Registration attempt {attempt}/{MAX_RETRIES} unexpected error: {e}")

        # Exponential backoff (skip sleep on last attempt)
        if attempt < MAX_RETRIES:
            import asyncio
            logger.info(f"Retrying in {backoff:.0f}s...")
            await asyncio.sleep(backoff)
            backoff *= 2

    logger.error(f"Device registration failed after {MAX_RETRIES} attempts — will retry on next startup")
    return False
