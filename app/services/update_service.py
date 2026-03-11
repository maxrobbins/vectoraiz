"""BQ-VZ-AUTO-UPDATE: Software Update Service

- Checks GHCR for the latest published image tag
- Compares against current APP_VERSION
- Can trigger docker pull + container restart via Docker socket
- Caches GHCR results for 6 hours
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory cache (module-level singleton)
# ---------------------------------------------------------------------------
_cache: dict = {
    "latest_version": None,
    "latest_published_at": None,
    "checked_at": 0.0,  # monotonic timestamp
}

CACHE_TTL_S = 6 * 60 * 60  # 6 hours

GHCR_IMAGE = "aidotmarket/vectoraiz"
GHCR_REGISTRY = "ghcr.io"

# Semver-like tag pattern: optional v prefix, digits.digits.digits, optional -rc.N
_SEMVER_RE = re.compile(r"^v?(\d+\.\d+\.\d+)(?:-rc\.(\d+))?$")


def _get_current_version() -> str:
    """Import API_VERSION lazily to avoid circular imports."""
    from app.main import API_VERSION
    return API_VERSION


def _parse_semver(tag: str) -> tuple[int, ...] | None:
    """Parse a version tag into a sortable tuple.

    Stable versions sort higher than RC of the same base:
      1.20.34        → (1, 20, 34, 1, 0)   (stable flag=1)
      1.20.34-rc.3   → (1, 20, 34, 0, 3)   (rc flag=0, rc_num=3)
    """
    m = _SEMVER_RE.match(tag)
    if not m:
        return None
    base = tuple(int(p) for p in m.group(1).split("."))
    rc = m.group(2)
    if rc is not None:
        return base + (0, int(rc))  # RC: sorts below stable
    return base + (1, 0)  # Stable: sorts above any RC


# ---------------------------------------------------------------------------
# GHCR version check
# ---------------------------------------------------------------------------
async def _fetch_ghcr_token() -> str:
    """Get an anonymous bearer token for pulling from GHCR."""
    url = f"https://{GHCR_REGISTRY}/token?scope=repository:{GHCR_IMAGE}:pull"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()["token"]


async def _fetch_latest_tag() -> str | None:
    """Fetch tags from GHCR and return the highest semver tag."""
    token = await _fetch_ghcr_token()
    url = f"https://{GHCR_REGISTRY}/v2/{GHCR_IMAGE}/tags/list"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
        resp.raise_for_status()
        data = resp.json()

    tags = data.get("tags", [])
    semver_tags: list[tuple[tuple[int, ...], str]] = []
    for tag in tags:
        parsed = _parse_semver(tag)
        if parsed:
            semver_tags.append((parsed, tag))

    if not semver_tags:
        return None

    # Prefer the highest stable tag; fall back to highest RC if no stable exists
    stable = [(t, raw) for t, raw in semver_tags if t[-2] == 1]  # stable flag == 1
    if stable:
        stable.sort(reverse=True)
        return stable[0][1]
    semver_tags.sort(reverse=True)
    return semver_tags[0][1]


async def check_for_updates(force: bool = False) -> dict:
    """
    Return version info. Uses cache unless *force* is True or cache is stale.
    """
    current = _get_current_version()
    now = time.monotonic()

    if not force and _cache["latest_version"] and (now - _cache["checked_at"]) < CACHE_TTL_S:
        latest = _cache["latest_version"]
        return _build_response(current, latest, _cache["latest_published_at"])

    # Fetch from GHCR
    try:
        latest = await _fetch_latest_tag()
        _cache["latest_version"] = latest
        _cache["latest_published_at"] = None  # GHCR tags/list doesn't include dates
        _cache["checked_at"] = now
        logger.info("GHCR version check: current=%s latest=%s", current, latest)
    except Exception as e:
        logger.warning("GHCR version check failed: %s", e)
        latest = _cache.get("latest_version")
        if latest is None:
            return {
                "current": current,
                "latest": None,
                "update_available": False,
                "error": str(e),
                "changelog_url": "https://github.com/aidotmarket/vectoraiz/releases",
                "can_auto_update": _docker_available(),
            }

    return _build_response(current, latest, _cache.get("latest_published_at"))


def _build_response(current: str, latest: str | None, published_at: str | None) -> dict:
    update_available = False
    if latest:
        cur = _parse_semver(current)
        lat = _parse_semver(latest)
        if cur and lat:
            update_available = lat > cur

    return {
        "current": current,
        "latest": latest,
        "update_available": update_available,
        "latest_published_at": published_at,
        "changelog_url": "https://github.com/aidotmarket/vectoraiz/releases",
        "can_auto_update": _docker_available(),
    }


# ---------------------------------------------------------------------------
# Docker socket availability
# ---------------------------------------------------------------------------
def _docker_available() -> bool:
    return os.path.exists("/var/run/docker.sock")


# ---------------------------------------------------------------------------
# Docker-based auto-update
# ---------------------------------------------------------------------------
async def trigger_update() -> dict:
    """Pull the latest image and signal for restart."""
    if not _docker_available():
        return {
            "status": "docker_not_available",
            "message": (
                "Docker socket not mounted. To update manually:\n"
                "1. cd to your vectoraiz directory\n"
                "2. docker compose -f docker-compose.customer.yml pull vectoraiz\n"
                "3. docker compose -f docker-compose.customer.yml up -d vectoraiz"
            ),
        }

    current = _get_current_version()
    latest_info = await check_for_updates(force=True)
    if not latest_info.get("update_available"):
        return {"status": "up_to_date", "message": f"Already running the latest version ({current})."}

    latest = latest_info.get("latest", "latest")

    # Run the Docker pull in a thread to avoid blocking the event loop
    try:
        result = await asyncio.to_thread(_docker_pull_and_signal, latest)
        return result
    except Exception as e:
        logger.error("Auto-update failed: %s", e, exc_info=True)
        return {"status": "error", "message": f"Update failed: {e}"}


def _docker_pull_and_signal(tag: str) -> dict:
    """Synchronous Docker operations: pull image and write update-pending marker."""
    import docker

    client = docker.from_env()

    # 1. Pull the latest image
    image_name = f"{GHCR_REGISTRY}/{GHCR_IMAGE}"
    logger.info("Pulling %s:%s ...", image_name, tag)
    client.images.pull(image_name, tag=tag)
    # Also pull :latest to keep it in sync
    client.images.pull(image_name, tag="latest")
    logger.info("Pull complete for %s:%s", image_name, tag)

    # 2. Write update-pending marker so the entrypoint can detect a restart is needed
    update_marker = Path("/data/.update-pending")
    update_marker.write_text(tag)
    logger.info("Wrote update marker: %s", update_marker)

    # 3. Find our own container and restart it
    hostname = os.environ.get("HOSTNAME", "")
    try:
        container = client.containers.get(hostname)
        logger.info("Restarting container %s ...", container.name)
        container.restart(timeout=30)
    except Exception as e:
        # If we can't find by hostname, try by label or just signal via the marker
        logger.warning(
            "Could not restart container by hostname (%s): %s. "
            "The update marker has been written — restart the container manually or "
            "run: docker compose -f docker-compose.customer.yml up -d vectoraiz",
            hostname, e,
        )

    return {
        "status": "updating",
        "message": f"Pulled {image_name}:{tag}. Container is restarting...",
    }


# ---------------------------------------------------------------------------
# Background check (called from lifespan)
# ---------------------------------------------------------------------------
async def background_update_check_loop():
    """Check for updates on startup, then every 6 hours."""
    # Initial delay to let the app fully start
    await asyncio.sleep(30)

    while True:
        try:
            info = await check_for_updates()
            if info.get("update_available"):
                logger.info(
                    "Update available: current=%s latest=%s",
                    info["current"], info.get("latest"),
                )
        except Exception as e:
            logger.warning("Background update check failed: %s", e)

        await asyncio.sleep(CACHE_TTL_S)
