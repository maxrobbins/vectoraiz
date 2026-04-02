"""
Request Sync Service
====================

Polls the ai.market Data Request Board API, caches buyer requests locally.
Uses cursor-based pagination (Slice A) for incremental sync.

Phase: BQ-VZ-REQUEST-ENGINE Slice B
Created: 2026-04-02
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from sqlmodel import select

from app.core.database import get_session_context
from app.models.cached_requests import CachedRequest

logger = logging.getLogger(__name__)


async def poll_requests(
    api_base_url: str,
    sync_cursor: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Fetch data requests from ai.market's cursor-paginated API.

    Returns (items, next_cursor). Follows pages until exhausted or
    a reasonable cap (10 pages) to avoid runaway syncs.
    """
    all_items: List[Dict[str, Any]] = []
    cursor = sync_cursor
    max_pages = 10
    headers: Dict[str, str] = {}
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    url = f"{api_base_url.rstrip('/')}/api/v1/data-requests"

    async with httpx.AsyncClient(timeout=30.0) as client:
        for _ in range(max_pages):
            params: Dict[str, Any] = {"limit": 50}
            if cursor:
                params["cursor"] = cursor

            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            body = resp.json()

            items = body.get("items", [])
            all_items.extend(items)

            cursor = body.get("next_cursor")
            if not cursor or not items:
                break

    return all_items, cursor


def upsert_cached_requests(items: List[Dict[str, Any]]) -> Tuple[int, int]:
    """
    Insert or update cached requests from API response items.

    Returns (new_count, updated_count).
    """
    new_count = 0
    updated_count = 0
    now = datetime.now(timezone.utc)

    with get_session_context() as session:
        for item in items:
            mp_id = item.get("id", "")
            existing = session.exec(
                select(CachedRequest).where(
                    CachedRequest.marketplace_request_id == mp_id
                )
            ).first()

            categories = item.get("categories", [])
            if isinstance(categories, list):
                categories_json = json.dumps(categories)
            else:
                categories_json = json.dumps([])

            if existing:
                existing.title = item.get("title", existing.title)
                existing.description = item.get("description", existing.description)
                existing.categories = categories_json
                existing.urgency = item.get("urgency")
                existing.status = item.get("status", existing.status)
                existing.published_at = _parse_dt(item.get("published_at"))
                existing.expires_at = _parse_dt(item.get("expires_at"))
                existing.updated_at = now
                existing.synced_at = now
                session.add(existing)
                updated_count += 1
            else:
                cached = CachedRequest(
                    id=str(uuid.uuid4()),
                    marketplace_request_id=mp_id,
                    title=item.get("title", ""),
                    description=item.get("description", ""),
                    categories=categories_json,
                    urgency=item.get("urgency"),
                    status=item.get("status", "open"),
                    published_at=_parse_dt(item.get("published_at")),
                    expires_at=_parse_dt(item.get("expires_at")),
                    updated_at=now,
                    synced_at=now,
                )
                session.add(cached)
                new_count += 1

        session.commit()

    return new_count, updated_count


def get_sync_cursor() -> Optional[str]:
    """
    Derive the sync cursor from the latest synced_at timestamp.

    The ai.market API uses opaque cursors, but for our local state we track
    the most recent synced_at so the caller can pass it on the next poll.
    We store the cursor in a simple convention: the marketplace_request_id
    of the most recently synced item (sorted by synced_at desc).
    """
    with get_session_context() as session:
        result = session.exec(
            select(CachedRequest)
            .order_by(CachedRequest.synced_at.desc())  # type: ignore[union-attr]
            .limit(1)
        ).first()
        if result:
            return result.marketplace_request_id
    return None


async def full_sync(
    api_base_url: str,
    auth_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Complete sync loop: get cursor → poll → upsert → return summary.
    """
    cursor = get_sync_cursor()
    items, next_cursor = await poll_requests(api_base_url, cursor, auth_token)
    new_count, updated_count = upsert_cached_requests(items)

    return {
        "synced": len(items),
        "new": new_count,
        "updated": updated_count,
        "cursor": next_cursor,
    }


def _parse_dt(value: Any) -> Optional[datetime]:
    """Parse an ISO datetime string, returning None on failure."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
