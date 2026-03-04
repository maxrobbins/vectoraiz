"""
Notification Router
===================

REST API endpoints for the persistent notification system.

Phase: BQ-VZ-NOTIFICATIONS — Persistent Notification System
Created: 2026-03-04
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()


# -- Response models --

class NotificationResponse(BaseModel):
    id: str
    type: str
    category: str
    title: str
    message: str
    metadata_json: Optional[str] = None
    read: bool
    batch_id: Optional[str] = None
    source: str
    created_at: str


class NotificationListResponse(BaseModel):
    notifications: list[NotificationResponse]
    count: int


class UnreadCountResponse(BaseModel):
    count: int


class MarkAllReadResponse(BaseModel):
    marked: int


def _to_response(n) -> NotificationResponse:
    return NotificationResponse(
        id=n.id,
        type=n.type,
        category=n.category,
        title=n.title,
        message=n.message,
        metadata_json=n.metadata_json,
        read=n.read,
        batch_id=n.batch_id,
        source=n.source,
        created_at=n.created_at.isoformat(),
    )


@router.get(
    "",
    response_model=NotificationListResponse,
    summary="List notifications",
)
async def list_notifications(
    limit: int = 50,
    offset: int = 0,
    category: Optional[str] = None,
    unread_only: bool = False,
):
    from app.services.notification_service import get_notification_service

    svc = get_notification_service()
    notifications = svc.list(limit=limit, offset=offset, category=category, unread_only=unread_only)
    return NotificationListResponse(
        notifications=[_to_response(n) for n in notifications],
        count=len(notifications),
    )


@router.get(
    "/unread-count",
    response_model=UnreadCountResponse,
    summary="Get unread notification count",
)
async def get_unread_count():
    from app.services.notification_service import get_notification_service

    svc = get_notification_service()
    return UnreadCountResponse(count=svc.get_unread_count())


@router.patch(
    "/{notification_id}/read",
    response_model=NotificationResponse,
    summary="Mark notification as read",
)
async def mark_read(notification_id: str):
    from app.services.notification_service import get_notification_service

    svc = get_notification_service()
    notification = svc.mark_read(notification_id)
    if not notification:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Notification not found")
    return _to_response(notification)


@router.post(
    "/read-all",
    response_model=MarkAllReadResponse,
    summary="Mark all notifications as read",
)
async def mark_all_read():
    from app.services.notification_service import get_notification_service

    svc = get_notification_service()
    count = svc.mark_all_read()
    return MarkAllReadResponse(marked=count)


@router.delete(
    "/{notification_id}",
    summary="Delete a notification",
)
async def delete_notification(notification_id: str):
    from app.services.notification_service import get_notification_service

    svc = get_notification_service()
    deleted = svc.delete(notification_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Notification not found")
    return {"message": "Notification deleted"}
