"""
Notification Service
====================

CRUD operations for the persistent notification system.
Handles creation, listing, read/unread state, and pruning.

Phase: BQ-VZ-NOTIFICATIONS — Persistent Notification System
Created: 2026-03-04
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlmodel import select, func, col

logger = logging.getLogger(__name__)


class NotificationService:
    """Service for managing persistent notifications."""

    def create(
        self,
        type: str,
        category: str,
        title: str,
        message: str,
        metadata_json: Optional[str] = None,
        batch_id: Optional[str] = None,
        source: str = "system",
    ):
        from app.core.database import get_session_context
        from app.models.notification import Notification

        notification = Notification(
            type=type,
            category=category,
            title=title,
            message=message,
            metadata_json=metadata_json,
            batch_id=batch_id,
            source=source,
        )
        with get_session_context() as session:
            session.add(notification)
            session.commit()
            session.refresh(notification)
            return notification

    def list(
        self,
        limit: int = 50,
        offset: int = 0,
        category: Optional[str] = None,
        unread_only: bool = False,
    ):
        from app.core.database import get_session_context
        from app.models.notification import Notification

        with get_session_context() as session:
            stmt = select(Notification)
            if category:
                stmt = stmt.where(Notification.category == category)
            if unread_only:
                stmt = stmt.where(Notification.read == False)  # noqa: E712
            stmt = stmt.order_by(col(Notification.created_at).desc())
            stmt = stmt.offset(offset).limit(limit)
            return session.exec(stmt).all()

    def get_unread_count(self) -> int:
        from app.core.database import get_session_context
        from app.models.notification import Notification

        with get_session_context() as session:
            stmt = select(func.count()).select_from(Notification).where(
                Notification.read == False  # noqa: E712
            )
            return session.exec(stmt).one()

    def mark_read(self, notification_id: str):
        from app.core.database import get_session_context
        from app.models.notification import Notification

        with get_session_context() as session:
            notification = session.get(Notification, notification_id)
            if not notification:
                return None
            notification.read = True
            session.add(notification)
            session.commit()
            session.refresh(notification)
            return notification

    def mark_all_read(self) -> int:
        from app.core.database import get_session_context
        from app.models.notification import Notification

        with get_session_context() as session:
            stmt = select(Notification).where(Notification.read == False)  # noqa: E712
            unread = session.exec(stmt).all()
            count = len(unread)
            for n in unread:
                n.read = True
                session.add(n)
            session.commit()
            return count

    def delete(self, notification_id: str) -> bool:
        from app.core.database import get_session_context
        from app.models.notification import Notification

        with get_session_context() as session:
            notification = session.get(Notification, notification_id)
            if not notification:
                return False
            session.delete(notification)
            session.commit()
            return True

    def prune_old(self, days: int = 30) -> int:
        from app.core.database import get_session_context
        from app.models.notification import Notification

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        with get_session_context() as session:
            stmt = select(Notification).where(Notification.created_at < cutoff)
            old = session.exec(stmt).all()
            count = len(old)
            for n in old:
                session.delete(n)
            session.commit()
            logger.info("Pruned %d notifications older than %d days", count, days)
            return count


_notification_service: Optional[NotificationService] = None


def get_notification_service() -> NotificationService:
    global _notification_service
    if _notification_service is None:
        _notification_service = NotificationService()
    return _notification_service
