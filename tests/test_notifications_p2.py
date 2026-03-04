"""
Tests for BQ-VZ-NOTIFICATIONS: Persistent Notification System (Phase 2)

Tests cover:
- CRUD operations via the notification service
- REST API endpoints
- Filtering and pagination
- Mark read / mark all read
- Pruning old notifications
"""

import pytest
from datetime import datetime, timezone, timedelta
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def clean_notifications():
    """Per-test cleanup of notifications table."""
    from app.core.database import get_session_context
    from app.models.notification import Notification
    from sqlmodel import select

    with get_session_context() as session:
        for n in session.exec(select(Notification)).all():
            session.delete(n)
        session.commit()
    yield


@pytest.fixture
def client():
    from app.main import app
    return TestClient(app)


@pytest.fixture
def svc():
    from app.services.notification_service import get_notification_service
    return get_notification_service()


class TestNotificationService:
    """Service-level tests."""

    def test_create_notification(self, svc):
        n = svc.create(
            type="info",
            category="system",
            title="Test notification",
            message="This is a test.",
        )
        assert n.id
        assert n.type == "info"
        assert n.category == "system"
        assert n.title == "Test notification"
        assert n.read is False
        assert n.source == "system"

    def test_list_notifications(self, svc):
        svc.create(type="info", category="system", title="First", message="msg1")
        svc.create(type="error", category="upload", title="Second", message="msg2")
        svc.create(type="warning", category="diagnostic", title="Third", message="msg3")

        all_notifs = svc.list()
        assert len(all_notifs) == 3

    def test_list_with_category_filter(self, svc):
        svc.create(type="info", category="system", title="Sys", message="msg")
        svc.create(type="error", category="upload", title="Upload", message="msg")

        system_only = svc.list(category="system")
        assert len(system_only) == 1
        assert system_only[0].category == "system"

    def test_list_unread_only(self, svc):
        n1 = svc.create(type="info", category="system", title="Unread", message="msg")
        n2 = svc.create(type="info", category="system", title="Read", message="msg")
        svc.mark_read(n2.id)

        unread = svc.list(unread_only=True)
        assert len(unread) == 1
        assert unread[0].id == n1.id

    def test_unread_count(self, svc):
        svc.create(type="info", category="system", title="A", message="msg")
        svc.create(type="info", category="system", title="B", message="msg")
        assert svc.get_unread_count() == 2

        svc.create(type="info", category="system", title="C", message="msg")
        assert svc.get_unread_count() == 3

    def test_mark_read(self, svc):
        n = svc.create(type="info", category="system", title="T", message="msg")
        assert n.read is False

        updated = svc.mark_read(n.id)
        assert updated.read is True
        assert svc.get_unread_count() == 0

    def test_mark_read_nonexistent(self, svc):
        result = svc.mark_read("nonexistent-id")
        assert result is None

    def test_mark_all_read(self, svc):
        svc.create(type="info", category="system", title="A", message="msg")
        svc.create(type="info", category="system", title="B", message="msg")
        svc.create(type="info", category="system", title="C", message="msg")

        count = svc.mark_all_read()
        assert count == 3
        assert svc.get_unread_count() == 0

    def test_delete(self, svc):
        n = svc.create(type="info", category="system", title="Del", message="msg")
        assert svc.delete(n.id) is True
        assert len(svc.list()) == 0

    def test_delete_nonexistent(self, svc):
        assert svc.delete("nonexistent-id") is False

    def test_prune_old(self, svc):
        # Create a notification and manually backdate it
        n = svc.create(type="info", category="system", title="Old", message="msg")

        from app.core.database import get_session_context
        from app.models.notification import Notification

        with get_session_context() as session:
            notification = session.get(Notification, n.id)
            notification.created_at = datetime.now(timezone.utc) - timedelta(days=31)
            session.add(notification)
            session.commit()

        pruned = svc.prune_old(days=30)
        assert pruned == 1
        assert len(svc.list()) == 0


class TestNotificationEndpoints:
    """API endpoint tests."""

    def test_list_empty(self, client):
        resp = client.get("/api/notifications")
        assert resp.status_code == 200
        data = resp.json()
        assert data["notifications"] == []
        assert data["count"] == 0

    def test_create_and_list(self, client, svc):
        svc.create(type="success", category="upload", title="Uploaded", message="file.csv uploaded")

        resp = client.get("/api/notifications")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["notifications"][0]["title"] == "Uploaded"
        assert data["notifications"][0]["type"] == "success"

    def test_unread_count_endpoint(self, client, svc):
        svc.create(type="info", category="system", title="A", message="msg")
        svc.create(type="info", category="system", title="B", message="msg")

        resp = client.get("/api/notifications/unread-count")
        assert resp.status_code == 200
        assert resp.json()["count"] == 2

    def test_mark_read_endpoint(self, client, svc):
        n = svc.create(type="info", category="system", title="T", message="msg")

        resp = client.patch(f"/api/notifications/{n.id}/read")
        assert resp.status_code == 200
        assert resp.json()["read"] is True

    def test_mark_read_not_found(self, client):
        resp = client.patch("/api/notifications/nonexistent/read")
        assert resp.status_code == 404

    def test_mark_all_read_endpoint(self, client, svc):
        svc.create(type="info", category="system", title="A", message="msg")
        svc.create(type="info", category="system", title="B", message="msg")

        resp = client.post("/api/notifications/read-all")
        assert resp.status_code == 200
        assert resp.json()["marked"] == 2

        # Verify all read
        resp2 = client.get("/api/notifications/unread-count")
        assert resp2.json()["count"] == 0

    def test_delete_endpoint(self, client, svc):
        n = svc.create(type="info", category="system", title="Del", message="msg")

        resp = client.delete(f"/api/notifications/{n.id}")
        assert resp.status_code == 200

        # Verify deleted
        resp2 = client.get("/api/notifications")
        assert resp2.json()["count"] == 0

    def test_delete_not_found(self, client):
        resp = client.delete("/api/notifications/nonexistent")
        assert resp.status_code == 404

    def test_filter_by_category(self, client, svc):
        svc.create(type="info", category="system", title="Sys", message="msg")
        svc.create(type="error", category="upload", title="Upload", message="msg")

        resp = client.get("/api/notifications?category=upload")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["notifications"][0]["category"] == "upload"

    def test_filter_unread_only(self, client, svc):
        n1 = svc.create(type="info", category="system", title="Unread", message="msg")
        n2 = svc.create(type="info", category="system", title="Read", message="msg")
        svc.mark_read(n2.id)

        resp = client.get("/api/notifications?unread_only=true")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1
        assert data["notifications"][0]["id"] == n1.id
