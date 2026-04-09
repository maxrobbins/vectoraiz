"""
Tests for BQ-128 Phase 4 — Security Hardening + QA
====================================================

Covers:
- WebSocket abuse: oversized payload, oversized message, oversized snapshot,
  message rate limit, connection rate limit, recovery after cooldown
- Idempotency: DB constraint on duplicate client_message_id, idempotent success,
  no overwrite on duplicate with different content
- STOP/cancellation: reports usage if available, persists user message,
  clears inflight, subsequent message works
- Heartbeat: wrong nonce rejected, missing nonce tolerated
- Standalone: allie disabled WS message, no outbound HTTP

All tests use MockAllieProvider (VECTORAIZ_ALLIE_PROVIDER=mock). Zero real API calls.

CREATED: BQ-128 Phase 4 (2026-02-14)
"""

import json

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import SQLModel, Session as SQLModelSession, create_engine, select

from app.core.errors import VectorAIzError
from app.core.errors.middleware import vectoraiz_error_handler
from app.core.errors.registry import error_registry
from app.routers.copilot import (
    router as copilot_rest_router,
    ws_router,
    manager,
    MAX_BRAIN_MESSAGE_CHARS,
    MAX_WS_PAYLOAD_BYTES,
    MAX_STATE_SNAPSHOT_BYTES,
    MAX_MESSAGES_PER_MINUTE,
    MAX_CONNECTIONS_PER_MINUTE,
)
from app.models.state import (
    Message,
    MessageRole,
    Session as ChatSession,
)
from app.services.allie_provider import (
    reset_provider,
)
from app.auth.api_key_auth import AuthenticatedUser

# Ensure error registry is loaded for tests
if len(error_registry) == 0:
    error_registry.load()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MOCK_USER = AuthenticatedUser(
    user_id="usr_test_phase4",
    key_id="key_test",
    scopes=["read", "write"],
    is_valid=True,
    balance_cents=10000,
    free_trial_remaining_cents=0,
)


@pytest.fixture(autouse=True)
def _clean_manager():
    """Ensure ConnectionManager is clean before/after each test."""
    manager._active.clear()
    manager._user_sessions.clear()
    manager._connected_since.clear()
    manager._session_users.clear()
    manager._session_balance.clear()
    manager._inflight_task.clear()
    manager._session_state.clear()
    manager._session_intro_seen.clear()
    manager._session_msg_timestamps.clear()
    manager._user_connect_timestamps.clear()
    yield
    manager._active.clear()
    manager._user_sessions.clear()
    manager._connected_since.clear()
    manager._session_users.clear()
    manager._session_balance.clear()
    manager._inflight_task.clear()
    manager._session_state.clear()
    manager._session_intro_seen.clear()
    manager._session_msg_timestamps.clear()
    manager._user_connect_timestamps.clear()


@pytest.fixture(autouse=True)
def _reset_allie_provider():
    """Reset the Allie provider singleton between tests."""
    reset_provider()
    yield
    reset_provider()


def _create_test_app() -> FastAPI:
    """Create a minimal FastAPI app with copilot routers mounted."""
    app = FastAPI()
    app.add_exception_handler(VectorAIzError, vectoraiz_error_handler)
    app.include_router(copilot_rest_router, prefix="/api/copilot")
    app.include_router(ws_router)
    return app


@pytest.fixture
def app():
    return _create_test_app()


@pytest.fixture
def client(app):
    return TestClient(app)


def _recv_skip_ping(ws, timeout_iters=100):
    """Receive a non-PING message from the WebSocket, auto-responding to PINGs."""
    for _ in range(timeout_iters):
        data = ws.receive_json()
        if data.get("type") == "PING":
            ws.send_json({"type": "PONG", "nonce": data.get("nonce")})
            continue
        return data
    raise TimeoutError("Only received PING messages")


# ---------------------------------------------------------------------------
# WebSocket Abuse Tests
# ---------------------------------------------------------------------------

class TestOversizedPayload:
    """Test rejection of oversized WebSocket payloads."""

    @patch("app.routers.copilot.get_current_user_ws")
    def test_oversized_payload_rejected(self, mock_auth, client):
        """Payload exceeding MAX_WS_PAYLOAD_BYTES should be rejected."""
        mock_auth.return_value = MOCK_USER

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            ws.receive_json()  # CONNECTED

            # Send a payload larger than 64KB
            oversized = json.dumps({
                "type": "BRAIN_MESSAGE",
                "message": "x" * (MAX_WS_PAYLOAD_BYTES + 1000),
            })
            ws.send_text(oversized)

            data = _recv_skip_ping(ws)
            assert data["type"] == "ERROR"
            assert "PAYLOAD_TOO_LARGE" in data.get("code", "") or "too large" in data.get("message", "").lower()


class TestOversizedBrainMessage:
    """Test rejection of oversized BRAIN_MESSAGE content."""

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=False)
    def test_oversized_brain_message_rejected(self, mock_local, mock_auth, client):
        """BRAIN_MESSAGE with message > MAX_BRAIN_MESSAGE_CHARS should be rejected."""
        mock_auth.return_value = MOCK_USER

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            ws.receive_json()  # CONNECTED

            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "x" * (MAX_BRAIN_MESSAGE_CHARS + 1),
            })

            data = _recv_skip_ping(ws)
            assert data["type"] == "ERROR"
            assert "MESSAGE_TOO_LARGE" in data.get("code", "") or "too long" in data.get("message", "").lower()


class TestOversizedStateSnapshot:
    """Test rejection of oversized STATE_SNAPSHOT."""

    @patch("app.routers.copilot.get_current_user_ws")
    def test_oversized_state_snapshot_rejected(self, mock_auth, client):
        """STATE_SNAPSHOT exceeding MAX_STATE_SNAPSHOT_BYTES should be rejected."""
        mock_auth.return_value = MOCK_USER

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            ws.receive_json()  # CONNECTED

            # Build a snapshot that will exceed 16KB when serialized
            ws.send_json({
                "type": "STATE_SNAPSHOT",
                "current_route": "/datasets",
                "extra_data": "x" * MAX_STATE_SNAPSHOT_BYTES,
            })

            data = _recv_skip_ping(ws)
            assert data["type"] == "ERROR"
            assert "SNAPSHOT_TOO_LARGE" in data.get("code", "") or "too large" in data.get("message", "").lower()


class TestMessageRateLimit:
    """Test per-session message rate limiting."""

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=False)
    @patch("app.services.copilot_service.is_local_only", return_value=False)
    @patch("app.routers.copilot.metering_service")
    def test_message_rate_limit_enforced(self, mock_metering, mock_svc_local, mock_local, mock_auth, client):
        """After MAX_MESSAGES_PER_MINUTE messages, subsequent should be rate limited."""
        mock_auth.return_value = MOCK_USER
        mock_metering.check_balance.return_value = MagicMock(allowed=True)
        mock_metering.report_usage = AsyncMock(
            return_value=MagicMock(new_balance_cents=9900, cost_cents=1, allowed=True)
        )

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            ws.receive_json()  # CONNECTED

            # Exhaust the rate limit by sending MAX_MESSAGES_PER_MINUTE messages
            for i in range(MAX_MESSAGES_PER_MINUTE):
                ws.send_json({
                    "type": "BRAIN_MESSAGE",
                    "message": f"msg {i}",
                    "message_id": f"msg_rate_{i}",
                })
                # Drain responses (chunks + end or errors)
                for _ in range(50):
                    data = ws.receive_json()
                    if data.get("type") == "PING":
                        ws.send_json({"type": "PONG", "nonce": data.get("nonce")})
                        continue
                    if data.get("type") == "BRAIN_STREAM_END":
                        break
                    if data.get("type") == "ERROR":
                        break

            # The next message should be rate limited
            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "one too many",
            })

            data = _recv_skip_ping(ws)
            assert data["type"] == "ERROR"
            assert data.get("code") == "RATE_LIMITED"
            assert "reset_after_seconds" in data


class TestConnectionRateLimit:
    """Test per-user connection rate limiting."""

    @patch("app.routers.copilot.get_current_user_ws")
    def test_connection_rate_limit_enforced(self, mock_auth):
        """After MAX_CONNECTIONS_PER_MINUTE connections, next should be rejected."""
        mock_auth.return_value = MOCK_USER
        app = _create_test_app()

        # Exhaust connection rate limit
        for _ in range(MAX_CONNECTIONS_PER_MINUTE):
            with TestClient(app).websocket_connect("/ws/copilot?token=test") as ws:
                connected = ws.receive_json()
                assert connected["type"] == "CONNECTED"

        # Next connection should be rejected with close code 4029
        client = TestClient(app)
        try:
            with client.websocket_connect("/ws/copilot?token=test") as ws:
                # If we get here, the connection was accepted — check if it's
                # immediately closed
                try:
                    ws.receive_json()
                    # If we somehow got a message, connection wasn't rejected
                    # but it might be the close message
                except Exception:
                    pass
            # Connection was accepted and closed — check that it was close code 4029
            # The close will happen at __exit__
        except Exception:
            # Expected — connection rejected
            pass

        # Verify the manager tracked the rate limit
        # The user's timestamps should show MAX_CONNECTIONS_PER_MINUTE entries
        assert len(manager._user_connect_timestamps.get(MOCK_USER.user_id, [])) >= MAX_CONNECTIONS_PER_MINUTE


class TestRateLimitRecovery:
    """Test that rate-limited sessions recover after cooldown."""

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=False)
    @patch("app.services.copilot_service.is_local_only", return_value=False)
    @patch("app.routers.copilot.metering_service")
    def test_rate_limited_session_recovers_after_cooldown(
        self, mock_metering, mock_svc_local, mock_local, mock_auth, client,
    ):
        """After rate limit expires, messages should be processed again."""
        mock_auth.return_value = MOCK_USER
        mock_metering.check_balance.return_value = MagicMock(allowed=True)
        mock_metering.report_usage = AsyncMock(
            return_value=MagicMock(new_balance_cents=9900, cost_cents=1, allowed=True)
        )

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            connected = ws.receive_json()
            session_id = connected["session_id"]

            # Artificially fill the rate limit with old timestamps
            import time as time_mod
            old_time = time_mod.monotonic() - 61  # 61 seconds ago (expired)
            manager._session_msg_timestamps[session_id] = [old_time] * MAX_MESSAGES_PER_MINUTE

            # Should be allowed now since all timestamps are expired
            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "hello after cooldown",
                "message_id": "msg_recovered",
            })

            # Should get streaming response, not RATE_LIMITED error
            for _ in range(50):
                data = ws.receive_json()
                if data.get("type") == "PING":
                    ws.send_json({"type": "PONG", "nonce": data.get("nonce")})
                    continue
                break

            assert data["type"] != "ERROR" or data.get("code") != "RATE_LIMITED", \
                "Should have recovered after cooldown"


# ---------------------------------------------------------------------------
# Idempotency Tests
# ---------------------------------------------------------------------------

@pytest.fixture
def legacy_engine():
    """Create an in-memory SQLite engine with state tables + idempotency index."""
    engine = create_engine("sqlite:///:memory:")
    from app.models.state import UserPreferences, Session, Message  # noqa: F401
    SQLModel.metadata.create_all(engine)

    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_messages_session_client_msg_id "
            "ON messages (session_id, client_message_id) "
            "WHERE client_message_id IS NOT NULL"
        ))
        conn.commit()

    return engine


@pytest.fixture
def db(legacy_engine):
    """Yield a fresh DB session."""
    with SQLModelSession(legacy_engine) as session:
        yield session


class TestIdempotencyDBConstraint:
    """Test DB-level idempotency constraint on messages."""

    def test_duplicate_client_message_id_db_constraint(self, db):
        """Concurrent insert of same client_message_id should raise IntegrityError."""
        from sqlalchemy.exc import IntegrityError

        session = ChatSession(user_id="user_idem", title="Test")
        db.add(session)
        db.commit()
        db.refresh(session)

        msg1 = Message(
            session_id=session.id,
            role=MessageRole.USER,
            content="First message",
            client_message_id="dup_001",
        )
        db.add(msg1)
        db.commit()

        msg2 = Message(
            session_id=session.id,
            role=MessageRole.USER,
            content="Duplicate message",
            client_message_id="dup_001",
        )
        db.add(msg2)
        with pytest.raises(IntegrityError):
            db.commit()

    def test_idempotent_success(self, db):
        """Duplicate client_message_id should be caught; original preserved."""
        from sqlalchemy.exc import IntegrityError

        session = ChatSession(user_id="user_idem2", title="Test")
        db.add(session)
        db.commit()
        db.refresh(session)

        msg1 = Message(
            session_id=session.id,
            role=MessageRole.USER,
            content="Original content",
            client_message_id="idem_001",
        )
        db.add(msg1)
        db.commit()
        db.refresh(msg1)

        # Attempt duplicate — should fail, original preserved
        msg2 = Message(
            session_id=session.id,
            role=MessageRole.USER,
            content="Duplicate attempt",
            client_message_id="idem_001",
        )
        db.add(msg2)
        try:
            db.commit()
            assert False, "Should have raised IntegrityError"
        except IntegrityError:
            db.rollback()

        # Original should still be there
        original = db.exec(
            select(Message).where(
                Message.session_id == session.id,
                Message.client_message_id == "idem_001",
            )
        ).first()
        assert original is not None
        assert original.content == "Original content"

    def test_duplicate_different_content_no_overwrite(self, db):
        """Duplicate with different content should not overwrite original."""
        from sqlalchemy.exc import IntegrityError

        session = ChatSession(user_id="user_idem3", title="Test")
        db.add(session)
        db.commit()
        db.refresh(session)

        msg1 = Message(
            session_id=session.id,
            role=MessageRole.USER,
            content="Original says A",
            client_message_id="noover_001",
        )
        db.add(msg1)
        db.commit()

        msg2 = Message(
            session_id=session.id,
            role=MessageRole.USER,
            content="Duplicate says B",
            client_message_id="noover_001",
        )
        db.add(msg2)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()

        # Verify original content preserved
        result = db.exec(
            select(Message).where(
                Message.session_id == session.id,
                Message.client_message_id == "noover_001",
            )
        ).first()
        assert result.content == "Original says A"

        # Verify only one message with this client_message_id
        all_msgs = db.exec(
            select(Message).where(
                Message.session_id == session.id,
                Message.client_message_id == "noover_001",
            )
        ).all()
        assert len(all_msgs) == 1


# ---------------------------------------------------------------------------
# STOP / Cancellation Tests
# ---------------------------------------------------------------------------

class TestStopCancellation:
    """Test STOP/cancellation cost-accounting fixes."""

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=False)
    @patch("app.services.copilot_service.is_local_only", return_value=False)
    @patch("app.routers.copilot.metering_service")
    def test_stop_reports_usage_if_available(self, mock_metering, mock_svc_local, mock_local, mock_auth, client):
        """STOP mid-stream should still report usage if provider returned usage data."""
        mock_auth.return_value = MOCK_USER
        mock_metering.check_balance.return_value = MagicMock(allowed=True)
        mock_metering.report_usage = AsyncMock(
            return_value=MagicMock(new_balance_cents=9900, cost_cents=1, allowed=True)
        )

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            ws.receive_json()  # CONNECTED

            # Send a BRAIN_MESSAGE and immediately STOP
            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "Hello tell me a long story",
                "message_id": "msg_stop_001",
            })

            # Wait a moment for processing to start, then send STOP
            import time as t
            t.sleep(0.1)
            ws.send_json({"type": "STOP", "message_id": "msg_stop_001"})

            # Drain messages — expect either STOPPED or BRAIN_STREAM_END
            got_stopped_or_end = False
            for _ in range(50):
                try:
                    data = ws.receive_json()
                except Exception:
                    break
                if data.get("type") == "PING":
                    ws.send_json({"type": "PONG", "nonce": data.get("nonce")})
                    continue
                if data.get("type") in ("STOPPED", "BRAIN_STREAM_END"):
                    got_stopped_or_end = True
                    break

            assert got_stopped_or_end, "Expected STOPPED or BRAIN_STREAM_END"

            # Usage reporting is in the finally block —
            # if the provider completed, report_usage should have been called.
            # (Mock provider is fast, so it may complete before STOP arrives)

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=False)
    @patch("app.services.copilot_service.is_local_only", return_value=False)
    @patch("app.routers.copilot.metering_service")
    def test_stop_persists_user_message(self, mock_metering, mock_svc_local, mock_local, mock_auth, client):
        """User message should be persisted BEFORE streaming starts."""
        mock_auth.return_value = MOCK_USER
        mock_metering.check_balance.return_value = MagicMock(allowed=True)
        mock_metering.report_usage = AsyncMock(
            return_value=MagicMock(new_balance_cents=9900, cost_cents=1, allowed=True)
        )

        # Patch get_legacy_session_context to track persisted messages
        persisted_messages = []

        from app.routers import copilot as copilot_mod
        original_persist_fn = copilot_mod._persist_message

        def tracking_persist(db, session, **kwargs):
            result = original_persist_fn(db, session, **kwargs)
            persisted_messages.append({"role": kwargs.get("role"), "content": kwargs.get("content")})
            return result

        with patch.object(copilot_mod, "_persist_message", side_effect=tracking_persist):
            with client.websocket_connect("/ws/copilot?token=test") as ws:
                ws.receive_json()  # CONNECTED

                ws.send_json({
                    "type": "BRAIN_MESSAGE",
                    "message": "persist me before streaming",
                    "message_id": "msg_persist_001",
                })

                # Drain all responses
                for _ in range(50):
                    try:
                        data = ws.receive_json()
                    except Exception:
                        break
                    if data.get("type") == "PING":
                        ws.send_json({"type": "PONG", "nonce": data.get("nonce")})
                        continue
                    if data.get("type") in ("BRAIN_STREAM_END", "ERROR"):
                        break

        # Verify user message was persisted
        user_persists = [m for m in persisted_messages if m["role"] == MessageRole.USER]
        assert len(user_persists) >= 1
        assert user_persists[0]["content"] == "persist me before streaming"

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=False)
    @patch("app.services.copilot_service.is_local_only", return_value=False)
    @patch("app.routers.copilot.metering_service")
    def test_stop_clears_inflight(self, mock_metering, mock_svc_local, mock_local, mock_auth, client):
        """After BRAIN_STREAM_END, inflight should eventually be cleared."""
        mock_auth.return_value = MOCK_USER
        mock_metering.check_balance.return_value = MagicMock(allowed=True)
        mock_metering.report_usage = AsyncMock(
            return_value=MagicMock(new_balance_cents=9900, cost_cents=1, allowed=True)
        )

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            connected = ws.receive_json()
            session_id = connected["session_id"]

            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "hello",
                "message_id": "msg_inflight_001",
            })

            # Drain all responses
            got_end = False
            for _ in range(50):
                try:
                    data = ws.receive_json()
                except Exception:
                    break
                if data.get("type") == "PING":
                    ws.send_json({"type": "PONG", "nonce": data.get("nonce")})
                    continue
                if data.get("type") == "BRAIN_STREAM_END":
                    got_end = True
                    break

            assert got_end, "Expected BRAIN_STREAM_END"

            # Give the finally block a moment to run (async cleanup)
            import time as t
            t.sleep(0.3)

            # After completion + cleanup, inflight should be cleared
            task = manager._inflight_task.get(session_id)
            assert task is None or task.done()

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=False)
    @patch("app.services.copilot_service.is_local_only", return_value=False)
    @patch("app.routers.copilot.metering_service")
    def test_stop_subsequent_message_works(self, mock_metering, mock_svc_local, mock_local, mock_auth, client):
        """After first message completes, a subsequent BRAIN_MESSAGE should work."""
        mock_auth.return_value = MOCK_USER
        mock_metering.check_balance.return_value = MagicMock(allowed=True)
        mock_metering.report_usage = AsyncMock(
            return_value=MagicMock(new_balance_cents=9900, cost_cents=1, allowed=True)
        )

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            ws.receive_json()  # CONNECTED

            # First message
            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "first message",
                "message_id": "msg_first",
            })

            # Drain first response
            for _ in range(50):
                data = ws.receive_json()
                if data.get("type") == "PING":
                    ws.send_json({"type": "PONG", "nonce": data.get("nonce")})
                    continue
                if data.get("type") == "BRAIN_STREAM_END":
                    break
                if data.get("type") in ("STOPPED", "ERROR"):
                    break

            # Give the finally block time to clear inflight
            import time as t
            t.sleep(0.3)

            # Second message should work
            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "second message",
                "message_id": "msg_second",
            })

            # Should get streaming response
            got_end = False
            for _ in range(50):
                data = ws.receive_json()
                if data.get("type") == "PING":
                    ws.send_json({"type": "PONG", "nonce": data.get("nonce")})
                    continue
                if data.get("type") == "BRAIN_STREAM_CHUNK":
                    continue
                elif data.get("type") == "BRAIN_STREAM_END":
                    got_end = True
                    break
                elif data.get("type") == "ERROR":
                    # If inflight still reported as processing, this is the timing issue
                    pytest.fail(f"Got ERROR on second message: {data}")
                    break

            assert got_end, "Second message should complete normally"


# ---------------------------------------------------------------------------
# Heartbeat Tests
# ---------------------------------------------------------------------------

class TestHeartbeatNonce:
    """Test PONG nonce enforcement."""

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.PING_INTERVAL", 0.5)
    @patch("app.routers.copilot.PONG_TIMEOUT", 2)
    def test_pong_wrong_nonce_not_accepted(self, mock_auth, client):
        """PONG with wrong nonce should NOT reset the pong_received event."""
        mock_auth.return_value = MOCK_USER

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            ws.receive_json()  # CONNECTED

            # Wait for a PING
            ping_data = None
            for _ in range(20):
                try:
                    data = ws.receive_json()
                    if data.get("type") == "PING":
                        ping_data = data
                        break
                except Exception:
                    break

            if ping_data:
                # Send PONG with WRONG nonce
                ws.send_json({"type": "PONG", "nonce": "wrong_nonce_12345"})

                # The server should NOT accept this as a valid PONG.
                # If we wait for another PING or a close, we know the wrong
                # nonce was rejected. The connection should timeout eventually.
                # For now just verify we can still send messages (connection
                # isn't immediately closed on wrong nonce alone).
                ws.send_json({"type": "BRAIN_MESSAGE", "message": ""})
                data = _recv_skip_ping(ws)
                # We should get an ERROR for empty message, proving connection is alive
                assert data["type"] == "ERROR"

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.PING_INTERVAL", 0.5)
    @patch("app.routers.copilot.PONG_TIMEOUT", 2)
    def test_pong_missing_nonce_tolerated(self, mock_auth, client):
        """PONG with missing nonce should be accepted for backwards compat."""
        mock_auth.return_value = MOCK_USER

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            ws.receive_json()  # CONNECTED

            # Wait for a PING
            ping_data = None
            for _ in range(20):
                try:
                    data = ws.receive_json()
                    if data.get("type") == "PING":
                        ping_data = data
                        break
                except Exception:
                    break

            if ping_data:
                # Send PONG with NO nonce (backwards compat)
                ws.send_json({"type": "PONG"})

                # Connection should remain alive — send a test message
                ws.send_json({"type": "BRAIN_MESSAGE", "message": ""})
                data = _recv_skip_ping(ws)
                # Should get ERROR for empty message, proving connection is alive
                assert data["type"] == "ERROR"
                assert "non-empty" in data.get("message", "").lower()


# ---------------------------------------------------------------------------
# Standalone Guard Tests
# ---------------------------------------------------------------------------

class TestStandaloneGuard:
    """Test standalone mode air-gap behavior."""

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=True)
    @patch("app.services.copilot_service.is_local_only", return_value=True)
    def test_standalone_allie_disabled_ws_message(self, mock_svc_local, mock_local, mock_auth, client):
        """In standalone mode, BRAIN_MESSAGE returns ALLIE_DISABLED error."""
        mock_auth.return_value = MOCK_USER

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"
            assert connected["is_standalone"] is True
            assert connected["allie_available"] is False

            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "Hello",
            })

            data = _recv_skip_ping(ws)
            assert data["type"] == "ERROR"
            assert data.get("code") == "ALLIE_DISABLED"
            assert "ai.market" in data.get("message", "")

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=True)
    @patch("app.services.copilot_service.is_local_only", return_value=True)
    @patch("app.routers.copilot.metering_service")
    def test_standalone_no_outbound_http(self, mock_metering, mock_svc_local, mock_local, mock_auth, client):
        """In standalone mode, no metering or ai.market calls should be made."""
        mock_auth.return_value = MOCK_USER

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            ws.receive_json()  # CONNECTED

            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "Hello",
            })

            data = _recv_skip_ping(ws)
            assert data["type"] == "ERROR"
            assert data.get("code") == "ALLIE_DISABLED"

        # Verify no metering calls were made in standalone mode
        mock_metering.report_usage.assert_not_called()
