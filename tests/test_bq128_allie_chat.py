"""
Tests for BQ-128 Phase 1 — Allie Chat + Streaming Foundation
=============================================================

Covers:
- Task 1.1: WebSocket streaming protocol (BRAIN_STREAM_CHUNK, BRAIN_STREAM_END)
- Task 1.2: Schema changes (MessageKind, user_id, usage fields, client_message_id)
- Task 1.3: Standalone guard (AllieDisabledError)
- Task 1.4: Frontend components (tested via build)
- Task 1.5: REST session endpoints (user_id scoping, 404 on cross-user)
- Task 1.6: CONNECTED message includes allie_available, is_standalone, rate_limit

Uses MockAllieProvider — zero real API calls.

CREATED: BQ-128 Phase 1 (2026-02-14)
"""

import asyncio
import json
import os
import tempfile

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import SQLModel, Session as SQLModelSession, create_engine

from app.core.errors import VectorAIzError
from app.core.errors.middleware import vectoraiz_error_handler
from app.core.errors.registry import error_registry
from app.routers.copilot import (
    router as copilot_rest_router,
    ws_router,
    manager,
)
from app.models.state import (
    Message,
    MessageKind,
    MessageRole,
    Session as ChatSession,
)
from app.services.allie_provider import (
    AiMarketAllieProvider,
    AllieDisabledError,
    InsufficientBalanceError,
    MockAllieProvider,
    RateLimitExceededError,
    get_allie_provider,
    reset_provider,
)
from app.services.copilot_service import CoPilotService
from app.auth.api_key_auth import AuthenticatedUser

# Ensure error registry is loaded for tests
if len(error_registry) == 0:
    error_registry.load()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clean_manager():
    """Ensure ConnectionManager is clean before/after each test."""
    manager._active.clear()
    manager._user_sessions.clear()
    manager._connected_since.clear()
    manager._session_users.clear()
    manager._session_balance.clear()
    manager._inflight_task.clear()
    manager._session_msg_timestamps.clear()
    manager._user_connect_timestamps.clear()
    yield
    manager._active.clear()
    manager._user_sessions.clear()
    manager._connected_since.clear()
    manager._session_users.clear()
    manager._session_balance.clear()
    manager._inflight_task.clear()
    manager._session_msg_timestamps.clear()
    manager._user_connect_timestamps.clear()


@pytest.fixture(autouse=True)
def _reset_allie_provider():
    """Reset the Allie provider singleton between tests."""
    reset_provider()
    yield
    reset_provider()


@pytest.fixture
def legacy_db():
    """Create a temporary SQLite legacy DB with tables."""
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "test_legacy.db")
    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    with SQLModelSession(engine) as session:
        yield session


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


MOCK_USER = AuthenticatedUser(
    user_id="usr_test_bq128",
    key_id="key_test",
    scopes=["read", "write"],
    is_valid=True,
    balance_cents=10000,
    free_trial_remaining_cents=0,
)

# Auth-disabled mock user (used when VECTORAIZ_AUTH_ENABLED=false)
AUTH_DISABLED_USER_ID = "mock_user_auth_disabled"


# ---------------------------------------------------------------------------
# Task 1.2: Schema Tests
# ---------------------------------------------------------------------------

class TestSchemaChanges:
    """Test BQ-128 schema additions to Session and Message models."""

    def test_session_has_user_id(self, legacy_db):
        """Session model has user_id field."""
        session = ChatSession(user_id="usr_123", title="Test")
        legacy_db.add(session)
        legacy_db.commit()
        legacy_db.refresh(session)
        assert session.user_id == "usr_123"

    def test_message_has_kind_default(self, legacy_db):
        """Message kind defaults to 'chat'."""
        session = ChatSession(user_id="usr_123")
        legacy_db.add(session)
        legacy_db.commit()
        legacy_db.refresh(session)

        msg = Message(
            session_id=session.id,
            role=MessageRole.USER,
            content="Hello",
        )
        legacy_db.add(msg)
        legacy_db.commit()
        legacy_db.refresh(msg)
        assert msg.kind == MessageKind.CHAT

    def test_message_kind_enum(self):
        """MessageKind enum has expected values."""
        assert MessageKind.CHAT == "chat"
        assert MessageKind.NUDGE == "nudge"
        assert MessageKind.SYSTEM == "system"

    def test_message_usage_fields(self, legacy_db):
        """Message usage fields are nullable and persist correctly."""
        session = ChatSession(user_id="usr_123")
        legacy_db.add(session)
        legacy_db.commit()
        legacy_db.refresh(session)

        msg = Message(
            session_id=session.id,
            role=MessageRole.ASSISTANT,
            content="Response",
            input_tokens=100,
            output_tokens=50,
            cost_cents=2,
            provider="mock",
            model="mock-allie-v1",
        )
        legacy_db.add(msg)
        legacy_db.commit()
        legacy_db.refresh(msg)
        assert msg.input_tokens == 100
        assert msg.output_tokens == 50
        assert msg.cost_cents == 2
        assert msg.provider == "mock"
        assert msg.model == "mock-allie-v1"

    def test_message_client_message_id(self, legacy_db):
        """Message client_message_id stores idempotency key."""
        session = ChatSession(user_id="usr_123")
        legacy_db.add(session)
        legacy_db.commit()
        legacy_db.refresh(session)

        msg = Message(
            session_id=session.id,
            role=MessageRole.USER,
            content="Hello",
            client_message_id="cli_abc123",
        )
        legacy_db.add(msg)
        legacy_db.commit()
        legacy_db.refresh(msg)
        assert msg.client_message_id == "cli_abc123"

    def test_existing_messages_backward_compat(self, legacy_db):
        """Messages without new fields still load correctly."""
        session = ChatSession(user_id="usr_123")
        legacy_db.add(session)
        legacy_db.commit()

        # Create a minimal message (simulating old schema)
        msg = Message(
            session_id=session.id,
            role=MessageRole.USER,
            content="Old message",
        )
        legacy_db.add(msg)
        legacy_db.commit()
        legacy_db.refresh(msg)

        # New fields default correctly
        assert msg.kind == MessageKind.CHAT
        assert msg.client_message_id is None
        assert msg.input_tokens is None
        assert msg.output_tokens is None
        assert msg.cost_cents is None
        assert msg.provider is None
        assert msg.model is None


# ---------------------------------------------------------------------------
# Task 1.3: Standalone Guard Tests
# ---------------------------------------------------------------------------

class TestStandaloneGuard:
    """Test Allie is disabled in standalone mode."""

    @patch("app.services.copilot_service.is_local_only", return_value=True)
    def test_streaming_raises_in_standalone(self, mock_local):
        """process_message_streaming raises AllieDisabledError in standalone."""
        service = CoPilotService()

        async def _run():
            with pytest.raises(AllieDisabledError):
                await service.process_message_streaming(
                    user=MOCK_USER,
                    message="Hello",
                    session_id="test_sid",
                    message_id="test_mid",
                    send_chunk=AsyncMock(),
                )

        asyncio.get_event_loop().run_until_complete(_run())

    def test_allie_disabled_error_message(self):
        """AllieDisabledError has the expected message."""
        err = AllieDisabledError("Test message")
        assert str(err) == "Test message"


# ---------------------------------------------------------------------------
# Task 1.1: MockAllieProvider + Streaming Tests
# ---------------------------------------------------------------------------

class TestMockAllieProvider:
    """Test the MockAllieProvider streaming."""

    @pytest.mark.asyncio
    async def test_mock_provider_streams_chunks(self):
        """MockAllieProvider yields multiple chunks + final usage."""
        provider = MockAllieProvider()
        chunks = []
        async for chunk in provider.stream("Hello"):
            chunks.append(chunk)

        assert len(chunks) > 1
        # Last chunk should be the done marker with usage
        final = chunks[-1]
        assert final.done is True
        assert final.usage is not None
        assert final.usage.provider == "mock"
        assert final.usage.model == "mock-allie-v1"
        assert final.usage.input_tokens > 0
        assert final.usage.output_tokens > 0

    @pytest.mark.asyncio
    async def test_mock_provider_full_text(self):
        """Mock provider produces non-empty full text."""
        provider = MockAllieProvider()
        full_text = ""
        async for chunk in provider.stream("hello"):
            if chunk.text:
                full_text += chunk.text

        assert len(full_text) > 10
        assert "Allie" in full_text or "Hello" in full_text

    @pytest.mark.asyncio
    async def test_mock_provider_help_response(self):
        """Mock provider returns help text for 'help' queries."""
        provider = MockAllieProvider()
        full_text = ""
        async for chunk in provider.stream("what can you help with"):
            if chunk.text:
                full_text += chunk.text

        assert "Data exploration" in full_text or "data exploration" in full_text.lower()


class TestCoPilotServiceStreaming:
    """Test the CoPilotService.process_message_streaming method."""

    @pytest.mark.asyncio
    @patch("app.services.copilot_service.is_local_only", return_value=False)
    async def test_streaming_calls_send_chunk(self, mock_local):
        """Streaming sends chunks to the callback."""
        service = CoPilotService()
        chunks_received = []

        async def capture_chunk(text: str):
            chunks_received.append(text)

        full_text, usage = await service.process_message_streaming(
            user=MOCK_USER,
            message="Hello",
            session_id="test_sid",
            message_id="test_mid",
            send_chunk=capture_chunk,
        )

        assert len(chunks_received) > 0
        assert len(full_text) > 0
        assert usage is not None
        assert usage.input_tokens > 0
        assert usage.output_tokens > 0

    @pytest.mark.asyncio
    @patch("app.services.copilot_service.is_local_only", return_value=False)
    async def test_streaming_cancellation(self, mock_local):
        """STOP cancellation propagates CancelledError."""
        service = CoPilotService()

        call_count = 0

        async def slow_chunk(text: str):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await service.process_message_streaming(
                user=MOCK_USER,
                message="Hello",
                session_id="test_sid",
                message_id="test_mid",
                send_chunk=slow_chunk,
            )

        # At least one chunk was sent before cancel
        assert call_count >= 2


# ---------------------------------------------------------------------------
# Task 1.1 + 1.6: WebSocket Tests
# ---------------------------------------------------------------------------

class TestWebSocketStreaming:
    """Test WebSocket streaming protocol."""

    @patch("app.routers.copilot.get_current_user_ws")
    def test_connected_message_has_allie_flags(self, mock_auth, client):
        """CONNECTED message includes allie_available and is_standalone."""
        mock_auth.return_value = MOCK_USER

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            data = ws.receive_json()
            assert data["type"] == "CONNECTED"
            assert "allie_available" in data
            assert "is_standalone" in data
            assert "rate_limit" in data
            assert "session_id" in data

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=False)
    @patch("app.services.copilot_service.is_local_only", return_value=False)
    @patch("app.routers.copilot.metering_service")
    def test_brain_message_streams_chunks(self, mock_metering, mock_svc_local, mock_local, mock_auth, client):
        """BRAIN_MESSAGE → BRAIN_STREAM_CHUNK... → BRAIN_STREAM_END."""
        mock_auth.return_value = MOCK_USER
        mock_metering.check_balance.return_value = MagicMock(allowed=True)
        mock_metering.report_usage = AsyncMock(
            return_value=MagicMock(
                new_balance_cents=9900, cost_cents=1, allowed=True
            )
        )

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            # Receive CONNECTED
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            # Send BRAIN_MESSAGE
            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "Hello",
                "message_id": "msg_test_001",
            })

            # Collect responses until BRAIN_STREAM_END
            chunks = []
            end_msg = None
            unexpected = []
            for _ in range(100):
                data = ws.receive_json()
                if data["type"] == "BRAIN_STREAM_CHUNK":
                    chunks.append(data)
                elif data["type"] == "BRAIN_STREAM_END":
                    end_msg = data
                    break
                elif data["type"] == "PING":
                    ws.send_json({"type": "PONG", "nonce": data.get("nonce")})
                    continue
                else:
                    unexpected.append(data)
                    break

            assert len(chunks) > 0, f"Expected chunks but got unexpected={unexpected}"
            assert end_msg is not None, "Expected BRAIN_STREAM_END"
            assert end_msg["message_id"] == "msg_test_001"
            assert "full_text" in end_msg
            assert len(end_msg["full_text"]) > 0
            assert "usage" in end_msg
            assert end_msg["usage"]["provider"] == "mock"

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=True)
    @patch("app.services.copilot_service.is_local_only", return_value=True)
    def test_standalone_mode_allie_disabled(self, mock_svc_local, mock_local, mock_auth, client):
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

            # Should receive an ERROR with ALLIE_DISABLED code
            data = ws.receive_json()
            while data.get("type") == "PING":
                ws.send_json({"type": "PONG", "nonce": data.get("nonce")})
                data = ws.receive_json()

            assert data["type"] == "ERROR"
            assert "ALLIE_DISABLED" in data.get("code", "") or "ai.market" in data.get("message", "")

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=False)
    @patch("app.services.copilot_service.is_local_only", return_value=False)
    @patch("app.routers.copilot.metering_service")
    def test_stop_sends_stopped_when_no_inflight(self, mock_metering, mock_svc_local, mock_local, mock_auth, client):
        """STOP when no task inflight sends STOPPED message."""
        mock_auth.return_value = MOCK_USER
        mock_metering.check_balance.return_value = MagicMock(allowed=True)

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            ws.receive_json()  # CONNECTED

            # Send STOP without any BRAIN_MESSAGE
            ws.send_json({"type": "STOP"})

            data = ws.receive_json()
            while data.get("type") == "PING":
                ws.send_json({"type": "PONG", "nonce": data.get("nonce")})
                data = ws.receive_json()

            assert data["type"] == "STOPPED"

    @patch("app.routers.copilot.get_current_user_ws")
    @patch("app.routers.copilot.is_local_only", return_value=False)
    def test_connected_mode_rate_limit_info(self, mock_local, mock_auth, client):
        """CONNECTED message includes rate_limit info in connected mode."""
        mock_auth.return_value = MOCK_USER

        with client.websocket_connect("/ws/copilot?token=test") as ws:
            data = ws.receive_json()
            assert data["type"] == "CONNECTED"
            assert data["allie_available"] is True
            assert "rate_limit" in data
            assert "remaining_tokens_today" in data["rate_limit"]
            assert "daily_limit" in data["rate_limit"]


# ---------------------------------------------------------------------------
# Task 1.5: REST Session Endpoint Tests
# ---------------------------------------------------------------------------

class TestSessionEndpoints:
    """Test REST session history endpoints.

    Uses auth-disabled mock user (VECTORAIZ_AUTH_ENABLED=false from conftest).
    Uses file-based SQLite for legacy DB via dependency override.
    """

    def _make_app_client(self, engine):
        """Create app + client with dependency override for legacy DB."""
        from app.core.database import get_legacy_session as _get_legacy_session

        app = FastAPI()
        app.add_exception_handler(VectorAIzError, vectoraiz_error_handler)
        app.include_router(copilot_rest_router, prefix="/api/copilot")
        app.include_router(ws_router)

        def _override_db():
            with SQLModelSession(engine) as session:
                yield session

        app.dependency_overrides[_get_legacy_session] = _override_db
        return TestClient(app)

    def _fresh_engine(self):
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        engine = create_engine(f"sqlite:///{tmp.name}", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(engine)
        return engine

    def test_create_session(self):
        """POST /api/copilot/sessions creates a session with user_id."""
        engine = self._fresh_engine()
        client = self._make_app_client(engine)

        response = client.post(
            "/api/copilot/sessions",
            json={"title": "My Chat"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["title"] == "My Chat"
        # Auth disabled returns mock_user_auth_disabled
        assert data["user_id"] == AUTH_DISABLED_USER_ID

    def test_list_sessions_user_scoped(self):
        """GET /api/copilot/sessions only returns sessions for the user."""
        engine = self._fresh_engine()

        with SQLModelSession(engine) as db:
            s1 = ChatSession(user_id=AUTH_DISABLED_USER_ID, title="Mine")
            s2 = ChatSession(user_id="usr_other", title="Theirs")
            db.add_all([s1, s2])
            db.commit()

        client = self._make_app_client(engine)
        response = client.get("/api/copilot/sessions")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["title"] == "Mine"

    def test_get_current_session_messages_empty(self):
        """GET /sessions/current/messages returns [] when no session exists."""
        engine = self._fresh_engine()
        client = self._make_app_client(engine)

        response = client.get("/api/copilot/sessions/current/messages")
        assert response.status_code == 200
        assert response.json() == []

    def test_get_session_messages_cross_user_404(self):
        """GET /sessions/{id}/messages returns 404 for another user's session."""
        engine = self._fresh_engine()

        with SQLModelSession(engine) as db:
            other_session = ChatSession(user_id="usr_other", title="Secret")
            db.add(other_session)
            db.commit()
            db.refresh(other_session)
            other_id = str(other_session.id)

        client = self._make_app_client(engine)
        response = client.get(f"/api/copilot/sessions/{other_id}/messages")
        # Returns 404 (not 403) to avoid enumeration
        assert response.status_code == 404

    def test_get_current_session_messages_with_data(self):
        """GET /sessions/current/messages returns messages for active session."""
        engine = self._fresh_engine()

        with SQLModelSession(engine) as db:
            session = ChatSession(user_id=AUTH_DISABLED_USER_ID, title="Test Chat")
            db.add(session)
            db.commit()
            db.refresh(session)

            msg1 = Message(
                session_id=session.id,
                role=MessageRole.USER,
                content="Hello",
            )
            msg2 = Message(
                session_id=session.id,
                role=MessageRole.ASSISTANT,
                content="Hi there!",
                input_tokens=10,
                output_tokens=5,
                provider="mock",
                model="mock-allie-v1",
            )
            db.add_all([msg1, msg2])
            db.commit()

        client = self._make_app_client(engine)
        response = client.get("/api/copilot/sessions/current/messages")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["role"] == "user"
        assert data[0]["content"] == "Hello"
        assert data[1]["role"] == "assistant"
        assert data[1]["content"] == "Hi there!"
        assert data[1]["provider"] == "mock"


# ---------------------------------------------------------------------------
# Task 1.3: Local-only guard unit tests
# ---------------------------------------------------------------------------

class TestLocalOnlyGuard:
    """Test the local_only_guard module."""

    @patch("app.core.local_only_guard.settings")
    def test_is_local_only_standalone(self, mock_settings):
        """is_local_only returns True in standalone mode."""
        mock_settings.mode = "standalone"
        from app.core.local_only_guard import is_local_only
        assert is_local_only() is True

    @patch("app.core.local_only_guard.settings")
    def test_is_local_only_connected(self, mock_settings):
        """is_local_only returns False in connected mode."""
        mock_settings.mode = "connected"
        from app.core.local_only_guard import is_local_only
        assert is_local_only() is False


# ---------------------------------------------------------------------------
# BQ-129: AiMarketAllieProvider Tests
# ---------------------------------------------------------------------------

def _sse_lines(*events):
    """Build raw SSE text from (event_type, data_dict) tuples."""
    lines = []
    for event_type, data in events:
        lines.append(f"event: {event_type}")
        lines.append(f"data: {json.dumps(data)}")
        lines.append("")  # blank line = end of event
    return "\n".join(lines)


class _FakeSSEResponse:
    """Minimal fake for httpx streaming response."""

    def __init__(self, status_code: int, sse_text: str = ""):
        self.status_code = status_code
        self._sse_text = sse_text

    async def aiter_lines(self):
        for line in self._sse_text.split("\n"):
            yield line

    async def aiter_text(self):
        yield self._sse_text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class _FakeAsyncClient:
    """Minimal fake for httpx.AsyncClient that returns a FakeSSEResponse."""

    def __init__(self, response: _FakeSSEResponse):
        self._response = response
        self.last_request = None

    def stream(self, method, url, **kwargs):
        self.last_request = {"method": method, "url": url, **kwargs}
        return self._response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class TestAiMarketAllieProvider:
    """Test the AiMarketAllieProvider SSE client."""

    def _make_provider(self):
        """Create an AiMarketAllieProvider with test config."""
        provider = AiMarketAllieProvider.__new__(AiMarketAllieProvider)
        provider.base_url = "https://test.ai.market"
        provider.api_key = "aim_test_key"
        provider.timeout = None
        return provider

    @pytest.mark.asyncio
    async def test_stream_parses_sse_events(self):
        """Provider parses start, delta, done SSE events correctly."""
        sse = _sse_lines(
            ("start", {"request_id": "req_123", "model": "claude-sonnet-4-5-20250929"}),
            ("delta", {"text": "Hello"}),
            ("delta", {"text": " world"}),
            ("done", {
                "request_id": "req_123",
                "usage": {"input_tokens": 10, "output_tokens": 5},
                "cost_cents": 2,
                "remaining_balance_cents": 9998,
            }),
        )
        fake_resp = _FakeSSEResponse(200, sse)
        fake_client = _FakeAsyncClient(fake_resp)
        provider = self._make_provider()

        chunks = []
        with patch("app.services.allie_provider.httpx.AsyncClient", return_value=fake_client):
            async for chunk in provider.stream("Hi"):
                chunks.append(chunk)

        # Should get 2 delta chunks + 1 done chunk
        assert len(chunks) == 3
        assert chunks[0].text == "Hello"
        assert chunks[1].text == " world"
        assert chunks[2].done is True
        assert chunks[2].usage is not None
        assert chunks[2].usage.input_tokens == 10
        assert chunks[2].usage.output_tokens == 5
        assert chunks[2].usage.cost_cents == 2
        assert chunks[2].usage.provider == "aimarket"
        assert chunks[2].usage.model == "claude-sonnet-4-5-20250929"

    @pytest.mark.asyncio
    async def test_stream_sends_correct_request(self):
        """Provider sends correct URL, headers, and body."""
        sse = _sse_lines(
            ("start", {"request_id": "req_123", "model": "test"}),
            ("done", {"usage": {"input_tokens": 1, "output_tokens": 1}, "cost_cents": 1}),
        )
        fake_resp = _FakeSSEResponse(200, sse)
        fake_client = _FakeAsyncClient(fake_resp)
        provider = self._make_provider()

        with patch("app.services.allie_provider.httpx.AsyncClient", return_value=fake_client):
            async for _ in provider.stream("test msg", context="system prompt"):
                pass

        req = fake_client.last_request
        assert req["url"] == "https://test.ai.market/api/v1/allie/chat"
        assert req["headers"]["X-API-Key"] == "aim_test_key"
        assert req["headers"]["Accept"] == "text/event-stream"
        # Messages should include context + user message
        messages = req["json"]["messages"]
        assert len(messages) == 2
        assert messages[0] == {"role": "user", "content": "system prompt"}
        assert messages[1] == {"role": "user", "content": "test msg"}

    @pytest.mark.asyncio
    async def test_stream_without_context(self):
        """Provider sends single message when no context provided."""
        sse = _sse_lines(
            ("start", {"request_id": "r", "model": "m"}),
            ("done", {"usage": {"input_tokens": 1, "output_tokens": 1}, "cost_cents": 0}),
        )
        fake_resp = _FakeSSEResponse(200, sse)
        fake_client = _FakeAsyncClient(fake_resp)
        provider = self._make_provider()

        with patch("app.services.allie_provider.httpx.AsyncClient", return_value=fake_client):
            async for _ in provider.stream("hello"):
                pass

        messages = fake_client.last_request["json"]["messages"]
        assert len(messages) == 1
        assert messages[0] == {"role": "user", "content": "hello"}

    @pytest.mark.asyncio
    async def test_error_401(self):
        """401 raises AllieDisabledError."""
        fake_resp = _FakeSSEResponse(401)
        fake_client = _FakeAsyncClient(fake_resp)
        provider = self._make_provider()

        with patch("app.services.allie_provider.httpx.AsyncClient", return_value=fake_client):
            with pytest.raises(AllieDisabledError, match="authentication failed"):
                async for _ in provider.stream("test"):
                    pass

    @pytest.mark.asyncio
    async def test_error_402(self):
        """402 raises InsufficientBalanceError."""
        fake_resp = _FakeSSEResponse(402)
        fake_client = _FakeAsyncClient(fake_resp)
        provider = self._make_provider()

        with patch("app.services.allie_provider.httpx.AsyncClient", return_value=fake_client):
            with pytest.raises(InsufficientBalanceError, match="Insufficient balance"):
                async for _ in provider.stream("test"):
                    pass

    @pytest.mark.asyncio
    async def test_error_403(self):
        """403 raises AllieDisabledError."""
        fake_resp = _FakeSSEResponse(403)
        fake_client = _FakeAsyncClient(fake_resp)
        provider = self._make_provider()

        with patch("app.services.allie_provider.httpx.AsyncClient", return_value=fake_client):
            with pytest.raises(AllieDisabledError, match="scope"):
                async for _ in provider.stream("test"):
                    pass

    @pytest.mark.asyncio
    async def test_error_429(self):
        """429 raises RateLimitExceededError."""
        fake_resp = _FakeSSEResponse(429)
        fake_client = _FakeAsyncClient(fake_resp)
        provider = self._make_provider()

        with patch("app.services.allie_provider.httpx.AsyncClient", return_value=fake_client):
            with pytest.raises(RateLimitExceededError):
                async for _ in provider.stream("test"):
                    pass

    @pytest.mark.asyncio
    async def test_error_500(self):
        """500 raises AllieDisabledError with status code in message."""
        fake_resp = _FakeSSEResponse(500, "Internal Server Error")
        fake_client = _FakeAsyncClient(fake_resp)
        provider = self._make_provider()

        with patch("app.services.allie_provider.httpx.AsyncClient", return_value=fake_client):
            with pytest.raises(AllieDisabledError, match="500"):
                async for _ in provider.stream("test"):
                    pass

    @pytest.mark.asyncio
    async def test_sse_error_event(self):
        """SSE error event raises AllieDisabledError."""
        sse = _sse_lines(
            ("start", {"request_id": "r", "model": "m"}),
            ("error", {"error": "overloaded", "message": "Server busy", "retryable": False}),
        )
        fake_resp = _FakeSSEResponse(200, sse)
        fake_client = _FakeAsyncClient(fake_resp)
        provider = self._make_provider()

        with patch("app.services.allie_provider.httpx.AsyncClient", return_value=fake_client):
            with pytest.raises(AllieDisabledError, match="Server busy"):
                async for _ in provider.stream("test"):
                    pass

    @pytest.mark.asyncio
    async def test_sse_retryable_error_event(self):
        """SSE retryable error includes 'retryable' in message."""
        sse = _sse_lines(
            ("error", {"error": "timeout", "message": "Upstream timeout", "retryable": True}),
        )
        fake_resp = _FakeSSEResponse(200, sse)
        fake_client = _FakeAsyncClient(fake_resp)
        provider = self._make_provider()

        with patch("app.services.allie_provider.httpx.AsyncClient", return_value=fake_client):
            with pytest.raises(AllieDisabledError, match="retryable"):
                async for _ in provider.stream("test"):
                    pass

    def test_init_requires_api_key(self):
        """AiMarketAllieProvider raises ValueError without internal_api_key."""
        with patch("app.services.allie_provider.AiMarketAllieProvider.__init__.__module__", create=True):
            with patch("app.config.settings") as mock_settings:
                mock_settings.ai_market_url = "https://test.example.com"
                mock_settings.internal_api_key = None
                with pytest.raises(ValueError, match="VECTORAIZ_INTERNAL_API_KEY"):
                    AiMarketAllieProvider()


class TestGetAllieProviderFactory:
    """Test the get_allie_provider() factory function."""

    def test_mock_provider_default(self):
        """Default provider is MockAllieProvider."""
        reset_provider()
        with patch.dict(os.environ, {"VECTORAIZ_ALLIE_PROVIDER": "mock"}):
            provider = get_allie_provider()
        assert isinstance(provider, MockAllieProvider)

    def test_aimarket_provider(self):
        """VECTORAIZ_ALLIE_PROVIDER=aimarket returns AiMarketAllieProvider."""
        reset_provider()
        with patch.dict(os.environ, {"VECTORAIZ_ALLIE_PROVIDER": "aimarket"}):
            with patch("app.config.settings") as mock_settings:
                mock_settings.ai_market_url = "https://test.example.com"
                mock_settings.internal_api_key = "aim_test_key"
                provider = get_allie_provider()
        assert isinstance(provider, AiMarketAllieProvider)
        assert provider.base_url == "https://test.example.com"

    def test_unknown_provider_falls_back_to_mock(self):
        """Unknown provider type falls back to MockAllieProvider."""
        reset_provider()
        with patch.dict(os.environ, {"VECTORAIZ_ALLIE_PROVIDER": "unknown_thing"}):
            provider = get_allie_provider()
        assert isinstance(provider, MockAllieProvider)
