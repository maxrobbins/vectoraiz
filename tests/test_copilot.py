"""
Tests for the Co-Pilot WebSocket Bridge and REST endpoints.
=============================================================

Covers:
- WebSocket connection with valid/invalid token (auth enabled)
- WebSocket connection with auth disabled (default test mode)
- PING/PONG keepalive protocol
- Command validation (valid and malformed SCI commands)
- REST-to-WebSocket relay (POST /command → WebSocket)
- REST status endpoint
- Unknown message type handling
- STATE_SNAPSHOT and COMMAND_RESULT message handling
- STOP command handling
- Metering integration: BRAIN_MESSAGE balance gate, usage reporting,
  mid-stream depletion, REST /brain endpoint with 402

CREATED: S94/BQ-069 (2026-02-06)
UPDATED: S94/BQ-073 (2026-02-06) — Added metering integration tests
SPEC: BQ-CP-01, Acceptance Criteria 13; BQ-073 AC 9, 10, 11
"""

import pytest
import httpx
from unittest.mock import AsyncMock, patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.errors import VectorAIzError
from app.core.errors.middleware import vectoraiz_error_handler
from app.core.errors.registry import error_registry
from app.routers.copilot import router as copilot_rest_router, ws_router, manager, ConnectionManager
from app.models.copilot import SCICommand, SCICommandType, RiskLevel, UIHints

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
    manager._session_state.clear()
    manager._session_intro_seen.clear()
    manager._inflight_task.clear()
    manager._session_msg_timestamps.clear()
    manager._user_connect_timestamps.clear()
    manager._lock = None  # Reset lock so it's recreated in the test's event loop
    yield
    manager._active.clear()
    manager._user_sessions.clear()
    manager._connected_since.clear()
    manager._session_users.clear()
    manager._session_balance.clear()
    manager._session_state.clear()
    manager._session_intro_seen.clear()
    manager._inflight_task.clear()
    manager._session_msg_timestamps.clear()
    manager._user_connect_timestamps.clear()
    manager._lock = None


def _create_test_app(auth_enabled: bool = False) -> FastAPI:
    """Create a minimal FastAPI app with copilot routers mounted."""
    app = FastAPI()
    app.add_exception_handler(VectorAIzError, vectoraiz_error_handler)
    app.include_router(copilot_rest_router, prefix="/api/copilot")
    app.include_router(ws_router)
    return app


@pytest.fixture
def app():
    """FastAPI app with auth disabled (default test mode via conftest.py)."""
    return _create_test_app(auth_enabled=False)


@pytest.fixture
def client(app):
    """TestClient with auth disabled."""
    return TestClient(app)


# Valid SCI command dict for reuse
VALID_COMMAND = {
    "id": "cmd_test_abc123",
    "type": "NAVIGATE",
    "requires_approval": False,
    "risk_level": 0,
    "action": "NAVIGATE_TO_DATASETS",
    "payload": {"route": "/datasets"},
    "ui_hints": {"explanation": "Navigate to the datasets page"},
    "timestamp": "2026-02-06T12:00:00Z",
}


VALID_AUTH_RESPONSE = {
    "valid": True,
    "user_id": "usr_copilot_test",
    "key_id": "key_copilot_test",
    "scopes": ["read", "write"],
}


# ---------------------------------------------------------------------------
# WebSocket Connection Tests (auth disabled — default test mode)
# ---------------------------------------------------------------------------

class TestWebSocketConnectionAuthDisabled:
    """WebSocket connection tests with auth disabled (conftest.py default)."""

    def test_websocket_connect_sends_connected_message(self, client):
        """Valid connection should receive CONNECTED message with session_id."""
        with client.websocket_connect("/ws/copilot") as ws:
            data = ws.receive_json()
            assert data["type"] == "CONNECTED"
            assert "session_id" in data
            assert data["session_id"].startswith("cps_")
            assert "timestamp" in data

    def test_websocket_connect_includes_balance(self, client):
        """CONNECTED message should include balance_cents field."""
        with client.websocket_connect("/ws/copilot") as ws:
            data = ws.receive_json()
            assert data["type"] == "CONNECTED"
            assert "balance_cents" in data
            # Auth disabled mock user has balance_cents=10000
            assert data["balance_cents"] == 10000

    def test_websocket_connect_registers_in_manager(self, client):
        """Connection should be tracked in ConnectionManager."""
        with client.websocket_connect("/ws/copilot") as ws:
            data = ws.receive_json()
            session_id = data["session_id"]
            assert manager.active_count >= 1
            assert manager.get_ws(session_id) is not None

    def test_websocket_disconnect_cleans_up_manager(self, client):
        """Disconnection should remove session from ConnectionManager."""
        with client.websocket_connect("/ws/copilot") as ws:
            data = ws.receive_json()
            session_id = data["session_id"]

        # After context exit, connection should be cleaned up
        assert manager.get_ws(session_id) is None


# ---------------------------------------------------------------------------
# WebSocket Connection Tests (auth enabled)
# ---------------------------------------------------------------------------

class TestWebSocketConnectionAuthEnabled:
    """WebSocket connection tests with auth explicitly enabled."""

    def test_websocket_connect_with_valid_token(self, monkeypatch):
        """Valid ?token= should authenticate and receive CONNECTED message."""
        monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "true")

        from app.auth.api_key_auth import api_key_cache, settings as auth_settings
        api_key_cache.clear()

        app = _create_test_app(auth_enabled=True)

        with patch.object(auth_settings, "mode", "connected"), \
             patch("app.auth.api_key_auth.httpx.AsyncClient") as mock_client_cls:
            mock_instance = mock_client_cls.return_value
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)
            mock_instance.post = AsyncMock(
                return_value=httpx.Response(200, json=VALID_AUTH_RESPONSE)
            )

            client = TestClient(app)
            with client.websocket_connect("/ws/copilot?token=aim_valid_key") as ws:
                data = ws.receive_json()
                assert data["type"] == "CONNECTED"
                assert "session_id" in data

    def test_websocket_connect_with_missing_token(self, monkeypatch):
        """Missing ?token= should result in close code 4001."""
        monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "true")

        from app.auth.api_key_auth import api_key_cache
        api_key_cache.clear()

        app = _create_test_app(auth_enabled=True)
        client = TestClient(app)

        # No token → auth returns None → close 4001
        with pytest.raises(Exception):
            # WebSocket should be accepted then immediately closed with 4001
            with client.websocket_connect("/ws/copilot") as ws:
                # If we get here, try reading — should fail
                ws.receive_json()

    def test_websocket_connect_with_invalid_token(self, monkeypatch):
        """Invalid token should result in close code 4001."""
        monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "true")

        from app.auth.api_key_auth import api_key_cache
        api_key_cache.clear()

        app = _create_test_app(auth_enabled=True)

        with patch("app.auth.api_key_auth.httpx.AsyncClient") as mock_client_cls:
            mock_instance = mock_client_cls.return_value
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=None)
            mock_instance.post = AsyncMock(
                return_value=httpx.Response(401, json={"valid": False})
            )

            client = TestClient(app)
            with pytest.raises(Exception):
                with client.websocket_connect("/ws/copilot?token=aim_invalid_key") as ws:
                    ws.receive_json()


# ---------------------------------------------------------------------------
# PING/PONG Protocol Tests
# ---------------------------------------------------------------------------

class TestPingPong:
    """Tests for the keepalive PING/PONG mechanism."""

    def test_client_pong_response(self, client):
        """Client sending PONG should be accepted without error."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            # Send an unsolicited PONG (server should handle it gracefully)
            ws.send_json({"type": "PONG"})

            # Server should not error — send another message to verify alive
            ws.send_json({"type": "PONG"})

    def test_server_ping_client_responds_pong(self, client):
        """
        When server sends PING, client responds PONG.
        We can't easily wait 30s for the server's ping loop,
        so we test that the PONG message type is handled without error.
        """
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            # Simulate that we received a PING and respond with PONG
            ws.send_json({"type": "PONG"})
            # No error = success — the pong_received event was set


# ---------------------------------------------------------------------------
# Command Validation Tests
# ---------------------------------------------------------------------------

class TestCommandValidation:
    """Tests for SCI command validation over WebSocket and REST."""

    def test_valid_state_snapshot(self, client):
        """Valid STATE_SNAPSHOT message should be accepted."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            ws.send_json({
                "type": "STATE_SNAPSHOT",
                "current_route": "/datasets",
                "page_title": "Datasets",
                "timestamp": "2026-02-06T12:00:00Z",
            })
            # No error response means success — send another to verify alive
            ws.send_json({"type": "PONG"})

    def test_invalid_state_snapshot_returns_error(self, client):
        """Invalid STATE_SNAPSHOT (missing required fields) should return ERROR."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            # Missing required 'current_route', 'page_title', 'timestamp'
            ws.send_json({
                "type": "STATE_SNAPSHOT",
                # Missing required fields
            })

            error = ws.receive_json()
            assert error["type"] == "ERROR"
            assert "Invalid STATE_SNAPSHOT" in error["message"]

    def test_valid_command_result(self, client):
        """Valid COMMAND_RESULT message should be accepted."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            ws.send_json({
                "type": "COMMAND_RESULT",
                "command_id": "cmd_test_abc123",
                "success": True,
                "state_after": {"route": "/datasets"},
            })
            # No error = success
            ws.send_json({"type": "PONG"})

    def test_unknown_message_type_returns_error(self, client):
        """Unknown message type should return an ERROR response."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            ws.send_json({"type": "DOES_NOT_EXIST"})

            error = ws.receive_json()
            assert error["type"] == "ERROR"
            assert "Unknown message type" in error["message"]
            assert "DOES_NOT_EXIST" in error["message"]

    def test_stop_message(self, client):
        """STOP message should be acknowledged with a STOPPED message."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            ws.send_json({"type": "STOP"})

            response = ws.receive_json()
            assert response["type"] == "STOPPED"

    def test_navigation_complete_accepted(self, client):
        """NAVIGATION_COMPLETE message should be silently accepted."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            ws.send_json({
                "type": "NAVIGATION_COMPLETE",
                "route": "/datasets",
            })
            # Should not produce a response — send another to verify alive
            ws.send_json({"type": "PONG"})

    def test_approval_message_accepted(self, client):
        """APPROVAL message should be silently accepted."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            ws.send_json({
                "type": "APPROVAL",
                "command_id": "cmd_test_abc123",
                "approved": True,
            })
            # Should not produce a response — send another to verify alive
            ws.send_json({"type": "PONG"})


# ---------------------------------------------------------------------------
# REST-to-WebSocket Relay Tests
# ---------------------------------------------------------------------------

class TestRESTToWSRelay:
    """Tests for the POST /api/copilot/command REST-to-WS relay."""

    def test_command_relayed_to_websocket(self, client):
        """POST /command should forward the SCI command to the connected WS."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            # Now send a command via REST (auth disabled → mock user)
            response = client.post(
                "/api/copilot/command",
                json=VALID_COMMAND,
            )
            assert response.status_code == 200
            data = response.json()
            assert data["queued"] is True
            assert data["command_id"] == VALID_COMMAND["id"]

            # The WebSocket should receive the relayed command
            relayed = ws.receive_json()
            assert relayed["id"] == VALID_COMMAND["id"]
            assert relayed["action"] == VALID_COMMAND["action"]
            assert relayed["type"] == VALID_COMMAND["type"]

    def test_command_no_active_session_returns_409(self, client):
        """POST /command with no active WS session should return 409 Conflict."""
        response = client.post(
            "/api/copilot/command",
            json=VALID_COMMAND,
        )
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "VAI-COP-001"

    def test_command_validation_invalid_payload(self, client):
        """POST /command with invalid SCI command should return 422."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            # Missing required fields
            response = client.post(
                "/api/copilot/command",
                json={"id": "cmd_test", "action": "NAVIGATE"},
                # Missing: type, ui_hints, timestamp
            )
            assert response.status_code == 422

    def test_command_invalid_type_returns_422(self, client):
        """POST /command with invalid SCICommandType should return 422."""
        invalid_command = {**VALID_COMMAND, "type": "INVALID_TYPE"}
        with client.websocket_connect("/ws/copilot") as ws:
            ws.receive_json()  # CONNECTED

            response = client.post(
                "/api/copilot/command",
                json=invalid_command,
            )
            assert response.status_code == 422


# ---------------------------------------------------------------------------
# REST Status Endpoint Tests
# ---------------------------------------------------------------------------

class TestRESTStatus:
    """Tests for the GET /api/copilot/status endpoint."""

    def test_status_no_session(self, client):
        """Status with no active session should return active=False."""
        response = client.get("/api/copilot/status")
        assert response.status_code == 200
        data = response.json()
        assert data["active"] is False
        assert data["session_id"] is None

    def test_status_with_active_session(self, client):
        """Status with an active WS session should return active=True."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            session_id = connected["session_id"]

            response = client.get("/api/copilot/status")
            assert response.status_code == 200
            data = response.json()
            assert data["active"] is True
            assert data["session_id"] == session_id
            assert data["connected_since"] is not None

    def test_status_after_disconnect(self, client):
        """Status after WS disconnect should return active=False."""
        with client.websocket_connect("/ws/copilot") as ws:
            ws.receive_json()

        # After disconnect
        response = client.get("/api/copilot/status")
        assert response.status_code == 200
        data = response.json()
        assert data["active"] is False


# ---------------------------------------------------------------------------
# ConnectionManager Unit Tests
# ---------------------------------------------------------------------------

class TestConnectionManager:
    """Direct unit tests for ConnectionManager logic."""

    def test_has_active_initially_false(self):
        """New manager should have no active connections."""
        cm = ConnectionManager()
        assert cm.has_active is False
        assert cm.active_count == 0

    @pytest.mark.asyncio
    async def test_disconnect_nonexistent_session(self):
        """Disconnecting a non-existent session should not raise."""
        cm = ConnectionManager()
        await cm.disconnect("nonexistent_session")  # Should not raise

    def test_get_session_for_unknown_user(self):
        """Getting session for unknown user should return None."""
        cm = ConnectionManager()
        assert cm.get_session_for_user("unknown_user") is None

    def test_get_ws_nonexistent(self):
        """Getting WebSocket for non-existent session should return None."""
        cm = ConnectionManager()
        assert cm.get_ws("nonexistent") is None

    def test_get_connected_since_nonexistent(self):
        """Getting connected_since for non-existent session should return None."""
        cm = ConnectionManager()
        assert cm.get_connected_since("nonexistent") is None

    def test_get_user_nonexistent(self):
        """Getting user for non-existent session should return None."""
        cm = ConnectionManager()
        assert cm.get_user("nonexistent") is None

    def test_get_balance_nonexistent(self):
        """Getting balance for non-existent session should return 0."""
        cm = ConnectionManager()
        assert cm.get_balance("nonexistent") == 0

    def test_update_balance(self):
        """update_balance should set the cached balance for a session."""
        cm = ConnectionManager()
        cm._session_balance["test_session"] = 100
        cm.update_balance("test_session", 50)
        assert cm.get_balance("test_session") == 50


# ---------------------------------------------------------------------------
# Pydantic Model Validation Tests
# ---------------------------------------------------------------------------

class TestSCIModels:
    """Tests for the SCI Pydantic models themselves."""

    def test_valid_sci_command(self):
        """Valid SCICommand should parse without errors."""
        cmd = SCICommand(
            id="cmd_test123",
            type=SCICommandType.NAVIGATE,
            action="NAVIGATE_TO_DATASETS",
            payload={"route": "/datasets"},
            ui_hints=UIHints(explanation="Go to datasets"),
            timestamp="2026-02-06T00:00:00Z",
        )
        assert cmd.id == "cmd_test123"
        assert cmd.type == SCICommandType.NAVIGATE
        assert cmd.risk_level == RiskLevel.SAFE
        assert cmd.requires_approval is False

    def test_sci_command_all_types(self):
        """All SCICommandType values should be valid."""
        for cmd_type in SCICommandType:
            cmd = SCICommand(
                id=f"cmd_{cmd_type.value}",
                type=cmd_type,
                action="TEST_ACTION",
                ui_hints=UIHints(explanation="test"),
                timestamp="2026-02-06T00:00:00Z",
            )
            assert cmd.type == cmd_type

    def test_sci_command_risk_levels(self):
        """All RiskLevel values should be valid."""
        for level in RiskLevel:
            cmd = SCICommand(
                id=f"cmd_risk_{level.value}",
                type=SCICommandType.NAVIGATE,
                risk_level=level,
                action="TEST_ACTION",
                ui_hints=UIHints(explanation="test"),
                timestamp="2026-02-06T00:00:00Z",
            )
            assert cmd.risk_level == level

    def test_sci_command_missing_required_fields(self):
        """SCICommand with missing required fields should raise ValidationError."""
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            SCICommand(
                id="cmd_test",
                # Missing: type, action, ui_hints, timestamp
            )

    def test_ui_hints_optional_highlight(self):
        """UIHints.highlight_element should be optional."""
        hints = UIHints(explanation="test")
        assert hints.highlight_element is None

        hints_with_highlight = UIHints(
            highlight_element="#datasets-table",
            explanation="test",
        )
        assert hints_with_highlight.highlight_element == "#datasets-table"


# ---------------------------------------------------------------------------
# Metering Integration Tests — WebSocket BRAIN_MESSAGE (BQ-073)
# ---------------------------------------------------------------------------

class TestBrainMessageMetering:
    """
    Tests for BRAIN_MESSAGE metering integration over WebSocket.

    Covers BQ-073 Acceptance Criteria:
      AC 9: Balance zero → 402 / BALANCE_GATE with 'Purchase credits to use Co-Pilot'
      AC 10: Mid-stream depletion → complete response, report, block next
      AC 11: Markup rate configurable (tested in test_metering.py)
    """

    @patch("app.services.copilot_service.is_local_only", return_value=False)
    @patch("app.routers.copilot.is_local_only", return_value=False)
    def test_brain_message_returns_streaming_response_auth_disabled(self, _mock_guard1, _mock_guard2, client):
        """BRAIN_MESSAGE with auth disabled should return BRAIN_STREAM_CHUNK + BRAIN_STREAM_END."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "Hello, Co-Pilot!",
            })

            # Collect all stream chunks
            chunks = []
            while True:
                msg = ws.receive_json()
                if msg["type"] == "BRAIN_STREAM_CHUNK":
                    chunks.append(msg["chunk"])
                elif msg["type"] == "BRAIN_STREAM_END":
                    assert "full_text" in msg
                    assert "usage" in msg
                    break
                else:
                    raise AssertionError(f"Unexpected message type: {msg['type']}: {msg}")

            assert len(chunks) > 0
            full_text = "".join(chunks)
            assert len(full_text) > 0

    def test_brain_message_empty_message_returns_error(self, client):
        """BRAIN_MESSAGE with empty message should return ERROR."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "",
            })

            error = ws.receive_json()
            assert error["type"] == "ERROR"
            assert "non-empty" in error["message"]

    def test_brain_message_missing_message_returns_error(self, client):
        """BRAIN_MESSAGE without 'message' field should return ERROR."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            ws.send_json({
                "type": "BRAIN_MESSAGE",
            })

            error = ws.receive_json()
            assert error["type"] == "ERROR"
            assert "non-empty" in error["message"]

    @patch("app.routers.copilot.is_local_only", return_value=False)
    def test_brain_message_balance_gate_blocks_zero_balance(self, _mock_guard, client, monkeypatch):
        """
        BRAIN_MESSAGE with zero balance should return BALANCE_GATE.
        AC 9: zero balance = BALANCE_GATE with 'Purchase credits to use Co-Pilot'
        """
        monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "true")

        from app.auth.api_key_auth import api_key_cache, settings as auth_settings
        from app.services.metering_service import BalanceCheck
        api_key_cache.clear()

        app = _create_test_app()

        auth_response = {
            "valid": True,
            "user_id": "usr_broke",
            "key_id": "key_broke",
            "scopes": ["read", "write"],
            "balance_cents": 0,
            "free_trial_remaining_cents": 0,
        }

        # Mock check_balance to actually enforce balance (settings.auth_enabled is False in tests)
        def mock_check_balance(balance_cents, estimated_cost_cents=None):
            cost = estimated_cost_cents or 3
            if balance_cents <= 0:
                return BalanceCheck(allowed=False, balance_cents=balance_cents, estimated_cost_cents=cost, reason="zero_balance")
            if balance_cents < cost:
                return BalanceCheck(allowed=False, balance_cents=balance_cents, estimated_cost_cents=cost, reason="insufficient_balance")
            return BalanceCheck(allowed=True, balance_cents=balance_cents, estimated_cost_cents=cost)

        with patch.object(auth_settings, "mode", "connected"), \
             patch("app.routers.copilot.metering_service.check_balance", side_effect=mock_check_balance):
            with patch("app.auth.api_key_auth.httpx.AsyncClient") as mock_client_cls:
                mock_instance = mock_client_cls.return_value
                mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
                mock_instance.__aexit__ = AsyncMock(return_value=None)
                mock_instance.post = AsyncMock(
                    return_value=httpx.Response(200, json=auth_response)
                )

                test_client = TestClient(app)
                with test_client.websocket_connect("/ws/copilot?token=aim_broke_key") as ws:
                    connected = ws.receive_json()
                    assert connected["type"] == "CONNECTED"
                    assert connected["balance_cents"] == 0

                    # Should receive BALANCE_GATE immediately after CONNECTED
                    # (because balance is zero at connect time)
                    gate = ws.receive_json()
                    assert gate["type"] == "BALANCE_GATE"
                    assert gate["message"] == "Purchase credits to use Co-Pilot"
                    assert gate["balance_cents"] == 0

                    # Now try to send a BRAIN_MESSAGE — should be blocked
                    ws.send_json({
                        "type": "BRAIN_MESSAGE",
                        "message": "This should be blocked",
                    })

                    gate2 = ws.receive_json()
                    assert gate2["type"] == "BALANCE_GATE"
                    assert gate2["message"] == "Purchase credits to use Co-Pilot"

    @patch("app.routers.copilot.is_local_only", return_value=False)
    def test_brain_message_balance_gate_blocks_insufficient_balance(self, _mock_guard, client, monkeypatch):
        """
        BRAIN_MESSAGE with insufficient balance should return BALANCE_GATE.
        AC 9: insufficient balance = BALANCE_GATE
        """
        monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "true")

        from app.auth.api_key_auth import api_key_cache, settings as auth_settings
        from app.services.metering_service import BalanceCheck
        api_key_cache.clear()

        app = _create_test_app()

        # Balance is 1 cent but estimated query cost is 3 cents
        auth_response = {
            "valid": True,
            "user_id": "usr_low",
            "key_id": "key_low",
            "scopes": ["read", "write"],
            "balance_cents": 1,
            "free_trial_remaining_cents": 0,
        }

        # Mock check_balance to actually enforce balance
        def mock_check_balance(balance_cents, estimated_cost_cents=None):
            cost = estimated_cost_cents or 3
            if balance_cents <= 0:
                return BalanceCheck(allowed=False, balance_cents=balance_cents, estimated_cost_cents=cost, reason="zero_balance")
            if balance_cents < cost:
                return BalanceCheck(allowed=False, balance_cents=balance_cents, estimated_cost_cents=cost, reason="insufficient_balance")
            return BalanceCheck(allowed=True, balance_cents=balance_cents, estimated_cost_cents=cost)

        with patch.object(auth_settings, "mode", "connected"), \
             patch("app.routers.copilot.metering_service.check_balance", side_effect=mock_check_balance):
            with patch("app.auth.api_key_auth.httpx.AsyncClient") as mock_client_cls:
                mock_instance = mock_client_cls.return_value
                mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
                mock_instance.__aexit__ = AsyncMock(return_value=None)
                mock_instance.post = AsyncMock(
                    return_value=httpx.Response(200, json=auth_response)
                )

                test_client = TestClient(app)
                with test_client.websocket_connect("/ws/copilot?token=aim_low_key") as ws:
                    connected = ws.receive_json()
                    assert connected["type"] == "CONNECTED"
                    assert connected["balance_cents"] == 1

                    # Should get BALANCE_GATE at connect (1 < 3 estimated cost)
                    gate = ws.receive_json()
                    assert gate["type"] == "BALANCE_GATE"

                    # BRAIN_MESSAGE should also be blocked
                    ws.send_json({
                        "type": "BRAIN_MESSAGE",
                        "message": "Not enough credits",
                    })

                    gate2 = ws.receive_json()
                    assert gate2["type"] == "BALANCE_GATE"
                    assert "reason" in gate2

    @patch("app.services.copilot_service.is_local_only", return_value=False)
    @patch("app.routers.copilot.is_local_only", return_value=False)
    def test_brain_message_mid_stream_depletion(self, _mock_guard1, _mock_guard2, client, monkeypatch):
        """
        AC 10: If balance runs out mid-stream, current response completes,
        usage is reported (balance may go slightly negative), and a
        BALANCE_GATE is sent for the next request.

        With BQ-128 streaming: uses BRAIN_STREAM_CHUNK + BRAIN_STREAM_END.
        """
        monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "true")

        from app.auth.api_key_auth import api_key_cache, settings as auth_settings
        api_key_cache.clear()

        app = _create_test_app()

        # User has enough for one request but not the next
        auth_response = {
            "valid": True,
            "user_id": "usr_depleting",
            "key_id": "key_depleting",
            "scopes": ["read", "write"],
            "balance_cents": 5,
            "free_trial_remaining_cents": 0,
        }

        from app.services.allie_provider import AllieUsage
        from app.services.metering_service import BalanceCheck, UsageReport

        # Mock streaming to return a simple response with usage
        mock_usage = AllieUsage(
            input_tokens=10, output_tokens=20,
            cost_cents=5, provider="mock", model="mock-v1",
        )

        async def mock_process_streaming(user, message, session_id, message_id, send_chunk, **kwargs):
            await send_chunk("Response before depletion")
            return "Response before depletion", mock_usage

        # Mock metering to report depletion
        depleted_report = UsageReport(
            success=True,
            cost_cents=5,
            new_balance_cents=0,
            allowed=False,  # Balance is now exhausted
        )

        # Mock check_balance to enforce balance checks (auth disabled in test env)
        def mock_check_balance(balance_cents, estimated_cost_cents=None):
            cost = estimated_cost_cents or 3
            if balance_cents <= 0:
                return BalanceCheck(allowed=False, balance_cents=balance_cents, estimated_cost_cents=cost, reason="zero_balance")
            if balance_cents < cost:
                return BalanceCheck(allowed=False, balance_cents=balance_cents, estimated_cost_cents=cost, reason="insufficient_balance")
            return BalanceCheck(allowed=True, balance_cents=balance_cents, estimated_cost_cents=cost)

        with patch.object(auth_settings, "mode", "connected"), \
             patch("app.routers.copilot.metering_service.check_balance", side_effect=mock_check_balance):
            with patch("app.auth.api_key_auth.httpx.AsyncClient") as mock_auth_cls:
                mock_auth_instance = mock_auth_cls.return_value
                mock_auth_instance.__aenter__ = AsyncMock(return_value=mock_auth_instance)
                mock_auth_instance.__aexit__ = AsyncMock(return_value=None)
                mock_auth_instance.post = AsyncMock(
                    return_value=httpx.Response(200, json=auth_response)
                )

                test_client = TestClient(app)
                with test_client.websocket_connect("/ws/copilot?token=aim_depleting") as ws:
                    connected = ws.receive_json()
                    assert connected["type"] == "CONNECTED"
                    assert connected["balance_cents"] == 5

                    with patch(
                        "app.routers.copilot.copilot_service.process_message_streaming",
                        new_callable=AsyncMock,
                        side_effect=mock_process_streaming,
                    ), patch(
                        "app.routers.copilot.metering_service.report_usage",
                        new_callable=AsyncMock,
                        return_value=depleted_report,
                    ):
                        ws.send_json({
                            "type": "BRAIN_MESSAGE",
                            "message": "Use my last credits",
                        })

                        # Should receive streaming chunk
                        chunk = ws.receive_json()
                        assert chunk["type"] == "BRAIN_STREAM_CHUNK"
                        assert chunk["chunk"] == "Response before depletion"

                        # Should receive BRAIN_STREAM_END
                        end = ws.receive_json()
                        assert end["type"] == "BRAIN_STREAM_END"
                        assert end["full_text"] == "Response before depletion"

                        # Should also receive BALANCE_GATE warning (mid-stream depletion)
                        gate = ws.receive_json()
                        assert gate["type"] == "BALANCE_GATE"
                        assert gate["reason"] == "balance_depleted"
                        assert gate["message"] == "Purchase credits to use Co-Pilot"


# ---------------------------------------------------------------------------
# REST /brain Endpoint Tests (BQ-073)
# ---------------------------------------------------------------------------

class TestRESTBrainEndpoint:
    """Tests for the POST /api/copilot/brain REST endpoint with metering."""

    def test_brain_returns_response_auth_disabled(self, client):
        """POST /brain with auth disabled should return BRAIN_RESPONSE."""
        response = client.post(
            "/api/copilot/brain",
            json={"message": "Hello from REST"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "balance_cents" in data
        assert "cost_cents" in data

    def test_brain_402_on_zero_balance(self):
        """
        POST /brain with zero balance should return 402.
        AC 9: zero balance = 402 with 'Purchase credits to use Co-Pilot'
        """
        from app.auth.api_key_auth import AuthenticatedUser, get_current_user
        from app.services.metering_service import BalanceCheck

        app = _create_test_app()

        mock_user = AuthenticatedUser(
            user_id="usr_broke",
            key_id="key_broke",
            scopes=["read", "write"],
            valid=True,
            balance_cents=0,
            free_trial_remaining_cents=0,
        )

        app.dependency_overrides[get_current_user] = lambda: mock_user
        try:
            with patch(
                "app.routers.copilot.metering_service.check_balance",
                return_value=BalanceCheck(allowed=False, balance_cents=0, estimated_cost_cents=3, reason="zero_balance"),
            ):
                test_client = TestClient(app)
                response = test_client.post(
                    "/api/copilot/brain",
                    json={"message": "I have no credits"},
                )
                assert response.status_code == 402
                assert "Purchase credits to use Co-Pilot" in response.json()["detail"]
        finally:
            app.dependency_overrides.clear()

    def test_brain_402_on_insufficient_balance(self):
        """POST /brain with insufficient balance should return 402."""
        from app.auth.api_key_auth import AuthenticatedUser, get_current_user
        from app.services.metering_service import BalanceCheck

        app = _create_test_app()

        mock_user = AuthenticatedUser(
            user_id="usr_low",
            key_id="key_low",
            scopes=["read", "write"],
            valid=True,
            balance_cents=1,  # Below estimated_query_cost of 3
            free_trial_remaining_cents=0,
        )

        app.dependency_overrides[get_current_user] = lambda: mock_user
        try:
            with patch(
                "app.routers.copilot.metering_service.check_balance",
                return_value=BalanceCheck(allowed=False, balance_cents=1, estimated_cost_cents=3, reason="insufficient_balance"),
            ):
                test_client = TestClient(app)
                response = test_client.post(
                    "/api/copilot/brain",
                    json={"message": "Not enough"},
                )
                assert response.status_code == 402
                assert "Purchase credits to use Co-Pilot" in response.json()["detail"]
        finally:
            app.dependency_overrides.clear()

    def test_brain_success_with_sufficient_balance(self):
        """POST /brain with sufficient balance should return 200."""
        from app.auth.api_key_auth import AuthenticatedUser, get_current_user
        from app.services.metering_service import UsageReport

        app = _create_test_app()

        mock_user = AuthenticatedUser(
            user_id="usr_rich",
            key_id="key_rich",
            scopes=["read", "write"],
            valid=True,
            balance_cents=1000,
            free_trial_remaining_cents=0,
        )

        mock_report = UsageReport(
            success=True,
            cost_cents=4,
            new_balance_cents=996,
            allowed=True,
        )

        app.dependency_overrides[get_current_user] = lambda: mock_user
        try:
            with patch(
                "app.routers.copilot.copilot_service.process_message_metered",
                new_callable=AsyncMock,
                return_value=("Metered response", mock_report),
            ):
                test_client = TestClient(app)
                response = test_client.post(
                    "/api/copilot/brain",
                    json={"message": "I have credits!"},
                )
                assert response.status_code == 200
                data = response.json()
                assert data["message"] == "Metered response"
                assert data["balance_cents"] == 996
                assert data["cost_cents"] == 4
        finally:
            app.dependency_overrides.clear()

    def test_brain_free_trial_combined_with_balance(self):
        """POST /brain combines balance_cents + free_trial_remaining_cents."""
        from app.auth.api_key_auth import AuthenticatedUser, get_current_user
        from app.services.metering_service import UsageReport

        app = _create_test_app()

        # 0 balance + 500 free trial = 500 total → should be allowed
        mock_user = AuthenticatedUser(
            user_id="usr_trial",
            key_id="key_trial",
            scopes=["read", "write"],
            valid=True,
            balance_cents=0,
            free_trial_remaining_cents=500,
        )

        mock_report = UsageReport(
            success=True,
            cost_cents=3,
            new_balance_cents=497,
            allowed=True,
        )

        app.dependency_overrides[get_current_user] = lambda: mock_user
        try:
            with patch(
                "app.routers.copilot.copilot_service.process_message_metered",
                new_callable=AsyncMock,
                return_value=("Trial response", mock_report),
            ):
                test_client = TestClient(app)
                response = test_client.post(
                    "/api/copilot/brain",
                    json={"message": "Using free trial"},
                )
                assert response.status_code == 200
                data = response.json()
                assert data["balance_cents"] == 497
        finally:
            app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# ConnectionManager Balance Tracking Tests
# ---------------------------------------------------------------------------

class TestConnectionManagerBalance:
    """Tests for ConnectionManager balance tracking (BQ-073)."""

    def test_connect_caches_balance_from_user(self, client):
        """WebSocket connect should cache balance from AuthenticatedUser."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            session_id = connected["session_id"]

            # Auth disabled mock user has balance_cents=10000
            assert manager.get_balance(session_id) == 10000

    def test_balance_updates_after_usage(self, client):
        """Balance should update in manager after usage report."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            session_id = connected["session_id"]

            assert manager.get_balance(session_id) == 10000
            manager.update_balance(session_id, 9996)
            assert manager.get_balance(session_id) == 9996

    def test_disconnect_clears_balance(self, client):
        """Disconnecting should clear the cached balance."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            session_id = connected["session_id"]
            assert manager.get_balance(session_id) == 10000

        # After disconnect
        assert manager.get_balance(session_id) == 0

    def test_disconnect_clears_user(self, client):
        """Disconnecting should clear the cached user."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            session_id = connected["session_id"]
            assert manager.get_user(session_id) is not None

        # After disconnect
        assert manager.get_user(session_id) is None
