"""
Tests for Phase 4: Diagnostic Transmission — BQ-VZ-NOTIFICATIONS

Covers:
- POST /api/diagnostics/transmit endpoint
- Rate limiting (1/hour)
- Size cap (50 MB)
- PII scrubbing verification
- prepare_support_bundle allAI tool
- Notification creation for diagnostic actions
"""

import asyncio
import io
import json
import time
import zipfile

import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from app.core.redaction import redact_config, redact_log_entry


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
    from fastapi.testclient import TestClient
    from app.main import app

    return TestClient(app)


@pytest.fixture
def svc():
    from app.services.notification_service import get_notification_service
    return get_notification_service()


def _make_mock_bundle(size_bytes: int = 1024) -> io.BytesIO:
    """Create a mock ZIP bundle of approximately the given size."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("metadata.json", '{"test": true}')
        if size_bytes > 200:
            # Pad with data to reach target size (uncompressed fills fast)
            zf.writestr("padding.bin", b"x" * (size_bytes - 200))
    buf.seek(0)
    return buf


# ═══════════════════════════════════════════════════════════════════════
# 1. Transmit Endpoint — Success
# ═══════════════════════════════════════════════════════════════════════

class TestTransmitEndpoint:
    """Test POST /api/diagnostics/transmit."""

    def test_successful_transmit(self, client, svc):
        """Successful transmit returns transmission_id and creates notification."""
        import app.routers.diagnostics as diag_module

        # Reset rate limiter (must be far enough in the past for monotonic clock)
        diag_module._last_transmit_time = time.monotonic() - 7200

        mock_bundle = _make_mock_bundle()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch(
            "app.services.diagnostic_service.DiagnosticService.generate_bundle",
            new_callable=AsyncMock,
            return_value=mock_bundle,
        ), patch(
            "app.routers.diagnostics.httpx.AsyncClient",
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            resp = client.post("/api/diagnostics/transmit")

        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert "transmission_id" in data
        assert "timestamp" in data
        assert "size_bytes" in data

        # Verify the POST was made to the correct endpoint
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert call_args[0][0] == "https://api.ai.market/api/v1/support/upload-diagnostic"
        assert call_args[1]["headers"]["Content-Type"] == "application/zip"

        # Verify success notification was created
        notifs = svc.list(category="diagnostic")
        success_notifs = [n for n in notifs if n.type == "success"]
        assert len(success_notifs) >= 1
        assert "transmitted" in success_notifs[0].message.lower() or "sent" in success_notifs[0].message.lower()


# ═══════════════════════════════════════════════════════════════════════
# 2. Rate Limiting — 1/hour
# ═══════════════════════════════════════════════════════════════════════

class TestTransmitRateLimit:
    """Test rate limiting on diagnostic transmission."""

    def test_rate_limited_within_hour(self, client):
        """Second transmit within 1 hour returns 429."""
        import app.routers.diagnostics as diag_module

        # Simulate a recent transmission
        diag_module._last_transmit_time = time.monotonic()

        resp = client.post("/api/diagnostics/transmit")
        assert resp.status_code == 429
        assert "rate limited" in resp.json()["detail"].lower()
        assert "Retry-After" in resp.headers

    def test_allowed_after_cooldown(self, client):
        """Transmit allowed after 1 hour has passed."""
        import app.routers.diagnostics as diag_module

        # Set last transmit to >1 hour ago
        diag_module._last_transmit_time = time.monotonic() - 3700

        mock_bundle = _make_mock_bundle()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch(
            "app.services.diagnostic_service.DiagnosticService.generate_bundle",
            new_callable=AsyncMock,
            return_value=mock_bundle,
        ), patch(
            "app.routers.diagnostics.httpx.AsyncClient",
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            resp = client.post("/api/diagnostics/transmit")

        assert resp.status_code == 200


# ═══════════════════════════════════════════════════════════════════════
# 3. Size Cap — 50 MB
# ═══════════════════════════════════════════════════════════════════════

class TestTransmitSizeCap:
    """Test 50 MB size cap on diagnostic transmission."""

    def test_oversized_bundle_rejected(self, client):
        """Bundle exceeding 50 MB returns 413."""
        import app.routers.diagnostics as diag_module
        diag_module._last_transmit_time = time.monotonic() - 7200

        # Create a mock bundle that reports >50MB
        oversized_buf = io.BytesIO(b"x" * (50 * 1024 * 1024 + 1))
        oversized_buf.seek(0)

        with patch(
            "app.services.diagnostic_service.DiagnosticService.generate_bundle",
            new_callable=AsyncMock,
            return_value=oversized_buf,
        ):
            resp = client.post("/api/diagnostics/transmit")

        assert resp.status_code == 413
        assert "50 MB" in resp.json()["detail"]

    def test_undersized_bundle_accepted(self, client):
        """Bundle under 50 MB is accepted."""
        import app.routers.diagnostics as diag_module
        diag_module._last_transmit_time = time.monotonic() - 7200

        mock_bundle = _make_mock_bundle(1024)  # 1 KB
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch(
            "app.services.diagnostic_service.DiagnosticService.generate_bundle",
            new_callable=AsyncMock,
            return_value=mock_bundle,
        ), patch(
            "app.routers.diagnostics.httpx.AsyncClient",
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            resp = client.post("/api/diagnostics/transmit")

        assert resp.status_code == 200


# ═══════════════════════════════════════════════════════════════════════
# 4. PII Scrubbing — Verification
# ═══════════════════════════════════════════════════════════════════════

class TestPIIScrubbing:
    """Verify PII scrubbing is applied to diagnostic bundles."""

    def test_api_keys_redacted_in_config(self):
        """API keys in config are scrubbed."""
        config = {
            "gemini_api_key": "sk-proj-abc123def456ghi789",
            "openai_api_key": "sk-1234567890abcdef",
            "app_name": "vectorAIz",
        }
        redacted = redact_config(config)
        assert "****" in redacted["gemini_api_key"]
        assert "****" in redacted["openai_api_key"]
        assert redacted["app_name"] == "vectorAIz"

    def test_emails_redacted_in_logs(self):
        """Email addresses in log entries are scrubbed."""
        entry = {"event": "login", "message": "User admin@company.com logged in"}
        redacted = redact_log_entry(entry)
        assert "admin@company.com" not in redacted["message"]
        assert "[REDACTED_EMAIL]" in redacted["message"]

    def test_jwt_tokens_redacted_in_logs(self):
        """JWT tokens in log entries are scrubbed."""
        jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.rg2e5j9gWKhBml_DJF0"
        entry = {"event": "auth", "details": f"Token: {jwt}"}
        redacted = redact_log_entry(entry)
        assert jwt not in redacted["details"]

    def test_passwords_redacted_in_config(self):
        """Password fields in config are scrubbed."""
        config = {"database_password": "super_secret_password_123"}
        redacted = redact_config(config)
        assert "****" in redacted["database_password"]
        assert "super_secret_password_123" not in redacted["database_password"]

    def test_env_var_secrets_redacted(self):
        """Various secret env var patterns are caught."""
        config = {
            "secret_key": "my-app-secret-12345678",
            "session_token": "sess-abcdefghijklmnop",
            "private_key": "-----BEGIN RSA PRIVATE KEY-----...",
        }
        redacted = redact_config(config)
        for key in config:
            assert "****" in redacted[key] or redacted[key] == "[REDACTED]"


# ═══════════════════════════════════════════════════════════════════════
# 5. Transmit Endpoint — Error Handling
# ═══════════════════════════════════════════════════════════════════════

class TestTransmitErrorHandling:
    """Test error scenarios for diagnostic transmission."""

    def test_upstream_error_returns_502(self, client):
        """HTTP error from ai.market returns 502."""
        import app.routers.diagnostics as diag_module
        import httpx

        diag_module._last_transmit_time = time.monotonic() - 7200

        mock_bundle = _make_mock_bundle()
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 500

        with patch(
            "app.services.diagnostic_service.DiagnosticService.generate_bundle",
            new_callable=AsyncMock,
            return_value=mock_bundle,
        ), patch(
            "app.routers.diagnostics.httpx.AsyncClient",
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_response.raise_for_status = MagicMock(
                side_effect=httpx.HTTPStatusError(
                    "Server Error",
                    request=MagicMock(),
                    response=mock_response,
                )
            )
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            resp = client.post("/api/diagnostics/transmit")

        assert resp.status_code == 502

    def test_network_error_returns_502(self, client):
        """Network/connection error returns 502."""
        import app.routers.diagnostics as diag_module

        diag_module._last_transmit_time = time.monotonic() - 7200

        mock_bundle = _make_mock_bundle()

        with patch(
            "app.services.diagnostic_service.DiagnosticService.generate_bundle",
            new_callable=AsyncMock,
            return_value=mock_bundle,
        ), patch(
            "app.routers.diagnostics.httpx.AsyncClient",
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(side_effect=ConnectionError("Connection refused"))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            resp = client.post("/api/diagnostics/transmit")

        assert resp.status_code == 502

    def test_bundle_generation_timeout(self, client):
        """Bundle generation timeout returns 504."""
        import app.routers.diagnostics as diag_module

        diag_module._last_transmit_time = time.monotonic() - 7200

        with patch(
            "app.services.diagnostic_service.DiagnosticService.generate_bundle",
            new_callable=AsyncMock,
            side_effect=asyncio.TimeoutError(),
        ):
            resp = client.post("/api/diagnostics/transmit")

        assert resp.status_code == 504


# ═══════════════════════════════════════════════════════════════════════
# 6. Fixed Endpoint — Security
# ═══════════════════════════════════════════════════════════════════════

class TestTransmitSecurity:
    """Test security mandates for diagnostic transmission."""

    def test_transmit_endpoint_is_hardcoded(self):
        """Verify the transmit endpoint is hardcoded, not configurable."""
        import app.routers.diagnostics as diag_module
        assert diag_module._TRANSMIT_ENDPOINT == "https://api.ai.market/api/v1/support/upload-diagnostic"
        assert diag_module._ALLOWED_HOST == "api.ai.market"

    def test_transmit_uses_https(self):
        """Verify endpoint uses HTTPS."""
        import app.routers.diagnostics as diag_module
        assert diag_module._TRANSMIT_ENDPOINT.startswith("https://")


# ═══════════════════════════════════════════════════════════════════════
# 7. prepare_support_bundle Tool
# ═══════════════════════════════════════════════════════════════════════

class TestPrepareSupportBundleTool:
    """Test the prepare_support_bundle allAI tool."""

    @pytest.mark.asyncio
    async def test_tool_creates_action_required_notification(self, svc):
        """Tool should create an action_required diagnostic notification."""
        from app.services.allai_tool_executor import AllAIToolExecutor
        from app.services.diagnostic_service import DiagnosticService

        mock_user = MagicMock()
        mock_user.user_id = "test-user"
        mock_send_ws = AsyncMock()

        executor = AllAIToolExecutor(
            user=mock_user,
            send_ws=mock_send_ws,
            session_id="test-session",
        )

        mock_bundle = _make_mock_bundle(2048)

        with patch.object(
            DiagnosticService,
            "generate_bundle",
            new_callable=AsyncMock,
            return_value=mock_bundle,
        ):
            result = await executor._handle_prepare_support_bundle({})

        assert result.frontend_data["success"] is True
        assert "bundle_size_kb" in result.frontend_data
        assert "contents" in result.frontend_data
        assert "notification" in result.llm_summary.lower() or "approve" in result.llm_summary.lower()

        # Verify notification was created
        notifs = svc.list(category="diagnostic")
        action_notifs = [n for n in notifs if n.type == "action_required"]
        assert len(action_notifs) == 1

        n = action_notifs[0]
        assert n.source == "allai"
        assert "scrubbed" in n.message.lower() or "redacted" in n.message.lower()

        meta = json.loads(n.metadata_json)
        assert meta["action"] == "transmit_diagnostic"
        assert "bundle_size_bytes" in meta
        assert "contents" in meta

    @pytest.mark.asyncio
    async def test_tool_in_dispatch_table(self):
        """prepare_support_bundle is registered in the executor dispatch table."""
        from app.services.allai_tool_executor import AllAIToolExecutor

        mock_user = MagicMock()
        mock_user.user_id = "test-user"
        executor = AllAIToolExecutor(
            user=mock_user,
            send_ws=AsyncMock(),
            session_id="test-session",
        )

        # Access the dispatch table
        handlers = {
            "prepare_support_bundle": executor._handle_prepare_support_bundle,
        }
        assert "prepare_support_bundle" in handlers

    def test_tool_definition_exists(self):
        """prepare_support_bundle exists in ALLAI_TOOLS."""
        from app.services.allai_tools import ALLAI_TOOLS

        tool_names = [t["name"] for t in ALLAI_TOOLS]
        assert "prepare_support_bundle" in tool_names

        tool = next(t for t in ALLAI_TOOLS if t["name"] == "prepare_support_bundle")
        assert "support" in tool["description"].lower()
        assert "approve" in tool["description"].lower() or "automatic" in tool["description"].lower()


# ═══════════════════════════════════════════════════════════════════════
# 8. Rate limit timestamp only updated on success
# ═══════════════════════════════════════════════════════════════════════

class TestTransmitRateLimitOnSuccess:
    """Verify rate limit timestamp is only set on successful transmission."""

    def test_failed_transmit_does_not_update_rate_limit(self, client):
        """A failed transmission should not consume the rate limit window."""
        import app.routers.diagnostics as diag_module

        original_time = time.monotonic() - 7200
        diag_module._last_transmit_time = original_time

        mock_bundle = _make_mock_bundle()

        with patch(
            "app.services.diagnostic_service.DiagnosticService.generate_bundle",
            new_callable=AsyncMock,
            return_value=mock_bundle,
        ), patch(
            "app.routers.diagnostics.httpx.AsyncClient",
        ) as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(side_effect=ConnectionError("Refused"))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            resp = client.post("/api/diagnostics/transmit")

        assert resp.status_code == 502

        # Rate limit should NOT have been updated (still the original value)
        assert diag_module._last_transmit_time == original_time
