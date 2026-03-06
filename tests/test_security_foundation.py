"""
Tests for BQ-VZ-CONTROL-PLANE Step 2: Security & Event Foundation

Covers:
1. ApprovalTokenService — token lifecycle, CAS, expiry, wrong_user preservation
2. Tool classification — 3-path routing, unclassified = denied
3. Capability auth — default-deny for unmapped tools
4. Output redaction — UUIDs preserved, API keys/paths/PEM stripped
5. Audit logger — entries written to SQLite
6. Event bus — multi-tab emit, subscribe/unsubscribe

PHASE: BQ-VZ-CONTROL-PLANE Step 2 Tests
CREATED: 2026-03-05
"""

import asyncio
import hashlib
import json
import os
import sqlite3
import tempfile
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.approval_token_service import (
    ALL_CLASSIFIED_TOOLS,
    AUTO_APPROVE_TOOLS,
    MUTATION_TOOLS,
    READ_ONLY_TOOLS,
    RISK_TTL,
    TOOL_CAPABILITIES,
    ApprovalTokenService,
    check_capabilities,
    get_user_capabilities,
)
from app.services.audit_logger import AuditLogger
from app.services.event_bus import EventBus, VZEvent
from app.utils.output_redaction import redact_for_llm


# =====================================================================
# ApprovalTokenService Tests
# =====================================================================

class TestApprovalTokenService:

    def setup_method(self):
        self.svc = ApprovalTokenService()

    def test_create_token_returns_token(self):
        token = self.svc.create_token(
            user_id="user1", session_id="sess1",
            tool_name="delete_dataset",
            tool_input={"dataset_id": "abc123"},
        )
        assert token.id is not None
        assert token.user_id == "user1"
        assert token.session_id == "sess1"
        assert token.tool_name == "delete_dataset"
        assert token.status == "pending"
        assert token.risk_level == "high"

    def test_create_token_risk_based_ttl(self):
        # High risk → 30s
        token = self.svc.create_token(
            user_id="u", session_id="s",
            tool_name="delete_dataset",
            tool_input={},
        )
        assert token.expires_at - token.created_at == pytest.approx(30, abs=1)

        # Low risk → 120s
        token2 = self.svc.create_token(
            user_id="u", session_id="s",
            tool_name="stop_public_tunnel",
            tool_input={},
        )
        assert token2.expires_at - token2.created_at == pytest.approx(120, abs=1)

        # Medium risk → 60s
        token3 = self.svc.create_token(
            user_id="u", session_id="s",
            tool_name="connectivity_enable",
            tool_input={},
        )
        assert token3.expires_at - token3.created_at == pytest.approx(60, abs=1)

    def test_validate_and_consume_success(self):
        token = self.svc.create_token(
            user_id="user1", session_id="sess1",
            tool_name="delete_dataset",
            tool_input={"dataset_id": "abc"},
        )
        result = self.svc.validate_and_consume(
            token_id=token.id, user_id="user1", session_id="sess1",
        )
        assert result.success is True
        assert result.tool_name == "delete_dataset"
        assert result.tool_input == {"dataset_id": "abc"}

    def test_single_use_token(self):
        """Token can only be consumed once."""
        token = self.svc.create_token(
            user_id="user1", session_id="sess1",
            tool_name="delete_dataset",
            tool_input={},
        )
        result1 = self.svc.validate_and_consume(token.id, "user1", "sess1")
        assert result1.success is True

        # Second attempt should fail
        result2 = self.svc.validate_and_consume(token.id, "user1", "sess1")
        assert result2.success is False
        assert result2.reason == "not_found_or_already_used"

    def test_wrong_user_does_not_consume(self):
        """Wrong user should NOT consume the token — it stays pending."""
        token = self.svc.create_token(
            user_id="user1", session_id="sess1",
            tool_name="delete_dataset",
            tool_input={},
        )
        # Wrong user tries
        result = self.svc.validate_and_consume(token.id, "attacker", "sess1")
        assert result.success is False
        assert result.reason == "wrong_user"

        # Correct user can still use it
        result2 = self.svc.validate_and_consume(token.id, "user1", "sess1")
        assert result2.success is True

    def test_wrong_session_does_not_consume(self):
        """Wrong session should NOT consume the token — it stays pending."""
        token = self.svc.create_token(
            user_id="user1", session_id="sess1",
            tool_name="delete_dataset",
            tool_input={},
        )
        result = self.svc.validate_and_consume(token.id, "user1", "other_session")
        assert result.success is False
        assert result.reason == "wrong_session"

        # Correct session can still use it
        result2 = self.svc.validate_and_consume(token.id, "user1", "sess1")
        assert result2.success is True

    def test_expired_token(self):
        """Expired tokens should be rejected."""
        token = self.svc.create_token(
            user_id="user1", session_id="sess1",
            tool_name="delete_dataset",
            tool_input={},
        )
        # Manually expire it
        token.expires_at = time.time() - 1

        result = self.svc.validate_and_consume(token.id, "user1", "sess1")
        assert result.success is False
        assert result.reason == "expired"

    def test_deny_token(self):
        token = self.svc.create_token(
            user_id="u", session_id="s",
            tool_name="delete_dataset",
            tool_input={},
        )
        assert self.svc.deny_token(token.id) is True

        # Can't consume after deny
        result = self.svc.validate_and_consume(token.id, "u", "s")
        assert result.success is False

    def test_args_hash_integrity(self):
        """Token stores and verifies args hash."""
        tool_input = {"dataset_id": "abc123"}
        token = self.svc.create_token(
            user_id="u", session_id="s",
            tool_name="delete_dataset",
            tool_input=tool_input,
        )
        expected_hash = hashlib.sha256(
            json.dumps(tool_input, sort_keys=True).encode()
        ).hexdigest()
        assert token.tool_args_hash == expected_hash

    def test_cleanup_expired(self):
        """Expired tokens should be cleaned up."""
        token = self.svc.create_token(
            user_id="u", session_id="s",
            tool_name="delete_dataset",
            tool_input={},
        )
        token.expires_at = time.time() - 100
        self.svc._cleanup_expired()
        assert token.id not in self.svc._pending


# =====================================================================
# Tool Classification Tests
# =====================================================================

class TestToolClassification:

    def test_all_tools_classified(self):
        """Every tool in ALLAI_TOOLS should be in exactly one category."""
        from app.services.allai_tools import ALLAI_TOOLS
        tool_names = {t["name"] for t in ALLAI_TOOLS}

        for name in tool_names:
            assert name in ALL_CLASSIFIED_TOOLS, (
                f"Tool '{name}' is not classified in any category"
            )

    def test_no_overlap_between_categories(self):
        """Categories should be mutually exclusive."""
        assert READ_ONLY_TOOLS & AUTO_APPROVE_TOOLS == set()
        assert READ_ONLY_TOOLS & set(MUTATION_TOOLS.keys()) == set()
        assert AUTO_APPROVE_TOOLS & set(MUTATION_TOOLS.keys()) == set()

    def test_read_only_tools(self):
        expected = {
            "list_datasets", "get_dataset_detail", "preview_rows",
            "run_sql_query", "search_vectors", "get_system_status",
            "get_dataset_statistics", "connectivity_status",
            "get_notifications", "get_tunnel_status",
            "generate_diagnostic_bundle", "connectivity_test",
            "connectivity_generate_setup",
        }
        assert READ_ONLY_TOOLS == expected

    def test_auto_approve_tools(self):
        expected = {
            "log_feedback", "submit_feedback",
            "create_notification", "prepare_support_bundle",
            "create_artifact", "create_artifact_from_query",
        }
        assert AUTO_APPROVE_TOOLS == expected

    def test_mutation_tools_have_risk(self):
        for tool_name, info in MUTATION_TOOLS.items():
            assert "risk" in info, f"Mutation tool '{tool_name}' missing risk"
            assert info["risk"] in RISK_TTL, f"Invalid risk level for '{tool_name}'"

    def test_delete_dataset_is_high_risk(self):
        assert MUTATION_TOOLS["delete_dataset"]["risk"] == "high"


# =====================================================================
# Capability Auth Tests
# =====================================================================

class TestCapabilityAuth:

    def test_all_classified_tools_have_capabilities(self):
        """Every classified tool must have a capability mapping."""
        for tool_name in ALL_CLASSIFIED_TOOLS:
            assert tool_name in TOOL_CAPABILITIES, (
                f"Tool '{tool_name}' missing from TOOL_CAPABILITIES"
            )

    def test_unmapped_tool_denied(self):
        """Unmapped tools should be denied by default."""
        error = check_capabilities(MagicMock(), "totally_fake_tool")
        assert error is not None
        assert "denied by default" in error

    def test_mapped_tool_allowed(self):
        """Mapped tools should be allowed for V1 user."""
        user = MagicMock()
        for tool_name in ALL_CLASSIFIED_TOOLS:
            error = check_capabilities(user, tool_name)
            assert error is None, f"Tool '{tool_name}' was denied: {error}"

    def test_user_has_all_capabilities_v1(self):
        """V1: single-user gets all capabilities."""
        caps = get_user_capabilities(MagicMock())
        for tool_name, required in TOOL_CAPABILITIES.items():
            assert required.issubset(caps), f"Missing caps for {tool_name}"


# =====================================================================
# Executor Routing Tests
# =====================================================================

class TestExecutorRouting:

    def _make_executor(self, user_id="user1", session_id="sess1"):
        from app.services.allai_tool_executor import AllAIToolExecutor
        user = MagicMock()
        user.user_id = user_id
        send_ws = AsyncMock()
        return AllAIToolExecutor(user=user, send_ws=send_ws, session_id=session_id)

    @pytest.mark.asyncio
    async def test_unclassified_tool_denied(self):
        executor = self._make_executor()
        result = await executor.execute("nonexistent_tool", {})
        assert "denied" in result.llm_summary.lower() or "denied" in result.frontend_data.get("error", "").lower()

    @pytest.mark.asyncio
    async def test_read_only_executes_immediately(self):
        executor = self._make_executor()
        with patch.object(executor, '_dispatch', new_callable=AsyncMock) as mock_dispatch:
            mock_dispatch.return_value = MagicMock(
                frontend_data={"test": True}, llm_summary="success"
            )
            result = await executor.execute("get_system_status", {})
            mock_dispatch.assert_called_once_with("get_system_status", {})

    @pytest.mark.asyncio
    async def test_auto_approve_executes_immediately(self):
        executor = self._make_executor()
        with patch.object(executor, '_dispatch', new_callable=AsyncMock) as mock_dispatch:
            mock_dispatch.return_value = MagicMock(
                frontend_data={}, llm_summary="logged"
            )
            result = await executor.execute("log_feedback", {"category": "general", "sentiment": "positive", "summary": "test", "raw_message": "test"})
            mock_dispatch.assert_called_once()

    @pytest.mark.asyncio
    async def test_mutation_requests_approval(self):
        executor = self._make_executor()
        with patch.object(executor, '_get_resource_details', new_callable=AsyncMock) as mock_details, \
             patch.object(executor, '_authorize', return_value=(True, "")):
            mock_details.return_value = {"description": "Delete dataset 'test'"}
            result = await executor.execute("delete_dataset", {"dataset_id": "abc"})
            assert result.frontend_data.get("status") == "confirmation_requested"
            assert "confirm_id" in result.frontend_data


# =====================================================================
# Output Redaction Tests
# =====================================================================

class TestOutputRedaction:

    def test_uuid_preserved(self):
        uuid_str = "550e8400-e29b-41d4-a716-446655440000"
        text = f"Dataset {uuid_str} processed successfully"
        result = redact_for_llm(text)
        assert uuid_str in result

    def test_sk_api_key_redacted(self):
        text = "Using API key sk-abc123def456ghi789jkl012mno345"
        result = redact_for_llm(text)
        assert "sk-abc123" not in result
        assert "[API_KEY_REDACTED]" in result

    def test_sk_ant_api_key_redacted(self):
        text = "Using sk-ant-abc123-def456ghi789jkl012mno345 for auth"
        result = redact_for_llm(text)
        assert "sk-ant-abc123" not in result
        assert "REDACTED" in result

    def test_xai_key_redacted(self):
        text = "XAI key: xai-abc123def456ghi789jkl012mno345"
        result = redact_for_llm(text)
        assert "xai-abc123" not in result
        assert "[API_KEY_REDACTED]" in result

    def test_gsk_key_redacted(self):
        text = "Key: gsk_abc123def456ghi789jkl012mno345"
        result = redact_for_llm(text)
        assert "gsk_abc123" not in result
        assert "[API_KEY_REDACTED]" in result

    def test_google_key_redacted(self):
        text = "Key: AIzaSyABC123def456ghi789jkl012mno345pqrst"
        result = redact_for_llm(text)
        assert "AIzaSy" not in result
        assert "[API_KEY_REDACTED]" in result

    def test_bearer_token_redacted(self):
        text = "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.abc123"
        result = redact_for_llm(text)
        assert "eyJhbG" not in result
        assert "Bearer [REDACTED]" in result

    def test_key_value_secret_redacted(self):
        text = "password=supersecretvalue123"
        result = redact_for_llm(text)
        assert "supersecret" not in result
        assert "REDACTED" in result

    def test_users_path_redacted(self):
        text = "File at /Users/john/Documents/secret.txt"
        result = redact_for_llm(text)
        assert "/Users/john" not in result
        assert "[PATH_REDACTED]" in result

    def test_home_path_redacted(self):
        text = "Config at /home/deploy/.ssh/id_rsa"
        result = redact_for_llm(text)
        assert "/home/deploy" not in result
        assert "[PATH_REDACTED]" in result

    def test_pem_private_key_redacted(self):
        text = "-----BEGIN RSA PRIVATE KEY-----\nMIIBogIBAAJBALG\n-----END RSA PRIVATE KEY-----"
        result = redact_for_llm(text)
        assert "MIIBog" not in result
        assert "[PRIVATE_KEY_REDACTED]" in result

    def test_postgres_url_redacted(self):
        text = "postgres://admin:password123@localhost:5432/mydb"
        result = redact_for_llm(text)
        assert "password123" not in result
        assert "[CREDENTIALS_REDACTED]" in result

    def test_empty_string(self):
        assert redact_for_llm("") == ""

    def test_none_returns_none(self):
        assert redact_for_llm(None) is None

    def test_uuid_with_api_key_preserves_uuid(self):
        """UUID preserved even when text also contains API key."""
        uuid_str = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        text = f"Dataset {uuid_str} key=sk-abc123def456ghi789jkl012mno345"
        result = redact_for_llm(text)
        assert uuid_str in result
        assert "sk-abc123" not in result


# =====================================================================
# Audit Logger Tests
# =====================================================================

class TestAuditLogger:

    @pytest.mark.asyncio
    async def test_audit_entry_written(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            logger = AuditLogger(db_path=db_path)
            await logger.log(
                session_id="sess1",
                user_id="user1",
                tool_name="delete_dataset",
                tool_input={"dataset_id": "abc123"},
                outcome="success",
                duration_ms=42,
                approval_token_id="tok1",
            )

            conn = sqlite3.connect(db_path)
            rows = conn.execute("SELECT * FROM audit_log").fetchall()
            conn.close()

            assert len(rows) == 1
            row = rows[0]
            # row: id, timestamp, session_id, user_id, tool_name, tool_input_hash,
            #      resource_id, outcome, duration_ms, error_category, approval_token_id, created_at
            assert row[2] == "sess1"
            assert row[3] == "user1"
            assert row[4] == "delete_dataset"
            assert row[6] == "abc123"  # resource_id extracted
            assert row[7] == "success"
            assert row[8] == 42
            assert row[10] == "tok1"
        finally:
            os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_resource_id_extraction(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            logger = AuditLogger(db_path=db_path)

            # dataset_id field
            await logger.log(
                session_id="s", user_id="u", tool_name="t",
                tool_input={"dataset_id": "ds1"}, outcome="ok",
            )
            # token_id field
            await logger.log(
                session_id="s", user_id="u", tool_name="t",
                tool_input={"token_id": "tok1"}, outcome="ok",
            )
            # no resource field
            await logger.log(
                session_id="s", user_id="u", tool_name="t",
                tool_input={"limit": 10}, outcome="ok",
            )

            conn = sqlite3.connect(db_path)
            rows = conn.execute("SELECT resource_id FROM audit_log ORDER BY id").fetchall()
            conn.close()

            assert rows[0][0] == "ds1"
            assert rows[1][0] == "tok1"
            assert rows[2][0] is None
        finally:
            os.unlink(db_path)


# =====================================================================
# Event Bus Tests
# =====================================================================

class TestEventBus:

    @pytest.mark.asyncio
    async def test_subscribe_and_emit(self):
        bus = EventBus()
        queue = bus.subscribe("sess1")

        event = VZEvent(event_type="test", session_id="sess1", data={"key": "val"})
        await bus.emit("sess1", event)

        received = queue.get_nowait()
        assert received.event_type == "test"
        assert received.data == {"key": "val"}

    @pytest.mark.asyncio
    async def test_multi_tab_emit(self):
        """Multiple subscribers for same session should all receive events."""
        bus = EventBus()
        q1 = bus.subscribe("sess1")
        q2 = bus.subscribe("sess1")

        event = VZEvent(event_type="test", session_id="sess1", data={})
        await bus.emit("sess1", event)

        assert q1.get_nowait().event_type == "test"
        assert q2.get_nowait().event_type == "test"

    @pytest.mark.asyncio
    async def test_unsubscribe(self):
        bus = EventBus()
        q1 = bus.subscribe("sess1")
        q2 = bus.subscribe("sess1")

        bus.unsubscribe("sess1", q1)

        event = VZEvent(event_type="test", session_id="sess1", data={})
        await bus.emit("sess1", event)

        assert q1.empty()
        assert q2.get_nowait().event_type == "test"

    @pytest.mark.asyncio
    async def test_unsubscribe_last_cleans_up(self):
        bus = EventBus()
        q = bus.subscribe("sess1")
        bus.unsubscribe("sess1", q)
        assert "sess1" not in bus._subscribers

    @pytest.mark.asyncio
    async def test_emit_to_nonexistent_session(self):
        """Emitting to a session with no subscribers should not error."""
        bus = EventBus()
        event = VZEvent(event_type="test", session_id="nobody", data={})
        await bus.emit("nobody", event)  # Should not raise

    def test_vzevent_to_sse(self):
        event = VZEvent(event_type="test_event", session_id="s", data={"x": 1}, timestamp=1234567890.0)
        sse = event.to_sse()
        assert "event: test_event\n" in sse
        assert '"type": "test_event"' in sse
        assert '"x": 1' in sse
