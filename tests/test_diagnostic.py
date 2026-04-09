"""
BQ-123B: Tests for diagnostic bundle — redaction, log buffer, collectors,
bundle service, and API endpoint.

Covers ≥12 tests per spec acceptance criteria.
"""

import asyncio
import io
import json
import time
import zipfile

import pytest
from unittest.mock import patch, AsyncMock

from app.core.redaction import (
    redact_config,
    redact_log_entry,
    redact_value,
    _is_sensitive_key,
)
from app.core.log_buffer import LogRingBuffer


# ═══════════════════════════════════════════════════════════════════════
# 1. Config Redaction — Key-based
# ═══════════════════════════════════════════════════════════════════════

class TestConfigRedaction:
    """Test key-based redaction for config values."""

    def test_sensitive_keys_redacted(self):
        """Keys containing sensitive substrings are redacted."""
        config = {
            "gemini_api_key": "sk-proj-abc123def456ghi789",
            "stripe_secret_key": "sk_live_abcdef1234567890",
            "internal_api_key": "aim_test_key_12345678",
            "app_name": "vectorAIz",
        }
        redacted = redact_config(config)

        assert redacted["app_name"] == "vectorAIz"
        assert "****" in redacted["gemini_api_key"]
        assert "****" in redacted["stripe_secret_key"]
        assert "****" in redacted["internal_api_key"]
        # Original values must not appear
        assert "abc123def456ghi789" not in redacted["gemini_api_key"]

    def test_non_secrets_preserved(self):
        """Non-sensitive values are passed through unchanged."""
        config = {
            "app_name": "vectorAIz",
            "debug": False,
            "qdrant_host": "qdrant",
            "qdrant_port": 6333,
            "cors_origins": ["http://localhost:5173"],
        }
        redacted = redact_config(config)
        assert redacted == config

    def test_nested_dict_redaction(self):
        """Sensitive keys inside nested dicts are redacted."""
        config = {
            "database": {
                "password": "super_secret_pw",
                "host": "localhost",
            }
        }
        redacted = redact_config(config)
        assert "****" in redacted["database"]["password"]
        assert redacted["database"]["host"] == "localhost"

    def test_short_value_fully_redacted(self):
        """Values ≤8 chars get fully redacted (not partial)."""
        config = {"api_key": "short"}
        redacted = redact_config(config)
        assert redacted["api_key"] == "[REDACTED]"

    def test_partial_redaction_format(self):
        """Values >8 chars show first4 + **** + last4."""
        result = redact_value("api_key", "abcdefghijklmnop")
        assert result == "abcd****mnop"

    def test_all_sensitive_substrings_matched(self):
        """All documented sensitive key substrings trigger redaction."""
        for substring in [
            "password", "passwd", "secret", "token", "apikey", "api_key",
            "authorization", "bearer", "cookie", "session", "private",
            "ssh", "cert", "key", "salt", "credential",
        ]:
            assert _is_sensitive_key(f"my_{substring}_value"), f"Failed for: {substring}"
            assert _is_sensitive_key(substring.upper()), f"Case failed for: {substring}"


# ═══════════════════════════════════════════════════════════════════════
# 2. Log Entry Redaction — Value-based
# ═══════════════════════════════════════════════════════════════════════

class TestLogEntryRedaction:
    """Test value-based redaction for log entries."""

    def test_jwt_redacted(self):
        """JWT tokens in log values are replaced."""
        jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"
        # Use a non-sensitive key so value-based redaction runs
        entry = {
            "event": "auth_attempt",
            "details": f"Used bearer {jwt}",
        }
        redacted = redact_log_entry(entry)
        assert "[REDACTED_JWT]" in redacted["details"]
        assert jwt not in redacted["details"]

    def test_email_redacted(self):
        """Email addresses in log values are replaced."""
        entry = {"event": "user_login", "message": "User user@example.com logged in"}
        redacted = redact_log_entry(entry)
        assert "[REDACTED_EMAIL]" in redacted["message"]
        assert "user@example.com" not in redacted["message"]

    def test_url_query_string_stripped(self):
        """URL query strings are stripped but host/path preserved."""
        entry = {
            "event": "api_call",
            "url": "https://api.example.com/v1/validate?token=secret&user=admin",
        }
        redacted = redact_log_entry(entry)
        assert "https://api.example.com/v1/validate" in redacted["url"]
        assert "token=secret" not in redacted["url"]

    def test_key_based_in_log_entries(self):
        """Sensitive keys in log entries are also redacted."""
        entry = {
            "event": "config_loaded",
            "api_key": "sk-proj-abc123def456ghi789",
            "level": "info",
        }
        redacted = redact_log_entry(entry)
        assert "****" in redacted["api_key"]
        assert redacted["level"] == "info"


# ═══════════════════════════════════════════════════════════════════════
# 3. Log Ring Buffer
# ═══════════════════════════════════════════════════════════════════════

class TestLogRingBuffer:
    """Test in-memory ring buffer behavior."""

    def test_append_and_retrieve(self):
        """Entries can be appended and retrieved."""
        buf = LogRingBuffer(max_size=5)
        buf.append({"event": "test1"})
        buf.append({"event": "test2"})
        entries = buf.get_entries()
        assert len(entries) == 2
        assert entries[0]["event"] == "test1"

    def test_overflow_evicts_oldest(self):
        """Buffer drops oldest entries when full."""
        buf = LogRingBuffer(max_size=3)
        for i in range(5):
            buf.append({"event": f"entry_{i}"})
        entries = buf.get_entries()
        assert len(entries) == 3
        assert entries[0]["event"] == "entry_2"
        assert entries[2]["event"] == "entry_4"

    def test_get_entries_with_limit(self):
        """get_entries respects the limit parameter."""
        buf = LogRingBuffer(max_size=100)
        for i in range(50):
            buf.append({"event": f"entry_{i}"})
        entries = buf.get_entries(limit=10)
        assert len(entries) == 10
        assert entries[0]["event"] == "entry_40"

    def test_clear(self):
        """clear() empties the buffer."""
        buf = LogRingBuffer(max_size=10)
        buf.append({"event": "test"})
        buf.clear()
        assert len(buf) == 0
        assert buf.get_entries() == []

    def test_thread_safety(self):
        """Buffer handles concurrent writes without crashing."""
        import threading

        buf = LogRingBuffer(max_size=1000)
        errors = []

        def writer(start):
            try:
                for i in range(100):
                    buf.append({"n": start + i})
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(i * 100,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(buf) == 500


# ═══════════════════════════════════════════════════════════════════════
# 4. Collector Timeout Handling
# ═══════════════════════════════════════════════════════════════════════

class TestCollectorTimeout:
    """Test that slow collectors return partial results + error."""

    @pytest.mark.asyncio
    async def test_slow_collector_returns_error(self):
        """A collector exceeding its timeout returns an error note."""
        from app.services.diagnostic_collectors import BaseCollector

        class SlowCollector(BaseCollector):
            name = "slow"
            timeout = 0.1

            async def collect(self):
                await asyncio.sleep(5)
                return {"data": "should not appear"}

        result = await SlowCollector().safe_collect()
        assert result.error is not None
        assert "timed out" in result.error
        assert result.data == {}

    @pytest.mark.asyncio
    async def test_failing_collector_returns_error(self):
        """A collector that raises returns an error note."""
        from app.services.diagnostic_collectors import BaseCollector

        class FailCollector(BaseCollector):
            name = "fail"
            timeout = 5.0

            async def collect(self):
                raise RuntimeError("database exploded")

        result = await FailCollector().safe_collect()
        assert result.error is not None
        assert "RuntimeError" in result.error
        assert result.data == {}

    @pytest.mark.asyncio
    async def test_successful_collector_has_no_error(self):
        """A normal collector returns data with no error."""
        from app.services.diagnostic_collectors import BaseCollector

        class GoodCollector(BaseCollector):
            name = "good"
            timeout = 5.0

            async def collect(self):
                return {"status": "ok", "items": 42}

        result = await GoodCollector().safe_collect()
        assert result.error is None
        assert result.data == {"status": "ok", "items": 42}
        assert result.duration_ms > 0


# ═══════════════════════════════════════════════════════════════════════
# 5. Individual Collectors
# ═══════════════════════════════════════════════════════════════════════

class TestIndividualCollectors:
    """Test that individual collectors return valid JSON-serializable data."""

    @pytest.mark.asyncio
    async def test_system_collector_returns_valid_data(self):
        """SystemCollector returns python version, cpu count, memory, disk."""
        from app.services.diagnostic_collectors import SystemCollector

        result = await SystemCollector().safe_collect()
        assert result.error is None
        data = result.data
        assert "python_version" in data
        assert "cpu_count" in data
        assert "memory_total_mb" in data
        assert "disk_total_gb" in data
        assert "vectoraiz_version" in data
        # Verify JSON-serializable
        json.dumps(data)

    @pytest.mark.asyncio
    async def test_config_collector_redacts_secrets(self):
        """ConfigCollector output has secrets redacted."""
        from app.services.diagnostic_collectors import ConfigCollector

        result = await ConfigCollector().safe_collect()
        assert result.error is None
        data = result.data

        # If any api_key fields have values, they should be redacted
        for key, val in data.items():
            if "key" in key.lower() and isinstance(val, str) and val not in ("", None):
                # Either partially redacted (****) or fully redacted
                assert "****" in val or val == "[REDACTED]", f"Key {key} not redacted: {val}"

    @pytest.mark.asyncio
    async def test_issue_collector_returns_list(self):
        """IssueCollector returns an issues list."""
        from app.services.diagnostic_collectors import IssueCollector

        result = await IssueCollector().safe_collect()
        assert result.error is None
        assert "issues" in result.data
        assert isinstance(result.data["issues"], list)

    @pytest.mark.asyncio
    async def test_error_collector_returns_registry(self):
        """ErrorCollector returns registry data."""
        from app.core.errors.registry import error_registry
        error_registry.load()

        from app.services.diagnostic_collectors import ErrorCollector

        result = await ErrorCollector().safe_collect()
        assert result.error is None
        assert "registry" in result.data
        assert result.data["registry"]["total_codes"] >= 30

    @pytest.mark.asyncio
    async def test_process_collector_returns_task_info(self):
        """ProcessCollector returns asyncio task info."""
        from app.services.diagnostic_collectors import ProcessCollector

        result = await ProcessCollector().safe_collect()
        assert result.error is None
        assert "asyncio_task_count" in result.data
        assert result.data["asyncio_task_count"] > 0

    @pytest.mark.asyncio
    async def test_qdrant_collector_handles_connection_failure(self):
        """QdrantCollector returns error on connection failure (graceful)."""
        from app.services.diagnostic_collectors import QdrantCollector

        collector = QdrantCollector()
        collector.timeout = 3.0
        result = await collector.safe_collect()
        # In CI/test environment Qdrant is likely not running
        # The collector should either succeed or return a clean error
        assert isinstance(result.data, dict)
        if result.error:
            assert isinstance(result.error, str)

    @pytest.mark.asyncio
    async def test_log_collector_returns_entries(self):
        """LogCollector returns recent log entries."""
        from app.core.log_buffer import log_ring_buffer
        from app.services.diagnostic_collectors import LogCollector

        # Seed some entries
        log_ring_buffer.append({"event": "test_log_1", "level": "info"})
        log_ring_buffer.append({"event": "test_log_2", "level": "error"})

        result = await LogCollector().safe_collect()
        assert result.error is None
        assert result.data["count"] >= 2


# ═══════════════════════════════════════════════════════════════════════
# 6. Bundle ZIP Structure
# ═══════════════════════════════════════════════════════════════════════

class TestBundleZipStructure:
    """Test the ZIP bundle has correct files and metadata."""

    @pytest.mark.asyncio
    async def test_bundle_contains_required_files(self):
        """Bundle ZIP has metadata.json and collector outputs."""
        from app.services.diagnostic_service import DiagnosticService
        from app.services.diagnostic_collectors import BaseCollector

        # Use minimal mock collectors for speed
        class MockCollector(BaseCollector):
            def __init__(self, name_val):
                self.name = name_val
                self.timeout = 5.0

            async def collect(self):
                return {"status": "ok"}

        service = DiagnosticService(collectors=[
            MockCollector("health"),
            MockCollector("config"),
            MockCollector("system"),
        ])
        buf = await service.generate_bundle()

        with zipfile.ZipFile(buf, "r") as zf:
            names = zf.namelist()
            assert "metadata.json" in names
            assert "collector_summary.json" in names
            assert "health/health_snapshot.json" in names
            assert "config/redacted_config.json" in names
            assert "system/runtime.json" in names

    @pytest.mark.asyncio
    async def test_metadata_includes_required_fields(self):
        """metadata.json has bundle_version, generated_at, vectoraiz_version, host_id."""
        from app.services.diagnostic_service import DiagnosticService
        from app.services.diagnostic_collectors import BaseCollector

        class StubCollector(BaseCollector):
            name = "stub"
            async def collect(self):
                return {"ok": True}

        service = DiagnosticService(collectors=[StubCollector()])
        buf = await service.generate_bundle()

        with zipfile.ZipFile(buf, "r") as zf:
            metadata = json.loads(zf.read("metadata.json"))
            assert "bundle_version" in metadata
            assert "generated_at" in metadata
            assert "vectoraiz_version" in metadata
            assert "host_id" in metadata
            assert "hostname" not in metadata
            assert metadata["bundle_version"] == 1

    @pytest.mark.asyncio
    async def test_logs_stored_as_ndjson(self):
        """Log entries are stored as NDJSON in logs/recent.jsonl."""
        from app.services.diagnostic_service import DiagnosticService
        from app.services.diagnostic_collectors import BaseCollector

        class FakeLogCollector(BaseCollector):
            name = "logs"
            async def collect(self):
                return {
                    "count": 2,
                    "entries": [
                        {"event": "a", "level": "info"},
                        {"event": "b", "level": "error"},
                    ],
                }

        service = DiagnosticService(collectors=[FakeLogCollector()])
        buf = await service.generate_bundle()

        with zipfile.ZipFile(buf, "r") as zf:
            assert "logs/recent.jsonl" in zf.namelist()
            lines = zf.read("logs/recent.jsonl").decode().strip().split("\n")
            assert len(lines) == 2
            # Each line is valid JSON
            for line in lines:
                json.loads(line)

    @pytest.mark.asyncio
    async def test_failed_collector_included_with_error(self):
        """A failed collector's output includes _collector_error."""
        from app.services.diagnostic_service import DiagnosticService
        from app.services.diagnostic_collectors import BaseCollector

        class BrokenCollector(BaseCollector):
            name = "system"
            timeout = 0.1

            async def collect(self):
                raise ValueError("boom")

        service = DiagnosticService(collectors=[BrokenCollector()])
        buf = await service.generate_bundle()

        with zipfile.ZipFile(buf, "r") as zf:
            data = json.loads(zf.read("system/runtime.json"))
            assert "_collector_error" in data
            assert "ValueError" in data["_collector_error"]


# ═══════════════════════════════════════════════════════════════════════
# 7. API Endpoint — Rate Limiting & Auth
# ═══════════════════════════════════════════════════════════════════════

class TestDiagnosticEndpoint:
    """Test the POST /api/diagnostics/bundle endpoint."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from app.main import app
        return TestClient(app)

    def test_auth_required_when_enabled(self):
        """Unauthenticated request returns 401 when auth is enabled."""
        import os
        old = os.environ.get("VECTORAIZ_AUTH_ENABLED", "")
        os.environ["VECTORAIZ_AUTH_ENABLED"] = "true"
        try:
            from fastapi.testclient import TestClient
            from app.main import app
            client = TestClient(app, raise_server_exceptions=False)
            response = client.post("/api/diagnostics/bundle")
            assert response.status_code == 401
        finally:
            os.environ["VECTORAIZ_AUTH_ENABLED"] = old or "false"

    def test_rate_limiting(self, client):
        """Second request within 1 minute returns 429."""
        import app.routers.diagnostics as diag_module

        # Simulate a recent bundle by setting last_bundle_time to now
        diag_module._last_bundle_time = time.monotonic()

        response = client.post("/api/diagnostics/bundle")
        assert response.status_code == 429
        assert "Rate limited" in response.json()["detail"]

    def test_successful_download(self):
        """Successful request returns ZIP content-type with attachment header."""
        import app.routers.diagnostics as diag_module

        # Reset rate limiter — set far enough in the past to pass the 60s check
        diag_module._last_bundle_time = time.monotonic() - 120

        # Create a real minimal ZIP
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("metadata.json", '{"test": true}')
        buf.seek(0)

        with patch(
            "app.services.diagnostic_service.DiagnosticService.generate_bundle",
            new_callable=AsyncMock,
            return_value=buf,
        ):
            # Use a fresh client to avoid any cached state from prior tests
            from fastapi.testclient import TestClient
            from app.main import app
            fresh_client = TestClient(app)
            response = fresh_client.post("/api/diagnostics/bundle")
            assert response.status_code == 200
            assert response.headers["content-type"] == "application/zip"
            assert "vectoraiz-diagnostic-" in response.headers["content-disposition"]

            # Verify it's a valid ZIP
            result_zip = zipfile.ZipFile(io.BytesIO(response.content))
            assert "metadata.json" in result_zip.namelist()


# ═══════════════════════════════════════════════════════════════════════
# 8. Database Collector — Schema Version
# ═══════════════════════════════════════════════════════════════════════

class TestDatabaseCollector:
    """Test DatabaseCollector returns schema version."""

    @pytest.mark.asyncio
    async def test_returns_alembic_version(self):
        """DatabaseCollector returns backend type and alembic version info."""
        from app.services.diagnostic_collectors import DatabaseCollector
        from sqlalchemy import create_engine

        # Use an in-memory SQLite engine for testing
        test_engine = create_engine("sqlite:///:memory:")
        with test_engine.connect() as conn:
            conn.execute(__import__("sqlalchemy", fromlist=["text"]).text(
                "CREATE TABLE alembic_version (version_num VARCHAR(32))"
            ))
            conn.execute(__import__("sqlalchemy", fromlist=["text"]).text(
                "INSERT INTO alembic_version VALUES ('abc123')"
            ))
            conn.commit()

        with patch("app.core.database.get_engine", return_value=test_engine), \
             patch("app.core.database.DATABASE_URL", "sqlite:///:memory:"):
            result = await DatabaseCollector().safe_collect()

        assert result.error is None
        assert result.data["backend"] == "sqlite"
        assert result.data["alembic_version"] == "abc123"
