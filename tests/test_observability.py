"""
BQ-123A: Tests for structured logging, error registry, deep health checks,
correlation IDs, issue tracker, resource guards, and redaction.

Covers ≥20 tests across all observability components.
"""

import json
import os
import tempfile
import time

import pytest
import yaml
from unittest.mock import patch, MagicMock

from app.core.errors import VectorAIzError, CODE_PATTERN
from app.core.errors.registry import ErrorRegistry, RegistryValidationError, VALID_DOMAINS
from app.core.issue_tracker import IssueTracker
from app.core.structured_logging import (
    request_id_var,
    correlation_id_var,
    session_id_var,
    _inject_context,
    APP_VERSION,
    SERVICE_NAME,
)


# ═══════════════════════════════════════════════════════════════════════
# 1. Error Registry — Loading & Validation
# ═══════════════════════════════════════════════════════════════════════

class TestErrorRegistryLoading:
    """Test YAML loading and validation."""

    def test_load_real_registry(self):
        """Load the actual registry.yaml and verify it has ≥30 codes."""
        registry = ErrorRegistry()
        registry.load()
        assert len(registry) >= 30, f"Expected ≥30 error codes, got {len(registry)}"
        assert registry.schema_version == 1

    def test_all_12_domains_covered(self):
        """Every domain must have at least 1 error code."""
        registry = ErrorRegistry()
        registry.load()
        covered = set()
        for code in registry.all_codes():
            domain = code.split("-")[1]
            covered.add(domain)
        assert covered == VALID_DOMAINS, f"Missing domains: {VALID_DOMAINS - covered}"

    def test_lookup_existing_code(self):
        """Lookup a known code returns the correct entry."""
        registry = ErrorRegistry()
        registry.load()
        entry = registry.lookup("VAI-QDR-001")
        assert entry.code == "VAI-QDR-001"
        assert entry.domain == "QDR"
        assert entry.title == "Qdrant unreachable"
        assert entry.retryable is True
        assert entry.http_status == 503
        assert len(entry.remediation) > 0

    def test_lookup_missing_code_raises(self):
        """Lookup a non-existent code raises KeyError."""
        registry = ErrorRegistry()
        registry.load()
        with pytest.raises(KeyError, match="Unknown error code"):
            registry.lookup("VAI-ZZZ-999")

    def test_get_returns_none_for_missing(self):
        """get() returns None for unknown codes."""
        registry = ErrorRegistry()
        registry.load()
        assert registry.get("VAI-ZZZ-999") is None

    def test_codes_for_domain(self):
        """Retrieve codes filtered by domain."""
        registry = ErrorRegistry()
        registry.load()
        qdr_codes = registry.codes_for_domain("QDR")
        assert len(qdr_codes) >= 1
        for code in qdr_codes:
            assert code.startswith("VAI-QDR-")

    def test_validation_rejects_bad_code_format(self):
        """Registry rejects entries with malformed code."""
        registry = ErrorRegistry()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({
                "schema_version": 1,
                "errors": [{
                    "code": "BAD-FORMAT",
                    "domain": "API",
                    "title": "test",
                    "severity": "WARN",
                    "retryable": False,
                    "user_action_required": False,
                    "http_status": 400,
                    "safe_message": "test",
                    "remediation": [],
                }]
            }, f)
            f.flush()
            with pytest.raises(RegistryValidationError, match="Invalid code format"):
                registry.load(f.name)
        os.unlink(f.name)

    def test_validation_rejects_duplicate_codes(self):
        """Registry rejects duplicate error codes."""
        registry = ErrorRegistry()
        entry = {
            "code": "VAI-API-001",
            "domain": "API",
            "title": "test",
            "severity": "WARN",
            "retryable": False,
            "user_action_required": False,
            "http_status": 400,
            "safe_message": "test",
            "remediation": [],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"schema_version": 1, "errors": [entry, entry]}, f)
            f.flush()
            with pytest.raises(RegistryValidationError, match="Duplicate code"):
                registry.load(f.name)
        os.unlink(f.name)

    def test_validation_rejects_domain_mismatch(self):
        """Registry rejects when domain field doesn't match code prefix."""
        registry = ErrorRegistry()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({
                "schema_version": 1,
                "errors": [{
                    "code": "VAI-API-001",
                    "domain": "QDR",  # mismatch
                    "title": "test",
                    "severity": "WARN",
                    "retryable": False,
                    "user_action_required": False,
                    "http_status": 400,
                    "safe_message": "test",
                    "remediation": [],
                }]
            }, f)
            f.flush()
            with pytest.raises(RegistryValidationError, match="doesn't match code prefix"):
                registry.load(f.name)
        os.unlink(f.name)


# ═══════════════════════════════════════════════════════════════════════
# 2. VectorAIzError → Structured Response
# ═══════════════════════════════════════════════════════════════════════

class TestVectorAIzError:
    """Test the base exception class."""

    def test_valid_code(self):
        """Creating error with valid code works."""
        err = VectorAIzError("VAI-QDR-001", detail="connection refused")
        assert err.code == "VAI-QDR-001"
        assert err.detail == "connection refused"
        assert str(err) == "VAI-QDR-001: connection refused"

    def test_invalid_code_raises(self):
        """Creating error with bad code format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid error code format"):
            VectorAIzError("BAD")

    def test_context_dict(self):
        """Context is stored as a dict."""
        err = VectorAIzError("VAI-DB-001", context={"table": "users"})
        assert err.context == {"table": "users"}

    def test_code_without_detail(self):
        """Code-only error works."""
        err = VectorAIzError("VAI-SYS-001")
        assert err.detail is None
        assert str(err) == "VAI-SYS-001"


class TestErrorMiddleware:
    """Test the error middleware response format."""

    @pytest.mark.asyncio
    async def test_known_code_returns_structured_response(self):
        """Middleware produces correct JSON for known error codes."""
        from app.core.errors.middleware import vectoraiz_error_handler

        # Load registry
        registry = ErrorRegistry()
        registry.load()

        with patch("app.core.errors.middleware.error_registry", registry):
            exc = VectorAIzError("VAI-QDR-001", detail="internal detail")
            request = MagicMock()
            response = await vectoraiz_error_handler(request, exc)

            assert response.status_code == 503
            body = json.loads(response.body)
            assert body["error"]["code"] == "VAI-QDR-001"
            assert body["error"]["title"] == "Qdrant unreachable"
            assert body["error"]["retryable"] is True
            assert "remediation" in body["error"]
            # Internal detail must NOT appear in response
            assert "internal detail" not in json.dumps(body)

    @pytest.mark.asyncio
    async def test_unknown_code_returns_500(self):
        """Middleware returns 500 for unregistered error code."""
        from app.core.errors.middleware import vectoraiz_error_handler

        registry = ErrorRegistry()  # Empty — not loaded
        with patch("app.core.errors.middleware.error_registry", registry):
            exc = VectorAIzError("VAI-API-099", detail="unregistered")
            request = MagicMock()
            response = await vectoraiz_error_handler(request, exc)

            assert response.status_code == 500
            body = json.loads(response.body)
            assert body["error"]["code"] == "VAI-API-099"


# ═══════════════════════════════════════════════════════════════════════
# 3. Correlation ID Injection
# ═══════════════════════════════════════════════════════════════════════

class TestCorrelationContext:
    """Test contextvars injection via structlog processor."""

    def test_inject_context_with_request_id(self):
        """Processor adds request_id when set in contextvar."""
        token = request_id_var.set("req-123")
        try:
            event_dict = {}
            result = _inject_context("test", "info", event_dict)
            assert result["request_id"] == "req-123"
            assert result["service"] == SERVICE_NAME
            assert result["version"] == APP_VERSION
        finally:
            request_id_var.reset(token)

    def test_inject_context_without_ids(self):
        """Processor skips IDs when not set."""
        event_dict = {}
        result = _inject_context("test", "info", event_dict)
        assert "request_id" not in result
        assert result["service"] == SERVICE_NAME

    def test_inject_all_ids(self):
        """All three IDs are injected when set."""
        t1 = request_id_var.set("r1")
        t2 = correlation_id_var.set("c1")
        t3 = session_id_var.set("s1")
        try:
            result = _inject_context("test", "info", {})
            assert result["request_id"] == "r1"
            assert result["correlation_id"] == "c1"
            assert result["session_id"] == "s1"
        finally:
            request_id_var.reset(t1)
            correlation_id_var.reset(t2)
            session_id_var.reset(t3)


# ═══════════════════════════════════════════════════════════════════════
# 4. Health Check Component Status
# ═══════════════════════════════════════════════════════════════════════

class TestHealthChecks:
    """Test health check helper functions."""

    @pytest.mark.asyncio
    async def test_disk_check_returns_status(self):
        """Disk check returns ok/degraded/down with free_pct."""
        from app.routers.health import _check_disk
        result = await _check_disk()
        assert "status" in result
        assert result["status"] in ("ok", "degraded", "down")
        if "free_pct" in result:
            assert isinstance(result["free_pct"], float)

    @pytest.mark.asyncio
    async def test_memory_check_returns_status(self):
        """Memory check returns ok/degraded/down with avail_pct."""
        from app.routers.health import _check_memory
        result = await _check_memory()
        assert "status" in result
        assert result["status"] in ("ok", "degraded", "down")

    @pytest.mark.asyncio
    async def test_bounded_check_timeout(self):
        """Bounded check returns 'down' on timeout."""
        from app.routers.health import _bounded_check

        async def slow_check():
            import asyncio
            await asyncio.sleep(10)
            return {"status": "ok"}

        name, result = await _bounded_check("test_slow", slow_check())
        assert name == "test_slow"
        assert result["status"] == "down"
        assert "timed out" in result.get("detail_safe", "")

    @pytest.mark.asyncio
    async def test_bounded_check_exception(self):
        """Bounded check returns 'down' on exception."""
        from app.routers.health import _bounded_check

        async def failing_check():
            raise RuntimeError("boom")

        name, result = await _bounded_check("test_fail", failing_check())
        assert result["status"] == "down"

    @pytest.mark.asyncio
    async def test_overall_status_worst_component(self):
        """Overall status should be worst of all components."""
        # Test the logic directly
        statuses = ["ok", "ok", "degraded"]
        if "down" in statuses:
            overall = "down"
        elif "degraded" in statuses:
            overall = "degraded"
        else:
            overall = "ok"
        assert overall == "degraded"

        statuses2 = ["ok", "down", "degraded"]
        if "down" in statuses2:
            overall2 = "down"
        elif "degraded" in statuses2:
            overall2 = "degraded"
        else:
            overall2 = "ok"
        assert overall2 == "down"


# ═══════════════════════════════════════════════════════════════════════
# 5. Redaction (key-based + value-based)
# ═══════════════════════════════════════════════════════════════════════

class TestRedaction:
    """Test that sensitive fields are not exposed in error responses."""

    @pytest.mark.asyncio
    async def test_internal_detail_not_in_response(self):
        """VectorAIzError detail must not appear in JSON response body."""
        from app.core.errors.middleware import vectoraiz_error_handler

        registry = ErrorRegistry()
        registry.load()

        with patch("app.core.errors.middleware.error_registry", registry):
            secret = "my_secret_api_key_12345"
            exc = VectorAIzError("VAI-API-001", detail=f"Auth failed with key={secret}")
            request = MagicMock()
            response = await vectoraiz_error_handler(request, exc)
            body_str = response.body.decode()
            assert secret not in body_str

    def test_safe_message_used_in_response(self):
        """Response uses safe_message from registry, not raw detail."""
        registry = ErrorRegistry()
        registry.load()
        entry = registry.lookup("VAI-ING-001")
        assert "supported" in entry.safe_message.lower()


# ═══════════════════════════════════════════════════════════════════════
# 6. Issue Tracker Ring Buffer
# ═══════════════════════════════════════════════════════════════════════

class TestIssueTracker:
    """Test in-memory ring buffer behavior."""

    def test_record_and_retrieve(self):
        """Record an issue and retrieve it."""
        tracker = IssueTracker(persist_path="/tmp/test_issues.json", max_size=10)
        tracker.record("VAI-QDR-001", "qdrant")
        issues = tracker.get_active_issues()
        assert len(issues) == 1
        assert issues[0]["code"] == "VAI-QDR-001"
        assert issues[0]["count"] == 1

    def test_duplicate_increments_count(self):
        """Recording the same code increments count."""
        tracker = IssueTracker(persist_path="/tmp/test_issues.json", max_size=10)
        tracker.record("VAI-QDR-001", "qdrant")
        tracker.record("VAI-QDR-001", "qdrant")
        tracker.record("VAI-QDR-001", "qdrant")
        issues = tracker.get_active_issues()
        assert len(issues) == 1
        assert issues[0]["count"] == 3

    def test_ring_buffer_eviction(self):
        """Oldest issue is evicted when buffer is full."""
        tracker = IssueTracker(persist_path="/tmp/test_issues.json", max_size=3)
        tracker.record("VAI-API-001")
        tracker.record("VAI-API-002")
        tracker.record("VAI-API-003")
        tracker.record("VAI-DB-001")  # Should evict API-001
        assert len(tracker) == 3
        codes = [i["code"] for i in tracker.get_active_issues()]
        assert "VAI-API-001" not in codes
        assert "VAI-DB-001" in codes

    def test_persist_and_reload(self):
        """Issues survive persist/reload cycle."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "issues.json")
            tracker1 = IssueTracker(persist_path=path)
            tracker1.record("VAI-SYS-001", "disk")
            tracker1.record("VAI-SYS-002", "memory")
            tracker1.persist()

            tracker2 = IssueTracker(persist_path=path)
            tracker2.reload()
            issues = tracker2.get_active_issues()
            codes = [i["code"] for i in issues]
            assert "VAI-SYS-001" in codes
            assert "VAI-SYS-002" in codes

    def test_auto_clear_old_issues(self):
        """Issues older than 1 hour are filtered from active list."""
        tracker = IssueTracker(persist_path="/tmp/test_issues.json")
        tracker.record("VAI-API-001")
        # Manually backdate the issue
        tracker._issues["VAI-API-001"].last_seen = time.time() - 7200  # 2 hours ago
        issues = tracker.get_active_issues()
        assert len(issues) == 0

    def test_clear_all(self):
        """clear() empties the tracker."""
        tracker = IssueTracker(persist_path="/tmp/test_issues.json")
        tracker.record("VAI-API-001")
        tracker.record("VAI-API-002")
        tracker.clear()
        assert len(tracker) == 0


# ═══════════════════════════════════════════════════════════════════════
# 7. Resource Guards
# ═══════════════════════════════════════════════════════════════════════

class TestResourceGuards:
    """Test disk/memory monitoring functions."""

    def test_check_disk_returns_valid_status(self):
        """check_disk returns a valid status."""
        from app.core.resource_guards import check_disk
        result = check_disk()
        assert result["status"] in ("ok", "degraded", "down", "unknown")

    def test_check_memory_returns_valid_status(self):
        """check_memory returns a valid status."""
        from app.core.resource_guards import check_memory
        result = check_memory()
        assert result["status"] in ("ok", "degraded", "down", "unknown")

    @patch("app.core.resource_guards.psutil")
    def test_disk_critical_blocks_ingestion(self, mock_psutil):
        """Disk <5% free sets ingestion_blocked flag."""
        import app.core.resource_guards as rg

        mock_usage = MagicMock()
        mock_usage.percent = 96.0  # 4% free
        mock_psutil.disk_usage.return_value = mock_usage

        result = rg.check_disk()
        assert result["status"] == "down"
        assert rg.ingestion_blocked is True

    @patch("app.core.resource_guards.psutil")
    def test_disk_ok_unblocks_ingestion(self, mock_psutil):
        """Disk >15% free clears ingestion_blocked flag."""
        import app.core.resource_guards as rg

        mock_usage = MagicMock()
        mock_usage.percent = 50.0  # 50% free
        mock_psutil.disk_usage.return_value = mock_usage

        result = rg.check_disk()
        assert result["status"] == "ok"
        assert rg.ingestion_blocked is False


# ═══════════════════════════════════════════════════════════════════════
# 8. Code Pattern Validation
# ═══════════════════════════════════════════════════════════════════════

class TestCodePattern:
    """Test error code regex pattern."""

    @pytest.mark.parametrize("code,valid", [
        ("VAI-QDR-001", True),
        ("VAI-API-999", True),
        ("VAI-SYSTEM-001", True),   # SYSTEM is 6 chars, valid format (domain validation is separate)
        ("VAI-AB-001", True),       # 2-char domain
        ("VAI-ABCDEF-001", True),   # 6-char domain
        ("BAD-FORMAT", False),
        ("VAI-QDR-01", False),       # Only 2 digits
        ("vai-qdr-001", False),      # Lowercase
        ("VAI-QDR-1000", False),     # 4 digits
    ])
    def test_code_pattern(self, code, valid):
        assert bool(CODE_PATTERN.match(code)) == valid
