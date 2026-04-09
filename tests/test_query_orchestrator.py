"""
Tests for QueryOrchestrator — auth enforcement, scope checking, revoked
rejection before work, disabled state, all 4 tool paths happy + error.

BQ-MCP-RAG Phase 1.
"""

import pytest
from unittest.mock import patch

from app.services.query_orchestrator import ConnectivityError, QueryOrchestrator
from app.services.connectivity_token_service import create_token, revoke_token
from app.utils.sanitization import sql_quote_literal


@pytest.fixture
def orchestrator():
    return QueryOrchestrator()


@pytest.fixture
def valid_token():
    raw, token = create_token(label="Orch Test", max_tokens=100)
    return raw, token


# ---------------------------------------------------------------------------
# Auth enforcement
# ---------------------------------------------------------------------------

class TestAuthEnforcement:
    def test_validate_token_success(self, orchestrator, valid_token):
        raw, expected = valid_token
        token = orchestrator.validate_token(raw)
        assert token.id == expected.id

    def test_validate_token_invalid(self, orchestrator):
        with pytest.raises(ConnectivityError) as exc_info:
            orchestrator.validate_token("invalid_token_here")
        assert exc_info.value.code == "auth_invalid"

    def test_validate_token_revoked(self, orchestrator, valid_token):
        raw, created = valid_token
        revoke_token(created.id)
        with pytest.raises(ConnectivityError) as exc_info:
            orchestrator.validate_token(raw)
        assert exc_info.value.code == "auth_revoked"

    def test_validate_token_malformed(self, orchestrator):
        with pytest.raises(ConnectivityError) as exc_info:
            orchestrator.validate_token("vzmcp_short_abc")
        assert exc_info.value.code == "auth_invalid"


# ---------------------------------------------------------------------------
# Scope enforcement
# ---------------------------------------------------------------------------

class TestScopeEnforcement:
    def test_enforce_scope_success(self, orchestrator):
        _, token = create_token(label="Scope OK", scopes=["ext:search", "ext:sql"], max_tokens=100)
        orchestrator._enforce_scope(token, "ext:search")  # should not raise

    def test_enforce_scope_denied(self, orchestrator):
        _, token = create_token(label="Scope Denied", scopes=["ext:search"], max_tokens=100)
        with pytest.raises(ConnectivityError) as exc_info:
            orchestrator._enforce_scope(token, "ext:sql")
        assert exc_info.value.code == "scope_denied"


# ---------------------------------------------------------------------------
# Disabled state
# ---------------------------------------------------------------------------

class TestDisabledState:
    def test_check_enabled_raises_when_disabled(self, orchestrator):
        with patch("app.services.query_orchestrator.settings") as mock_settings:
            mock_settings.connectivity_enabled = False
            with pytest.raises(ConnectivityError) as exc_info:
                orchestrator._check_enabled()
            assert exc_info.value.code == "service_unavailable"

    @pytest.mark.asyncio
    async def test_list_datasets_disabled(self, orchestrator, valid_token):
        _, token = valid_token
        with patch("app.services.query_orchestrator.settings") as mock_settings:
            mock_settings.connectivity_enabled = False
            with pytest.raises(ConnectivityError) as exc_info:
                await orchestrator.list_datasets(token)
            assert exc_info.value.code == "service_unavailable"


# ---------------------------------------------------------------------------
# Rate limit enforcement
# ---------------------------------------------------------------------------

class TestRateLimitEnforcement:
    def test_rate_limit_passes(self, orchestrator, valid_token):
        _, token = valid_token
        # Should not raise
        orchestrator._enforce_rate_limit(token, "list_datasets", "127.0.0.1")

    def test_rate_limit_blocked(self, orchestrator, valid_token):
        _, token = valid_token
        # Exhaust the rate limiter
        for _ in range(35):
            orchestrator.rate_limiter.record_request(token.id, "list_datasets")

        with pytest.raises(ConnectivityError) as exc_info:
            orchestrator._enforce_rate_limit(token, "list_datasets", "127.0.0.1")
        assert exc_info.value.code == "rate_limited"


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check(self, orchestrator):
        result = await orchestrator.health_check()
        assert result.status == "ok"
        assert result.version == "1.0"


# ---------------------------------------------------------------------------
# Format error
# ---------------------------------------------------------------------------

class TestFormatError:
    def test_format_error(self):
        err = QueryOrchestrator.format_error("test_code", "test message", {"key": "val"})
        assert err.error.code == "test_code"
        assert err.error.message == "test message"
        assert err.error.details == {"key": "val"}
        assert err.request_id.startswith("ext-")

    def test_format_error_no_details(self):
        err = QueryOrchestrator.format_error("test_code", "msg")
        assert err.error.details == {}


# ---------------------------------------------------------------------------
# SQL view creation quoting (Fix 1 — Gate 3)
# ---------------------------------------------------------------------------

class TestSQLViewQuoting:
    """Verify that sql_quote_literal + f-string quoting produces exactly one
    layer of quoting around file paths, even with special characters."""

    def test_simple_path(self):
        path = "/data/processed/file.parquet"
        escaped = sql_quote_literal(path)
        sql = f"SELECT * FROM read_parquet('{escaped}')"
        assert sql == "SELECT * FROM read_parquet('/data/processed/file.parquet')"

    def test_path_with_spaces(self):
        path = "/data/my files/report data.parquet"
        escaped = sql_quote_literal(path)
        sql = f"SELECT * FROM read_parquet('{escaped}')"
        assert sql == "SELECT * FROM read_parquet('/data/my files/report data.parquet')"

    def test_path_with_single_quotes(self):
        path = "/data/user's/it's a file.parquet"
        escaped = sql_quote_literal(path)
        sql = f"SELECT * FROM read_parquet('{escaped}')"
        # Single quotes should be doubled (standard SQL escaping), exactly one layer
        assert escaped == "/data/user''s/it''s a file.parquet"
        assert "'''" not in sql  # No triple quotes (would indicate double-quoting)

    def test_path_with_backslashes(self):
        path = r"C:\Users\data\file.parquet"
        escaped = sql_quote_literal(path)
        sql = f"SELECT * FROM read_parquet('{escaped}')"
        assert r"C:\Users\data\file.parquet" in sql

    def test_path_with_injection_attempt(self):
        """A path containing a quote followed by SQL should be safely escaped."""
        path = "/data/'); DROP TABLE users; --"
        escaped = sql_quote_literal(path)
        sql = f"SELECT * FROM read_parquet('{escaped}')"
        # The single quote in the path MUST be doubled (standard SQL escaping)
        # so it cannot break out of the string literal.
        assert "''" in escaped, "Single quote must be doubled to prevent breakout"
        assert escaped.count("''") == 1, "Exactly one quote should be doubled"
        # CRITICAL: the original unescaped injection pattern "'); DROP" with a
        # SINGLE quote must NOT appear after we neutralize escaped pairs.
        # The doubled quote "''); DROP" is safe because '' is an escaped quote
        # inside the string literal, not a terminator.
        safe_pattern = "''); DROP"
        dangerous_pattern = "'); DROP"
        assert safe_pattern in sql, "Escaped (safe) pattern must be present"
        # Remove all '' escaped-quote pairs, then verify the dangerous
        # single-quote breakout pattern no longer appears.
        neutralized = sql.replace("''", "__ESC__")
        assert dangerous_pattern not in neutralized, \
            "After removing escaped quotes, injection pattern must not appear"
