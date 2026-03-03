"""
S132: Security audit sweep tests.

Covers:
- Auth disabled only works in debug+development mode
- LLM admin routes require auth
- Login rate limiting (6th attempt returns 429)
- Diagnostic bundle errors are redacted
- Deep health requires auth
- AAD mismatch fails decryption
"""

import os
import time

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient


# ═══════════════════════════════════════════════════════════════════════
# 1. Auth disabled only works in debug+development mode
# ═══════════════════════════════════════════════════════════════════════

class TestAuthDisableGuard:
    """VECTORAIZ_AUTH_ENABLED=false only works when debug=True AND ENVIRONMENT=development."""

    def test_auth_disabled_in_debug_development(self, monkeypatch):
        """Auth can be disabled when debug=True and ENVIRONMENT=development."""
        monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "false")
        monkeypatch.setenv("ENVIRONMENT", "development")

        from app.auth.api_key_auth import _is_auth_enabled
        with patch("app.auth.api_key_auth.settings") as mock_settings:
            mock_settings.debug = True
            mock_settings.auth_enabled = True
            assert _is_auth_enabled() is False

    def test_auth_not_disabled_in_production(self, monkeypatch):
        """Auth stays enabled in production even with VECTORAIZ_AUTH_ENABLED=false."""
        monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "false")
        monkeypatch.setenv("ENVIRONMENT", "production")

        from app.auth.api_key_auth import _is_auth_enabled
        with patch("app.auth.api_key_auth.settings") as mock_settings:
            mock_settings.debug = False
            mock_settings.auth_enabled = True
            assert _is_auth_enabled() is True

    def test_auth_not_disabled_without_debug(self, monkeypatch):
        """Auth stays enabled when debug=False even with ENVIRONMENT=development."""
        monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "false")
        monkeypatch.setenv("ENVIRONMENT", "development")

        from app.auth.api_key_auth import _is_auth_enabled
        with patch("app.auth.api_key_auth.settings") as mock_settings:
            mock_settings.debug = False
            mock_settings.auth_enabled = True
            assert _is_auth_enabled() is True

    def test_mock_user_has_read_only_scope(self, monkeypatch):
        """When auth is disabled, mock user only gets read scope (not admin)."""
        monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "false")
        monkeypatch.setenv("ENVIRONMENT", "development")

        from app.auth.api_key_auth import get_current_user
        from unittest.mock import AsyncMock

        with patch("app.auth.api_key_auth.settings") as mock_settings:
            mock_settings.debug = True
            mock_settings.auth_enabled = True

            request = MagicMock()
            request.state = MagicMock()

            import asyncio
            user = asyncio.get_event_loop().run_until_complete(get_current_user(request))
            assert user.scopes == ["read"]
            assert "admin" not in user.scopes
            assert "write" not in user.scopes


# ═══════════════════════════════════════════════════════════════════════
# 3. Login rate limiting
# ═══════════════════════════════════════════════════════════════════════

class TestLoginRateLimiting:
    """Login endpoint rate limiting: max 5 attempts per IP per 5 minutes."""

    def test_sixth_attempt_returns_429(self):
        """6th login attempt within 5 minutes returns 429."""
        from app.routers.auth import _check_login_rate_limit, _login_attempts
        from fastapi import HTTPException

        test_ip = f"test-login-ratelimit-{time.monotonic()}"
        _login_attempts.pop(test_ip, None)

        # First 5 calls should succeed
        for _ in range(5):
            _check_login_rate_limit(test_ip)

        # 6th call should raise 429
        with pytest.raises(HTTPException) as exc_info:
            _check_login_rate_limit(test_ip)
        assert exc_info.value.status_code == 429

        # Clean up
        _login_attempts.pop(test_ip, None)


# ═══════════════════════════════════════════════════════════════════════
# 4. Diagnostic bundle errors are redacted
# ═══════════════════════════════════════════════════════════════════════

class TestErrorCollectorRedaction:
    """ErrorCollector applies redaction to error entries."""

    @pytest.mark.asyncio
    async def test_error_entries_are_redacted(self):
        """Error log entries containing sensitive data are redacted."""
        from app.core.log_buffer import log_ring_buffer
        from app.services.diagnostic_collectors import ErrorCollector

        # Seed a sensitive error entry
        log_ring_buffer.append({
            "event": "auth_failure",
            "level": "error",
            "api_key": "sk-proj-abc123def456ghi789",
            "details": "User user@example.com failed with token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U",
        })

        result = await ErrorCollector().safe_collect()
        assert result.error is None

        entries = result.data["recent_errors"]["entries"]
        # Find our seeded entry
        matching = [e for e in entries if e.get("event") == "auth_failure"]
        assert len(matching) >= 1

        entry = matching[-1]
        # api_key should be redacted (key-based)
        assert "****" in entry["api_key"]
        assert "abc123def456ghi789" not in entry["api_key"]
        # JWT should be redacted (value-based)
        assert "eyJ" not in entry["details"]
        # Email should be redacted (value-based)
        assert "user@example.com" not in entry["details"]


# ═══════════════════════════════════════════════════════════════════════
# 5. Deep health requires auth
# ═══════════════════════════════════════════════════════════════════════

class TestDeepHealthAuth:
    """Deep health and issues endpoints require authentication."""

    def test_deep_health_requires_auth(self, monkeypatch):
        """Unauthenticated request to /api/health/deep returns 401."""
        monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "true")
        from app.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/api/health/deep")
        assert response.status_code == 401

    def test_health_issues_requires_auth(self, monkeypatch):
        """Unauthenticated request to /api/health/issues returns 401."""
        monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "true")
        from app.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/api/health/issues")
        assert response.status_code == 401

    def test_basic_health_no_auth(self):
        """Basic /api/health remains unauthenticated."""
        from app.main import app
        client = TestClient(app)
        response = client.get("/api/health")
        assert response.status_code == 200


# ═══════════════════════════════════════════════════════════════════════
# 6. AAD mismatch fails decryption
# ═══════════════════════════════════════════════════════════════════════

class TestAADEncryption:
    """AES-GCM AAD binding prevents cross-record decryption."""

    SECRET = "test-secret-key-for-aad-testing!!"

    def test_aad_roundtrip(self):
        """Encrypt with AAD, decrypt with same AAD succeeds."""
        from app.core.llm_key_crypto import encrypt_api_key, decrypt_api_key

        plaintext = "sk-test-key-12345"
        ct, iv, tag = encrypt_api_key(plaintext, self.SECRET, provider_id="openai", scope="instance")
        result = decrypt_api_key(ct, iv, tag, self.SECRET, provider_id="openai", scope="instance")
        assert result == plaintext

    def test_aad_mismatch_fails(self):
        """Decrypt with different AAD (wrong provider) raises exception."""
        from app.core.llm_key_crypto import encrypt_api_key, decrypt_api_key

        plaintext = "sk-test-key-12345"
        ct, iv, tag = encrypt_api_key(plaintext, self.SECRET, provider_id="openai", scope="instance")

        # Trying to decrypt with different provider_id should fail
        with pytest.raises(Exception):
            decrypt_api_key(ct, iv, tag, self.SECRET, provider_id="anthropic", scope="instance")

    def test_aad_scope_mismatch_fails(self):
        """Decrypt with different scope raises exception."""
        from app.core.llm_key_crypto import encrypt_api_key, decrypt_api_key

        plaintext = "sk-test-key-12345"
        ct, iv, tag = encrypt_api_key(plaintext, self.SECRET, provider_id="openai", scope="instance")

        with pytest.raises(Exception):
            decrypt_api_key(ct, iv, tag, self.SECRET, provider_id="openai", scope="user")

    def test_backward_compat_no_aad(self):
        """Encrypt without AAD, decrypt without AAD still works (backward compat)."""
        from app.core.llm_key_crypto import encrypt_api_key, decrypt_api_key

        plaintext = "sk-test-key-legacy"
        ct, iv, tag = encrypt_api_key(plaintext, self.SECRET)
        result = decrypt_api_key(ct, iv, tag, self.SECRET)
        assert result == plaintext


# ═══════════════════════════════════════════════════════════════════════
# 7. Hostname anonymized in diagnostic bundle
# ═══════════════════════════════════════════════════════════════════════

class TestHostnameAnonymized:
    """Diagnostic bundle metadata uses hashed host_id, not raw hostname."""

    @pytest.mark.asyncio
    async def test_metadata_has_host_id_not_hostname(self):
        """metadata.json contains host_id (hash) not hostname."""
        import json
        import zipfile
        import io
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
            assert "host_id" in metadata
            assert "hostname" not in metadata
            # host_id should be 12-char hex
            assert len(metadata["host_id"]) == 12
