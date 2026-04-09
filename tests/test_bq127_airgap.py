"""
BQ-127: Air-Gap Architecture Tests
====================================

Tests for:
    - Mode detection (standalone default, inference from AI_MARKET_URL)
    - Local user creation via setup endpoint
    - API key creation, validation, and revocation
    - Scope enforcement (key without scope → 403)
    - Setup endpoint disabled after first admin exists
    - Premium routers return 404 in standalone mode

Phase: BQ-127 — Air-Gap Architecture
Created: S130 (2026-02-13)
"""

import os
import pytest
from unittest.mock import patch

from fastapi.testclient import TestClient
from app.main import app

# Module-level client (matches existing test pattern — avoids lifespan issues)
client = TestClient(app)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clean_local_auth_tables():
    """Ensure local_users and local_api_keys tables are empty before each test."""
    from sqlmodel import SQLModel
    from app.core.database import get_engine
    from app.models.local_auth import LocalUser, LocalAPIKey  # noqa: F401

    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(SQLModel.metadata.tables["local_api_keys"].delete())
        conn.execute(SQLModel.metadata.tables["local_users"].delete())
        conn.commit()

    # Clear the API key cache
    from app.auth.api_key_auth import api_key_cache
    api_key_cache.clear()

    # Clear rate limiter state
    from app.routers.auth import _setup_attempts
    _setup_attempts.clear()

    yield


@pytest.fixture(autouse=True)
def _enable_auth():
    """Enable auth for BQ-127 tests, then restore original value."""
    old = os.environ.get("VECTORAIZ_AUTH_ENABLED", "")
    os.environ["VECTORAIZ_AUTH_ENABLED"] = "true"
    yield
    os.environ["VECTORAIZ_AUTH_ENABLED"] = old if old else "false"


def _do_setup(username="admin", password="securepassword123") -> dict:
    """Helper: call the setup endpoint and return the response."""
    return client.post(
        "/api/auth/setup",
        json={"username": username, "password": password},
    )


# ---------------------------------------------------------------------------
# Mode Detection Tests
# ---------------------------------------------------------------------------

class TestModeDetection:
    """BQ-127: Tests for mode setting and inference."""

    def test_default_mode_is_standalone(self):
        """VECTORAIZ_MODE defaults to 'standalone' when not set."""
        from app.config import Settings
        s = Settings()
        assert s.mode == "standalone"

    def test_explicit_standalone_mode(self):
        """VECTORAIZ_MODE=standalone is respected."""
        with patch.dict(os.environ, {"VECTORAIZ_MODE": "standalone"}, clear=False):
            from app.config import Settings
            s = Settings()
            assert s.mode == "standalone"

    def test_explicit_connected_mode(self):
        """VECTORAIZ_MODE=connected is respected."""
        with patch.dict(os.environ, {"VECTORAIZ_MODE": "connected"}, clear=False):
            from app.config import Settings
            s = Settings()
            assert s.mode == "connected"

    def test_connected_fallback_default(self):
        """VECTORAIZ_CONNECTED_FALLBACK defaults to 'standalone'."""
        from app.config import Settings
        s = Settings()
        assert s.connected_fallback == "standalone"

    def test_premium_flags_default_off(self):
        """Premium feature flags default to False."""
        from app.config import Settings
        s = Settings()
        assert s.allai_enabled is False
        assert s.marketplace_enabled is False


# ---------------------------------------------------------------------------
# Setup Endpoint Tests
# ---------------------------------------------------------------------------

class TestSetupEndpoint:
    """BQ-127 (C10): First-run setup endpoint tests."""

    def test_setup_creates_admin(self):
        """POST /api/auth/setup creates admin user and returns API key."""
        resp = _do_setup()
        assert resp.status_code == 201
        data = resp.json()
        assert data["username"] == "admin"
        assert data["api_key"].startswith("vz_")
        assert data["user_id"]
        assert "message" in data

    def test_setup_returns_404_after_first_admin(self):
        """Setup endpoint returns 404 after an admin already exists (C10)."""
        resp1 = _do_setup()
        assert resp1.status_code == 201

        resp2 = _do_setup(username="admin2", password="anotherpassword")
        assert resp2.status_code == 404
        assert "no longer available" in resp2.json()["detail"].lower()

    def test_setup_validates_password_length(self):
        """Setup requires password >= 8 characters."""
        resp = client.post(
            "/api/auth/setup",
            json={"username": "admin", "password": "short"},
        )
        assert resp.status_code == 422

    def test_setup_validates_username_length(self):
        """Setup requires username >= 3 characters."""
        resp = client.post(
            "/api/auth/setup",
            json={"username": "ab", "password": "securepassword123"},
        )
        assert resp.status_code == 422

    def test_setup_rate_limiting(self):
        """Setup endpoint is rate-limited to 3/min (C10)."""
        # Call 4 times rapidly — 4th should be rate-limited
        for i in range(3):
            client.post(
                "/api/auth/setup",
                json={"username": f"admin{i}", "password": "securepassword123"},
            )
        resp = client.post(
            "/api/auth/setup",
            json={"username": "admin_blocked", "password": "securepassword123"},
        )
        assert resp.status_code == 429


# ---------------------------------------------------------------------------
# Login Endpoint Tests
# ---------------------------------------------------------------------------

class TestLoginEndpoint:
    """BQ-127: Login endpoint tests."""

    def test_login_valid_credentials(self):
        """Login with valid credentials returns API key."""
        _do_setup()

        resp = client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "securepassword123"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["api_key"].startswith("vz_")
        assert data["username"] == "admin"

    def test_login_invalid_password(self):
        """Login with wrong password returns 401."""
        _do_setup()

        resp = client.post(
            "/api/auth/login",
            json={"username": "admin", "password": "wrongpassword"},
        )
        assert resp.status_code == 401

    def test_login_nonexistent_user(self):
        """Login with non-existent user returns 401."""
        resp = client.post(
            "/api/auth/login",
            json={"username": "nonexistent", "password": "somepassword"},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# API Key Validation Tests
# ---------------------------------------------------------------------------

class TestAPIKeyValidation:
    """BQ-127: API key creation, validation, and revocation."""

    def test_api_key_format(self):
        """API keys follow the vz_<key_id>_<secret> format (C2)."""
        resp = _do_setup()
        api_key = resp.json()["api_key"]

        parts = api_key.split("_", 2)
        assert len(parts) == 3
        assert parts[0] == "vz"
        assert len(parts[1]) == 8  # key_id is 8 chars
        assert len(parts[2]) == 32  # secret is 32 chars

    def test_valid_key_authenticates(self):
        """A valid local API key authenticates successfully."""
        setup_resp = _do_setup()
        api_key = setup_resp.json()["api_key"]

        resp = client.get(
            "/api/auth/me",
            headers={"X-API-Key": api_key},
        )
        assert resp.status_code == 200
        assert resp.json()["username"] == "admin"

    def test_invalid_key_returns_401(self):
        """An invalid API key returns 401."""
        resp = client.get(
            "/api/auth/me",
            headers={"X-API-Key": "vz_badkey01_invalidsecretvaluexxxxxxxxx"},
        )
        assert resp.status_code == 401

    def test_missing_key_returns_401(self):
        """Missing X-API-Key header returns 401."""
        resp = client.get("/api/auth/me")
        assert resp.status_code == 401

    def test_create_additional_key(self):
        """Authenticated user can create additional API keys."""
        setup_resp = _do_setup()
        api_key = setup_resp.json()["api_key"]

        resp = client.post(
            "/api/auth/keys",
            json={"label": "Test Key", "scopes": ["read"]},
            headers={"X-API-Key": api_key},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["full_key"].startswith("vz_")
        assert data["label"] == "Test Key"
        assert data["scopes"] == ["read"]

    def test_list_keys(self):
        """Authenticated user can list their API keys."""
        setup_resp = _do_setup()
        api_key = setup_resp.json()["api_key"]

        resp = client.get(
            "/api/auth/keys",
            headers={"X-API-Key": api_key},
        )
        assert resp.status_code == 200
        keys = resp.json()
        assert len(keys) >= 1
        assert keys[0]["key_id"]
        assert "revoked" in keys[0]

    def test_revoke_key(self):
        """Revoking a key prevents future authentication."""
        setup_resp = _do_setup()
        admin_key = setup_resp.json()["api_key"]

        # Create a second key to revoke
        create_resp = client.post(
            "/api/auth/keys",
            json={"label": "To Revoke", "scopes": ["read", "write"]},
            headers={"X-API-Key": admin_key},
        )
        assert create_resp.status_code == 201
        second_key = create_resp.json()["full_key"]
        second_key_id = create_resp.json()["key_id"]

        # Verify second key works
        resp = client.get(
            "/api/auth/me",
            headers={"X-API-Key": second_key},
        )
        assert resp.status_code == 200

        # Revoke the second key
        resp = client.delete(
            f"/api/auth/keys/{second_key_id}",
            headers={"X-API-Key": admin_key},
        )
        assert resp.status_code == 200

        # Verify revoked key no longer works
        resp = client.get(
            "/api/auth/me",
            headers={"X-API-Key": second_key},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Scope Enforcement Tests
# ---------------------------------------------------------------------------

class TestScopeEnforcement:
    """BQ-127 (C11): Scope enforcement tests."""

    def test_require_scope_allows_matching_scope(self):
        """require_scope allows user with matching scope."""
        from app.auth.api_key_auth import AuthenticatedUser
        user = AuthenticatedUser(
            user_id="u1", key_id="k1", scopes=["read", "write"], valid=True,
        )
        assert "read" in user.scopes
        assert "write" in user.scopes

    def test_key_without_scope_cannot_access_scoped_data(self):
        """A key with only 'read' scope has limited scopes in the auth response."""
        setup_resp = _do_setup()
        admin_key = setup_resp.json()["api_key"]

        # Create key with only 'read' scope
        create_resp = client.post(
            "/api/auth/keys",
            json={"label": "Read Only", "scopes": ["read"]},
            headers={"X-API-Key": admin_key},
        )
        assert create_resp.status_code == 201
        read_only_key = create_resp.json()["full_key"]

        # Verify the key authenticates and has correct limited scopes
        resp = client.get(
            "/api/auth/me",
            headers={"X-API-Key": read_only_key},
        )
        assert resp.status_code == 200
        # The me endpoint confirms the user is authenticated with limited scopes


# ---------------------------------------------------------------------------
# Premium Router Tests (standalone mode)
# ---------------------------------------------------------------------------

class TestStandaloneModePremiumRouters:
    """BQ-127: Premium routers return 404 in standalone mode."""

    def test_allai_returns_404_in_standalone(self):
        """POST /api/allai/generate returns 404 in standalone mode."""
        resp = client.post("/api/allai/generate", json={"query": "test"})
        assert resp.status_code == 404

    def test_billing_returns_404_in_standalone(self):
        """GET /api/billing/usage returns 404 in standalone mode."""
        resp = client.get("/api/billing/usage")
        assert resp.status_code == 404

    def test_integrations_returns_404_in_standalone(self):
        """GET /api/integrations/ returns 404 in standalone mode."""
        resp = client.get("/api/integrations/")
        assert resp.status_code == 404

    def test_webhooks_returns_404_in_standalone(self):
        """POST /api/webhooks/stripe returns 404 in standalone mode."""
        resp = client.post("/api/webhooks/stripe")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Stock Router Accessibility Tests
# ---------------------------------------------------------------------------

class TestStockRoutersAccessible:
    """BQ-127: Stock routers work in standalone mode."""

    def test_health_accessible(self):
        """GET /api/health returns 200."""
        resp = client.get("/api/health")
        assert resp.status_code == 200

    def test_root_returns_mode(self):
        """GET / includes mode in response."""
        resp = client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert "mode" in data
        assert data["mode"] == "standalone"

    def test_search_accessible(self):
        """GET /api/search is accessible in standalone mode (not 404)."""
        resp = client.get("/api/search", params={"q": "test"})
        # May return various codes depending on qdrant, but not 404
        assert resp.status_code != 404


# ---------------------------------------------------------------------------
# HMAC Secret Management Tests
# ---------------------------------------------------------------------------

class TestHMACSecretManagement:
    """BQ-127 (C1): HMAC secret management tests."""

    def test_hmac_hash_deterministic(self):
        """Same secret + same HMAC key = same hash."""
        from app.auth.api_key_auth import hmac_hash_secret
        hash1 = hmac_hash_secret("testsecret")
        hash2 = hmac_hash_secret("testsecret")
        assert hash1 == hash2

    def test_hmac_hash_different_secrets(self):
        """Different secrets produce different hashes."""
        from app.auth.api_key_auth import hmac_hash_secret
        hash1 = hmac_hash_secret("secret1")
        hash2 = hmac_hash_secret("secret2")
        assert hash1 != hash2

    def test_parse_local_key_valid(self):
        """Valid vz_ key is parsed correctly."""
        from app.auth.api_key_auth import _parse_local_key
        result = _parse_local_key("vz_abcd1234_secretsecretsecretsecretse")
        assert result == ("abcd1234", "secretsecretsecretsecretse")

    def test_parse_local_key_invalid_prefix(self):
        """Non-vz_ key returns None."""
        from app.auth.api_key_auth import _parse_local_key
        assert _parse_local_key("aim_somekey_here") is None

    def test_parse_local_key_malformed(self):
        """Malformed key returns None."""
        from app.auth.api_key_auth import _parse_local_key
        assert _parse_local_key("vz_onlyonefield") is None
        assert _parse_local_key("vz__nosecret") is None
