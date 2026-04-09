"""
BQ-MCP-RAG Phase 3: Connectivity Management API Tests.

Tests for /api/connectivity/* endpoints (Settings UI backend).

PHASE: BQ-MCP-RAG Phase 3 Tests
CREATED: S136
"""

import os
import pytest

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clean_tables():
    """Clean local auth + connectivity token tables before each test."""
    from sqlmodel import SQLModel
    from app.core.database import get_engine
    from app.models.local_auth import LocalUser, LocalAPIKey  # noqa: F401
    from app.models.connectivity import ConnectivityTokenRecord  # noqa: F401

    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(SQLModel.metadata.tables["local_api_keys"].delete())
        conn.execute(SQLModel.metadata.tables["local_users"].delete())
        conn.execute(SQLModel.metadata.tables["connectivity_tokens"].delete())
        conn.commit()

    from app.auth.api_key_auth import api_key_cache
    api_key_cache.clear()

    from app.routers.auth import _setup_attempts
    _setup_attempts.clear()

    yield

    with engine.connect() as conn:
        conn.execute(SQLModel.metadata.tables["connectivity_tokens"].delete())
        conn.commit()


@pytest.fixture(autouse=True)
def _enable_auth():
    old = os.environ.get("VECTORAIZ_AUTH_ENABLED", "")
    os.environ["VECTORAIZ_AUTH_ENABLED"] = "true"
    yield
    os.environ["VECTORAIZ_AUTH_ENABLED"] = old if old else "false"


def _setup_and_get_key() -> str:
    """Create admin user and return API key."""
    resp = client.post("/api/auth/setup", json={"username": "admin", "password": "securepassword123"})
    assert resp.status_code == 201
    return resp.json()["api_key"]


def _auth_headers(api_key: str) -> dict:
    return {"X-API-Key": api_key}


# =====================================================================
# Status endpoint
# =====================================================================

class TestConnectivityStatus:
    def test_status_requires_auth(self):
        resp = client.get("/api/connectivity/status")
        assert resp.status_code in (401, 403)

    def test_status_returns_data(self):
        key = _setup_and_get_key()
        resp = client.get("/api/connectivity/status", headers=_auth_headers(key))
        assert resp.status_code == 200
        data = resp.json()
        assert "enabled" in data
        assert "tokens" in data
        assert "token_count" in data
        assert "active_token_count" in data
        assert "metrics" in data


# =====================================================================
# Enable / Disable
# =====================================================================

class TestConnectivityToggle:
    def test_enable(self):
        key = _setup_and_get_key()
        from app.config import settings
        original = settings.connectivity_enabled
        try:
            settings.connectivity_enabled = False
            resp = client.post("/api/connectivity/enable", headers=_auth_headers(key))
            assert resp.status_code == 200
            data = resp.json()
            assert data["enabled"] is True
            assert data["changed"] is True
            assert settings.connectivity_enabled is True
        finally:
            settings.connectivity_enabled = original

    def test_disable(self):
        key = _setup_and_get_key()
        from app.config import settings
        original = settings.connectivity_enabled
        try:
            settings.connectivity_enabled = True
            resp = client.post("/api/connectivity/disable", headers=_auth_headers(key))
            assert resp.status_code == 200
            data = resp.json()
            assert data["enabled"] is False
            assert data["changed"] is True
            assert settings.connectivity_enabled is False
        finally:
            settings.connectivity_enabled = original

    def test_enable_already_enabled(self):
        key = _setup_and_get_key()
        from app.config import settings
        original = settings.connectivity_enabled
        try:
            settings.connectivity_enabled = True
            resp = client.post("/api/connectivity/enable", headers=_auth_headers(key))
            assert resp.status_code == 200
            assert resp.json()["changed"] is False
        finally:
            settings.connectivity_enabled = original


# =====================================================================
# Token CRUD
# =====================================================================

class TestTokenCRUD:
    def test_list_tokens_empty(self):
        key = _setup_and_get_key()
        resp = client.get("/api/connectivity/tokens", headers=_auth_headers(key))
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["tokens"] == []

    def test_create_token(self):
        key = _setup_and_get_key()
        resp = client.post(
            "/api/connectivity/tokens",
            headers=_auth_headers(key),
            json={"label": "Test Token"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["label"] == "Test Token"
        assert data["token"].startswith("vzmcp_")
        assert data["token_id"]
        assert data["secret_last4"]
        assert "warning" in data

    def test_create_token_with_scopes(self):
        key = _setup_and_get_key()
        resp = client.post(
            "/api/connectivity/tokens",
            headers=_auth_headers(key),
            json={"label": "Limited", "scopes": ["ext:search"]},
        )
        assert resp.status_code == 201
        assert resp.json()["scopes"] == ["ext:search"]

    def test_create_token_invalid_scope(self):
        key = _setup_and_get_key()
        resp = client.post(
            "/api/connectivity/tokens",
            headers=_auth_headers(key),
            json={"label": "Bad", "scopes": ["ext:admin"]},
        )
        assert resp.status_code == 400

    def test_list_tokens_after_create(self):
        key = _setup_and_get_key()
        client.post("/api/connectivity/tokens", headers=_auth_headers(key), json={"label": "Token 1"})
        client.post("/api/connectivity/tokens", headers=_auth_headers(key), json={"label": "Token 2"})
        resp = client.get("/api/connectivity/tokens", headers=_auth_headers(key))
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2

    def test_revoke_token(self):
        key = _setup_and_get_key()
        create_resp = client.post(
            "/api/connectivity/tokens",
            headers=_auth_headers(key),
            json={"label": "To Revoke"},
        )
        token_id = create_resp.json()["token_id"]

        resp = client.delete(
            f"/api/connectivity/tokens/{token_id}",
            headers=_auth_headers(key),
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["revoked"] is True
        assert data["token_id"] == token_id

    def test_revoke_nonexistent_token(self):
        key = _setup_and_get_key()
        resp = client.delete(
            "/api/connectivity/tokens/noSuchId",
            headers=_auth_headers(key),
        )
        assert resp.status_code == 404

    def test_revoke_already_revoked(self):
        key = _setup_and_get_key()
        create_resp = client.post(
            "/api/connectivity/tokens",
            headers=_auth_headers(key),
            json={"label": "Double Revoke"},
        )
        token_id = create_resp.json()["token_id"]
        client.delete(f"/api/connectivity/tokens/{token_id}", headers=_auth_headers(key))
        resp = client.delete(f"/api/connectivity/tokens/{token_id}", headers=_auth_headers(key))
        assert resp.status_code == 400


# =====================================================================
# Setup generation
# =====================================================================

class TestSetupGeneration:
    def test_generate_claude_desktop(self):
        key = _setup_and_get_key()
        resp = client.post(
            "/api/connectivity/setup",
            headers=_auth_headers(key),
            json={"platform": "claude_desktop", "token": "vzmcp_test1234_abcdef0123456789abcdef0123456789"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["platform"] == "claude_desktop"
        assert "steps" in data

    def test_generate_unsupported_platform(self):
        key = _setup_and_get_key()
        resp = client.post(
            "/api/connectivity/setup",
            headers=_auth_headers(key),
            json={"platform": "unsupported_xyz"},
        )
        assert resp.status_code == 400
        assert "unsupported" in resp.json()["detail"].lower()

    def test_generate_all_platforms(self):
        key = _setup_and_get_key()
        from app.services.connectivity_setup_generator import SUPPORTED_PLATFORMS
        for platform in SUPPORTED_PLATFORMS:
            resp = client.post(
                "/api/connectivity/setup",
                headers=_auth_headers(key),
                json={"platform": platform, "token": "vzmcp_test1234_abcdef0123456789abcdef0123456789"},
            )
            assert resp.status_code == 200, f"Failed for platform {platform}"


# =====================================================================
# Token test endpoint
# =====================================================================

class TestTokenTest:
    def test_token_test_no_path_param(self):
        """POST /api/connectivity/test works without token_id in path."""
        key = _setup_and_get_key()
        from app.config import settings
        original = settings.connectivity_enabled
        try:
            settings.connectivity_enabled = True
            create_resp = client.post(
                "/api/connectivity/tokens",
                headers=_auth_headers(key),
                json={"label": "Test Me"},
            )
            raw_token = create_resp.json()["token"]

            resp = client.post(
                "/api/connectivity/test",
                headers=_auth_headers(key),
                json={"token": raw_token},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["connectivity_enabled"] is True
            assert data["token_valid"] is True
            assert data["token_label"] == "Test Me"
        finally:
            settings.connectivity_enabled = original

    def test_token_test_old_path_returns_404_or_405(self):
        """Old /test/{token_id} path no longer exists."""
        key = _setup_and_get_key()
        resp = client.post(
            "/api/connectivity/test/some-token-id",
            headers=_auth_headers(key),
            json={"token": "vzmcp_fake_abcdef0123456789abcdef0123456789"},
        )
        # Should be 404 (no such route) or 405
        assert resp.status_code in (404, 405)

    def test_token_test_invalid_token(self):
        """Error response when token is invalid."""
        key = _setup_and_get_key()
        from app.config import settings
        original = settings.connectivity_enabled
        try:
            settings.connectivity_enabled = True
            resp = client.post(
                "/api/connectivity/test",
                headers=_auth_headers(key),
                json={"token": "vzmcp_bad_0000000000000000000000000000000000"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["token_valid"] is False
            assert data["error"] is not None
        finally:
            settings.connectivity_enabled = original

    def test_token_test_connectivity_disabled(self):
        """Returns error when connectivity is disabled."""
        key = _setup_and_get_key()
        from app.config import settings
        original = settings.connectivity_enabled
        try:
            settings.connectivity_enabled = False
            resp = client.post(
                "/api/connectivity/test",
                headers=_auth_headers(key),
                json={"token": "vzmcp_any_abcdef0123456789abcdef0123456789"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["connectivity_enabled"] is False
            assert data["token_valid"] is False
            assert "disabled" in data["error"].lower()
        finally:
            settings.connectivity_enabled = original
