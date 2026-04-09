"""
Tests for REST API endpoints — /api/v1/ext/*.

HTTP status codes (401/403/404/408/429/500/503), auth header parsing, OpenAPI.

BQ-MCP-RAG Phase 1.
"""


import pytest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.config import settings

# Enable connectivity for all ext_api tests
settings.connectivity_enabled = True


def _get_test_client():
    """Create a test client with connectivity enabled."""
    from app.main import create_app
    app = create_app()
    return TestClient(app)


@pytest.fixture
def client():
    settings.connectivity_enabled = True
    # Reset the orchestrator singleton so rate limiter starts fresh
    import app.services.query_orchestrator as _qo
    _qo._orchestrator = None
    return _get_test_client()


@pytest.fixture
def valid_token_header():
    """Create a real token and return the Authorization header."""
    from app.services.connectivity_token_service import create_token
    raw, _ = create_token(label="REST Test", max_tokens=100)
    return {"Authorization": f"Bearer {raw}"}


# ---------------------------------------------------------------------------
# Health endpoint (no auth)
# ---------------------------------------------------------------------------

class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        response = client.get("/api/v1/ext/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "connectivity_enabled" in data
        assert data["version"] == "1.0"

    def test_health_no_auth_required(self, client):
        # No Authorization header
        response = client.get("/api/v1/ext/health")
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# Auth header parsing
# ---------------------------------------------------------------------------

class TestAuthHeaderParsing:
    def test_missing_auth_header(self, client):
        response = client.get("/api/v1/ext/datasets")
        assert response.status_code == 401
        data = response.json()
        assert data["error"]["code"] == "auth_invalid"

    def test_malformed_auth_header(self, client):
        response = client.get(
            "/api/v1/ext/datasets",
            headers={"Authorization": "Token abc123"},
        )
        assert response.status_code == 401
        data = response.json()
        assert data["error"]["code"] == "auth_invalid"

    def test_invalid_token_format(self, client):
        response = client.get(
            "/api/v1/ext/datasets",
            headers={"Authorization": "Bearer not_a_valid_token"},
        )
        assert response.status_code == 401

    def test_unknown_token(self, client):
        response = client.get(
            "/api/v1/ext/datasets",
            headers={"Authorization": "Bearer vzmcp_zZzZzZzZ_abcdef0123456789abcdef0123456789"},
        )
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# Datasets endpoint
# ---------------------------------------------------------------------------

class TestDatasetsEndpoint:
    def setup_method(self):
        settings.connectivity_enabled = True

    def test_list_datasets_authenticated(self, client, valid_token_header):
        with patch("app.services.qdrant_service.get_qdrant_service") as mock_qdrant:
            mock_qdrant.return_value.list_collections.return_value = []
            response = client.get("/api/v1/ext/datasets", headers=valid_token_header)
        assert response.status_code == 200
        data = response.json()
        assert "datasets" in data
        assert "count" in data

    def test_list_datasets_revoked_token(self, client):
        from app.services.connectivity_token_service import create_token, revoke_token
        raw, created = create_token(label="Revoke REST Test", max_tokens=100)
        revoke_token(created.id)
        response = client.get(
            "/api/v1/ext/datasets",
            headers={"Authorization": f"Bearer {raw}"},
        )
        assert response.status_code == 401
        assert response.json()["error"]["code"] == "auth_revoked"


# ---------------------------------------------------------------------------
# Schema endpoint
# ---------------------------------------------------------------------------

class TestSchemaEndpoint:
    def test_schema_nonexistent_dataset(self, client, valid_token_header):
        settings.connectivity_enabled = True
        response = client.get(
            "/api/v1/ext/datasets/nonexistent_id/schema",
            headers=valid_token_header,
        )
        assert response.status_code == 404
        data = response.json()
        assert data["error"]["code"] == "dataset_not_found"


# ---------------------------------------------------------------------------
# SQL endpoint
# ---------------------------------------------------------------------------

class TestSQLEndpoint:
    def setup_method(self):
        settings.connectivity_enabled = True

    def test_sql_missing_auth(self, client):
        response = client.post(
            "/api/v1/ext/sql",
            json={"sql": "SELECT 1"},
        )
        assert response.status_code == 401

    def test_sql_too_long_pydantic(self, client, valid_token_header):
        """SQL exceeding Pydantic max_length gets 422."""
        long_sql = "SELECT " + "x" * 5000
        response = client.post(
            "/api/v1/ext/sql",
            json={"sql": long_sql},
            headers=valid_token_header,
        )
        assert response.status_code == 422  # Pydantic validation

    def test_sql_forbidden_statement(self, client, valid_token_header):
        response = client.post(
            "/api/v1/ext/sql",
            json={"sql": "DROP TABLE users"},
            headers=valid_token_header,
        )
        # 400 forbidden_sql (sandbox blocks DROP)
        assert response.status_code == 400


# ---------------------------------------------------------------------------
# Search endpoint
# ---------------------------------------------------------------------------

class TestSearchEndpoint:
    def test_search_missing_auth(self, client):
        response = client.post(
            "/api/v1/ext/search",
            json={"query": "test query"},
        )
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# Error envelope structure
# ---------------------------------------------------------------------------

class TestErrorEnvelope:
    def test_error_has_correct_structure(self, client):
        """All error responses must have the standard envelope."""
        response = client.get("/api/v1/ext/datasets")
        assert response.status_code == 401
        data = response.json()
        assert "error" in data
        assert "code" in data["error"]
        assert "message" in data["error"]
        assert "details" in data["error"]
        assert "request_id" in data
        assert data["request_id"].startswith("ext-")

    def test_error_request_id_unique(self, client):
        r1 = client.get("/api/v1/ext/datasets")
        r2 = client.get("/api/v1/ext/datasets")
        assert r1.json()["request_id"] != r2.json()["request_id"]


# ---------------------------------------------------------------------------
# Pre-auth IP blocking (Fix 2 — Gate 3)
# ---------------------------------------------------------------------------

class TestPreAuthIPBlocking:
    """Verify that blocked IPs get 429 BEFORE any token validation occurs."""

    def test_ip_blocked_after_auth_failures(self, client):
        """Send multiple invalid-token requests from same IP; verify IP gets blocked."""
        # The default per_ip_auth_fail_limit is 5
        # Send 6 requests with invalid tokens to exceed the threshold
        for i in range(6):
            client.get(
                "/api/v1/ext/datasets",
                headers={"Authorization": "Bearer not_a_valid_token"},
            )

        # The next request should be blocked at the IP level (429)
        # even before token validation
        response = client.get(
            "/api/v1/ext/datasets",
            headers={"Authorization": "Bearer not_a_valid_token"},
        )
        assert response.status_code == 429
        assert response.json()["error"]["code"] == "ip_blocked"

    def test_ip_blocked_returns_429_on_all_endpoints(self, client):
        """Once blocked, all endpoints should return 429."""
        # Exhaust auth failure limit
        for i in range(6):
            client.get(
                "/api/v1/ext/datasets",
                headers={"Authorization": "Bearer invalid_token"},
            )

        # Try different endpoints — all should be blocked
        for endpoint in ["/api/v1/ext/datasets", "/api/v1/ext/datasets/some_id/schema"]:
            response = client.get(
                endpoint,
                headers={"Authorization": "Bearer invalid_token"},
            )
            assert response.status_code == 429
            assert response.json()["error"]["code"] == "ip_blocked"

    def test_revoked_token_does_not_count_as_auth_failure(self, client):
        """auth_revoked should NOT increment the IP failure counter."""
        from app.services.connectivity_token_service import create_token, revoke_token

        # Create and revoke several tokens, send requests with them
        for _ in range(6):
            raw, created = create_token(label="Revoke test", max_tokens=100)
            revoke_token(created.id)
            client.get(
                "/api/v1/ext/datasets",
                headers={"Authorization": f"Bearer {raw}"},
            )

        # IP should NOT be blocked (revoked tokens aren't attack indicators)
        response = client.get(
            "/api/v1/ext/datasets",
            headers={"Authorization": "Bearer not_a_valid_token"},
        )
        # Should get 401 (auth_invalid), not 429 (ip_blocked)
        assert response.status_code == 401
