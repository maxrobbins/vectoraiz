"""
BQ-VZ-SHARED-SEARCH: Access code auth tests (Mandate M5)

Tests: weak code rejected, rate limit (6th attempt -> 429),
session invalidation on code rotation.
"""

import pytest
from fastapi.testclient import TestClient

from app.models.portal import (
    AccessCodeValidator,
    save_portal_config,
    reset_portal_config_cache,
)
from app.schemas.portal import PortalConfig, DatasetPortalConfig, PortalTier


@pytest.fixture(autouse=True)
def reset_portal(tmp_path, monkeypatch):
    """Reset portal config and rate limits for each test."""
    monkeypatch.setattr("app.models.portal._PORTAL_CONFIG_PATH", tmp_path / "portal_config.json")
    monkeypatch.setattr("app.middleware.portal_auth._PORTAL_JWT_SECRET_PATH", tmp_path / "portal_jwt.key")
    monkeypatch.setattr("app.middleware.portal_auth._portal_jwt_secret", None)
    reset_portal_config_cache()
    AccessCodeValidator.clear_rate_limits()
    yield
    reset_portal_config_cache()
    AccessCodeValidator.clear_rate_limits()


@pytest.fixture
def client():
    from app.main import app
    with TestClient(app) as c:
        yield c


@pytest.fixture
def code_tier_portal():
    """Enable portal in code tier with a known access code."""
    code = "SecureCode1"
    config = PortalConfig(
        enabled=True,
        tier=PortalTier.code,
        base_url="http://localhost:8100",
        access_code_hash=AccessCodeValidator.hash_code(code),
        datasets={
            "test-dataset": DatasetPortalConfig(
                portal_visible=True,
                display_columns=["name"],
                max_results=50,
            ),
        },
    )
    save_portal_config(config)
    return code


# ---------------------------------------------------------------------------
# Access code strength validation
# ---------------------------------------------------------------------------

def test_weak_code_too_short():
    assert AccessCodeValidator.validate_strength("abc") is False


def test_weak_code_pure_numeric():
    assert AccessCodeValidator.validate_strength("123456") is False


def test_weak_code_special_chars():
    assert AccessCodeValidator.validate_strength("abc!@#") is False


def test_strong_code_accepted():
    assert AccessCodeValidator.validate_strength("Team2025") is True


def test_admin_rejects_weak_code(client, code_tier_portal):
    """PUT /settings/portal with weak access code returns 400."""
    response = client.put(
        "/api/settings/portal",
        json={"access_code": "123"},
        headers={"X-API-Key": "test"},
    )
    assert response.status_code == 400
    assert "alphanumeric" in response.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Rate limiting (M5): 5 attempts per 15 min per IP
# ---------------------------------------------------------------------------

def test_rate_limit_6th_attempt_returns_429(client, code_tier_portal):
    """6th wrong attempt within 15 minutes returns 429."""
    for i in range(5):
        resp = client.post("/api/portal/auth/code", json={"code": "wrong"})
        assert resp.status_code == 401, f"Attempt {i+1} should be 401"

    # 6th attempt should be rate-limited
    resp = client.post("/api/portal/auth/code", json={"code": "wrong"})
    assert resp.status_code == 429
    assert "too many" in resp.json()["detail"].lower()


def test_correct_code_still_rate_limited_after_5_wrong(client, code_tier_portal):
    """Even with the correct code, 6th attempt is blocked."""
    for _ in range(5):
        client.post("/api/portal/auth/code", json={"code": "wrong"})

    resp = client.post("/api/portal/auth/code", json={"code": code_tier_portal})
    assert resp.status_code == 429


# ---------------------------------------------------------------------------
# Successful authentication
# ---------------------------------------------------------------------------

def test_correct_code_returns_token(client, code_tier_portal):
    """Correct access code returns a portal JWT."""
    resp = client.post("/api/portal/auth/code", json={"code": code_tier_portal})
    assert resp.status_code == 200
    data = resp.json()
    assert "token" in data
    assert data["tier"] == "code"
    assert "expires_at" in data


def test_token_grants_access_to_datasets(client, code_tier_portal):
    """Portal JWT allows access to /api/portal/datasets."""
    auth_resp = client.post("/api/portal/auth/code", json={"code": code_tier_portal})
    token = auth_resp.json()["token"]

    resp = client.get(
        "/api/portal/datasets",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Session invalidation on code rotation (SS-C1)
# ---------------------------------------------------------------------------

def test_session_invalidated_on_code_rotation(client, code_tier_portal):
    """Changing the access code invalidates existing portal sessions."""
    # Get a valid token
    auth_resp = client.post("/api/portal/auth/code", json={"code": code_tier_portal})
    old_token = auth_resp.json()["token"]

    # Rotate the access code via admin
    client.put(
        "/api/settings/portal",
        json={"access_code": "NewCode2025"},
        headers={"X-API-Key": "test"},
    )

    # Old token should now be rejected (portal_session_version mismatch)
    resp = client.get(
        "/api/portal/datasets",
        headers={"Authorization": f"Bearer {old_token}"},
    )
    assert resp.status_code == 401
    assert "re-authenticate" in resp.json()["detail"].lower()


def test_new_code_works_after_rotation(client, code_tier_portal):
    """After rotating the code, the new code works."""
    new_code = "NewCode2025"
    client.put(
        "/api/settings/portal",
        json={"access_code": new_code},
        headers={"X-API-Key": "test"},
    )

    resp = client.post("/api/portal/auth/code", json={"code": new_code})
    assert resp.status_code == 200
    assert "token" in resp.json()


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_auth_code_on_open_tier_returns_400(client):
    """POST /auth/code on open tier returns 400."""
    config = PortalConfig(
        enabled=True,
        tier=PortalTier.open,
        base_url="http://localhost:8100",
    )
    save_portal_config(config)

    resp = client.post("/api/portal/auth/code", json={"code": "anything"})
    assert resp.status_code == 400


def test_auth_code_portal_disabled_returns_404(client):
    """POST /auth/code when portal disabled returns 404."""
    config = PortalConfig(enabled=False)
    save_portal_config(config)

    resp = client.post("/api/portal/auth/code", json={"code": "anything"})
    assert resp.status_code == 404


def test_no_bearer_on_code_tier_returns_401(client, code_tier_portal):
    """Accessing datasets without Bearer token on code tier returns 401."""
    resp = client.get("/api/portal/datasets")
    assert resp.status_code == 401
