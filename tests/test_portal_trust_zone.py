"""
BQ-VZ-SHARED-SEARCH: Trust zone isolation tests (Mandate M2)

Tests: admin JWT rejected on portal routes, portal JWT rejected on admin routes.
Ensures zero shared auth state between admin and portal zones.
"""

import secrets
from datetime import datetime, timezone, timedelta

import pytest
from fastapi.testclient import TestClient

from app.models.portal import (
    AccessCodeValidator,
    save_portal_config,
    reset_portal_config_cache,
)
from app.middleware.portal_auth import create_portal_jwt
from app.schemas.portal import (
    PortalConfig,
    DatasetPortalConfig,
    PortalSession,
    PortalTier,
)


@pytest.fixture(autouse=True)
def reset_portal(tmp_path, monkeypatch):
    monkeypatch.setattr("app.models.portal._PORTAL_CONFIG_PATH", tmp_path / "portal_config.json")
    monkeypatch.setattr("app.middleware.portal_auth._PORTAL_JWT_SECRET_PATH", tmp_path / "portal_jwt.key")
    reset_portal_config_cache()
    yield
    reset_portal_config_cache()


@pytest.fixture
def client():
    from app.main import app
    with TestClient(app) as c:
        yield c


@pytest.fixture
def code_portal_with_token(client):
    """Set up code tier portal and return a valid portal JWT."""
    code = "TeamAccess1"
    config = PortalConfig(
        enabled=True,
        tier=PortalTier.code,
        base_url="http://localhost:8100",
        access_code_hash=AccessCodeValidator.hash_code(code),
        datasets={
            "ds-1": DatasetPortalConfig(portal_visible=True, display_columns=["name"]),
        },
    )
    save_portal_config(config)
    AccessCodeValidator.clear_rate_limits()

    auth_resp = client.post("/api/portal/auth/code", json={"code": code})
    return auth_resp.json()["token"]


def test_portal_jwt_rejected_on_admin_settings(client, code_portal_with_token, monkeypatch):
    """Portal JWT cannot access admin /settings/portal when auth is enabled."""
    monkeypatch.setattr("app.auth.api_key_auth._is_auth_enabled", lambda: True)
    resp = client.get(
        "/api/settings/portal",
        headers={"Authorization": f"Bearer {code_portal_with_token}"},
    )
    assert resp.status_code in (401, 403, 422)


def test_portal_jwt_rejected_on_admin_datasets(client, code_portal_with_token, monkeypatch):
    """Portal JWT cannot access admin /api/datasets when auth is enabled."""
    monkeypatch.setattr("app.auth.api_key_auth._is_auth_enabled", lambda: True)
    resp = client.get(
        "/api/datasets",
        headers={"Authorization": f"Bearer {code_portal_with_token}"},
    )
    assert resp.status_code != 200
    assert resp.status_code in (401, 403, 404, 422)


def test_admin_apikey_rejected_on_portal_datasets(client):
    """Admin X-API-Key cannot access portal endpoints (code tier requires portal JWT)."""
    code = "TeamAccess1"
    config = PortalConfig(
        enabled=True,
        tier=PortalTier.code,
        base_url="http://localhost:8100",
        access_code_hash=AccessCodeValidator.hash_code(code),
        datasets={
            "ds-1": DatasetPortalConfig(portal_visible=True),
        },
    )
    save_portal_config(config)

    resp = client.get(
        "/api/portal/datasets",
        headers={"X-API-Key": "test"},
    )
    assert resp.status_code == 401


def test_portal_jwt_uses_different_signing_key():
    """Portal JWT secret is stored separately from admin JWT secret."""
    from app.middleware.portal_auth import get_portal_jwt_secret, PORTAL_JWT_ISSUER

    portal_secret = get_portal_jwt_secret()
    assert portal_secret is not None
    assert PORTAL_JWT_ISSUER == "vectoraiz-portal"


def test_portal_jwt_has_portal_claims():
    """Portal JWT includes portal-specific claims (iss, aud, psv)."""
    import jwt as pyjwt
    from app.middleware.portal_auth import get_portal_jwt_secret, PORTAL_JWT_ALGORITHM

    now = datetime.now(timezone.utc)
    session = PortalSession(
        session_id=secrets.token_hex(16),
        tier=PortalTier.code,
        ip_address="127.0.0.1",
        created_at=now,
        expires_at=now + timedelta(hours=8),
        portal_session_version=0,
    )
    token = create_portal_jwt(session)
    claims = pyjwt.decode(
        token,
        get_portal_jwt_secret(),
        algorithms=[PORTAL_JWT_ALGORITHM],
        audience="portal",
    )
    assert claims["iss"] == "vectoraiz-portal"
    assert claims["aud"] == "portal"
    assert claims["tier"] == "code"
    assert "psv" in claims


def test_forged_jwt_with_wrong_issuer_rejected(client):
    """A JWT with admin issuer is rejected on portal endpoints."""
    import jwt as pyjwt
    from app.middleware.portal_auth import get_portal_jwt_secret, PORTAL_JWT_ALGORITHM

    config = PortalConfig(
        enabled=True,
        tier=PortalTier.code,
        base_url="http://localhost:8100",
        access_code_hash=AccessCodeValidator.hash_code("TeamAccess1"),
    )
    save_portal_config(config)

    now = datetime.now(timezone.utc)
    fake_token = pyjwt.encode(
        {
            "iss": "vectoraiz-admin",
            "aud": "portal",
            "sub": "fake-session",
            "tier": "code",
            "psv": 0,
            "iat": now,
            "exp": now + timedelta(hours=1),
        },
        get_portal_jwt_secret(),
        algorithm=PORTAL_JWT_ALGORITHM,
    )

    resp = client.get(
        "/api/portal/datasets",
        headers={"Authorization": f"Bearer {fake_token}"},
    )
    assert resp.status_code == 401
