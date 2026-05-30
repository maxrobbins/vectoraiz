"""
BQ-VZ-SHARED-SEARCH: Base URL validation tests (Mandate M6)

Tests: enabling portal without base_url -> validation error.
"""

import pytest
from fastapi.testclient import TestClient

from app.models.portal import save_portal_config, reset_portal_config_cache
from app.schemas.portal import PortalConfig, PortalTier


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


def test_enable_without_base_url_returns_400(client):
    """Enabling portal without setting base_url returns 400 (Mandate M6)."""
    config = PortalConfig(enabled=False, tier=PortalTier.open, base_url="")
    save_portal_config(config)

    resp = client.put(
        "/api/settings/portal",
        json={"enabled": True},
        headers={"X-API-Key": "test"},
    )
    assert resp.status_code == 400
    assert "base url" in resp.json()["detail"].lower()


def test_enable_with_base_url_succeeds(client):
    """Enabling portal with base_url set succeeds."""
    config = PortalConfig(enabled=False, tier=PortalTier.open, base_url="")
    save_portal_config(config)

    resp = client.put(
        "/api/settings/portal",
        json={"enabled": True, "base_url": "http://myserver.local:8100"},
        headers={"X-API-Key": "test"},
    )
    assert resp.status_code == 200
    assert resp.json()["enabled"] is True


def test_set_base_url_then_enable(client):
    """Setting base_url first, then enabling works."""
    config = PortalConfig(enabled=False, tier=PortalTier.open, base_url="")
    save_portal_config(config)

    # Set base_url
    resp1 = client.put(
        "/api/settings/portal",
        json={"base_url": "http://myserver.local:8100"},
        headers={"X-API-Key": "test"},
    )
    assert resp1.status_code == 200

    # Now enable
    resp2 = client.put(
        "/api/settings/portal",
        json={"enabled": True},
        headers={"X-API-Key": "test"},
    )
    assert resp2.status_code == 200
    assert resp2.json()["enabled"] is True


def test_enable_code_tier_without_access_code_returns_400(client):
    """Enabling with code tier but no access code returns 400."""
    config = PortalConfig(
        enabled=False,
        tier=PortalTier.code,
        base_url="http://myserver.local:8100",
    )
    save_portal_config(config)

    resp = client.put(
        "/api/settings/portal",
        json={"enabled": True},
        headers={"X-API-Key": "test"},
    )
    assert resp.status_code == 400
    assert "access code" in resp.json()["detail"].lower()


def test_base_url_trailing_slash_stripped(client):
    """Trailing slash is stripped from base_url."""
    config = PortalConfig(enabled=False, base_url="")
    save_portal_config(config)

    resp = client.put(
        "/api/settings/portal",
        json={"base_url": "http://myserver.local:8100/"},
        headers={"X-API-Key": "test"},
    )
    assert resp.status_code == 200
    assert resp.json()["base_url"] == "http://myserver.local:8100"
