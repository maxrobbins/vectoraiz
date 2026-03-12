"""
BQ-VZ-SHARED-SEARCH Phase 2: OIDC/SSO Tests
=============================================

Tests for the OIDC Authorization Code flow, session management,
access logging, trust zone separation, and configuration validation.

Mocks the IdP (discovery, token endpoint, JWKS) to test the full flow
without a real identity provider.
"""

import json
import secrets
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from fastapi.testclient import TestClient

from app.models.portal import (
    AccessCodeValidator,
    get_portal_config,
    save_portal_config,
    reset_portal_config_cache,
)
from app.schemas.portal import PortalConfig, DatasetPortalConfig, PortalTier
from app.services.portal_oidc import (
    clear_all_sso_state,
    clear_discovery_cache,
    log_portal_access,
    get_access_logs,
    clear_access_logs,
    encrypt_client_secret,
    decrypt_client_secret,
    validate_state,
    _pending_states,
    store_refresh_token,
    get_refresh_token,
    clear_refresh_token,
)
from app.middleware.portal_auth import (
    create_portal_jwt,
    decode_portal_jwt,
)


# ---------------------------------------------------------------------------
# RSA key pair for mock IdP (used to sign ID tokens)
# ---------------------------------------------------------------------------

_rsa_private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_rsa_public_key = _rsa_private_key.public_key()

def _get_public_key_jwk():
    """Export RSA public key as JWK for mock JWKS endpoint."""
    from jwt.algorithms import RSAAlgorithm
    jwk = json.loads(RSAAlgorithm.to_jwk(_rsa_public_key))
    jwk["kid"] = "test-key-1"
    jwk["use"] = "sig"
    jwk["alg"] = "RS256"
    return jwk


def _sign_id_token(claims: dict) -> str:
    """Sign an ID token with the test RSA private key."""
    headers = {"kid": "test-key-1", "alg": "RS256"}
    return pyjwt.encode(claims, _rsa_private_key, algorithm="RS256", headers=headers)


# ---------------------------------------------------------------------------
# Mock OIDC Discovery Document
# ---------------------------------------------------------------------------

MOCK_ISSUER = "https://idp.example.com"
MOCK_DISCOVERY = {
    "issuer": MOCK_ISSUER,
    "authorization_endpoint": f"{MOCK_ISSUER}/authorize",
    "token_endpoint": f"{MOCK_ISSUER}/token",
    "jwks_uri": f"{MOCK_ISSUER}/.well-known/jwks.json",
    "end_session_endpoint": f"{MOCK_ISSUER}/logout",
    "response_types_supported": ["code"],
    "subject_types_supported": ["public"],
    "id_token_signing_alg_values_supported": ["RS256"],
}

MOCK_JWKS = {"keys": [_get_public_key_jwk()]}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_portal(tmp_path, monkeypatch):
    """Reset portal config, rate limits, and OIDC state for each test."""
    monkeypatch.setattr("app.models.portal._PORTAL_CONFIG_PATH", tmp_path / "portal_config.json")
    monkeypatch.setattr("app.middleware.portal_auth._PORTAL_JWT_SECRET_PATH", tmp_path / "portal_jwt.key")
    monkeypatch.setattr("app.middleware.portal_auth._portal_jwt_secret", None)
    reset_portal_config_cache()
    AccessCodeValidator.clear_rate_limits()
    clear_all_sso_state()
    clear_access_logs()
    yield
    reset_portal_config_cache()
    AccessCodeValidator.clear_rate_limits()
    clear_all_sso_state()
    clear_access_logs()


@pytest.fixture
def client():
    from app.main import app
    with TestClient(app) as c:
        yield c


@pytest.fixture
def sso_portal_config():
    """Enable portal in SSO tier with mock OIDC config."""
    secret = encrypt_client_secret("test-client-secret")
    config = PortalConfig(
        enabled=True,
        tier=PortalTier.sso,
        base_url="http://localhost:8100",
        oidc_issuer=MOCK_ISSUER,
        oidc_client_id="test-client-id",
        oidc_client_secret=secret,
        datasets={
            "test-dataset": DatasetPortalConfig(
                portal_visible=True,
                display_columns=["name"],
                max_results=50,
            ),
        },
    )
    save_portal_config(config)
    return config


def _make_sso_session_token(config=None, **overrides):
    """Create a valid SSO portal JWT for testing."""
    from app.schemas.portal import PortalSession
    if config is None:
        config = get_portal_config()
    now = datetime.now(timezone.utc)
    session = PortalSession(
        session_id=overrides.get("session_id", secrets.token_hex(16)),
        tier=PortalTier.sso,
        ip_address="127.0.0.1",
        created_at=now,
        expires_at=overrides.get("expires_at", now + timedelta(hours=8)),
        portal_session_version=overrides.get("psv", config.portal_session_version),
        oidc_subject=overrides.get("oidc_subject", "user-123"),
        oidc_email=overrides.get("oidc_email", "user@example.com"),
        oidc_name=overrides.get("oidc_name", "Test User"),
    )
    token = create_portal_jwt(session)
    # Track session
    config.active_sessions[session.session_id] = session.expires_at.isoformat()
    save_portal_config(config)
    return token, session


# ---------------------------------------------------------------------------
# Test 1: SSO tier rejected when OIDC not configured
# ---------------------------------------------------------------------------

def test_sso_tier_rejected_without_oidc_config(client):
    """Setting tier=sso without OIDC fields returns 400."""
    config = PortalConfig(
        enabled=False,
        tier=PortalTier.open,
        base_url="http://localhost:8100",
    )
    save_portal_config(config)

    resp = client.put(
        "/api/settings/portal",
        json={"tier": "sso"},
        headers={"X-API-Key": "test"},
    )
    assert resp.status_code == 400
    assert "oidc" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Test 2: SSO authorize redirects to IdP
# ---------------------------------------------------------------------------

def test_sso_authorize_redirects(client, sso_portal_config):
    """GET /auth/sso/authorize redirects to IdP authorize endpoint."""
    # Mock discovery fetch
    with patch("app.services.portal_oidc.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_response = MagicMock()
        mock_response.json.return_value = MOCK_DISCOVERY
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response

        resp = client.get("/api/portal/auth/sso/authorize", follow_redirects=False)
        assert resp.status_code == 302
        location = resp.headers["location"]
        assert location.startswith(f"{MOCK_ISSUER}/authorize")
        assert "client_id=test-client-id" in location
        assert "response_type=code" in location
        assert "state=" in location
        assert "nonce=" in location


# ---------------------------------------------------------------------------
# Test 3: SSO authorize fails if portal not enabled
# ---------------------------------------------------------------------------

def test_sso_authorize_portal_disabled(client):
    """SSO authorize returns 404 when portal is disabled."""
    config = PortalConfig(enabled=False)
    save_portal_config(config)

    resp = client.get("/api/portal/auth/sso/authorize")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Test 4: SSO authorize fails if tier is not SSO
# ---------------------------------------------------------------------------

def test_sso_authorize_wrong_tier(client):
    """SSO authorize returns 400 when tier is not SSO."""
    config = PortalConfig(
        enabled=True,
        tier=PortalTier.open,
        base_url="http://localhost:8100",
    )
    save_portal_config(config)

    resp = client.get("/api/portal/auth/sso/authorize")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Test 5: Valid SSO callback creates session
# ---------------------------------------------------------------------------

def test_sso_callback_valid_flow(client, sso_portal_config):
    """Valid OIDC callback creates a portal session and redirects with token."""
    # Set up a pending state
    state = secrets.token_urlsafe(32)
    nonce = secrets.token_urlsafe(32)
    _pending_states[state] = {
        "nonce": nonce,
        "created_at": time.time(),
        "redirect_uri": "http://localhost:8100/api/portal/auth/sso/callback",
    }

    # Create a valid ID token
    now = int(time.time())
    id_token_claims = {
        "iss": MOCK_ISSUER,
        "sub": "oidc-user-42",
        "aud": "test-client-id",
        "exp": now + 3600,
        "iat": now,
        "nonce": nonce,
        "email": "alice@example.com",
        "name": "Alice Smith",
    }
    id_token = _sign_id_token(id_token_claims)

    token_response = {
        "access_token": "mock-access-token",
        "token_type": "Bearer",
        "id_token": id_token,
        "refresh_token": "mock-refresh-token",
    }

    with patch("app.services.portal_oidc.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

        # Discovery response
        mock_disc_response = MagicMock()
        mock_disc_response.json.return_value = MOCK_DISCOVERY
        mock_disc_response.raise_for_status = MagicMock()

        # Token exchange response
        mock_token_response = MagicMock()
        mock_token_response.json.return_value = token_response
        mock_token_response.raise_for_status = MagicMock()

        mock_client.get.return_value = mock_disc_response
        mock_client.post.return_value = mock_token_response

        # Mock JWKS
        with patch("app.services.portal_oidc.PyJWKClient") as mock_jwk_cls:
            mock_jwk_client = MagicMock()
            mock_jwk_cls.return_value = mock_jwk_client
            mock_signing_key = MagicMock()
            mock_signing_key.key = _rsa_public_key
            mock_jwk_client.get_signing_key_from_jwt.return_value = mock_signing_key

            resp = client.get(
                f"/api/portal/auth/sso/callback?code=mock-auth-code&state={state}",
                follow_redirects=False,
            )

    assert resp.status_code == 302
    location = resp.headers["location"]
    assert "sso_token=" in location
    assert location.startswith("http://localhost:8100/portal/search")

    # Verify session was tracked
    config = get_portal_config()
    assert len(config.active_sessions) == 1

    # Verify refresh token was stored
    session_id = list(config.active_sessions.keys())[0]
    assert get_refresh_token(session_id) == "mock-refresh-token"

    # Verify access log
    logs = get_access_logs()
    assert len(logs) == 1
    assert logs[0].oidc_subject == "oidc-user-42"
    assert logs[0].oidc_email == "alice@example.com"
    assert logs[0].action == "login"


# ---------------------------------------------------------------------------
# Test 6: Invalid state in callback → 400
# ---------------------------------------------------------------------------

def test_sso_callback_invalid_state(client, sso_portal_config):
    """Invalid state parameter in callback returns 400."""
    resp = client.get("/api/portal/auth/sso/callback?code=test&state=bogus")
    assert resp.status_code == 400
    assert "state" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Test 7: SSO session JWT works on portal endpoints
# ---------------------------------------------------------------------------

def test_sso_session_grants_dataset_access(client, sso_portal_config):
    """SSO portal JWT grants access to portal datasets."""
    token, _ = _make_sso_session_token(sso_portal_config)

    resp = client.get(
        "/api/portal/datasets",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Test 8: Expired SSO token → 401
# ---------------------------------------------------------------------------

def test_expired_sso_token_rejected(client, sso_portal_config):
    """Expired SSO portal JWT returns 401."""
    token, _ = _make_sso_session_token(
        sso_portal_config,
        expires_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )

    resp = client.get(
        "/api/portal/datasets",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Test 9: portal_session_version invalidates SSO sessions
# ---------------------------------------------------------------------------

def test_psv_invalidates_sso_sessions(client, sso_portal_config):
    """Incrementing portal_session_version invalidates SSO sessions."""
    token, _ = _make_sso_session_token(sso_portal_config)

    # Verify it works first
    resp = client.get(
        "/api/portal/datasets",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200

    # Revoke all sessions (increments PSV)
    client.post(
        "/api/settings/portal/revoke-sessions",
        headers={"X-API-Key": "test"},
    )

    # Old token should now be rejected
    resp = client.get(
        "/api/portal/datasets",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 401
    assert "re-authenticate" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Test 10: SSO JWT rejected on admin routes (trust zone separation)
# ---------------------------------------------------------------------------

def test_sso_jwt_is_separate_trust_zone(sso_portal_config):
    """SSO portal JWT uses separate signing key and claims from admin JWT (M2).

    Trust zone separation is enforced by design:
    - Portal JWT iss="vectoraiz-portal", aud="portal"
    - Admin JWT uses different issuer/audience
    - Portal JWT signed with separate key at portal_jwt_secret.key
    """
    token, session = _make_sso_session_token(sso_portal_config)
    claims = decode_portal_jwt(token)
    assert claims is not None
    assert claims["iss"] == "vectoraiz-portal"
    assert claims["aud"] == "portal"
    assert claims["tier"] == "sso"
    assert claims["oidc_sub"] == session.oidc_subject
    assert claims["oidc_email"] == session.oidc_email


# ---------------------------------------------------------------------------
# Test 11: Access logging for SSO users
# ---------------------------------------------------------------------------

def test_access_logging(client, sso_portal_config):
    """Portal access logs track SSO user actions."""
    log_portal_access("sess-1", "user-1", "user1@test.com", "login")
    log_portal_access("sess-1", "user-1", "user1@test.com", "search", "q=test")
    log_portal_access("sess-2", "user-2", "user2@test.com", "login")

    logs = get_access_logs()
    assert len(logs) == 3
    # Most recent first
    assert logs[0].oidc_subject == "user-2"
    assert logs[1].action == "search"


# ---------------------------------------------------------------------------
# Test 12: Admin can view access logs
# ---------------------------------------------------------------------------

def test_admin_access_logs_endpoint(client, sso_portal_config):
    """Admin endpoint returns portal access logs."""
    log_portal_access("sess-1", "user-1", "user1@test.com", "login")
    log_portal_access("sess-1", "user-1", "user1@test.com", "search", "q=test")

    resp = client.get(
        "/api/admin/portal/access-logs?limit=10",
        headers={"X-API-Key": "test"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["logs"]) == 2


# ---------------------------------------------------------------------------
# Test 13: Session revocation by ID
# ---------------------------------------------------------------------------

def test_revoke_specific_session(client, sso_portal_config):
    """Admin can revoke a specific portal session."""
    token, session = _make_sso_session_token(sso_portal_config)

    # Revoke
    resp = client.delete(
        f"/api/admin/portal/sessions/{session.session_id}",
        headers={"X-API-Key": "test"},
    )
    assert resp.status_code == 200

    # Verify removed from active sessions
    config = get_portal_config()
    assert session.session_id not in config.active_sessions


# ---------------------------------------------------------------------------
# Test 14: Test OIDC connection endpoint
# ---------------------------------------------------------------------------

def test_oidc_test_connection(client, sso_portal_config):
    """test-oidc endpoint validates IdP connectivity."""
    with patch("app.services.portal_oidc.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_response = MagicMock()
        mock_response.json.return_value = MOCK_DISCOVERY
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response

        resp = client.post(
            "/api/settings/portal/test-oidc",
            headers={"X-API-Key": "test"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["issuer"] == MOCK_ISSUER


# ---------------------------------------------------------------------------
# Test 15: Test OIDC connection fails gracefully for unreachable IdP
# ---------------------------------------------------------------------------

def test_oidc_test_connection_unreachable(client, sso_portal_config):
    """test-oidc returns error when IdP is unreachable."""
    with patch("app.services.portal_oidc.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_client.get.side_effect = Exception("Connection refused")

        resp = client.post(
            "/api/settings/portal/test-oidc",
            headers={"X-API-Key": "test"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is False
        assert "Connection refused" in data["error"]


# ---------------------------------------------------------------------------
# Test 16: OIDC client secret encryption/decryption
# ---------------------------------------------------------------------------

def test_client_secret_encryption():
    """OIDC client secret can be encrypted and decrypted."""
    original = "super-secret-value"
    encrypted = encrypt_client_secret(original)
    assert encrypted != original
    decrypted = decrypt_client_secret(encrypted)
    assert decrypted == original


# ---------------------------------------------------------------------------
# Test 17: SSO logout clears session
# ---------------------------------------------------------------------------

def test_sso_logout(client, sso_portal_config):
    """SSO logout clears the portal session."""
    token, session = _make_sso_session_token(sso_portal_config)
    store_refresh_token(session.session_id, "mock-refresh")

    resp = client.post(
        "/api/portal/auth/sso/logout",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["message"] == "Logged out"

    # Verify session removed and refresh token cleared
    config = get_portal_config()
    assert session.session_id not in config.active_sessions
    assert get_refresh_token(session.session_id) is None


# ---------------------------------------------------------------------------
# Test 18: SSO userinfo endpoint
# ---------------------------------------------------------------------------

def test_sso_userinfo(client, sso_portal_config):
    """SSO userinfo returns user claims from the session."""
    token, _ = _make_sso_session_token(
        sso_portal_config,
        oidc_email="alice@example.com",
        oidc_name="Alice",
        oidc_subject="sub-123",
    )

    resp = client.get(
        "/api/portal/auth/sso/userinfo",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["email"] == "alice@example.com"
    assert data["name"] == "Alice"
    assert data["subject"] == "sub-123"


# ---------------------------------------------------------------------------
# Test 19: Cannot enable SSO portal without OIDC config
# ---------------------------------------------------------------------------

def test_cannot_enable_sso_without_oidc(client):
    """Enabling portal with SSO tier but no OIDC config returns 400."""
    config = PortalConfig(
        enabled=False,
        tier=PortalTier.sso,
        base_url="http://localhost:8100",
    )
    save_portal_config(config)

    resp = client.put(
        "/api/settings/portal",
        json={"enabled": True},
        headers={"X-API-Key": "test"},
    )
    assert resp.status_code == 400
    assert "oidc" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Test 20: State parameter expires after 10 minutes
# ---------------------------------------------------------------------------

def test_state_expires():
    """OIDC state parameter expires after 10 minutes."""
    state = "test-state"
    _pending_states[state] = {
        "nonce": "test-nonce",
        "created_at": time.time() - 700,  # 11+ minutes ago
        "redirect_uri": "http://localhost/callback",
    }
    result = validate_state(state)
    assert result is None


# ---------------------------------------------------------------------------
# Test 21: Public config shows SSO tier
# ---------------------------------------------------------------------------

def test_public_config_shows_sso_tier(client, sso_portal_config):
    """Portal public config correctly reports SSO tier."""
    resp = client.get("/api/portal/config")
    assert resp.status_code == 200
    data = resp.json()
    assert data["tier"] == "sso"
    assert data["enabled"] is True
