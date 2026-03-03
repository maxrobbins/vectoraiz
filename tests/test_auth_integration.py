import pytest
import httpx
from fastapi.testclient import TestClient

# NOTE: The client fixture will enable auth and import the app,
# so we import other modules inside the tests or fixtures where needed.

VALID_KEY = "aim_valid_123"
INVALID_KEY = "aim_invalid_456"
VALID_RESPONSE_PAYLOAD = {
    "valid": True,
    "user_id": "usr_abc123",
    "key_id": "key_xyz789",
    "scopes": ["read", "write"]
}

@pytest.fixture(scope="function")
def auth_client(monkeypatch):
    """
    Provides a TestClient with auth enabled, overriding conftest.py.
    Clears the auth cache for each test for isolation.
    """
    # This must be set BEFORE the application and its modules are imported.
    monkeypatch.setenv("VECTORAIZ_AUTH_ENABLED", "true")

    # Delay import until env var is set
    from fastapi import Depends, APIRouter
    from app.auth.api_key_auth import api_key_cache, AuthenticatedUser, get_current_user
    from app.main import app

    # Patch Alembic upgrade to no-op: conftest.py already created tables via
    # SQLModel.metadata.create_all(). The lifespan's init_db() re-runs Alembic
    # migrations which fail with "table already exists" on the shared test DB.
    # We keep the rest of init_db (legacy migrations) but skip Alembic.
    monkeypatch.setattr("app.core.database._run_alembic_upgrade", lambda: None)

    # Ensure Alembic-only tables exist (not backed by SQLModel metadata)
    from app.services.deduction_queue import deductions_metadata
    from app.core.database import get_engine
    deductions_metadata.create_all(get_engine(), checkfirst=True)

    api_key_cache.clear()

    # Tests use aim_ keys which require connected mode
    from app.config import settings
    monkeypatch.setattr(settings, "mode", "connected")

    # Reset shared httpx client so mock is used
    import app.auth.api_key_auth as _auth_mod
    _auth_mod._http_client = None

    test_auth_router = APIRouter()
    @test_auth_router.get("/protected-auth-test")
    async def protected_endpoint(user: AuthenticatedUser = Depends(get_current_user)):
        return {"status": "ok", "user": user.model_dump()}

    app.include_router(test_auth_router, prefix="/api/v1")

    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def mock_httpx_client(mocker):
    """
    Mocks the httpx.AsyncClient used for API key validation, handling the
    async context manager (`async with`).
    """
    mock = mocker.patch("app.auth.api_key_auth.httpx.AsyncClient", autospec=True)
    instance = mock.return_value
    instance.__aenter__.return_value = instance
    instance.__aexit__.return_value = None
    return instance


def test_valid_api_key(auth_client: TestClient, mock_httpx_client):
    """
    Scenario: A valid API key is provided.
    Expected: Request succeeds (200 OK) after validating with ai.market.
    """
    mock_response = httpx.Response(200, json=VALID_RESPONSE_PAYLOAD)
    mock_httpx_client.post.return_value = mock_response

    headers = {"X-API-Key": VALID_KEY}
    response = auth_client.get("/api/v1/protected-auth-test", headers=headers)

    assert response.status_code == 200
    assert response.json()["user"]["user_id"] == "usr_abc123"
    mock_httpx_client.post.assert_awaited_once()


def test_invalid_api_key(auth_client: TestClient, mock_httpx_client):
    """
    Scenario: An invalid API key is provided.
    Expected: Request is rejected (401 Unauthorized).
    """
    mock_response = httpx.Response(401, json={"valid": False})
    mock_httpx_client.post.return_value = mock_response

    headers = {"X-API-Key": INVALID_KEY}
    response = auth_client.get("/api/v1/protected-auth-test", headers=headers)

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid API Key."


def test_missing_api_key_header(auth_client: TestClient, mock_httpx_client):
    """
    Scenario: The X-API-Key header is not included.
    Expected: Request is rejected (401 Unauthorized) without calling ai.market.
    """
    response = auth_client.get("/api/v1/protected-auth-test")

    assert response.status_code == 401
    assert response.json()["detail"] == "Not authenticated. Provide X-API-Key header or vz_session cookie."
    mock_httpx_client.post.assert_not_called()


def test_auth_service_unavailable(auth_client: TestClient, mock_httpx_client):
    """
    Scenario: The ai.market auth service is down or unreachable.
    Expected: Request fails (503 Service Unavailable).
    """
    mock_httpx_client.post.side_effect = httpx.RequestError("Connection failed")

    headers = {"X-API-Key": VALID_KEY}
    response = auth_client.get("/api/v1/protected-auth-test", headers=headers)

    assert response.status_code == 503
    assert "Authentication service is currently unavailable" in response.json()["detail"]


def test_api_key_caching(auth_client: TestClient, mock_httpx_client):
    """
    Scenario: A valid API key is used twice.
    Expected: The ai.market validation endpoint is called only once.
    """
    mock_response = httpx.Response(200, json=VALID_RESPONSE_PAYLOAD)
    mock_httpx_client.post.return_value = mock_response

    headers = {"X-API-Key": VALID_KEY}
    
    # First request - should hit ai.market
    response1 = auth_client.get("/api/v1/protected-auth-test", headers=headers)
    assert response1.status_code == 200

    # Second request - should be served from cache
    response2 = auth_client.get("/api/v1/protected-auth-test", headers=headers)
    assert response2.status_code == 200

    # Assert that the external call was only made once
    mock_httpx_client.post.assert_awaited_once()
