import httpx
import time
from typing import Dict, Optional, List

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from starlette import status

from app.config import settings

# --- Pydantic Models for Type Safety ---
class AuthDetails(BaseModel):
    """Represents the validated authentication details from ai.market."""
    user_id: str
    key_id: str
    scopes: List[str]

# --- In-Memory Cache with TTL ---
# Using a simple dictionary for the cache store and another for timestamps
_api_key_cache: Dict[str, AuthDetails] = {}
_api_key_cache_expiry: Dict[str, float] = {}

# --- API Key Header Definition ---
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)

async def _validate_key_on_aimarket(api_key: str) -> Optional[AuthDetails]:
    """Makes a network call to ai.market to validate the API key."""
    validation_url = f"{settings.ai_market_url}/api/v1/gateway/validate"
    headers = {"X-API-Key": api_key}
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(validation_url, headers=headers)
        
        if response.status_code == status.HTTP_200_OK:
            data = response.json()
            if data.get("valid") is True:
                return AuthDetails(
                    user_id=data.get("user_id"), 
                    key_id=data.get("key_id"), 
                    scopes=data.get("scopes", [])
                )
        return None
    except httpx.RequestError:
        # Network errors should fail open if we want high availability,
        # but for security, we'll fail closed.
        return None

async def get_current_user_auth(
    request: Request,
    api_key: str = Security(api_key_header)
) -> AuthDetails:
    """FastAPI dependency to validate API key and attach auth details to the request state."""
    if not settings.auth_enabled:
        # If auth is disabled, return a mock object for development
        mock_auth = AuthDetails(user_id="dev_user", key_id="dev_key", scopes=["*"])
        request.state.auth = mock_auth
        return mock_auth

    # Check cache first
    if api_key in _api_key_cache and time.time() < _api_key_cache_expiry.get(api_key, 0):
        auth_details = _api_key_cache[api_key]
        request.state.auth = auth_details
        return auth_details

    # If not in cache or expired, validate against ai.market
    auth_details = await _validate_key_on_aimarket(api_key)
    if not auth_details:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired API Key",
            headers={"WWW-Authenticate": "Header"},
        )

    # Store in cache
    _api_key_cache[api_key] = auth_details
    _api_key_cache_expiry[api_key] = time.time() + settings.auth_cache_ttl
    
    # Attach to request state for use in endpoints
    request.state.auth = auth_details
    return auth_details
