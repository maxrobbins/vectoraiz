"""
BQ-VZ-SHARED-SEARCH: Portal Pydantic Schemas
==============================================

Separate schema namespace for the portal trust zone (Mandate M2).
These schemas are NOT shared with admin endpoints.
"""

from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict

from pydantic import BaseModel, Field


class PortalTier(str, Enum):
    open = "open"
    code = "code"       # Shared access code (M5)
    sso = "sso"         # OIDC — Phase 2 only


class DatasetPortalConfig(BaseModel):
    """Per-dataset portal visibility and column restrictions."""
    portal_visible: bool = False
    search_columns: List[str] = Field(default_factory=list)
    display_columns: List[str] = Field(default_factory=list)
    max_results: int = Field(default=100, ge=1, le=1000)


class PortalConfig(BaseModel):
    """Portal configuration — stored in /data/portal_config.json."""
    enabled: bool = False
    tier: PortalTier = PortalTier.open
    base_url: str = ""                              # M6: explicit, required when enabled
    access_code_hash: Optional[str] = None          # bcrypt hash
    portal_session_version: int = 0                 # Incremented on code rotation (SS-C1)
    session_ttl_minutes: int = 480                  # 8 hours
    active_sessions: Dict[str, str] = Field(default_factory=dict)  # session_id -> expiry ISO
    datasets: Dict[str, DatasetPortalConfig] = Field(default_factory=dict)
    # OIDC fields — Phase 2
    oidc_issuer: Optional[str] = None
    oidc_client_id: Optional[str] = None
    oidc_client_secret: Optional[str] = None


class PortalConfigUpdate(BaseModel):
    """Admin request to update portal settings."""
    enabled: Optional[bool] = None
    tier: Optional[PortalTier] = None
    base_url: Optional[str] = None
    access_code: Optional[str] = None               # plaintext, will be hashed
    session_ttl_minutes: Optional[int] = Field(default=None, ge=30, le=10080)
    # OIDC fields — Phase 2
    oidc_issuer: Optional[str] = None
    oidc_client_id: Optional[str] = None
    oidc_client_secret: Optional[str] = None


class PortalSession(BaseModel):
    """Portal session — separate from admin sessions (M2)."""
    session_id: str
    tier: PortalTier
    ip_address: str
    created_at: datetime
    expires_at: datetime
    portal_session_version: int
    # OIDC fields — Phase 2 (populated when tier=sso)
    oidc_subject: Optional[str] = None
    oidc_email: Optional[str] = None
    oidc_name: Optional[str] = None


class PortalSearchQuery(BaseModel):
    """Portal search request."""
    dataset_id: str
    query: str
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


class PortalSearchResult(BaseModel):
    """Portal search response."""
    dataset_id: str
    dataset_name: str
    total_count: int
    results: List[dict]  # Only display_columns included
    query: str


class PortalDatasetInfo(BaseModel):
    """Dataset info visible to portal users."""
    dataset_id: str
    name: str
    description: Optional[str] = None
    row_count: int = 0
    searchable_columns: List[str] = Field(default_factory=list)


class PortalAuthRequest(BaseModel):
    """Access code authentication request."""
    code: str


class PortalAuthResponse(BaseModel):
    """Access code authentication response."""
    token: str
    expires_at: datetime
    tier: str


class PortalPublicConfig(BaseModel):
    """Public-facing portal config (no secrets)."""
    enabled: bool
    tier: PortalTier
    name: str = "Search Portal"


class PortalAccessLog(BaseModel):
    """Per-user access log entry for SSO sessions (Phase 2)."""
    timestamp: datetime
    session_id: str
    oidc_subject: str
    oidc_email: Optional[str] = None
    action: str  # "login", "search", "logout"
    detail: Optional[str] = None


class PortalSSOUserInfo(BaseModel):
    """SSO user info shown in portal header."""
    email: Optional[str] = None
    name: Optional[str] = None
