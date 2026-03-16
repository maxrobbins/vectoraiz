"""
Connectivity Models — Pydantic models for external LLM connectivity.

Covers: tokens, requests, responses, and structured errors for the
MCP Server and REST API (BQ-MCP-RAG §4.2, §5.2, §5.4).

Phase: BQ-MCP-RAG — Universal LLM Connectivity
Created: S136
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import re

from pydantic import BaseModel, Field, field_validator
from sqlmodel import Column, SQLModel, Text, Field as SQLField

# Regex for safe dataset IDs — shared by all tools
DATASET_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")
DATASET_ID_MAX_LEN = 64


# ---------------------------------------------------------------------------
# Database model (SQLModel)
# ---------------------------------------------------------------------------

class ConnectivityTokenRecord(SQLModel, table=True):
    """Persistent record for a connectivity token (§10)."""

    __tablename__ = "connectivity_tokens"

    id: str = SQLField(primary_key=True, max_length=8)  # 8-char alphanumeric
    label: str = SQLField(max_length=255)
    hmac_hash: str = SQLField(max_length=255)
    secret_last4: str = SQLField(max_length=4)
    scopes: str = SQLField(
        default='["ext:search","ext:sql","ext:schema","ext:datasets","ext:profile","ext:pii"]',
        sa_column=Column(Text, default='["ext:search","ext:sql","ext:schema","ext:datasets","ext:profile","ext:pii"]'),
    )
    created_at: datetime = SQLField(default_factory=lambda: datetime.utcnow())
    expires_at: Optional[datetime] = SQLField(default=None, nullable=True)
    last_used_at: Optional[datetime] = SQLField(default=None, nullable=True)
    request_count: int = SQLField(default=0)
    is_revoked: bool = SQLField(default=False, index=True)
    revoked_at: Optional[datetime] = SQLField(default=None, nullable=True)


# ---------------------------------------------------------------------------
# In-memory token representation (after validation)
# ---------------------------------------------------------------------------

class ConnectivityToken(BaseModel):
    """Validated connectivity token — returned by validate_token()."""

    id: str
    label: str
    scopes: List[str]
    secret_last4: str
    created_at: datetime
    expires_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None
    request_count: int = 0


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

def validate_dataset_id(value: str) -> str:
    """Validate a dataset_id: alphanumeric/dash/underscore, max 64 chars."""
    if len(value) > DATASET_ID_MAX_LEN:
        raise ValueError(f"dataset_id exceeds maximum length of {DATASET_ID_MAX_LEN}")
    if not DATASET_ID_PATTERN.match(value):
        raise ValueError("dataset_id contains invalid characters")
    return value


class DatasetIdInput(BaseModel):
    """Validated dataset_id input — used by tools that accept a single dataset_id."""

    dataset_id: str = Field(..., description="Dataset identifier")

    @field_validator("dataset_id")
    @classmethod
    def check_dataset_id(cls, v: str) -> str:
        return validate_dataset_id(v)


class VectorSearchRequest(BaseModel):
    """Input for vectoraiz_search tool (§5.2)."""

    query: str = Field(..., max_length=1000, description="Natural language search query")
    dataset_id: Optional[str] = Field(None, description="Optional: limit to specific dataset")
    top_k: int = Field(5, ge=1, le=20, description="Number of results (default 5)")

    @field_validator("dataset_id")
    @classmethod
    def check_dataset_id(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v != "":
            return validate_dataset_id(v)
        return v


class SQLQueryRequest(BaseModel):
    """Input for vectoraiz_sql tool (§5.2)."""

    sql: str = Field(..., max_length=4096, description="SQL SELECT query")
    dataset_id: Optional[str] = Field(None, description="Optional: scope to a specific dataset")

    @field_validator("dataset_id")
    @classmethod
    def check_dataset_id(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v != "":
            return validate_dataset_id(v)
        return v


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class DatasetInfo(BaseModel):
    """Single dataset in list_datasets response."""

    id: str
    name: str
    description: Optional[str] = None
    type: str
    row_count: int
    column_count: int
    created_at: str
    has_vectors: bool


class DatasetListResponse(BaseModel):
    """Response for vectoraiz_list_datasets."""

    datasets: List[DatasetInfo]
    count: int


class ColumnInfo(BaseModel):
    """Column definition in schema response."""

    name: str
    type: str
    nullable: bool = True
    description: Optional[str] = None
    sample_values: List[str] = Field(default_factory=list)


class SchemaResponse(BaseModel):
    """Response for vectoraiz_get_schema."""

    dataset_id: str
    table_name: str
    row_count: int
    columns: List[ColumnInfo]


class ProfileColumnInfo(BaseModel):
    """Column info in profile response."""

    name: str
    type: str
    null_count: int = 0
    null_rate: float = 0.0
    sample_values: List[Any] = Field(default_factory=list)


class ProfileResponse(BaseModel):
    """Response for vectoraiz_profile_dataset."""

    dataset_id: str
    row_count: int
    column_count: int
    columns: List[ProfileColumnInfo]
    sample_rows: List[List[Any]] = Field(default_factory=list)


class PIIReportResponse(BaseModel):
    """Response for vectoraiz_get_pii_report."""

    dataset_id: str
    status: str = "available"
    message: Optional[str] = None
    report: Optional[Dict[str, Any]] = None


class SearchMatch(BaseModel):
    """Single match in search response."""

    id: str
    score: float
    text: str = Field(default="", max_length=2000)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SearchResponse(BaseModel):
    """Response for vectoraiz_search."""

    matches: List[SearchMatch]
    count: int
    truncated: bool = False
    request_id: str


class SQLLimits(BaseModel):
    """Limits applied to an SQL query."""

    max_rows: int
    max_runtime_ms: int
    max_memory_mb: int


class SQLResponse(BaseModel):
    """Response for vectoraiz_sql."""

    columns: List[str]
    rows: List[List[Any]]
    row_count: int
    truncated: bool = False
    execution_ms: int
    limits_applied: SQLLimits
    request_id: str


class HealthResponse(BaseModel):
    """Response for health endpoint."""

    status: str = "ok"
    connectivity_enabled: bool = True
    version: str = "1.0"


# ---------------------------------------------------------------------------
# Error models (§5.4)
# ---------------------------------------------------------------------------

class ConnectivityErrorDetail(BaseModel):
    """Structured error detail."""

    code: str
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)


class ConnectivityErrorResponse(BaseModel):
    """Structured error envelope for MCP + REST."""

    error: ConnectivityErrorDetail
    request_id: str


# ---------------------------------------------------------------------------
# Error code → HTTP status mapping (§5.4, M22)
# ---------------------------------------------------------------------------

ERROR_HTTP_STATUS: Dict[str, int] = {
    "auth_invalid": 401,
    "auth_revoked": 401,
    "auth_expired": 401,
    "scope_denied": 403,
    "rate_limited": 429,
    "ip_blocked": 429,
    "forbidden_sql": 400,
    "sql_too_long": 400,
    "dataset_not_found": 404,
    "query_timeout": 408,
    "service_unavailable": 503,
    "internal_error": 500,
}


# All valid scopes
VALID_SCOPES = {"ext:search", "ext:sql", "ext:schema", "ext:datasets", "ext:profile", "ext:pii"}
