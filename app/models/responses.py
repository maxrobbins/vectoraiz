"""
Standardized response models for API documentation.
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Any


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., example="healthy", description="Service health status")
    timestamp: str = Field(..., example="2024-01-15T10:30:00", description="ISO timestamp")
    service: str = Field(..., example="vectoraiz-api", description="Service name")

    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "timestamp": "2024-01-15T10:30:00.000000",
                "service": "vectoraiz-api"
            }
        }


class DatasetListResponse(BaseModel):
    """Response for dataset listing."""
    datasets: List[Dict[str, Any]] = Field(..., description="List of dataset records")
    count: int = Field(..., example=5, description="Total number of datasets")


class SearchResultItem(BaseModel):
    """Single search result."""
    dataset_id: str = Field(..., example="abc12345", description="Dataset identifier")
    dataset_name: str = Field(..., example="companies.csv", description="Original filename")
    score: float = Field(..., example=0.8542, description="Relevance score (0-1)")
    row_index: int = Field(..., example=3, description="Row index in dataset")
    text_content: str = Field(..., description="Text that was matched")
    row_data: Dict[str, Any] = Field(..., description="Full row data")


class SearchResponse(BaseModel):
    """Search results response."""
    query: str = Field(..., example="technology", description="Original search query")
    results: List[SearchResultItem] = Field(..., description="Search results")
    total: int = Field(..., example=10, description="Total results returned")
    datasets_searched: int = Field(..., example=3, description="Number of datasets searched")
    duration_ms: float = Field(..., example=45.2, description="Search duration in milliseconds")


class SQLQueryResponse(BaseModel):
    """SQL query response."""
    query: str = Field(..., description="Executed SQL query")
    columns: List[str] = Field(..., example=["id", "name", "value"], description="Column names")
    data: List[Dict[str, Any]] = Field(..., description="Query results")
    row_count: int = Field(..., example=10, description="Number of rows returned")
    limit: int = Field(..., example=1000, description="Applied row limit")
    offset: int = Field(..., example=0, description="Applied offset")
    duration_ms: float = Field(..., example=12.5, description="Query duration")
    truncated: bool = Field(..., example=False, description="Whether results were truncated")


class PIIScanResponse(BaseModel):
    """PII scan results response."""
    dataset_id: str = Field(..., description="Dataset identifier")
    overall_risk: str = Field(..., example="medium", description="Overall risk level")
    columns_with_pii: int = Field(..., example=2, description="Columns containing PII")
    columns_clean: int = Field(..., example=5, description="Clean columns")
    pii_findings: List[Dict[str, Any]] = Field(..., description="Detailed PII findings")
    recommendations: List[Dict[str, str]] = Field(..., description="Action recommendations")


class ErrorResponse(BaseModel):
    """Standard error response."""
    detail: str = Field(..., example="Resource not found", description="Error message")
