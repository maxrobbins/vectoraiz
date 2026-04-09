"""
Pydantic Schemas for Listing Metadata Generation
=================================================
BQ-085: Transform vectorAIz processing results into marketplace-ready metadata.
"""
from pydantic import BaseModel, Field
from typing import List
from datetime import datetime


class ColumnSummary(BaseModel):
    name: str
    type: str
    null_percentage: float = 0.0
    uniqueness_ratio: float = 0.0
    sample_values: List[str] = Field(default_factory=list)


class ListingMetadata(BaseModel):
    title: str = Field(..., description="Auto-generated listing title")
    description: str = Field(..., description="Human-readable description from column profiles")
    tags: List[str] = Field(default_factory=list, description="Tags from column names + semantic types")
    column_summary: List[ColumnSummary] = Field(default_factory=list)
    row_count: int = 0
    column_count: int = 0
    file_format: str = ""
    size_bytes: int = 0
    freshness_score: float = Field(0.0, description="0.0-1.0 based on file modification time")
    privacy_score: float = Field(1.0, description="1.0 = no PII detected, 0.0 = high PII risk")
    data_categories: List[str] = Field(default_factory=list, description="Inferred data categories")
    generated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
