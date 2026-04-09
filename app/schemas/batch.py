"""
Batch Upload Schemas
====================

Pydantic request/response schemas for batch upload, preview, and confirm endpoints.

Phase: BQ-108+109 — Enhanced Upload Pipeline
Created: 2026-02-13
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# --- Batch Upload Response ---

class BatchItemAccepted(BaseModel):
    client_file_index: int
    original_filename: str
    relative_path: Optional[str] = None
    size_bytes: int
    status: str = "accepted"
    dataset_id: str
    preview_url: str
    status_url: str


class BatchItemRejected(BaseModel):
    client_file_index: int
    original_filename: str
    status: str = "rejected"
    error_code: str
    error: str


class BatchUploadResponse(BaseModel):
    batch_id: str
    accepted: int
    rejected: int
    items: List[Dict[str, Any]]


# --- Batch Status Response ---

class BatchStatusItem(BaseModel):
    dataset_id: str
    original_filename: str
    status: str
    size_bytes: int


class BatchStatusResponse(BaseModel):
    batch_id: str
    total: int
    by_status: Dict[str, int]
    items: List[BatchStatusItem]


# --- Dataset Status Response ---

class DatasetStatusResponse(BaseModel):
    dataset_id: str
    status: str
    original_filename: str
    batch_id: Optional[str] = None
    error_message: Optional[str] = None


# --- Preview Response ---

class PreviewFileInfo(BaseModel):
    original_filename: str
    file_type: str
    size_bytes: int
    encoding: Optional[str] = "utf-8"


class PreviewData(BaseModel):
    model_config = {"populate_by_name": True}

    text: Optional[str] = None
    kind: Optional[str] = None
    row_count_estimate: Optional[int] = None
    column_count: Optional[int] = None
    schema_preview: Optional[List[Dict[str, str]]] = Field(default=None, alias="schema")
    sample_rows: Optional[List[Dict[str, Any]]] = None


class PreviewResponse(BaseModel):
    dataset_id: str
    status: str
    file: Optional[PreviewFileInfo] = None
    preview: Optional[PreviewData] = None
    warnings: Optional[List[str]] = None
    error_message: Optional[str] = None
    actions: Optional[Dict[str, str]] = None


# --- Confirm Request/Response ---

class ConfirmRequest(BaseModel):
    index: bool = True


class ConfirmResponse(BaseModel):
    status: str
    error: Optional[str] = None


# --- Batch Confirm Response ---

class BatchConfirmResponse(BaseModel):
    batch_id: str
    confirmed: int
    already_indexing_or_ready: int
    skipped_error: int
