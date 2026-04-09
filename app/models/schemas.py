from pydantic import BaseModel
from datetime import datetime


class DatasetMetadata(BaseModel):
    id: str
    filename: str
    row_count: int
    column_count: int
    size_bytes: int
    created_at: datetime
    status: str  # processing, ready, error


class HealthResponse(BaseModel):
    status: str
    timestamp: str
    service: str
