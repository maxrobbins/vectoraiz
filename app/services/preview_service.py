"""
Preview Service
===============

Lightweight preview generation from cached dataset metadata.
Uses DuckDB LIMIT 1000 for tabular schema discovery.

Phase: BQ-109 — Data Preview
Created: 2026-02-13
"""

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.models.dataset import DatasetStatus
from app.services.processing_service import get_processing_service

logger = logging.getLogger(__name__)

# PII column name patterns
_PII_PATTERNS = [
    (re.compile(r"(?i)(e[-_]?mail)"), "email"),
    (re.compile(r"(?i)(phone|mobile|tel)"), "phone"),
    (re.compile(r"(?i)(ssn|social.?security)"), "ssn"),
    (re.compile(r"(?i)(first.?name|last.?name|full.?name)"), "name"),
    (re.compile(r"(?i)(address|street|city|zip|postal)"), "address"),
    (re.compile(r"(?i)(dob|date.?of.?birth|birth.?date)"), "date_of_birth"),
    (re.compile(r"(?i)(credit.?card|card.?number|ccn)"), "credit_card"),
    (re.compile(r"(?i)(ip.?addr)"), "ip_address"),
]

# PII regex patterns for sample data
_PII_VALUE_PATTERNS = [
    (re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"), "email"),
    (re.compile(r"\b\d{3}[-.]?\d{2}[-.]?\d{4}\b"), "ssn"),
    (re.compile(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b"), "phone"),
]


class PreviewService:
    """Generates and returns preview data for datasets."""

    def get_preview(self, dataset_id: str) -> Dict[str, Any]:
        """Return preview response appropriate for the dataset's current status."""
        processing = get_processing_service()
        record = processing.get_dataset(dataset_id)

        if not record:
            return None

        status = record.status.value if isinstance(record.status, DatasetStatus) else record.status

        # Cancelled datasets are invisible
        if status == DatasetStatus.CANCELLED.value:
            return None

        # Still processing
        if status in (DatasetStatus.UPLOADED.value, DatasetStatus.EXTRACTING.value):
            return {
                "dataset_id": dataset_id,
                "status": status,
                "file": None,
                "preview": None,
            }

        # Error state
        if status == DatasetStatus.ERROR.value:
            return {
                "dataset_id": dataset_id,
                "status": status,
                "preview": None,
                "error_message": record.error or "Unknown error",
            }

        # preview_ready, indexing, or ready — return cached preview
        return self._build_full_preview(record, status)

    def _build_full_preview(self, record, status: str) -> Dict[str, Any]:
        """Build full preview response from cached data."""
        file_info = {
            "original_filename": record.original_filename,
            "file_type": record.file_type,
            "size_bytes": record.file_size_bytes,
            "encoding": "utf-8",
        }

        preview_data = None
        warnings = []

        pm = record.preview_metadata or {}

        if pm:
            preview_data = {
                "text": record.preview_text,
                "kind": pm.get("kind"),
                "row_count_estimate": pm.get("row_count_estimate"),
                "column_count": pm.get("column_count"),
                "schema": pm.get("schema"),
                "sample_rows": pm.get("sample_rows"),
            }

            # Lightweight PII scan on schema column names
            schema = pm.get("schema", [])
            warnings = self._scan_pii_columns(schema, pm.get("sample_rows"))

        actions = None
        if status == DatasetStatus.PREVIEW_READY.value:
            actions = {
                "confirm_url": f"/api/datasets/{record.id}/confirm",
                "cancel_url": f"/api/datasets/{record.id}",
            }

        return {
            "dataset_id": record.id,
            "status": status,
            "file": file_info,
            "preview": preview_data,
            "warnings": warnings if warnings else None,
            "actions": actions,
        }

    def detect_pii_columns(
        self,
        dataset_id: str,
        sample_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> set:
        """Return set of column names flagged as PII (pipeline scan + heuristic)."""
        pii_cols: set = set()

        # Check pipeline PII scan results first
        try:
            from app.config import settings as app_settings
            pii_scan_path = Path(app_settings.data_directory) / "processed" / dataset_id / "pii_scan.json"
            if pii_scan_path.exists():
                import json as _json
                with open(pii_scan_path) as f:
                    pii_data = _json.load(f)
                for col_name, col_info in pii_data.get("columns", {}).items():
                    if isinstance(col_info, dict) and col_info.get("pii_types"):
                        pii_cols.add(col_name)
                for cr in pii_data.get("column_results", []):
                    if cr.get("pii_detected") or cr.get("total_pii_findings", 0) > 0:
                        col = cr.get("column_name", "")
                        if col:
                            pii_cols.add(col)
        except Exception:
            pass  # Fall back to heuristic

        # Heuristic: column name + value patterns
        if sample_rows:
            col_names = list(sample_rows[0].keys())
            for col_name in col_names:
                for pattern, _pii_type in _PII_PATTERNS:
                    if pattern.search(col_name):
                        pii_cols.add(col_name)
                        break

            for row in sample_rows[:5]:
                for col_name, value in row.items():
                    if col_name in pii_cols or not isinstance(value, str):
                        continue
                    for pattern, _pii_type in _PII_VALUE_PATTERNS:
                        if pattern.search(value):
                            pii_cols.add(col_name)
                            break

        return pii_cols

    def _scan_pii_columns(
        self,
        schema: List[Dict[str, str]],
        sample_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> List[str]:
        """Lightweight PII scan: column name pattern matching + regex on sample data."""
        warnings = []
        seen = set()

        # Column name matching
        for col in schema:
            col_name = col.get("name", "")
            for pattern, pii_type in _PII_PATTERNS:
                if pattern.search(col_name) and pii_type not in seen:
                    warnings.append(f"Possible PII detected in column: {col_name} ({pii_type})")
                    seen.add(pii_type)

        # Sample value matching
        if sample_rows:
            for row in sample_rows[:5]:
                for _key, value in row.items():
                    if not isinstance(value, str):
                        continue
                    for pattern, pii_type in _PII_VALUE_PATTERNS:
                        if pattern.search(value) and pii_type not in seen:
                            warnings.append(f"Possible PII values detected: {pii_type}")
                            seen.add(pii_type)

        return warnings


# Singleton
_preview_service: Optional[PreviewService] = None


def get_preview_service() -> PreviewService:
    global _preview_service
    if _preview_service is None:
        _preview_service = PreviewService()
    return _preview_service
