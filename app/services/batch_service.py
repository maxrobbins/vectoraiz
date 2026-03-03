"""
Batch Service
=============

Orchestrates multi-file batch upload: validation, saving, and status aggregation.

Phase: BQ-108 — Bulk Upload
Created: 2026-02-13
"""

import logging
import mimetypes
import os
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlmodel import select

from app.config import settings
from app.models.dataset import DatasetRecord as DBDatasetRecord, DatasetStatus
from app.services.processing_service import (
    DatasetRecord,
    get_processing_service,
)
from app.utils.sanitization import sanitize_filename

logger = logging.getLogger(__name__)

# Limits
MAX_FILES = 10_000
MAX_TOTAL_BYTES = 500 * 1024 * 1024 * 1024  # 500 GB (effectively unlimited for local app)

from app.services.processing_service import PROCESSABLE_TYPES

# Build dotted extension set from the canonical PROCESSABLE_TYPES
SUPPORTED_EXTENSIONS = {f'.{t}' for t in PROCESSABLE_TYPES}

# Magic-byte signatures for MIME validation
_MAGIC_SIGNATURES: Dict[str, List[bytes]] = {
    ".pdf": [b"%PDF"],
    ".xlsx": [b"PK\x03\x04"],            # ZIP-based
    ".xls": [b"\xd0\xcf\x11\xe0"],       # OLE2
    ".docx": [b"PK\x03\x04"],
    ".doc": [b"\xd0\xcf\x11\xe0"],
    ".pptx": [b"PK\x03\x04"],
    ".ppt": [b"\xd0\xcf\x11\xe0"],
    ".parquet": [b"PAR1"],
    # Native document formats (BQ-VZ-PERF Phase 3)
    ".rtf": [b"{\\rtf"],
    ".epub": [b"PK\x03\x04"],
    ".odt": [b"PK\x03\x04"],
    ".ods": [b"PK\x03\x04"],
    ".odp": [b"PK\x03\x04"],
    # Text-based formats (.xml, .rss, .eml, .msg, .mbox, .ics, .vcf)
    # have no magic byte entry → _check_magic_bytes returns True
}


def _check_magic_bytes(data: bytes, extension: str) -> bool:
    """Return True if the file magic bytes are consistent with the extension."""
    sigs = _MAGIC_SIGNATURES.get(extension)
    if sigs is None:
        # No signature check for plain text formats (csv, json, txt, md, html, etc.)
        return True

    # RTF BOM fix: strip UTF-8 BOM and leading whitespace before checking
    check_data = data
    if extension == '.rtf':
        check_data = data.lstrip(b'\xef\xbb\xbf \t\r\n')

    return any(check_data.startswith(sig) for sig in sigs)


class BatchService:
    """Validates, saves, and tracks multi-file batch uploads."""

    def __init__(self):
        self.upload_dir = Path(settings.upload_directory)
        self.upload_dir.mkdir(parents=True, exist_ok=True)

    def validate_batch(
        self,
        filenames: List[str],
        sizes: List[int],
        headers: List[bytes],
        paths: Optional[List[str]],
    ) -> Tuple[List[int], List[Dict[str, Any]]]:
        """Validate batch constraints. Returns (accepted_indices, rejected_items)."""
        rejected = []

        if len(filenames) > MAX_FILES:
            # Reject everything beyond the limit
            for i in range(MAX_FILES, len(filenames)):
                rejected.append({
                    "client_file_index": i,
                    "original_filename": filenames[i],
                    "status": "rejected",
                    "error_code": "batch_limit",
                    "error": f"Exceeds max {MAX_FILES} files per batch",
                })

        # Check paths length
        if paths is not None and len(paths) != len(filenames):
            raise ValueError(
                f"paths length ({len(paths)}) must match files length ({len(filenames)})"
            )

        total_size = 0
        accepted = []

        for i in range(min(len(filenames), MAX_FILES)):
            fname = filenames[i]
            size = sizes[i]
            header = headers[i]
            ext = Path(fname).suffix.lower()

            # Extension check
            if ext not in SUPPORTED_EXTENSIONS:
                rejected.append({
                    "client_file_index": i,
                    "original_filename": fname,
                    "status": "rejected",
                    "error_code": "unsupported_type",
                    "error": f"Unsupported file type: {ext}",
                })
                continue

            # MIME / magic byte check
            if not _check_magic_bytes(header, ext):
                rejected.append({
                    "client_file_index": i,
                    "original_filename": fname,
                    "status": "rejected",
                    "error_code": "mime_mismatch",
                    "error": f"File content does not match extension {ext}",
                })
                continue

            total_size += size
            if total_size > MAX_TOTAL_BYTES:
                rejected.append({
                    "client_file_index": i,
                    "original_filename": fname,
                    "status": "rejected",
                    "error_code": "batch_size_limit",
                    "error": f"Batch total exceeds {MAX_TOTAL_BYTES // (1024*1024)}MB",
                })
                continue

            accepted.append(i)

        return accepted, rejected

    def create_dataset_record(
        self,
        filename: str,
        file_type: str,
        file_size: int,
        batch_id: str,
        relative_path: Optional[str],
    ) -> DatasetRecord:
        """Create a dataset record with batch metadata."""
        processing = get_processing_service()
        record = processing.create_dataset(
            original_filename=filename,
            file_type=file_type,
        )
        record.batch_id = batch_id
        record.relative_path = relative_path or filename
        record.file_size_bytes = file_size
        # Save updated batch fields
        storage_fn = record.upload_path.name if record.upload_path else f"{record.id}"
        processing._save_record(record, storage_fn)
        return record

    def get_batch_status(self, batch_id: str) -> Optional[Dict[str, Any]]:
        """Aggregate status of all datasets in a batch."""
        from app.core.database import get_session_context

        with get_session_context() as session:
            stmt = (
                select(DBDatasetRecord)
                .where(DBDatasetRecord.batch_id == batch_id)
                .order_by(DBDatasetRecord.created_at)
            )
            rows = session.exec(stmt).all()

        if not rows:
            return None

        status_counts: Counter = Counter()
        items = []
        for row in rows:
            status_counts[row.status] += 1
            items.append({
                "dataset_id": row.id,
                "original_filename": row.original_filename,
                "status": row.status,
                "size_bytes": row.file_size_bytes or 0,
            })

        return {
            "batch_id": batch_id,
            "total": len(rows),
            "by_status": dict(status_counts),
            "items": items,
        }

    def confirm_batch(self, batch_id: str, user_id: str) -> Dict[str, Any]:
        """Confirm all preview_ready datasets in a batch. Returns counts.

        Race-condition fix: items still in UPLOADED/EXTRACTING get confirmed_at
        stamped so that process_dataset_task auto-indexes after extraction.
        """
        from app.core.database import get_session_context

        confirmed = 0
        already = 0
        skipped_error = 0
        pending_confirm = 0

        with get_session_context() as session:
            stmt = (
                select(DBDatasetRecord)
                .where(DBDatasetRecord.batch_id == batch_id)
            )
            rows = session.exec(stmt).all()

            now = datetime.now(timezone.utc)
            for row in rows:
                if row.status == DatasetStatus.PREVIEW_READY.value:
                    row.status = DatasetStatus.INDEXING.value
                    row.confirmed_at = now
                    row.confirmed_by = user_id
                    row.updated_at = now
                    session.add(row)
                    confirmed += 1
                elif row.status in (
                    DatasetStatus.UPLOADED.value,
                    DatasetStatus.EXTRACTING.value,
                ):
                    # Still processing — stamp confirmed_at so the background
                    # task auto-indexes when extraction completes.
                    row.confirmed_at = now
                    row.confirmed_by = user_id
                    row.updated_at = now
                    session.add(row)
                    pending_confirm += 1
                elif row.status in (
                    DatasetStatus.INDEXING.value,
                    DatasetStatus.READY.value,
                ):
                    already += 1
                elif row.status == DatasetStatus.ERROR.value:
                    skipped_error += 1

            session.commit()

        # Gather IDs that need indexing
        confirmed_ids = []
        with get_session_context() as session:
            stmt = (
                select(DBDatasetRecord)
                .where(DBDatasetRecord.batch_id == batch_id)
                .where(DBDatasetRecord.status == DatasetStatus.INDEXING.value)
            )
            rows = session.exec(stmt).all()
            confirmed_ids = [r.id for r in rows]

        return {
            "batch_id": batch_id,
            "confirmed": confirmed,
            "pending_confirm": pending_confirm,
            "already_indexing_or_ready": already,
            "skipped_error": skipped_error,
            "confirmed_ids": confirmed_ids,
        }

    def batch_belongs_to_user(self, batch_id: str, user_id: str) -> bool:
        """Check if any datasets in the batch exist (ownership placeholder)."""
        from app.core.database import get_session_context

        with get_session_context() as session:
            stmt = (
                select(DBDatasetRecord)
                .where(DBDatasetRecord.batch_id == batch_id)
                .limit(1)
            )
            row = session.exec(stmt).first()
            return row is not None


# Singleton
_batch_service: Optional[BatchService] = None


def get_batch_service() -> BatchService:
    global _batch_service
    if _batch_service is None:
        _batch_service = BatchService()
    return _batch_service
