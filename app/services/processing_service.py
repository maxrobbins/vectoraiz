import logging
import os
import uuid
import asyncio
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timezone
from enum import Enum
import json
import csv

from app.config import settings
from app.models.dataset import DatasetStatus
from app.services.duckdb_service import get_duckdb_service
from app.utils.sanitization import sanitize_filename, sql_quote_literal

_log = logging.getLogger(__name__)


# Backward-compat alias — existing code references ProcessingStatus
ProcessingStatus = DatasetStatus


class DatasetRecord:
    """
    In-memory representation of a dataset for API compatibility.

    Backed by the ``dataset_records`` SQL table (BQ-111) but presented
    to callers with the same attribute interface as before so that
    routers and other services need no changes.
    """

    def __init__(
        self,
        dataset_id: str,
        original_filename: str,
        file_type: str,
    ):
        self.id = dataset_id
        self.original_filename = original_filename
        self.file_type = file_type
        self.status = DatasetStatus.UPLOADED
        self.error: Optional[str] = None
        self.created_at = datetime.now(timezone.utc)
        self.updated_at = datetime.now(timezone.utc)
        self.upload_path: Optional[Path] = None
        self.processed_path: Optional[Path] = None
        self.metadata: Dict[str, Any] = {}
        self.document_content: Optional[Dict[str, Any]] = None  # For document types
        # BQ-108+109: Batch + preview fields
        self.batch_id: Optional[str] = None
        self.relative_path: Optional[str] = None
        self.preview_text: Optional[str] = None
        self.preview_metadata: Optional[Dict[str, Any]] = None
        self.confirmed_at: Optional[datetime] = None
        self.confirmed_by: Optional[str] = None
        self.file_size_bytes: int = 0

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "id": self.id,
            "original_filename": self.original_filename,
            "file_type": self.file_type,
            "status": self.status.value if isinstance(self.status, ProcessingStatus) else self.status,
            "error": self.error,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, datetime) else self.created_at,
            "updated_at": self.updated_at.isoformat() if isinstance(self.updated_at, datetime) else self.updated_at,
            "upload_path": str(self.upload_path) if self.upload_path else None,
            "processed_path": str(self.processed_path) if self.processed_path else None,
            "metadata": self.metadata,
        }
        if self.document_content:
            result["document_info"] = {
                "text_blocks": self.document_content.get("metadata", {}).get("text_blocks", 0),
                "table_count": self.document_content.get("metadata", {}).get("table_count", 0),
                "processor": self.document_content.get("metadata", {}).get("processor", "unknown"),
            }
        return result


# File type categories
TABULAR_TYPES = {'csv', 'tsv', 'json', 'parquet'}
DOCUMENT_TYPES = {
    # Existing (Unstructured)
    'pdf', 'docx', 'doc', 'pptx', 'ppt',
    # Tika-powered (BQ-TIKA)
    'rtf', 'odt', 'ods', 'odp', 'epub',
    'eml', 'msg', 'mbox',
    'xml', 'rss',
    'pages', 'numbers', 'key',
    'wps', 'wpd',
    'ics', 'vcf',
}
SPREADSHEET_TYPES = {'xlsx', 'xls'}
TEXT_TYPES = {'txt', 'md', 'html', 'htm'}


def _db_to_record(db_row) -> DatasetRecord:
    """Convert a DatasetRecord DB model to the in-memory DatasetRecord."""
    rec = DatasetRecord(
        dataset_id=db_row.id,
        original_filename=db_row.original_filename,
        file_type=db_row.file_type,
    )
    # Map legacy status values to new DatasetStatus enum
    status_map = {
        "uploading": DatasetStatus.UPLOADED,
        "processing": DatasetStatus.READY,
        "failed": DatasetStatus.ERROR,
    }
    raw_status = db_row.status
    if raw_status in status_map:
        rec.status = status_map[raw_status]
    else:
        try:
            rec.status = DatasetStatus(raw_status)
        except ValueError:
            rec.status = DatasetStatus.ERROR

    rec.created_at = db_row.created_at
    rec.updated_at = db_row.updated_at
    rec.processed_path = Path(db_row.processed_path) if db_row.processed_path else None
    rec.file_size_bytes = db_row.file_size_bytes or 0

    # Deserialize metadata_json
    try:
        meta = json.loads(db_row.metadata_json) if db_row.metadata_json else {}
    except (json.JSONDecodeError, TypeError):
        meta = {}

    # Extract document_content from metadata if present
    rec.document_content = meta.pop("document_content", None)
    rec.metadata = meta

    # Reconstruct upload_path from storage_filename
    upload_dir = Path(settings.upload_directory)
    if db_row.storage_filename and db_row.storage_filename != "__migration_marker__":
        rec.upload_path = upload_dir / db_row.storage_filename

    # Extract error from metadata if stored there
    rec.error = meta.pop("error", None)

    # BQ-108+109 fields
    rec.batch_id = getattr(db_row, "batch_id", None)
    rec.relative_path = getattr(db_row, "relative_path", None)
    rec.preview_text = getattr(db_row, "preview_text", None)
    rec.confirmed_at = getattr(db_row, "confirmed_at", None)
    rec.confirmed_by = getattr(db_row, "confirmed_by", None)

    # Deserialize preview_metadata JSON
    raw_pm = getattr(db_row, "preview_metadata", None)
    if raw_pm:
        try:
            rec.preview_metadata = json.loads(raw_pm)
        except (json.JSONDecodeError, TypeError):
            rec.preview_metadata = None
    else:
        rec.preview_metadata = None

    return rec


def _record_to_db(rec: DatasetRecord, storage_filename: str):
    """Create a DB model dict from the in-memory DatasetRecord."""
    from app.models.dataset import DatasetRecord as DBDatasetRecord

    metadata = dict(rec.metadata)
    if rec.document_content:
        metadata["document_content"] = rec.document_content
    if rec.error:
        metadata["error"] = rec.error

    file_size = rec.file_size_bytes or 0
    if not file_size and rec.upload_path:
        try:
            file_size = os.path.getsize(rec.upload_path)
            _log.debug("_record_to_db: file_size_bytes was 0, resolved %d from disk for %s", file_size, rec.id)
        except OSError:
            _log.warning("_record_to_db: file_size_bytes=0 and cannot stat %s for %s", rec.upload_path, rec.id)

    return DBDatasetRecord(
        id=rec.id,
        original_filename=rec.original_filename,
        storage_filename=storage_filename,
        file_type=rec.file_type,
        file_size_bytes=file_size,
        status=rec.status.value if isinstance(rec.status, DatasetStatus) else rec.status,
        processed_path=str(rec.processed_path) if rec.processed_path else None,
        metadata_json=json.dumps(metadata, default=str),
        created_at=rec.created_at,
        updated_at=rec.updated_at,
        batch_id=rec.batch_id,
        relative_path=rec.relative_path,
        preview_text=rec.preview_text,
        preview_metadata=json.dumps(rec.preview_metadata, default=str) if rec.preview_metadata else None,
        confirmed_at=rec.confirmed_at,
        confirmed_by=rec.confirmed_by,
    )


class ProcessingService:
    """Handles file processing and conversion to Parquet.

    BQ-111: Dataset records are now persisted in the ``dataset_records``
    SQL table instead of an in-memory dict + datasets.json file.
    """

    def __init__(self):
        self.upload_dir = Path(settings.upload_directory)
        self.processed_dir = Path(settings.processed_directory)

        # Create directories
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_session():
        from app.core.database import get_session_context
        return get_session_context()

    def _save_record(self, rec: DatasetRecord, storage_filename: str) -> None:
        """Upsert a DatasetRecord into the database."""
        from app.models.dataset import DatasetRecord as DBDatasetRecord
        from app.core.database import _sqlite_retry, _is_sqlite

        def _do():
            with self._get_session() as session:
                existing = session.get(DBDatasetRecord, rec.id)
                if existing is not None:
                    existing.original_filename = rec.original_filename
                    existing.storage_filename = storage_filename
                    existing.file_type = rec.file_type
                    existing.status = rec.status.value if isinstance(rec.status, DatasetStatus) else rec.status
                    existing.processed_path = str(rec.processed_path) if rec.processed_path else None

                    metadata = dict(rec.metadata)
                    if rec.document_content:
                        metadata["document_content"] = rec.document_content
                    if rec.error:
                        metadata["error"] = rec.error
                    existing.metadata_json = json.dumps(metadata, default=str)
                    existing.updated_at = datetime.now(timezone.utc)

                    if rec.upload_path:
                        try:
                            existing.file_size_bytes = os.path.getsize(rec.upload_path)
                        except OSError:
                            pass

                    # BQ-108+109 fields
                    existing.batch_id = rec.batch_id
                    existing.relative_path = rec.relative_path
                    existing.preview_text = rec.preview_text
                    existing.preview_metadata = json.dumps(rec.preview_metadata, default=str) if rec.preview_metadata else None
                    existing.confirmed_at = rec.confirmed_at
                    existing.confirmed_by = rec.confirmed_by

                    session.add(existing)
                else:
                    db_rec = _record_to_db(rec, storage_filename)
                    session.add(db_rec)
                session.commit()

        if _is_sqlite:
            _sqlite_retry(_do)
        else:
            _do()

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def create_dataset(self, original_filename: str, file_type: str) -> DatasetRecord:
        """Create a new dataset record for an incoming upload."""
        dataset_id = str(uuid.uuid4())[:8]  # Short ID
        record = DatasetRecord(
            dataset_id=dataset_id,
            original_filename=original_filename,
            file_type=file_type,
        )

        # Sanitize the user-supplied filename before constructing the storage path
        safe_name = sanitize_filename(original_filename)
        safe_filename = f"{dataset_id}_{safe_name}"
        upload_path = (self.upload_dir / safe_filename).resolve()

        # Verify the resolved path stays within the upload directory (no traversal)
        if not str(upload_path).startswith(str(self.upload_dir.resolve())):
            raise ValueError("Invalid filename: path traversal detected")

        record.upload_path = upload_path
        # Store original filename in metadata for display purposes
        record.metadata["original_filename"] = original_filename

        self._save_record(record, safe_filename)
        return record

    def get_dataset(self, dataset_id: str) -> Optional[DatasetRecord]:
        """Get a dataset record by ID."""
        from app.models.dataset import DatasetRecord as DBDatasetRecord

        with self._get_session() as session:
            db_row = session.get(DBDatasetRecord, dataset_id)
            if db_row is None or db_row.id == "__migrated__":
                return None
            return _db_to_record(db_row)

    def list_datasets(self) -> list[DatasetRecord]:
        """List all dataset records."""
        from app.models.dataset import DatasetRecord as DBDatasetRecord
        from sqlmodel import select

        with self._get_session() as session:
            stmt = (
                select(DBDatasetRecord)
                .where(DBDatasetRecord.id != "__migrated__")
                .where(DBDatasetRecord.status != "deleted")
                .order_by(DBDatasetRecord.created_at.desc())
            )
            rows = session.exec(stmt).all()
            return [_db_to_record(r) for r in rows]


    def find_by_filename(self, filename: str) -> Optional["DatasetRecord"]:
        """Find an existing non-deleted dataset with the same original filename."""
        from app.models.dataset import DatasetRecord as DBDatasetRecord
        from sqlmodel import select

        with self._get_session() as session:
            stmt = (
                select(DBDatasetRecord)
                .where(DBDatasetRecord.original_filename == filename)
                .where(DBDatasetRecord.id != "__migrated__")
                .where(DBDatasetRecord.status != "deleted")
                .order_by(DBDatasetRecord.created_at.desc())
                .limit(1)
            )
            row = session.exec(stmt).first()
            if row is None:
                return None
            return _db_to_record(row)

    def delete_dataset(self, dataset_id: str) -> bool:
        """Delete a dataset and its files."""
        record = self.get_dataset(dataset_id)
        if not record:
            return False

        # Delete files
        if record.upload_path and record.upload_path.exists():
            record.upload_path.unlink()
        if record.processed_path and record.processed_path.exists():
            record.processed_path.unlink()

        # Remove from DB
        from app.models.dataset import DatasetRecord as DBDatasetRecord

        with self._get_session() as session:
            db_row = session.get(DBDatasetRecord, dataset_id)
            if db_row:
                session.delete(db_row)
                session.commit()
        return True

    def _is_cancelled(self, dataset_id: str) -> bool:
        """Check if a dataset has been cancelled (call between phases)."""
        from app.models.dataset import DatasetRecord as DBDatasetRecord
        with self._get_session() as session:
            db_row = session.get(DBDatasetRecord, dataset_id)
            if db_row and db_row.status == DatasetStatus.CANCELLED.value:
                return True
        return False

    def _set_status(self, dataset_id: str, status: DatasetStatus) -> None:
        """Atomically set dataset status in DB."""
        from app.models.dataset import DatasetRecord as DBDatasetRecord
        from app.core.database import _sqlite_retry, _is_sqlite

        def _do():
            with self._get_session() as session:
                db_row = session.get(DBDatasetRecord, dataset_id)
                if db_row:
                    db_row.status = status.value
                    db_row.updated_at = datetime.now(timezone.utc)
                    session.add(db_row)
                    session.commit()

        if _is_sqlite:
            _sqlite_retry(_do)
        else:
            _do()

    def _cache_preview(self, record: DatasetRecord) -> None:
        """Populate preview_text and preview_metadata after extraction."""
        preview_text = None
        preview_meta: Dict[str, Any] = {
            "file_type": record.file_type,
            "size_bytes": record.file_size_bytes,
        }

        # Extract preview text
        if record.document_content:
            # Use pre-computed preview if available (streaming path)
            doc_meta = record.document_content.get("metadata", {})
            if doc_meta.get("preview_text"):
                preview_text = doc_meta["preview_text"][:500]
            else:
                # Legacy path: take first block only, never join all
                text_blocks = record.document_content.get("text_content", [])
                if text_blocks:
                    first = text_blocks[0]
                    preview_text = (first.get("text", "") if isinstance(first, dict) else str(first))[:500]
                else:
                    preview_text = None
            preview_meta["kind"] = "document"
        elif record.processed_path and record.processed_path.exists():
            # For tabular: get sample rows + schema via DuckDB
            try:
                duckdb = get_duckdb_service()
                meta = duckdb.get_file_metadata(record.processed_path)
                preview_meta["row_count_estimate"] = meta.get("row_count", 0)
                preview_meta["column_count"] = meta.get("column_count", 0)
                preview_meta["kind"] = "tabular"

                # Schema preview
                profiles = duckdb.get_column_profile(record.processed_path)
                preview_meta["schema"] = [
                    {"name": p["name"], "type": p.get("type", "UNKNOWN")}
                    for p in profiles[:50]
                ]

                # Sample rows for preview
                sample = duckdb.get_sample_rows(record.processed_path, limit=5)
                preview_meta["sample_rows"] = sample

                # First 500 chars of text representation
                if sample:
                    text_repr = json.dumps(sample[:3], default=str)
                    preview_text = text_repr[:500]
            except Exception:
                pass

        # Enforce 2KB limit on preview_text
        if preview_text and len(preview_text.encode("utf-8")) > 2048:
            preview_text = preview_text[:500]

        record.preview_text = preview_text
        record.preview_metadata = preview_meta

    # ------------------------------------------------------------------
    # BQ-VZ-LARGE-FILES: Streaming helpers
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def process_file(self, dataset_id: str, skip_indexing: bool = False) -> DatasetRecord:
        """Process an uploaded file based on its type.

        Args:
            dataset_id: The dataset to process.
            skip_indexing: If True, stop after extraction (preview mode).

        BQ-VZ-LARGE-FILES: Routes large files (>= LARGE_FILE_THRESHOLD_MB) to
        the streaming subprocess path. Falls back to in-memory for small files
        if streaming fails (M10 graceful degradation).
        """
        record = self.get_dataset(dataset_id)
        if not record:
            raise ValueError(f"Dataset {dataset_id} not found")

        if not record.upload_path or not record.upload_path.exists():
            record.status = DatasetStatus.ERROR
            record.error = "Upload file not found"
            self._save_record(record, record.upload_path.name if record.upload_path else f"{dataset_id}")
            return record

        # Populate file_size_bytes from disk so fallback size checks are accurate
        if record.upload_path and record.upload_path.exists():
            try:
                record.file_size_bytes = os.path.getsize(record.upload_path)
            except OSError:
                pass

        # Check cancellation before starting
        if self._is_cancelled(dataset_id):
            record.status = DatasetStatus.CANCELLED
            return record

        # Phase 1: Extract
        record.status = DatasetStatus.EXTRACTING
        record.updated_at = datetime.now(timezone.utc)
        self._save_record(record, record.upload_path.name if record.upload_path else f"{dataset_id}")

        try:
            file_type = record.file_type.lower()

            if file_type in (TABULAR_TYPES | DOCUMENT_TYPES):
                # BQ-VZ-LARGE-FILES: Force all documents and tabular files through streaming subprocess
                try:
                    await self._extract_streaming(record)
                    record.metadata["processing_mode"] = "streaming"
                except Exception as streaming_err:
                    # M10: Graceful degradation — fall back to in-memory
                    # for files under fallback_max_size_mb (separate from
                    # the streaming entry threshold so that files between
                    # large_file_threshold_mb and fallback_max_size_mb can
                    # still fall back).
                    file_size = record.file_size_bytes or 0
                    fallback_limit = settings.fallback_max_size_mb * 1024 * 1024
                    if file_size < fallback_limit:
                        # Small enough to fall back to in-memory
                        _log.warning(
                            "Streaming failed for %s (%s, %d bytes), "
                            "falling back to in-memory: %s",
                            record.id, file_type, file_size, streaming_err,
                        )
                        record.metadata["processing_mode"] = "fallback_in_memory"
                        record.metadata["streaming_error"] = str(streaming_err)
                        await self._extract_in_memory(record, file_type)
                    else:
                        # Too large for in-memory fallback
                        _log.error(
                            "Streaming failed for large file %s (%s, %d bytes), "
                            "no fallback available: %s",
                            record.id, file_type, file_size, streaming_err,
                        )
                        raise ValueError(
                            f"Streaming processing failed for large file "
                            f"({file_type}, {file_size / (1024**2):.0f}MB): {streaming_err}"
                        ) from streaming_err
            else:
                # Standard in-memory path (unchanged for small/special files)
                record.metadata["processing_mode"] = "in_memory"
                await self._extract_in_memory(record, file_type)

            # Cache preview data after extraction
            self._cache_preview(record)

        except Exception as e:
            record.status = DatasetStatus.ERROR
            record.error = str(e)
            record.updated_at = datetime.now(timezone.utc)
            storage_fn = record.upload_path.name if record.upload_path else f"{dataset_id}"
            self._save_record(record, storage_fn)
            return record

        # Check cancellation between phases
        if self._is_cancelled(dataset_id):
            record.status = DatasetStatus.CANCELLED
            storage_fn = record.upload_path.name if record.upload_path else f"{dataset_id}"
            self._save_record(record, storage_fn)
            return record

        if skip_indexing:
            record.status = DatasetStatus.PREVIEW_READY
            record.updated_at = datetime.now(timezone.utc)
            storage_fn = record.upload_path.name if record.upload_path else f"{dataset_id}"
            self._save_record(record, storage_fn)
            return record

        # Phase 2: Index
        record.status = DatasetStatus.INDEXING
        record.updated_at = datetime.now(timezone.utc)
        storage_fn = record.upload_path.name if record.upload_path else f"{dataset_id}"
        self._save_record(record, storage_fn)

        try:
            # Run indexing in thread pool so embedding computation
            # doesn't block the event loop (health checks stay responsive)
            await asyncio.get_event_loop().run_in_executor(
                None, self._run_indexing, record,
            )
            record.status = DatasetStatus.READY
            record.updated_at = datetime.now(timezone.utc)
        except Exception as e:
            record.status = DatasetStatus.ERROR
            record.error = f"Indexing failed: {e}"
            record.updated_at = datetime.now(timezone.utc)

        storage_fn = record.upload_path.name if record.upload_path else f"{dataset_id}"
        self._save_record(record, storage_fn)
        return record

    async def _extract_in_memory(self, record: DatasetRecord, file_type: str) -> None:
        """Standard in-memory extraction path (unchanged from original)."""
        if file_type in TABULAR_TYPES:
            await self._extract_tabular(record)
        elif file_type in DOCUMENT_TYPES:
            await self._process_document(record)
        elif file_type in SPREADSHEET_TYPES:
            await self._process_spreadsheet(record)
        elif file_type in TEXT_TYPES:
            await self._process_text(record)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    async def _extract_streaming(self, record: DatasetRecord) -> None:
        """BQ-VZ-LARGE-FILES: Extract via streaming subprocess.

        Routes tabular files through StreamingTabularProcessor and
        document files through StreamingDocumentProcessor, both running
        in isolated subprocesses via ProcessWorkerManager.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq
        from app.services.process_worker import (
            get_worker_manager,
            deserialize_record_batch,
        )

        file_type = record.file_type.lower()
        parquet_filename = f"{record.id}.parquet"
        partial_path = self.processed_dir / f"{record.id}.parquet.partial"
        final_path = self.processed_dir / parquet_filename

        manager = get_worker_manager()

        if file_type in TABULAR_TYPES:
            handle = manager.submit_tabular(record.upload_path, file_type)

            # Estimate total rows from file size (~150 bytes/row avg) for progress
            file_bytes = record.file_size_bytes or 0
            estimated_rows = max(file_bytes / 150, 1)

            # M3: Consume RecordBatch chunks and write Parquet incrementally
            # with configurable row group size targeting PARQUET_ROW_GROUP_SIZE_MB.
            writer: Optional[pq.ParquetWriter] = None
            rows_total = 0
            row_group_target_rows: Optional[int] = None
            try:
                for raw_data in handle.iter_data():
                    batch = deserialize_record_batch(raw_data)
                    if writer is None:
                        writer = pq.ParquetWriter(
                            str(partial_path),
                            batch.schema,
                            compression="zstd",
                        )
                        # Estimate target row count per row group from first batch.
                        # nbytes gives the in-memory size; Parquet on-disk will be
                        # smaller due to compression, but in-memory is a reasonable
                        # proxy for controlling memory pressure during writes.
                        target_bytes = settings.parquet_row_group_size_mb * 1024 * 1024
                        bytes_per_row = max(batch.nbytes / max(batch.num_rows, 1), 1)
                        row_group_target_rows = max(int(target_bytes / bytes_per_row), 1024)
                        # Refine estimate with actual bytes-per-row from first batch
                        estimated_rows = max(file_bytes / bytes_per_row, 1)

                    writer.write_batch(batch, row_group_size=row_group_target_rows)
                    rows_total += batch.num_rows

                    # Update progress (cap extraction at 90%)
                    from app.services.processing_queue import get_processing_queue
                    pct = min((rows_total / estimated_rows) * 90, 90)
                    get_processing_queue().update_progress(
                        record.id, "extracting", pct,
                        f"{rows_total:,} rows extracted",
                    )
            except Exception:
                # M3: On error/crash, clean up .partial and re-raise
                if writer:
                    writer.close()
                    writer = None
                if partial_path.exists():
                    partial_path.unlink()
                raise
            finally:
                if writer:
                    writer.close()

            # Check worker result
            progress = handle.get_progress()
            if progress and progress.get("status") == "error":
                # Clean up partial file on worker-reported error
                if partial_path.exists():
                    partial_path.unlink()
                raise RuntimeError(progress.get("error", "Unknown worker error"))

            # Atomic rename: .partial → final
            if partial_path.exists():
                partial_path.rename(final_path)
            record.processed_path = final_path

            # Extract metadata from the final Parquet
            try:
                duckdb = get_duckdb_service()
                record.metadata = duckdb.get_file_metadata(record.processed_path)
                record.metadata["column_profiles"] = duckdb.get_column_profile(
                    record.processed_path
                )
                record.metadata["sample_rows"] = duckdb.get_sample_rows(
                    record.processed_path, limit=5
                )
                record.metadata["streaming_rows_total"] = rows_total
            except Exception as e:
                _log.exception("Metadata extraction failed for %s", record.id)
                record.metadata = {
                    "extraction_error": {
                        "code": "METADATA_EXTRACTION_FAILED",
                        "type": type(e).__name__,
                    },
                    "streaming_rows_total": rows_total,
                }

            from app.services.processing_queue import get_processing_queue
            get_processing_queue().update_progress(
                record.id, "extracting", 100, "Extraction complete",
            )

        elif file_type in DOCUMENT_TYPES:
            handle = manager.submit_document(record.upload_path, file_type)

            # Incremental Parquet writing — never accumulate all blocks in memory
            doc_schema = pa.schema([
                ("block_index", pa.int64()),
                ("block_type", pa.string()),
                ("page_number", pa.int64()),
                ("content", pa.string()),
            ])

            doc_writer = None
            block_idx = 0
            text_blocks_count = 0
            table_count = 0
            first_text_preview = None
            # Estimate total pages from file size (~5KB/page avg) for progress
            doc_file_bytes = record.file_size_bytes or 0
            estimated_pages = max(doc_file_bytes / 5000, 1)

            try:
                for block_dict in handle.iter_data():
                    page = int(block_dict.get("page_num") or 0)
                    text = block_dict.get("text") or ""

                    if text.strip():
                        if first_text_preview is None:
                            first_text_preview = text[:500]
                        batch = pa.RecordBatch.from_arrays([
                            pa.array([block_idx], type=pa.int64()),
                            pa.array(["Text"]),
                            pa.array([page], type=pa.int64()),
                            pa.array([text]),
                        ], schema=doc_schema)
                        if doc_writer is None:
                            doc_writer = pq.ParquetWriter(str(partial_path), doc_schema, compression="zstd")
                        doc_writer.write_batch(batch)
                        block_idx += 1
                        text_blocks_count += 1

                    for tbl in block_dict.get("tables", []) or []:
                        batch = pa.RecordBatch.from_arrays([
                            pa.array([block_idx], type=pa.int64()),
                            pa.array(["Table"]),
                            pa.array([page], type=pa.int64()),
                            pa.array([tbl]),
                        ], schema=doc_schema)
                        if doc_writer is None:
                            doc_writer = pq.ParquetWriter(str(partial_path), doc_schema, compression="zstd")
                        doc_writer.write_batch(batch)
                        block_idx += 1
                        table_count += 1

                    # Update progress (cap at 90% for extraction)
                    from app.services.processing_queue import get_processing_queue
                    pct = min((page / estimated_pages) * 90, 90)
                    get_processing_queue().update_progress(
                        record.id, "extracting", pct,
                        f"{text_blocks_count} blocks, {table_count} tables",
                    )
            except Exception:
                if doc_writer:
                    doc_writer.close()
                    doc_writer = None
                if partial_path.exists():
                    partial_path.unlink()
                raise
            finally:
                if doc_writer:
                    doc_writer.close()

            # Check worker result
            progress = handle.get_progress()
            if progress and progress.get("status") == "error":
                if partial_path.exists():
                    partial_path.unlink()
                raise RuntimeError(progress.get("error", "Unknown worker error"))

            # Atomic rename
            if partial_path.exists():
                partial_path.rename(final_path)
            record.processed_path = final_path

            # Store SUMMARY metadata only — data lives in Parquet
            record.document_content = {
                "text_content": [],
                "tables": [],
                "metadata": {
                    "filename": record.original_filename,
                    "file_type": file_type,
                    "text_blocks": text_blocks_count,
                    "table_count": table_count,
                    "processor": "streaming_subprocess",
                    "preview_text": first_text_preview,
                },
            }

            try:
                duckdb = get_duckdb_service()
                metadata = duckdb.get_file_metadata(record.processed_path)
                metadata["source_type"] = "document"
                metadata["original_format"] = file_type
                metadata["text_blocks"] = text_blocks_count
                metadata["tables_extracted"] = table_count
                record.metadata = metadata
            except Exception as e:
                _log.exception("Metadata extraction failed for %s", record.id)
                record.metadata = {
                    "source_type": "document",
                    "text_blocks": text_blocks_count,
                    "tables_extracted": table_count,
                }

            from app.services.processing_queue import get_processing_queue
            get_processing_queue().update_progress(
                record.id, "extracting", 100, "Extraction complete",
            )
        else:
            raise ValueError(f"Streaming not supported for file type: {file_type}")

    def _run_indexing(self, record: DatasetRecord) -> None:
        """Phase 2: chunk → embed → Qdrant via streaming for memory safety.

        Always uses the streaming indexing path (index_streaming) isolated
        in a ProcessWorkerManager subprocess. This prevents the primary API
        server from encountering MemoryErrors when PyTorch embeddings process
        heavy arrays.
        """
        import time
        if not record.processed_path or not record.processed_path.exists():
            return

        # Guard: defer indexing when available memory is critically low
        import psutil
        available_mb = psutil.virtual_memory().available / (1024 * 1024)
        if available_mb < 500:
            _log.warning(
                "Low memory (%.0fMB free), deferring indexing for %s",
                available_mb, record.id,
            )
            record.metadata["index_status"] = {"status": "deferred", "reason": "low_memory"}
            return

        try:
            from app.services.process_worker import get_worker_manager
            from app.services.processing_queue import get_processing_queue

            manager = get_worker_manager()
            handle = manager.submit_indexing(record.id, record.processed_path)

            timeout_s = settings.process_worker_timeout_s * 2  # Indexing gets 2x timeout
            start_time = time.monotonic()

            # Subprocess spawned. Block to poll worker progress
            while getattr(handle.future, "is_alive", lambda: False)():
                elapsed = time.monotonic() - start_time
                if elapsed > timeout_s:
                    handle.cancel()
                    raise TimeoutError(f"Indexing worker exceeded {timeout_s}s timeout")

                progress = handle.get_progress()
                if progress:
                    state = progress.get("status")
                    if state == "processing":
                        get_processing_queue().update_progress(
                            record.id, "indexing", progress.get("pct", 50),
                            f"{progress.get('rows_done', 0):,} / {progress.get('total_rows', 0):,} rows indexed"
                        )
                    elif state == "completed":
                        record.metadata["index_status"] = progress.get("result", {"status": "success"})
                        break
                    elif state == "error":
                        raise RuntimeError(progress.get("error", "Unknown worker error"))
                    elif state == "cancelled":
                        raise InterruptedError("Worker cancelled externally")
                time.sleep(1)
                
            # Ensures cleanup of handles
            handle.wait()
            
            if "index_status" not in record.metadata:
                # Fallback if completion packet missed but worker dead
                record.metadata["index_status"] = {"status": "error", "error": "Index worker exited unexpectedly without completion packet"}

        except Exception as e:
            _log.error("Indexing failed for dataset %s: %s", record.id, e, exc_info=True)
            record.metadata["index_status"] = {"status": "error", "error": str(e)}

        # PII scan
        try:
            from app.services.pii_service import get_pii_service
            pii_service = get_pii_service()
            pii_result = pii_service.scan_dataset(record.processed_path)
            record.metadata["pii_scan"] = pii_result
        except Exception as e:
            record.metadata["pii_scan"] = {"status": "scan_failed", "error": str(e)}

    async def run_index_phase(self, dataset_id: str) -> DatasetRecord:
        """Run only the index phase (called after confirm)."""
        record = self.get_dataset(dataset_id)
        if not record:
            raise ValueError(f"Dataset {dataset_id} not found")

        if self._is_cancelled(dataset_id):
            record.status = DatasetStatus.CANCELLED
            return record

        record.status = DatasetStatus.INDEXING
        record.updated_at = datetime.now(timezone.utc)
        storage_fn = record.upload_path.name if record.upload_path else f"{dataset_id}"
        self._save_record(record, storage_fn)

        try:
            # Run indexing in thread pool so embedding computation
            # doesn't block the event loop (health checks stay responsive)
            await asyncio.get_event_loop().run_in_executor(
                None, self._run_indexing, record,
            )
            if self._is_cancelled(dataset_id):
                record.status = DatasetStatus.CANCELLED
            else:
                record.status = DatasetStatus.READY
            record.updated_at = datetime.now(timezone.utc)
        except Exception as e:
            record.status = DatasetStatus.ERROR
            record.error = f"Indexing failed: {e}"
            record.updated_at = datetime.now(timezone.utc)

        self._save_record(record, storage_fn)
        return record

    # Maximum time (seconds) allowed for converting a file to Parquet.
    # Large CSVs/TSVs (400MB+) may need several minutes.
    EXTRACT_TIMEOUT_S = 300  # 5 minutes

    MAX_PROCESSING_ATTEMPTS = 3

    def recover_stuck_records(self) -> int:
        """On startup: find records stuck in processing states and handle retries."""
        from app.models.dataset import DatasetRecord as DBDatasetRecord
        from sqlmodel import select

        with self._get_session() as session:
            stmt = select(DBDatasetRecord).where(
                DBDatasetRecord.status.in_(["extracting", "indexing"])
            )
            stuck = session.exec(stmt).all()
            recovered = 0
            for row in stuck:
                meta = json.loads(row.metadata_json) if row.metadata_json else {}
                attempts = meta.get("processing_attempts", 0) + 1
                meta["processing_attempts"] = attempts

                if attempts >= self.MAX_PROCESSING_ATTEMPTS:
                    row.status = "error"
                    meta["error"] = f"Failed after {attempts} attempts (likely OOM for large file)"
                    meta["error_permanent"] = True
                    _log.warning("Marking %s as permanently failed after %d attempts", row.id, attempts)
                else:
                    row.status = "uploaded"
                    _log.info("Resetting %s for retry (attempt %d/%d)", row.id, attempts, self.MAX_PROCESSING_ATTEMPTS)

                row.metadata_json = json.dumps(meta, default=str)
                row.updated_at = datetime.now(timezone.utc)
                session.add(row)
                recovered += 1

            if recovered:
                session.commit()
            return recovered

    async def _extract_tabular(self, record: DatasetRecord):
        """Extract phase for tabular files (CSV, JSON, Parquet) - convert to Parquet."""
        import logging
        _log = logging.getLogger(__name__)

        file_size = record.file_size_bytes or 0
        file_mb = file_size / (1024 * 1024)

        duckdb = get_duckdb_service()

        # Determine output parquet path
        parquet_filename = f"{record.id}.parquet"
        record.processed_path = self.processed_dir / parquet_filename

        # Get the read function for this file type
        read_func = duckdb.get_read_function(record.file_type, str(record.upload_path))

        # Convert to Parquet using streaming COPY
        safe_out = sql_quote_literal(str(record.processed_path))
        copy_query = f"""
            COPY (SELECT * FROM {read_func})
            TO '{safe_out}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """

        def _run_conversion(conn, query):
            conn.execute(query)

        try:
            await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None, _run_conversion, duckdb.connection, copy_query,
                ),
                timeout=self.EXTRACT_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            _log.error(
                "Parquet conversion timed out for %s (%s, %.0fMB) after %ds",
                record.id, record.file_type, file_mb, self.EXTRACT_TIMEOUT_S,
            )
            raise ValueError(
                f"File processing timed out ({record.file_type}, {file_mb:.0f}MB). "
                f"Files over ~400MB may require more processing time."
            )
        except Exception as e:
            err_msg = str(e)
            _log.error(
                "Parquet conversion failed for %s (%s, %d bytes): %s",
                record.id, record.file_type, file_size, err_msg,
            )
            # Retry with a fresh connection and higher memory limit for large files
            if file_size > 100 * 1024 * 1024:  # >100MB
                _log.info("Retrying large file %s with dedicated connection", record.id)
                # Cap at 60% of detected worker memory, never exceed 8GB
                mem_cap_mb = min(int(settings.process_worker_memory_limit_mb * 0.6), 8192)
                large_conn = duckdb.create_ephemeral_connection(
                    memory_limit=f"{mem_cap_mb}MB", threads=4,
                )
                try:
                    large_read_func = duckdb.get_read_function(record.file_type, str(record.upload_path))
                    large_query = (
                        f"COPY (SELECT * FROM {large_read_func}) "
                        f"TO '{safe_out}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                    await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(
                            None, _run_conversion, large_conn, large_query,
                        ),
                        timeout=self.EXTRACT_TIMEOUT_S,
                    )
                except asyncio.TimeoutError:
                    raise ValueError(
                        f"Large file processing timed out ({record.file_type}, {file_mb:.0f}MB). "
                        f"Maximum processing time is {self.EXTRACT_TIMEOUT_S}s."
                    )
                except Exception as retry_err:
                    raise ValueError(
                        f"Large file conversion failed ({record.file_type}, "
                        f"{file_mb:.0f}MB): {retry_err}"
                    ) from retry_err
                finally:
                    large_conn.close()
            else:
                raise ValueError(
                    f"Parquet conversion failed ({record.file_type}, "
                    f"{file_mb:.0f}MB): {err_msg}"
                ) from e

        # Extract comprehensive metadata from the processed Parquet file
        try:
            record.metadata = duckdb.get_file_metadata(record.processed_path)
            record.metadata['column_profiles'] = duckdb.get_column_profile(record.processed_path)
            record.metadata['sample_rows'] = duckdb.get_sample_rows(record.processed_path, limit=5)
        except Exception as e:
            _log.exception("Metadata extraction failed for %s", record.id)
            record.metadata = {"extraction_error": {"code": "METADATA_EXTRACTION_FAILED", "type": type(e).__name__}}

    async def _process_document(self, record: DatasetRecord):
        """Process document files (PDF, Word, PowerPoint) using Unstructured."""
        from app.services.document_service import get_document_service

        doc_service = get_document_service()

        # Process document to extract content
        content = doc_service.process_document(record.upload_path)
        record.document_content = content

        # Convert extracted text to a searchable Parquet file
        parquet_filename = f"{record.id}.parquet"
        record.processed_path = self.processed_dir / parquet_filename

        # Create CSV from extracted content, then convert to Parquet
        temp_csv = self.processed_dir / f"{record.id}_temp.csv"

        try:
            with open(temp_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['block_index', 'block_type', 'page_number', 'content'])

                for i, block in enumerate(content["text_content"]):
                    page_num = block.get("metadata", {}).get("page_number", 0)
                    writer.writerow([
                        i,
                        block.get("type", "Text"),
                        page_num,
                        block.get("text", "")
                    ])

                # Add tables as separate rows
                for i, table in enumerate(content["tables"]):
                    page_num = table.get("metadata", {}).get("page_number", 0)
                    writer.writerow([
                        len(content["text_content"]) + i,
                        "Table",
                        page_num,
                        table.get("content", "")
                    ])

            # Convert to Parquet
            duckdb = get_duckdb_service()
            safe_csv = sql_quote_literal(str(temp_csv))
            safe_out = sql_quote_literal(str(record.processed_path))
            copy_query = f"""
                COPY (SELECT * FROM read_csv_auto('{safe_csv}'))
                TO '{safe_out}'
                (FORMAT PARQUET, COMPRESSION ZSTD)
            """
            duckdb.connection.execute(copy_query)

            # Extract metadata
            metadata = duckdb.get_file_metadata(record.processed_path)
            metadata["source_type"] = "document"
            metadata["original_format"] = record.file_type
            metadata["text_blocks"] = len(content["text_content"])
            metadata["tables_extracted"] = len(content["tables"])
            record.metadata = metadata

        finally:
            # Clean up temp file
            if temp_csv.exists():
                temp_csv.unlink()

    async def _process_text(self, record: DatasetRecord):
        """Process text files (.txt, .md, .html, .htm) using TextProcessor."""
        from app.services.text_processor import TextProcessor

        processor = TextProcessor()
        content = processor.process(record.upload_path)

        # For large text files (>1MB), truncate stored text but keep full metadata
        MAX_TEXT_BYTES = 1 * 1024 * 1024  # 1MB
        full_text = content["text_content"]
        if len(full_text.encode("utf-8", errors="replace")) > MAX_TEXT_BYTES:
            truncated_text = full_text[:MAX_TEXT_BYTES]
            content["metadata"]["truncated"] = True
            content["metadata"]["original_char_count"] = content["metadata"]["char_count"]
        else:
            truncated_text = full_text

        record.document_content = content

        # Convert to a searchable Parquet file
        parquet_filename = f"{record.id}.parquet"
        record.processed_path = self.processed_dir / parquet_filename

        temp_csv = self.processed_dir / f"{record.id}_temp.csv"

        try:
            with open(temp_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['block_index', 'block_type', 'content'])
                writer.writerow([0, 'text', truncated_text])

            duckdb = get_duckdb_service()
            safe_csv = sql_quote_literal(str(temp_csv))
            safe_out = sql_quote_literal(str(record.processed_path))
            copy_query = f"""
                COPY (SELECT * FROM read_csv_auto('{safe_csv}'))
                TO '{safe_out}'
                (FORMAT PARQUET, COMPRESSION ZSTD)
            """
            duckdb.connection.execute(copy_query)

            metadata = duckdb.get_file_metadata(record.processed_path)
            metadata["source_type"] = "text"
            metadata["original_format"] = record.file_type
            metadata.update(content["metadata"])
            record.metadata = metadata

        finally:
            if temp_csv.exists():
                temp_csv.unlink()

    async def _process_spreadsheet(self, record: DatasetRecord):
        """Process Excel spreadsheets (.xlsx, .xls) via pandas."""
        import pandas as pd

        duckdb = get_duckdb_service()

        parquet_filename = f"{record.id}.parquet"
        record.processed_path = self.processed_dir / parquet_filename

        ext = record.upload_path.suffix.lower()

        # Select the right engine: openpyxl for .xlsx, xlrd for .xls
        engine = "xlrd" if ext == ".xls" else "openpyxl"

        try:
            xlsx = pd.ExcelFile(record.upload_path, engine=engine)
            sheet_names = xlsx.sheet_names

            # Read first sheet
            df = pd.read_excel(xlsx, sheet_name=0)

            # Save to Parquet
            df.to_parquet(record.processed_path, compression='zstd', index=False)

            metadata = duckdb.get_file_metadata(record.processed_path)
            metadata["source_type"] = "spreadsheet"
            metadata["original_format"] = record.file_type
            metadata["sheet_names"] = sheet_names
            metadata["sheets_count"] = len(sheet_names)
            record.metadata = metadata

        except Exception as e:
            raise ValueError(
                f"Excel processing failed for {record.original_filename} "
                f"(engine={engine}): {e}"
            )


# Singleton instance
_processing_service: Optional[ProcessingService] = None


def get_processing_service() -> ProcessingService:
    """Get the singleton processing service instance."""
    global _processing_service
    if _processing_service is None:
        _processing_service = ProcessingService()
    return _processing_service
