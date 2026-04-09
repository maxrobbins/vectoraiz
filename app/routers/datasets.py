from fastapi import APIRouter, HTTPException, Depends, Request, UploadFile, File, Form, BackgroundTasks, Query
from fastapi.responses import JSONResponse
from typing import List, Optional
from pathlib import Path
import aiofiles
import asyncio
import os
import json
import logging
import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from app.core.async_utils import run_sync
from app.core.errors import VectorAIzError
from app.services.batch_service import _check_magic_bytes
from app.utils.sanitization import validate_path_traversal

from app.config import settings
from app.models.dataset import DatasetStatus
from app.services.duckdb_service import ephemeral_duckdb_service
from app.services.processing_service import (
    get_processing_service,
    ProcessingService,
    ProcessingStatus,
)
from app.services.indexing_service import get_indexing_service, IndexingService
from app.services.attestation_service import AttestationService, get_attestation_service
from app.models.attestation_schemas import QualityAttestation
from app.services.compliance_service import ComplianceService, get_compliance_service
from app.models.compliance_schemas import ComplianceReport
from app.services.listing_metadata_service import ListingMetadataService, get_listing_metadata_service
from app.models.listing_metadata_schemas import ListingMetadata
from app.services.pipeline_service import PipelineService, get_pipeline_service
from app.services.sketch_service import get_sketch_service
from app.services.quality_contract_service import get_quality_contract_service
from app.services.marketplace_push_service import MarketplacePushService, get_marketplace_push_service, MarketplacePushError
from app.services.batch_service import get_batch_service, BatchService
from app.services.preview_service import get_preview_service, PreviewService
from app.services.notification_service import get_notification_service
from app.schemas.batch import ConfirmRequest
from app.auth.api_key_auth import get_current_user, AuthenticatedUser
from app.services.serial_metering import metered, MeterDecision

logger = logging.getLogger(__name__)

router = APIRouter()

from app.services.processing_service import PROCESSABLE_TYPES

# Build dotted extension set from the canonical PROCESSABLE_TYPES
SUPPORTED_EXTENSIONS = {f'.{t}' for t in PROCESSABLE_TYPES}


def get_file_extension(filename: str) -> str:
    """Extract file extension from filename."""
    return Path(filename).suffix.lower().strip()


@router.get("/")
async def list_datasets(
    facets: bool = Query(False, description="Include facet counts in response"),
    processing: ProcessingService = Depends(get_processing_service),
):
    """List all datasets with their processing status. Optionally include facet counts."""
    try:
        records = await asyncio.wait_for(
            asyncio.to_thread(processing.list_datasets),
            timeout=5.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Database query timed out")

    response = {
        "datasets": [r.to_dict() for r in records],
        "count": len(records),
    }

    if facets:
        from app.services.facet_service import get_facets
        response["facets"] = get_facets()

    return response


@router.post("/upload")
async def upload_dataset(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    processing: ProcessingService = Depends(get_processing_service),
    user: AuthenticatedUser = Depends(get_current_user),
    allow_duplicate: bool = Query(False, description="Skip duplicate filename check"),
    batch_id: Optional[str] = Query(None, description="Optional batch ID to group upload notifications"),
    _meter: MeterDecision = Depends(metered("setup")),
):
    """
    Upload a new dataset file.

    Accepts CSV, JSON, Parquet, Excel files.
    Files are streamed to disk, then processed in the background.
    Requires X-API-Key header.
    """
    # Path traversal check on filename
    fname_err = validate_path_traversal(file.filename or "")
    if fname_err:
        raise HTTPException(status_code=422, detail=f"Invalid filename: {fname_err}")

    # Validate file extension against processable types
    extension = get_file_extension(file.filename)
    if extension not in SUPPORTED_EXTENSIONS:
        supported_list = sorted(t for t in PROCESSABLE_TYPES)
        raise HTTPException(
            status_code=422,
            detail=f"File type '{extension}' is not supported. Supported types: {', '.join(supported_list)}",
        )

    file_type = extension[1:]  # Remove the dot

    # Check for duplicate filename (unless explicitly allowed)
    # Only reject true duplicates: same filename AND same file size
    if not allow_duplicate:
        existing = processing.find_by_filename(file.filename)
        if existing:
            # If both sizes are known, only block true duplicates (same name + same size)
            # If either size is unknown, fall back to name-only dedup (safer default)
            sizes_known = existing.file_size_bytes is not None and file.size is not None
            if not sizes_known or existing.file_size_bytes == file.size:
                from datetime import datetime as dt
                return JSONResponse(
                    status_code=409,
                    content={
                        "error": "duplicate_filename",
                        "detail": f"A dataset with filename '{file.filename}' already exists",
                        "existing_dataset": {
                            "id": existing.id,
                            "filename": existing.original_filename,
                            "status": existing.status.value if hasattr(existing.status, 'value') else str(existing.status),
                            "created_at": existing.created_at.isoformat() if isinstance(existing.created_at, dt) else str(existing.created_at),
                        },
                    },
                )

    # Create dataset record
    record = processing.create_dataset(
        original_filename=file.filename,
        file_type=file_type
    )
    
    try:
        # Stream file to disk in chunks
        bytes_written = 0
        magic_header = b""
        async with aiofiles.open(record.upload_path, 'wb') as out_file:
            while chunk := await file.read(settings.chunk_size):
                bytes_written += len(chunk)
                # Capture first 8 bytes for magic-byte validation
                if len(magic_header) < 8:
                    magic_header += chunk[:8 - len(magic_header)]
                await out_file.write(chunk)

        # Update actual file size after write completes
        record.file_size_bytes = bytes_written
        storage_fn = record.upload_path.name
        processing._save_record(record, storage_fn)

        # Magic-byte content-type validation (match bulk upload behavior)
        if not _check_magic_bytes(magic_header, extension):
            try:
                os.unlink(record.upload_path)
            except OSError:
                pass
            processing.delete_dataset(record.id)
            raise HTTPException(
                status_code=422,
                detail=f"File content does not match extension {extension}",
            )

        # Queue background processing (sequential — one file at a time)
        from app.services.processing_queue import get_processing_queue
        await get_processing_queue().submit(record.id)

        # Create success notification
        try:
            get_notification_service().create(
                type="success",
                category="upload",
                title=f"Uploaded: {file.filename}",
                message=f"File uploaded successfully ({bytes_written} bytes)",
                batch_id=batch_id,
                source="upload",
            )
        except Exception:
            logger.warning("Failed to create upload success notification for %s", file.filename)

        return JSONResponse(
            status_code=202,  # Accepted
            content={
                "message": "File uploaded successfully. Processing started.",
                "dataset_id": record.id,
                "status": record.status.value,
                "filename": record.original_filename,
            }
        )

    except HTTPException as he:
        # Create error notification for upload failures
        try:
            get_notification_service().create(
                type="error",
                category="upload",
                title=f"Upload failed: {file.filename}",
                message=he.detail if hasattr(he, 'detail') else "Upload failed",
                metadata_json=json.dumps({
                    "filename": file.filename,
                    "error": he.detail if hasattr(he, 'detail') else "Upload failed",
                    "status_code": he.status_code,
                }),
                batch_id=batch_id,
                source="upload",
            )
        except Exception:
            logger.warning("Failed to create upload error notification for %s", file.filename)
        raise  # Re-raise 413 and 422 without wrapping
    except (ConnectionError, asyncio.CancelledError) as e:
        # BUG-6: Client disconnected during large upload — clean up gracefully
        logger.warning("Upload connection aborted for %s: %s", record.id, type(e).__name__)
        try:
            os.unlink(record.upload_path)
        except OSError:
            pass
        processing.delete_dataset(record.id)
        raise HTTPException(status_code=499, detail="Upload aborted by client")
    except Exception as e:
        # Clean up on failure
        processing.delete_dataset(record.id)
        # Create error notification
        try:
            get_notification_service().create(
                type="error",
                category="upload",
                title=f"Upload failed: {file.filename}",
                message=str(e),
                metadata_json=json.dumps({
                    "filename": file.filename,
                    "error": str(e),
                }),
                batch_id=batch_id,
                source="upload",
            )
        except Exception:
            logger.warning("Failed to create upload error notification for %s", file.filename)
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


async def process_dataset_task(dataset_id: str, skip_indexing: bool = False):
    """Background task to process an uploaded dataset.

    Race-condition fix: after extraction in preview mode, check if the batch
    was already confirmed (confirmed_at set). If so, auto-transition to
    indexing so the user doesn't have to re-confirm.
    """
    processing = get_processing_service()
    await processing.process_file(dataset_id, skip_indexing=skip_indexing)

    if not skip_indexing:
        return

    # Check if the batch was confirmed while extraction was in progress
    from app.core.database import get_session_context
    from app.models.dataset import DatasetRecord as DBDatasetRecord

    should_index = False
    with get_session_context() as session:
        db_row = session.get(DBDatasetRecord, dataset_id)
        if db_row and db_row.confirmed_at and db_row.status == DatasetStatus.PREVIEW_READY.value:
            logger.info(
                "Auto-indexing dataset %s (batch confirmed during extraction)",
                dataset_id,
            )
            db_row.status = DatasetStatus.INDEXING.value
            db_row.updated_at = datetime.now(timezone.utc)
            session.add(db_row)
            session.commit()
            should_index = True

    if should_index:
        await index_dataset_task(dataset_id)


async def index_dataset_task(dataset_id: str):
    """Background task to run index phase after confirm."""
    processing = get_processing_service()
    await processing.run_index_phase(dataset_id)


# ---------------------------------------------------------------------------
# Upload batch summary — called by frontend after individual uploads finish
# ---------------------------------------------------------------------------

class UploadSummaryRequest(BaseModel):
    batch_id: str
    accepted: int = Field(ge=0)
    rejected: int = Field(ge=0)
    failed_filenames: List[str] = []


@router.post("/upload-summary")
async def upload_batch_summary(
    body: UploadSummaryRequest,
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Create a summary notification after a series of individual uploads."""
    total = body.accepted + body.rejected

    if body.rejected == 0:
        summary_type = "info"
        summary_title = "Upload complete"
    elif body.accepted == 0:
        summary_type = "error"
        summary_title = "Upload failed"
    else:
        summary_type = "warning"
        summary_title = "Upload partially complete"

    summary_msg = f"{body.accepted} of {total} files uploaded successfully."
    if body.rejected > 0:
        summary_msg += f" {body.rejected} failed."

    try:
        get_notification_service().create(
            type=summary_type,
            category="upload",
            title=summary_title,
            message=summary_msg,
            metadata_json=json.dumps({
                "accepted": body.accepted,
                "rejected": body.rejected,
                "total": total,
                "failed_filenames": body.failed_filenames,
            }),
            batch_id=body.batch_id,
            source="upload",
        )
    except Exception:
        logger.warning("Failed to create batch summary notification for %s", body.batch_id)

    return {"ok": True}


# ---------------------------------------------------------------------------
# BQ-108+109: Batch upload, preview, confirm endpoints
# ---------------------------------------------------------------------------

@router.post("/batch")
async def batch_upload(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    paths: Optional[str] = Form(default=None),
    mode: str = Form(default="preview"),
    batch_id: Optional[str] = Form(default=None),
    processing: ProcessingService = Depends(get_processing_service),
    _meter: MeterDecision = Depends(metered("setup")),
    batch_service: BatchService = Depends(get_batch_service),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """
    BQ-108: Bulk upload multiple files.
    mode=preview (default) stops at extraction; mode=process auto-indexes.
    """
    if not files:
        raise HTTPException(status_code=422, detail="No files provided")

    # Validate filenames for path traversal
    for i, f in enumerate(files):
        fname_err = validate_path_traversal(f.filename or "")
        if fname_err:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid filename at index {i}: {fname_err}",
            )

    # Parse paths JSON array if provided
    path_list: Optional[List[str]] = None
    if paths:
        try:
            path_list = json.loads(paths)
            if not isinstance(path_list, list):
                raise ValueError
        except (json.JSONDecodeError, ValueError):
            raise HTTPException(status_code=422, detail="paths must be a JSON array of strings")
        if len(path_list) != len(files):
            raise HTTPException(
                status_code=422,
                detail=f"paths length ({len(path_list)}) must match files length ({len(files)})",
            )
        # Path traversal protection — reject null bytes, "..", absolute, and "./" paths
        for i, p in enumerate(path_list):
            path_err = validate_path_traversal(p)
            if path_err:
                raise HTTPException(
                    status_code=422,
                    detail=f"Invalid path at index {i}: {path_err}",
                )
            # Normalize and replace backslashes for consistency
            path_list[i] = os.path.normpath(p).replace("\\", "/")

    # Read first 8 bytes of each file for magic-byte validation, plus sizes
    filenames = []
    sizes = []
    headers = []
    for f in files:
        filenames.append(f.filename or "unnamed")
        header = await f.read(8)
        headers.append(header)
        # Seek back so the file can be fully read later
        await f.seek(0)
        # Estimate size from content-length or header
        sizes.append(f.size or 0)

    # Validate batch
    try:
        accepted_indices, rejected_items = batch_service.validate_batch(
            filenames, sizes, headers, path_list,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    bid = batch_id or f"bch_{uuid.uuid4().hex[:12]}"
    skip_indexing = mode != "process"
    notifications = get_notification_service()

    items: List[dict] = list(rejected_items)  # start with rejected

    # Log notifications for pre-validation rejections
    for rj in rejected_items:
        try:
            notifications.create(
                type="error",
                category="upload",
                title=f"Upload failed: {rj.get('original_filename', 'unknown')}",
                message=f"Rejected: {rj.get('error', 'validation failed')}",
                metadata_json=json.dumps({
                    "filename": rj.get("original_filename"),
                    "error_code": rj.get("error_code"),
                    "error": rj.get("error"),
                }),
                batch_id=bid,
                source="upload",
            )
        except Exception:
            logger.warning("Failed to create rejection notification for %s", rj.get("original_filename"))

    # Save accepted files and create records — per-file fault tolerance
    for idx in accepted_indices:
        f = files[idx]
        fname = filenames[idx]
        ext = Path(fname).suffix.lower()
        file_type = ext[1:] if ext else "unknown"
        rel_path = path_list[idx] if path_list else fname

        try:
            record = batch_service.create_dataset_record(
                filename=fname,
                file_type=file_type,
                file_size=sizes[idx],
                batch_id=bid,
                relative_path=rel_path,
            )

            # Stream file to disk
            bytes_written = 0
            async with aiofiles.open(record.upload_path, "wb") as out_file:
                await f.seek(0)
                while chunk := await f.read(settings.chunk_size):
                    bytes_written += len(chunk)
                    await out_file.write(chunk)

            # Update actual file size
            record.file_size_bytes = bytes_written
            storage_fn = record.upload_path.name
            processing._save_record(record, storage_fn)

            # Queue background extraction (sequential — one file at a time)
            from app.services.processing_queue import get_processing_queue
            await get_processing_queue().submit(record.id, skip_indexing=skip_indexing)

            items.append({
                "client_file_index": idx,
                "original_filename": fname,
                "relative_path": rel_path,
                "size_bytes": record.file_size_bytes,
                "status": "accepted",
                "dataset_id": record.id,
                "preview_url": f"/api/datasets/{record.id}/preview",
                "status_url": f"/api/datasets/{record.id}/status",
            })

            notifications.create(
                type="success",
                category="upload",
                title=f"Uploaded: {fname}",
                message=f"File uploaded successfully ({record.file_size_bytes} bytes)",
                batch_id=bid,
                source="upload",
            )

        except Exception as e:
            logger.error("Batch file %d (%s) failed: %s", idx, fname, e, exc_info=True)
            # Clean up partial record if it was created
            try:
                processing.delete_dataset(record.id)
            except Exception:
                pass
            items.append({
                "client_file_index": idx,
                "original_filename": fname,
                "status": "rejected",
                "error_code": "save_failed",
                "error": str(e),
            })
            notifications.create(
                type="error",
                category="upload",
                title=f"Upload failed: {fname}",
                message=f"Error: {str(e)}",
                metadata_json=json.dumps({
                    "filename": fname,
                    "error_code": "save_failed",
                    "error": str(e),
                }),
                batch_id=bid,
                source="upload",
            )
            continue

    # Sort items by client_file_index for consistent ordering
    items.sort(key=lambda x: x.get("client_file_index", 0))

    accepted_count = sum(1 for i in items if i.get("status") == "accepted")
    rejected_count = sum(1 for i in items if i.get("status") == "rejected")
    total_count = len(items)
    failed_filenames = [i["original_filename"] for i in items if i.get("status") == "rejected"]

    # Summary notification
    if rejected_count == 0:
        summary_type = "info"
        summary_title = "Upload complete"
    elif accepted_count == 0:
        summary_type = "error"
        summary_title = "Upload failed"
    else:
        summary_type = "warning"
        summary_title = "Upload partially complete"

    summary_msg = f"{accepted_count} of {total_count} files uploaded successfully."
    if rejected_count > 0:
        summary_msg += f" {rejected_count} failed."

    try:
        notifications.create(
            type=summary_type,
            category="upload",
            title=summary_title,
            message=summary_msg,
            metadata_json=json.dumps({
                "accepted": accepted_count,
                "rejected": rejected_count,
                "total": total_count,
                "failed_filenames": failed_filenames,
            }),
            batch_id=bid,
            source="upload",
        )
    except Exception:
        logger.warning("Failed to create batch summary notification for %s", bid)

    return JSONResponse(
        status_code=202,
        content={
            "batch_id": bid,
            "accepted": accepted_count,
            "rejected": rejected_count,
            "items": items,
        },
    )


@router.get("/batch/{batch_id}")
async def get_batch_status(
    batch_id: str,
    batch_service: BatchService = Depends(get_batch_service),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """BQ-108: Get aggregated status of all datasets in a batch."""
    result = batch_service.get_batch_status(batch_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Batch not found")
    return result


@router.post("/batch/{batch_id}/confirm-all")
async def confirm_batch(
    batch_id: str,
    background_tasks: BackgroundTasks,
    batch_service: BatchService = Depends(get_batch_service),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """BQ-109: Confirm all preview_ready datasets in a batch."""
    if not batch_service.batch_belongs_to_user(batch_id, user.user_id):
        raise HTTPException(status_code=404, detail="Batch not found")

    result = batch_service.confirm_batch(batch_id, user.user_id)

    # Queue indexing for each confirmed dataset (sequential)
    from app.services.processing_queue import get_processing_queue
    queue = get_processing_queue()
    for ds_id in result.pop("confirmed_ids", []):
        await queue.submit(ds_id, index_only=True)

    return JSONResponse(status_code=202, content=result)


@router.get("/{dataset_id}")
async def get_dataset(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service),
):
    """Get metadata for a specific dataset."""
    record = processing.get_dataset(dataset_id)
    if not record:
        raise VectorAIzError("VAI-UX-001", detail=f"Dataset '{dataset_id}' not found")

    response = record.to_dict()

    # If ready, include full metadata from DuckDB
    if record.status == ProcessingStatus.READY and record.processed_path:
        try:
            def _get_metadata():
                with ephemeral_duckdb_service() as duckdb:
                    return duckdb.get_file_metadata(record.processed_path)
            metadata = await run_sync(_get_metadata)
            response["metadata"] = metadata
        except Exception as e:
            response["metadata_error"] = str(e)

    return response


@router.get("/{dataset_id}/status")
async def get_dataset_status(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service)
):
    """Get processing status for a dataset (for polling)."""
    record = processing.get_dataset(dataset_id)
    if not record:
        raise VectorAIzError("VAI-UX-001", detail=f"Dataset '{dataset_id}' not found")

    status_val = record.status.value if isinstance(record.status, DatasetStatus) else record.status
    result = {
        "dataset_id": dataset_id,
        "status": status_val,
        "original_filename": record.original_filename,
        "batch_id": record.batch_id,
        "error": record.error if status_val == DatasetStatus.ERROR.value else None,
    }

    # Show queue position for datasets waiting to be processed
    from app.services.processing_queue import get_processing_queue
    pq_inst = get_processing_queue()
    pos = pq_inst.get_position(dataset_id)
    if pos is not None:
        result["queue_position"] = pos
        result["queue_depth"] = pq_inst.queue_depth

    # Real-time progress data (in-memory)
    progress_info = pq_inst.get_progress(dataset_id)
    if progress_info:
        result["phase"] = progress_info["phase"]
        result["progress_pct"] = progress_info["progress_pct"]
        result["progress_detail"] = progress_info["detail"]

    return result


@router.get("/{dataset_id}/preview")
async def get_dataset_preview(
    dataset_id: str,
    preview_service: PreviewService = Depends(get_preview_service),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """BQ-109: Get data preview for a dataset. Returns status-appropriate response for all states."""
    result = preview_service.get_preview(dataset_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return result


@router.post("/{dataset_id}/confirm")
async def confirm_dataset(
    dataset_id: str,
    body: ConfirmRequest,
    background_tasks: BackgroundTasks,
    processing: ProcessingService = Depends(get_processing_service),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """
    BQ-109: Confirm a dataset for indexing. Idempotent.
    preview_ready → 202, indexing → 202 (no-op), ready → 200 (no-op),
    extracting → 409, error → 409, cancelled → 404.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail="Dataset not found")

    status_val = record.status.value if isinstance(record.status, DatasetStatus) else record.status

    if status_val == DatasetStatus.CANCELLED.value:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if status_val == DatasetStatus.EXTRACTING.value:
        return JSONResponse(
            status_code=409,
            content={"status": status_val, "error": "Dataset still extracting, try again later"},
        )

    if status_val == DatasetStatus.ERROR.value:
        return JSONResponse(
            status_code=409,
            content={"status": status_val, "error": "Dataset in error state, cannot confirm"},
        )

    if status_val == DatasetStatus.READY.value:
        return JSONResponse(status_code=200, content={"status": "ready"})

    if status_val == DatasetStatus.INDEXING.value:
        return JSONResponse(status_code=202, content={"status": "indexing"})

    if status_val == DatasetStatus.PREVIEW_READY.value:
        # Transition to indexing
        processing._set_status(dataset_id, DatasetStatus.INDEXING)
        # Record confirmation
        from app.models.dataset import DatasetRecord as DBDatasetRecord
        from app.core.database import get_session_context
        with get_session_context() as session:
            db_row = session.get(DBDatasetRecord, dataset_id)
            if db_row:
                db_row.confirmed_at = datetime.now(timezone.utc)
                db_row.confirmed_by = user.user_id
                session.add(db_row)
                session.commit()

        from app.services.processing_queue import get_processing_queue
        await get_processing_queue().submit(dataset_id, index_only=True)
        return JSONResponse(status_code=202, content={"status": "indexing"})

    # Fallback for uploaded state
    return JSONResponse(
        status_code=409,
        content={"status": status_val, "error": "Dataset not ready for confirmation"},
    )


@router.post("/{dataset_id}/pipeline")
async def run_processing_pipeline(
    dataset_id: str,
    background_tasks: BackgroundTasks,
    pipeline_service: PipelineService = Depends(get_pipeline_service),
    processing: ProcessingService = Depends(get_processing_service),
    user: AuthenticatedUser = Depends(get_current_user),
    _meter: MeterDecision = Depends(metered("setup")),
):
    """
    Run the full processing pipeline on a dataset.
    This includes DuckDB analysis, PII scan, compliance, attestation, and listing metadata.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    
    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready for pipeline. Current status: {record.status.value}"
        )
    
    background_tasks.add_task(pipeline_service.run_pipeline, dataset_id)
    
    return JSONResponse(
        status_code=202,
        content={
            "message": "Processing pipeline started in the background.",
            "dataset_id": dataset_id,
        }
    )



@router.post("/{dataset_id}/process-full")
async def process_full_pipeline(
    dataset_id: str,
    background_tasks: BackgroundTasks,
    pipeline_service: PipelineService = Depends(get_pipeline_service),
    processing: ProcessingService = Depends(get_processing_service),
    user: AuthenticatedUser = Depends(get_current_user),
    _meter: MeterDecision = Depends(metered("setup")),
):
    """
    BQ-088: Trigger the full processing pipeline on a dataset.
    Runs DuckDB analysis, PII scan, and compliance check in background.
    Returns a job ID for tracking progress via GET /pipeline-status.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready for pipeline processing. Current status: {record.status.value}"
        )

    background_tasks.add_task(pipeline_service.run_full_pipeline, dataset_id)

    return JSONResponse(
        status_code=202,
        content={
            "message": "Full processing pipeline started.",
            "dataset_id": dataset_id,
            "status_url": f"/api/datasets/{dataset_id}/pipeline-status",
        }
    )


@router.get("/{dataset_id}/pipeline-status")
async def get_pipeline_status(
    dataset_id: str,
    pipeline_service: PipelineService = Depends(get_pipeline_service),
    user: AuthenticatedUser = Depends(get_current_user)
):
    """
    BQ-088: Get pipeline progress with per-step status.
    Returns overall status, message, and individual step statuses
    (pending/running/success/failed/skipped) with timestamps.
    """
    status_data = pipeline_service.get_pipeline_status(dataset_id)

    if status_data.get("status") == "failed" and status_data.get("message") == "No pipeline run found for this dataset.":
        raise HTTPException(
            status_code=404,
            detail=f"No pipeline has been run for dataset '{dataset_id}'.",
        )

    return status_data


@router.get("/{dataset_id}/sample")
async def get_dataset_sample(
    dataset_id: str,
    limit: int = 10,
    redact_pii: bool = True,
    processing: ProcessingService = Depends(get_processing_service),
    preview_service: PreviewService = Depends(get_preview_service),
):
    """Get sample rows from a dataset. PII columns are masked by default."""
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}"
        )

    if not record.processed_path or not record.processed_path.exists():
        raise HTTPException(status_code=500, detail="Processed file not found")

    try:
        def _get_sample():
            with ephemeral_duckdb_service() as duckdb:
                return duckdb.get_sample_rows(record.processed_path, limit)
        sample = await run_sync(_get_sample)

        if redact_pii and sample:
            pii_columns = preview_service.detect_pii_columns(dataset_id, sample)
            if pii_columns:
                sample = _redact_pii_rows(sample, pii_columns)

        return {
            "dataset_id": dataset_id,
            "sample": sample,
            "count": len(sample),
            "pii_redacted": redact_pii,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _redact_pii_rows(rows: list, pii_columns: set) -> list:
    """Mask values in PII-flagged columns with '***'."""
    return [
        {k: "***" if k in pii_columns else v for k, v in row.items()}
        for row in rows
    ]


@router.get("/{dataset_id}/statistics")
async def get_dataset_statistics(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service),
):
    """Get column statistics for a dataset."""
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}"
        )

    if not record.processed_path or not record.processed_path.exists():
        raise HTTPException(status_code=500, detail="Processed file not found")

    try:
        def _get_statistics():
            with ephemeral_duckdb_service() as duckdb:
                return duckdb.get_column_statistics(record.processed_path)
        stats = await run_sync(_get_statistics)
        return {
            "dataset_id": dataset_id,
            "statistics": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{dataset_id}/profile")
async def get_dataset_profile(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service),
):
    """
    Get detailed column profiles including null analysis, uniqueness, and semantic types.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}"
        )

    if not record.processed_path or not record.processed_path.exists():
        raise HTTPException(status_code=500, detail="Processed file not found")

    try:
        def _get_profile():
            with ephemeral_duckdb_service() as duckdb:
                return duckdb.get_column_profile(record.processed_path)
        profiles = await run_sync(_get_profile)
        return {
            "dataset_id": dataset_id,
            "column_profiles": profiles,
            "column_count": len(profiles)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{dataset_id}/attestation", response_model=QualityAttestation)
async def generate_dataset_attestation(
    dataset_id: str,
    attestation_service: AttestationService = Depends(get_attestation_service),
    processing: ProcessingService = Depends(get_processing_service),
    _meter: MeterDecision = Depends(metered("setup")),
):
    """Get quality attestation report for a dataset."""
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    
    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}"
        )
    
    try:
        attestation = await attestation_service.generate_attestation(dataset_id)
        return attestation
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        logger.exception("Attestation generation failed for dataset %s", dataset_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{dataset_id}/compliance", response_model=ComplianceReport)
async def get_dataset_compliance(
    dataset_id: str,
    compliance_service: ComplianceService = Depends(get_compliance_service),
    processing: ProcessingService = Depends(get_processing_service)
):
    """Get compliance report for a dataset, based on PII scan results."""
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    
    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}"
        )
    
    try:
        report = await compliance_service.generate_compliance_report(dataset_id)
        return report
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{dataset_id}/listing-metadata", response_model=ListingMetadata)
async def generate_listing_metadata(
    dataset_id: str,
    listing_service: ListingMetadataService = Depends(get_listing_metadata_service),
    processing: ProcessingService = Depends(get_processing_service),
    _meter: MeterDecision = Depends(metered("setup")),
):
    """Generate marketplace-ready listing metadata for a dataset."""
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    
    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}"
        )
    
    try:
        metadata = await listing_service.generate_listing_metadata(dataset_id)
        return metadata
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        logger.exception("Listing metadata generation failed for dataset %s", dataset_id)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{dataset_id}/searchability")
async def get_dataset_searchability(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service),
):
    """
    Get searchability score indicating how well the dataset can be semantically searched.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}"
        )

    if not record.processed_path or not record.processed_path.exists():
        raise HTTPException(status_code=500, detail="Processed file not found")

    try:
        def _get_searchability():
            with ephemeral_duckdb_service() as duckdb:
                return duckdb.calculate_searchability_score(record.processed_path)
        searchability = await run_sync(_get_searchability)
        return {
            "dataset_id": dataset_id,
            **searchability
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{dataset_id}/full")
async def get_dataset_full_metadata(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service),
):
    """
    Get comprehensive metadata including basic info, column profiles, and searchability.
    This is the complete dataset analysis endpoint.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}"
        )

    if not record.processed_path or not record.processed_path.exists():
        raise HTTPException(status_code=500, detail="Processed file not found")

    try:
        def _get_full_metadata():
            with ephemeral_duckdb_service() as duckdb:
                return duckdb.get_enhanced_metadata(record.processed_path)
        full_metadata = await run_sync(_get_full_metadata)
        return {
            "dataset_id": dataset_id,
            "original_filename": record.original_filename,
            **full_metadata
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{dataset_id}/content")
async def get_dataset_content(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service)
):
    """
    Get extracted content for document types (PDF, Word, PowerPoint).
    Returns text blocks and tables extracted from the document.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    
    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}"
        )
    
    if not record.document_content:
        raise HTTPException(
            status_code=400,
            detail="This dataset is not a document type. Use /sample for tabular data."
        )
    
    return {
        "dataset_id": dataset_id,
        "original_filename": record.original_filename,
        "text_blocks": record.document_content.get("text_content", []),
        "tables": record.document_content.get("tables", []),
        "metadata": record.document_content.get("metadata", {})
    }


@router.post("/{dataset_id}/index")
async def index_dataset(
    dataset_id: str,
    row_limit: int = 10000,
    recreate: bool = False,
    processing: ProcessingService = Depends(get_processing_service),
    indexing: IndexingService = Depends(get_indexing_service),
    user: AuthenticatedUser = Depends(get_current_user),
    _meter: MeterDecision = Depends(metered("setup")),
):
    """
    Trigger indexing for a dataset (runs in background).

    Returns HTTP 202 with a job_id immediately. Indexing proceeds
    as a background asyncio task. Check status via GET /{dataset_id}/index.
    Requires X-API-Key header.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}"
        )

    if not record.processed_path or not record.processed_path.exists():
        raise HTTPException(status_code=500, detail="Processed file not found")

    job_id = str(uuid.uuid4())
    filepath = record.processed_path

    async def _index_background():
        try:
            await run_sync(
                indexing.index_dataset,
                dataset_id, filepath, row_limit, None, recreate,
            )
            logger.info("Background indexing complete for dataset %s (job %s)", dataset_id, job_id)
        except Exception:
            logger.exception("Background indexing failed for dataset %s (job %s)", dataset_id, job_id)

    asyncio.create_task(_index_background())

    return JSONResponse(
        status_code=202,
        content={
            "job_id": job_id,
            "status": "indexing",
            "dataset_id": dataset_id,
        }
    )


@router.get("/{dataset_id}/index")
async def get_index_status(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service),
    indexing: IndexingService = Depends(get_indexing_service),
):
    """Get the indexing status for a dataset."""
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    
    return await run_sync(indexing.get_index_status, dataset_id)


@router.delete("/{dataset_id}/index")
async def delete_index(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service),
    indexing: IndexingService = Depends(get_indexing_service),
    user: AuthenticatedUser = Depends(get_current_user)
):
    """Delete the search index for a dataset. Requires X-API-Key header."""
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    
    success = await run_sync(indexing.delete_dataset_index, dataset_id)
    if success:
        return {"message": f"Index for dataset '{dataset_id}' deleted successfully"}
    else:
        raise HTTPException(status_code=404, detail="Index not found")


@router.delete("/{dataset_id}")
async def delete_dataset(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service),
    user: AuthenticatedUser = Depends(get_current_user)
):
    """Delete a dataset and its files. Handles cancellation for pre-ready states."""
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    status_val = record.status.value if isinstance(record.status, DatasetStatus) else record.status

    # For pre-ready states: set cancelled immediately so background tasks abort
    if status_val in (
        DatasetStatus.UPLOADED.value,
        DatasetStatus.EXTRACTING.value,
        DatasetStatus.PREVIEW_READY.value,
        DatasetStatus.INDEXING.value,
    ):
        processing._set_status(dataset_id, DatasetStatus.CANCELLED)

    success = processing.delete_dataset(dataset_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete dataset")

    # Rebuild facets async after dataset removal
    from app.services.facet_service import rebuild_facets_async
    rebuild_facets_async()

    return {"message": f"Dataset '{dataset_id}' deleted successfully"}


@router.post("/{dataset_id}/publish")
async def publish_to_marketplace(
    dataset_id: str,
    price: float = 25.0,
    category: str = "tabular",
    model_provider: str = "local",
    processing: ProcessingService = Depends(get_processing_service),
    push_service: MarketplacePushService = Depends(get_marketplace_push_service),
    user: AuthenticatedUser = Depends(get_current_user),
    _meter: MeterDecision = Depends(metered("setup")),
):
    """
    Publish a processed dataset to ai.market.

    BQ-090: Sends listing metadata, compliance report, and attestation
    to the marketplace API. The dataset must be in READY status.

    - **price**: Listing price in USD (minimum $25)
    - **category**: Primary category slug (e.g. "tabular", "financial")
    - **model_provider**: AI model used for analysis ("local", "anthropic", etc.)
    """
    # Verify dataset exists and is ready
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready for publish. Current status: {record.status.value}. "
                   "Run the processing pipeline first."
        )

    try:
        result = await push_service.push_to_marketplace(
            dataset_id=dataset_id,
            price=price,
            category=category,
            model_provider=model_provider,
        )
        return result
    except MarketplacePushError as e:
        status_code = e.status_code or 502
        raise HTTPException(
            status_code=status_code,
            detail={
                "error": str(e),
                "marketplace_status": e.status_code,
                "detail": e.detail,
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error during publish: {str(e)}"
        )


# ── BQ-VZ-DATA-READINESS: Readiness Report ─────────────────────────


@router.get("/{dataset_id}/readiness")
async def get_dataset_readiness(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Get combined readiness report: schema + PII risk + quality scorecard + statistical profile."""
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}",
        )

    data_dir = Path(settings.data_directory) / "processed" / dataset_id

    def _load_json(filename: str):
        path = data_dir / filename
        if not path.exists():
            return None
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    # Load all report artifacts (any may be missing if pipeline step failed)
    schema_report = _load_json("duckdb_analysis.json")
    pii_report = _load_json("pii_scan.json")
    quality_scorecard = _load_json("quality_scorecard.json")
    sketch_profile = _load_json("sketch_profile.json")

    # If sketch/quality don't exist yet, generate them on-demand
    if sketch_profile is None:
        try:
            svc = get_sketch_service()
            profile = await run_sync(lambda: svc.generate_profile(dataset_id))
            sketch_profile = profile.model_dump()
        except Exception as e:
            logger.warning("On-demand sketch profile failed for %s: %s", dataset_id, e)

    if quality_scorecard is None:
        try:
            svc = get_quality_contract_service()
            scorecard = await run_sync(lambda: svc.validate_dataset(dataset_id))
            quality_scorecard = scorecard.model_dump()
        except Exception as e:
            logger.warning("On-demand quality scorecard failed for %s: %s", dataset_id, e)

    return {
        "dataset_id": dataset_id,
        "schema_report": schema_report,
        "pii_risk": pii_report,
        "quality_scorecard": quality_scorecard,
        "statistical_profile": sketch_profile,
    }
