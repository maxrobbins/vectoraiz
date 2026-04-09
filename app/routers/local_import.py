"""VZ-PERF-P1: Local directory import from host-mounted /data/import (read-only).

Provides browse, process, and status endpoints for importing files from
a host-mounted directory into the vectorAIz processing pipeline.
"""
import os

import logging
from pathlib import Path
from typing import List, Optional, Set

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

IMPORT_ROOT = Path("/data/import")
UPLOAD_DIR = Path("/data/uploads")
MAX_DEPTH = 3

# Extensions accepted for import (kept in sync with datasets router)
_SUPPORTED_EXTENSIONS: Optional[Set[str]] = None


def _get_supported_extensions() -> set:
    global _SUPPORTED_EXTENSIONS
    if _SUPPORTED_EXTENSIONS is None:
        from app.routers.datasets import SUPPORTED_EXTENSIONS
        _SUPPORTED_EXTENSIONS = SUPPORTED_EXTENSIONS
    return _SUPPORTED_EXTENSIONS


def _validate_import_path(relative: str) -> Path:
    """Resolve a relative path under IMPORT_ROOT with security checks.

    - realpath prefix check (no escaping /data/import)
    - Reject symlinks anywhere in the path (checked on *unresolved* path)
    - Max depth enforcement (directory levels, not counting filename)
    """
    import_root_resolved = IMPORT_ROOT.resolve()

    # Symlink check — walk the *unresolved* path so symlinks are visible
    normalized = Path(os.path.normpath(IMPORT_ROOT / relative))
    try:
        unresolved_rel = normalized.relative_to(Path(os.path.normpath(IMPORT_ROOT)))
    except ValueError:
        raise ValueError("Path outside import directory")
    check = Path(os.path.normpath(IMPORT_ROOT))
    for part in unresolved_rel.parts:
        check = check / part
        if check.exists() and check.is_symlink():
            raise ValueError("Symlinks not allowed")

    # Now resolve for prefix check
    candidate = normalized.resolve()

    # Prefix check — must stay under /data/import
    if not str(candidate).startswith(str(import_root_resolved) + os.sep) and candidate != import_root_resolved:
        raise ValueError("Path outside import directory")

    # Depth check — count directory levels (file at depth 3 = a/b/c/file.csv is OK)
    try:
        rel = candidate.relative_to(import_root_resolved)
    except ValueError:
        raise ValueError("Path outside import directory")
    depth = len(rel.parts)
    if candidate.is_file() and depth > 0:
        depth -= 1  # Don't count the filename itself
    if depth > MAX_DEPTH:
        raise ValueError(f"Exceeds max depth of {MAX_DEPTH}")

    return candidate


@router.get("/status")
async def import_status():
    """Check if /data/import is mounted and readable."""
    resolved = IMPORT_ROOT.resolve()
    exists = resolved.is_dir()
    readable = exists and os.access(str(resolved), os.R_OK)
    return {
        "available": exists and readable,
        "path": str(IMPORT_ROOT),
        "exists": exists,
        "readable": readable,
    }


@router.get("/browse")
async def browse_import(
    path: str = Query("", description="Relative path under /data/import"),
):
    """List files and directories under /data/import."""
    try:
        resolved = _validate_import_path(path)
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))

    if not resolved.is_dir():
        raise HTTPException(status_code=404, detail="Directory not found")

    supported = _get_supported_extensions()
    entries = []
    try:
        for entry in sorted(resolved.iterdir(), key=lambda e: (not e.is_dir(), e.name.lower())):
            if entry.name.startswith("."):
                continue
            if entry.is_symlink():
                continue
            if entry.is_dir():
                # Check depth before listing subdirectories
                try:
                    rel = entry.resolve().relative_to(IMPORT_ROOT.resolve())
                    if len(rel.parts) <= MAX_DEPTH:
                        entries.append({"name": entry.name, "type": "directory"})
                except ValueError:
                    continue
            elif entry.is_file() and entry.suffix.lower() in supported:
                entries.append({
                    "name": entry.name,
                    "type": "file",
                    "size_bytes": entry.stat().st_size,
                })
    except PermissionError:
        raise HTTPException(status_code=403, detail="Permission denied")

    return {"path": path or "/", "entries": entries}


class ProcessRequest(BaseModel):
    paths: List[str]


@router.post("/process")
async def process_import(req: ProcessRequest):
    """Copy files from /data/import to /data/uploads and trigger processing."""
    if not req.paths:
        raise HTTPException(status_code=400, detail="No paths provided")

    from app.services.processing_service import get_processing_service
    from app.services.processing_queue import get_processing_queue

    processing = get_processing_service()
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    for rel_path in req.paths:
        try:
            source = _validate_import_path(rel_path)
        except ValueError as e:
            results.append({"path": rel_path, "error": str(e)})
            continue

        if not source.is_file():
            results.append({"path": rel_path, "error": "Not a file"})
            continue

        try:
            file_type = source.suffix.lstrip(".")
            record = processing.create_dataset(
                original_filename=source.name,
                file_type=file_type,
            )
            dest = Path(record.upload_path)
            dest.parent.mkdir(parents=True, exist_ok=True)

            # Copy file (source is on read-only mount, so hardlink won't work)
            import shutil
            shutil.copy2(str(source), str(dest))

            # Queue for sequential processing
            await get_processing_queue().submit(record.id)

            results.append({
                "path": rel_path,
                "dataset_id": record.id,
                "status": "processing",
            })
        except Exception as e:
            logger.error("Failed to import %s: %s", rel_path, e)
            results.append({"path": rel_path, "error": str(e)})

    return {"results": results}
