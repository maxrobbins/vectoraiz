"""Local directory import endpoints."""
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from pydantic import BaseModel, Field
from typing import List

from app.auth.api_key_auth import get_current_user, AuthenticatedUser
from app.services.import_service import get_import_service, ImportService

router = APIRouter()


class ScanRequest(BaseModel):
    path: str
    recursive: bool = True
    max_depth: int = Field(default=5, ge=1, le=10)


class StartRequest(BaseModel):
    path: str
    files: List[str]


@router.get("/browse")
async def browse_directory(
    path: str = Query("/imports/"),
    limit: int = Query(500, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    user: AuthenticatedUser = Depends(get_current_user),
    svc: ImportService = Depends(get_import_service),
):
    try:
        return svc.browse(path, limit=limit, offset=offset)
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Directory not found")


@router.post("/scan")
async def scan_directory(
    req: ScanRequest,
    user: AuthenticatedUser = Depends(get_current_user),
    svc: ImportService = Depends(get_import_service),
):
    try:
        return svc.scan(req.path, recursive=req.recursive, max_depth=req.max_depth)
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Directory not found")


@router.post("/start")
async def start_import(
    req: StartRequest,
    background_tasks: BackgroundTasks,
    user: AuthenticatedUser = Depends(get_current_user),
    svc: ImportService = Depends(get_import_service),
):
    try:
        job = svc.start_import(req.path, req.files)
    except ValueError as e:
        if "already running" in str(e):
            raise HTTPException(status_code=409, detail=str(e))
        raise HTTPException(status_code=400, detail=str(e))

    background_tasks.add_task(svc.run_import, job)

    return {
        "job_id": job.job_id,
        "total_files": len(job.files),
        "total_bytes": job.total_bytes,
        "status": job.status,
    }


@router.get("/{job_id}")
async def get_import_status(
    job_id: str,
    user: AuthenticatedUser = Depends(get_current_user),
    svc: ImportService = Depends(get_import_service),
):
    job = svc.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Import job not found")

    return {
        "job_id": job.job_id,
        "status": job.status,
        "progress": {
            "files_total": len(job.files),
            "files_complete": sum(1 for f in job.files if f.status in ("processing", "ready")),
            "files_copying": sum(1 for f in job.files if f.status == "copying"),
            "files_pending": sum(1 for f in job.files if f.status == "pending"),
            "bytes_copied": job.bytes_copied,
            "bytes_total": job.total_bytes,
            "current_file": next((f.relative_path for f in job.files if f.status == "copying"), None),
            "current_file_pct": round(
                next((f.bytes_copied / f.size_bytes * 100 for f in job.files if f.status == "copying" and f.size_bytes > 0), 0), 1
            ),
        },
        "results": [
            {
                "file": f.relative_path,
                "status": f.status,
                "dataset_id": f.dataset_id,
                "error": f.error,
            }
            for f in job.files
        ],
    }


@router.post("/{job_id}/cancel")
async def cancel_import(
    job_id: str,
    user: AuthenticatedUser = Depends(get_current_user),
    svc: ImportService = Depends(get_import_service),
):
    if svc.cancel_job(job_id):
        return {"status": "cancelling"}
    raise HTTPException(status_code=404, detail="Job not found or not running")
