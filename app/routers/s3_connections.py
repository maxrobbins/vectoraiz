"""
S3 Connection Router
====================

Customer-facing S3 STS connection setup and verification endpoints.
"""

import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field
from sqlmodel import select

from app.config import settings
from app.core.database import get_session_context
from app.models.dataset import DatasetRecord
from app.models.s3_connection import S3Connection
from app.models.s3_object_metadata import S3ObjectMetadata
from app.models.s3_scan_job import S3ScanJob
from app.services.s3_scan_service import S3ScanService

router = APIRouter()

ROLE_ARN_RE = re.compile(r"^arn:aws:iam::\d{12}:role/.+$")


class S3ConnectionCreate(BaseModel):
    name: str = Field(..., max_length=255)
    bucket: str = Field(..., max_length=255)
    region: str = Field(..., max_length=64)
    prefix: Optional[str] = Field(default=None, max_length=512)


class S3ConnectionRoleArn(BaseModel):
    role_arn: str


class S3ConnectionResponse(BaseModel):
    id: str
    name: str
    bucket: str
    region: str
    prefix: Optional[str] = None
    role_arn: Optional[str] = None
    external_id: Optional[str] = None
    status: str
    error_message: Optional[str] = None
    last_scanned_at: Optional[str] = None
    created_at: str
    updated_at: str
    trust_policy: Optional[Dict[str, Any]] = None
    permission_policy: Optional[Dict[str, Any]] = None


class S3VerifyResponse(BaseModel):
    status: str
    error_message: Optional[str] = None
    verified_at: Optional[str] = None


class S3ConfigResponse(BaseModel):
    aws_account_id: str


class S3ScanJobResponse(BaseModel):
    id: str
    connection_id: str
    status: str
    started_at: str
    completed_at: Optional[str] = None
    continuation_token: Optional[str] = None
    error_message: Optional[str] = None
    objects_enumerated: int
    created_at: str
    updated_at: str


class S3ObjectMetadataResponse(BaseModel):
    id: str
    connection_id: str
    scan_job_id: str
    object_key: str
    size_bytes: int
    content_type: str
    last_modified: str
    etag: str
    dataset_id: Optional[str] = None
    created_at: str
    updated_at: str


class S3ObjectsResponse(BaseModel):
    items: List[S3ObjectMetadataResponse]
    limit: int
    offset: int
    total: int


class S3ObjectRegisterRequest(BaseModel):
    dataset_id: Optional[str] = None
    listing_id: Optional[str] = None


class S3DatasetResponse(BaseModel):
    id: str
    original_filename: str
    storage_filename: str
    file_type: str
    file_size_bytes: int
    status: str
    listing_id: Optional[str] = None
    created_at: str
    updated_at: str


class S3ObjectRegisterResponse(BaseModel):
    dataset: S3DatasetResponse
    object: S3ObjectMetadataResponse


def _policy_prefix(prefix: Optional[str]) -> str:
    return prefix or ""


def _trust_policy(connection: S3Connection) -> Dict[str, Any]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": f"arn:aws:iam::{settings.ai_market_aws_account_id}:root",
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": connection.external_id,
                    },
                },
            }
        ],
    }


def _permission_policy(connection: S3Connection) -> Dict[str, Any]:
    prefix = _policy_prefix(connection.prefix)
    list_statement: Dict[str, Any] = {
        "Effect": "Allow",
        "Action": ["s3:ListBucket"],
        "Resource": f"arn:aws:s3:::{connection.bucket}",
    }
    get_resource = f"arn:aws:s3:::{connection.bucket}/*"
    if prefix:
        list_statement["Condition"] = {
            "StringLike": {
                "s3:prefix": [f"{prefix}*"],
            },
        }
        get_resource = f"arn:aws:s3:::{connection.bucket}/{prefix}*"

    return {
        "Version": "2012-10-17",
        "Statement": [
            list_statement,
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": get_resource,
            },
        ],
    }


def _to_response(connection: S3Connection, include_policies: bool = False) -> S3ConnectionResponse:
    response = S3ConnectionResponse(
        id=connection.id,
        name=connection.name,
        bucket=connection.bucket,
        region=connection.region,
        prefix=connection.prefix,
        role_arn=connection.role_arn,
        external_id=connection.external_id,
        status=connection.status,
        error_message=connection.error_message,
        last_scanned_at=connection.last_scanned_at.isoformat() if connection.last_scanned_at else None,
        created_at=connection.created_at.isoformat(),
        updated_at=connection.updated_at.isoformat(),
    )
    if include_policies:
        response.trust_policy = _trust_policy(connection)
        response.permission_policy = _permission_policy(connection)
    return response


def _scan_job_response(scan_job: S3ScanJob) -> S3ScanJobResponse:
    return S3ScanJobResponse(
        id=scan_job.id,
        connection_id=scan_job.connection_id,
        status=scan_job.status,
        started_at=scan_job.started_at.isoformat(),
        completed_at=scan_job.completed_at.isoformat() if scan_job.completed_at else None,
        continuation_token=scan_job.continuation_token,
        error_message=scan_job.error_message,
        objects_enumerated=scan_job.objects_enumerated,
        created_at=scan_job.created_at.isoformat(),
        updated_at=scan_job.updated_at.isoformat(),
    )


def _object_response(metadata: S3ObjectMetadata) -> S3ObjectMetadataResponse:
    return S3ObjectMetadataResponse(
        id=metadata.id,
        connection_id=metadata.connection_id,
        scan_job_id=metadata.scan_job_id,
        object_key=metadata.object_key,
        size_bytes=metadata.size_bytes,
        content_type=metadata.content_type,
        last_modified=metadata.last_modified.isoformat(),
        etag=metadata.etag,
        dataset_id=metadata.dataset_id,
        created_at=metadata.created_at.isoformat(),
        updated_at=metadata.updated_at.isoformat(),
    )


def _dataset_response(dataset: DatasetRecord) -> S3DatasetResponse:
    return S3DatasetResponse(
        id=dataset.id,
        original_filename=dataset.original_filename,
        storage_filename=dataset.storage_filename,
        file_type=dataset.file_type,
        file_size_bytes=dataset.file_size_bytes,
        status=dataset.status,
        listing_id=dataset.listing_id,
        created_at=dataset.created_at.isoformat(),
        updated_at=dataset.updated_at.isoformat(),
    )


def _dataset_file_type(object_key: str) -> str:
    extension = os.path.splitext(object_key)[1].lstrip(".").lower()
    return extension or "unknown"


def _create_dataset_for_object(metadata: S3ObjectMetadata, body: S3ObjectRegisterRequest) -> DatasetRecord:
    now = datetime.now(timezone.utc)
    return DatasetRecord(
        id=body.dataset_id or str(uuid.uuid4()),
        original_filename=os.path.basename(metadata.object_key) or metadata.object_key,
        storage_filename=metadata.object_key,
        file_type=_dataset_file_type(metadata.object_key),
        file_size_bytes=metadata.size_bytes,
        status="s3_linked",
        listing_id=body.listing_id,
        created_at=now,
        updated_at=now,
    )


def _boto3_client(service_name: str, **kwargs):
    import boto3

    return boto3.client(service_name, **kwargs)


@router.get("/config", summary="Get S3 connection setup config")
async def get_config() -> S3ConfigResponse:
    return S3ConfigResponse(aws_account_id=settings.ai_market_aws_account_id)


@router.post("/", status_code=201, summary="Create S3 connection")
async def create_connection(body: S3ConnectionCreate) -> S3ConnectionResponse:
    connection = S3Connection(
        id=str(uuid.uuid4()),
        name=body.name,
        bucket=body.bucket,
        region=body.region,
        prefix=body.prefix or None,
        external_id=str(uuid.uuid4()),
        status="onboarding",
    )
    with get_session_context() as session:
        session.add(connection)
        session.commit()
        session.refresh(connection)
        return _to_response(connection, include_policies=True)


@router.get("/", summary="List S3 connections")
async def list_connections() -> List[S3ConnectionResponse]:
    with get_session_context() as session:
        rows = session.exec(select(S3Connection)).all()
        return [_to_response(row) for row in rows]


@router.get("/{connection_id}", summary="Get S3 connection")
async def get_connection(connection_id: str) -> S3ConnectionResponse:
    with get_session_context() as session:
        connection = session.get(S3Connection, connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="S3 connection not found")
        return _to_response(connection, include_policies=True)


@router.put("/{connection_id}/role-arn", summary="Set S3 role ARN")
async def set_role_arn(connection_id: str, body: S3ConnectionRoleArn) -> S3ConnectionResponse:
    if not ROLE_ARN_RE.match(body.role_arn):
        raise HTTPException(status_code=400, detail="Invalid IAM role ARN")

    with get_session_context() as session:
        connection = session.get(S3Connection, connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="S3 connection not found")

        connection.role_arn = body.role_arn
        connection.status = "configured"
        connection.error_message = None
        connection.updated_at = datetime.now(timezone.utc)
        session.add(connection)
        session.commit()
        session.refresh(connection)
        return _to_response(connection)


@router.post("/{connection_id}/verify", summary="Verify S3 connection")
async def verify_connection(connection_id: str) -> S3VerifyResponse:
    with get_session_context() as session:
        connection = session.get(S3Connection, connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="S3 connection not found")
        if not connection.role_arn:
            raise HTTPException(status_code=400, detail="S3 connection role ARN is not configured")

        try:
            sts_client = _boto3_client("sts", region_name=connection.region)
            assumed = sts_client.assume_role(
                RoleArn=connection.role_arn,
                RoleSessionName="aim-data-verify",
                ExternalId=connection.external_id,
            )
            credentials = assumed["Credentials"]
            s3_client = _boto3_client(
                "s3",
                region_name=connection.region,
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
            )
            s3_client.list_objects_v2(
                Bucket=connection.bucket,
                Prefix=connection.prefix or "",
                MaxKeys=1,
            )
        except Exception as exc:
            connection.status = "error"
            connection.error_message = str(exc)
            connection.updated_at = datetime.now(timezone.utc)
            session.add(connection)
            session.commit()
            return S3VerifyResponse(status=connection.status, error_message=connection.error_message)

        verified_at = datetime.now(timezone.utc)
        connection.status = "verified"
        connection.last_scanned_at = verified_at
        connection.error_message = None
        connection.updated_at = verified_at
        session.add(connection)
        session.commit()
        return S3VerifyResponse(status=connection.status, verified_at=verified_at.isoformat())


@router.post("/{connection_id}/scan", summary="Scan S3 connection objects")
async def scan_connection(connection_id: str) -> S3ScanJobResponse:
    try:
        scan_job = S3ScanService().scan_connection(connection_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="S3 connection not found") from None
    return _scan_job_response(scan_job)


@router.get("/{connection_id}/scan/{scan_job_id}", summary="Get S3 scan job")
async def get_scan_job(connection_id: str, scan_job_id: str) -> S3ScanJobResponse:
    with get_session_context() as session:
        scan_job = session.get(S3ScanJob, scan_job_id)
        if scan_job is None or scan_job.connection_id != connection_id:
            raise HTTPException(status_code=404, detail="S3 scan job not found")
        return _scan_job_response(scan_job)


@router.get("/{connection_id}/objects", summary="List scanned S3 objects")
async def list_objects(
    connection_id: str,
    limit: int = 100,
    offset: int = 0,
    dataset_linked: Optional[bool] = None,
) -> S3ObjectsResponse:
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must be non-negative")

    with get_session_context() as session:
        connection = session.get(S3Connection, connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="S3 connection not found")

        stmt = select(S3ObjectMetadata).where(S3ObjectMetadata.connection_id == connection_id)
        if dataset_linked is True:
            stmt = stmt.where(S3ObjectMetadata.dataset_id.is_not(None))
        elif dataset_linked is False:
            stmt = stmt.where(S3ObjectMetadata.dataset_id.is_(None))

        rows = session.exec(stmt.order_by(S3ObjectMetadata.object_key)).all()
        page = rows[offset : offset + limit]
        return S3ObjectsResponse(
            items=[_object_response(row) for row in page],
            limit=limit,
            offset=offset,
            total=len(rows),
        )


@router.post("/{connection_id}/objects/{object_id}/register", summary="Register scanned S3 object as dataset")
async def register_object(
    connection_id: str,
    object_id: str,
    body: S3ObjectRegisterRequest,
) -> S3ObjectRegisterResponse:
    with get_session_context() as session:
        metadata = session.get(S3ObjectMetadata, object_id)
        if metadata is None or metadata.connection_id != connection_id:
            raise HTTPException(status_code=404, detail="S3 object metadata not found")

        dataset: Optional[DatasetRecord] = None
        if body.dataset_id:
            dataset = session.get(DatasetRecord, body.dataset_id)
        elif body.listing_id:
            dataset = session.exec(select(DatasetRecord).where(DatasetRecord.listing_id == body.listing_id)).first()

        if dataset is not None:
            # Ownership guard (S729 security review): an existing dataset may only be
            # linked if THIS connection already owns it (has an object linked to it).
            # Prevents one connection attaching its object to another seller's dataset.
            owned = session.exec(
                select(S3ObjectMetadata)
                .where(S3ObjectMetadata.dataset_id == dataset.id)
                .where(S3ObjectMetadata.connection_id == connection_id)
            ).first()
            if owned is None:
                raise HTTPException(status_code=403, detail="Dataset is not owned by this connection")

        if dataset is None:
            dataset = _create_dataset_for_object(metadata, body)
        else:
            dataset.updated_at = datetime.now(timezone.utc)
            if body.listing_id and not dataset.listing_id:
                dataset.listing_id = body.listing_id

        session.add(dataset)
        session.flush()

        metadata.dataset_id = dataset.id
        metadata.updated_at = datetime.now(timezone.utc)
        session.add(metadata)
        session.commit()
        session.refresh(dataset)
        session.refresh(metadata)

        return S3ObjectRegisterResponse(
            dataset=_dataset_response(dataset),
            object=_object_response(metadata),
        )


@router.delete("/{connection_id}", status_code=204, summary="Delete S3 connection")
async def delete_connection(connection_id: str) -> Response:
    with get_session_context() as session:
        connection = session.get(S3Connection, connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="S3 connection not found")
        session.delete(connection)
        session.commit()
    return Response(status_code=204)
