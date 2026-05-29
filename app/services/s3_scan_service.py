"""
S3 scan service for STS-backed, no-copy listing registration.
"""

from __future__ import annotations

import logging
import mimetypes
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlmodel import select

from app.core.database import get_session_context
from app.models.s3_connection import S3Connection
from app.models.s3_object_metadata import S3ObjectMetadata
from app.models.s3_scan_job import S3ScanJob
from app.services.sts_broker import STSAssumeError, STSBroker

logger = logging.getLogger(__name__)


def _boto3_client(service_name: str, **kwargs):
    import boto3

    return boto3.client(service_name, **kwargs)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _content_type_for_key(object_key: str) -> str:
    return mimetypes.guess_type(object_key)[0] or "application/octet-stream"


def _object_last_modified(item: dict[str, Any]) -> datetime:
    value = item.get("LastModified")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return _now()


class S3ScanService:
    """Enumerates seller-owned S3 objects using assumed-role credentials."""

    def __init__(self, broker: Optional[STSBroker] = None) -> None:
        self.broker = broker or STSBroker()

    def scan_connection(self, connection_id: str) -> S3ScanJob:
        with get_session_context() as session:
            connection = session.get(S3Connection, connection_id)
            if connection is None:
                raise ValueError("S3 connection not found")

            scan_job = S3ScanJob(
                id=str(uuid.uuid4()),
                connection_id=connection.id,
                status="running",
                started_at=_now(),
                updated_at=_now(),
            )
            session.add(scan_job)
            session.commit()
            session.refresh(scan_job)

            try:
                credentials = self.broker.assume_role(connection, purpose="scan")
                s3_client = _boto3_client(
                    "s3",
                    region_name=credentials.region,
                    aws_access_key_id=credentials.access_key_id,
                    aws_secret_access_key=credentials.secret_access_key,
                    aws_session_token=credentials.session_token,
                )

                enumerated = 0
                continuation_token: Optional[str] = None
                while True:
                    request: dict[str, Any] = {
                        "Bucket": connection.bucket,
                        "Prefix": connection.prefix or "",
                    }
                    if continuation_token:
                        request["ContinuationToken"] = continuation_token

                    response = s3_client.list_objects_v2(**request)
                    for item in response.get("Contents", []):
                        object_key = item["Key"]
                        existing = session.exec(
                            select(S3ObjectMetadata)
                            .where(S3ObjectMetadata.connection_id == connection.id)
                            .where(S3ObjectMetadata.object_key == object_key)
                        ).first()

                        if existing is None:
                            existing = S3ObjectMetadata(
                                id=str(uuid.uuid4()),
                                connection_id=connection.id,
                                scan_job_id=scan_job.id,
                                object_key=object_key,
                                size_bytes=int(item.get("Size") or 0),
                                content_type=_content_type_for_key(object_key),
                                last_modified=_object_last_modified(item),
                                etag=item.get("ETag", ""),
                            )
                        else:
                            existing.scan_job_id = scan_job.id
                            existing.size_bytes = int(item.get("Size") or 0)
                            existing.content_type = _content_type_for_key(object_key)
                            existing.last_modified = _object_last_modified(item)
                            existing.etag = item.get("ETag", "")
                            existing.updated_at = _now()

                        session.add(existing)
                        enumerated += 1

                    continuation_token = response.get("NextContinuationToken")
                    scan_job.objects_enumerated = enumerated
                    scan_job.continuation_token = continuation_token
                    scan_job.updated_at = _now()
                    session.add(scan_job)
                    session.commit()

                    if not response.get("IsTruncated"):
                        break

                completed_at = _now()
                scan_job.status = "completed"
                scan_job.completed_at = completed_at
                scan_job.continuation_token = None
                scan_job.updated_at = completed_at
                connection.last_scanned_at = completed_at
                connection.continuation_token = None
                connection.updated_at = completed_at
                session.add(scan_job)
                session.add(connection)
                session.commit()
                session.refresh(scan_job)
                session.expunge(scan_job)
                return scan_job
            except STSAssumeError as exc:
                failed_at = _now()
                scan_job.status = "failed"
                scan_job.error_message = str(exc)
                scan_job.completed_at = failed_at
                scan_job.updated_at = failed_at
                session.add(scan_job)
                session.commit()
                session.refresh(scan_job)
                session.expunge(scan_job)
                return scan_job
            except Exception as exc:  # any other failure must fail-closed, not stick "running"
                logger.warning(
                    "s3_scan_failed",
                    extra={
                        "connection_id": connection_id,
                        "scan_job_id": scan_job.id,
                        "error_type": type(exc).__name__,
                    },
                )
                failed_at = _now()
                scan_job.status = "failed"
                scan_job.error_message = (
                    "Scan failed. Verify the connection's bucket and role permissions, then retry."
                )
                scan_job.completed_at = failed_at
                scan_job.updated_at = failed_at
                session.add(scan_job)
                session.commit()
                session.refresh(scan_job)
                session.expunge(scan_job)
                return scan_job
