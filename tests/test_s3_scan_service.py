from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Optional
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, select

from app.main import app
from app.models.dataset import DatasetRecord
from app.models.s3_connection import S3Connection
from app.models.s3_object_metadata import S3ObjectMetadata
from app.models.s3_scan_job import S3ScanJob
from app.routers import s3_connections
from app.services import fulfillment_service, s3_scan_service
from app.services.fulfillment_service import FulfillmentService
from app.services.s3_scan_service import S3ScanService
from app.services.sts_broker import AssumedCredentials, STSAssumeError


@pytest.fixture
def s3_engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    SQLModel.metadata.create_all(engine)
    return engine


@pytest.fixture
def session_context(s3_engine, monkeypatch):
    @contextmanager
    def _session_context():
        with Session(s3_engine) as session:
            yield session

    monkeypatch.setattr(s3_scan_service, "get_session_context", _session_context)
    monkeypatch.setattr(s3_connections, "get_session_context", _session_context)
    monkeypatch.setattr(fulfillment_service, "get_session_context", _session_context)
    return _session_context


@pytest.fixture
def client(session_context):
    return TestClient(app)


class FakeBroker:
    def __init__(self, error: Optional[Exception] = None):
        self.error = error
        self.calls = []

    def assume_role(self, connection, purpose: str):
        self.calls.append((connection.id, purpose))
        if self.error:
            raise self.error
        return AssumedCredentials(
            access_key_id="access-key",
            secret_access_key="secret-key",
            session_token="session-token",
            expiration=datetime.now(timezone.utc) + timedelta(hours=1),
            region=connection.region,
        )


class FakeS3Client:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def list_objects_v2(self, **kwargs):
        self.calls.append(kwargs)
        if "ContinuationToken" in kwargs:
            return self.pages[1]
        return self.pages[0]


def _connection(**overrides) -> S3Connection:
    values = {
        "id": str(uuid4()),
        "name": "Seller bucket",
        "bucket": "seller-bucket",
        "region": "us-east-1",
        "prefix": "exports/",
        "role_arn": "arn:aws:iam::210987654321:role/aim-data",
        "external_id": str(uuid4()),
        "status": "verified",
    }
    values.update(overrides)
    return S3Connection(**values)


def _add_connection(session_context, **overrides) -> S3Connection:
    connection = _connection(**overrides)
    with session_context() as session:
        session.add(connection)
        session.commit()
        session.refresh(connection)
        session.expunge(connection)
    return connection


def _page(*objects, truncated=False, token=None):
    response = {
        "IsTruncated": truncated,
        "Contents": list(objects),
    }
    if token:
        response["NextContinuationToken"] = token
    return response


def _object(key: str, size: int = 123):
    return {
        "Key": key,
        "Size": size,
        "ETag": '"etag"',
        "LastModified": datetime(2026, 5, 29, tzinfo=timezone.utc),
    }


def test_scan_persists_one_row_per_object(session_context, monkeypatch):
    connection = _add_connection(session_context)
    s3_client = FakeS3Client(
        [
            _page(_object("exports/a.csv"), truncated=True, token="next"),
            _page(_object("exports/b.json", 456)),
        ]
    )
    monkeypatch.setattr(s3_scan_service, "_boto3_client", lambda *_args, **_kwargs: s3_client)

    scan_job = S3ScanService(FakeBroker()).scan_connection(connection.id)

    assert scan_job.status == "completed"
    assert scan_job.objects_enumerated == 2
    assert s3_client.calls[0]["Bucket"] == "seller-bucket"
    assert s3_client.calls[0]["Prefix"] == "exports/"
    assert s3_client.calls[1]["ContinuationToken"] == "next"
    with session_context() as session:
        objects = session.exec(select(S3ObjectMetadata).order_by(S3ObjectMetadata.object_key)).all()
        stored_connection = session.get(S3Connection, connection.id)
    assert [obj.object_key for obj in objects] == ["exports/a.csv", "exports/b.json"]
    assert objects[0].content_type == "text/csv"
    assert objects[1].size_bytes == 456
    assert stored_connection.last_scanned_at is not None


def test_rescan_is_idempotent_and_preserves_dataset_id(session_context, monkeypatch):
    connection = _add_connection(session_context)
    dataset = DatasetRecord(
        id=str(uuid4()),
        original_filename="a.csv",
        storage_filename="exports/a.csv",
        file_type="csv",
        file_size_bytes=123,
        status="s3_linked",
    )
    dataset_id = dataset.id
    scan_job = S3ScanJob(id=str(uuid4()), connection_id=connection.id, status="completed")
    original_object_id = str(uuid4())
    with session_context() as session:
        session.add(dataset)
        session.add(scan_job)
        session.add(
            S3ObjectMetadata(
                id=original_object_id,
                connection_id=connection.id,
                scan_job_id=scan_job.id,
                object_key="exports/a.csv",
                size_bytes=1,
                content_type="text/csv",
                last_modified=datetime.now(timezone.utc),
                etag="old",
                dataset_id=dataset.id,
            )
        )
        session.commit()

    s3_client = FakeS3Client([_page(_object("exports/a.csv", 999))])
    monkeypatch.setattr(s3_scan_service, "_boto3_client", lambda *_args, **_kwargs: s3_client)

    S3ScanService(FakeBroker()).scan_connection(connection.id)

    with session_context() as session:
        objects = session.exec(select(S3ObjectMetadata)).all()
    assert len(objects) == 1
    assert objects[0].id == original_object_id
    assert objects[0].dataset_id == dataset_id
    assert objects[0].size_bytes == 999


def test_scan_sts_error_marks_failed_without_raw_aws_internals(session_context):
    connection = _add_connection(session_context)
    raw = "An error occurred (AccessDenied) when calling the AssumeRole operation"
    broker = FakeBroker(STSAssumeError("Confirm the trust policy and ExternalId.", "AccessDenied"))

    scan_job = S3ScanService(broker).scan_connection(connection.id)

    assert scan_job.status == "failed"
    assert "Confirm the trust policy" in scan_job.error_message
    assert raw not in scan_job.error_message
    with session_context() as session:
        stored = session.get(S3ScanJob, scan_job.id)
    assert stored.status == "failed"


def test_register_endpoint_links_object_and_creates_dataset(client, session_context):
    connection = _add_connection(session_context)
    scan_job = S3ScanJob(id=str(uuid4()), connection_id=connection.id, status="completed")
    metadata = S3ObjectMetadata(
        id=str(uuid4()),
        connection_id=connection.id,
        scan_job_id=scan_job.id,
        object_key="exports/report.csv",
        size_bytes=789,
        content_type="text/csv",
        last_modified=datetime.now(timezone.utc),
        etag='"etag"',
    )
    metadata_id = metadata.id
    with session_context() as session:
        session.add(scan_job)
        session.add(metadata)
        session.commit()

    response = client.post(
        f"/api/s3-connections/{connection.id}/objects/{metadata_id}/register",
        json={"listing_id": "lst_123"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["dataset"]["original_filename"] == "report.csv"
    assert body["dataset"]["file_type"] == "csv"
    assert body["dataset"]["file_size_bytes"] == 789
    assert body["dataset"]["status"] == "s3_linked"
    assert body["dataset"]["storage_filename"] == "exports/report.csv"
    assert body["dataset"]["listing_id"] == "lst_123"
    assert body["object"]["dataset_id"] == body["dataset"]["id"]

    second = client.post(
        f"/api/s3-connections/{connection.id}/objects/{metadata_id}/register",
        json={"listing_id": "lst_123"},
    )
    assert second.status_code == 200
    assert second.json()["dataset"]["id"] == body["dataset"]["id"]


def test_objects_endpoint_paginates_and_filters(client, session_context):
    connection = _add_connection(session_context)
    dataset = DatasetRecord(
        id=str(uuid4()),
        original_filename="linked.csv",
        storage_filename="exports/linked.csv",
        file_type="csv",
        file_size_bytes=1,
        status="s3_linked",
    )
    scan_job = S3ScanJob(id=str(uuid4()), connection_id=connection.id, status="completed")
    with session_context() as session:
        session.add(dataset)
        session.add(scan_job)
        session.add(
            S3ObjectMetadata(
                id=str(uuid4()),
                connection_id=connection.id,
                scan_job_id=scan_job.id,
                object_key="exports/linked.csv",
                size_bytes=1,
                content_type="text/csv",
                last_modified=datetime.now(timezone.utc),
                etag="etag",
                dataset_id=dataset.id,
            )
        )
        session.add(
            S3ObjectMetadata(
                id=str(uuid4()),
                connection_id=connection.id,
                scan_job_id=scan_job.id,
                object_key="exports/unlinked.csv",
                size_bytes=1,
                content_type="text/csv",
                last_modified=datetime.now(timezone.utc),
                etag="etag",
            )
        )
        session.commit()

    response = client.get(f"/api/s3-connections/{connection.id}/objects", params={"dataset_linked": False})

    assert response.status_code == 200
    assert response.json()["total"] == 1
    assert response.json()["items"][0]["object_key"] == "exports/unlinked.csv"


def test_fulfillment_resolves_registered_s3_dataset(session_context):
    connection = _add_connection(session_context)
    dataset = DatasetRecord(
        id=str(uuid4()),
        original_filename="listing.csv",
        storage_filename="exports/listing.csv",
        file_type="csv",
        file_size_bytes=321,
        status="s3_linked",
        listing_id="listing-123",
    )
    dataset_id = dataset.id
    scan_job = S3ScanJob(id=str(uuid4()), connection_id=connection.id, status="completed")
    metadata = S3ObjectMetadata(
        id=str(uuid4()),
        connection_id=connection.id,
        scan_job_id=scan_job.id,
        object_key="exports/listing.csv",
        size_bytes=321,
        content_type="text/csv",
        last_modified=datetime.now(timezone.utc),
        etag="etag",
        dataset_id=dataset.id,
    )
    metadata_id = metadata.id
    with session_context() as session:
        session.add(dataset)
        session.add(scan_job)
        session.add(metadata)
        session.commit()

    service = FulfillmentService(SimpleNamespace())
    found_dataset, file_path = service._find_dataset("listing-123")
    s3_object = service._find_s3_object(found_dataset)

    assert file_path is None
    assert found_dataset.id == dataset_id
    assert s3_object is not None
    found_connection, found_metadata = s3_object
    assert found_connection.id == connection.id
    assert found_metadata.id == metadata_id


def test_register_rejects_unowned_existing_dataset(client, session_context):
    """A connection cannot attach its object to a dataset it does not own (S729 sec review)."""
    connection = _add_connection(session_context)
    foreign = DatasetRecord(
        id=str(uuid4()),
        original_filename="foreign.csv",
        storage_filename="foreign.csv",
        file_type="csv",
        file_size_bytes=1,
        status="s3_linked",
        listing_id="foreign-listing",
    )
    scan_job = S3ScanJob(id=str(uuid4()), connection_id=connection.id, status="completed")
    metadata = S3ObjectMetadata(
        id=str(uuid4()),
        connection_id=connection.id,
        scan_job_id=scan_job.id,
        object_key="exports/mine.csv",
        size_bytes=1,
        content_type="text/csv",
        last_modified=datetime.now(timezone.utc),
        etag="etag",
    )
    metadata_id = metadata.id
    with session_context() as session:
        session.add(foreign)
        session.add(scan_job)
        session.add(metadata)
        session.commit()

    resp = client.post(
        f"/api/s3-connections/{connection.id}/objects/{metadata_id}/register",
        json={"listing_id": "foreign-listing"},
    )
    assert resp.status_code == 403
