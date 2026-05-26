from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, insert, select
from sqlalchemy.exc import IntegrityError
from sqlmodel import SQLModel, Session

from app.models.dataset import DatasetRecord  # noqa: F401
from app.models.s3_connection import S3Connection
from app.models.s3_object_metadata import S3ObjectMetadata
from app.models.s3_scan_job import S3ScanJob


@pytest.fixture
def s3_engine():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    SQLModel.metadata.create_all(engine)
    return engine


def _id() -> str:
    return str(uuid4())


def test_s3_connection_constraint_rejects_null_creds_when_configured(s3_engine):
    with pytest.raises(IntegrityError), Session(s3_engine) as session:
        session.exec(
            insert(S3Connection).values(
                id=_id(),
                name="Configured connection",
                bucket="seller-bucket",
                region="us-east-1",
                status="configured",
                role_arn=None,
                external_id=None,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
        )
        session.commit()


def test_s3_connection_allows_null_creds_when_onboarding(s3_engine):
    connection_id = _id()
    with Session(s3_engine) as session:
        session.add(
            S3Connection(
                id=connection_id,
                name="Onboarding connection",
                bucket="seller-bucket",
                region="us-east-1",
                status="onboarding",
                role_arn=None,
                external_id=None,
            )
        )
        session.commit()

    with Session(s3_engine) as session:
        connection = session.get(S3Connection, connection_id)
        assert connection is not None
        assert connection.role_arn is None
        assert connection.external_id is None


def test_s3_object_metadata_fk_cascade_on_connection_delete(s3_engine):
    connection_id = _id()
    scan_job_id = _id()
    object_id = _id()
    with Session(s3_engine) as session:
        session.add(
            S3Connection(
                id=connection_id,
                name="Cascade connection",
                bucket="seller-bucket",
                region="us-east-1",
                role_arn="arn:aws:iam::123456789012:role/vectoraiz",
                external_id="aim-data-seller-test",
            )
        )
        session.add(S3ScanJob(id=scan_job_id, connection_id=connection_id))
        session.add(
            S3ObjectMetadata(
                id=object_id,
                connection_id=connection_id,
                scan_job_id=scan_job_id,
                object_key="folder/object.csv",
                size_bytes=128,
                content_type="text/csv",
                last_modified=datetime.now(timezone.utc),
                etag="etag",
                extraction_status="EXTRACTED",
            )
        )
        session.commit()

        connection = session.get(S3Connection, connection_id)
        session.delete(connection)
        session.commit()

        assert session.get(S3Connection, connection_id) is None
        assert session.get(S3ScanJob, scan_job_id) is None
        assert session.get(S3ObjectMetadata, object_id) is None


def test_dataset_id_on_per_object_row_not_on_scan_job(s3_engine):
    assert "dataset_id" in S3ObjectMetadata.__table__.columns
    assert "dataset_id" not in S3ScanJob.__table__.columns

    with Session(s3_engine) as session:
        assert session.exec(select(S3ObjectMetadata)).all() == []
