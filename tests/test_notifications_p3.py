"""
Tests for Upload Resilience — Per-file fault tolerance + notification logging (Phase 3)

Tests cover:
- Batch upload with one bad file continues processing remaining files
- Notifications are created for successes and failures
- Summary notification is created with correct counts
- Response includes per-file status
- Single-file upload creates notifications on success/failure
- Upload-summary endpoint creates summary notifications
"""

import io
import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def clean_notifications():
    """Per-test cleanup of notifications table."""
    from app.core.database import get_session_context
    from app.models.notification import Notification
    from sqlmodel import select

    with get_session_context() as session:
        for n in session.exec(select(Notification)).all():
            session.delete(n)
        session.commit()
    yield


@pytest.fixture
def client():
    from app.main import app
    from app.services.processing_service import get_processing_service, ProcessingService
    from app.auth.api_key_auth import get_current_user

    mock_proc = MagicMock(spec=ProcessingService)
    mock_proc.delete_dataset = MagicMock()
    mock_user = MagicMock()
    mock_user.id = "test-user"

    app.dependency_overrides[get_processing_service] = lambda: mock_proc
    app.dependency_overrides[get_current_user] = lambda: mock_user

    c = TestClient(app)
    yield c

    app.dependency_overrides.clear()


@pytest.fixture
def svc():
    from app.services.notification_service import get_notification_service
    return get_notification_service()


def _make_upload_file(filename: str, content: bytes = b"test content"):
    """Create a tuple suitable for TestClient multipart upload."""
    return ("files", (filename, io.BytesIO(content), "application/octet-stream"))


def _patch_batch_processing(mock_create_fn=None):
    """Context manager that patches batch service and processing queue for tests."""
    from app.services.batch_service import BatchService

    def default_validate(self_bs, filenames, sizes, headers, paths):
        return list(range(len(filenames))), []

    counter = [0]

    def default_create(self_bs, filename, file_type, file_size, batch_id, relative_path):
        counter[0] += 1
        record = MagicMock()
        record.id = f"ds_{counter[0]}"
        record.upload_path = MagicMock()
        record.upload_path.name = f"file_{counter[0]}.csv"
        record.file_size_bytes = file_size
        return record

    create_fn = mock_create_fn or default_create

    return (
        patch.object(BatchService, "validate_batch", default_validate),
        patch.object(BatchService, "create_dataset_record", create_fn),
        patch("app.services.processing_queue.get_processing_queue", return_value=AsyncMock()),
        patch("aiofiles.open", new_callable=lambda: _mock_aiofiles_open),
    )


def _mock_aiofiles_open(*args, **kwargs):
    """Return an async context manager mock for aiofiles.open."""
    mock_file = AsyncMock()
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=mock_file)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


class TestBatchUploadResilience:
    """Test that a single file failure doesn't kill the entire batch."""

    def test_bad_file_continues_processing(self, client, svc):
        """One file failing to save should not prevent other files from uploading."""
        from app.services.batch_service import BatchService

        call_count = [0]

        def mock_create(self_bs, filename, file_type, file_size, batch_id, relative_path):
            call_count[0] += 1
            if call_count[0] == 2:
                raise IOError("Simulated disk write failure")
            record = MagicMock()
            record.id = f"ds_{call_count[0]}"
            record.upload_path = MagicMock()
            record.upload_path.name = f"file_{call_count[0]}.csv"
            record.file_size_bytes = file_size
            return record

        patches = _patch_batch_processing(mock_create)
        with patches[0], patches[1], patches[2], patches[3]:
            resp = client.post(
                "/api/datasets/batch",
                files=[
                    _make_upload_file("good1.csv"),
                    _make_upload_file("bad.csv"),
                    _make_upload_file("good2.csv"),
                ],
            )

        assert resp.status_code == 202
        data = resp.json()

        assert data["accepted"] == 2
        assert data["rejected"] == 1

        items = data["items"]
        assert len(items) == 3

        statuses = {item["original_filename"]: item["status"] for item in items}
        assert statuses["good1.csv"] == "accepted"
        assert statuses["bad.csv"] == "rejected"
        assert statuses["good2.csv"] == "accepted"

        rejected = [i for i in items if i["status"] == "rejected"]
        assert len(rejected) == 1
        assert rejected[0]["error_code"] == "save_failed"

    def test_notifications_created_for_successes_and_failures(self, client, svc):
        """Each file should generate a notification (success or error)."""
        from app.services.batch_service import BatchService

        call_count = [0]

        def mock_create(self_bs, filename, file_type, file_size, batch_id, relative_path):
            call_count[0] += 1
            if call_count[0] == 2:
                raise IOError("Disk full")
            record = MagicMock()
            record.id = f"ds_{call_count[0]}"
            record.upload_path = MagicMock()
            record.upload_path.name = f"file_{call_count[0]}.csv"
            record.file_size_bytes = file_size
            return record

        patches = _patch_batch_processing(mock_create)
        with patches[0], patches[1], patches[2], patches[3]:
            client.post(
                "/api/datasets/batch",
                files=[
                    _make_upload_file("ok.csv"),
                    _make_upload_file("fail.csv"),
                ],
            )

        notifs = svc.list(category="upload")
        # 1 success + 1 error + 1 summary = 3
        assert len(notifs) >= 3

        types = [n.type for n in notifs]
        assert "success" in types
        assert "error" in types

        error_notifs = [n for n in notifs if n.type == "error"]
        assert len(error_notifs) >= 1
        error_n = error_notifs[0]
        assert error_n.source == "upload"
        assert error_n.batch_id is not None
        meta = json.loads(error_n.metadata_json)
        assert "filename" in meta
        assert "error" in meta

    def test_summary_notification_correct_counts(self, client, svc):
        """Summary notification should report accurate success/failure counts."""
        from app.services.batch_service import BatchService

        call_count = [0]

        def mock_create(self_bs, filename, file_type, file_size, batch_id, relative_path):
            call_count[0] += 1
            if filename in ("bad1.csv", "bad2.csv"):
                raise IOError("Simulated failure")
            record = MagicMock()
            record.id = f"ds_{call_count[0]}"
            record.upload_path = MagicMock()
            record.upload_path.name = f"file_{call_count[0]}.csv"
            record.file_size_bytes = file_size
            return record

        patches = _patch_batch_processing(mock_create)
        with patches[0], patches[1], patches[2], patches[3]:
            client.post(
                "/api/datasets/batch",
                files=[
                    _make_upload_file("good1.csv"),
                    _make_upload_file("good2.csv"),
                    _make_upload_file("good3.csv"),
                    _make_upload_file("bad1.csv"),
                    _make_upload_file("bad2.csv"),
                ],
            )

        notifs = svc.list(category="upload")
        summary_notifs = [n for n in notifs if "of" in n.message and "files uploaded" in n.message]
        assert len(summary_notifs) == 1

        summary = summary_notifs[0]
        assert summary.type == "warning"
        assert summary.title == "Upload partially complete"
        assert "3 of 5 files uploaded successfully" in summary.message
        assert "2 failed" in summary.message

        meta = json.loads(summary.metadata_json)
        assert meta["accepted"] == 3
        assert meta["rejected"] == 2
        assert meta["total"] == 5
        assert sorted(meta["failed_filenames"]) == ["bad1.csv", "bad2.csv"]

    def test_summary_all_succeed(self, client, svc):
        """Summary should be 'info' type when all files succeed."""
        patches = _patch_batch_processing()
        with patches[0], patches[1], patches[2], patches[3]:
            resp = client.post(
                "/api/datasets/batch",
                files=[
                    _make_upload_file("a.csv"),
                    _make_upload_file("b.csv"),
                ],
            )

        assert resp.status_code == 202
        assert resp.json()["accepted"] == 2
        assert resp.json()["rejected"] == 0

        notifs = svc.list(category="upload")
        summary_notifs = [n for n in notifs if "files uploaded" in n.message]
        assert len(summary_notifs) == 1
        assert summary_notifs[0].type == "info"
        assert summary_notifs[0].title == "Upload complete"

    def test_summary_all_fail(self, client, svc):
        """Summary should be 'error' type when all files fail."""
        def mock_create(self_bs, filename, file_type, file_size, batch_id, relative_path):
            raise IOError("All disks dead")

        patches = _patch_batch_processing(mock_create)
        with patches[0], patches[1], patches[2], patches[3]:
            resp = client.post(
                "/api/datasets/batch",
                files=[
                    _make_upload_file("x.csv"),
                    _make_upload_file("y.csv"),
                ],
            )

        assert resp.status_code == 202
        assert resp.json()["accepted"] == 0
        assert resp.json()["rejected"] == 2

        notifs = svc.list(category="upload")
        summary_notifs = [n for n in notifs if "files uploaded" in n.message]
        assert len(summary_notifs) == 1
        assert summary_notifs[0].type == "error"
        assert summary_notifs[0].title == "Upload failed"

    def test_response_includes_per_file_status(self, client, svc):
        """Each item in response should have filename and status."""
        from app.services.batch_service import BatchService

        def mock_validate(self_bs, filenames, sizes, headers, paths):
            accepted = [0]
            rejected = [{
                "client_file_index": 1,
                "original_filename": filenames[1],
                "status": "rejected",
                "error_code": "unsupported_type",
                "error": "Unsupported file type: .xyz",
            }]
            return accepted, rejected

        counter = [0]

        def mock_create(self_bs, filename, file_type, file_size, batch_id, relative_path):
            counter[0] += 1
            record = MagicMock()
            record.id = f"ds_{counter[0]}"
            record.upload_path = MagicMock()
            record.upload_path.name = f"file_{counter[0]}.csv"
            record.file_size_bytes = file_size
            return record

        with patch.object(BatchService, "validate_batch", mock_validate), \
             patch.object(BatchService, "create_dataset_record", mock_create), \
             patch("app.services.processing_queue.get_processing_queue", return_value=AsyncMock()), \
             patch("aiofiles.open", new_callable=lambda: _mock_aiofiles_open):

            resp = client.post(
                "/api/datasets/batch",
                files=[
                    _make_upload_file("good.csv"),
                    _make_upload_file("bad.xyz"),
                ],
            )

        data = resp.json()
        assert data["accepted"] == 1
        assert data["rejected"] == 1

        items = data["items"]
        assert len(items) == 2

        for item in items:
            assert "original_filename" in item
            assert "status" in item
            assert item["status"] in ("accepted", "rejected")

        rejected = [i for i in items if i["status"] == "rejected"]
        assert rejected[0]["error_code"] == "unsupported_type"

        # Validation rejections also create notifications
        notifs = svc.list(category="upload")
        error_notifs = [n for n in notifs if n.type == "error" and "bad.xyz" in n.title]
        assert len(error_notifs) >= 1


# ---------------------------------------------------------------------------
# Single-file upload endpoint — notification tests
# ---------------------------------------------------------------------------

def _patch_single_upload():
    """Patches for single-file upload tests: processing service, processing queue, aiofiles."""
    from app.services.processing_service import ProcessingService

    mock_record = MagicMock()
    mock_record.id = "ds_single_1"
    mock_record.upload_path = MagicMock()
    mock_record.upload_path.name = "test_file.csv"
    mock_record.file_size_bytes = 100
    mock_record.status = MagicMock()
    mock_record.status.value = "processing"
    mock_record.original_filename = "test.csv"

    mock_proc = MagicMock(spec=ProcessingService)
    mock_proc.create_dataset.return_value = mock_record
    mock_proc.find_by_filename.return_value = None
    mock_proc._save_record = MagicMock()

    return mock_proc, mock_record


class TestSingleUploadNotifications:
    """Test that single-file uploads via /api/datasets/upload create notifications."""

    def test_success_creates_notification(self, client, svc):
        """Successful single-file upload should create a success notification."""
        mock_proc, mock_record = _patch_single_upload()

        from app.services.processing_service import get_processing_service
        from app.main import app

        app.dependency_overrides[get_processing_service] = lambda: mock_proc

        with patch("app.services.processing_queue.get_processing_queue", return_value=AsyncMock()), \
             patch("aiofiles.open", new_callable=lambda: _mock_aiofiles_open), \
             patch("app.routers.datasets._check_magic_bytes", return_value=True):
            resp = client.post(
                "/api/datasets/upload?batch_id=test_batch_123",
                files={"file": ("test.csv", io.BytesIO(b"a,b\n1,2"), "text/csv")},
            )

        assert resp.status_code == 202

        notifs = svc.list(category="upload")
        success_notifs = [n for n in notifs if n.type == "success"]
        assert len(success_notifs) >= 1
        assert success_notifs[0].batch_id == "test_batch_123"
        assert "test.csv" in success_notifs[0].title
        assert success_notifs[0].source == "upload"

    def test_failure_creates_error_notification(self, client, svc):
        """Failed single-file upload should create an error notification."""
        mock_proc, mock_record = _patch_single_upload()
        # Make magic bytes check fail to trigger an error
        mock_proc.delete_dataset = MagicMock()

        from app.services.processing_service import get_processing_service
        from app.main import app

        app.dependency_overrides[get_processing_service] = lambda: mock_proc

        with patch("aiofiles.open", new_callable=lambda: _mock_aiofiles_open), \
             patch("app.routers.datasets._check_magic_bytes", return_value=False):
            resp = client.post(
                "/api/datasets/upload?batch_id=test_batch_456",
                files={"file": ("bad.csv", io.BytesIO(b"\x89PNG"), "text/csv")},
            )

        assert resp.status_code == 422

        notifs = svc.list(category="upload")
        error_notifs = [n for n in notifs if n.type == "error"]
        assert len(error_notifs) >= 1
        assert error_notifs[0].batch_id == "test_batch_456"
        assert "bad.csv" in error_notifs[0].title
        meta = json.loads(error_notifs[0].metadata_json)
        assert "filename" in meta
        assert "error" in meta

    def test_batch_id_optional(self, client, svc):
        """Upload without batch_id should still create notification (batch_id=None)."""
        mock_proc, mock_record = _patch_single_upload()

        from app.services.processing_service import get_processing_service
        from app.main import app

        app.dependency_overrides[get_processing_service] = lambda: mock_proc

        with patch("app.services.processing_queue.get_processing_queue", return_value=AsyncMock()), \
             patch("aiofiles.open", new_callable=lambda: _mock_aiofiles_open), \
             patch("app.routers.datasets._check_magic_bytes", return_value=True):
            resp = client.post(
                "/api/datasets/upload",
                files={"file": ("solo.csv", io.BytesIO(b"a,b\n1,2"), "text/csv")},
            )

        assert resp.status_code == 202

        notifs = svc.list(category="upload")
        success_notifs = [n for n in notifs if n.type == "success"]
        assert len(success_notifs) >= 1
        assert success_notifs[0].batch_id is None


# ---------------------------------------------------------------------------
# Upload summary endpoint tests
# ---------------------------------------------------------------------------

class TestUploadSummaryEndpoint:
    """Test the /api/datasets/upload-summary endpoint."""

    def test_summary_partial_success(self, client, svc):
        """Summary with mixed results should create a warning notification."""
        resp = client.post(
            "/api/datasets/upload-summary",
            json={
                "batch_id": "upl_abc123",
                "accepted": 3,
                "rejected": 2,
                "failed_filenames": ["bad1.csv", "bad2.csv"],
            },
        )

        assert resp.status_code == 200
        assert resp.json()["ok"] is True

        notifs = svc.list(category="upload")
        summary = [n for n in notifs if "files uploaded" in n.message]
        assert len(summary) == 1
        assert summary[0].type == "warning"
        assert summary[0].title == "Upload partially complete"
        assert "3 of 5" in summary[0].message
        assert "2 failed" in summary[0].message
        assert summary[0].batch_id == "upl_abc123"

        meta = json.loads(summary[0].metadata_json)
        assert meta["accepted"] == 3
        assert meta["rejected"] == 2
        assert sorted(meta["failed_filenames"]) == ["bad1.csv", "bad2.csv"]

    def test_summary_all_success(self, client, svc):
        """Summary with all successes should create an info notification."""
        resp = client.post(
            "/api/datasets/upload-summary",
            json={
                "batch_id": "upl_def456",
                "accepted": 5,
                "rejected": 0,
                "failed_filenames": [],
            },
        )

        assert resp.status_code == 200
        notifs = svc.list(category="upload")
        summary = [n for n in notifs if "files uploaded" in n.message]
        assert len(summary) == 1
        assert summary[0].type == "info"
        assert summary[0].title == "Upload complete"

    def test_summary_all_failed(self, client, svc):
        """Summary with all failures should create an error notification."""
        resp = client.post(
            "/api/datasets/upload-summary",
            json={
                "batch_id": "upl_ghi789",
                "accepted": 0,
                "rejected": 3,
                "failed_filenames": ["a.csv", "b.csv", "c.csv"],
            },
        )

        assert resp.status_code == 200
        notifs = svc.list(category="upload")
        summary = [n for n in notifs if "files uploaded" in n.message]
        assert len(summary) == 1
        assert summary[0].type == "error"
        assert summary[0].title == "Upload failed"
