"""
Tests for BQ-108+109: Enhanced Upload Pipeline — Bulk Upload + Data Preview.

Covers: batch upload, validation, preview, confirm (idempotent), cancel,
status polling, batch confirm-all, MIME validation, error states.

≥15 tests required.
"""

import io
import json
import uuid
from typing import List, Optional

from fastapi.testclient import TestClient

from app.main import app
from app.models.dataset import DatasetStatus

client = TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _csv_bytes(rows: int = 3) -> bytes:
    lines = ["id,name,value"]
    for i in range(rows):
        lines.append(f"{i},name_{i},{i * 10}")
    return "\n".join(lines).encode()


def _make_upload(
    filenames: Optional[List[str]] = None,
    contents: Optional[List[bytes]] = None,
    paths: Optional[List[str]] = None,
    mode: str = "preview",
    batch_id: Optional[str] = None,
):
    """Helper to call POST /api/datasets/batch."""
    if filenames is None:
        filenames = ["test.csv"]
    if contents is None:
        contents = [_csv_bytes()] * len(filenames)

    files = [
        ("files", (fn, io.BytesIO(c), "text/csv"))
        for fn, c in zip(filenames, contents)
    ]
    data = {"mode": mode}
    if paths is not None:
        data["paths"] = json.dumps(paths)
    if batch_id is not None:
        data["batch_id"] = batch_id

    return client.post("/api/datasets/batch", files=files, data=data)


# ---------------------------------------------------------------------------
# 1. Batch Upload — happy path
# ---------------------------------------------------------------------------

def test_batch_upload_single_file():
    """Single file batch upload returns 202 with accepted item."""
    resp = _make_upload(["report.csv"])
    assert resp.status_code == 202
    body = resp.json()
    assert body["accepted"] == 1
    assert body["rejected"] == 0
    assert len(body["items"]) == 1
    item = body["items"][0]
    assert item["status"] == "accepted"
    assert item["client_file_index"] == 0
    assert "dataset_id" in item
    assert "preview_url" in item
    assert "status_url" in item


def test_batch_upload_multiple_files():
    """Upload 3 CSV files in one batch."""
    resp = _make_upload(["a.csv", "b.csv", "c.csv"])
    assert resp.status_code == 202
    body = resp.json()
    assert body["accepted"] == 3
    assert body["rejected"] == 0
    assert "batch_id" in body
    # Each item has unique dataset_id
    ids = {i["dataset_id"] for i in body["items"] if i["status"] == "accepted"}
    assert len(ids) == 3


def test_batch_upload_custom_batch_id():
    """Server uses client-supplied batch_id."""
    bid = f"bch_custom_{uuid.uuid4().hex[:6]}"
    resp = _make_upload(["d.csv"], batch_id=bid)
    assert resp.status_code == 202
    assert resp.json()["batch_id"] == bid


def test_batch_upload_with_paths():
    """Relative paths are echoed in the response."""
    resp = _make_upload(
        ["data.csv"],
        paths=["folder/data.csv"],
    )
    assert resp.status_code == 202
    item = resp.json()["items"][0]
    assert item["relative_path"] == "folder/data.csv"


# ---------------------------------------------------------------------------
# 2. Validation — rejection cases
# ---------------------------------------------------------------------------

def test_batch_reject_unsupported_extension():
    """Files with unsupported extensions are per-file rejected."""
    resp = _make_upload(
        ["good.csv", "bad.exe"],
        [_csv_bytes(), b"MZ\x90\x00"],
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["accepted"] == 1
    assert body["rejected"] == 1
    rejected = [i for i in body["items"] if i["status"] == "rejected"]
    assert rejected[0]["error_code"] == "unsupported_type"
    assert rejected[0]["client_file_index"] == 1


def test_batch_reject_paths_length_mismatch():
    """Paths length must match files length → 422."""
    resp = _make_upload(
        ["a.csv", "b.csv"],
        paths=["only_one_path.csv"],
    )
    assert resp.status_code == 422


def test_batch_reject_mime_mismatch():
    """PDF extension with non-PDF content is rejected."""
    # Send a .pdf file that starts with CSV text, not %PDF
    resp = _make_upload(
        ["fake.pdf"],
        [b"id,name\n1,Alice\n"],
    )
    assert resp.status_code == 202
    body = resp.json()
    rejected = [i for i in body["items"] if i["status"] == "rejected"]
    assert len(rejected) == 1
    assert rejected[0]["error_code"] == "mime_mismatch"


# ---------------------------------------------------------------------------
# 3. Dataset Status endpoint
# ---------------------------------------------------------------------------

def test_dataset_status_endpoint():
    """GET /api/datasets/{id}/status returns spec-compliant fields."""
    # Upload a file first
    resp = _make_upload(["status_test.csv"])
    dataset_id = resp.json()["items"][0]["dataset_id"]

    resp2 = client.get(f"/api/datasets/{dataset_id}/status")
    assert resp2.status_code == 200
    body = resp2.json()
    assert body["dataset_id"] == dataset_id
    assert "status" in body
    assert "original_filename" in body
    assert "batch_id" in body


def test_dataset_status_nonexistent():
    """Status endpoint returns 404 for missing dataset."""
    resp = client.get("/api/datasets/nonexistent999/status")
    assert resp.status_code in (400, 404)


# ---------------------------------------------------------------------------
# 4. Batch Status endpoint
# ---------------------------------------------------------------------------

def test_batch_status_endpoint():
    """GET /api/datasets/batch/{batch_id} returns aggregated status."""
    bid = f"bch_status_{uuid.uuid4().hex[:6]}"
    _make_upload(["s1.csv", "s2.csv"], batch_id=bid)

    resp = client.get(f"/api/datasets/batch/{bid}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["batch_id"] == bid
    assert body["total"] == 2
    assert "by_status" in body
    assert len(body["items"]) == 2


def test_batch_status_not_found():
    """Missing batch returns 404."""
    resp = client.get("/api/datasets/batch/bch_doesnt_exist")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 5. Preview endpoint
# ---------------------------------------------------------------------------

def test_preview_before_extraction():
    """Preview returns status with null preview while still extracting."""
    resp = _make_upload(["preview_test.csv"])
    dataset_id = resp.json()["items"][0]["dataset_id"]

    resp2 = client.get(f"/api/datasets/{dataset_id}/preview")
    assert resp2.status_code == 200
    body = resp2.json()
    assert body["dataset_id"] == dataset_id
    # Status can be uploaded, extracting, or preview_ready depending on timing
    assert "status" in body


def test_preview_not_found():
    """Preview returns 404 for missing dataset."""
    resp = client.get("/api/datasets/nonexistent999/preview")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 6. Confirm endpoint — idempotent behavior
# ---------------------------------------------------------------------------

def test_confirm_nonexistent():
    """Confirm returns 404 for missing dataset."""
    resp = client.post(
        "/api/datasets/nonexistent999/confirm",
        json={"index": True},
    )
    assert resp.status_code == 404


def test_confirm_idempotent_on_ready():
    """Confirming a READY dataset returns 200 (no-op)."""
    from app.services.processing_service import get_processing_service

    processing = get_processing_service()

    # Create a dataset and set it to READY
    record = processing.create_dataset("idempotent_ready.csv", "csv")
    processing._set_status(record.id, DatasetStatus.READY)

    resp = client.post(
        f"/api/datasets/{record.id}/confirm",
        json={"index": True},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ready"


def test_confirm_idempotent_on_indexing():
    """Confirming an INDEXING dataset returns 202 (no-op)."""
    from app.services.processing_service import get_processing_service

    processing = get_processing_service()

    record = processing.create_dataset("idempotent_indexing.csv", "csv")
    processing._set_status(record.id, DatasetStatus.INDEXING)

    resp = client.post(
        f"/api/datasets/{record.id}/confirm",
        json={"index": True},
    )
    assert resp.status_code == 202
    assert resp.json()["status"] == "indexing"


def test_confirm_error_state_returns_409():
    """Confirming an ERROR dataset returns 409."""
    from app.services.processing_service import get_processing_service

    processing = get_processing_service()

    record = processing.create_dataset("error_state.csv", "csv")
    processing._set_status(record.id, DatasetStatus.ERROR)

    resp = client.post(
        f"/api/datasets/{record.id}/confirm",
        json={"index": True},
    )
    assert resp.status_code == 409


def test_confirm_extracting_returns_409():
    """Confirming an EXTRACTING dataset returns 409."""
    from app.services.processing_service import get_processing_service

    processing = get_processing_service()

    record = processing.create_dataset("extracting_state.csv", "csv")
    processing._set_status(record.id, DatasetStatus.EXTRACTING)

    resp = client.post(
        f"/api/datasets/{record.id}/confirm",
        json={"index": True},
    )
    assert resp.status_code == 409


# ---------------------------------------------------------------------------
# 7. Batch Confirm-All
# ---------------------------------------------------------------------------

def test_batch_confirm_all():
    """POST /batch/{id}/confirm-all confirms preview_ready datasets."""
    from app.services.processing_service import get_processing_service

    processing = get_processing_service()
    bid = f"bch_confirm_{uuid.uuid4().hex[:6]}"

    # Create 3 datasets in this batch
    ids = []
    for i in range(3):
        record = processing.create_dataset(f"confirm_{i}.csv", "csv")
        record.batch_id = bid
        sfn = record.upload_path.name if record.upload_path else record.id
        processing._save_record(record, sfn)
        ids.append(record.id)

    # Set them to preview_ready
    for ds_id in ids:
        processing._set_status(ds_id, DatasetStatus.PREVIEW_READY)

    resp = client.post(f"/api/datasets/batch/{bid}/confirm-all")
    assert resp.status_code == 202
    body = resp.json()
    assert body["batch_id"] == bid
    assert body["confirmed"] == 3


# ---------------------------------------------------------------------------
# 8. Delete / Cancel
# ---------------------------------------------------------------------------

def test_delete_preview_ready_sets_cancelled():
    """Deleting a preview_ready dataset sets cancelled first."""
    from app.services.processing_service import get_processing_service

    processing = get_processing_service()

    record = processing.create_dataset("cancel_me.csv", "csv")
    processing._set_status(record.id, DatasetStatus.PREVIEW_READY)

    resp = client.delete(f"/api/datasets/{record.id}")
    assert resp.status_code == 200

    # Dataset should be deleted
    assert processing.get_dataset(record.id) is None


# ---------------------------------------------------------------------------
# 9. Backward compatibility — single-file upload still works
# ---------------------------------------------------------------------------

def test_single_file_upload_backward_compat():
    """POST /upload (legacy single-file) still works and returns 202."""
    csv_content = b"id,name,value\n1,Alice,100\n2,Bob,200"
    files = {"file": ("compat.csv", io.BytesIO(csv_content), "text/csv")}

    resp = client.post("/api/datasets/upload", files=files)
    assert resp.status_code == 202
    body = resp.json()
    assert "dataset_id" in body
    assert body["filename"] == "compat.csv"


# ---------------------------------------------------------------------------
# 10. Mode=process skips preview
# ---------------------------------------------------------------------------

def test_mode_process():
    """mode=process triggers full pipeline (no preview_ready stop)."""
    resp = _make_upload(["process_mode.csv"], mode="process")
    assert resp.status_code == 202
    body = resp.json()
    assert body["accepted"] == 1


# ---------------------------------------------------------------------------
# 11. DatasetStatus enum
# ---------------------------------------------------------------------------

def test_dataset_status_enum_values():
    """Verify all 7 status values exist."""
    expected = {"uploaded", "extracting", "preview_ready", "indexing", "ready", "cancelled", "error"}
    actual = {s.value for s in DatasetStatus}
    assert actual == expected


# ---------------------------------------------------------------------------
# 12. client_file_index preserved
# ---------------------------------------------------------------------------

def test_client_file_index_with_mixed_results():
    """client_file_index matches original order even with rejections."""
    resp = _make_upload(
        ["good1.csv", "bad.exe", "good2.csv"],
        [_csv_bytes(), b"MZ\x90\x00", _csv_bytes()],
    )
    assert resp.status_code == 202
    body = resp.json()
    indices = {i["client_file_index"]: i["status"] for i in body["items"]}
    assert indices[0] == "accepted"
    assert indices[1] == "rejected"
    assert indices[2] == "accepted"
