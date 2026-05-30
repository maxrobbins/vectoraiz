"""
Tests for raw file registration and metadata.
==============================================

Covers: registration, hash computation, MIME detection, large file, missing file.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-03
"""

import os
from pathlib import Path

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.middleware.auth import require_admin
from app.routers.raw_listings import router as raw_listings_router


@pytest.fixture
def app():
    _app = FastAPI()
    _app.include_router(raw_listings_router, prefix="/api/raw")
    return _app


@pytest.fixture
def client(app):
    return TestClient(app)


@pytest.fixture
def protected_app(monkeypatch):
    async def _fake_get_current_user(request):
        request.state.user_role = "admin"
        return {"user_id": "test", "key_id": "test", "scopes": ["admin"], "valid": True}

    monkeypatch.setattr("app.auth.api_key_auth._is_auth_enabled", lambda: True)
    monkeypatch.setattr("app.auth.api_key_auth.get_current_user", _fake_get_current_user)

    _app = FastAPI()
    _app.include_router(
        raw_listings_router,
        prefix="/api/raw",
        dependencies=[Depends(require_admin)],
    )
    return _app


@pytest.fixture
def protected_client(protected_app):
    return TestClient(protected_app)


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file for testing."""
    f = tmp_path / "test_data.csv"
    f.write_text("id,name,value\n1,Alice,100\n2,Bob,200\n")
    return str(f)


@pytest.fixture
def sample_json(tmp_path):
    """Create a sample JSON file for testing."""
    f = tmp_path / "test_data.json"
    f.write_text('{"records": [{"id": 1}, {"id": 2}]}')
    return str(f)


@pytest.fixture
def large_file(tmp_path):
    """Create a 1MB file for testing large file handling."""
    f = tmp_path / "large_file.bin"
    f.write_bytes(os.urandom(1024 * 1024))
    return str(f)


class TestRawFileRegistration:
    """Test POST /api/raw/files — register raw file."""

    def test_register_csv_file(self, client, sample_csv):
        """Registration of a CSV file succeeds with correct metadata."""
        resp = client.post("/api/raw/files", json={"file_path": sample_csv})
        assert resp.status_code == 201
        data = resp.json()
        assert data["filename"] == "test_data.csv"
        assert data["file_size_bytes"] > 0
        assert len(data["content_hash"]) == 64  # SHA256 hex digest
        assert data["mime_type"] == "text/csv"
        assert data["id"]

    def test_register_json_file(self, client, sample_json):
        """Registration of a JSON file detects application/json MIME."""
        resp = client.post("/api/raw/files", json={"file_path": sample_json})
        assert resp.status_code == 201
        data = resp.json()
        assert data["filename"] == "test_data.json"
        assert data["mime_type"] == "application/json"

    def test_register_missing_file(self, client):
        """Registration of a nonexistent file returns 404."""
        resp = client.post("/api/raw/files", json={"file_path": "/nonexistent/file.csv"})
        assert resp.status_code == 404

    def test_register_large_file(self, client, large_file):
        """Large files are hashed correctly via streaming."""
        resp = client.post("/api/raw/files", json={"file_path": large_file})
        assert resp.status_code == 201
        data = resp.json()
        assert data["file_size_bytes"] == 1024 * 1024
        assert len(data["content_hash"]) == 64

    def test_hash_determinism(self, client, sample_csv):
        """Registering the same file twice produces the same content_hash."""
        r1 = client.post("/api/raw/files", json={"file_path": sample_csv})
        r2 = client.post("/api/raw/files", json={"file_path": sample_csv})
        assert r1.json()["content_hash"] == r2.json()["content_hash"]

    def test_upload_raw_file(self, client, tmp_path, monkeypatch):
        """Multipart upload persists into the import directory and registers the file."""
        import_dir = tmp_path / "raw-import"
        monkeypatch.setattr("app.config.settings.raw_file_import_directory", str(import_dir))
        monkeypatch.setattr("app.services.raw_file_service.settings.raw_file_import_directory", str(import_dir))

        resp = client.post(
            "/api/raw/files/upload",
            files={"file": ("browser.csv", b"id,name\n1,Alice\n", "text/csv")},
        )

        assert resp.status_code == 201
        data = resp.json()
        assert data["filename"] == "browser.csv"
        assert data["mime_type"] == "text/csv"
        assert Path(data["file_path"]).parent == import_dir
        assert Path(data["file_path"]).read_text() == "id,name\n1,Alice\n"

    def test_upload_raw_file_appears_in_list(self, client, tmp_path, monkeypatch):
        """Uploaded files are visible through GET /api/raw/files."""
        import_dir = tmp_path / "raw-import"
        monkeypatch.setattr("app.config.settings.raw_file_import_directory", str(import_dir))
        monkeypatch.setattr("app.services.raw_file_service.settings.raw_file_import_directory", str(import_dir))

        upload = client.post(
            "/api/raw/files/upload",
            files={"file": ("listed.json", b'{\"ok\": true}', "application/json")},
        )

        assert upload.status_code == 201
        file_id = upload.json()["id"]

        listed = client.get("/api/raw/files")
        assert listed.status_code == 200
        assert any(item["id"] == file_id for item in listed.json())

    def test_upload_size_limit_exceeded(self, client, tmp_path, monkeypatch):
        """Multipart upload rejects files larger than the configured limit."""
        import_dir = tmp_path / "raw-import"
        monkeypatch.setattr("app.config.settings.raw_file_import_directory", str(import_dir))
        monkeypatch.setattr("app.services.raw_file_service.settings.raw_file_import_directory", str(import_dir))
        monkeypatch.setattr("app.config.settings.raw_file_upload_max_size_mb", 1)
        monkeypatch.setattr("app.routers.raw_listings.settings.raw_file_upload_max_size_mb", 1)

        resp = client.post(
            "/api/raw/files/upload",
            files={"file": ("too-big.bin", os.urandom(1024 * 1024 + 1), "application/octet-stream")},
        )

        assert resp.status_code == 413
        assert "max upload size" in resp.json()["detail"].lower()
        assert not import_dir.exists() or not any(import_dir.iterdir())

    def test_upload_requires_authentication(self, protected_client, tmp_path, monkeypatch):
        """Multipart upload is rejected when the raw router is mounted with auth and no auth is provided."""
        import_dir = tmp_path / "raw-import"
        monkeypatch.setattr("app.config.settings.raw_file_import_directory", str(import_dir))
        monkeypatch.setattr("app.services.raw_file_service.settings.raw_file_import_directory", str(import_dir))

        resp = protected_client.post(
            "/api/raw/files/upload",
            files={"file": ("browser.csv", b"id,name\n1,Alice\n", "text/csv")},
        )

        assert resp.status_code == 401


class TestRawFileGet:
    """Test GET /api/raw/files/{id} — file metadata lookup."""

    def test_get_file_metadata(self, client, sample_csv):
        """Registered file can be retrieved by ID."""
        reg = client.post("/api/raw/files", json={"file_path": sample_csv})
        file_id = reg.json()["id"]

        resp = client.get(f"/api/raw/files/{file_id}")
        assert resp.status_code == 200
        assert resp.json()["id"] == file_id
        assert resp.json()["filename"] == "test_data.csv"

    def test_get_nonexistent_file(self, client):
        """Requesting a nonexistent file ID returns 404."""
        resp = client.get("/api/raw/files/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404


class TestRawFileList:
    """Test GET /api/raw/files — list all files."""

    def test_list_empty(self, client):
        """Empty list returns empty array."""
        resp = client.get("/api/raw/files")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_list_after_registration(self, client, sample_csv, sample_json):
        """Registered files appear in the list."""
        client.post("/api/raw/files", json={"file_path": sample_csv})
        client.post("/api/raw/files", json={"file_path": sample_json})
        resp = client.get("/api/raw/files")
        assert resp.status_code == 200
        assert len(resp.json()) >= 2


class TestRawFileDelete:
    """Test DELETE /api/raw/files/{id} — delete file and metadata."""

    def test_delete_registered_file(self, client, sample_csv):
        """Deleting a registered file returns 204 and removes it."""
        reg = client.post("/api/raw/files", json={"file_path": sample_csv})
        file_id = reg.json()["id"]

        resp = client.delete(f"/api/raw/files/{file_id}")
        assert resp.status_code == 204

        resp = client.get(f"/api/raw/files/{file_id}")
        assert resp.status_code == 404

    def test_delete_nonexistent_file(self, client):
        """Deleting a nonexistent file returns 404."""
        resp = client.delete("/api/raw/files/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404
