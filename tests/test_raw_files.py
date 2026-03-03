"""
Tests for raw file registration and metadata.
==============================================

Covers: registration, hash computation, MIME detection, large file, missing file.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-03
"""

import os
import tempfile

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

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
