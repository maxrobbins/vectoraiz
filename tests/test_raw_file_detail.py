"""
Tests for Slice D: Raw File Detail Enhancements
=================================================

Covers: GET file detail with listing status, PATCH metadata editor,
listing readiness logic, vectorize CTA routing.

Phase: BQ-VZ-DATA-CHANNEL Slice D
"""

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
def sample_file(tmp_path):
    f = tmp_path / "detail_test.csv"
    f.write_text("id,name,value\n1,Alice,100\n")
    return str(f)


@pytest.fixture
def registered_file(client, sample_file):
    resp = client.post("/api/raw/files", json={"file_path": sample_file})
    return resp.json()


class TestRawFileDetailEndpoint:
    """Test GET /api/raw/files/{id} — detail with listing status."""

    def test_detail_returns_full_data(self, client, registered_file):
        """GET detail returns all fields including metadata and listing_status."""
        file_id = registered_file["id"]
        resp = client.get(f"/api/raw/files/{file_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == file_id
        assert data["filename"] == "detail_test.csv"
        assert data["file_size_bytes"] > 0
        assert data["content_hash"]
        assert data["mime_type"] == "text/csv"
        assert "metadata" in data
        assert "listing_status" in data

    def test_detail_listing_status_none_when_no_listing(self, client, registered_file):
        """listing_status is null when no listing exists for this file."""
        file_id = registered_file["id"]
        resp = client.get(f"/api/raw/files/{file_id}")
        assert resp.status_code == 200
        assert resp.json()["listing_status"] is None

    def test_detail_listing_status_draft(self, client, registered_file):
        """listing_status reflects draft when a draft listing exists."""
        file_id = registered_file["id"]
        client.post("/api/raw/listings", json={
            "raw_file_id": file_id,
            "title": "Test",
            "description": "Test desc",
        })
        resp = client.get(f"/api/raw/files/{file_id}")
        assert resp.status_code == 200
        assert resp.json()["listing_status"] == "draft"

    def test_detail_listing_status_listed(self, client, registered_file):
        """listing_status reflects listed after publishing."""
        file_id = registered_file["id"]
        listing = client.post("/api/raw/listings", json={
            "raw_file_id": file_id,
            "title": "Test",
            "description": "Test desc",
        }).json()
        client.post(f"/api/raw/listings/{listing['id']}/publish")
        resp = client.get(f"/api/raw/files/{file_id}")
        assert resp.status_code == 200
        assert resp.json()["listing_status"] == "listed"


class TestMetadataEditorPatch:
    """Test PATCH /api/raw/files/{id} — metadata editor saves changes."""

    def test_patch_saves_metadata(self, client, registered_file):
        """PATCH updates the metadata field and returns updated data."""
        file_id = registered_file["id"]
        metadata = {
            "title": "My Dataset",
            "description": "Financial quarterly data",
            "tags": ["finance", "quarterly"],
        }
        resp = client.patch(f"/api/raw/files/{file_id}", json={"metadata": metadata})
        assert resp.status_code == 200
        data = resp.json()
        assert data["metadata"]["title"] == "My Dataset"
        assert data["metadata"]["description"] == "Financial quarterly data"
        assert data["metadata"]["tags"] == ["finance", "quarterly"]

    def test_patch_preserves_existing_metadata(self, client, registered_file):
        """PATCH with new fields preserves the full metadata object."""
        file_id = registered_file["id"]
        # First save
        client.patch(f"/api/raw/files/{file_id}", json={
            "metadata": {"title": "V1", "custom_field": "keep_me"},
        })
        # Second save with updated title
        resp = client.patch(f"/api/raw/files/{file_id}", json={
            "metadata": {"title": "V2", "custom_field": "keep_me", "new_field": "added"},
        })
        assert resp.status_code == 200
        meta = resp.json()["metadata"]
        assert meta["title"] == "V2"
        assert meta["custom_field"] == "keep_me"
        assert meta["new_field"] == "added"

    def test_patch_nonexistent_file(self, client):
        """PATCH on nonexistent file returns 404."""
        resp = client.patch(
            "/api/raw/files/00000000-0000-0000-0000-000000000000",
            json={"metadata": {"title": "nope"}},
        )
        assert resp.status_code == 404

    def test_patch_returns_listing_status(self, client, registered_file):
        """PATCH response includes listing_status."""
        file_id = registered_file["id"]
        client.post("/api/raw/listings", json={
            "raw_file_id": file_id,
            "title": "Test",
            "description": "Test desc",
        })
        resp = client.patch(f"/api/raw/files/{file_id}", json={
            "metadata": {"title": "Updated"},
        })
        assert resp.status_code == 200
        assert resp.json()["listing_status"] == "draft"


class TestDeleteReturns204NoBody:
    """Test DELETE /api/raw/files/{id} returns 204 with no body (Gate 3 R1 fix)."""

    def test_delete_returns_204_no_body(self, client, registered_file):
        """DELETE returns 204 with empty body — frontend must not parse JSON."""
        file_id = registered_file["id"]
        resp = client.delete(f"/api/raw/files/{file_id}")
        assert resp.status_code == 204
        assert resp.content == b""

    def test_delete_nonexistent_returns_404(self, client):
        """DELETE on nonexistent file returns 404 (still JSON-parseable)."""
        resp = client.delete("/api/raw/files/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404
        assert "detail" in resp.json()


class TestPriceCentsInResponse:
    """Test that price_cents from listing is included in file detail response (Gate 3 R1 fix)."""

    def test_price_cents_null_when_no_listing(self, client, registered_file):
        """price_cents is null when no listing exists."""
        file_id = registered_file["id"]
        resp = client.get(f"/api/raw/files/{file_id}")
        assert resp.status_code == 200
        assert resp.json()["price_cents"] is None

    def test_price_cents_from_listing(self, client, registered_file):
        """price_cents reflects the listing's price."""
        file_id = registered_file["id"]
        client.post("/api/raw/listings", json={
            "raw_file_id": file_id,
            "title": "Priced",
            "description": "Has a price",
            "price_cents": 999,
        })
        resp = client.get(f"/api/raw/files/{file_id}")
        assert resp.status_code == 200
        assert resp.json()["price_cents"] == 999

    def test_price_cents_zero_means_free(self, client, registered_file):
        """price_cents=0 is valid (free listing) and distinct from null."""
        file_id = registered_file["id"]
        client.post("/api/raw/listings", json={
            "raw_file_id": file_id,
            "title": "Free",
            "description": "Free listing",
            "price_cents": 0,
        })
        resp = client.get(f"/api/raw/files/{file_id}")
        assert resp.status_code == 200
        assert resp.json()["price_cents"] == 0

    def test_price_cents_in_patch_response(self, client, registered_file):
        """PATCH response also includes price_cents from listing."""
        file_id = registered_file["id"]
        client.post("/api/raw/listings", json={
            "raw_file_id": file_id,
            "title": "Priced",
            "description": "Has a price",
            "price_cents": 500,
        })
        resp = client.patch(f"/api/raw/files/{file_id}", json={
            "metadata": {"title": "Updated"},
        })
        assert resp.status_code == 200
        assert resp.json()["price_cents"] == 500


class TestFileContentEndpoint:
    """Test GET /api/raw/files/{id}/content — authenticated file download (Gate 3 R1 fix)."""

    def test_content_download(self, client, registered_file):
        """Content endpoint streams the file with correct headers."""
        file_id = registered_file["id"]
        resp = client.get(f"/api/raw/files/{file_id}/content")
        assert resp.status_code == 200
        assert b"id,name,value" in resp.content

    def test_content_nonexistent_404(self, client):
        """Content endpoint returns 404 for missing file."""
        resp = client.get("/api/raw/files/00000000-0000-0000-0000-000000000000/content")
        assert resp.status_code == 404


class TestListingReadinessLogic:
    """Test listing readiness checklist logic (backend data contract)."""

    def test_readiness_metadata_complete(self, client, registered_file):
        """File with complete metadata (title, description, tags) passes readiness."""
        file_id = registered_file["id"]
        metadata = {
            "title": "Complete Dataset",
            "description": "A fully described dataset",
            "tags": ["data", "test"],
        }
        resp = client.patch(f"/api/raw/files/{file_id}", json={"metadata": metadata})
        assert resp.status_code == 200
        meta = resp.json()["metadata"]
        # Verify all required fields are present for frontend readiness check
        assert meta.get("title")
        assert meta.get("description")
        assert isinstance(meta.get("tags"), list) and len(meta["tags"]) > 0

    def test_readiness_metadata_incomplete(self, client, registered_file):
        """File with partial metadata fails readiness check."""
        file_id = registered_file["id"]
        # Only title, no description or tags
        resp = client.patch(f"/api/raw/files/{file_id}", json={
            "metadata": {"title": "Only Title"},
        })
        assert resp.status_code == 200
        meta = resp.json()["metadata"]
        assert meta.get("title")
        assert not meta.get("description")
        assert not meta.get("tags")
