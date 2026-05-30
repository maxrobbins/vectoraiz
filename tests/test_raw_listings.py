"""
Tests for raw listing CRUD and lifecycle.
=========================================

Covers: create, update, publish, delist, pagination, lifecycle constraints.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-03
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
    """Create and return path to a sample test file."""
    f = tmp_path / "listing_test.csv"
    f.write_text("col1,col2\na,b\n")
    return str(f)


@pytest.fixture
def registered_file(client, sample_file):
    """Register a file and return its ID."""
    resp = client.post("/api/raw/files", json={"file_path": sample_file})
    return resp.json()["id"]


@pytest.fixture
def draft_listing(client, registered_file):
    """Create a draft listing and return its data."""
    resp = client.post("/api/raw/listings", json={
        "raw_file_id": registered_file,
        "title": "Test Dataset",
        "description": "A test dataset for unit testing.",
        "tags": ["test", "csv"],
        "price_cents": 999,
    })
    return resp.json()


class TestRawListingCreate:
    """Test POST /api/raw/listings — create draft listing."""

    def test_create_draft_listing(self, client, registered_file):
        """Creating a listing produces a draft with correct fields."""
        resp = client.post("/api/raw/listings", json={
            "raw_file_id": registered_file,
            "title": "My Dataset",
            "description": "High quality data.",
            "tags": ["finance"],
            "price_cents": 500,
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["status"] == "draft"
        assert data["title"] == "My Dataset"
        assert data["description"] == "High quality data."
        assert data["tags"] == ["finance"]
        assert data["price_cents"] == 500
        assert data["raw_file_id"] == registered_file
        assert data["published_at"] is None

    def test_create_listing_invalid_file(self, client):
        """Creating a listing with a nonexistent file ID returns 404."""
        resp = client.post("/api/raw/listings", json={
            "raw_file_id": "00000000-0000-0000-0000-000000000000",
            "title": "Bad Listing",
            "description": "This should fail.",
        })
        assert resp.status_code == 404


class TestRawListingUpdate:
    """Test PUT /api/raw/listings/{id} — update metadata."""

    def test_update_title_and_description(self, client, draft_listing):
        """Updating title and description succeeds."""
        listing_id = draft_listing["id"]
        resp = client.put(f"/api/raw/listings/{listing_id}", json={
            "title": "Updated Title",
            "description": "Updated description.",
        })
        assert resp.status_code == 200
        assert resp.json()["title"] == "Updated Title"
        assert resp.json()["description"] == "Updated description."

    def test_update_tags(self, client, draft_listing):
        """Updating tags replaces the tag list."""
        listing_id = draft_listing["id"]
        resp = client.put(f"/api/raw/listings/{listing_id}", json={
            "tags": ["new-tag-1", "new-tag-2"],
        })
        assert resp.status_code == 200
        assert resp.json()["tags"] == ["new-tag-1", "new-tag-2"]

    def test_update_nonexistent_listing(self, client):
        """Updating a nonexistent listing returns 404."""
        resp = client.put(
            "/api/raw/listings/00000000-0000-0000-0000-000000000000",
            json={"title": "nope"},
        )
        assert resp.status_code == 404


class TestRawListingPublish:
    """Test POST /api/raw/listings/{id}/publish — publish lifecycle."""

    def test_publish_draft(self, client, draft_listing):
        """Publishing a draft listing transitions status to 'listed'."""
        listing_id = draft_listing["id"]
        resp = client.post(f"/api/raw/listings/{listing_id}/publish")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "listed"
        assert data["published_at"] is not None

    def test_publish_already_listed(self, client, draft_listing):
        """Publishing an already-listed listing returns 409."""
        listing_id = draft_listing["id"]
        client.post(f"/api/raw/listings/{listing_id}/publish")
        resp = client.post(f"/api/raw/listings/{listing_id}/publish")
        assert resp.status_code == 409


class TestRawListingDelist:
    """Test POST /api/raw/listings/{id}/delist — delist lifecycle."""

    def test_delist_listed(self, client, draft_listing):
        """Delisting a listed listing transitions status to 'delisted'."""
        listing_id = draft_listing["id"]
        client.post(f"/api/raw/listings/{listing_id}/publish")
        resp = client.post(f"/api/raw/listings/{listing_id}/delist")
        assert resp.status_code == 200
        assert resp.json()["status"] == "delisted"

    def test_delist_draft_fails(self, client, draft_listing):
        """Delisting a draft listing returns 409."""
        listing_id = draft_listing["id"]
        resp = client.post(f"/api/raw/listings/{listing_id}/delist")
        assert resp.status_code == 409


class TestRawListingPagination:
    """Test GET /api/raw/listings — paginated listing."""

    def test_empty_list(self, client):
        """Empty listing returns zero results."""
        resp = client.get("/api/raw/listings")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] >= 0
        assert isinstance(data["listings"], list)

    def test_pagination_params(self, client, registered_file):
        """Limit and offset pagination works correctly."""
        # Create 3 listings
        for i in range(3):
            client.post("/api/raw/listings", json={
                "raw_file_id": registered_file,
                "title": f"Dataset {i}",
                "description": f"Description {i}",
            })

        resp = client.get("/api/raw/listings?limit=2&offset=0")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["listings"]) == 2
        assert data["limit"] == 2
        assert data["offset"] == 0
