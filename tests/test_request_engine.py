"""
Tests for BQ-VZ-REQUEST-ENGINE Slice B
=======================================

Covers: sync service, match engine scoring, API endpoints.
"""

import json
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from app.core.database import get_engine, get_session
from app.main import app
from app.models.cached_requests import CachedRequest, ResponseDraft
from app.models.dataset import DatasetRecord
from app.services.request_match_service import (
    match_request,
    _category_score,
    _text_similarity,
    _tokenize,
    _size_score,
    _freshness_score,
)
from app.services.request_sync_service import upsert_cached_requests, get_sync_cursor


client = TestClient(app)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_cached_request(**overrides) -> CachedRequest:
    defaults = dict(
        id=str(uuid.uuid4()),
        marketplace_request_id=str(uuid.uuid4()),
        title="Need financial transaction data",
        description="Looking for anonymized credit card transaction records with merchant categories",
        categories=json.dumps(["finance", "transactions", "banking"]),
        urgency="high",
        status="open",
        published_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        synced_at=datetime.now(timezone.utc),
    )
    defaults.update(overrides)
    return CachedRequest(**defaults)


def _make_dataset(
    metadata_title="",
    metadata_desc="",
    tags=None,
    data_categories=None,
    row_count=1000,
    freshness_score=0.8,
    status="ready",
    **overrides,
) -> DatasetRecord:
    meta = {
        "title": metadata_title or "Test Dataset",
        "description": metadata_desc or "A test dataset",
        "tags": tags or [],
        "data_categories": data_categories or [],
        "row_count": row_count,
        "column_count": 5,
        "file_format": "csv",
        "size_bytes": 1024,
        "freshness_score": freshness_score,
        "privacy_score": 1.0,
    }
    defaults = dict(
        id=str(uuid.uuid4()),
        original_filename="test.csv",
        storage_filename="test.csv",
        file_type="csv",
        file_size_bytes=1024,
        status=status,
        metadata_json=json.dumps(meta),
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )
    defaults.update(overrides)
    return DatasetRecord(**defaults)


# ---------------------------------------------------------------------------
# Sync Service Tests
# ---------------------------------------------------------------------------

class TestSyncService:
    def test_upsert_new_items(self):
        mp_id = str(uuid.uuid4())
        items = [{
            "id": mp_id,
            "title": "Test Request",
            "description": "Need data",
            "categories": ["health"],
            "urgency": "low",
            "status": "open",
            "published_at": "2026-04-01T00:00:00Z",
        }]
        new, updated = upsert_cached_requests(items)
        assert new == 1
        assert updated == 0

        # Verify it was persisted
        cursor = get_sync_cursor()
        assert cursor == mp_id

    def test_upsert_updates_existing(self):
        mp_id = str(uuid.uuid4())
        items = [{
            "id": mp_id,
            "title": "Original",
            "description": "v1",
            "categories": [],
            "status": "open",
        }]
        upsert_cached_requests(items)

        # Update
        items[0]["title"] = "Updated"
        items[0]["description"] = "v2"
        new, updated = upsert_cached_requests(items)
        assert new == 0
        assert updated == 1

    def test_upsert_empty_list(self):
        new, updated = upsert_cached_requests([])
        assert new == 0
        assert updated == 0

    @pytest.mark.asyncio
    async def test_poll_requests_mock(self):
        from app.services.request_sync_service import poll_requests

        mock_response = {
            "items": [{"id": "abc", "title": "Test"}],
            "next_cursor": None,
        }

        with patch("app.services.request_sync_service.httpx.AsyncClient") as MockClient:
            mock_client = AsyncMock()
            mock_resp = MagicMock()
            mock_resp.json.return_value = mock_response
            mock_resp.raise_for_status = MagicMock()
            mock_client.get.return_value = mock_resp
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            MockClient.return_value = mock_client

            items, cursor = await poll_requests("https://api.example.com")
            assert len(items) == 1
            assert items[0]["id"] == "abc"
            assert cursor is None


# ---------------------------------------------------------------------------
# Match Engine Tests
# ---------------------------------------------------------------------------

class TestMatchEngine:
    def test_high_match(self):
        """Dataset with overlapping categories and text should score high."""
        req = _make_cached_request(
            title="Need financial transaction data",
            description="Credit card transactions with merchant categories",
            categories=json.dumps(["finance", "transactions"]),
        )
        ds = _make_dataset(
            metadata_title="Credit Card Transactions 2025",
            metadata_desc="Anonymized credit card transaction records with merchant category codes",
            tags=["finance", "transactions", "credit-cards"],
            data_categories=["finance"],
            row_count=50000,
            freshness_score=0.9,
        )
        matches = match_request(req, [ds])
        assert len(matches) == 1
        assert matches[0].score >= 0.5
        assert matches[0].require_review is False or matches[0].score >= 0.7

    def test_low_match(self):
        """Dataset with no category overlap and different text should score low."""
        req = _make_cached_request(
            title="Need weather data for agriculture",
            description="Historical weather patterns for farming regions",
            categories=json.dumps(["weather", "agriculture"]),
        )
        ds = _make_dataset(
            metadata_title="Social Media Engagement Metrics",
            metadata_desc="Twitter and Instagram engagement analytics",
            tags=["social-media", "marketing"],
            data_categories=["marketing"],
            row_count=500,
        )
        matches = match_request(req, [ds])
        assert len(matches) == 1
        assert matches[0].score < 0.5
        assert matches[0].require_review is True

    def test_no_match_empty_datasets(self):
        """No datasets → empty results."""
        req = _make_cached_request()
        matches = match_request(req, [])
        assert matches == []

    def test_ranking_order(self):
        """Multiple datasets should be ranked by score descending."""
        req = _make_cached_request(
            title="Financial data",
            categories=json.dumps(["finance"]),
        )
        high = _make_dataset(
            metadata_title="Finance Dataset",
            tags=["finance"],
            data_categories=["finance"],
        )
        low = _make_dataset(
            metadata_title="Cooking Recipes",
            tags=["food", "cooking"],
            data_categories=["food"],
        )
        matches = match_request(req, [high, low])
        assert len(matches) == 2
        assert matches[0].score >= matches[1].score

    def test_score_reasons_populated(self):
        """Score reasons should contain all four factors."""
        req = _make_cached_request()
        ds = _make_dataset()
        matches = match_request(req, [ds])
        reasons = matches[0].score_reasons
        assert "category_overlap" in reasons
        assert "text_similarity" in reasons
        assert "freshness" in reasons
        assert "size" in reasons

    def test_row_count_range(self):
        """Row count range should be a human-readable string."""
        req = _make_cached_request()
        ds = _make_dataset(row_count=5000)
        matches = match_request(req, [ds])
        assert matches[0].row_count_range == "1K-10K"

    def test_freshness_category(self):
        req = _make_cached_request()
        ds = _make_dataset(freshness_score=0.9)
        matches = match_request(req, [ds])
        assert matches[0].freshness_category == "fresh"


class TestScoringHelpers:
    def test_category_score_full_overlap(self):
        assert _category_score(["a", "b"], ["a", "b"]) == 1.0

    def test_category_score_no_overlap(self):
        assert _category_score(["a"], ["b"]) == 0.0

    def test_category_score_partial(self):
        score = _category_score(["a", "b"], ["b", "c"])
        assert 0.0 < score < 1.0

    def test_category_score_empty(self):
        assert _category_score([], ["a"]) == 0.0
        assert _category_score(["a"], []) == 0.0

    def test_text_similarity_identical(self):
        tokens = _tokenize("financial data analysis")
        score = _text_similarity(tokens, tokens)
        assert score > 0.99

    def test_text_similarity_no_overlap(self):
        t1 = _tokenize("financial data")
        t2 = _tokenize("cooking recipes")
        score = _text_similarity(t1, t2)
        assert score == 0.0

    def test_size_score_sweet_spot(self):
        assert _size_score(10000) == 1.0

    def test_size_score_tiny(self):
        assert _size_score(5) == 0.2

    def test_size_score_unknown(self):
        assert _size_score(0) == 0.3

    def test_freshness_from_created_at(self):
        ds = _make_dataset(freshness_score=0.0)
        ds.created_at = datetime.now(timezone.utc) - timedelta(days=3)
        score, cat = _freshness_score(ds, None)
        assert score == 1.0
        assert cat == "fresh"


# ---------------------------------------------------------------------------
# API Endpoint Tests
# ---------------------------------------------------------------------------

class TestAPIEndpoints:
    def _seed_request(self) -> str:
        """Insert a cached request and return its id."""
        req = _make_cached_request()
        req_id = req.id
        with Session(get_engine()) as session:
            session.add(req)
            session.commit()
        return req_id

    def _seed_dataset(self) -> str:
        """Insert a ready dataset and return its id."""
        ds = _make_dataset(
            metadata_title="Seeded Dataset",
            tags=["finance"],
            data_categories=["finance"],
        )
        ds_id = ds.id
        with Session(get_engine()) as session:
            session.add(ds)
            session.commit()
        return ds_id

    def test_list_cached_requests(self):
        self._seed_request()
        resp = client.get("/api/request-engine/cached-requests")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) >= 1

    def test_list_cached_requests_with_status_filter(self):
        self._seed_request()
        resp = client.get("/api/request-engine/cached-requests?status=open")
        assert resp.status_code == 200

    def test_get_matches(self):
        req_id = self._seed_request()
        self._seed_dataset()
        resp = client.get(f"/api/request-engine/cached-requests/{req_id}/matches")
        assert resp.status_code == 200
        matches = resp.json()
        assert isinstance(matches, list)

    def test_get_matches_not_found(self):
        resp = client.get(f"/api/request-engine/cached-requests/{uuid.uuid4()}/matches")
        assert resp.status_code == 404

    def test_create_draft(self):
        req_id = self._seed_request()
        ds_id = self._seed_dataset()
        body = {
            "matched_dataset_id": ds_id,
            "title": "My Response Draft",
            "description": "This dataset matches well",
            "score": 0.85,
            "score_reasons": {"category_overlap": {"score": 0.9}},
            "require_review": False,
        }
        resp = client.post(
            f"/api/request-engine/cached-requests/{req_id}/draft",
            json=body,
        )
        assert resp.status_code == 201
        draft = resp.json()
        assert draft["cached_request_id"] == req_id
        assert draft["matched_dataset_id"] == ds_id
        assert draft["status"] == "draft"
        assert draft["score"] == 0.85

    def test_create_draft_not_found(self):
        body = {
            "matched_dataset_id": str(uuid.uuid4()),
            "title": "Test",
        }
        resp = client.post(
            f"/api/request-engine/cached-requests/{uuid.uuid4()}/draft",
            json=body,
        )
        assert resp.status_code == 404

    def test_sync_endpoint_requires_url(self):
        resp = client.post("/api/request-engine/sync")
        assert resp.status_code == 422  # Missing required query param
