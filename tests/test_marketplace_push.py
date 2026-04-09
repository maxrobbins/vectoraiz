"""
Tests for MarketplacePushService (BQ-090)
==========================================
Tests the marketplace push service in isolation using mocked HTTP responses.
"""
import json
import pytest
from pathlib import Path
from unittest.mock import patch, AsyncMock

import httpx

from app.services.marketplace_push_service import (
    MarketplacePushService,
    MarketplacePushError,
)
from app.models.listing_metadata_schemas import ListingMetadata
from app.models.compliance_schemas import ComplianceReport
from app.models.attestation_schemas import QualityAttestation


# ---- Fixtures ----

SAMPLE_LISTING_METADATA = {
    "title": "Sales Data (50.0K rows)",
    "description": "Financial and time-series dataset with 50,000 rows across 8 columns.",
    "tags": ["financial", "time-series", "transactional"],
    "column_summary": [
        {"name": "order_id", "type": "INTEGER", "null_percentage": 0.0, "uniqueness_ratio": 1.0, "sample_values": ["1", "2", "3"]},
        {"name": "amount", "type": "DOUBLE", "null_percentage": 2.5, "uniqueness_ratio": 0.87, "sample_values": ["19.99", "45.50"]},
    ],
    "row_count": 50000,
    "column_count": 8,
    "file_format": "csv",
    "size_bytes": 4500000,
    "freshness_score": 0.95,
    "privacy_score": 0.9,
    "data_categories": ["financial", "time-series"],
    "generated_at": "2026-02-11T00:00:00",
}

SAMPLE_COMPLIANCE = {
    "dataset_id": "test-dataset",
    "flags": [
        {
            "regulation_name": "GDPR",
            "applicable": True,
            "risk_level": "medium",
            "flagged_columns": ["email"],
            "recommended_actions": ["Ensure lawful basis"],
        }
    ],
    "compliance_score": 75,
    "pii_entities_found": ["EMAIL"],
    "generated_at": "2026-02-11T00:00:00",
}

SAMPLE_ATTESTATION = {
    "data_hash": "abc123def456",
    "attestation_hash": "deadbeef1234567890",
    "row_count": 50000,
    "column_count": 8,
    "completeness_score": 0.975,
    "type_consistency_score": 1.0,
    "freshness_score": 0.85,
    "null_ratio_per_column": [],
    "quality_grade": "A",
    "generated_at": "2026-02-11T00:00:00",
    "data_profile": None,
    "pii_risk": None,
    "compliance": None,
    "quality_scores": None,
}


def _write_test_data(tmp_path: Path, dataset_id: str = "test-dataset"):
    """Write sample processing output files."""
    base = tmp_path / dataset_id
    base.mkdir(parents=True, exist_ok=True)

    with open(base / "listing_metadata.json", "w") as f:
        json.dump(SAMPLE_LISTING_METADATA, f)
    with open(base / "compliance_report.json", "w") as f:
        json.dump(SAMPLE_COMPLIANCE, f)
    with open(base / "attestation.json", "w") as f:
        json.dump(SAMPLE_ATTESTATION, f)

    return base


# ---- Unit Tests ----

class TestPayloadBuilding:
    """Test that the service correctly maps local data to ai.market schema."""

    def test_build_payload_maps_fields(self):
        service = MarketplacePushService()
        metadata = ListingMetadata(**SAMPLE_LISTING_METADATA)
        compliance = ComplianceReport(**SAMPLE_COMPLIANCE)
        attestation = QualityAttestation(**SAMPLE_ATTESTATION)

        payload = service._build_payload(
            listing_metadata=metadata,
            compliance=compliance,
            attestation=attestation,
            price=50.0,
            category="financial",
            model_provider="local",
        )

        assert payload["title"] == "Sales Data (50.0K rows)"
        assert payload["price"] == 50.0
        assert payload["model_provider"] == "local"
        assert payload["category"] == "financial"
        assert payload["privacy_score"] == 9.0  # 0.9 * 10
        assert payload["compliance_status"] == "medium_risk"  # score 75
        assert payload["source_row_count"] == 50000
        assert "columns" in payload["schema_info"]
        assert "attestation" in payload["schema_info"]

    def test_build_payload_minimum_price(self):
        service = MarketplacePushService()
        metadata = ListingMetadata(**SAMPLE_LISTING_METADATA)

        payload = service._build_payload(
            listing_metadata=metadata,
            compliance=None,
            attestation=None,
            price=10.0,  # Below minimum
            category="tabular",
            model_provider="local",
        )

        assert payload["price"] == 25.0  # Enforced minimum

    def test_build_payload_without_compliance(self):
        service = MarketplacePushService()
        metadata = ListingMetadata(**SAMPLE_LISTING_METADATA)

        payload = service._build_payload(
            listing_metadata=metadata,
            compliance=None,
            attestation=None,
            price=25.0,
            category="tabular",
            model_provider="local",
        )

        assert payload["compliance_status"] == "not_checked"
        assert payload["compliance_details"] is None

    def test_build_payload_high_risk_compliance(self):
        service = MarketplacePushService()
        metadata = ListingMetadata(**SAMPLE_LISTING_METADATA)
        high_risk = ComplianceReport(
            dataset_id="test",
            compliance_score=30,
            pii_entities_found=["SSN", "CREDIT_CARD"],
            generated_at="2026-02-11T00:00:00",
        )

        payload = service._build_payload(
            listing_metadata=metadata,
            compliance=high_risk,
            attestation=None,
            price=25.0,
            category="tabular",
            model_provider="local",
        )

        assert payload["compliance_status"] == "high_risk"

    def test_build_payload_low_risk_compliance(self):
        service = MarketplacePushService()
        metadata = ListingMetadata(**SAMPLE_LISTING_METADATA)
        low_risk = ComplianceReport(
            dataset_id="test",
            compliance_score=95,
            pii_entities_found=[],
            generated_at="2026-02-11T00:00:00",
        )

        payload = service._build_payload(
            listing_metadata=metadata,
            compliance=low_risk,
            attestation=None,
            price=25.0,
            category="tabular",
            model_provider="local",
        )

        assert payload["compliance_status"] == "low_risk"

    def test_categories_from_metadata(self):
        service = MarketplacePushService()
        metadata = ListingMetadata(**SAMPLE_LISTING_METADATA)

        payload = service._build_payload(
            listing_metadata=metadata,
            compliance=None,
            attestation=None,
            price=25.0,
            category="tabular",
            model_provider="local",
        )

        # data_categories = ["financial", "time-series"]
        # First becomes primary, rest become secondary
        assert payload["category"] == "financial"
        assert payload["secondary_categories"] == ["time-series"]


class TestDataLoading:
    """Test file loading logic."""

    def test_load_listing_metadata(self, tmp_path):
        base = _write_test_data(tmp_path)
        service = MarketplacePushService()
        metadata = service._load_listing_metadata(base)
        assert metadata.title == "Sales Data (50.0K rows)"
        assert metadata.row_count == 50000

    def test_load_missing_metadata_raises(self, tmp_path):
        service = MarketplacePushService()
        with pytest.raises(MarketplacePushError, match="not found"):
            service._load_listing_metadata(tmp_path / "nonexistent")

    def test_load_compliance_optional(self, tmp_path):
        base = tmp_path / "no-compliance"
        base.mkdir()
        service = MarketplacePushService()
        result = service._load_compliance_report(base)
        assert result is None

    def test_load_attestation_optional(self, tmp_path):
        base = tmp_path / "no-attestation"
        base.mkdir()
        service = MarketplacePushService()
        result = service._load_attestation(base)
        assert result is None

    def test_save_publish_result(self, tmp_path):
        base = tmp_path / "test-dataset"
        base.mkdir()
        service = MarketplacePushService()
        result = {"status": "created", "listing_id": "abc123"}
        service._save_publish_result(base, result)

        saved = json.loads((base / "publish_result.json").read_text())
        assert saved["status"] == "created"
        assert saved["listing_id"] == "abc123"


class TestRetryLogic:
    """Test HTTP retry and conflict handling."""

    @pytest.mark.asyncio
    async def test_successful_create(self):
        service = MarketplacePushService()
        service.api_key = "test-key"

        mock_response = httpx.Response(
            201,
            json={"id": "listing-123", "title": "Test"},
            request=httpx.Request("POST", "http://test"),
        )

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_response):
            result = await service._push_with_retry({"title": "Test", "price": 25.0})

        assert result["status"] == "created"
        assert result["listing_id"] == "listing-123"

    @pytest.mark.asyncio
    async def test_auth_failure_no_retry(self):
        service = MarketplacePushService()
        service.api_key = "bad-key"

        mock_response = httpx.Response(
            401,
            text="Unauthorized",
            request=httpx.Request("POST", "http://test"),
        )

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_response):
            with pytest.raises(MarketplacePushError, match="Authentication failed"):
                await service._push_with_retry({"title": "Test"})

    @pytest.mark.asyncio
    async def test_server_error_retries(self):
        service = MarketplacePushService()
        service.api_key = "test-key"

        call_count = 0

        async def mock_post(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return httpx.Response(
                    500,
                    text="Internal Server Error",
                    request=httpx.Request("POST", "http://test"),
                )
            return httpx.Response(
                201,
                json={"id": "listing-456"},
                request=httpx.Request("POST", "http://test"),
            )

        with patch("httpx.AsyncClient.post", side_effect=mock_post):
            with patch("asyncio.sleep", new_callable=AsyncMock):  # Skip actual delays
                result = await service._push_with_retry({"title": "Test"})

        assert result["status"] == "created"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_client_error_no_retry(self):
        service = MarketplacePushService()
        service.api_key = "test-key"

        mock_response = httpx.Response(
            422,
            text='{"detail": "Validation error"}',
            request=httpx.Request("POST", "http://test"),
        )

        with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_response):
            with pytest.raises(MarketplacePushError, match="rejected"):
                await service._push_with_retry({"title": "Test"})
