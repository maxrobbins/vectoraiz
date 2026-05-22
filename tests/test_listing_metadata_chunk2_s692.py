import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.models.listing_metadata_schemas import ListingMetadata
from app.routers.datasets import generate_listing_metadata as route_generate_listing_metadata
from app.services.listing_metadata_service import ListingMetadataService
from app.services.marketplace_push_service import MarketplacePushService
from app.services.processing_service import ProcessingStatus

BASE_METADATA = {"title": "Preview Document (0 rows)", "description": "Document dataset with extracted metadata.", "tags": ["text"], "column_summary": [], "row_count": 0, "column_count": 0, "file_format": "pdf", "size_bytes": 1234, "freshness_score": 0.9, "data_categories": ["text"], "generated_at": "2026-05-22T00:00:00+00:00"}


def _metadata(score=None):
    return {**BASE_METADATA, "privacy_score": score}


def _map_data(root):
    return lambda value: root / str(value).removeprefix("/data/processed/") if str(value).startswith("/data/processed/") else Path(value)


def _payload(score):
    return MarketplacePushService()._build_payload(ListingMetadata(**_metadata(score)), None, None, 25.0, "tabular", "local")


async def _call_route(status, score=None):
    listing_service = SimpleNamespace(generate_listing_metadata=AsyncMock(return_value=ListingMetadata(**_metadata(score))))
    processing = SimpleNamespace(get_dataset=lambda dataset_id: SimpleNamespace(status=status))
    result = await route_generate_listing_metadata("dataset", listing_service=listing_service, processing=processing, _meter=None)
    return result, listing_service


def test_listing_metadata_service_compute_privacy_score_returns_none_when_pii_scan_absent(tmp_path):
    with patch("app.services.listing_metadata_service.Path", side_effect=_map_data(tmp_path)):
        assert ListingMetadataService()._compute_privacy_score("missing-scan") is None


def test_listing_metadata_service_compute_privacy_score_returns_none_when_pii_scan_missing_field(tmp_path):
    dataset_dir = tmp_path / "missing-field"
    dataset_dir.mkdir()
    (dataset_dir / "pii_scan.json").write_text(json.dumps({"overall_risk": "low"}))
    with patch("app.services.listing_metadata_service.Path", side_effect=_map_data(tmp_path)):
        assert ListingMetadataService()._compute_privacy_score("missing-field") is None


def test_listing_metadata_service_compute_privacy_score_returns_value_when_pii_scan_present_with_field(tmp_path):
    dataset_dir = tmp_path / "scored"
    dataset_dir.mkdir()
    (dataset_dir / "pii_scan.json").write_text(json.dumps({"privacy_score": 8.75}))
    with patch("app.services.listing_metadata_service.Path", side_effect=_map_data(tmp_path)):
        assert ListingMetadataService()._compute_privacy_score("scored") == 8.75


@pytest.mark.asyncio
async def test_listing_metadata_service_line_169_round_handles_none(tmp_path):
    processed_file = tmp_path / "document.pdf"
    processed_file.write_text("content")
    duckdb = SimpleNamespace(
        __enter__=lambda self: SimpleNamespace(get_enhanced_metadata=lambda path: {"file_type": "pdf", "size_bytes": 7, "row_count": 0, "column_count": 0, "column_profiles": []}),
        __exit__=lambda self, exc_type, exc, tb: False,
    )
    processing = SimpleNamespace(get_dataset=lambda requested_id: SimpleNamespace(processed_path=processed_file))
    service = ListingMetadataService()
    with (
        patch("app.services.processing_service.get_processing_service", return_value=processing),
        patch("app.services.listing_metadata_service.ephemeral_duckdb_service", return_value=duckdb),
        patch("app.services.listing_metadata_service.Path", side_effect=_map_data(tmp_path)),
        patch.object(service, "_compute_privacy_score", return_value=None),
    ):
        listing = await service.generate_listing_metadata("line-169-none")
    assert listing.privacy_score is None


def test_marketplace_push_service_handles_null_privacy_score_no_multiply_typeerror():
    payload = _payload(None)
    assert payload["privacy_score"] is None
    assert payload["privacy_scan_status"] == "not_scanned"


def test_marketplace_push_service_handles_value_privacy_score_no_multiplier():
    payload = _payload(7.3)
    assert payload["privacy_score"] == 7.3
    assert payload["privacy_scan_status"] == "scanned"


@pytest.mark.asyncio
async def test_datasets_router_precondition_relax_accepts_preview_ready():
    result, listing_service = await _call_route(ProcessingStatus.PREVIEW_READY, None)
    assert result.privacy_score is None
    listing_service.generate_listing_metadata.assert_awaited_once_with("dataset")


@pytest.mark.asyncio
async def test_datasets_router_precondition_relax_accepts_ready_regression():
    result, listing_service = await _call_route(ProcessingStatus.READY, 10.0)
    assert result.privacy_score == 10.0
    listing_service.generate_listing_metadata.assert_awaited_once_with("dataset")


@pytest.mark.asyncio
async def test_datasets_router_precondition_relax_rejects_uploaded():
    listing_service = SimpleNamespace(generate_listing_metadata=AsyncMock())
    processing = SimpleNamespace(get_dataset=lambda dataset_id: SimpleNamespace(status=ProcessingStatus.UPLOADED))
    with pytest.raises(HTTPException) as exc_info:
        await route_generate_listing_metadata("dataset", listing_service=listing_service, processing=processing, _meter=None)
    assert exc_info.value.status_code == 400
    listing_service.generate_listing_metadata.assert_not_called()


def test_listing_metadata_validation_fails_for_out_of_range_privacy_score():
    for bad_score in (-0.1, 11.0):
        with pytest.raises(ValidationError):
            ListingMetadata(**_metadata(bad_score))


@pytest.mark.asyncio
async def test_e2e_non_vector_publish_with_null_privacy_score(tmp_path):
    dataset_id = "e2e-null-privacy"
    base_path = tmp_path / dataset_id
    base_path.mkdir()
    (base_path / "listing_metadata.json").write_text(json.dumps(_metadata(None)))
    service = MarketplacePushService()
    service.api_key = "test-key"
    service.base_url = "https://market.test"
    response = httpx.Response(201, json={"id": "listing-s692"}, request=httpx.Request("POST", "https://market.test/api/v1/listings/"))
    with (
        patch("app.services.marketplace_push_service.Path", side_effect=_map_data(tmp_path)),
        patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=response) as post,
    ):
        result = await service.push_to_marketplace(dataset_id, price=25.0, category="documents")
    payload = post.await_args.kwargs["json"]
    assert result["status"] == "created"
    assert payload["data_format"] == "pdf"
    assert payload["privacy_score"] is None
    assert payload["privacy_scan_status"] == "not_scanned"
