"""
Marketplace Push Service for vectorAIz
=======================================
BQ-090: Push listing metadata, compliance report, and quality attestation
from local vectorAIz instance to ai.market backend API.

Non-custodial: Only metadata is sent — never actual data rows.
Auth: Uses VECTORAIZ_INTERNAL_API_KEY (X-API-Key header) which maps
to the system user on ai.market.

Retry: Exponential backoff (3 attempts, 1s/2s/4s).
Conflict: 409 → PATCH update instead of POST create.
"""
import json
import logging
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import httpx

from app.config import settings
from app.models.listing_metadata_schemas import ListingMetadata
from app.models.compliance_schemas import ComplianceReport
from app.models.attestation_schemas import QualityAttestation

logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3
BACKOFF_BASE = 1.0  # seconds: 1, 2, 4
REQUEST_TIMEOUT = 30.0


class MarketplacePushError(Exception):
    """Raised when the marketplace push fails after all retries."""
    def __init__(self, message: str, status_code: Optional[int] = None, detail: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail


class MarketplacePushService:
    """
    Pushes processed dataset metadata to ai.market.
    """

    def __init__(self):
        self.base_url = settings.ai_market_url.rstrip("/")
        self.api_key = settings.internal_api_key
        if not self.api_key:
            logger.warning("VECTORAIZ_INTERNAL_API_KEY not set — marketplace push will fail auth")

    async def push_to_marketplace(
        self,
        dataset_id: str,
        price: float = 25.0,
        category: str = "tabular",
        model_provider: str = "local",
    ) -> Dict[str, Any]:
        """
        Push a processed dataset to ai.market.

        Reads listing_metadata.json, compliance report, and attestation
        from /data/processed/{dataset_id}/, builds the ListingCreate payload,
        and POSTs to the marketplace API.

        Args:
            dataset_id: The local dataset identifier.
            price: Listing price in USD (minimum $25).
            category: Primary category slug for the listing.
            model_provider: AI model provider used for analysis.

        Returns:
            Dict with marketplace listing ID, URL, and status.

        Raises:
            MarketplacePushError: If push fails after all retries.
        """
        base_path = Path(f"/data/processed/{dataset_id}")

        # 1. Load local processing results
        listing_metadata = self._load_listing_metadata(base_path)
        compliance = self._load_compliance_report(base_path)
        attestation = self._load_attestation(base_path)

        # 2. Build ListingCreate payload
        payload = self._build_payload(
            listing_metadata=listing_metadata,
            compliance=compliance,
            attestation=attestation,
            price=price,
            category=category,
            model_provider=model_provider,
        )

        # 3. Push to ai.market with retry
        result = await self._push_with_retry(payload)

        # 4. Save result locally
        self._save_publish_result(base_path, result)

        logger.info(f"Dataset {dataset_id} published to ai.market: {result.get('listing_id', 'unknown')}")
        return result

    # ---- Data Loading ----

    def _load_listing_metadata(self, base_path: Path) -> ListingMetadata:
        """Load listing metadata from processing output."""
        meta_path = base_path / "listing_metadata.json"
        if not meta_path.exists():
            raise MarketplacePushError(
                f"Listing metadata not found at {meta_path}. "
                "Run the processing pipeline first."
            )
        with open(meta_path) as f:
            data = json.load(f)
        return ListingMetadata(**data)

    def _load_compliance_report(self, base_path: Path) -> Optional[ComplianceReport]:
        """Load compliance report if available."""
        report_path = base_path / "compliance_report.json"
        if not report_path.exists():
            logger.info("No compliance report found — pushing without compliance data")
            return None
        try:
            with open(report_path) as f:
                data = json.load(f)
            return ComplianceReport(**data)
        except Exception as e:
            logger.warning(f"Failed to load compliance report: {e}")
            return None

    def _load_attestation(self, base_path: Path) -> Optional[QualityAttestation]:
        """Load quality attestation if available."""
        att_path = base_path / "attestation.json"
        if not att_path.exists():
            logger.info("No attestation found — pushing without attestation data")
            return None
        try:
            with open(att_path) as f:
                data = json.load(f)
            return QualityAttestation(**data)
        except Exception as e:
            logger.warning(f"Failed to load attestation: {e}")
            return None

    # ---- Payload Building ----

    def _build_payload(
        self,
        listing_metadata: ListingMetadata,
        compliance: Optional[ComplianceReport],
        attestation: Optional[QualityAttestation],
        price: float,
        category: str,
        model_provider: str,
    ) -> Dict[str, Any]:
        """Map local processing results to ai.market ListingCreate schema."""

        # Build schema_info from column summaries (no actual data)
        schema_info = {
            "columns": [
                {
                    "name": col.name,
                    "type": col.type,
                    "null_percentage": col.null_percentage,
                    "uniqueness_ratio": col.uniqueness_ratio,
                }
                for col in listing_metadata.column_summary
            ],
            "row_count": listing_metadata.row_count,
            "column_count": listing_metadata.column_count,
            "file_format": listing_metadata.file_format,
            "size_bytes": listing_metadata.size_bytes,
        }

        # Map privacy_score: vectoraiz uses 0.0-1.0, ai.market uses 0-10
        privacy_score_10 = round(listing_metadata.privacy_score * 10, 1)

        # Map compliance status
        compliance_status = "not_checked"
        compliance_details = None
        if compliance:
            if compliance.compliance_score >= 90:
                compliance_status = "low_risk"
            elif compliance.compliance_score >= 60:
                compliance_status = "medium_risk"
            else:
                compliance_status = "high_risk"
            compliance_details = {
                "score": compliance.compliance_score,
                "pii_entities": compliance.pii_entities_found,
                "flags": [f.model_dump() for f in compliance.flags],
            }

        # Use first data_category as primary category, rest as secondary
        primary_category = category
        secondary_categories = None
        if listing_metadata.data_categories:
            primary_category = listing_metadata.data_categories[0]
            if len(listing_metadata.data_categories) > 1:
                secondary_categories = listing_metadata.data_categories[1:]

        payload = {
            "title": listing_metadata.title[:255],
            "description": listing_metadata.description[:10000],
            "price": max(price, 25.0),
            "model_provider": model_provider,
            "category": primary_category,
            "secondary_categories": secondary_categories,
            "tags": listing_metadata.tags[:20],
            "schema_info": schema_info,
            "privacy_score": privacy_score_10,
            "compliance_status": compliance_status,
            "compliance_details": compliance_details,
            "data_format": listing_metadata.file_format or "parquet",
            "source_row_count": listing_metadata.row_count,
            "source_column_count": listing_metadata.column_count,
        }

        # Add attestation data if available
        if attestation:
            payload["schema_info"]["attestation"] = {
                "data_hash": attestation.data_hash,
                "attestation_hash": attestation.attestation_hash,
                "completeness_score": attestation.completeness_score,
                "type_consistency_score": attestation.type_consistency_score,
                "freshness_score": attestation.freshness_score,
                "quality_grade": attestation.quality_grade,
                "generated_at": attestation.generated_at,
            }

        return payload

    # ---- HTTP Push with Retry ----

    async def _push_with_retry(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST to ai.market with exponential backoff retry.
        On 409 conflict, attempts PATCH update instead.
        """
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key or "",
        }
        create_url = f"{self.base_url}/api/v1/listings/"

        last_error: Optional[Exception] = None

        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            for attempt in range(MAX_RETRIES):
                try:
                    response = await client.post(
                        create_url,
                        json=payload,
                        headers=headers,
                    )

                    if response.status_code == 201:
                        data = response.json()
                        return {
                            "status": "created",
                            "listing_id": data.get("id"),
                            "marketplace_url": f"{self.base_url}/listing/{data.get('id')}",
                            "published_at": datetime.now(timezone.utc).isoformat(),
                            "response": data,
                        }

                    if response.status_code == 409:
                        # Listing already exists — try update
                        logger.info("Listing already exists (409), attempting update...")
                        return await self._update_existing(client, headers, payload, response)

                    if response.status_code in (401, 403):
                        raise MarketplacePushError(
                            f"Authentication failed ({response.status_code}). "
                            "Check VECTORAIZ_INTERNAL_API_KEY.",
                            status_code=response.status_code,
                            detail=response.text,
                        )

                    if response.status_code >= 500:
                        # Server error — retry
                        last_error = MarketplacePushError(
                            f"Server error {response.status_code}",
                            status_code=response.status_code,
                            detail=response.text,
                        )
                        logger.warning(
                            f"Marketplace push attempt {attempt + 1}/{MAX_RETRIES} "
                            f"failed: {response.status_code}"
                        )
                    else:
                        # Client error (4xx, not 409) — don't retry
                        raise MarketplacePushError(
                            f"Marketplace rejected listing: {response.status_code}",
                            status_code=response.status_code,
                            detail=response.text,
                        )

                except httpx.RequestError as exc:
                    last_error = MarketplacePushError(
                        f"Network error: {exc}",
                        detail=str(exc),
                    )
                    logger.warning(
                        f"Marketplace push attempt {attempt + 1}/{MAX_RETRIES} "
                        f"network error: {exc}"
                    )

                # Exponential backoff
                if attempt < MAX_RETRIES - 1:
                    backoff = BACKOFF_BASE * (2 ** attempt)
                    logger.info(f"Retrying in {backoff}s...")
                    await asyncio.sleep(backoff)

        raise MarketplacePushError(
            f"Marketplace push failed after {MAX_RETRIES} attempts",
            detail=str(last_error),
        )

    async def _update_existing(
        self,
        client: httpx.AsyncClient,
        headers: Dict[str, str],
        payload: Dict[str, Any],
        conflict_response: httpx.Response,
    ) -> Dict[str, Any]:
        """
        Handle 409 conflict by extracting existing listing ID and PATCHing.
        """
        # Try to extract listing ID from the 409 response
        listing_id = None
        try:
            conflict_data = conflict_response.json()
            listing_id = conflict_data.get("existing_listing_id") or conflict_data.get("id")
        except Exception:
            pass

        if not listing_id:
            # If we can't get the ID from the 409, search for it by title
            logger.warning("No listing ID in 409 response — searching by title")
            listing_id = await self._find_listing_by_title(client, headers, payload["title"])

        if not listing_id:
            raise MarketplacePushError(
                "Listing conflict (409) but couldn't find existing listing to update",
                status_code=409,
            )

        # Build update payload (subset of fields that ListingUpdate accepts)
        update_payload = {
            k: v for k, v in payload.items()
            if k in (
                "title", "description", "price", "category",
                "secondary_categories", "tags", "model_provider",
            ) and v is not None
        }

        update_url = f"{self.base_url}/api/v1/listings/{listing_id}"
        response = await client.patch(update_url, json=update_payload, headers=headers)

        if response.status_code == 200:
            data = response.json()
            return {
                "status": "updated",
                "listing_id": listing_id,
                "marketplace_url": f"{self.base_url}/listing/{listing_id}",
                "published_at": datetime.now(timezone.utc).isoformat(),
                "response": data,
            }

        raise MarketplacePushError(
            f"Failed to update existing listing {listing_id}: {response.status_code}",
            status_code=response.status_code,
            detail=response.text,
        )

    async def _find_listing_by_title(
        self,
        client: httpx.AsyncClient,
        headers: Dict[str, str],
        title: str,
    ) -> Optional[str]:
        """Search for a listing by title to resolve 409 conflicts."""
        try:
            search_url = f"{self.base_url}/api/v1/listings/mine"
            response = await client.get(search_url, headers=headers)
            if response.status_code == 200:
                listings = response.json()
                for listing in listings:
                    if listing.get("title") == title:
                        return listing.get("id")
        except Exception as e:
            logger.warning(f"Failed to search for existing listing: {e}")
        return None

    # ---- Result Persistence ----

    def _save_publish_result(self, base_path: Path, result: Dict[str, Any]) -> None:
        """Save publish result to local filesystem."""
        result_path = base_path / "publish_result.json"
        result_path.parent.mkdir(parents=True, exist_ok=True)
        with open(result_path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        logger.info(f"Publish result saved: {result_path}")


def get_marketplace_push_service() -> MarketplacePushService:
    """Factory function for dependency injection."""
    return MarketplacePushService()
