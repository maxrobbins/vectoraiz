"""
Listing Metadata Service for vectorAIz
=======================================
BQ-085: Transform DuckDB enhanced_metadata + PII scan results into
structured listing metadata that ai.market can ingest.

Generates human-readable descriptions, extracts tags from column profiles,
computes freshness, and builds the ListingMetadata schema.
"""
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional

from app.services.duckdb_service import ephemeral_duckdb_service

logger = logging.getLogger(__name__)
from app.models.listing_metadata_schemas import ListingMetadata, ColumnSummary


# Semantic type -> category mappings
TYPE_CATEGORIES = {
    "TIMESTAMP": ["time-series"],
    "DATE": ["time-series"],
    "DOUBLE": ["numerical"],
    "FLOAT": ["numerical"],
    "INTEGER": ["numerical"],
    "BIGINT": ["numerical"],
    "BOOLEAN": ["categorical"],
    "VARCHAR": ["text"],
}

# Column name pattern -> tag mappings
NAME_TAG_PATTERNS = {
    "price": ["pricing", "financial"],
    "cost": ["pricing", "financial"],
    "revenue": ["financial"],
    "amount": ["financial"],
    "trade": ["financial", "trading"],
    "order": ["transactional"],
    "transaction": ["transactional"],
    "email": ["contact", "pii-risk"],
    "phone": ["contact", "pii-risk"],
    "address": ["location", "pii-risk"],
    "name": ["identity", "pii-risk"],
    "lat": ["geospatial"],
    "lon": ["geospatial"],
    "longitude": ["geospatial"],
    "latitude": ["geospatial"],
    "timestamp": ["time-series"],
    "date": ["time-series"],
    "time": ["time-series"],
    "created": ["time-series"],
    "updated": ["time-series"],
    "id": ["identifier"],
    "uuid": ["identifier"],
    "url": ["web"],
    "domain": ["web"],
    "ip": ["network"],
    "country": ["geographic"],
    "city": ["geographic"],
    "region": ["geographic"],
    "category": ["categorical"],
    "type": ["categorical"],
    "status": ["categorical"],
    "score": ["metrics"],
    "rating": ["metrics"],
    "count": ["metrics"],
    "quantity": ["metrics"],
    "description": ["text"],
    "comment": ["text"],
    "review": ["text", "sentiment"],
    "sentiment": ["sentiment"],
    "image": ["media"],
    "video": ["media"],
    "product": ["e-commerce"],
    "sku": ["e-commerce"],
    "customer": ["crm"],
    "user": ["user-data"],
}


class ListingMetadataService:
    """
    Generates marketplace-ready listing metadata from vectorAIz processing results.
    """

    def __init__(self):
        pass

    async def generate_listing_metadata(self, dataset_id: str) -> ListingMetadata:
        """
        Generate structured listing metadata for a processed dataset.

        Pulls enhanced metadata from DuckDB, infers tags and categories,
        generates a human-readable description, and persists the result.
        """
        # Try to get filepath from processing record first (works for all file types)
        from app.services.processing_service import get_processing_service
        processing = get_processing_service()
        record = processing.get_dataset(dataset_id)

        filepath: Optional[Path] = None
        if record and record.processed_path and record.processed_path.exists():
            filepath = record.processed_path
        else:
            with ephemeral_duckdb_service() as duckdb:
                dataset_info = duckdb.get_dataset_by_id(dataset_id)
            if dataset_info:
                filepath = Path(dataset_info["filepath"])

        if not filepath or not filepath.exists():
            raise ValueError(f"Dataset with id '{dataset_id}' not found or not yet processed.")

        # Get metrics from DuckDB — gracefully degrade for non-tabular files
        try:
            with ephemeral_duckdb_service() as duckdb:
                metadata = duckdb.get_enhanced_metadata(filepath)
        except Exception as e:
            logger.warning("DuckDB metadata unavailable for listing: %s", e)
            metadata = {
                "file_type": filepath.suffix.lstrip("."),
                "size_bytes": filepath.stat().st_size if filepath.exists() else 0,
                "row_count": 0,
                "column_count": 0,
                "column_profiles": [],
            }

        column_profiles = metadata.get("column_profiles", [])
        row_count = metadata.get("row_count", 0)
        column_count = metadata.get("column_count", 0)
        file_type = metadata.get("file_type", filepath.suffix.lstrip("."))
        size_bytes = metadata.get("size_bytes", 0)

        # Build column summaries
        column_summaries = self._build_column_summaries(column_profiles)

        # Extract tags from column names and types
        tags = self._extract_tags(column_profiles)

        # Infer data categories
        categories = self._infer_categories(column_profiles, tags)

        # Generate human-readable description
        description = self._generate_description(
            column_profiles, row_count, column_count, file_type, categories
        )

        # Generate title from filename + key characteristics
        title = self._generate_title(dataset_id, filepath, categories, row_count)

        # Compute freshness score (based on file mod time)
        freshness_score = self._compute_freshness(filepath)

        # Compute privacy score (check if PII scan exists)
        privacy_score = self._compute_privacy_score(dataset_id)

        listing = ListingMetadata(
            title=title,
            description=description,
            tags=sorted(set(tags)),
            column_summary=column_summaries,
            row_count=row_count,
            column_count=column_count,
            file_format=file_type,
            size_bytes=size_bytes,
            freshness_score=round(freshness_score, 4),
            privacy_score=round(privacy_score, 4),
            data_categories=sorted(set(categories)),
            generated_at=datetime.now(timezone.utc).isoformat(),
        )

        # Persist to JSON
        output_path = Path(f"/data/processed/{dataset_id}/listing_metadata.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(listing.model_dump(), f, indent=2)

        return listing

    def _build_column_summaries(self, profiles: list) -> List[ColumnSummary]:
        """Build ColumnSummary objects from DuckDB column profiles."""
        summaries = []
        for p in profiles:
            summaries.append(
                ColumnSummary(
                    name=p.get("name", "unknown"),
                    type=p.get("type", "unknown"),
                    null_percentage=p.get("null_percentage", 0.0),
                    uniqueness_ratio=p.get("uniqueness_ratio", 0.0),
                    sample_values=[
                        str(v) for v in p.get("sample_values", [])[:5]
                    ],
                )
            )
        return summaries

    def _extract_tags(self, profiles: list) -> List[str]:
        """Extract tags from column names and types using pattern matching."""
        tags = []
        for p in profiles:
            col_name = p.get("name", "").lower()
            col_type = p.get("type", "").upper()

            # Match column name patterns
            for pattern, pattern_tags in NAME_TAG_PATTERNS.items():
                if pattern in col_name:
                    tags.extend(pattern_tags)

            # Match type-based categories
            for type_key, type_tags in TYPE_CATEGORIES.items():
                if type_key in col_type:
                    tags.extend(type_tags)

        return list(set(tags))

    def _infer_categories(self, profiles: list, tags: list) -> List[str]:
        """Infer high-level data categories from tags and column patterns."""
        categories = []

        tag_set = set(tags)
        if tag_set & {"financial", "trading", "pricing"}:
            categories.append("financial")
        if tag_set & {"geospatial", "geographic", "location"}:
            categories.append("geographic")
        if tag_set & {"time-series"}:
            categories.append("time-series")
        if tag_set & {"e-commerce", "transactional"}:
            categories.append("commerce")
        if tag_set & {"user-data", "crm", "contact"}:
            categories.append("people")
        if tag_set & {"text", "sentiment"}:
            categories.append("text-analytics")
        if tag_set & {"web", "network"}:
            categories.append("digital")
        if tag_set & {"media"}:
            categories.append("media")
        if tag_set & {"metrics"}:
            categories.append("analytics")

        # If no specific categories, label as general tabular
        if not categories:
            categories.append("tabular")

        return categories

    def _generate_description(
        self,
        profiles: list,
        row_count: int,
        column_count: int,
        file_type: str,
        categories: list,
    ) -> str:
        """Generate a human-readable description from column profiles."""
        # Group columns by type
        type_groups: Dict[str, List[str]] = {}
        for p in profiles:
            col_type = p.get("type", "unknown")
            base_type = col_type.split("(")[0].upper()
            type_groups.setdefault(base_type, []).append(p.get("name", "unknown"))

        # Build type summary
        type_parts = []
        for t, cols in sorted(type_groups.items()):
            if len(cols) <= 3:
                type_parts.append(f"{', '.join(cols)} ({t.lower()})")
            else:
                type_parts.append(f"{len(cols)} {t.lower()} columns")

        type_summary = ", ".join(type_parts) if type_parts else "mixed columns"

        # Category label
        cat_label = " and ".join(categories) if categories else "general"

        # Null quality note
        high_null_cols = [
            p["name"] for p in profiles if p.get("null_percentage", 0) > 20
        ]
        quality_note = ""
        if high_null_cols:
            quality_note = (
                f" Note: {', '.join(high_null_cols[:3])} "
                f"{'have' if len(high_null_cols) > 1 else 'has'} >20% null values."
            )

        size_label = f"{row_count:,}" if row_count else "unknown number of"

        return (
            f"{cat_label.capitalize()} dataset with {size_label} rows across "
            f"{column_count} columns. Includes {type_summary}.{quality_note}"
        )

    def _generate_title(
        self,
        dataset_id: str,
        filepath: Path,
        categories: list,
        row_count: int,
    ) -> str:
        """Generate a concise listing title."""
        stem = filepath.stem.replace("_", " ").replace("-", " ").title()

        if row_count >= 1_000_000:
            magnitude = f"{row_count / 1_000_000:.1f}M rows"
        elif row_count >= 1_000:
            magnitude = f"{row_count / 1_000:.1f}K rows"
        else:
            magnitude = f"{row_count} rows"

        return f"{stem} ({magnitude})"

    def _compute_freshness(self, filepath: Path) -> float:
        """
        Compute freshness score 0.0-1.0.
        1.0 = modified today, decays over 365 days.
        """
        try:
            mtime = datetime.fromtimestamp(
                filepath.stat().st_mtime, tz=timezone.utc
            )
            age_days = (datetime.now(timezone.utc) - mtime).days
            return max(0.0, 1.0 - (age_days / 365.0))
        except (OSError, ValueError):
            return 0.5

    def _compute_privacy_score(self, dataset_id: str) -> float:
        """
        Read PII scan results if they exist. 1.0 = no PII, 0.0 = high PII.
        Falls back to 1.0 (assume clean) if no scan exists.
        """
        pii_path = Path(f"/data/processed/{dataset_id}/pii_scan.json")
        if not pii_path.exists():
            return 1.0

        try:
            with open(pii_path) as f:
                pii_data = json.load(f)
            return float(pii_data.get("privacy_score", 1.0))
        except (json.JSONDecodeError, ValueError, KeyError):
            return 1.0


def get_listing_metadata_service() -> ListingMetadataService:
    """Factory function to get ListingMetadataService instance."""
    return ListingMetadataService()
