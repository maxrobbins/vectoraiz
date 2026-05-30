"""
BQ-VZ-SHARED-SEARCH: Portal Service — search execution with column restrictions.

Uses existing SearchService for DuckDB/Qdrant search, then filters results
to only include display_columns configured per-dataset.
"""

import json
import logging
from typing import List, Optional

from app.models.portal import get_portal_config
from app.schemas.portal import (
    DatasetPortalConfig,
    PortalDatasetInfo,
    PortalSearchResult,
)
from app.services.search_service import get_search_service
from app.services.processing_service import get_processing_service

logger = logging.getLogger(__name__)


class PortalService:
    """Portal search — wraps SearchService with ACL and column restrictions."""

    def get_visible_datasets(self) -> List[PortalDatasetInfo]:
        """Return datasets that are portal_visible=True."""
        config = get_portal_config()
        processing_svc = get_processing_service()
        result = []

        for dataset_id, ds_config in config.datasets.items():
            if not ds_config.portal_visible:
                continue

            record = processing_svc.get_dataset(dataset_id)
            if not record:
                continue

            # Row count is stored in metadata_json
            row_count = 0
            try:
                meta = json.loads(record.metadata_json) if record.metadata_json else {}
                row_count = meta.get("row_count", 0) or 0
            except (json.JSONDecodeError, TypeError):
                pass

            result.append(PortalDatasetInfo(
                dataset_id=dataset_id,
                name=record.original_filename or dataset_id,
                description=None,
                row_count=row_count,
                searchable_columns=ds_config.search_columns,
            ))

        return result

    def is_dataset_visible(self, dataset_id: str) -> bool:
        """Check if a dataset is portal-visible (M1 ACL)."""
        config = get_portal_config()
        ds_config = config.datasets.get(dataset_id)
        return ds_config is not None and ds_config.portal_visible

    def get_dataset_portal_config(self, dataset_id: str) -> Optional[DatasetPortalConfig]:
        """Get portal config for a specific dataset."""
        config = get_portal_config()
        return config.datasets.get(dataset_id)

    def search_dataset(
        self,
        dataset_id: str,
        query: str,
        limit: int = 20,
        offset: int = 0,
    ) -> PortalSearchResult:
        """Search a dataset and return only display_columns."""
        ds_config = self.get_dataset_portal_config(dataset_id)
        if not ds_config or not ds_config.portal_visible:
            raise ValueError(f"Dataset '{dataset_id}' is not available on portal")

        # Cap limit to dataset max_results
        effective_limit = min(limit, ds_config.max_results)

        search_svc = get_search_service()
        processing_svc = get_processing_service()

        raw_results = search_svc.search(
            query=query,
            dataset_id=dataset_id,
            limit=effective_limit,
        )

        # Filter to display_columns only
        filtered_results = []
        display_cols = set(ds_config.display_columns) if ds_config.display_columns else None

        for r in raw_results.get("results", []):
            row_data = r.get("row_data", {})
            if display_cols:
                row_data = {k: v for k, v in row_data.items() if k in display_cols}

            filtered_results.append({
                "score": r.get("score"),
                "row_data": row_data,
                "text_content": r.get("text_content", ""),
            })

        # Get dataset name
        record = processing_svc.get_dataset(dataset_id)
        dataset_name = record.original_filename if record else dataset_id

        return PortalSearchResult(
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            total_count=raw_results.get("total", 0),
            results=filtered_results,
            query=query,
        )

    def search_all_visible(
        self,
        query: str,
        limit: int = 20,
    ) -> List[PortalSearchResult]:
        """Search across all portal-visible datasets."""
        datasets = self.get_visible_datasets()
        results = []
        for ds in datasets:
            try:
                result = self.search_dataset(ds.dataset_id, query, limit)
                if result.results:
                    results.append(result)
            except Exception as e:
                logger.warning("Portal search failed for dataset %s: %s", ds.dataset_id, e)
                continue
        return results


# Singleton
_portal_service: Optional[PortalService] = None


def get_portal_service() -> PortalService:
    global _portal_service
    if _portal_service is None:
        _portal_service = PortalService()
    return _portal_service
