"""
Faceted Metadata Filter Service (BQ-VZ-HYBRID-SEARCH Phase 1A)
==============================================================
Pre-computes facet counts on dataset ingest for fast filtering.
Stores aggregated JSON at /data/facets.json, rebuilt on dataset add/remove.
"""

import json
import logging
import threading
from pathlib import Path
from typing import Optional, Dict, Any, List

from app.config import settings

logger = logging.getLogger(__name__)

_facets_lock = threading.Lock()


def _facets_path() -> Path:
    return Path(settings.data_directory) / "facets.json"


def _bucket_row_count(count: Optional[int]) -> str:
    """Bucket row counts for faceting."""
    if count is None:
        return "unknown"
    if count < 100:
        return "<100"
    if count < 1000:
        return "100-1K"
    if count < 10000:
        return "1K-10K"
    if count < 100000:
        return "10K-100K"
    return "100K+"


def _bucket_quality_score(score: Optional[float]) -> str:
    """Bucket quality scores for faceting."""
    if score is None:
        return "unknown"
    if score >= 0.9:
        return "excellent"
    if score >= 0.7:
        return "good"
    if score >= 0.5:
        return "fair"
    return "poor"


def rebuild_facets() -> Dict[str, Any]:
    """
    Rebuild facet counts from all datasets.

    Scans /data/processed/*/pipeline_status.json and sketch_profile.json
    to compute facet counts. Async-safe, <5s for 100 datasets.
    """
    from app.services.processing_service import get_processing_service

    processing = get_processing_service()
    records = processing.list_datasets()

    facets: Dict[str, Dict[str, int]] = {
        "file_type": {},
        "column_count": {},
        "row_count_bucket": {},
        "has_pii": {"true": 0, "false": 0},
        "quality_score_bucket": {},
    }

    for record in records:
        # file_type facet
        ft = record.file_type or "unknown"
        facets["file_type"][ft] = facets["file_type"].get(ft, 0) + 1

        # Parse metadata_json for column_count and row_count
        meta = {}
        if record.metadata_json:
            try:
                meta = json.loads(record.metadata_json) if isinstance(record.metadata_json, str) else record.metadata_json
            except (json.JSONDecodeError, TypeError):
                pass

        # column_count facet
        col_count = meta.get("column_count")
        if col_count is not None:
            bucket = str(col_count) if col_count <= 20 else "20+"
            facets["column_count"][bucket] = facets["column_count"].get(bucket, 0) + 1

        # row_count_bucket facet
        row_count = meta.get("row_count")
        bucket = _bucket_row_count(row_count)
        facets["row_count_bucket"][bucket] = facets["row_count_bucket"].get(bucket, 0) + 1

        # has_pii facet - check pii_scan.json
        dataset_dir = Path(settings.processed_directory) / record.id
        pii_path = dataset_dir / "pii_scan.json"
        has_pii = False
        if pii_path.exists():
            try:
                with open(pii_path) as f:
                    pii_data = json.load(f)
                has_pii = pii_data.get("total_pii_findings", 0) > 0
            except Exception:
                pass
        facets["has_pii"]["true" if has_pii else "false"] += 1

        # quality_score_bucket facet - check quality_scorecard.json
        quality_path = dataset_dir / "quality_scorecard.json"
        quality_score = None
        if quality_path.exists():
            try:
                with open(quality_path) as f:
                    quality_data = json.load(f)
                quality_score = quality_data.get("overall_score")
            except Exception:
                pass
        bucket = _bucket_quality_score(quality_score)
        facets["quality_score_bucket"][bucket] = facets["quality_score_bucket"].get(bucket, 0) + 1

    # For high-cardinality facets, keep top 50
    for key in facets:
        if len(facets[key]) > 50:
            sorted_items = sorted(facets[key].items(), key=lambda x: x[1], reverse=True)[:50]
            facets[key] = dict(sorted_items)

    # Persist
    facets_file = _facets_path()
    facets_file.parent.mkdir(parents=True, exist_ok=True)
    with _facets_lock:
        with open(facets_file, "w") as f:
            json.dump(facets, f, indent=2)

    logger.info("Facets rebuilt: %d datasets", len(records))
    return facets


def rebuild_facets_async() -> None:
    """Rebuild facets in a background thread."""
    thread = threading.Thread(target=rebuild_facets, daemon=True, name="facet-rebuild")
    thread.start()


def get_facets() -> Dict[str, Any]:
    """Get cached facet counts. Returns empty if not yet computed."""
    facets_file = _facets_path()
    if not facets_file.exists():
        return {}

    try:
        with _facets_lock:
            with open(facets_file) as f:
                return json.load(f)
    except Exception:
        return {}
