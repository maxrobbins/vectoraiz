"""
datasets.json → DatasetRecord Migration Script
================================================

Idempotent migration: reads datasets.json, inserts records into
the dataset_records SQL table, and renames the file to .bak.

Checks a migration marker row (id='__migrated__') to avoid
re-running. On failure the transaction rolls back and the
JSON file is left untouched so the next startup retries.

Phase: BQ-111 — Persistent State
Created: 2026-02-12

Usage:
    # Automatic — called from app startup
    # Manual — python -m app.scripts.migrate_json
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from sqlmodel import Session as SQLModelSession

from app.config import settings

logger = logging.getLogger(__name__)

MIGRATION_MARKER_ID = "__migrated__"


def migrate_datasets_json(engine) -> bool:
    """
    Migrate datasets.json into the dataset_records table.

    Returns True if migration ran (or was already done), False on error.
    """
    from app.models.dataset import DatasetRecord

    json_path = Path(settings.data_directory) / "datasets.json"

    if not json_path.exists():
        logger.info("No datasets.json found — nothing to migrate")
        return True

    # Check migration marker
    with SQLModelSession(engine) as session:
        marker = session.get(DatasetRecord, MIGRATION_MARKER_ID)
        if marker is not None:
            logger.info("datasets.json migration already completed (marker present)")
            return True

    # Read JSON
    try:
        with open(json_path, "r") as f:
            records = json.load(f)
    except Exception as exc:
        logger.error("Failed to read datasets.json: %s", exc)
        return False

    if not isinstance(records, list):
        logger.error("datasets.json is not a JSON array")
        return False

    logger.info("Migrating %d dataset records from datasets.json …", len(records))

    try:
        with SQLModelSession(engine) as session:
            for rec in records:
                dataset_id = rec.get("id", "")
                if not dataset_id or dataset_id == MIGRATION_MARKER_ID:
                    continue

                # Skip if already exists (idempotent)
                existing = session.get(DatasetRecord, dataset_id)
                if existing is not None:
                    continue

                original_filename = rec.get("original_filename", "unknown")
                file_type = rec.get("file_type", "unknown")
                upload_path = rec.get("upload_path", "")
                storage_filename = Path(upload_path).name if upload_path else f"{dataset_id}.{file_type}"

                # Compute file size if file still exists
                file_size = 0
                if upload_path:
                    try:
                        file_size = os.path.getsize(upload_path)
                    except OSError:
                        pass

                # Parse timestamps
                created_at = _parse_ts(rec.get("created_at"))
                updated_at = _parse_ts(rec.get("updated_at"))

                # Build metadata JSON (everything except top-level fields)
                metadata = rec.get("metadata", {})
                if rec.get("document_content"):
                    metadata["document_content"] = rec["document_content"]

                db_rec = DatasetRecord(
                    id=dataset_id,
                    original_filename=original_filename,
                    storage_filename=storage_filename,
                    file_type=file_type,
                    file_size_bytes=file_size,
                    status=rec.get("status", "ready"),
                    processed_path=rec.get("processed_path"),
                    metadata_json=json.dumps(metadata, default=str),
                    created_at=created_at,
                    updated_at=updated_at,
                )
                session.add(db_rec)

            # Insert migration marker
            marker = DatasetRecord(
                id=MIGRATION_MARKER_ID,
                original_filename="__migration_marker__",
                storage_filename="__migration_marker__",
                file_type="marker",
                file_size_bytes=0,
                status="deleted",
                metadata_json="{}",
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            session.add(marker)
            session.commit()

        # Rename original file
        bak_path = json_path.with_suffix(".json.bak")
        json_path.rename(bak_path)
        logger.info("datasets.json migrated successfully → %s", bak_path)
        return True

    except Exception as exc:
        logger.error("datasets.json migration failed (transaction rolled back): %s", exc)
        return False


def _parse_ts(val) -> datetime:
    """Parse an ISO-format timestamp string, or return utcnow()."""
    if not val:
        return datetime.now(timezone.utc)
    try:
        dt = datetime.fromisoformat(str(val))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from app.core.database import get_engine
    migrate_datasets_json(get_engine())
