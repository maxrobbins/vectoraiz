"""
Full-Text Search Service (BQ-VZ-HYBRID-SEARCH Phase 1A)
=======================================================
DuckDB FTS for structured data BM25 search.
Creates persistent FTS indexes per dataset in background.
"""

import logging
import threading
from pathlib import Path
from typing import Optional, List, Dict, Any

import duckdb

from app.config import settings

logger = logging.getLogger(__name__)


# Track FTS index status per dataset
_fts_status: Dict[str, str] = {}  # dataset_id -> "building" | "ready" | "unavailable"
_fts_lock = threading.Lock()


def _get_fts_db_path(dataset_id: str) -> Path:
    """Get the path to the FTS DuckDB database for a dataset."""
    return Path(settings.processed_directory) / dataset_id / "fts.duckdb"


def get_fts_status(dataset_id: str) -> str:
    """Get the FTS index status for a dataset."""
    with _fts_lock:
        return _fts_status.get(dataset_id, "unavailable")


def build_fts_index(dataset_id: str, parquet_path: Path) -> None:
    """
    Build FTS index for a dataset in a background thread.

    Creates a persistent DuckDB database with FTS extension and indexes
    all text-like columns for BM25 search.
    """
    with _fts_lock:
        _fts_status[dataset_id] = "building"

    def _build():
        try:
            fts_db_path = _get_fts_db_path(dataset_id)
            fts_db_path.parent.mkdir(parents=True, exist_ok=True)

            # Remove existing FTS db if present
            if fts_db_path.exists():
                fts_db_path.unlink()

            con = duckdb.connect(str(fts_db_path))
            try:
                con.execute("INSTALL fts")
                con.execute("LOAD fts")

                # Import data from parquet
                con.execute(
                    f"CREATE TABLE data AS SELECT * FROM read_parquet('{parquet_path}')"
                )

                # Detect text columns (VARCHAR type)
                cols = con.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_name = 'data'"
                ).fetchall()

                text_cols = [
                    c[0] for c in cols
                    if "VARCHAR" in c[1].upper() or "TEXT" in c[1].upper()
                ]

                if not text_cols:
                    logger.info("No text columns found for FTS in dataset %s", dataset_id)
                    with _fts_lock:
                        _fts_status[dataset_id] = "unavailable"
                    return

                # Create FTS index on all text columns
                col_list = ", ".join(f"'{c}'" for c in text_cols)
                con.execute(
                    f"PRAGMA create_fts_index('data', 'rowid', {col_list})"
                )

                with _fts_lock:
                    _fts_status[dataset_id] = "ready"
                logger.info(
                    "FTS index ready for dataset %s (%d text columns)",
                    dataset_id, len(text_cols),
                )
            finally:
                con.close()

        except Exception as e:
            logger.error("FTS index build failed for %s: %s", dataset_id, e, exc_info=True)
            with _fts_lock:
                _fts_status[dataset_id] = "unavailable"

    thread = threading.Thread(target=_build, daemon=True, name=f"fts-{dataset_id}")
    thread.start()


def search_fts(
    query: str,
    dataset_id: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """
    Search a dataset using DuckDB FTS (BM25).

    Returns list of dicts with 'rowid', 'score', and row data.
    Returns empty list if FTS index is not ready.
    """
    if get_fts_status(dataset_id) != "ready":
        return []

    fts_db_path = _get_fts_db_path(dataset_id)
    if not fts_db_path.exists():
        return []

    try:
        con = duckdb.connect(str(fts_db_path), read_only=True)
        try:
            con.execute("LOAD fts")

            # Use the FTS match_bm25 function
            safe_query = query.replace("'", "''")
            result = con.execute(
                f"""
                SELECT *, fts_main_data.match_bm25(rowid, '{safe_query}') AS score
                FROM data
                WHERE score IS NOT NULL
                ORDER BY score DESC
                LIMIT {int(limit)}
                """
            ).fetchall()

            # Get column names
            col_names = [desc[0] for desc in con.description]

            rows = []
            for row in result:
                row_dict = dict(zip(col_names, row))
                rows.append(row_dict)

            return rows
        finally:
            con.close()

    except Exception as e:
        logger.warning("FTS search failed for dataset %s: %s", dataset_id, e)
        return []
