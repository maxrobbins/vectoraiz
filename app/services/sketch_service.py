"""
DataSketches Profiling Service (BQ-VZ-DATA-READINESS Phase 1A)
==============================================================
Generates per-column statistical profiles using DataSketches:
- HLL distinct count
- KLL quantiles (p25/p50/p75/p95/p99)
- Frequent items (top-20)
- Null rate

Processes in 50K-row chunks via DuckDB fetch_record_batch.
"""
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from app.config import settings
from app.services.duckdb_service import ephemeral_duckdb_service

logger = logging.getLogger(__name__)

CHUNK_SIZE = 50_000


# ── Pydantic models ─────────────────────────────────────────────────

class ColumnSketchProfile(BaseModel):
    column_name: str
    dtype: str = ""
    null_count: int = 0
    total_count: int = 0
    null_rate: float = 0.0
    hll_distinct_estimate: int = 0
    quantiles: Optional[Dict[str, float]] = None  # p25, p50, p75, p95, p99
    frequent_items: Optional[List[Dict[str, Any]]] = None  # [{value, estimate}]


class DataSketchProfile(BaseModel):
    dataset_id: str
    row_count: int = 0
    column_count: int = 0
    columns: List[ColumnSketchProfile] = Field(default_factory=list)


# ── Service ──────────────────────────────────────────────────────────

class SketchService:
    """Generates statistical profiles for datasets using DataSketches."""

    def __init__(self):
        self.output_dir = Path(settings.data_directory) / "processed"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_profile(self, dataset_id: str) -> DataSketchProfile:
        """Generate a full sketch profile for a dataset.

        Returns a DataSketchProfile with per-column HLL distinct counts,
        KLL quantiles, frequent items, and null rates.
        """
        from datasketches import hll_sketch, kll_floats_sketch, frequent_strings_sketch, frequent_items_error_type

        # Resolve filepath
        with ephemeral_duckdb_service() as duckdb:
            dataset_info = duckdb.get_dataset_by_id(dataset_id)
        if not dataset_info:
            raise ValueError(f"Dataset '{dataset_id}' not found")

        filepath = Path(dataset_info["filepath"])

        with ephemeral_duckdb_service() as duckdb:
            file_type = duckdb.detect_file_type(filepath)
            if not file_type:
                raise ValueError(f"Unsupported file type: {filepath.suffix}")
            read_func = duckdb.get_read_function(file_type, str(filepath))

            # Get schema
            schema = duckdb.connection.execute(
                f"DESCRIBE SELECT * FROM {read_func}"
            ).fetchall()
            columns = [(row[0], row[1]) for row in schema]

            # Get row count
            row_count = duckdb.connection.execute(
                f"SELECT COUNT(*) FROM {read_func}"
            ).fetchone()[0]

            # Initialize per-column sketches
            hll_sketches: Dict[str, Any] = {}
            kll_sketches: Dict[str, Any] = {}
            freq_sketches: Dict[str, Any] = {}
            null_counts: Dict[str, int] = {}
            total_counts: Dict[str, int] = {}

            for col_name, col_type in columns:
                hll_sketches[col_name] = hll_sketch(12)  # lg_k=12 → ~0.65% error
                freq_sketches[col_name] = frequent_strings_sketch(64)  # max map size
                null_counts[col_name] = 0
                total_counts[col_name] = 0
                # KLL only for numeric columns
                col_lower = col_type.lower()
                if any(t in col_lower for t in ("int", "float", "double", "decimal", "numeric", "bigint", "smallint", "tinyint", "hugeint")):
                    kll_sketches[col_name] = kll_floats_sketch()

            # Process in chunks
            offset = 0
            while offset < row_count:
                chunk_rows = duckdb.connection.execute(
                    f"SELECT * FROM {read_func} LIMIT {CHUNK_SIZE} OFFSET {offset}"
                ).fetchall()
                if not chunk_rows:
                    break

                for row in chunk_rows:
                    for i, (col_name, _col_type) in enumerate(columns):
                        val = row[i]
                        total_counts[col_name] += 1

                        if val is None:
                            null_counts[col_name] += 1
                            continue

                        # HLL: update with string repr
                        str_val = str(val)
                        hll_sketches[col_name].update(str_val)

                        # Frequent items: string values
                        freq_sketches[col_name].update(str_val)

                        # KLL: numeric values
                        if col_name in kll_sketches:
                            try:
                                kll_sketches[col_name].update(float(val))
                            except (ValueError, TypeError):
                                pass

                offset += CHUNK_SIZE

        # Build output
        column_profiles = []
        for col_name, col_type in columns:
            total = total_counts[col_name]
            nulls = null_counts[col_name]
            null_rate = nulls / total if total > 0 else 0.0

            hll_est = int(hll_sketches[col_name].get_estimate())

            # Quantiles
            quantiles = None
            if col_name in kll_sketches and kll_sketches[col_name].n > 0:
                fractions = [0.25, 0.5, 0.75, 0.95, 0.99]
                q_values = kll_sketches[col_name].get_quantiles(fractions)
                quantiles = {
                    f"p{int(f*100)}": round(float(v), 4)
                    for f, v in zip(fractions, q_values)
                }

            # Frequent items (top-20)
            freq_items = None
            fi = freq_sketches[col_name].get_frequent_items(
                frequent_items_error_type.NO_FALSE_NEGATIVES
            )
            if fi:
                freq_items = [
                    {"value": item[0], "estimate": item[1]}
                    for item in sorted(fi, key=lambda x: x[1], reverse=True)[:20]
                ]

            column_profiles.append(ColumnSketchProfile(
                column_name=col_name,
                dtype=col_type,
                null_count=nulls,
                total_count=total,
                null_rate=round(null_rate, 6),
                hll_distinct_estimate=hll_est,
                quantiles=quantiles,
                frequent_items=freq_items,
            ))

        profile = DataSketchProfile(
            dataset_id=dataset_id,
            row_count=row_count,
            column_count=len(columns),
            columns=column_profiles,
        )

        # Persist
        output_path = self.output_dir / dataset_id / "sketch_profile.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(profile.model_dump(), f, indent=2)

        logger.info("Sketch profile generated for %s: %d columns", dataset_id, len(columns))
        return profile


# Singleton
_sketch_service: Optional[SketchService] = None


def get_sketch_service() -> SketchService:
    global _sketch_service
    if _sketch_service is None:
        _sketch_service = SketchService()
    return _sketch_service
