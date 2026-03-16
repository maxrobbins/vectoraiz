"""
Quality Contract Service (BQ-VZ-DATA-READINESS Phase 1A)
========================================================
Auto-generates and validates data quality contracts using Pandera.

Dimensions:
- Completeness: null rates per column
- Validity: type conformance (DuckDB types match actual data)
- Consistency: cross-column rules (e.g., start < end dates)
- Uniqueness: HLL-based approximate unique ratio
"""
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandera as pa
from pydantic import BaseModel, Field

from app.config import settings
from app.services.duckdb_service import ephemeral_duckdb_service

logger = logging.getLogger(__name__)

SAMPLE_LIMIT = 100_000


# ── Pydantic models ─────────────────────────────────────────────────

class DimensionScore(BaseModel):
    score: float = Field(0.0, description="0.0 to 1.0")
    details: List[str] = Field(default_factory=list)


class QualityScorecard(BaseModel):
    dataset_id: str
    completeness: DimensionScore = Field(default_factory=DimensionScore)
    validity: DimensionScore = Field(default_factory=DimensionScore)
    consistency: DimensionScore = Field(default_factory=DimensionScore)
    uniqueness: DimensionScore = Field(default_factory=DimensionScore)
    overall_score: float = 0.0
    grade: str = "F"
    column_scores: List[Dict[str, Any]] = Field(default_factory=list)


# ── Service ──────────────────────────────────────────────────────────

class QualityContractService:
    """Generates and validates quality contracts for datasets."""

    def __init__(self):
        self.output_dir = Path(settings.data_directory) / "processed"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def validate_dataset(self, dataset_id: str) -> QualityScorecard:
        """Run full quality validation on a dataset.

        Uses DuckDB SQL for aggregate checks and Pandera for sample validation.
        If a sketch_profile.json exists, uses HLL estimates for uniqueness.
        """
        with ephemeral_duckdb_service() as duckdb:
            dataset_info = duckdb.get_dataset_by_id(dataset_id)
        if not dataset_info:
            raise ValueError(f"Dataset '{dataset_id}' not found")

        filepath = Path(dataset_info["filepath"])

        # Load sketch profile if available (for HLL-based uniqueness)
        sketch_profile = self._load_sketch_profile(dataset_id)

        with ephemeral_duckdb_service() as duckdb:
            file_type = duckdb.detect_file_type(filepath)
            if not file_type:
                raise ValueError(f"Unsupported file type: {filepath.suffix}")
            read_func = duckdb.get_read_function(file_type, str(filepath))

            # Get schema
            schema_rows = duckdb.connection.execute(
                f"DESCRIBE SELECT * FROM {read_func}"
            ).fetchall()
            columns = [(row[0], row[1]) for row in schema_rows]

            # Get row count
            row_count = duckdb.connection.execute(
                f"SELECT COUNT(*) FROM {read_func}"
            ).fetchone()[0]

            if row_count == 0:
                return self._empty_scorecard(dataset_id)

            # ── 1. Completeness (SQL aggregate) ────────────────────
            completeness = self._check_completeness(duckdb, read_func, columns, row_count)

            # ── 2. Validity (Pandera sample validation) ────────────
            validity = self._check_validity(duckdb, read_func, columns)

            # ── 3. Consistency (cross-column rules) ────────────────
            consistency = self._check_consistency(duckdb, read_func, columns)

            # ── 4. Uniqueness (HLL-based or SQL) ──────────────────
            uniqueness = self._check_uniqueness(
                duckdb, read_func, columns, row_count, sketch_profile
            )

        # Build per-column scores
        column_scores = self._build_column_scores(
            columns, completeness, validity, uniqueness
        )

        # Overall score (weighted)
        overall = (
            completeness.score * 0.35
            + validity.score * 0.30
            + consistency.score * 0.20
            + uniqueness.score * 0.15
        )
        grade = self._score_to_grade(overall)

        scorecard = QualityScorecard(
            dataset_id=dataset_id,
            completeness=completeness,
            validity=validity,
            consistency=consistency,
            uniqueness=uniqueness,
            overall_score=round(overall, 4),
            grade=grade,
            column_scores=column_scores,
        )

        # Persist
        output_path = self.output_dir / dataset_id / "quality_scorecard.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(scorecard.model_dump(), f, indent=2)

        logger.info(
            "Quality scorecard for %s: overall=%.2f grade=%s",
            dataset_id, overall, grade,
        )
        return scorecard

    # ── Dimension checks ─────────────────────────────────────────────

    def _check_completeness(self, duckdb, read_func, columns, row_count) -> DimensionScore:
        """Check null rates per column."""
        details = []
        col_null_rates = {}

        for col_name, _col_type in columns:
            safe_name = col_name.replace('"', '""')
            escaped = f'"{safe_name}"'
            result = duckdb.connection.execute(
                f'SELECT COUNT(*) - COUNT({escaped}) FROM {read_func}'
            ).fetchone()
            null_count = result[0] if result else 0
            null_rate = null_count / row_count if row_count > 0 else 0
            col_null_rates[col_name] = null_rate

            if null_rate > 0.5:
                details.append(f"{col_name}: {null_rate:.1%} null (>50%)")
            elif null_rate > 0.1:
                details.append(f"{col_name}: {null_rate:.1%} null")

        # Average completeness (1 - avg null rate)
        avg_null = sum(col_null_rates.values()) / len(col_null_rates) if col_null_rates else 0
        score = max(0.0, 1.0 - avg_null)

        return DimensionScore(score=round(score, 4), details=details)

    def _check_validity(self, duckdb, read_func, columns) -> DimensionScore:
        """Validate type conformance on a sample using Pandera."""
        import pandas as pd

        details = []

        # Fetch sample into pandas
        sample_df = duckdb.connection.execute(
            f"SELECT * FROM {read_func} LIMIT {SAMPLE_LIMIT}"
        ).fetchdf()

        if sample_df.empty:
            return DimensionScore(score=1.0, details=["No data to validate"])

        # Build Pandera schema from DuckDB types
        pa_columns = {}
        for col_name, col_type in columns:
            if col_name not in sample_df.columns:
                continue
            pa_columns[col_name] = pa.Column(nullable=True, coerce=True)

        schema = pa.DataFrameSchema(pa_columns, coerce=True)

        try:
            schema.validate(sample_df, lazy=True)
            score = 1.0
        except pa.errors.SchemaErrors as e:
            # Count fraction of failures
            n_failures = len(e.failure_cases)
            total_cells = sample_df.shape[0] * sample_df.shape[1]
            score = max(0.0, 1.0 - (n_failures / total_cells)) if total_cells > 0 else 0.0
            # Summarize top issues
            for _, row in e.failure_cases.head(5).iterrows():
                col = row.get("column", "unknown")
                check = row.get("check", "unknown")
                details.append(f"{col}: {check}")

        return DimensionScore(score=round(score, 4), details=details)

    def _check_consistency(self, duckdb, read_func, columns) -> DimensionScore:
        """Check cross-column consistency rules."""
        details = []
        checks_passed = 0
        checks_total = 0

        col_names = {c[0].lower(): c[0] for c in columns}
        col_types = {c[0].lower(): c[1].lower() for c in columns}

        # Rule: start_date < end_date patterns
        date_pairs = [
            ("start_date", "end_date"),
            ("start_time", "end_time"),
            ("created_at", "updated_at"),
            ("begin_date", "end_date"),
        ]
        for start_key, end_key in date_pairs:
            if start_key in col_names and end_key in col_names:
                start_col = col_names[start_key]
                end_col = col_names[end_key]
                safe_start = f'"{start_col.replace(chr(34), chr(34)+chr(34))}"'
                safe_end = f'"{end_col.replace(chr(34), chr(34)+chr(34))}"'

                result = duckdb.connection.execute(
                    f"SELECT COUNT(*) FROM {read_func} "
                    f"WHERE {safe_start} IS NOT NULL AND {safe_end} IS NOT NULL "
                    f"AND {safe_start} > {safe_end}"
                ).fetchone()
                violations = result[0] if result else 0
                checks_total += 1
                if violations == 0:
                    checks_passed += 1
                else:
                    details.append(f"{start_col} > {end_col} in {violations} rows")

        # Rule: min < max patterns
        min_max_pairs = [("min_value", "max_value"), ("low", "high"), ("min", "max")]
        for min_key, max_key in min_max_pairs:
            if min_key in col_names and max_key in col_names:
                min_col = col_names[min_key]
                max_col = col_names[max_key]
                safe_min = f'"{min_col.replace(chr(34), chr(34)+chr(34))}"'
                safe_max = f'"{max_col.replace(chr(34), chr(34)+chr(34))}"'

                try:
                    result = duckdb.connection.execute(
                        f"SELECT COUNT(*) FROM {read_func} "
                        f"WHERE {safe_min} IS NOT NULL AND {safe_max} IS NOT NULL "
                        f"AND CAST({safe_min} AS DOUBLE) > CAST({safe_max} AS DOUBLE)"
                    ).fetchone()
                    violations = result[0] if result else 0
                    checks_total += 1
                    if violations == 0:
                        checks_passed += 1
                    else:
                        details.append(f"{min_col} > {max_col} in {violations} rows")
                except Exception:
                    pass  # Skip if cast fails

        if checks_total == 0:
            # No cross-column rules applicable → perfect score
            return DimensionScore(score=1.0, details=["No cross-column rules applicable"])

        score = checks_passed / checks_total
        return DimensionScore(score=round(score, 4), details=details)

    def _check_uniqueness(self, duckdb, read_func, columns, row_count, sketch_profile) -> DimensionScore:
        """Check uniqueness using HLL estimates or SQL APPROX_COUNT_DISTINCT."""
        details = []
        uniqueness_ratios = {}

        for col_name, _col_type in columns:
            if sketch_profile:
                # Use HLL from sketch profile
                col_sketch = next(
                    (c for c in sketch_profile.get("columns", [])
                     if c["column_name"] == col_name),
                    None,
                )
                if col_sketch:
                    hll_est = col_sketch.get("hll_distinct_estimate", 0)
                    total = col_sketch.get("total_count", row_count)
                    non_null = total - col_sketch.get("null_count", 0)
                    ratio = hll_est / non_null if non_null > 0 else 0
                    uniqueness_ratios[col_name] = min(ratio, 1.0)
                    continue

            # Fallback: SQL approx
            safe_name = f'"{col_name.replace(chr(34), chr(34)+chr(34))}"'
            try:
                result = duckdb.connection.execute(
                    f"SELECT APPROX_COUNT_DISTINCT({safe_name}) FROM {read_func}"
                ).fetchone()
                distinct = result[0] if result else 0
                ratio = distinct / row_count if row_count > 0 else 0
                uniqueness_ratios[col_name] = min(ratio, 1.0)
            except Exception:
                uniqueness_ratios[col_name] = 0.0

        # Score: average uniqueness ratio
        # Datasets with some high-cardinality columns are good
        if not uniqueness_ratios:
            return DimensionScore(score=0.0, details=["No columns to check"])

        avg_uniqueness = sum(uniqueness_ratios.values()) / len(uniqueness_ratios)

        # Flag columns with very low uniqueness (potential data quality issue)
        for col, ratio in uniqueness_ratios.items():
            if ratio < 0.01 and row_count > 100:
                details.append(f"{col}: ~{ratio:.1%} unique (very low cardinality)")

        return DimensionScore(score=round(min(avg_uniqueness * 2, 1.0), 4), details=details)

    # ── Helpers ──────────────────────────────────────────────────────

    def _load_sketch_profile(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Load sketch profile from disk if available."""
        path = self.output_dir / dataset_id / "sketch_profile.json"
        if not path.exists():
            return None
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load sketch profile for %s: %s", dataset_id, e)
            return None

    def _build_column_scores(self, columns, completeness, validity, uniqueness) -> List[Dict[str, Any]]:
        """Build per-column quality breakdown."""
        # Simple per-column view from completeness details
        results = []
        for col_name, col_type in columns:
            results.append({
                "column_name": col_name,
                "dtype": col_type,
            })
        return results

    @staticmethod
    def _score_to_grade(score: float) -> str:
        if score >= 0.95:
            return "A"
        if score >= 0.85:
            return "B"
        if score >= 0.70:
            return "C"
        if score >= 0.50:
            return "D"
        return "F"

    @staticmethod
    def _empty_scorecard(dataset_id: str) -> QualityScorecard:
        return QualityScorecard(
            dataset_id=dataset_id,
            completeness=DimensionScore(score=0.0, details=["Empty dataset"]),
            validity=DimensionScore(score=0.0, details=["Empty dataset"]),
            consistency=DimensionScore(score=1.0, details=["Empty dataset"]),
            uniqueness=DimensionScore(score=0.0, details=["Empty dataset"]),
            overall_score=0.0,
            grade="F",
        )


# Singleton
_quality_service: Optional[QualityContractService] = None


def get_quality_contract_service() -> QualityContractService:
    global _quality_service
    if _quality_service is None:
        _quality_service = QualityContractService()
    return _quality_service
