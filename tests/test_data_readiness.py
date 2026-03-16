"""
Tests for BQ-VZ-DATA-READINESS Phase 1A:
- Sketch service (DataSketches profiling)
- Quality contract service (Pandera validation)
- PII structured scanning + settings
- Readiness endpoint
"""
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from app.config import settings


# ── Helpers ──────────────────────────────────────────────────────────

def _make_csv(tmp_path: Path, name: str = "test.csv", rows: int = 200) -> Path:
    """Create a test CSV with known data patterns."""
    filepath = tmp_path / name
    lines = ["id,name,email,age,score,created_at\n"]
    for i in range(rows):
        lines.append(
            f"{i},Person {i},person{i}@example.com,{20 + (i % 50)},"
            f"{round(50 + (i % 50) * 0.8, 1)},2026-01-{1 + (i % 28):02d}\n"
        )
    filepath.write_text("".join(lines))
    return filepath


def _mock_duckdb_for_file(filepath: Path):
    """Create a real ephemeral DuckDB context that reads a real file."""
    from app.services.duckdb_service import DuckDBService

    class _FakeCtx:
        def __init__(self):
            self.svc = DuckDBService()

        def __enter__(self):
            return self.svc

        def __exit__(self, *args):
            self.svc.close()

    return _FakeCtx()


# ── Sketch Service ───────────────────────────────────────────────────


class TestSketchService:
    """Tests for DataSketches profiling."""

    @pytest.fixture
    def csv_path(self, tmp_path):
        return _make_csv(tmp_path, rows=500)

    @pytest.fixture
    def sketch_service(self, tmp_path, csv_path):
        from app.services.sketch_service import SketchService

        svc = SketchService()
        svc.output_dir = tmp_path / "processed"
        svc.output_dir.mkdir()
        return svc

    def test_generate_profile_basic(self, sketch_service, csv_path, tmp_path):
        """Sketch profile generates per-column stats for a CSV."""
        dataset_id = csv_path.stem

        # Patch get_dataset_by_id to return our CSV
        def _patched_ctx():
            return _mock_duckdb_for_file(csv_path)

        with patch("app.services.sketch_service.ephemeral_duckdb_service", _patched_ctx):
            # Also patch data_directory so get_dataset_by_id finds the file
            with patch.object(settings, "data_directory", str(tmp_path)):
                profile = sketch_service.generate_profile(dataset_id)

        assert profile.dataset_id == dataset_id
        assert profile.row_count == 500
        assert profile.column_count == 6
        assert len(profile.columns) == 6

        # Check HLL accuracy: id column should have ~500 distinct values
        id_col = next(c for c in profile.columns if c.column_name == "id")
        assert id_col.hll_distinct_estimate > 0
        # HLL should be within 5% of exact for 500+ rows
        error_pct = abs(id_col.hll_distinct_estimate - 500) / 500
        assert error_pct < 0.05, f"HLL error {error_pct:.1%} exceeds 5% threshold"

        # Null rate should be 0 for all columns
        for col in profile.columns:
            assert col.null_rate == 0.0

    def test_quantiles_for_numeric_columns(self, sketch_service, csv_path, tmp_path):
        """KLL quantiles should be generated for numeric columns."""
        dataset_id = csv_path.stem

        def _patched_ctx():
            return _mock_duckdb_for_file(csv_path)

        with patch("app.services.sketch_service.ephemeral_duckdb_service", _patched_ctx):
            with patch.object(settings, "data_directory", str(tmp_path)):
                profile = sketch_service.generate_profile(dataset_id)

        # age and score should have quantiles; name/email should not
        age_col = next(c for c in profile.columns if c.column_name == "age")
        assert age_col.quantiles is not None
        assert "p50" in age_col.quantiles

        name_col = next(c for c in profile.columns if c.column_name == "name")
        assert name_col.quantiles is None

    def test_frequent_items(self, sketch_service, csv_path, tmp_path):
        """Frequent items should be populated."""
        dataset_id = csv_path.stem

        def _patched_ctx():
            return _mock_duckdb_for_file(csv_path)

        with patch("app.services.sketch_service.ephemeral_duckdb_service", _patched_ctx):
            with patch.object(settings, "data_directory", str(tmp_path)):
                profile = sketch_service.generate_profile(dataset_id)

        # age has limited range (20-69), so frequent items should be populated
        age_col = next(c for c in profile.columns if c.column_name == "age")
        assert age_col.frequent_items is not None
        assert len(age_col.frequent_items) > 0

    def test_profile_persisted_to_disk(self, sketch_service, csv_path, tmp_path):
        """Profile should be saved as JSON."""
        dataset_id = csv_path.stem

        def _patched_ctx():
            return _mock_duckdb_for_file(csv_path)

        with patch("app.services.sketch_service.ephemeral_duckdb_service", _patched_ctx):
            with patch.object(settings, "data_directory", str(tmp_path)):
                sketch_service.generate_profile(dataset_id)

        output_path = sketch_service.output_dir / dataset_id / "sketch_profile.json"
        assert output_path.exists()
        data = json.loads(output_path.read_text())
        assert data["dataset_id"] == dataset_id


# ── Quality Contract Service ─────────────────────────────────────────


class TestQualityContractService:
    """Tests for Pandera quality contract validation."""

    @pytest.fixture
    def csv_path(self, tmp_path):
        return _make_csv(tmp_path, rows=200)

    @pytest.fixture
    def quality_service(self, tmp_path):
        from app.services.quality_contract_service import QualityContractService

        svc = QualityContractService()
        svc.output_dir = tmp_path / "processed"
        svc.output_dir.mkdir()
        return svc

    def test_validate_dataset_basic(self, quality_service, csv_path, tmp_path):
        """Quality scorecard should have all four dimensions."""
        dataset_id = csv_path.stem

        def _patched_ctx():
            return _mock_duckdb_for_file(csv_path)

        with patch("app.services.quality_contract_service.ephemeral_duckdb_service", _patched_ctx):
            with patch.object(settings, "data_directory", str(tmp_path)):
                scorecard = quality_service.validate_dataset(dataset_id)

        assert scorecard.dataset_id == dataset_id
        assert 0.0 <= scorecard.completeness.score <= 1.0
        assert 0.0 <= scorecard.validity.score <= 1.0
        assert 0.0 <= scorecard.consistency.score <= 1.0
        assert 0.0 <= scorecard.uniqueness.score <= 1.0
        assert 0.0 <= scorecard.overall_score <= 1.0
        assert scorecard.grade in ("A", "B", "C", "D", "F")

    def test_completeness_perfect_for_no_nulls(self, quality_service, csv_path, tmp_path):
        """Dataset with no nulls should have completeness ~1.0."""
        dataset_id = csv_path.stem

        def _patched_ctx():
            return _mock_duckdb_for_file(csv_path)

        with patch("app.services.quality_contract_service.ephemeral_duckdb_service", _patched_ctx):
            with patch.object(settings, "data_directory", str(tmp_path)):
                scorecard = quality_service.validate_dataset(dataset_id)

        assert scorecard.completeness.score >= 0.99

    def test_scorecard_persisted_to_disk(self, quality_service, csv_path, tmp_path):
        """Scorecard should be saved as JSON."""
        dataset_id = csv_path.stem

        def _patched_ctx():
            return _mock_duckdb_for_file(csv_path)

        with patch("app.services.quality_contract_service.ephemeral_duckdb_service", _patched_ctx):
            with patch.object(settings, "data_directory", str(tmp_path)):
                quality_service.validate_dataset(dataset_id)

        output_path = quality_service.output_dir / dataset_id / "quality_scorecard.json"
        assert output_path.exists()

    def test_consistency_no_cross_column_rules(self, quality_service, csv_path, tmp_path):
        """Without date pairs, consistency should be 1.0."""
        dataset_id = csv_path.stem

        def _patched_ctx():
            return _mock_duckdb_for_file(csv_path)

        with patch("app.services.quality_contract_service.ephemeral_duckdb_service", _patched_ctx):
            with patch.object(settings, "data_directory", str(tmp_path)):
                scorecard = quality_service.validate_dataset(dataset_id)

        assert scorecard.consistency.score == 1.0


# ── PII Structured Scanning + Settings ───────────────────────────────


class TestPIIStructuredAndSettings:
    """Tests for Presidio structured scanning and global PII settings."""

    def test_pii_settings_roundtrip(self, tmp_path):
        """Save and load PII settings."""
        from app.services.pii_service import PIIService

        svc = PIIService()

        with patch.object(settings, "data_directory", str(tmp_path)):
            svc._config_dir = tmp_path / "pii_configs"
            svc._config_dir.mkdir()

            # Default
            s = svc.get_pii_settings()
            assert s["score_threshold"] == 0.5

            # Save
            svc.save_pii_settings({
                "score_threshold": 0.8,
                "excluded_patterns": ["SENSOR-", "DEV-"],
                "entity_overrides": {},
            })

            # Load
            s = svc.get_pii_settings()
            assert s["score_threshold"] == 0.8
            assert "SENSOR-" in s["excluded_patterns"]

    def test_pii_settings_invalid_threshold(self, tmp_path):
        """Invalid threshold should raise ValueError."""
        from app.services.pii_service import PIIService

        svc = PIIService()

        with patch.object(settings, "data_directory", str(tmp_path)):
            with pytest.raises(ValueError, match="score_threshold"):
                svc.save_pii_settings({"score_threshold": 1.5})


# ── Pipeline Steps ───────────────────────────────────────────────────


class TestPipelineSteps:
    """Verify pipeline includes new steps."""

    def test_extended_pipeline_steps_order(self):
        """EXTENDED_PIPELINE_STEPS should include sketch_profile and quality_check."""
        from app.services.pipeline_service import EXTENDED_PIPELINE_STEPS

        assert "sketch_profile" in EXTENDED_PIPELINE_STEPS
        assert "quality_check" in EXTENDED_PIPELINE_STEPS
        # Order: duckdb_analysis → sketch_profile → pii_scan → quality_check → ...
        idx_sketch = EXTENDED_PIPELINE_STEPS.index("sketch_profile")
        idx_duckdb = EXTENDED_PIPELINE_STEPS.index("duckdb_analysis")
        idx_pii = EXTENDED_PIPELINE_STEPS.index("pii_scan")
        idx_quality = EXTENDED_PIPELINE_STEPS.index("quality_check")
        assert idx_duckdb < idx_sketch < idx_pii < idx_quality

    def test_pipeline_service_has_new_services(self):
        """PipelineService should have sketch and quality services."""
        from app.services.pipeline_service import PipelineService

        with patch("app.services.pipeline_service.get_pii_service"):
            with patch("app.services.pipeline_service.get_compliance_service"):
                with patch("app.services.pipeline_service.get_attestation_service"):
                    with patch("app.services.pipeline_service.get_listing_metadata_service"):
                        with patch("app.services.pipeline_service.get_sketch_service") as mock_sketch:
                            with patch("app.services.pipeline_service.get_quality_contract_service") as mock_quality:
                                svc = PipelineService()
                                assert svc.sketch_service is not None
                                assert svc.quality_contract_service is not None
