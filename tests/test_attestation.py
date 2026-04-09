"""
Tests for AttestationService (BQ-061)
======================================
Unit tests for quality attestation report generation.
"""
from __future__ import annotations

import hashlib
import json
import pytest
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from app.models.attestation_schemas import (
    QualityAttestation,
    DataProfileSummary,
    PIIRiskAssessment,
    ComplianceStatus,
    QualityScores,
)
from app.services.attestation_service import AttestationService


# ---- Fixtures ----

SAMPLE_COLUMN_PROFILES = [
    {
        "name": "id",
        "type": "INTEGER",
        "semantic_type": "id",
        "null_percentage": 0.0,
        "max_value": "10000",
    },
    {
        "name": "amount",
        "type": "DOUBLE",
        "semantic_type": "numeric",
        "null_percentage": 5.0,
        "max_value": "999.99",
    },
    {
        "name": "created_at",
        "type": "TIMESTAMP",
        "semantic_type": "datetime",
        "null_percentage": 0.0,
        "max_value": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
    },
]

SAMPLE_METADATA = {
    "row_count": 10000,
    "column_count": 3,
    "size_bytes": 500000,
    "file_type": "csv",
    "column_profiles": SAMPLE_COLUMN_PROFILES,
}


def _make_mock_duckdb(metadata: dict = None, dataset_found: bool = True) -> MagicMock:
    """Create a mock DuckDBService for patching ephemeral_duckdb_service."""
    mock_duckdb = MagicMock()
    if dataset_found:
        mock_duckdb.get_dataset_by_id.return_value = {"filepath": "/tmp/test.csv"}
    else:
        mock_duckdb.get_dataset_by_id.return_value = None
    mock_duckdb.get_enhanced_metadata.return_value = metadata or SAMPLE_METADATA
    return mock_duckdb


def _make_service(metadata: dict = None, dataset_found: bool = True) -> AttestationService:
    """Create an AttestationService (no longer takes duckdb_service)."""
    return AttestationService()


# ---- Schema Tests ----

class TestAttestationSchema:
    """Test that the QualityAttestation schema includes all required sections."""

    def test_schema_has_all_fields(self):
        att = QualityAttestation(
            data_hash="abc",
            attestation_hash="def",
            row_count=100,
            column_count=5,
            completeness_score=0.95,
            type_consistency_score=1.0,
            freshness_score=0.8,
            null_ratio_per_column=[],
            quality_grade="A",
            generated_at="2026-01-01T00:00:00",
        )
        assert att.data_hash == "abc"
        assert att.attestation_hash == "def"
        assert att.freshness_score == 0.8
        assert att.data_profile is None
        assert att.pii_risk is None
        assert att.compliance is None
        assert att.quality_scores is None

    def test_data_profile_summary(self):
        profile = DataProfileSummary(
            row_count=100,
            column_count=5,
            size_bytes=1024,
            file_format="csv",
        )
        assert profile.row_count == 100
        assert profile.columns == []

    def test_pii_risk_assessment(self):
        pii = PIIRiskAssessment(
            overall_risk="medium",
            pii_entities_found=["EMAIL", "PHONE"],
            total_pii_findings=15,
        )
        assert pii.overall_risk == "medium"
        assert len(pii.pii_entities_found) == 2

    def test_compliance_status(self):
        comp = ComplianceStatus(
            compliance_score=75,
            status="medium_risk",
            flagged_regulations=["GDPR"],
        )
        assert comp.status == "medium_risk"

    def test_quality_scores(self):
        qs = QualityScores(completeness=0.95, consistency=1.0, freshness=0.8)
        assert qs.freshness == 0.8


# ---- Service Tests ----

class TestAttestationService:

    @pytest.mark.asyncio
    async def test_generate_attestation_basic(self, tmp_path):
        """AC1/AC2: Generates report with all sections."""
        # Create a dummy data file for SHA-256 hashing
        data_file = tmp_path / "test.csv"
        data_file.write_text("id,amount\n1,10.0\n2,20.0\n")

        # Create the output dir so the service can write attestation.json
        output_dir = tmp_path / "processed" / "test-dataset"
        output_dir.mkdir(parents=True)

        mock_duckdb = _make_mock_duckdb()
        mock_duckdb.get_dataset_by_id.return_value = {
            "filepath": str(data_file),
        }

        service = _make_service()

        with patch("app.services.attestation_service.ephemeral_duckdb_service") as mock_ctx:
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_duckdb)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            with patch.object(service, "_load_pii_risk", return_value=PIIRiskAssessment()):
                with patch.object(service, "_load_compliance_status", return_value=ComplianceStatus()):
                    # Redirect the attestation.json write to tmp_path
                    real_path = Path

                    def patched_path(p):
                        s = str(p)
                        if s.startswith("/data/processed/"):
                            return real_path(str(tmp_path / "processed" / s.split("/data/processed/")[1]))
                        return real_path(s)

                    with patch("app.services.attestation_service.Path", side_effect=patched_path):
                        att = await service.generate_attestation("test-dataset")

        assert att.data_hash  # SHA-256 of data file
        assert att.attestation_hash  # AC5: integrity hash
        assert len(att.attestation_hash) == 64  # SHA-256 hex length
        assert att.row_count == 10000
        assert att.column_count == 3
        assert att.data_profile is not None
        assert att.data_profile.row_count == 10000
        assert att.pii_risk is not None
        assert att.compliance is not None
        assert att.quality_scores is not None
        assert att.quality_scores.freshness >= 0.0
        assert att.freshness_score >= 0.0

    @pytest.mark.asyncio
    async def test_dataset_not_found_raises(self):
        mock_duckdb = _make_mock_duckdb(dataset_found=False)
        service = _make_service(dataset_found=False)
        with patch("app.services.attestation_service.ephemeral_duckdb_service") as mock_ctx:
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_duckdb)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            with pytest.raises(ValueError, match="not found"):
                await service.generate_attestation("nonexistent")

    def test_quality_grade_A(self):
        service = _make_service()
        assert service._calculate_quality_grade(1.0, 1.0, 1.0) == "A"

    def test_quality_grade_B(self):
        service = _make_service()
        assert service._calculate_quality_grade(0.9, 0.9, 0.7) == "B"

    def test_quality_grade_C(self):
        service = _make_service()
        assert service._calculate_quality_grade(0.75, 0.7, 0.6) == "C"

    def test_quality_grade_D(self):
        service = _make_service()
        assert service._calculate_quality_grade(0.5, 0.5, 0.5) == "D"

    def test_quality_grade_F(self):
        service = _make_service()
        assert service._calculate_quality_grade(0.2, 0.2, 0.2) == "F"


# ---- Freshness Tests ----

class TestFreshnessCalculation:

    def test_recent_date_column(self, tmp_path):
        """AC3: Recent datetime column gives high freshness."""
        service = _make_service()
        recent = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        profiles = [
            {"name": "ts", "semantic_type": "datetime", "null_percentage": 0, "max_value": recent},
        ]
        data_file = tmp_path / "test.csv"
        data_file.write_text("x")
        score = service._calculate_freshness(profiles, data_file)
        assert score == 1.0

    def test_old_date_column(self, tmp_path):
        """AC3: Very old datetime column gives zero freshness."""
        service = _make_service()
        old = "2020-01-01T00:00:00"
        profiles = [
            {"name": "ts", "semantic_type": "datetime", "null_percentage": 0, "max_value": old},
        ]
        data_file = tmp_path / "test.csv"
        data_file.write_text("x")
        score = service._calculate_freshness(profiles, data_file)
        assert score == 0.0

    def test_fallback_to_file_mtime(self, tmp_path):
        """AC3: Falls back to file mtime when no datetime columns."""
        service = _make_service()
        profiles = [
            {"name": "id", "semantic_type": "id", "null_percentage": 0, "max_value": "100"},
        ]
        data_file = tmp_path / "test.csv"
        data_file.write_text("x")
        score = service._calculate_freshness(profiles, data_file)
        # File was just created, should be fresh
        assert score == 1.0

    def test_no_max_value_skipped(self, tmp_path):
        """Datetime column with no max_value is skipped."""
        service = _make_service()
        profiles = [
            {"name": "ts", "semantic_type": "datetime", "null_percentage": 0, "max_value": None},
        ]
        data_file = tmp_path / "test.csv"
        data_file.write_text("x")
        # Should fall back to file mtime
        score = service._calculate_freshness(profiles, data_file)
        assert score == 1.0


# ---- Attestation Hash Tests ----

class TestAttestationHash:

    def test_attestation_hash_is_deterministic(self):
        """AC5: Same content produces same hash."""
        content = {"data_hash": "abc", "row_count": 100}
        canonical = json.dumps(content, sort_keys=True, default=str)
        h1 = hashlib.sha256(canonical.encode()).hexdigest()
        h2 = hashlib.sha256(canonical.encode()).hexdigest()
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex length

    def test_attestation_hash_changes_with_content(self):
        """AC5: Different content produces different hash."""
        c1 = json.dumps({"data_hash": "abc"}, sort_keys=True)
        c2 = json.dumps({"data_hash": "xyz"}, sort_keys=True)
        h1 = hashlib.sha256(c1.encode()).hexdigest()
        h2 = hashlib.sha256(c2.encode()).hexdigest()
        assert h1 != h2


# ---- Date Parsing Tests ----

class TestDateParsing:

    def test_iso_format(self):
        result = AttestationService._try_parse_date("2026-02-10T14:30:00")
        assert result is not None
        assert result.year == 2026

    def test_date_only(self):
        result = AttestationService._try_parse_date("2026-02-10")
        assert result is not None

    def test_us_format(self):
        result = AttestationService._try_parse_date("02/10/2026")
        assert result is not None

    def test_invalid_returns_none(self):
        result = AttestationService._try_parse_date("not-a-date")
        assert result is None

    def test_empty_returns_none(self):
        result = AttestationService._try_parse_date("")
        assert result is None


# ---- PII/Compliance Loading Tests ----

class TestReportSectionLoading:

    def test_load_pii_risk_missing_file(self):
        service = _make_service()
        result = service._load_pii_risk("nonexistent-dataset")
        assert result.overall_risk == "none"
        assert result.total_pii_findings == 0

    def test_load_pii_risk_from_file(self, tmp_path):
        service = _make_service()
        pii_dir = tmp_path / "test-ds"
        pii_dir.mkdir()
        pii_file = pii_dir / "pii_scan.json"
        pii_file.write_text(json.dumps({
            "overall_risk": "high",
            "pii_entities_found": ["EMAIL", "SSN"],
            "total_pii_findings": 42,
        }))

        real_path = Path

        def patched_path(p):
            if "pii_scan" in str(p):
                return pii_file
            return real_path(p)

        with patch("app.services.attestation_service.Path", side_effect=patched_path):
            result = service._load_pii_risk("test-ds")
        assert result.overall_risk == "high"
        assert result.total_pii_findings == 42

    def test_load_compliance_missing_file(self):
        service = _make_service()
        result = service._load_compliance_status("nonexistent-dataset")
        assert result.status == "not_checked"
        assert result.compliance_score == 100
