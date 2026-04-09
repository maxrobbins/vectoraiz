"""
Attestation Service for vectorAIz (BQ-061)
============================================
Generates data quality attestation reports for datasets.
"""
from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from app.services.duckdb_service import ephemeral_duckdb_service
from app.models.attestation_schemas import (
    QualityAttestation,
    ColumnMetrics,
    DataProfileSummary,
    PIIRiskAssessment,
    ComplianceStatus,
    QualityScores,
)

logger = logging.getLogger(__name__)


class AttestationService:
    """Service to generate quality attestations from dataset profiles."""

    def __init__(self):
        pass

    async def generate_attestation(self, dataset_id: str) -> QualityAttestation:
        """
        Generates a quality attestation report for a given dataset.

        Includes: data profile summary, PII risk assessment, compliance status,
        quality scores (completeness, consistency, freshness), and attestation hash.
        """
        # Try to get filepath from processing record first (works for all file types)
        from app.services.processing_service import get_processing_service
        processing = get_processing_service()
        record = processing.get_dataset(dataset_id)

        filepath: Optional[Path] = None
        if record and record.processed_path and record.processed_path.exists():
            filepath = record.processed_path
        else:
            # Fallback: scan data directory
            with ephemeral_duckdb_service() as duckdb:
                dataset_info = duckdb.get_dataset_by_id(dataset_id)
            if dataset_info:
                filepath = Path(dataset_info["filepath"])

        if not filepath or not filepath.exists():
            raise ValueError(f"Dataset with id '{dataset_id}' not found or not yet processed.")

        # 1. Calculate data_hash (SHA-256 of the data file)
        data_hash = self._calculate_sha256(filepath)

        # 2. Get metrics from duckdb_service — gracefully degrade for non-tabular files
        try:
            with ephemeral_duckdb_service() as duckdb:
                metadata = duckdb.get_enhanced_metadata(filepath)
            column_profiles = metadata.get("column_profiles", [])
            row_count = metadata.get("row_count", 0)
            column_count = metadata.get("column_count", 0)
        except Exception as e:
            logger.warning("DuckDB metadata unavailable for %s: %s", dataset_id, e)
            metadata = {}
            column_profiles = []
            row_count = 0
            column_count = 0

        # 3. Calculate scores
        if not column_profiles:
            completeness_score = 0.0
            type_consistency_score = 0.0
            freshness_score = 0.0
            null_ratios: List[ColumnMetrics] = []
        else:
            total_null_percentage = sum(p["null_percentage"] for p in column_profiles)
            completeness_score = 1.0 - ((total_null_percentage / 100) / len(column_profiles))
            type_consistency_score = 1.0
            null_ratios = [
                ColumnMetrics(
                    column_name=p["name"],
                    null_ratio=p["null_percentage"] / 100.0,
                )
                for p in column_profiles
            ]
            # AC3: Freshness — score based on date column recency
            freshness_score = self._calculate_freshness(column_profiles, filepath)

        # 4. Quality grade
        quality_grade = self._calculate_quality_grade(
            completeness_score, type_consistency_score, freshness_score,
        )

        # 5. AC2: Build supplementary report sections
        data_profile = DataProfileSummary(
            row_count=row_count,
            column_count=column_count,
            size_bytes=metadata.get("size_bytes", 0),
            file_format=metadata.get("file_type", ""),
            columns=null_ratios,
        )

        pii_risk = self._load_pii_risk(dataset_id)
        compliance = self._load_compliance_status(dataset_id)

        quality_scores = QualityScores(
            completeness=round(completeness_score, 4),
            consistency=round(type_consistency_score, 4),
            freshness=round(freshness_score, 4),
        )

        # Build attestation WITHOUT the attestation_hash first
        attestation = QualityAttestation(
            data_hash=data_hash,
            attestation_hash="",  # placeholder
            row_count=row_count,
            column_count=column_count,
            completeness_score=round(completeness_score, 4),
            type_consistency_score=round(type_consistency_score, 4),
            freshness_score=round(freshness_score, 4),
            null_ratio_per_column=null_ratios,
            quality_grade=quality_grade,
            generated_at=datetime.now(timezone.utc).isoformat(),
            data_profile=data_profile,
            pii_risk=pii_risk,
            compliance=compliance,
            quality_scores=quality_scores,
        )

        # AC5: SHA-256 of the report content for integrity verification
        report_dict = attestation.model_dump() if hasattr(attestation, "model_dump") else attestation.dict()
        report_dict.pop("attestation_hash", None)
        canonical = json.dumps(report_dict, sort_keys=True, default=str)
        attestation.attestation_hash = hashlib.sha256(canonical.encode()).hexdigest()

        # Persist to disk
        attestation_path = Path(f"/data/processed/{dataset_id}/attestation.json")
        attestation_path.parent.mkdir(parents=True, exist_ok=True)
        final_dict = attestation.model_dump() if hasattr(attestation, "model_dump") else attestation.dict()
        with open(attestation_path, "w") as f:
            json.dump(final_dict, f, indent=4)

        return attestation

    # ---- Helpers ----

    def _calculate_sha256(self, filepath: Path) -> str:
        """Calculates the SHA-256 hash of a file."""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _calculate_quality_grade(
        self,
        completeness: float,
        consistency: float,
        freshness: float,
    ) -> str:
        """Computes a quality grade from a weighted score."""
        score = (completeness * 0.5) + (consistency * 0.3) + (freshness * 0.2)

        if score >= 0.95:
            return "A"
        elif score >= 0.85:
            return "B"
        elif score >= 0.70:
            return "C"
        elif score >= 0.50:
            return "D"
        else:
            return "F"

    def _calculate_freshness(
        self,
        column_profiles: List[Dict[str, Any]],
        filepath: Path,
    ) -> float:
        """
        Calculate freshness score (0.0-1.0) based on date column recency.

        Looks for datetime columns, parses their max value, and scores
        based on how recent it is. Falls back to file modification time
        if no date columns exist.
        """
        now = datetime.now(timezone.utc)

        # Find datetime columns and their max values
        date_columns = [
            p for p in column_profiles if p.get("semantic_type") == "datetime"
        ]

        most_recent: Optional[datetime] = None

        for col in date_columns:
            max_val = col.get("max_value")
            if not max_val:
                continue
            parsed = self._try_parse_date(str(max_val))
            if parsed and (most_recent is None or parsed > most_recent):
                most_recent = parsed

        # Fallback to file modification time
        if most_recent is None:
            try:
                mtime = filepath.stat().st_mtime
                most_recent = datetime.fromtimestamp(mtime, tz=timezone.utc)
            except OSError:
                return 0.0

        if most_recent is None:
            return 0.0

        # Ensure timezone-aware comparison
        if most_recent.tzinfo is None:
            most_recent = most_recent.replace(tzinfo=timezone.utc)

        age_days = (now - most_recent).days

        # Scoring: 1.0 for <7 days, decays linearly to 0.0 at 365 days
        if age_days <= 7:
            return 1.0
        if age_days >= 365:
            return 0.0
        return round(1.0 - (age_days - 7) / (365 - 7), 4)

    @staticmethod
    def _try_parse_date(value: str) -> Optional[datetime]:
        """Try common date formats."""
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d/%m/%Y",
        ):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    def _load_pii_risk(self, dataset_id: str) -> PIIRiskAssessment:
        """Load PII scan results if available."""
        pii_path = Path(f"/data/processed/{dataset_id}/pii_scan.json")
        if not pii_path.exists():
            return PIIRiskAssessment()
        try:
            with open(pii_path) as f:
                data = json.load(f)
            return PIIRiskAssessment(
                overall_risk=data.get("overall_risk", "none"),
                pii_entities_found=data.get("pii_entities_found", []),
                total_pii_findings=data.get("total_pii_findings", 0),
            )
        except Exception as e:
            logger.warning("Failed to load PII scan for attestation: %s", e)
            return PIIRiskAssessment()

    def _load_compliance_status(self, dataset_id: str) -> ComplianceStatus:
        """Load compliance report if available."""
        report_path = Path(f"/data/processed/{dataset_id}/compliance_report.json")
        if not report_path.exists():
            return ComplianceStatus()
        try:
            with open(report_path) as f:
                data = json.load(f)
            score = data.get("compliance_score", 100)
            if score >= 90:
                status = "low_risk"
            elif score >= 60:
                status = "medium_risk"
            else:
                status = "high_risk"
            flagged = [
                f.get("regulation_name", "")
                for f in data.get("flags", [])
                if f.get("applicable")
            ]
            return ComplianceStatus(
                compliance_score=score,
                status=status,
                flagged_regulations=flagged,
            )
        except Exception as e:
            logger.warning("Failed to load compliance report for attestation: %s", e)
            return ComplianceStatus()


def get_attestation_service() -> AttestationService:
    """Factory function to get an instance of the AttestationService."""
    return AttestationService()
