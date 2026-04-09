"""
Compliance Service for vectorAIz
=================================
BQ-086: GDPR/CCPA/HIPAA compliance flag generation from PII scan results.
Runs locally, produces a compliance report per dataset.
"""
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Set, Any

from app.models.compliance_schemas import ComplianceReport, RegulationFlag


# PII entity → applicable regulations
REGULATION_MAP: Dict[str, List[str]] = {
    "EMAIL":          ["GDPR", "CCPA"],
    "PHONE":          ["GDPR", "CCPA"],
    "NAME":           ["GDPR", "CCPA"],
    "ADDRESS":        ["GDPR", "CCPA"],
    "DOB":            ["GDPR", "CCPA", "HIPAA"],
    "IP_ADDRESS":     ["GDPR", "CCPA"],
    "SSN":            ["CCPA", "HIPAA"],
    "CREDIT_CARD":    ["CCPA"],
    "MEDICAL_RECORD": ["HIPAA"],
    "INSURANCE_ID":   ["HIPAA"],
}

# PII entity → risk level
RISK_LEVELS: Dict[str, str] = {
    "SSN":            "high",
    "CREDIT_CARD":    "high",
    "MEDICAL_RECORD": "high",
    "INSURANCE_ID":   "high",
    "EMAIL":          "medium",
    "PHONE":          "medium",
    "NAME":           "medium",
    "DOB":            "medium",
    "ADDRESS":        "medium",
    "IP_ADDRESS":     "low",
}

# Risk level → weight for score calculation
RISK_WEIGHTS: Dict[str, int] = {
    "high": 30,
    "medium": 15,
    "low": 5,
}

# Recommended actions per regulation
RECOMMENDED_ACTIONS: Dict[str, List[str]] = {
    "GDPR": [
        "Ensure lawful basis for processing (Art. 6)",
        "Implement data subject access request (DSAR) process",
        "Apply pseudonymization or encryption to personal data",
        "Document processing activities (Art. 30)",
    ],
    "CCPA": [
        "Provide opt-out mechanism for data sale",
        "Disclose categories of personal information collected",
        "Enable consumer deletion requests",
        "Update privacy policy with CCPA-required disclosures",
    ],
    "HIPAA": [
        "Apply HIPAA Safe Harbor de-identification (18 identifiers)",
        "Implement access controls and audit logging",
        "Encrypt PHI at rest and in transit",
        "Execute Business Associate Agreements if sharing data",
    ],
}


class ComplianceService:
    """
    Analyzes PII scan results and maps them to regulation-specific compliance flags.
    """

    async def generate_compliance_report(self, dataset_id: str) -> ComplianceReport:
        """
        Generate a compliance report for a dataset based on its PII scan.

        If no PII scan exists, returns an all-clear report.
        """
        pii_results = self._load_pii_scan(dataset_id)
        pii_entities = self._extract_pii_entities(pii_results)

        # Build regulation flags
        flags = self._build_regulation_flags(pii_entities)

        # Compute compliance score
        compliance_score = self._compute_score(flags)

        report = ComplianceReport(
            dataset_id=dataset_id,
            flags=flags,
            compliance_score=compliance_score,
            pii_entities_found=sorted(pii_entities),
            generated_at=datetime.now(timezone.utc).isoformat(),
        )

        # Persist
        output_path = Path(f"/data/processed/{dataset_id}/compliance_report.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(report.model_dump(), f, indent=2)

        return report

    def _load_pii_scan(self, dataset_id: str) -> Dict[str, Any]:
        """Load PII scan results from disk. Returns empty dict if not found."""
        pii_path = Path(f"/data/processed/{dataset_id}/pii_scan.json")
        if not pii_path.exists():
            return {}
        try:
            with open(pii_path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}

    def _extract_pii_entities(self, pii_results: Dict[str, Any]) -> Set[str]:
        """
        Extract distinct PII entity types from scan results.

        Handles multiple possible PII result formats:
        - {"columns": {"col_name": {"pii_types": ["EMAIL", ...]}}}
        - {"entities": [{"type": "EMAIL", "column": "..."}]}
        - {"results": [{"entity_type": "EMAIL", ...}]}
        """
        entities: Set[str] = set()

        # Format 1: columns dict with pii_types
        columns = pii_results.get("columns", {})
        if isinstance(columns, dict):
            for col_data in columns.values():
                if isinstance(col_data, dict):
                    for pii_type in col_data.get("pii_types", []):
                        entities.add(pii_type.upper())

        # Format 2: entities list
        for entity in pii_results.get("entities", []):
            if isinstance(entity, dict):
                etype = entity.get("type", entity.get("entity_type", ""))
                if etype:
                    entities.add(etype.upper())

        # Format 3: results list
        for result in pii_results.get("results", []):
            if isinstance(result, dict):
                etype = result.get("entity_type", result.get("type", ""))
                if etype:
                    entities.add(etype.upper())

        return entities

    def _get_flagged_columns(self, pii_results: Dict[str, Any], entity_type: str) -> List[str]:
        """Get column names that contain a specific PII entity type."""
        columns = []
        cols_data = pii_results.get("columns", {})
        if isinstance(cols_data, dict):
            for col_name, col_data in cols_data.items():
                if isinstance(col_data, dict):
                    types = [t.upper() for t in col_data.get("pii_types", [])]
                    if entity_type.upper() in types:
                        columns.append(col_name)
        return columns

    def _build_regulation_flags(self, pii_entities: Set[str]) -> List[RegulationFlag]:
        """Build regulation flags from detected PII entities."""
        regulations = {"GDPR", "CCPA", "HIPAA"}
        flags = []

        for reg in sorted(regulations):
            # Find all PII entities that trigger this regulation
            triggering_entities = []
            max_risk = "low"

            for entity in pii_entities:
                applicable_regs = REGULATION_MAP.get(entity, [])
                if reg in applicable_regs:
                    triggering_entities.append(entity)
                    entity_risk = RISK_LEVELS.get(entity, "low")
                    if RISK_WEIGHTS.get(entity_risk, 0) > RISK_WEIGHTS.get(max_risk, 0):
                        max_risk = entity_risk

            applicable = len(triggering_entities) > 0

            flags.append(
                RegulationFlag(
                    regulation_name=reg,
                    applicable=applicable,
                    risk_level=max_risk if applicable else "low",
                    flagged_columns=sorted(triggering_entities),  # PII types that triggered
                    recommended_actions=RECOMMENDED_ACTIONS.get(reg, []) if applicable else [],
                )
            )

        return flags

    def _compute_score(self, flags: List[RegulationFlag]) -> int:
        """
        Compute compliance score 0-100.
        100 = no regulations triggered (clean).
        Deducts points per applicable regulation weighted by risk level.
        """
        score = 100
        for flag in flags:
            if flag.applicable:
                deduction = RISK_WEIGHTS.get(flag.risk_level, 5)
                # Scale by number of flagged entities
                entity_count = len(flag.flagged_columns)
                deduction = min(deduction * entity_count, 40)  # Cap per regulation
                score -= deduction
        return max(0, score)


def get_compliance_service() -> ComplianceService:
    """Factory function for ComplianceService."""
    return ComplianceService()
