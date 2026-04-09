"""
Pydantic Schemas for Compliance Reports
========================================
BQ-086: Map PII scan results to GDPR/CCPA/HIPAA regulation flags.
"""
from pydantic import BaseModel, Field
from typing import List


class RegulationFlag(BaseModel):
    regulation_name: str = Field(..., description="GDPR, CCPA, or HIPAA")
    applicable: bool = Field(..., description="Whether this regulation is triggered")
    risk_level: str = Field("low", description="low, medium, or high")
    flagged_columns: List[str] = Field(default_factory=list)
    recommended_actions: List[str] = Field(default_factory=list)


class ComplianceReport(BaseModel):
    dataset_id: str
    flags: List[RegulationFlag] = Field(default_factory=list)
    compliance_score: int = Field(100, description="0-100, 100 = fully compliant / no PII")
    pii_entities_found: List[str] = Field(default_factory=list, description="Distinct PII types detected")
    generated_at: str
