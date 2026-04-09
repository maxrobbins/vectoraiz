"""
PII Detection API endpoints.
"""

import logging

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field
from typing import Dict

logger = logging.getLogger(__name__)

from app.services.pii_service import (
    get_pii_service,
    PIIService,
    DEFAULT_SAMPLE_SIZE,
)
from app.auth.api_key_auth import get_current_user, AuthenticatedUser
from app.services.processing_service import get_processing_service, ProcessingService, ProcessingStatus
from app.services.serial_metering import metered, MeterDecision

router = APIRouter()


# ── Schemas ─────────────────────────────────────────────────────────────

class PIIColumnConfig(BaseModel):
    """Per-column PII action decisions.

    Each key is a column name; each value is one of: exclude, redact, keep.
    Example: {"email": "redact", "ssn": "exclude", "city": "keep"}
    """

    column_actions: Dict[str, str] = Field(
        ...,
        description="Mapping of column name to action (exclude | redact | keep)",
        json_schema_extra={
            "example": {"email": "redact", "ssn": "exclude", "city": "keep"}
        },
    )


# ── Endpoints ───────────────────────────────────────────────────────────


@router.get("/entities")
async def list_entities():
    """
    List all PII entity types that can be detected.
    """
    from app.services.pii_service import DEFAULT_ENTITIES
    
    entity_descriptions = {
        "PERSON": "Names of individuals",
        "EMAIL_ADDRESS": "Email addresses",
        "PHONE_NUMBER": "Phone numbers (various formats)",
        "US_SSN": "US Social Security Numbers",
        "CREDIT_CARD": "Credit card numbers",
        "US_PASSPORT": "US Passport numbers",
        "US_DRIVER_LICENSE": "US Driver's license numbers",
        "IP_ADDRESS": "IP addresses (v4 and v6)",
        "IBAN_CODE": "International Bank Account Numbers",
        "US_BANK_NUMBER": "US bank account numbers",
        "LOCATION": "Physical addresses and locations",
        "DATE_TIME": "Dates and times that may identify individuals",
        "NRP": "Nationality, religion, political group",
        "MEDICAL_LICENSE": "Medical license numbers",
        "URL": "URLs that may contain identifying information",
    }
    
    return {
        "entities": [
            {"type": e, "description": entity_descriptions.get(e, "No description")}
            for e in DEFAULT_ENTITIES
        ],
        "count": len(DEFAULT_ENTITIES),
    }


@router.post("/scan/{dataset_id}")
async def scan_dataset(
    dataset_id: str,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    processing: ProcessingService = Depends(get_processing_service),
    pii_service: PIIService = Depends(get_pii_service),
    user: AuthenticatedUser = Depends(get_current_user),
    _meter: MeterDecision = Depends(metered("setup")),
):
    """
    Scan a dataset for PII.
    
    Samples rows from the dataset and analyzes each column for
    personally identifiable information.
    Requires X-API-Key header.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    
    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}"
        )
    
    if not record.processed_path or not record.processed_path.exists():
        raise HTTPException(status_code=500, detail="Processed file not found")
    
    try:
        # Use structured scan for tabular data (column-aware with type heuristics)
        result = pii_service.scan_structured(
            filepath=record.processed_path,
            sample_size=sample_size,
        )

        # Add recommendations
        result["recommendations"] = pii_service.get_recommendations(result)

        return {
            "dataset_id": dataset_id,
            "filename": record.original_filename,
            **result,
        }
    except (OSError, ConnectionError):
        logger.exception("PII scan failed for dataset %s", dataset_id)
        raise HTTPException(
            status_code=503,
            detail="PII scanning service unavailable. Ensure required NLP models are installed (python -m spacy download en_core_web_sm).",
        )
    except ImportError:
        logger.exception("PII scanning dependency missing")
        raise HTTPException(
            status_code=503,
            detail="PII scanning dependency not available. Check that all required packages are installed.",
        )
    except Exception:
        logger.exception("PII scan failed for dataset %s", dataset_id)
        raise HTTPException(status_code=500, detail="PII scan failed due to an internal error")


@router.get("/scan/{dataset_id}")
async def get_pii_scan_result(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service),
):
    """
    Get cached PII scan results for a dataset.
    
    Returns the PII scan that was performed during processing.
    Use POST /scan/{dataset_id} to trigger a new scan.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    
    pii_scan = record.metadata.get("pii_scan")
    if not pii_scan:
        raise HTTPException(
            status_code=404,
            detail="No PII scan results found. Use POST to trigger a scan."
        )
    
    return {
        "dataset_id": dataset_id,
        "filename": record.original_filename,
        **pii_scan,
    }


@router.post("/config/{dataset_id}")
async def save_pii_config(
    dataset_id: str,
    body: PIIColumnConfig,
    processing: ProcessingService = Depends(get_processing_service),
    pii_service: PIIService = Depends(get_pii_service),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """
    Save per-column PII action decisions for a dataset.

    Persists a JSON object mapping column names to actions so decisions
    survive page refresh.  Valid actions:

    - **exclude** — drop the column entirely before publishing
    - **redact** — mask/anonymize PII values in the column
    - **keep** — leave the column as-is (user accepts the risk)

    Requires X-API-Key header.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    try:
        config = pii_service.save_pii_config(
            dataset_id=dataset_id,
            column_actions=body.column_actions,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    return {
        "dataset_id": dataset_id,
        **config,
    }


@router.get("/config/{dataset_id}")
async def get_pii_config(
    dataset_id: str,
    processing: ProcessingService = Depends(get_processing_service),
    pii_service: PIIService = Depends(get_pii_service),
):
    """
    Retrieve saved per-column PII action decisions for a dataset.

    Returns the previously saved column_actions mapping, or an empty
    mapping if no decisions have been saved yet.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    config = pii_service.get_pii_config(dataset_id)
    if config is None:
        return {
            "dataset_id": dataset_id,
            "column_actions": {},
            "updated_at": None,
        }

    return {
        "dataset_id": dataset_id,
        **config,
    }


# ── BQ-VZ-DATA-READINESS: PII Settings ────────────────────────────────


class PIISettingsBody(BaseModel):
    """Global PII scanning settings."""
    score_threshold: float = Field(0.5, ge=0.0, le=1.0, description="Minimum confidence threshold (0.0-1.0)")
    entity_overrides: Dict = Field(default_factory=dict, description="Per-column entity overrides")
    excluded_patterns: list = Field(default_factory=list, description="Patterns to exclude from PII detection (e.g. sensor IDs)")


@router.get("/settings")
async def get_pii_settings(
    pii_service: PIIService = Depends(get_pii_service),
):
    """Get global PII scanning settings (thresholds, overrides, exclusions)."""
    return pii_service.get_pii_settings()


@router.put("/settings")
async def update_pii_settings(
    body: PIISettingsBody,
    pii_service: PIIService = Depends(get_pii_service),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Update global PII scanning settings."""
    try:
        return pii_service.save_pii_settings(body.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.post("/analyze-text")
async def analyze_text(
    text: str,
    pii_service: PIIService = Depends(get_pii_service),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """
    Analyze a single text string for PII.
    
    Useful for testing or analyzing specific values.
    Requires X-API-Key header.
    """
    if not text:
        raise HTTPException(status_code=400, detail="Text cannot be empty")
    
    if len(text) > 10000:
        raise HTTPException(status_code=400, detail="Text too long (max 10000 characters)")
    
    results = pii_service.scan_text(text)
    
    findings = []
    for result in results:
        findings.append({
            "entity_type": result.entity_type,
            "start": result.start,
            "end": result.end,
            "score": round(result.score, 3),
            "matched_text": text[result.start:result.end],
        })
    
    return {
        "text_length": len(text),
        "pii_found": len(findings) > 0,
        "findings": findings,
    }


@router.post("/scrub/{dataset_id}")
async def scrub_dataset(
    dataset_id: str,
    strategy: str = Query("mask", regex="^(mask|redact|hash)$"),
    processing: ProcessingService = Depends(get_processing_service),
    pii_service: PIIService = Depends(get_pii_service),
    user: AuthenticatedUser = Depends(get_current_user),
    _meter: MeterDecision = Depends(metered("setup")),
):
    """
    Scrub PII from a dataset using the specified strategy.
    
    Creates a new copy of the dataset with PII masked, redacted, or hashed.
    Strategies:
    - mask: Replace PII with ***
    - redact: Remove PII entirely
    - hash: Replace PII with SHA256 hash
    
    Requires X-API-Key header.
    """
    record = processing.get_dataset(dataset_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
    
    if record.status != ProcessingStatus.READY:
        raise HTTPException(
            status_code=400,
            detail=f"Dataset not ready. Current status: {record.status.value}"
        )
    
    if not record.processed_path or not record.processed_path.exists():
        raise HTTPException(status_code=500, detail="Processed file not found")
    
    try:
        result = pii_service.scrub_dataset(
            filepath=record.processed_path,
            strategy=strategy,
        )
        
        return {
            "dataset_id": dataset_id,
            "filename": record.original_filename,
            **result,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"PII scrub failed: {str(e)}")
