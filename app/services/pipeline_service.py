"""
Pipeline Orchestration Service for vectorAIz
=============================================
BQ-088: Orchestrates the full data processing pipeline.
BQ-117: Pipeline robustness — validate steps, atomic status, canonical status field.

- DuckDB analyze/process
- PII scan
- Compliance report
- Attestation
- Listing metadata generation

Features progress tracking, graceful degradation, and per-step status.
"""
import fcntl
import json
import logging
import os
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from app.config import settings
from app.services.duckdb_service import ephemeral_duckdb_service
from app.services.pii_service import get_pii_service
from app.services.compliance_service import get_compliance_service
from app.services.attestation_service import get_attestation_service
from app.services.listing_metadata_service import get_listing_metadata_service
from app.services.sketch_service import get_sketch_service
from app.services.quality_contract_service import get_quality_contract_service
from app.utils.sanitization import sql_quote_literal

logger = logging.getLogger(__name__)

# Valid per-step statuses
STEP_PENDING = "pending"
STEP_RUNNING = "running"
STEP_SUCCESS = "success"
STEP_FAILED = "failed"
STEP_SKIPPED = "skipped"

# Pipeline overall statuses
PIPELINE_RUNNING = "running"
PIPELINE_SUCCESS = "success"
PIPELINE_PARTIAL = "partial"
PIPELINE_FAILED = "failed"

# Steps in the full pipeline
FULL_PIPELINE_STEPS = ["analyze_process", "pii_scan", "compliance_check"]
EXTENDED_PIPELINE_STEPS = ["duckdb_analysis", "sketch_profile", "pii_scan", "quality_check", "compliance_report", "attestation", "listing_metadata"]


def _atomic_write_json(path: Path, data: dict) -> None:
    """Write JSON to *path* atomically using temp-file + os.replace, with flock."""
    dir_path = path.parent
    dir_path.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(dir=str(dir_path), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as tmp_f:
            fcntl.flock(tmp_f, fcntl.LOCK_EX)
            json.dump(data, tmp_f, indent=2)
            tmp_f.flush()
            os.fsync(tmp_f.fileno())
            fcntl.flock(tmp_f, fcntl.LOCK_UN)
        os.replace(tmp_path, str(path))
    except BaseException:
        # Clean up temp file on any failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _read_json_locked(path: Path) -> dict:
    """Read JSON from *path* with a shared flock to avoid tearing."""
    with open(path, "r") as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        data = json.load(f)
        fcntl.flock(f, fcntl.LOCK_UN)
    return data


def _read_modify_write_json(path: Path, modifier) -> dict:
    """Atomically read-modify-write JSON using an exclusive lock file.

    *modifier* receives the current dict and mutates it in place.
    Returns the modified dict.
    """
    lock_path = path.with_suffix(".lock")
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(lock_path, "w") as lock_f:
        fcntl.flock(lock_f, fcntl.LOCK_EX)
        try:
            if path.exists():
                with open(path, "r") as f:
                    data = json.load(f)
            else:
                data = {}
            modifier(data)
            _atomic_write_json(path, data)
            return data
        finally:
            fcntl.flock(lock_f, fcntl.LOCK_UN)


class PipelineService:
    """Orchestrates the multi-step data processing pipeline."""

    def __init__(self):
        self.pii_service = get_pii_service()
        self.compliance_service = get_compliance_service()
        self.attestation_service = get_attestation_service()
        self.listing_metadata_service = get_listing_metadata_service()
        self.sketch_service = get_sketch_service()
        self.quality_contract_service = get_quality_contract_service()
        self.processing_dir = Path(settings.data_directory) / "processed"
        self.processing_dir.mkdir(parents=True, exist_ok=True)

    def _get_dataset_dir(self, dataset_id: str) -> Path:
        """Returns the processing directory for a given dataset."""
        return self.processing_dir / dataset_id

    def _status_file(self, dataset_id: str) -> Path:
        return self._get_dataset_dir(dataset_id) / "pipeline_status.json"

    # ------------------------------------------------------------------
    # Atomic status helpers
    # ------------------------------------------------------------------

    def _init_pipeline_state(self, dataset_id: str, steps: List[str]) -> Dict[str, Any]:
        """Initialize pipeline state with all steps set to pending."""
        dataset_dir = self._get_dataset_dir(dataset_id)
        dataset_dir.mkdir(parents=True, exist_ok=True)

        now = datetime.now(timezone.utc).isoformat()
        state = {
            "dataset_id": dataset_id,
            "status": PIPELINE_RUNNING,
            "message": "Pipeline initialized.",
            "started_at": now,
            "updated_at": now,
            "steps": {
                step: {
                    "status": STEP_PENDING,
                    "started_at": None,
                    "finished_at": None,
                    "error": None,
                }
                for step in steps
            },
        }

        _atomic_write_json(self._status_file(dataset_id), state)
        logger.info("Pipeline state initialized for %s with steps: %s", dataset_id, steps)
        return state

    def _set_step_status(
        self,
        dataset_id: str,
        step_name: str,
        status: str,
        error_message: Optional[str] = None,
    ):
        """Update a single step's status in the pipeline state file (atomic RMW)."""
        status_file = self._status_file(dataset_id)

        if not status_file.exists():
            self._init_pipeline_state(dataset_id, FULL_PIPELINE_STEPS)

        now = datetime.now(timezone.utc).isoformat()

        def _modify(state: dict) -> None:
            if step_name not in state.get("steps", {}):
                state.setdefault("steps", {})[step_name] = {
                    "status": STEP_PENDING,
                    "started_at": None,
                    "finished_at": None,
                    "error": None,
                }

            step = state["steps"][step_name]
            step["status"] = status

            if status == STEP_RUNNING:
                step["started_at"] = now
            elif status in (STEP_SUCCESS, STEP_FAILED, STEP_SKIPPED):
                step["finished_at"] = now

            if error_message:
                step["error"] = error_message

            state["updated_at"] = now

        _read_modify_write_json(status_file, _modify)
        logger.info("Pipeline step '%s' for %s: %s", step_name, dataset_id, status)

    def _update_status(self, dataset_id: str, status: str, message: str, data: Dict[str, Any] = None):
        """Updates the pipeline status file (overall status and message) atomically."""
        dataset_dir = self._get_dataset_dir(dataset_id)
        dataset_dir.mkdir(parents=True, exist_ok=True)
        status_file = self._status_file(dataset_id)

        now = datetime.now(timezone.utc).isoformat()

        def _modify(state: dict) -> None:
            state["dataset_id"] = dataset_id
            state["status"] = status
            state["message"] = message
            state["updated_at"] = now
            state.setdefault("steps", {})
            if data:
                state.update(data)

        _read_modify_write_json(status_file, _modify)
        logger.info("Pipeline status for %s: %s - %s", dataset_id, status, message)

    def _compute_overall_status(self, steps: Dict[str, Any]) -> str:
        """Compute overall pipeline status from per-step results."""
        if not steps:
            return PIPELINE_FAILED

        statuses = [s.get("status", STEP_PENDING) for s in steps.values()]

        if all(s in (STEP_SUCCESS, STEP_SKIPPED) for s in statuses):
            return PIPELINE_SUCCESS

        # All non-skipped steps failed → overall failed
        non_skipped = [s for s in statuses if s != STEP_SKIPPED]
        if non_skipped and all(s == STEP_FAILED for s in non_skipped):
            return PIPELINE_FAILED

        has_success = any(s == STEP_SUCCESS for s in statuses)
        has_failure = any(s == STEP_FAILED for s in statuses)
        if has_success and has_failure:
            return PIPELINE_PARTIAL

        if any(s in (STEP_PENDING, STEP_RUNNING) for s in statuses):
            return PIPELINE_RUNNING

        return PIPELINE_PARTIAL

    # ------------------------------------------------------------------
    # BQ-117: File-type-aware parquet generation
    # ------------------------------------------------------------------

    def _generate_processed_parquet(self, filepath: Path, output_path: Path) -> None:
        """Generate processed.parquet from *filepath* using the correct DuckDB reader.

        Uses ``duckdb_service.get_read_function`` to pick the right reader
        (read_csv_auto, read_json_auto, read_parquet, or pandas-Excel) instead
        of blindly assuming parquet.

        Raises on failure so the caller can mark the step as failed.
        """
        with ephemeral_duckdb_service() as duckdb:
            file_type = duckdb.detect_file_type(filepath)
            if not file_type:
                raise ValueError(f"Unsupported file type: {filepath.suffix}")

            read_func = duckdb.get_read_function(file_type, str(filepath))
            escaped_output = sql_quote_literal(str(output_path))

            duckdb.connection.execute(
                f"COPY (SELECT * FROM {read_func}) "
                f"TO '{escaped_output}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )

    @staticmethod
    def _validate_parquet(path: Path) -> None:
        """Assert *path* exists and is non-empty. Raises ``RuntimeError`` on failure."""
        if not path.exists():
            raise RuntimeError(f"processed.parquet was not created at {path}")
        if path.stat().st_size == 0:
            raise RuntimeError(f"processed.parquet is empty (0 bytes) at {path}")

    # ------------------------------------------------------------------
    # BQ-088 Core: run_full_pipeline
    # ------------------------------------------------------------------

    async def run_full_pipeline(self, dataset_id: str) -> Dict[str, Any]:
        """
        Run the full 3-step pipeline: (1) analyze/process, (2) PII scan, (3) compliance check.

        Graceful degradation: if PII scan fails, compliance uses empty PII results.
        Produces output files: processed parquet, pii_scan.json, compliance_report.json.

        Args:
            dataset_id: The ID of the dataset to process.

        Returns:
            Dict with status and per-step results.
        """
        self._init_pipeline_state(dataset_id, FULL_PIPELINE_STEPS)
        self._update_status(dataset_id, PIPELINE_RUNNING, "Full pipeline started.")

        dataset_dir = self._get_dataset_dir(dataset_id)
        dataset_dir.mkdir(parents=True, exist_ok=True)

        # Resolve dataset filepath
        with ephemeral_duckdb_service() as duckdb:
            dataset_info = duckdb.get_dataset_by_id(dataset_id)
        if not dataset_info:
            self._update_status(dataset_id, PIPELINE_FAILED, f"Dataset '{dataset_id}' not found.")
            for step in FULL_PIPELINE_STEPS:
                self._set_step_status(dataset_id, step, STEP_FAILED, "Dataset not found")
            return self.get_pipeline_status(dataset_id)

        filepath = Path(dataset_info["filepath"])

        # ---- Step 1: Analyze / Process ----
        processed_parquet_path = dataset_dir / "processed.parquet"
        self._set_step_status(dataset_id, "analyze_process", STEP_RUNNING)
        try:
            self._update_status(dataset_id, PIPELINE_RUNNING, "Step 1/3: Analyzing and processing dataset...")
            with ephemeral_duckdb_service() as duckdb:
                metadata = duckdb.get_enhanced_metadata(filepath)

            # Save analysis output
            _atomic_write_json(dataset_dir / "analysis.json", metadata)

            # Generate processed parquet using correct reader per file type
            self._generate_processed_parquet(filepath, processed_parquet_path)

            # BQ-117: Validate output exists and is non-empty
            self._validate_parquet(processed_parquet_path)

            self._set_step_status(dataset_id, "analyze_process", STEP_SUCCESS)
        except Exception as e:
            logger.error("Analyze/process failed for %s: %s", dataset_id, e, exc_info=True)
            self._set_step_status(dataset_id, "analyze_process", STEP_FAILED, str(e))
            # Hard dependency — mark remaining steps as skipped
            self._set_step_status(dataset_id, "pii_scan", STEP_SKIPPED, "Skipped due to analyze failure")
            self._set_step_status(dataset_id, "compliance_check", STEP_SKIPPED, "Skipped due to analyze failure")
            self._update_status(dataset_id, PIPELINE_FAILED, "Analyze/process step failed. Pipeline halted.")
            return self.get_pipeline_status(dataset_id)

        # ---- Step 2: PII Scan ----
        pii_scan_result = None
        pii_scan_path = dataset_dir / "pii_scan.json"
        self._set_step_status(dataset_id, "pii_scan", STEP_RUNNING)
        try:
            self._update_status(dataset_id, PIPELINE_RUNNING, "Step 2/3: Scanning for PII...")
            scan_target = processed_parquet_path if processed_parquet_path.exists() else filepath
            pii_scan_result = self.pii_service.scan_structured(scan_target)

            _atomic_write_json(pii_scan_path, pii_scan_result)
            self._set_step_status(dataset_id, "pii_scan", STEP_SUCCESS)
        except Exception as e:
            logger.error("PII scan failed for %s: %s", dataset_id, e, exc_info=True)
            self._set_step_status(dataset_id, "pii_scan", STEP_FAILED, str(e))
            # Graceful degradation: write empty PII results so compliance can proceed
            pii_scan_result = {
                "dataset_id": dataset_id,
                "columns": {},
                "column_results": [],
                "overall_risk": "none",
                "total_pii_findings": 0,
                "scan_error": str(e),
            }
            _atomic_write_json(pii_scan_path, pii_scan_result)
            logger.info("Wrote empty PII results for %s to allow compliance step", dataset_id)

        # ---- Step 3: Compliance Check ----
        compliance_report_path = dataset_dir / "compliance_report.json"
        self._set_step_status(dataset_id, "compliance_check", STEP_RUNNING)
        try:
            self._update_status(dataset_id, PIPELINE_RUNNING, "Step 3/3: Running compliance check...")
            compliance_report = await self.compliance_service.generate_compliance_report(dataset_id)

            report_data = (
                compliance_report.model_dump()
                if hasattr(compliance_report, "model_dump")
                else compliance_report.dict()
                if hasattr(compliance_report, "dict")
                else compliance_report
            )
            _atomic_write_json(compliance_report_path, report_data)
            self._set_step_status(dataset_id, "compliance_check", STEP_SUCCESS)
        except Exception as e:
            logger.error("Compliance check failed for %s: %s", dataset_id, e, exc_info=True)
            self._set_step_status(dataset_id, "compliance_check", STEP_FAILED, str(e))

        # ---- Finalize ----
        result = self.get_pipeline_status(dataset_id)
        overall = result["status"]
        self._update_status(dataset_id, overall, f"Pipeline finished with status: {overall}")

        return result

    # ------------------------------------------------------------------
    # BQ-088: get_pipeline_status
    # ------------------------------------------------------------------

    def get_pipeline_status(self, dataset_id: str) -> Dict[str, Any]:
        """
        Get the current pipeline status for a dataset.

        Returns:
            Dict with:
                - dataset_id: str
                - status: 'running' | 'success' | 'partial' | 'failed'
                - message: str
                - steps: dict of step_name -> {status, started_at, finished_at, error}
                - output_files: dict of file_type -> path (if exists)
        """
        dataset_dir = self._get_dataset_dir(dataset_id)
        status_file = self._status_file(dataset_id)

        if not status_file.exists():
            return {
                "dataset_id": dataset_id,
                "status": PIPELINE_FAILED,
                "message": "No pipeline run found for this dataset.",
                "steps": {},
                "output_files": {},
            }

        try:
            state = _read_json_locked(status_file)
        except (json.JSONDecodeError, OSError):
            return {
                "dataset_id": dataset_id,
                "status": PIPELINE_FAILED,
                "message": "Could not read pipeline status file.",
                "steps": {},
                "output_files": {},
            }

        steps = state.get("steps", {})
        computed_status = self._compute_overall_status(steps)

        # Enumerate output files
        output_files = {}
        for name, filename in [
            ("processed_parquet", "processed.parquet"),
            ("pii_scan", "pii_scan.json"),
            ("compliance_report", "compliance_report.json"),
            ("attestation", "attestation.json"),
            ("analysis", "analysis.json"),
            ("duckdb_analysis", "duckdb_analysis.json"),
            ("sketch_profile", "sketch_profile.json"),
            ("quality_scorecard", "quality_scorecard.json"),
        ]:
            fpath = dataset_dir / filename
            if fpath.exists():
                output_files[name] = filename

        return {
            "dataset_id": dataset_id,
            "status": computed_status,
            "message": state.get("message", ""),
            "started_at": state.get("started_at"),
            "updated_at": state.get("updated_at"),
            "steps": steps,
            "output_files": output_files,
        }

    # ------------------------------------------------------------------
    # Extended pipeline (original run_pipeline with 5 steps)
    # ------------------------------------------------------------------

    async def run_pipeline(self, dataset_id: str):
        """
        Runs the extended processing pipeline for a dataset (7 steps).

        Args:
            dataset_id: The ID of the dataset to process.
        """
        self._init_pipeline_state(dataset_id, EXTENDED_PIPELINE_STEPS)
        self._update_status(dataset_id, PIPELINE_RUNNING, "Pipeline started.")

        total_steps = len(EXTENDED_PIPELINE_STEPS)

        with ephemeral_duckdb_service() as duckdb:
            dataset_info = duckdb.get_dataset_by_id(dataset_id)
        if not dataset_info:
            self._update_status(dataset_id, PIPELINE_FAILED, f"Dataset with id '{dataset_id}' not found.")
            return

        filepath = Path(dataset_info["filepath"])
        dataset_dir = self._get_dataset_dir(dataset_id)

        # --- Step 1: DuckDB Analysis ---
        self._set_step_status(dataset_id, "duckdb_analysis", STEP_RUNNING)
        try:
            self._update_status(dataset_id, PIPELINE_RUNNING, f"Step 1/{total_steps}: Analyzing dataset with DuckDB...")
            with ephemeral_duckdb_service() as duckdb:
                metadata = duckdb.get_enhanced_metadata(filepath)
            _atomic_write_json(dataset_dir / "duckdb_analysis.json", metadata)
            self._set_step_status(dataset_id, "duckdb_analysis", STEP_SUCCESS)
        except Exception as e:
            logger.error("DuckDB analysis failed for %s: %s", dataset_id, e, exc_info=True)
            self._set_step_status(dataset_id, "duckdb_analysis", STEP_FAILED, str(e))
            self._update_status(dataset_id, PIPELINE_FAILED, "DuckDB analysis failed. Halting pipeline.")
            return  # Hard dependency, cannot continue

        # --- Step 2: Sketch Profile (DataSketches) ---
        self._set_step_status(dataset_id, "sketch_profile", STEP_RUNNING)
        try:
            self._update_status(dataset_id, PIPELINE_RUNNING, f"Step 2/{total_steps}: Generating statistical profile...")
            self.sketch_service.generate_profile(dataset_id)
            self._set_step_status(dataset_id, "sketch_profile", STEP_SUCCESS)
        except Exception as e:
            logger.error("Sketch profile failed for %s: %s", dataset_id, e, exc_info=True)
            self._set_step_status(dataset_id, "sketch_profile", STEP_FAILED, str(e))

        # --- Step 3: PII Scan ---
        pii_scan_result = None
        self._set_step_status(dataset_id, "pii_scan", STEP_RUNNING)
        try:
            self._update_status(dataset_id, PIPELINE_RUNNING, f"Step 3/{total_steps}: Scanning for PII...")
            pii_scan_result = self.pii_service.scan_structured(filepath)
            _atomic_write_json(dataset_dir / "pii_scan.json", pii_scan_result)
            self._set_step_status(dataset_id, "pii_scan", STEP_SUCCESS)
        except Exception as e:
            logger.error("PII scan failed for %s: %s", dataset_id, e, exc_info=True)
            self._set_step_status(dataset_id, "pii_scan", STEP_FAILED, str(e))

        # --- Step 4: Quality Check (Pandera) ---
        self._set_step_status(dataset_id, "quality_check", STEP_RUNNING)
        try:
            self._update_status(dataset_id, PIPELINE_RUNNING, f"Step 4/{total_steps}: Running quality checks...")
            self.quality_contract_service.validate_dataset(dataset_id)
            self._set_step_status(dataset_id, "quality_check", STEP_SUCCESS)
        except Exception as e:
            logger.error("Quality check failed for %s: %s", dataset_id, e, exc_info=True)
            self._set_step_status(dataset_id, "quality_check", STEP_FAILED, str(e))

        # --- Step 5: Compliance Report ---
        self._set_step_status(dataset_id, "compliance_report", STEP_RUNNING)
        if pii_scan_result:
            try:
                self._update_status(dataset_id, PIPELINE_RUNNING, f"Step 5/{total_steps}: Generating compliance report...")
                compliance_report = await self.compliance_service.generate_compliance_report(dataset_id)
                self._set_step_status(dataset_id, "compliance_report", STEP_SUCCESS)
            except Exception as e:
                logger.error("Compliance report failed for %s: %s", dataset_id, e, exc_info=True)
                self._set_step_status(dataset_id, "compliance_report", STEP_FAILED, str(e))
        else:
            self._update_status(dataset_id, PIPELINE_RUNNING, f"Step 5/{total_steps}: Skipping compliance report (no PII results).")
            self._set_step_status(dataset_id, "compliance_report", STEP_SKIPPED, "No PII results available")

        # --- Step 6: Attestation ---
        self._set_step_status(dataset_id, "attestation", STEP_RUNNING)
        try:
            self._update_status(dataset_id, PIPELINE_RUNNING, f"Step 6/{total_steps}: Generating quality attestation...")
            attestation = await self.attestation_service.generate_attestation(dataset_id)
            self._set_step_status(dataset_id, "attestation", STEP_SUCCESS)
        except Exception as e:
            logger.error("Attestation failed for %s: %s", dataset_id, e, exc_info=True)
            self._set_step_status(dataset_id, "attestation", STEP_FAILED, str(e))

        # --- Step 7: Listing Metadata ---
        self._set_step_status(dataset_id, "listing_metadata", STEP_RUNNING)
        try:
            self._update_status(dataset_id, PIPELINE_RUNNING, f"Step 7/{total_steps}: Generating listing metadata...")
            listing_metadata = await self.listing_metadata_service.generate_listing_metadata(dataset_id)
            self._set_step_status(dataset_id, "listing_metadata", STEP_SUCCESS)
        except Exception as e:
            logger.error("Listing metadata generation failed for %s: %s", dataset_id, e, exc_info=True)
            self._set_step_status(dataset_id, "listing_metadata", STEP_FAILED, str(e))

        # --- Finalize ---
        result = self.get_pipeline_status(dataset_id)
        overall = result["status"]
        self._update_status(dataset_id, overall, f"Pipeline finished with status: {overall}")


# Singleton instance
_pipeline_service: Optional[PipelineService] = None


def get_pipeline_service() -> PipelineService:
    """Get the singleton PipelineService instance."""
    global _pipeline_service
    if _pipeline_service is None:
        _pipeline_service = PipelineService()
    return _pipeline_service
