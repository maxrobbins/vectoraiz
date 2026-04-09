"""
BQ-117: Pipeline robustness tests.

Covers:
  - Pipeline with CSV (correct DuckDB reader per file type)
  - Failing step → parquet validation, downstream skips
  - Atomic status writes under concurrency
  - Single canonical ``status`` field (no ``overall_status``)
  - job_id removed from process-full endpoint
"""
import asyncio
import csv
import json
from contextlib import contextmanager
from unittest.mock import MagicMock, AsyncMock, patch

import pytest

from app.services.pipeline_service import (
    PipelineService,
    PIPELINE_FAILED,
    PIPELINE_SUCCESS,
    PIPELINE_PARTIAL,
    PIPELINE_RUNNING,
    STEP_FAILED,
    STEP_SKIPPED,
    STEP_SUCCESS,
    _atomic_write_json,
    _read_json_locked,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def pipeline_dir(tmp_path):
    """Provide a temporary data directory for pipeline tests."""
    return tmp_path / "data"


@pytest.fixture
def sample_csv(tmp_path):
    """Create a minimal CSV fixture inside a DuckDB-scannable location."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    csv_file = data_dir / "test_dataset.csv"
    with open(csv_file, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "name", "value"])
        for i in range(5):
            w.writerow([i, f"row_{i}", i * 10])
    return csv_file


@pytest.fixture
def pipeline_service(pipeline_dir, sample_csv):
    """Build a PipelineService wired to temp directories with mocked deps."""
    with patch("app.services.pipeline_service.settings") as mock_settings, \
         patch("app.services.duckdb_service.settings") as duck_settings:

        mock_settings.data_directory = str(pipeline_dir)
        duck_settings.data_directory = str(pipeline_dir)
        duck_settings.duckdb_memory_limit = "256MB"
        duck_settings.duckdb_threads = 2

        # Real DuckDB service for CSV reading
        from app.services.duckdb_service import DuckDBService
        duckdb_svc = DuckDBService()
        # Eagerly create the connection so the temp dir is created under tmp_path
        _ = duckdb_svc.connection

        @contextmanager
        def _mock_ephemeral():
            yield duckdb_svc

        svc = PipelineService.__new__(PipelineService)
        svc.duckdb_service = duckdb_svc  # for test access; code uses ephemeral_duckdb_service
        svc.pii_service = MagicMock()
        svc.compliance_service = MagicMock()
        svc.attestation_service = MagicMock()
        svc.listing_metadata_service = MagicMock()
        svc.processing_dir = pipeline_dir / "processed"
        svc.processing_dir.mkdir(parents=True, exist_ok=True)

        # Default: mock services return something sensible
        svc.pii_service.scan_dataset.return_value = {
            "columns": {},
            "column_results": [],
            "overall_risk": "none",
            "total_pii_findings": 0,
        }
        svc.compliance_service.generate_compliance_report = AsyncMock(
            return_value={"flags": [], "compliance_score": 100}
        )

        with patch("app.services.pipeline_service.ephemeral_duckdb_service", side_effect=_mock_ephemeral):
            yield svc
        duckdb_svc.close()


# ---------------------------------------------------------------------------
# AC-1: Step 1 validates processed.parquet exists + size > 0
# ---------------------------------------------------------------------------

class TestParquetValidation:

    def test_validate_parquet_missing(self, tmp_path):
        """_validate_parquet raises when file doesn't exist."""
        with pytest.raises(RuntimeError, match="was not created"):
            PipelineService._validate_parquet(tmp_path / "nope.parquet")

    def test_validate_parquet_empty(self, tmp_path):
        """_validate_parquet raises when file is 0 bytes."""
        empty = tmp_path / "empty.parquet"
        empty.write_bytes(b"")
        with pytest.raises(RuntimeError, match="empty"):
            PipelineService._validate_parquet(empty)

    def test_validate_parquet_ok(self, tmp_path):
        """_validate_parquet passes for non-empty file."""
        ok = tmp_path / "ok.parquet"
        ok.write_bytes(b"\x00" * 100)
        PipelineService._validate_parquet(ok)  # should not raise

    @pytest.mark.asyncio
    async def test_step1_failure_skips_downstream(self, pipeline_service):
        """If analyze_process fails, PII and compliance are SKIPPED."""
        # Patch ephemeral to return a mock that points to nonexistent file
        mock_duckdb = MagicMock()
        mock_duckdb.get_dataset_by_id.return_value = {
            "filepath": "/nonexistent/file.csv",
            "file_type": "csv",
        }

        @contextmanager
        def _mock_ephemeral():
            yield mock_duckdb

        with patch("app.services.pipeline_service.ephemeral_duckdb_service", side_effect=_mock_ephemeral):
            result = await pipeline_service.run_full_pipeline("bad_dataset")

        assert result["status"] == PIPELINE_FAILED
        assert result["steps"]["analyze_process"]["status"] == STEP_FAILED
        assert result["steps"]["pii_scan"]["status"] == STEP_SKIPPED
        assert result["steps"]["compliance_check"]["status"] == STEP_SKIPPED


# ---------------------------------------------------------------------------
# AC-2: Step 1 uses correct DuckDB readers per file type (CSV test)
# ---------------------------------------------------------------------------

class TestCSVPipeline:

    @pytest.mark.asyncio
    async def test_full_pipeline_csv(self, pipeline_service, sample_csv):
        """Full pipeline succeeds with a CSV file and produces valid parquet."""
        pipeline_service.duckdb_service.get_dataset_by_id = MagicMock(
            return_value={
                "filepath": str(sample_csv),
                "file_type": "csv",
                "id": "test_dataset",
                "filename": "test_dataset.csv",
                "row_count": 5,
                "column_count": 3,
                "columns": [],
                "size_bytes": sample_csv.stat().st_size,
                "created_at": "2024-01-01T00:00:00",
                "modified_at": "2024-01-01T00:00:00",
                "status": "ready",
            }
        )

        result = await pipeline_service.run_full_pipeline("test_dataset")

        assert result["status"] == PIPELINE_SUCCESS
        assert result["steps"]["analyze_process"]["status"] == STEP_SUCCESS

        # Verify parquet file was actually created and is non-empty
        parquet_path = pipeline_service.processing_dir / "test_dataset" / "processed.parquet"
        assert parquet_path.exists()
        assert parquet_path.stat().st_size > 0


# ---------------------------------------------------------------------------
# AC-3: Status file uses atomic write (temp + os.replace) + file lock
# ---------------------------------------------------------------------------

class TestAtomicWrites:

    def test_atomic_write_creates_file(self, tmp_path):
        """_atomic_write_json creates a valid JSON file."""
        target = tmp_path / "status.json"
        data = {"status": "running", "step": 1}
        _atomic_write_json(target, data)

        assert target.exists()
        with open(target) as f:
            assert json.load(f) == data

    def test_atomic_write_replaces_existing(self, tmp_path):
        """_atomic_write_json atomically replaces an existing file."""
        target = tmp_path / "status.json"
        _atomic_write_json(target, {"v": 1})
        _atomic_write_json(target, {"v": 2})

        with open(target) as f:
            assert json.load(f)["v"] == 2

    def test_read_json_locked(self, tmp_path):
        """_read_json_locked returns valid JSON content."""
        target = tmp_path / "data.json"
        _atomic_write_json(target, {"key": "value"})
        result = _read_json_locked(target)
        assert result == {"key": "value"}

    def test_no_leftover_tmp_files(self, tmp_path):
        """After atomic write, no .tmp files should remain."""
        target = tmp_path / "status.json"
        _atomic_write_json(target, {"clean": True})

        tmp_files = list(tmp_path.glob("*.tmp"))
        assert len(tmp_files) == 0

    def test_concurrent_atomic_writes(self, tmp_path):
        """Multiple concurrent writes should not corrupt the status file."""
        target = tmp_path / "concurrent.json"

        def write_n(n):
            for i in range(20):
                _atomic_write_json(target, {"writer": n, "iteration": i})

        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(write_n, n) for n in range(4)]
            for f in futures:
                f.result()

        # File must be valid JSON after all concurrent writes
        result = _read_json_locked(target)
        assert "writer" in result
        assert "iteration" in result

        # No leftover temp files
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert len(tmp_files) == 0


# ---------------------------------------------------------------------------
# AC-4: Single canonical status field (no overall_status)
# ---------------------------------------------------------------------------

class TestCanonicalStatusField:

    @pytest.mark.asyncio
    async def test_get_pipeline_status_uses_status_not_overall_status(self, pipeline_service, sample_csv):
        """get_pipeline_status returns 'status' and never 'overall_status'."""
        pipeline_service.duckdb_service.get_dataset_by_id = MagicMock(
            return_value={
                "filepath": str(sample_csv),
                "file_type": "csv",
                "id": "test_dataset",
                "filename": "test_dataset.csv",
                "row_count": 5,
                "column_count": 3,
                "columns": [],
                "size_bytes": sample_csv.stat().st_size,
                "created_at": "2024-01-01T00:00:00",
                "modified_at": "2024-01-01T00:00:00",
                "status": "ready",
            }
        )

        result = await pipeline_service.run_full_pipeline("test_dataset")

        assert "status" in result
        assert "overall_status" not in result

    def test_no_pipeline_run_uses_status_field(self, pipeline_service):
        """When no pipeline has run, response uses 'status' not 'overall_status'."""
        result = pipeline_service.get_pipeline_status("nonexistent")
        assert "status" in result
        assert "overall_status" not in result
        assert result["status"] == PIPELINE_FAILED

    def test_compute_overall_status_running(self, pipeline_service):
        """Steps still pending/running → PIPELINE_RUNNING (not PIPELINE_PARTIAL)."""
        steps = {
            "step1": {"status": "success"},
            "step2": {"status": "running"},
        }
        assert pipeline_service._compute_overall_status(steps) == PIPELINE_RUNNING


# ---------------------------------------------------------------------------
# AC-5: job_id removed from process-full endpoint
# ---------------------------------------------------------------------------

class TestJobIdRemoved:

    @pytest.mark.asyncio
    async def test_process_full_no_job_id(self):
        """POST /process-full response no longer contains job_id."""
        from app.routers.datasets import process_full_pipeline

        # Check the function signature / source doesn't generate job_id
        import inspect
        source = inspect.getsource(process_full_pipeline)
        assert "job_id" not in source


# ---------------------------------------------------------------------------
# AC-6: Concurrent pipelines (different dataset IDs)
# ---------------------------------------------------------------------------

class TestConcurrentPipelines:

    @pytest.mark.asyncio
    async def test_two_concurrent_pipelines(self, pipeline_service, tmp_path):
        """Two pipelines for different datasets run concurrently without corruption."""
        # Create two CSV files
        data_dir = pipeline_service.duckdb_service.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)

        for dsid in ("ds_a", "ds_b"):
            csv_file = data_dir / f"{dsid}.csv"
            with open(csv_file, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["id", "name"])
                for i in range(3):
                    w.writerow([i, f"{dsid}_row_{i}"])

        # Make get_dataset_by_id resolve correctly

        def _patched_get(dataset_id):
            csv_file = data_dir / f"{dataset_id}.csv"
            if csv_file.exists():
                return pipeline_service.duckdb_service.get_file_metadata(csv_file)
            return None

        pipeline_service.duckdb_service.get_dataset_by_id = _patched_get

        # Run both pipelines concurrently
        results = await asyncio.gather(
            pipeline_service.run_full_pipeline("ds_a"),
            pipeline_service.run_full_pipeline("ds_b"),
        )

        for i, dsid in enumerate(("ds_a", "ds_b")):
            r = results[i]
            assert r["dataset_id"] == dsid
            assert r["status"] in (PIPELINE_SUCCESS, PIPELINE_PARTIAL)
            assert "overall_status" not in r

            # Each dataset's status file should be valid JSON
            sf = pipeline_service._status_file(dsid)
            assert sf.exists()
            data = _read_json_locked(sf)
            assert data["dataset_id"] == dsid
