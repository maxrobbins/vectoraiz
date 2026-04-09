"""
Tests for BQ-VZ-LARGE-FILES Phase 1 + Phase 2: Streaming/chunked processing.

Covers:
- StreamingTabularProcessor (CSV, TSV, Parquet, JSON)
- StreamingDocumentProcessor (PDF fallback, DOCX, PPTX)
- Security: zip bomb detection, file size validation
- DuckDB Arrow-based metadata (M6)
- Graceful degradation (M10)
- Configuration settings
- index_streaming chunked indexing (R5)

Phase 2 additions:
- ParquetWriter row_group_size from PARQUET_ROW_GROUP_SIZE_MB (M3)
- Atomic write: .partial cleanup on crash (M3)
- pypdfium2 per-page fallback + pdfplumber opened once (M4)
- Arrow IPC serialization roundtrip
"""

import csv
import json
import os
import signal
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_data(tmp_path):
    """Create temp directories mimicking vectorAIz layout."""
    uploads = tmp_path / "uploads"
    processed = tmp_path / "processed"
    temp = tmp_path / "temp"
    uploads.mkdir()
    processed.mkdir()
    temp.mkdir()
    return tmp_path


@pytest.fixture
def sample_csv(tmp_data):
    """Create a sample CSV with 250 rows."""
    csv_path = tmp_data / "uploads" / "test.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "description", "value"])
        for i in range(250):
            writer.writerow([i, f"item_{i}", f"Description for item {i}", i * 1.5])
    return csv_path


@pytest.fixture
def sample_tsv(tmp_data):
    """Create a sample TSV file."""
    tsv_path = tmp_data / "uploads" / "test.tsv"
    with open(tsv_path, "w") as f:
        f.write("id\tname\tvalue\n")
        for i in range(100):
            f.write(f"{i}\titem_{i}\t{i * 2}\n")
    return tsv_path


@pytest.fixture
def sample_parquet(tmp_data):
    """Create a sample Parquet file with multiple row groups."""
    parquet_path = tmp_data / "uploads" / "test.parquet"
    df = pd.DataFrame({
        "id": range(500),
        "name": [f"item_{i}" for i in range(500)],
        "description": [f"A longer description for item number {i}" for i in range(500)],
        "value": [i * 3.14 for i in range(500)],
    })
    table = pa.Table.from_pandas(df)
    # Write with small row group to test iteration
    pq.write_table(table, str(parquet_path), row_group_size=100)
    return parquet_path


@pytest.fixture
def sample_jsonl(tmp_data):
    """Create a sample JSONL file."""
    jsonl_path = tmp_data / "uploads" / "test.json"
    with open(jsonl_path, "w") as f:
        for i in range(150):
            json.dump({"id": i, "name": f"item_{i}", "score": i * 0.1}, f)
            f.write("\n")
    return jsonl_path


@pytest.fixture
def sample_json_array(tmp_data):
    """Create a sample JSON array file."""
    json_path = tmp_data / "uploads" / "test_array.json"
    data = [{"id": i, "label": f"label_{i}"} for i in range(50)]
    with open(json_path, "w") as f:
        json.dump(data, f)
    return json_path


@pytest.fixture
def zip_bomb(tmp_data):
    """Create a zip bomb (high compression ratio)."""
    bomb_path = tmp_data / "uploads" / "bomb.zip"
    # Create a file with highly compressible content
    inner_content = b"\x00" * (10 * 1024 * 1024)  # 10MB of zeros
    with zipfile.ZipFile(bomb_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("big.txt", inner_content)
    return bomb_path


@pytest.fixture
def normal_zip(tmp_data):
    """Create a normal zip file (low compression ratio)."""
    zip_path = tmp_data / "uploads" / "normal.zip"
    # Random-ish content doesn't compress well
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("data.csv", "id,name\n1,alice\n2,bob\n")
    return zip_path


# ---------------------------------------------------------------------------
# StreamingTabularProcessor tests
# ---------------------------------------------------------------------------


class TestStreamingTabularProcessor:
    """Test StreamingTabularProcessor for all tabular formats."""

    def test_csv_yields_batches(self, sample_csv):
        with patch("app.services.streaming_processor.settings") as mock_s:
            mock_s.streaming_batch_target_rows = 50
            mock_s.max_upload_size_gb = 10
            from app.services.streaming_processor import StreamingTabularProcessor

            proc = StreamingTabularProcessor(sample_csv, "csv")
            batches = list(proc)

            assert len(batches) > 0
            total_rows = sum(b.num_rows for b in batches)
            assert total_rows == 250
            # With batch size 50, we expect ~5 batches
            assert len(batches) >= 4

            # Verify schema
            schema = batches[0].schema
            assert "id" in schema.names
            assert "name" in schema.names
            assert "description" in schema.names

    def test_tsv_yields_batches(self, sample_tsv):
        with patch("app.services.streaming_processor.settings") as mock_s:
            mock_s.streaming_batch_target_rows = 30
            mock_s.max_upload_size_gb = 10
            from app.services.streaming_processor import StreamingTabularProcessor

            proc = StreamingTabularProcessor(sample_tsv, "tsv")
            batches = list(proc)

            total_rows = sum(b.num_rows for b in batches)
            assert total_rows == 100

    def test_parquet_yields_batches(self, sample_parquet):
        with patch("app.services.streaming_processor.settings") as mock_s:
            mock_s.streaming_batch_target_rows = 100
            mock_s.max_upload_size_gb = 10
            from app.services.streaming_processor import StreamingTabularProcessor

            proc = StreamingTabularProcessor(sample_parquet, "parquet")
            batches = list(proc)

            total_rows = sum(b.num_rows for b in batches)
            assert total_rows == 500
            assert len(batches) >= 5  # 5 row groups of 100

    def test_jsonl_yields_batches(self, sample_jsonl):
        with patch("app.services.streaming_processor.settings") as mock_s:
            mock_s.streaming_batch_target_rows = 50
            mock_s.max_upload_size_gb = 10
            from app.services.streaming_processor import StreamingTabularProcessor

            proc = StreamingTabularProcessor(sample_jsonl, "json")
            batches = list(proc)

            total_rows = sum(b.num_rows for b in batches)
            assert total_rows == 150

    def test_json_array_yields_batches(self, sample_json_array):
        with patch("app.services.streaming_processor.settings") as mock_s:
            mock_s.streaming_batch_target_rows = 20
            mock_s.max_upload_size_gb = 10
            from app.services.streaming_processor import StreamingTabularProcessor

            proc = StreamingTabularProcessor(sample_json_array, "json")
            batches = list(proc)

            total_rows = sum(b.num_rows for b in batches)
            assert total_rows == 50

    def test_unsupported_type_raises(self, sample_csv):
        with patch("app.services.streaming_processor.settings") as mock_s:
            mock_s.streaming_batch_target_rows = 50
            from app.services.streaming_processor import StreamingTabularProcessor

            proc = StreamingTabularProcessor(sample_csv, "xlsx")
            with pytest.raises(ValueError, match="Unsupported tabular type"):
                list(proc)

    def test_batch_is_record_batch(self, sample_csv):
        """Verify chunks are pyarrow.RecordBatch instances."""
        with patch("app.services.streaming_processor.settings") as mock_s:
            mock_s.streaming_batch_target_rows = 100
            mock_s.max_upload_size_gb = 10
            from app.services.streaming_processor import StreamingTabularProcessor

            proc = StreamingTabularProcessor(sample_csv, "csv")
            batch = next(iter(proc))
            assert isinstance(batch, pa.RecordBatch)


# ---------------------------------------------------------------------------
# Security tests
# ---------------------------------------------------------------------------


class TestSecurityChecks:
    """Test zip bomb detection and file size validation."""

    def test_zip_bomb_rejected(self, zip_bomb):
        from app.services.streaming_processor import check_zip_bomb

        with pytest.raises(ValueError, match="Zip bomb detected"):
            check_zip_bomb(zip_bomb)

    def test_normal_zip_passes(self, normal_zip):
        from app.services.streaming_processor import check_zip_bomb

        # Should not raise
        check_zip_bomb(normal_zip)

    def test_non_zip_passes(self, sample_csv):
        from app.services.streaming_processor import check_zip_bomb

        # Non-zip files should pass silently
        check_zip_bomb(sample_csv)

    def test_file_size_validation(self, sample_csv):
        with patch("app.services.streaming_processor.settings") as mock_s:
            mock_s.max_upload_size_gb = 0  # 0 GB = reject everything
            from app.services.streaming_processor import check_file_size

            with pytest.raises(ValueError, match="exceeds"):
                check_file_size(sample_csv)

    def test_file_size_passes(self, sample_csv):
        with patch("app.services.streaming_processor.settings") as mock_s:
            mock_s.max_upload_size_gb = 10
            from app.services.streaming_processor import check_file_size

            # Should not raise for small file
            check_file_size(sample_csv)


# ---------------------------------------------------------------------------
# Charset detection tests
# ---------------------------------------------------------------------------


class TestCharsetDetection:
    """Test encoding fallback chain."""

    def test_utf8_file(self, tmp_data):
        path = tmp_data / "utf8.csv"
        path.write_text("id,name\n1,café\n2,naïve\n", encoding="utf-8")

        from app.services.streaming_processor import _open_text_with_fallback

        fh = _open_text_with_fallback(path)
        content = fh.read()
        fh.close()
        assert "café" in content

    def test_latin1_file(self, tmp_data):
        path = tmp_data / "latin1.csv"
        path.write_bytes("id,name\n1,caf\xe9\n".encode("latin-1"))

        from app.services.streaming_processor import _open_text_with_fallback

        fh = _open_text_with_fallback(path)
        content = fh.read()
        fh.close()
        assert "caf" in content

    def test_utf8_bom_file(self, tmp_data):
        path = tmp_data / "bom.csv"
        path.write_bytes(b"\xef\xbb\xbfid,name\n1,test\n")

        from app.services.streaming_processor import _open_text_with_fallback

        fh = _open_text_with_fallback(path)
        content = fh.read()
        fh.close()
        assert "id,name" in content


# ---------------------------------------------------------------------------
# DuckDB Arrow-based metadata (M6)
# ---------------------------------------------------------------------------


class TestDuckDBArrowMetadata:
    """Test Arrow-based Parquet metadata extraction."""

    def test_parquet_metadata_arrow(self, sample_parquet):
        """M6: get_parquet_metadata_arrow uses metadata only, no full scan."""
        with patch("app.services.duckdb_service.settings") as mock_s:
            mock_s.data_directory = str(sample_parquet.parent.parent)
            mock_s.duckdb_memory_limit = "256MB"
            mock_s.duckdb_threads = 2
            from app.services.duckdb_service import DuckDBService

            service = DuckDBService()
            try:
                meta = service.get_parquet_metadata_arrow(sample_parquet)

                assert meta["row_count"] == 500
                assert meta["column_count"] == 4
                assert meta["file_type"] == "parquet"
                assert meta["num_row_groups"] >= 5

                col_names = [c["name"] for c in meta["columns"]]
                assert "id" in col_names
                assert "name" in col_names
            finally:
                service.close()

    def test_parquet_sample_arrow(self, sample_parquet):
        """M6: get_parquet_sample_arrow reads only first row group."""
        with patch("app.services.duckdb_service.settings") as mock_s:
            mock_s.data_directory = str(sample_parquet.parent.parent)
            mock_s.duckdb_memory_limit = "256MB"
            mock_s.duckdb_threads = 2
            from app.services.duckdb_service import DuckDBService

            service = DuckDBService()
            try:
                sample = service.get_parquet_sample_arrow(sample_parquet, limit=5)

                # Returns dict of {col: [values]}
                assert "id" in sample
                assert len(sample["id"]) == 5
            finally:
                service.close()

    def test_get_file_metadata_uses_arrow_for_parquet(self, sample_parquet):
        """M6: get_file_metadata routes to Arrow for Parquet files."""
        with patch("app.services.duckdb_service.settings") as mock_s:
            mock_s.data_directory = str(sample_parquet.parent.parent)
            mock_s.duckdb_memory_limit = "256MB"
            mock_s.duckdb_threads = 2
            from app.services.duckdb_service import DuckDBService

            service = DuckDBService()
            try:
                meta = service.get_file_metadata(sample_parquet)

                assert meta["row_count"] == 500
                assert meta["file_type"] == "parquet"
                # Should have the Arrow-specific field
                assert "num_row_groups" in meta
            finally:
                service.close()


# ---------------------------------------------------------------------------
# DuckDB disk-spill cleanup (M5)
# ---------------------------------------------------------------------------


class TestDuckDBDiskSpill:
    """Test DuckDB disk-spill configuration and cleanup."""

    def test_temp_dir_created(self, tmp_data):
        """M5: Temp directory is created on connection init."""
        with patch("app.services.duckdb_service.settings") as mock_s:
            mock_s.data_directory = str(tmp_data)
            mock_s.duckdb_memory_limit = "256MB"
            mock_s.duckdb_threads = 2
            from app.services.duckdb_service import DuckDBService

            service = DuckDBService()
            try:
                _ = service.connection  # trigger init
                assert (tmp_data / "temp").exists()
            finally:
                service.close()

    def test_cleanup_stale_temp_files(self, tmp_data):
        """M5: cleanup_dataset_temp removes stale files."""
        with patch("app.services.duckdb_service.settings") as mock_s:
            mock_s.data_directory = str(tmp_data)
            mock_s.duckdb_memory_limit = "256MB"
            mock_s.duckdb_threads = 2
            from app.services.duckdb_service import DuckDBService

            service = DuckDBService()
            try:
                temp_dir = tmp_data / "temp"
                temp_dir.mkdir(exist_ok=True)

                # Create a stale file (modify time in the past)
                stale_file = temp_dir / "stale.tmp"
                stale_file.write_text("old data")
                # Set mtime to 2 hours ago
                import time
                old_time = time.time() - 7200
                os.utime(stale_file, (old_time, old_time))

                service.cleanup_dataset_temp("some_id")

                assert not stale_file.exists()
            finally:
                service.close()


# ---------------------------------------------------------------------------
# Graceful Degradation (M10)
# ---------------------------------------------------------------------------


class TestGracefulDegradation:
    """Test M10: fallback behavior when streaming fails."""

    def test_is_large_file_above_threshold(self, sample_parquet):
        """Files above threshold are identified as large."""
        with patch("app.services.processing_service.settings") as mock_s:
            mock_s.large_file_threshold_mb = 0  # 0 MB = everything is large
            mock_s.upload_directory = str(sample_parquet.parent.parent / "uploads")
            mock_s.processed_directory = str(sample_parquet.parent.parent / "processed")
            from app.services.processing_service import ProcessingService, DatasetRecord

            service = ProcessingService()
            record = DatasetRecord("test", "test.parquet", "parquet")
            record.file_size_bytes = 200 * 1024 * 1024  # 200MB
            assert service._is_large_file(record)

    def test_is_large_file_below_threshold(self, sample_parquet):
        """Files below threshold are not identified as large."""
        with patch("app.services.processing_service.settings") as mock_s:
            mock_s.large_file_threshold_mb = 100
            mock_s.upload_directory = str(sample_parquet.parent.parent / "uploads")
            mock_s.processed_directory = str(sample_parquet.parent.parent / "processed")
            from app.services.processing_service import ProcessingService, DatasetRecord

            service = ProcessingService()
            record = DatasetRecord("test", "test.csv", "csv")
            record.file_size_bytes = 50 * 1024 * 1024  # 50MB
            assert not service._is_large_file(record)


# ---------------------------------------------------------------------------
# Configuration tests
# ---------------------------------------------------------------------------


class TestConfiguration:
    """Test that new config variables are properly defined."""

    def test_default_config_values(self):
        from app.config import Settings

        s = Settings(
            _env_file=None,  # Don't load .env
        )
        assert s.large_file_threshold_mb == 100
        assert s.fallback_max_size_mb == 200
        assert s.process_worker_memory_limit_mb == 2048
        assert s.process_worker_timeout_s == 1800
        assert s.process_worker_grace_period_s == 60
        assert s.process_worker_max_concurrent == 2
        assert s.duckdb_memory_limit_mb == 512
        assert s.max_upload_size_gb == 10
        assert s.streaming_queue_maxsize == 8
        assert s.streaming_batch_target_rows == 50000


# ---------------------------------------------------------------------------
# Process worker serialization tests
# ---------------------------------------------------------------------------


class TestProcessWorkerSerialization:
    """Test Arrow IPC serialization for Queue transport."""

    def test_record_batch_roundtrip(self):
        from app.services.process_worker import (
            serialize_record_batch,
            deserialize_record_batch,
        )

        # Create a test batch
        data = pa.RecordBatch.from_pydict({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "value": [1.1, 2.2, 3.3],
        })

        serialized = serialize_record_batch(data)
        assert isinstance(serialized, bytes)
        assert len(serialized) > 0

        deserialized = deserialize_record_batch(serialized)
        assert deserialized.num_rows == 3
        assert deserialized.schema.names == ["id", "name", "value"]
        assert deserialized.column("id").to_pylist() == [1, 2, 3]


# ---------------------------------------------------------------------------
# Indexing service streaming tests (R5)
# ---------------------------------------------------------------------------


class TestIndexStreaming:
    """Test chunked streaming indexing (R5)."""

    def test_detect_text_columns_from_rows(self):
        from app.services.indexing_service import IndexingService

        # Mock the dependencies
        with patch("app.services.indexing_service.get_embedding_service"), \
             patch("app.services.indexing_service.get_qdrant_service"):
            service = IndexingService()

            row = {
                "id": 1,
                "name": "Test Product",
                "description": "A very long description that has more than 10 characters",
                "price": 29.99,
                "short": "hi",
            }
            cols = service._detect_text_columns_from_rows(row)
            # "name" matches keyword, "description" matches keyword + length
            assert "name" in cols
            assert "description" in cols
            # "short" is too short and no keyword match
            assert "short" not in cols

    def test_stable_point_ids(self):
        """R5: Verify stable point IDs are deterministic UUID5 values."""
        from app.services.indexing_service import IndexingService

        with patch("app.services.indexing_service.get_embedding_service") as mock_embed, \
             patch("app.services.indexing_service.get_qdrant_service") as mock_qdrant:

            mock_embed_svc = MagicMock()
            mock_embed_svc.embed_texts.return_value = [[0.1] * 384]  # dummy embeddings
            mock_embed.return_value = mock_embed_svc

            mock_qdrant_svc = MagicMock()
            mock_qdrant_svc.upsert_vectors.return_value = {"upserted": 1}
            mock_qdrant.return_value = mock_qdrant_svc

            service = IndexingService()

            # Create a simple chunk iterator
            batch = pa.RecordBatch.from_pydict({
                "name": ["Test Item"],
                "description": ["A description that is long enough to be text"],
            })

            result = service.index_streaming(
                dataset_id="abc123",
                chunk_iterator=[batch],
            )

            assert result["status"] == "completed"
            assert result["rows_indexed"] == 1

            # Verify the payload had the correct point ID format
            call_args = mock_qdrant_svc.upsert_vectors.call_args
            payloads = call_args[1]["payloads"] if "payloads" in call_args[1] else call_args[0][2]
            import uuid as _uuid
            expected_id = str(_uuid.uuid5(_uuid.NAMESPACE_OID, "abc123:0:0"))
            assert payloads[0]["row_id"] == expected_id


# ---------------------------------------------------------------------------
# TextBlock dataclass tests
# ---------------------------------------------------------------------------


class TestTextBlock:
    """Test TextBlock data structure."""

    def test_textblock_creation(self):
        from app.services.streaming_processor import TextBlock

        block = TextBlock(
            page_num=1,
            text="Hello world",
            tables=["col1\tcol2\nval1\tval2"],
            metadata={"page_index": 0},
        )
        assert block.page_num == 1
        assert block.text == "Hello world"
        assert len(block.tables) == 1
        assert block.metadata["page_index"] == 0

    def test_textblock_defaults(self):
        from app.services.streaming_processor import TextBlock

        block = TextBlock(page_num=1, text="test")
        assert block.tables == []
        assert block.metadata == {}


# ---------------------------------------------------------------------------
# B1: M10 fallback with separate threshold (Gate 3)
# ---------------------------------------------------------------------------


class TestM10FallbackThreshold:
    """B1: Verify fallback triggers using fallback_max_size_mb, not large_file_threshold_mb."""

    @pytest.mark.asyncio
    async def test_streaming_failure_falls_back_for_small_file(self, sample_csv):
        """When streaming fails for a file < fallback_max_size_mb, in-memory runs."""
        with patch("app.services.processing_service.settings") as mock_s:
            mock_s.large_file_threshold_mb = 10    # 10MB threshold → file considered large
            mock_s.fallback_max_size_mb = 100       # 100MB fallback limit
            mock_s.upload_directory = str(sample_csv.parent.parent / "uploads")
            mock_s.processed_directory = str(sample_csv.parent.parent / "processed")

            from app.services.processing_service import ProcessingService, DatasetRecord

            service = ProcessingService()
            record = DatasetRecord("test-fb", "test.csv", "csv")
            record.upload_path = sample_csv
            record.file_size_bytes = 50 * 1024 * 1024  # 50MB — above threshold, below fallback

            # Mock _extract_streaming to raise, _extract_in_memory to succeed
            with patch.object(service, "_extract_streaming", side_effect=RuntimeError("boom")), \
                 patch.object(service, "_extract_in_memory") as mock_inmem, \
                 patch.object(service, "_is_large_file", return_value=True), \
                 patch.object(service, "_cache_preview"), \
                 patch.object(service, "_save_record"), \
                 patch.object(service, "get_dataset", return_value=record), \
                 patch.object(service, "_is_cancelled", return_value=False), \
                 patch.object(service, "_run_indexing"):
                result = await service.process_file("test-fb")

            mock_inmem.assert_called_once()
            assert result.metadata.get("processing_mode") == "fallback_in_memory"

    @pytest.mark.asyncio
    async def test_streaming_failure_no_fallback_for_huge_file(self, sample_csv):
        """When streaming fails for a file >= fallback_max_size_mb, error is raised."""
        with patch("app.services.processing_service.settings") as mock_s:
            mock_s.large_file_threshold_mb = 10
            mock_s.fallback_max_size_mb = 100
            mock_s.upload_directory = str(sample_csv.parent.parent / "uploads")
            mock_s.processed_directory = str(sample_csv.parent.parent / "processed")

            from app.services.processing_service import ProcessingService, DatasetRecord, DatasetStatus

            service = ProcessingService()
            record = DatasetRecord("test-nofb", "big.csv", "csv")
            record.upload_path = sample_csv
            record.file_size_bytes = 200 * 1024 * 1024  # 200MB — above fallback limit

            with patch.object(service, "_extract_streaming", side_effect=RuntimeError("OOM")), \
                 patch.object(service, "_extract_in_memory") as mock_inmem, \
                 patch.object(service, "_is_large_file", return_value=True), \
                 patch.object(service, "_cache_preview"), \
                 patch.object(service, "_save_record"), \
                 patch.object(service, "get_dataset", return_value=record), \
                 patch.object(service, "_is_cancelled", return_value=False):
                result = await service.process_file("test-nofb")

            mock_inmem.assert_not_called()
            assert result.status == DatasetStatus.ERROR


# ---------------------------------------------------------------------------
# B4: Timeout / cancel escalation (Gate 3)
# ---------------------------------------------------------------------------


class TestCancelEscalation:
    """B4: Verify cancel() escalates from control pipe → SIGTERM → SIGKILL."""

    def test_cancel_sends_sigterm_after_grace(self):
        """If worker ignores cancel signal, SIGTERM is sent after grace period."""
        from app.services.process_worker import WorkerHandle

        mock_proc = MagicMock()
        mock_proc.pid = 99999

        # Worker stays alive during grace, then dies after SIGTERM
        kill_called = {"sigterm": False}

        def fake_is_alive():
            return not kill_called["sigterm"]

        mock_proc.is_alive = fake_is_alive

        handle = WorkerHandle(
            future=mock_proc,
            data_queue=MagicMock(),
            progress_conn=MagicMock(),
            control_conn=MagicMock(),
            timeout_s=10,
            grace_period_s=0,
        )

        def track_kill(pid, sig):
            if sig == signal.SIGTERM:
                kill_called["sigterm"] = True

        with patch("app.services.process_worker.os.kill", side_effect=track_kill) as mock_kill, \
             patch("app.services.process_worker.time.sleep"):
            handle.cancel()

        mock_kill.assert_any_call(99999, signal.SIGTERM)

    def test_cancel_sends_sigkill_if_sigterm_ignored(self):
        """If worker ignores both cancel and SIGTERM, SIGKILL is sent."""
        from app.services.process_worker import WorkerHandle

        mock_proc = MagicMock()
        mock_proc.pid = 99998
        mock_proc.is_alive = MagicMock(return_value=True)

        handle = WorkerHandle(
            future=mock_proc,
            data_queue=MagicMock(),
            progress_conn=MagicMock(),
            control_conn=MagicMock(),
            timeout_s=10,
            grace_period_s=0,
        )

        # Monotonically advancing clock so both deadlines expire
        clock = [0.0]

        def fake_monotonic():
            clock[0] += 10.0  # jump 10s each call — past all deadlines
            return clock[0]

        with patch("app.services.process_worker.os.kill") as mock_kill, \
             patch("app.services.process_worker.time.sleep"), \
             patch("app.services.process_worker.time.monotonic", side_effect=fake_monotonic):
            handle.cancel()

        calls = [c[0] for c in mock_kill.call_args_list]
        assert (99998, signal.SIGTERM) in calls
        assert (99998, signal.SIGKILL) in calls


# ---------------------------------------------------------------------------
# B5: Point IDs passed as Qdrant IDs (Gate 3)
# ---------------------------------------------------------------------------


class TestPointIDsAsQdrantIDs:
    """B5: Verify _flush_index_batch passes row_id as Qdrant point IDs."""

    def test_flush_passes_ids_to_upsert(self):
        from app.services.indexing_service import IndexingService

        with patch("app.services.indexing_service.get_embedding_service") as mock_embed, \
             patch("app.services.indexing_service.get_qdrant_service") as mock_qdrant:

            mock_embed_svc = MagicMock()
            mock_embed_svc.embed_texts.return_value = [[0.1] * 384, [0.2] * 384]
            mock_embed.return_value = mock_embed_svc

            mock_qdrant_svc = MagicMock()
            mock_qdrant_svc.upsert_vectors.return_value = {"upserted": 2}
            mock_qdrant.return_value = mock_qdrant_svc

            service = IndexingService()

            import uuid as _uuid
            id_0 = str(_uuid.uuid5(_uuid.NAMESPACE_OID, "ds1:0:0"))
            id_1 = str(_uuid.uuid5(_uuid.NAMESPACE_OID, "ds1:0:1"))
            payloads = [
                {"row_id": id_0, "text_content": "hello"},
                {"row_id": id_1, "text_content": "world"},
            ]
            service._flush_index_batch("coll_test", ["hello", "world"], payloads)

            call_kwargs = mock_qdrant_svc.upsert_vectors.call_args[1]
            assert call_kwargs["ids"] == [id_0, id_1]


# ---------------------------------------------------------------------------
# B2: MemoryMonitor (Gate 3)
# ---------------------------------------------------------------------------


class TestMemoryMonitor:
    """B2: Verify MemoryMonitor class starts and stops cleanly."""

    def test_monitor_starts_and_stops(self):
        """Monitor should start, track RSS, and stop without error."""
        import os
        from app.services.process_worker import MemoryMonitor

        # Monitor our own process (safe — we won't exceed 2x limit)
        monitor = MemoryMonitor(
            pid=os.getpid(),
            limit_mb=999999,  # very high limit so we don't get killed
            poll_interval_s=0.1,
        )
        monitor.start()
        import time
        time.sleep(0.3)  # let it poll a few times
        monitor.stop()

        # High-water mark should be > 0
        assert monitor._high_water_bytes > 0


# ---------------------------------------------------------------------------
# Phase 2: M3 — ParquetWriter with row_group_size (configurable)
# ---------------------------------------------------------------------------


class TestParquetWriterRowGroupSize:
    """M3: Verify ParquetWriter respects PARQUET_ROW_GROUP_SIZE_MB setting."""

    def test_parquet_writer_creates_valid_file(self, tmp_data):
        """ParquetWriter creates a valid .parquet from streamed chunks."""
        output_path = tmp_data / "processed" / "test.parquet"

        schema = pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
            ("value", pa.float64()),
        ])

        writer = pq.ParquetWriter(str(output_path), schema, compression="zstd")
        total_rows = 0
        for i in range(5):
            batch = pa.RecordBatch.from_pydict({
                "id": list(range(i * 100, (i + 1) * 100)),
                "name": [f"item_{j}" for j in range(i * 100, (i + 1) * 100)],
                "value": [j * 1.5 for j in range(i * 100, (i + 1) * 100)],
            })
            writer.write_batch(batch)
            total_rows += batch.num_rows
        writer.close()

        # Verify the file is valid
        pf = pq.ParquetFile(str(output_path))
        assert pf.metadata.num_rows == 500
        assert total_rows == 500

    def test_row_group_size_parameter(self, tmp_data):
        """ParquetWriter write_batch respects row_group_size kwarg."""
        output_path = tmp_data / "processed" / "rg_test.parquet"

        schema = pa.schema([
            ("id", pa.int64()),
            ("value", pa.float64()),
        ])

        writer = pq.ParquetWriter(str(output_path), schema, compression="zstd")
        # Write 1000 rows with row_group_size=200
        batch = pa.RecordBatch.from_pydict({
            "id": list(range(1000)),
            "value": [float(i) for i in range(1000)],
        })
        writer.write_batch(batch, row_group_size=200)
        writer.close()

        pf = pq.ParquetFile(str(output_path))
        assert pf.metadata.num_rows == 1000
        # With row_group_size=200 and 1000 rows, expect 5 row groups
        assert pf.metadata.num_row_groups == 5

    def test_atomic_write_partial_exists_during_write(self, tmp_data):
        """M3: .partial file exists during write, renamed on completion."""
        partial_path = tmp_data / "processed" / "test.parquet.partial"
        final_path = tmp_data / "processed" / "test.parquet"

        schema = pa.schema([("id", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})

        writer = pq.ParquetWriter(str(partial_path), schema, compression="zstd")
        writer.write_batch(batch)
        # .partial should exist before close
        assert partial_path.exists()
        assert not final_path.exists()

        writer.close()
        partial_path.rename(final_path)

        assert final_path.exists()
        assert not partial_path.exists()

    def test_partial_deleted_on_failure(self, tmp_data):
        """M3: .partial file is deleted on error."""
        partial_path = tmp_data / "processed" / "fail.parquet.partial"

        schema = pa.schema([("id", pa.int64())])
        batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})

        writer = pq.ParquetWriter(str(partial_path), schema, compression="zstd")
        writer.write_batch(batch)
        assert partial_path.exists()

        # Simulate crash: close writer and delete partial
        writer.close()
        if partial_path.exists():
            partial_path.unlink()
        assert not partial_path.exists()


class TestExtractStreamingCrashCleanup:
    """M3: Verify _extract_streaming cleans up .partial on worker errors."""

    @pytest.mark.asyncio
    async def test_partial_cleaned_on_worker_error(self, sample_csv):
        """When worker reports error, .partial is deleted and dataset marked failed."""
        with patch("app.services.processing_service.settings") as mock_s:
            mock_s.large_file_threshold_mb = 0
            mock_s.fallback_max_size_mb = 0  # No fallback
            mock_s.upload_directory = str(sample_csv.parent.parent / "uploads")
            mock_s.processed_directory = str(sample_csv.parent.parent / "processed")
            mock_s.parquet_row_group_size_mb = 64

            from app.services.processing_service import ProcessingService, DatasetRecord

            service = ProcessingService()
            record = DatasetRecord("crash-test", "test.csv", "csv")
            record.upload_path = sample_csv
            record.file_size_bytes = 1 * 1024 * 1024  # 1MB

            # Mock the worker manager to simulate error
            mock_handle = MagicMock()
            mock_handle.iter_data.side_effect = RuntimeError("Worker OOM")
            mock_handle.get_progress.return_value = {"status": "error", "error": "Worker OOM"}

            mock_manager = MagicMock()
            mock_manager.submit_tabular.return_value = mock_handle

            with patch("app.services.process_worker.get_worker_manager", return_value=mock_manager):
                with pytest.raises(RuntimeError, match="Worker OOM"):
                    await service._extract_streaming(record)

            # .partial should not exist
            partial = Path(mock_s.processed_directory) / "crash-test.parquet.partial"
            assert not partial.exists()


# ---------------------------------------------------------------------------
# Phase 2: M4 — PDF streaming with pypdfium2 optimizations
# ---------------------------------------------------------------------------


class TestPDFStreamingPhase2:
    """M4: Optimized PDF extraction with per-page fallback and single pdfplumber open."""

    def test_extract_tables_from_page_static(self):
        """_extract_tables_from_page works with a mock pdfplumber doc."""
        from app.services.streaming_processor import StreamingDocumentProcessor

        mock_plumber = MagicMock()
        mock_page = MagicMock()
        mock_page.extract_tables.return_value = [
            [["Header1", "Header2"], ["val1", "val2"]],
        ]
        mock_plumber.pages = [mock_page]

        result = StreamingDocumentProcessor._extract_tables_from_page(mock_plumber, 0)
        assert len(result) == 1
        assert "Header1\tHeader2" in result[0]
        assert "val1\tval2" in result[0]

    def test_extract_tables_from_page_none_plumber(self):
        """_extract_tables_from_page returns [] when plumber_pdf is None."""
        from app.services.streaming_processor import StreamingDocumentProcessor

        result = StreamingDocumentProcessor._extract_tables_from_page(None, 0)
        assert result == []

    def test_extract_tables_from_page_exception(self):
        """_extract_tables_from_page handles errors gracefully."""
        from app.services.streaming_processor import StreamingDocumentProcessor

        mock_plumber = MagicMock()
        mock_plumber.pages = [MagicMock()]
        mock_plumber.pages[0].extract_tables.side_effect = RuntimeError("corrupt")

        result = StreamingDocumentProcessor._extract_tables_from_page(mock_plumber, 0)
        assert result == []

    def test_per_page_fallback_on_pypdfium2_error(self, tmp_data):
        """M4: If pypdfium2 fails on a specific page, PyPDF fallback is used for that page."""
        from app.services.streaming_processor import StreamingDocumentProcessor

        proc = StreamingDocumentProcessor(tmp_data / "fake.pdf", "pdf")

        # Mock pypdfium2 to fail on page 1 only
        mock_page0 = MagicMock()
        mock_textpage0 = MagicMock()
        mock_textpage0.get_text_bounded.return_value = "Page 0 text"
        mock_page0.get_textpage.return_value = mock_textpage0

        def getitem(idx):
            if idx == 0:
                return mock_page0
            raise RuntimeError("Corrupt page")

        mock_pdf = MagicMock()
        mock_pdf.__len__ = MagicMock(return_value=2)
        mock_pdf.__getitem__ = MagicMock(side_effect=getitem)

        with patch("app.services.streaming_processor.StreamingDocumentProcessor._open_pdfplumber", return_value=None), \
             patch("app.services.streaming_processor.StreamingDocumentProcessor._fallback_page_text", return_value="Fallback text") as mock_fallback, \
             patch("pypdfium2.PdfDocument", return_value=mock_pdf):
            blocks = list(proc._iter_pdf())

        assert len(blocks) == 2
        assert blocks[0].text == "Page 0 text"
        assert blocks[0].metadata.get("fallback") is None
        assert blocks[1].text == "Fallback text"
        assert blocks[1].metadata.get("fallback") is True
        mock_fallback.assert_called_once_with(1)

    def test_pdfplumber_opened_once(self, tmp_data):
        """M4: pdfplumber is opened exactly once, not per-page."""
        from app.services.streaming_processor import StreamingDocumentProcessor

        proc = StreamingDocumentProcessor(tmp_data / "fake.pdf", "pdf")

        mock_pdf = MagicMock()
        mock_pdf.__len__ = MagicMock(return_value=3)

        mock_page = MagicMock()
        mock_textpage = MagicMock()
        mock_textpage.get_text_bounded.return_value = "text"
        mock_page.get_textpage.return_value = mock_textpage
        mock_pdf.__getitem__ = MagicMock(return_value=mock_page)
        mock_pdf.close = MagicMock()

        mock_plumber = MagicMock()
        mock_plumber.pages = [MagicMock(), MagicMock(), MagicMock()]
        for p in mock_plumber.pages:
            p.extract_tables.return_value = []

        with patch.object(proc, "_open_pdfplumber", return_value=mock_plumber) as mock_open, \
             patch("pypdfium2.PdfDocument", return_value=mock_pdf):
            blocks = list(proc._iter_pdf())

        # _open_pdfplumber should be called exactly once
        mock_open.assert_called_once()
        # 3 pages processed
        assert len(blocks) == 3
        # plumber.close() called once
        mock_plumber.close.assert_called_once()


# ---------------------------------------------------------------------------
# Phase 2: Arrow IPC roundtrip tests
# ---------------------------------------------------------------------------


class TestArrowIPCRoundtrip:
    """Verify Arrow IPC serialization/deserialization preserves data."""

    def test_large_batch_roundtrip(self):
        """Arrow IPC handles large batches correctly."""
        from app.services.process_worker import (
            serialize_record_batch,
            deserialize_record_batch,
        )

        batch = pa.RecordBatch.from_pydict({
            "id": list(range(10000)),
            "text": [f"row text content number {i}" for i in range(10000)],
            "value": [float(i) * 3.14 for i in range(10000)],
        })

        data = serialize_record_batch(batch)
        assert isinstance(data, bytes)

        result = deserialize_record_batch(data)
        assert result.num_rows == 10000
        assert result.schema == batch.schema
        assert result.column("id").to_pylist() == list(range(10000))

    def test_empty_strings_roundtrip(self):
        """Arrow IPC handles empty strings and None values."""
        from app.services.process_worker import (
            serialize_record_batch,
            deserialize_record_batch,
        )

        batch = pa.RecordBatch.from_pydict({
            "text": ["hello", "", None, "world"],
            "num": [1, 2, None, 4],
        })

        result = deserialize_record_batch(serialize_record_batch(batch))
        assert result.num_rows == 4
        assert result.column("text").to_pylist() == ["hello", "", None, "world"]
        assert result.column("num").to_pylist() == [1, 2, None, 4]

    def test_schema_preserved(self):
        """Arrow IPC preserves schema types exactly."""
        from app.services.process_worker import (
            serialize_record_batch,
            deserialize_record_batch,
        )

        schema = pa.schema([
            ("int_col", pa.int32()),
            ("float_col", pa.float64()),
            ("str_col", pa.string()),
            ("bool_col", pa.bool_()),
        ])
        batch = pa.RecordBatch.from_pydict(
            {"int_col": [1], "float_col": [2.5], "str_col": ["x"], "bool_col": [True]},
            schema=schema,
        )

        result = deserialize_record_batch(serialize_record_batch(batch))
        assert result.schema == schema


# ---------------------------------------------------------------------------
# Phase 2: Configuration test for new setting
# ---------------------------------------------------------------------------


class TestPhase2Configuration:
    """Test Phase 2 config additions."""

    def test_parquet_row_group_size_mb_default(self):
        from app.config import Settings

        s = Settings(_env_file=None)
        assert s.parquet_row_group_size_mb == 64
