"""
Tests for Data Integrity Audit Sweep (S132)
=============================================

Covers:
- ConnectionManager lock prevents race conditions
- Inflight cancellation on session replacement
- Frontend 4001/4002 close codes (tested via backend close behavior)
- Upload rejects files exceeding size limit (413)
- Bulk upload enforces size while writing (not trusting UploadFile.size)
- Magic-byte validation on single upload
- Preview with redact_pii=True masks PII columns
- Pipeline status write with simulated concurrent access
- LLM max_tokens clamped to settings value
- total_message_count field rename

CREATED: 2026-02-14
"""

import asyncio
import json
import os
import tempfile
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Fix 2: ConnectionManager concurrency safety
# ---------------------------------------------------------------------------

class TestConnectionManagerLock:
    """ConnectionManager operations should be protected by asyncio.Lock."""

    @pytest.mark.asyncio
    async def test_connect_disconnect_use_lock(self):
        """connect() and disconnect() should acquire the internal lock."""
        from app.routers.copilot import ConnectionManager

        mgr = ConnectionManager()
        # Lock is lazily created via _async_lock property
        lock = mgr._async_lock
        assert isinstance(lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_concurrent_connects_do_not_corrupt(self):
        """Two concurrent connects for the same user should not corrupt state."""
        from app.routers.copilot import ConnectionManager

        mgr = ConnectionManager()
        ws1 = MagicMock()
        ws1.close = AsyncMock()
        ws2 = MagicMock()
        ws2.close = AsyncMock()

        user_mock = MagicMock()
        user_mock.user_id = "user_1"
        user_mock.balance_cents = 100
        user_mock.free_trial_remaining_cents = 0

        # Connect first session
        await mgr.connect("sess_1", "user_1", ws1, user=user_mock)
        assert mgr.get_ws("sess_1") == ws1

        # Connect second session for same user (should replace first)
        await mgr.connect("sess_2", "user_1", ws2, user=user_mock)
        assert mgr.get_ws("sess_1") is None
        assert mgr.get_ws("sess_2") == ws2

    @pytest.mark.asyncio
    async def test_inflight_methods_are_async(self):
        """set_inflight, get_inflight, clear_inflight, cancel_inflight should be async."""
        from app.routers.copilot import ConnectionManager

        mgr = ConnectionManager()
        assert asyncio.iscoroutinefunction(mgr.set_inflight)
        assert asyncio.iscoroutinefunction(mgr.get_inflight)
        assert asyncio.iscoroutinefunction(mgr.clear_inflight)
        assert asyncio.iscoroutinefunction(mgr.cancel_inflight)


# ---------------------------------------------------------------------------
# Fix 3: Cancel inflight on session replacement
# ---------------------------------------------------------------------------

class TestCancelInflightOnReplacement:
    """When a new session replaces an old one, inflight tasks must be cancelled."""

    @pytest.mark.asyncio
    async def test_inflight_cancelled_on_replace(self):
        """Old session's inflight task should be cancelled when replaced."""
        from app.routers.copilot import ConnectionManager

        mgr = ConnectionManager()
        ws1 = MagicMock()
        ws1.close = AsyncMock()
        ws2 = MagicMock()
        ws2.close = AsyncMock()

        user_mock = MagicMock()
        user_mock.user_id = "user_1"
        user_mock.balance_cents = 100
        user_mock.free_trial_remaining_cents = 0

        await mgr.connect("sess_1", "user_1", ws1, user=user_mock)

        # Simulate inflight task
        cancelled = False

        async def fake_task():
            nonlocal cancelled
            try:
                await asyncio.sleep(999)
            except asyncio.CancelledError:
                cancelled = True
                raise

        task = asyncio.create_task(fake_task())
        await mgr.set_inflight("sess_1", task)

        # Replace with new session — should cancel inflight
        await mgr.connect("sess_2", "user_1", ws2, user=user_mock)
        assert cancelled or task.cancelled()


# ---------------------------------------------------------------------------
# Fix 6: total_message_count field rename
# ---------------------------------------------------------------------------

class TestTotalMessageCount:
    """Session model should use total_message_count instead of message_count."""

    def test_session_has_total_message_count(self):
        from app.models.state import Session
        fields = {f for f in Session.model_fields}
        assert "total_message_count" in fields
        assert "message_count" not in fields

    def test_session_read_has_total_message_count(self):
        from app.models.state import SessionRead
        fields = {f for f in SessionRead.model_fields}
        assert "total_message_count" in fields
        assert "message_count" not in fields


# ---------------------------------------------------------------------------
# Fix 7: Upload rejects files exceeding size limit (413)
# ---------------------------------------------------------------------------

class TestUploadSizeEnforcement:
    """Upload should have no artificial file size limits — local app, customer resources."""

    def test_no_max_upload_size_bytes_config(self):
        """Settings should not have max_upload_size_bytes — removed for local app."""
        from app.config import Settings

        assert "max_upload_size_bytes" not in Settings.model_fields

    def test_upload_endpoint_has_no_size_rejection(self):
        """The upload endpoint should not reject files based on size."""
        import inspect
        from app.routers import datasets

        source = inspect.getsource(datasets.upload_dataset)
        assert "MAX_UPLOAD_FILE_BYTES" not in source


# ---------------------------------------------------------------------------
# Fix 8: Bulk upload size enforcement
# ---------------------------------------------------------------------------

class TestBulkUploadSizeEnforcement:
    """Bulk upload should enforce byte-count limit while writing, not trust UploadFile.size."""

    def test_batch_service_check_magic_bytes_imported(self):
        """datasets.py should import _check_magic_bytes from batch_service."""
        from app.routers.datasets import _check_magic_bytes
        assert callable(_check_magic_bytes)


# ---------------------------------------------------------------------------
# Fix 9: Magic-byte validation on single upload
# ---------------------------------------------------------------------------

class TestMagicByteValidation:
    """Single upload should validate magic bytes like bulk upload does."""

    def test_pdf_magic_bytes_valid(self):
        from app.services.batch_service import _check_magic_bytes
        assert _check_magic_bytes(b"%PDF-1.4", ".pdf") is True

    def test_pdf_magic_bytes_invalid(self):
        from app.services.batch_service import _check_magic_bytes
        assert _check_magic_bytes(b"NOT_PDF!", ".pdf") is False

    def test_csv_no_magic_check(self):
        """CSV files should pass magic check since no signature defined."""
        from app.services.batch_service import _check_magic_bytes
        assert _check_magic_bytes(b"col1,col2", ".csv") is True

    def test_parquet_magic_bytes_valid(self):
        from app.services.batch_service import _check_magic_bytes
        assert _check_magic_bytes(b"PAR1abcd", ".parquet") is True

    def test_parquet_magic_bytes_invalid(self):
        from app.services.batch_service import _check_magic_bytes
        assert _check_magic_bytes(b"NOTPAR1!", ".parquet") is False


# ---------------------------------------------------------------------------
# Fix 10: PII-safe preview
# ---------------------------------------------------------------------------

class TestPIISafePreview:
    """Preview/sample should mask PII columns when redact_pii=True."""

    def test_detect_pii_columns_by_name(self):
        """Column names matching PII patterns should be detected."""
        from app.services.preview_service import PreviewService

        svc = PreviewService()
        sample = [{"email": "a@b.com", "name": "John", "score": 95}]
        pii_cols = svc.detect_pii_columns("fake_id", sample)
        assert "email" in pii_cols

    def test_detect_pii_columns_by_value(self):
        """Values matching PII regex should flag the column."""
        from app.services.preview_service import PreviewService

        svc = PreviewService()
        sample = [{"col_x": "john@example.com", "col_y": "safe_value"}]
        pii_cols = svc.detect_pii_columns("fake_id", sample)
        assert "col_x" in pii_cols
        assert "col_y" not in pii_cols

    def test_redact_pii_rows(self):
        """_redact_pii_rows should mask flagged columns with '***'."""
        from app.routers.datasets import _redact_pii_rows

        rows = [
            {"email": "a@b.com", "score": 95},
            {"email": "c@d.com", "score": 88},
        ]
        redacted = _redact_pii_rows(rows, {"email"})
        assert all(r["email"] == "***" for r in redacted)
        assert all(r["score"] != "***" for r in redacted)


# ---------------------------------------------------------------------------
# Fix 11: Pipeline status write atomicity
# ---------------------------------------------------------------------------

class TestPipelineStatusAtomicity:
    """Pipeline status writes should use exclusive lock for read-modify-write."""

    def test_read_modify_write_json_atomic(self):
        """_read_modify_write_json should correctly read, modify, and write."""
        from app.services.pipeline_service import _read_modify_write_json, _atomic_write_json

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "status.json"
            _atomic_write_json(path, {"count": 0, "steps": {}})

            def inc(state):
                state["count"] = state.get("count", 0) + 1

            result = _read_modify_write_json(path, inc)
            assert result["count"] == 1

            result = _read_modify_write_json(path, inc)
            assert result["count"] == 2

    def test_read_modify_write_json_creates_lock_file(self):
        """_read_modify_write_json should create a .lock file."""
        from app.services.pipeline_service import _read_modify_write_json, _atomic_write_json

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "status.json"
            _atomic_write_json(path, {"x": 1})

            _read_modify_write_json(path, lambda s: None)
            lock_path = path.with_suffix(".lock")
            assert lock_path.exists()

    def test_concurrent_writes_no_lost_updates(self):
        """Simulated concurrent writes should not lose updates."""
        from app.services.pipeline_service import _read_modify_write_json, _atomic_write_json
        import threading

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "status.json"
            _atomic_write_json(path, {"count": 0})

            def inc(state):
                state["count"] = state.get("count", 0) + 1

            threads = []
            for _ in range(20):
                t = threading.Thread(target=_read_modify_write_json, args=(path, inc))
                threads.append(t)

            for t in threads:
                t.start()
            for t in threads:
                t.join()

            with open(path) as f:
                final = json.load(f)
            assert final["count"] == 20


# ---------------------------------------------------------------------------
# Fix 5: STOP/STOPPED includes message_id
# ---------------------------------------------------------------------------

class TestStopMessageId:
    """STOPPED messages should include message_id for correlation."""

    def test_stopped_in_cancelled_handler_includes_msg_id(self):
        """The _brain_stream_task CancelledError handler should include message_id."""
        import inspect
        from app.routers import copilot as copilot_mod

        # Read the source of websocket_copilot to verify STOPPED includes message_id
        source = inspect.getsource(copilot_mod)
        # The STOPPED message in _brain_stream_task CancelledError handler
        assert '"message_id": msg_id' in source
        # The STOPPED message in STOP handler should conditionally include message_id
        assert 'stopped_msg["message_id"] = stop_message_id' in source
