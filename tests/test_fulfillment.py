"""
BQ-D1: Fulfillment Listener Tests
==================================

Tests the vectorAIz fulfillment handler (§7, items 1-7):
  1. Valid deliver → metadata → chunks → complete
  2. Unknown listing_id → DATASET_NOT_FOUND error
  3. File not found on disk → FILE_READ_ERROR
  4. File > 500MB → error (size cap)
  5. Chunks stream without full-file memory buffering
  6. Multiple queued fulfillments process independently
  7. Fulfillment log records all fields
"""

import asyncio
import base64
import hashlib
import json
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sqlmodel import select

from app.core.database import get_session_context
from app.models.dataset import DatasetRecord
from app.models.fulfillment import FulfillmentLog
from app.services.fulfillment_service import (
    CHUNK_SIZE,
    FulfillmentService,
)
from app.services.trust_channel_client import TrustChannelClient


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture
def tmp_data_dir(tmp_path):
    """Create a temp data directory with uploads/ and processed/ subdirs."""
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    processed = tmp_path / "processed"
    processed.mkdir()
    return tmp_path


@pytest.fixture
def sample_file(tmp_data_dir):
    """Create a small sample dataset file (128KB — 2 chunks)."""
    file_path = tmp_data_dir / "uploads" / "ds_test123_data.csv"
    content = b"x" * (CHUNK_SIZE * 2)  # exactly 2 chunks
    file_path.write_bytes(content)
    return str(file_path)


@pytest.fixture
def sample_dataset(sample_file, tmp_data_dir):
    """Create a DatasetRecord in the DB with a listing_id."""
    dataset = DatasetRecord(
        id="test123",
        original_filename="data.csv",
        storage_filename="ds_test123_data.csv",
        file_type="csv",
        file_size_bytes=CHUNK_SIZE * 2,
        status="ready",
        listing_id="listing-abc-123",
    )
    with get_session_context() as session:
        session.merge(dataset)
        session.commit()
    return dataset


@pytest.fixture
def mock_client():
    """Create a mock TrustChannelClient."""
    client = MagicMock(spec=TrustChannelClient)
    client.send_action = AsyncMock()
    client.wait_for_action = AsyncMock(return_value={
        "action": "vai.fulfillment.ack",
        "transfer_id": "mock",
        "acked_through_index": 3,
        "status": "continue",
    })
    client.register_handler = MagicMock()
    return client


@pytest.fixture
def service(mock_client):
    """Create a FulfillmentService with a mock client."""
    svc = FulfillmentService(mock_client)
    return svc


def _make_deliver_message(listing_id: str = "listing-abc-123") -> dict:
    """Build a vai.fulfillment.deliver message."""
    return {
        "action": "vai.fulfillment.deliver",
        "message_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "parameters": {
            "order_id": str(uuid.uuid4()),
            "listing_id": listing_id,
            "buyer_id": str(uuid.uuid4()),
            "request_id": str(uuid.uuid4()),
        },
    }


# ===========================================================================
# Test 1: Valid deliver → metadata → chunks → complete
# ===========================================================================

class TestValidDeliverFlow:
    """§7.1: Valid deliver request → metadata sent → chunks streamed → complete sent."""

    @pytest.mark.asyncio
    async def test_full_flow_sends_metadata_chunks_complete(
        self, service, mock_client, sample_dataset, sample_file, tmp_data_dir
    ):
        """Happy path: deliver → metadata → chunks → complete."""
        message = _make_deliver_message("listing-abc-123")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        # Collect all sent actions
        calls = mock_client.send_action.call_args_list
        actions = [c[0][0]["action"] for c in calls]

        # Should send: metadata, chunk0, chunk1, complete
        assert actions[0] == "vai.fulfillment.metadata"
        assert actions[-1] == "vai.fulfillment.complete"

        # Verify metadata content
        metadata_msg = calls[0][0][0]
        params = metadata_msg["parameters"]
        assert params["total_bytes"] == CHUNK_SIZE * 2
        assert params["total_chunks"] == 2
        assert params["chunk_size"] == CHUNK_SIZE
        assert params["hash_algorithm"] == "sha256"
        assert len(params["sha256_hash"]) == 64  # hex SHA-256

        # Verify complete content
        complete_msg = calls[-1][0][0]
        assert complete_msg["parameters"]["status"] == "fulfilled"
        assert complete_msg["parameters"]["chunk_count"] == 2

    @pytest.mark.asyncio
    async def test_chunks_have_correct_fields(
        self, service, mock_client, sample_dataset, sample_file, tmp_data_dir
    ):
        """Each chunk message has chunk_index, byte_offset, payload_length, chunk_sha256, payload."""
        message = _make_deliver_message("listing-abc-123")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        calls = mock_client.send_action.call_args_list
        chunk_msgs = [c[0][0] for c in calls if c[0][0]["action"] == "vai.fulfillment.chunk"]

        assert len(chunk_msgs) == 2

        # First chunk
        c0 = chunk_msgs[0]
        assert c0["chunk_index"] == 0
        assert c0["byte_offset"] == 0
        assert c0["payload_length"] == CHUNK_SIZE
        assert len(c0["chunk_sha256"]) == 64
        # Verify payload is valid base64
        raw = base64.b64decode(c0["payload"])
        assert len(raw) == CHUNK_SIZE

        # Second chunk
        c1 = chunk_msgs[1]
        assert c1["chunk_index"] == 1
        assert c1["byte_offset"] == CHUNK_SIZE

    @pytest.mark.asyncio
    async def test_chunk_sha256_matches_payload(
        self, service, mock_client, sample_dataset, sample_file, tmp_data_dir
    ):
        """Per-chunk SHA-256 matches the actual chunk payload."""
        message = _make_deliver_message("listing-abc-123")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        calls = mock_client.send_action.call_args_list
        chunk_msgs = [c[0][0] for c in calls if c[0][0]["action"] == "vai.fulfillment.chunk"]

        for chunk in chunk_msgs:
            raw = base64.b64decode(chunk["payload"])
            expected_hash = hashlib.sha256(raw).hexdigest()
            assert chunk["chunk_sha256"] == expected_hash


# ===========================================================================
# Test 2: Unknown listing_id → DATASET_NOT_FOUND
# ===========================================================================

class TestUnknownListingId:
    """§7.2: Unknown listing_id → error response with DATASET_NOT_FOUND."""

    @pytest.mark.asyncio
    async def test_unknown_listing_sends_error(self, service, mock_client, tmp_data_dir):
        """Deliver with non-existent listing_id sends DATASET_NOT_FOUND error."""
        message = _make_deliver_message("nonexistent-listing-999")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        # Should have sent an error
        calls = mock_client.send_action.call_args_list
        assert len(calls) == 1
        error_msg = calls[0][0][0]
        assert error_msg["action"] == "vai.fulfillment.error"
        assert error_msg["parameters"]["error_code"] == "DATASET_NOT_FOUND"


# ===========================================================================
# Test 3: File not found on disk → FILE_READ_ERROR
# ===========================================================================

class TestFileNotFoundOnDisk:
    """§7.3: File not found on disk → error response with FILE_READ_ERROR."""

    @pytest.mark.asyncio
    async def test_missing_file_sends_error(self, service, mock_client, tmp_data_dir):
        """Dataset record exists but file is missing from disk."""
        # Create a dataset record pointing to a non-existent file
        dataset = DatasetRecord(
            id="ghost-dataset",
            original_filename="gone.csv",
            storage_filename="ds_ghost_gone.csv",
            file_type="csv",
            file_size_bytes=1000,
            status="ready",
            listing_id="listing-ghost-456",
        )
        with get_session_context() as session:
            session.merge(dataset)
            session.commit()

        message = _make_deliver_message("listing-ghost-456")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        calls = mock_client.send_action.call_args_list
        assert len(calls) == 1
        error_msg = calls[0][0][0]
        assert error_msg["action"] == "vai.fulfillment.error"
        assert error_msg["parameters"]["error_code"] == "FILE_READ_ERROR"


# ===========================================================================
# Test 4: File > 500MB → error (size cap)
# ===========================================================================

class TestFileSizeCap:
    """§7.4: File > 500MB → error response (size cap enforced before streaming)."""

    @pytest.mark.asyncio
    async def test_oversized_file_sends_error(self, service, mock_client, tmp_data_dir):
        """File exceeding 500MB should be rejected before streaming."""
        # Create a dataset record and a file that reports as oversized
        big_file = tmp_data_dir / "uploads" / "ds_big_huge.csv"
        big_file.write_bytes(b"x")  # tiny real file

        dataset = DatasetRecord(
            id="big-dataset",
            original_filename="huge.csv",
            storage_filename="ds_big_huge.csv",
            file_type="csv",
            file_size_bytes=600 * 1024 * 1024,
            status="ready",
            listing_id="listing-big-789",
        )
        with get_session_context() as session:
            session.merge(dataset)
            session.commit()

        message = _make_deliver_message("listing-big-789")

        # Mock os.path.getsize to return >500MB
        with patch("app.services.fulfillment_service.settings") as mock_settings, \
             patch("app.services.fulfillment_service.os.path.getsize", return_value=600 * 1024 * 1024):
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        calls = mock_client.send_action.call_args_list
        assert len(calls) == 1
        error_msg = calls[0][0][0]
        assert error_msg["action"] == "vai.fulfillment.error"
        assert error_msg["parameters"]["error_code"] == "FILE_TOO_LARGE"


# ===========================================================================
# Test 5: Chunks stream without full-file memory buffering
# ===========================================================================

class TestStreamingMemory:
    """§7.5: Chunks stream without loading full file into memory."""

    @pytest.mark.asyncio
    async def test_file_opened_once_and_read_in_chunks(
        self, service, mock_client, sample_dataset, tmp_data_dir
    ):
        """
        Verify that the file is read in CHUNK_SIZE increments,
        not loaded entirely into memory.
        """
        # Create a file that is 4 chunks (256KB)
        file_path = tmp_data_dir / "uploads" / "ds_test123_data.csv"
        content = b"A" * (CHUNK_SIZE * 4)
        file_path.write_bytes(content)

        message = _make_deliver_message("listing-abc-123")

        reads = []
        original_open = open

        class TrackingFile:
            """Wrapper to track read sizes."""
            def __init__(self, f):
                self._f = f

            def read(self, size=-1):
                data = self._f.read(size)
                if data:
                    reads.append(len(data))
                return data

            def seek(self, offset):
                return self._f.seek(offset)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self._f.close()

        def patched_open(path, mode="r", **kwargs):
            f = original_open(path, mode, **kwargs)
            if "b" in mode and str(file_path) in str(path):
                return TrackingFile(f)
            return f

        with patch("app.services.fulfillment_service.settings") as mock_settings, \
             patch("builtins.open", side_effect=patched_open):
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        # SHA-256 computation also reads in 64KB chunks, so we expect reads
        # from both hash computation and chunk streaming.
        # The key assertion: no single read was larger than CHUNK_SIZE.
        assert all(r <= CHUNK_SIZE for r in reads), (
            f"Expected all reads ≤ {CHUNK_SIZE}, got max={max(reads)}"
        )


# ===========================================================================
# Test 6: Multiple queued fulfillments process independently
# ===========================================================================

class TestQueuedFulfillments:
    """§7.6: Multiple queued fulfillments process independently."""

    @pytest.mark.asyncio
    async def test_failure_doesnt_block_next(self, service, mock_client, tmp_data_dir):
        """First fulfillment fails (unknown listing), second succeeds."""
        # Set up a valid dataset for the second request
        file_path = tmp_data_dir / "uploads" / "ds_valid_ok.csv"
        file_path.write_bytes(b"Y" * CHUNK_SIZE)

        dataset = DatasetRecord(
            id="valid-ds",
            original_filename="ok.csv",
            storage_filename="ds_valid_ok.csv",
            file_type="csv",
            file_size_bytes=CHUNK_SIZE,
            status="ready",
            listing_id="listing-valid-001",
        )
        with get_session_context() as session:
            session.merge(dataset)
            session.commit()

        # Two deliver messages: first will fail, second will succeed
        msg_fail = _make_deliver_message("listing-nonexistent")
        msg_ok = _make_deliver_message("listing-valid-001")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            # Process both sequentially (simulating queue behavior)
            await service._handle_deliver(msg_fail)
            mock_client.send_action.reset_mock()
            await service._handle_deliver(msg_ok)

        # Second request should have succeeded (metadata + chunk + complete)
        calls = mock_client.send_action.call_args_list
        actions = [c[0][0]["action"] for c in calls]
        assert "vai.fulfillment.metadata" in actions
        assert "vai.fulfillment.complete" in actions

    @pytest.mark.asyncio
    async def test_each_gets_unique_transfer_id(self, service, mock_client, tmp_data_dir):
        """Each fulfillment gets its own transfer_id."""
        file_path = tmp_data_dir / "uploads" / "ds_multi_multi.csv"
        file_path.write_bytes(b"Z" * CHUNK_SIZE)

        dataset = DatasetRecord(
            id="multi-ds",
            original_filename="multi.csv",
            storage_filename="ds_multi_multi.csv",
            file_type="csv",
            file_size_bytes=CHUNK_SIZE,
            status="ready",
            listing_id="listing-multi-002",
        )
        with get_session_context() as session:
            session.merge(dataset)
            session.commit()

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(_make_deliver_message("listing-multi-002"))
            await service._handle_deliver(_make_deliver_message("listing-multi-002"))

        # Check that two different transfer_ids were used in metadata messages
        calls = mock_client.send_action.call_args_list
        metadata_msgs = [c[0][0] for c in calls if c[0][0]["action"] == "vai.fulfillment.metadata"]
        transfer_ids = {m["transfer_id"] for m in metadata_msgs}
        assert len(transfer_ids) == 2, "Each fulfillment should have a unique transfer_id"


# ===========================================================================
# Test 7: Fulfillment log records all fields
# ===========================================================================

class TestFulfillmentLog:
    """§7.7: Fulfillment log records all fields correctly."""

    @pytest.mark.asyncio
    async def test_successful_fulfillment_logged(
        self, service, mock_client, sample_dataset, sample_file, tmp_data_dir
    ):
        """Successful fulfillment creates a log entry with all fields."""
        message = _make_deliver_message("listing-abc-123")
        params = message["parameters"]

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        # Check the log entry
        with get_session_context() as session:
            logs = session.exec(
                select(FulfillmentLog).where(
                    FulfillmentLog.order_id == params["order_id"]
                )
            ).all()
            assert len(logs) == 1

            log = logs[0]
            assert log.transfer_id is not None
            assert len(log.transfer_id) == 36  # UUID format
            assert log.order_id == params["order_id"]
            assert log.listing_id == params["listing_id"]
            assert log.request_id == params["request_id"]
            assert log.status == "completed"
            assert log.started_at is not None
            assert log.completed_at is not None
            assert log.file_size_bytes == CHUNK_SIZE * 2
            assert log.chunks_sent == 2
            assert log.error_code is None
            assert log.error_message is None

    @pytest.mark.asyncio
    async def test_failed_fulfillment_logged(self, service, mock_client, tmp_data_dir):
        """Failed fulfillment logs error_code and error_message."""
        message = _make_deliver_message("listing-not-real")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        with get_session_context() as session:
            logs = session.exec(
                select(FulfillmentLog).where(
                    FulfillmentLog.order_id == message["parameters"]["order_id"]
                )
            ).all()
            assert len(logs) == 1

            log = logs[0]
            assert log.status == "failed"
            assert log.error_code == "DATASET_NOT_FOUND"
            assert log.error_message is not None
            assert log.completed_at is not None

    @pytest.mark.asyncio
    async def test_log_excludes_sensitive_data(
        self, service, mock_client, sample_dataset, sample_file, tmp_data_dir
    ):
        """Log entries must NOT contain tokens, auth headers, chunk payloads, or buyer PII."""
        message = _make_deliver_message("listing-abc-123")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        with get_session_context() as session:
            log = session.exec(select(FulfillmentLog)).first()
            assert log is not None

            # Serialize all fields and check none contain buyer_id
            buyer_id = message["parameters"]["buyer_id"]
            log_data = {
                "id": log.id,
                "transfer_id": log.transfer_id,
                "order_id": log.order_id,
                "listing_id": log.listing_id,
                "request_id": log.request_id,
                "status": log.status,
                "error_code": log.error_code,
                "error_message": log.error_message,
            }
            serialized = json.dumps(log_data)
            assert buyer_id not in serialized, "buyer_id (PII) must not appear in log"


# ===========================================================================
# Additional edge case tests
# ===========================================================================

class TestACKBackpressure:
    """Verify windowed ACK behavior."""

    @pytest.mark.asyncio
    async def test_ack_requested_after_window(
        self, service, mock_client, tmp_data_dir
    ):
        """When file has more chunks than window size, ACK is awaited."""
        # Create a file of 6 chunks — should trigger ACK wait after first 4
        file_path = tmp_data_dir / "uploads" / "ds_ack_test_ackfile.csv"
        file_path.write_bytes(b"B" * (CHUNK_SIZE * 6))

        dataset = DatasetRecord(
            id="ack-test",
            original_filename="ackfile.csv",
            storage_filename="ds_ack_test_ackfile.csv",
            file_type="csv",
            file_size_bytes=CHUNK_SIZE * 6,
            status="ready",
            listing_id="listing-ack-test",
        )
        with get_session_context() as session:
            session.merge(dataset)
            session.commit()

        message = _make_deliver_message("listing-ack-test")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        # wait_for_action should have been called for ACK
        assert mock_client.wait_for_action.call_count >= 1
        # Verify it was called with the right action
        first_call = mock_client.wait_for_action.call_args_list[0]
        assert first_call[0][0] == "vai.fulfillment.ack"


class TestTransferIdGeneration:
    """Transfer IDs are valid UUID4s."""

    @pytest.mark.asyncio
    async def test_transfer_id_is_uuid4(
        self, service, mock_client, sample_dataset, sample_file, tmp_data_dir
    ):
        message = _make_deliver_message("listing-abc-123")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        calls = mock_client.send_action.call_args_list
        metadata_msg = calls[0][0][0]
        transfer_id = metadata_msg["transfer_id"]
        # Should be a valid UUID
        parsed = uuid.UUID(transfer_id)
        assert parsed.version == 4


# ===========================================================================
# Test 8: ACK with wrong/insufficient acked_through_index
# ===========================================================================

class TestACKValidation:
    """Gate 3: ACK acked_through_index must be >= expected, else fail."""

    @pytest.mark.asyncio
    async def test_insufficient_acked_through_index_raises(
        self, service, mock_client, tmp_data_dir
    ):
        """ACK with acked_through_index < expected should raise ConnectionError."""
        # Create a file of 6 chunks — triggers ACK after first window of 4
        file_path = tmp_data_dir / "uploads" / "ds_ackval_ackval.csv"
        file_path.write_bytes(b"V" * (CHUNK_SIZE * 6))

        dataset = DatasetRecord(
            id="ackval",
            original_filename="ackval.csv",
            storage_filename="ds_ackval_ackval.csv",
            file_type="csv",
            file_size_bytes=CHUNK_SIZE * 6,
            status="ready",
            listing_id="listing-ackval",
        )
        with get_session_context() as session:
            session.merge(dataset)
            session.commit()

        # Return ACK with acked_through_index=1, but expected is 3
        mock_client.wait_for_action = AsyncMock(return_value={
            "action": "vai.fulfillment.ack",
            "transfer_id": "mock",
            "acked_through_index": 1,
            "status": "continue",
        })

        message = _make_deliver_message("listing-ackval")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            # _handle_deliver catches ConnectionError internally
            await service._handle_deliver(message)

        # Should NOT have sent vai.fulfillment.complete (transfer aborted)
        calls = mock_client.send_action.call_args_list
        actions = [c[0][0]["action"] for c in calls]
        assert "vai.fulfillment.complete" not in actions


# ===========================================================================
# Test 9: Double ACK timeout → abort
# ===========================================================================

class TestDoubleACKTimeout:
    """Gate 3: First ACK times out, retry also times out → error + abort."""

    @pytest.mark.asyncio
    async def test_double_timeout_sends_error_and_aborts(
        self, service, mock_client, tmp_data_dir
    ):
        """Both ACK attempts time out → should send error and abort."""
        file_path = tmp_data_dir / "uploads" / "ds_dblack_dblack.csv"
        file_path.write_bytes(b"T" * (CHUNK_SIZE * 6))

        dataset = DatasetRecord(
            id="dblack",
            original_filename="dblack.csv",
            storage_filename="ds_dblack_dblack.csv",
            file_type="csv",
            file_size_bytes=CHUNK_SIZE * 6,
            status="ready",
            listing_id="listing-dblack",
        )
        with get_session_context() as session:
            session.merge(dataset)
            session.commit()

        # Both wait_for_action calls raise TimeoutError
        mock_client.wait_for_action = AsyncMock(side_effect=TimeoutError("no ack"))

        message = _make_deliver_message("listing-dblack")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        calls = mock_client.send_action.call_args_list
        actions = [c[0][0]["action"] for c in calls]

        # Should have sent metadata, chunks, then error (no complete)
        assert "vai.fulfillment.metadata" in actions
        assert "vai.fulfillment.error" in actions
        assert "vai.fulfillment.complete" not in actions

        # Verify error details
        error_msg = [c[0][0] for c in calls if c[0][0]["action"] == "vai.fulfillment.error"][0]
        assert error_msg["parameters"]["error_code"] == "TRANSFER_ABORTED"

        # Verify chunks 0-3 were sent TWICE (original + resend before second ACK attempt)
        chunk_msgs = [c[0][0] for c in calls if c[0][0]["action"] == "vai.fulfillment.chunk"]
        chunk_indices = [m["chunk_index"] for m in chunk_msgs]
        # Original window: [0, 1, 2, 3], resend: [0, 1, 2, 3] → 8 chunk messages
        assert chunk_indices == [0, 1, 2, 3, 0, 1, 2, 3], (
            f"Expected chunks 0-3 sent twice (original + resend), got {chunk_indices}"
        )

        # Verify log entry shows timed_out
        with get_session_context() as session:
            log = session.exec(
                select(FulfillmentLog).where(
                    FulfillmentLog.order_id == message["parameters"]["order_id"]
                )
            ).first()
            assert log is not None
            assert log.status == "timed_out"


# ===========================================================================
# Test 9b: ACK timeout → resend window → ACK succeeds → transfer completes
# ===========================================================================

class TestACKWindowResend:
    """Gate 3: First ACK times out → window re-sent → second ACK succeeds."""

    @pytest.mark.asyncio
    async def test_resend_on_first_timeout_then_completes(
        self, service, mock_client, tmp_data_dir
    ):
        """First ACK wait times out, same window is re-sent, second ACK succeeds."""
        file_path = tmp_data_dir / "uploads" / "ds_resend_resend.csv"
        file_path.write_bytes(b"W" * (CHUNK_SIZE * 6))

        dataset = DatasetRecord(
            id="resend",
            original_filename="resend.csv",
            storage_filename="ds_resend_resend.csv",
            file_type="csv",
            file_size_bytes=CHUNK_SIZE * 6,
            status="ready",
            listing_id="listing-resend",
        )
        with get_session_context() as session:
            session.merge(dataset)
            session.commit()

        # First wait_for_action → timeout, second → ACK success
        mock_client.wait_for_action = AsyncMock(side_effect=[
            TimeoutError("no ack"),
            {
                "action": "vai.fulfillment.ack",
                "transfer_id": "mock",
                "acked_through_index": 3,
                "status": "continue",
            },
        ])

        message = _make_deliver_message("listing-resend")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        calls = mock_client.send_action.call_args_list
        actions = [c[0][0]["action"] for c in calls]

        # Transfer should complete successfully
        assert "vai.fulfillment.complete" in actions
        assert "vai.fulfillment.error" not in actions

        # Verify chunk messages: original 0-3, resend 0-3, then 4-5
        chunk_msgs = [c[0][0] for c in calls if c[0][0]["action"] == "vai.fulfillment.chunk"]
        chunk_indices = [m["chunk_index"] for m in chunk_msgs]
        assert chunk_indices == [0, 1, 2, 3, 0, 1, 2, 3, 4, 5], (
            f"Expected [0,1,2,3,0,1,2,3,4,5], got {chunk_indices}"
        )

        # Verify re-sent chunks have same byte offsets and payloads as originals
        original_window = chunk_msgs[0:4]
        resent_window = chunk_msgs[4:8]
        for orig, resent in zip(original_window, resent_window):
            assert orig["byte_offset"] == resent["byte_offset"]
            assert orig["payload"] == resent["payload"]
            assert orig["chunk_sha256"] == resent["chunk_sha256"]

        # Complete message should report all 6 chunks
        complete_msg = [c[0][0] for c in calls if c[0][0]["action"] == "vai.fulfillment.complete"][0]
        assert complete_msg["parameters"]["chunk_count"] == 6

        # Log should show completed
        with get_session_context() as session:
            log = session.exec(
                select(FulfillmentLog).where(
                    FulfillmentLog.order_id == message["parameters"]["order_id"]
                )
            ).first()
            assert log is not None
            assert log.status == "completed"

    @pytest.mark.asyncio
    async def test_double_timeout_resends_then_aborts(
        self, service, mock_client, tmp_data_dir
    ):
        """Both ACK attempts timeout → window re-sent → TRANSFER_ABORTED."""
        file_path = tmp_data_dir / "uploads" / "ds_dblresend_dblresend.csv"
        file_path.write_bytes(b"Q" * (CHUNK_SIZE * 6))

        dataset = DatasetRecord(
            id="dblresend",
            original_filename="dblresend.csv",
            storage_filename="ds_dblresend_dblresend.csv",
            file_type="csv",
            file_size_bytes=CHUNK_SIZE * 6,
            status="ready",
            listing_id="listing-dblresend",
        )
        with get_session_context() as session:
            session.merge(dataset)
            session.commit()

        mock_client.wait_for_action = AsyncMock(side_effect=TimeoutError("no ack"))

        message = _make_deliver_message("listing-dblresend")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            await service._handle_deliver(message)

        calls = mock_client.send_action.call_args_list
        actions = [c[0][0]["action"] for c in calls]

        # Transfer aborted — no complete
        assert "vai.fulfillment.error" in actions
        assert "vai.fulfillment.complete" not in actions

        # Window was sent twice (original + resend) before abort
        chunk_msgs = [c[0][0] for c in calls if c[0][0]["action"] == "vai.fulfillment.chunk"]
        chunk_indices = [m["chunk_index"] for m in chunk_msgs]
        assert chunk_indices == [0, 1, 2, 3, 0, 1, 2, 3]

        # Log shows timed_out
        with get_session_context() as session:
            log = session.exec(
                select(FulfillmentLog).where(
                    FulfillmentLog.order_id == message["parameters"]["order_id"]
                )
            ).first()
            assert log is not None
            assert log.status == "timed_out"
            assert log.error_code == "TRANSFER_ABORTED"


# ===========================================================================
# Test 10: Queue race regression — enqueue during processing
# ===========================================================================

class TestQueueRaceRegression:
    """Gate 3: Second fulfillment enqueued while first is processing completes."""

    @pytest.mark.asyncio
    async def test_enqueue_during_processing_both_complete(
        self, mock_client, tmp_data_dir
    ):
        """Enqueue second fulfillment while first is processing → both complete."""
        # Create service inside the async test so Queue binds to the right loop
        svc = FulfillmentService(mock_client)

        # Create two datasets
        for ds_id, listing in [("race-a", "listing-race-a"), ("race-b", "listing-race-b")]:
            fp = tmp_data_dir / "uploads" / f"ds_{ds_id}_{ds_id}.csv"
            fp.write_bytes(b"R" * CHUNK_SIZE)
            dataset = DatasetRecord(
                id=ds_id,
                original_filename=f"{ds_id}.csv",
                storage_filename=f"ds_{ds_id}_{ds_id}.csv",
                file_type="csv",
                file_size_bytes=CHUNK_SIZE,
                status="ready",
                listing_id=listing,
            )
            with get_session_context() as session:
                session.merge(dataset)
                session.commit()

        msg_a = _make_deliver_message("listing-race-a")
        msg_b = _make_deliver_message("listing-race-b")

        with patch("app.services.fulfillment_service.settings") as mock_settings:
            mock_settings.upload_directory = str(tmp_data_dir / "uploads")
            mock_settings.processed_directory = str(tmp_data_dir / "processed")

            # Start the permanent worker task
            svc.start()

            # Enqueue both via _on_deliver (simulating rapid arrivals)
            await svc._on_deliver(msg_a)
            await svc._on_deliver(msg_b)

            # Wait for both to be processed
            await asyncio.wait_for(svc._queue.join(), timeout=10.0)

        # Both should have completed — look for 2 complete messages
        calls = mock_client.send_action.call_args_list
        complete_msgs = [c[0][0] for c in calls if c[0][0]["action"] == "vai.fulfillment.complete"]
        assert len(complete_msgs) == 2, (
            f"Expected 2 complete messages, got {len(complete_msgs)}"
        )
