"""
Fulfillment Service
===================

Handles `vai.fulfillment.deliver` actions from ai.market's Trust Channel.
Looks up the dataset, streams it back in 64KB chunks with windowed ACK
backpressure, and tracks fulfillment state in the local fulfillment_log table.

BQ-D1: Fulfillment Listener (vectorAIz side)

Transfer protocol (§4):
  1. Receive vai.fulfillment.deliver → generate transfer_id
  2. Send vai.fulfillment.metadata (filename, content_type, total_bytes, etc.)
  3. Stream vai.fulfillment.chunk in windows of 4 (256KB) — wait for ACK
  4. If no ACK within 30s → retry once → abort if still no ACK
  5. Send vai.fulfillment.complete when done
  6. On error → send vai.fulfillment.error

Multiple queued fulfillments are processed sequentially (§6.3).
Each is independent — failure of one does not block the next.
"""

import asyncio
import base64
import hashlib
import json
import logging
import math
import mimetypes
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from sqlmodel import select

from app.config import settings
from app.core.database import get_session_context
from app.models.dataset import DatasetRecord
from app.models.fulfillment import FulfillmentLog
from app.services.trust_channel_client import TrustChannelClient, get_trust_channel_client

logger = logging.getLogger(__name__)

# Protocol constants
CHUNK_SIZE = 65536  # 64KB
WINDOW_SIZE = 4  # 4 chunks per ACK window (256KB)
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB
ACK_TIMEOUT_S = 30.0


class FulfillmentService:
    """
    Processes fulfillment deliver requests from ai.market.

    Queued fulfillments are processed one at a time to avoid
    overwhelming bandwidth (§6.3).
    """

    def __init__(self, client: TrustChannelClient) -> None:
        self._client = client
        self._queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None

    def register(self) -> None:
        """Register the deliver handler with the Trust Channel client."""
        self._client.register_handler("vai.fulfillment.deliver", self._on_deliver)

    def start(self) -> None:
        """Start the background worker task for processing fulfillments."""
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._process_queue())

    async def _on_deliver(self, message: Dict[str, Any]) -> None:
        """
        Called when a vai.fulfillment.deliver action arrives.
        Enqueues the request for sequential processing.
        """
        await self._queue.put(message)
        logger.info(
            "Fulfillment request enqueued (order_id=%s, listing_id=%s)",
            message.get("parameters", {}).get("order_id", "?"),
            message.get("parameters", {}).get("listing_id", "?"),
        )

    async def _process_queue(self) -> None:
        """Permanent worker: process fulfillment requests one at a time."""
        while True:
            message = await self._queue.get()
            try:
                await self._handle_deliver(message)
            except Exception as e:
                logger.error("Fulfillment processing failed: %s", e, exc_info=True)
            finally:
                self._queue.task_done()

    async def _handle_deliver(self, message: Dict[str, Any]) -> None:
        """
        Full fulfillment flow for a single deliver request:
        parse → lookup → validate → hash → metadata → chunks → complete.
        """
        params = message.get("parameters", {})
        order_id = params.get("order_id", "")
        listing_id = params.get("listing_id", "")
        request_id = params.get("request_id", "")
        transfer_id = str(uuid.uuid4())

        # Create fulfillment log entry
        log_entry = FulfillmentLog(
            id=str(uuid.uuid4()),
            transfer_id=transfer_id,
            order_id=order_id,
            listing_id=listing_id,
            request_id=request_id,
            status="received",
            started_at=datetime.now(timezone.utc),
        )
        self._save_log(log_entry)

        try:
            # 1. Look up dataset by listing_id
            dataset, file_path = self._find_dataset(listing_id)
            if dataset is None:
                await self._send_error(
                    transfer_id, order_id,
                    "DATASET_NOT_FOUND",
                    f"No dataset found for listing_id={listing_id}",
                )
                self._update_log(log_entry, "failed", error_code="DATASET_NOT_FOUND",
                                 error_message=f"No dataset for listing_id={listing_id}")
                return

            # 2. Validate file exists and is accessible
            if file_path is None or not os.path.isfile(file_path):
                await self._send_error(
                    transfer_id, order_id,
                    "FILE_READ_ERROR",
                    f"Dataset file not found on disk",
                )
                self._update_log(log_entry, "failed", error_code="FILE_READ_ERROR",
                                 error_message="File not found on disk")
                return

            # 3. Validate file size ≤ 500MB
            file_size = os.path.getsize(file_path)
            if file_size > MAX_FILE_SIZE:
                await self._send_error(
                    transfer_id, order_id,
                    "FILE_TOO_LARGE",
                    f"File size {file_size} bytes exceeds 500MB limit",
                )
                self._update_log(log_entry, "failed", error_code="FILE_TOO_LARGE",
                                 error_message=f"File {file_size} bytes > 500MB")
                return

            # 4. Compute SHA-256 of entire file (streaming, not full load)
            sha256_hash = await asyncio.to_thread(self._compute_sha256, file_path)

            # 5. Calculate chunk info
            total_chunks = math.ceil(file_size / CHUNK_SIZE) if file_size > 0 else 1
            filename = os.path.basename(file_path)
            content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

            # 6. Send metadata
            log_entry.status = "uploading"
            log_entry.file_size_bytes = file_size
            self._save_log(log_entry)

            await self._client.send_action({
                "action": "vai.fulfillment.metadata",
                "transfer_id": transfer_id,
                "order_id": order_id,
                "listing_id": listing_id,
                "parameters": {
                    "filename": filename,
                    "content_type": content_type,
                    "total_bytes": file_size,
                    "total_chunks": total_chunks,
                    "chunk_size": CHUNK_SIZE,
                    "sha256_hash": sha256_hash,
                    "hash_algorithm": "sha256",
                },
            })

            # 7. Stream chunks with windowed ACK backpressure
            chunks_sent = await self._stream_chunks(
                file_path, transfer_id, order_id, listing_id, total_chunks, file_size,
            )

            # 8. Send complete
            await self._client.send_action({
                "action": "vai.fulfillment.complete",
                "transfer_id": transfer_id,
                "order_id": order_id,
                "parameters": {
                    "status": "fulfilled",
                    "file_size_bytes": file_size,
                    "chunk_count": chunks_sent,
                    "sha256_hash": sha256_hash,
                },
            })

            self._update_log(log_entry, "completed", chunks_sent=chunks_sent)
            logger.info(
                "Fulfillment complete: transfer_id=%s, chunks=%d, bytes=%d",
                transfer_id, chunks_sent, file_size,
            )

        except TimeoutError as e:
            logger.error("Fulfillment timed out: %s", e)
            await self._send_error(
                transfer_id, order_id,
                "TRANSFER_ABORTED",
                f"ACK timeout: {e}",
            )
            self._update_log(log_entry, "timed_out", error_code="TRANSFER_ABORTED",
                             error_message=str(e))

        except ConnectionError as e:
            logger.error("Fulfillment connection error: %s", e)
            self._update_log(log_entry, "failed", error_code="TRANSFER_ABORTED",
                             error_message=str(e))

        except Exception as e:
            logger.error("Fulfillment unexpected error: %s", e, exc_info=True)
            try:
                await self._send_error(
                    transfer_id, order_id,
                    "TRANSFER_ABORTED",
                    f"Internal error",
                )
            except Exception:
                pass
            self._update_log(log_entry, "failed", error_code="TRANSFER_ABORTED",
                             error_message=str(e))

    async def _stream_chunks(
        self,
        file_path: str,
        transfer_id: str,
        order_id: str,
        listing_id: str,
        total_chunks: int,
        file_size: int,
    ) -> int:
        """
        Stream file in 64KB chunks with windowed ACK backpressure.

        Sends WINDOW_SIZE chunks, then waits for ACK.
        If no ACK within 30s, re-sends the same window once, then aborts.

        Returns the number of chunks sent.
        """
        chunks_sent = 0

        def _read_chunk(f, offset: int, size: int) -> bytes:
            f.seek(offset)
            return f.read(size)

        with open(file_path, "rb") as f:
            while chunks_sent < total_chunks:
                # Build window of chunk messages (stored for potential resend)
                window_start = chunks_sent
                window_end = min(chunks_sent + WINDOW_SIZE, total_chunks)
                window_messages = []

                for chunk_index in range(window_start, window_end):
                    byte_offset = chunk_index * CHUNK_SIZE
                    chunk_data = _read_chunk(f, byte_offset, CHUNK_SIZE)
                    payload_b64 = base64.b64encode(chunk_data).decode("ascii")
                    chunk_hash = hashlib.sha256(chunk_data).hexdigest()

                    window_messages.append({
                        "action": "vai.fulfillment.chunk",
                        "transfer_id": transfer_id,
                        "chunk_index": chunk_index,
                        "byte_offset": byte_offset,
                        "payload_length": len(chunk_data),
                        "chunk_sha256": chunk_hash,
                        "payload": payload_b64,
                    })

                # Send window
                for msg in window_messages:
                    await self._client.send_action(msg)
                chunks_sent = window_end

                # Wait for ACK if more chunks remain
                if chunks_sent < total_chunks:
                    ack_index = window_end - 1
                    try:
                        ack = await self._client.wait_for_action(
                            "vai.fulfillment.ack", transfer_id, timeout=ACK_TIMEOUT_S
                        )
                        self._validate_ack(ack, ack_index, transfer_id)
                        logger.debug(
                            "ACK received: acked_through_index=%d",
                            ack.get("acked_through_index", -1),
                        )
                    except TimeoutError:
                        # RETRY: re-send same window, then wait for ACK once more
                        logger.warning(
                            "ACK timeout for transfer_id=%s (expected ack through %d), "
                            "re-sending window and retrying...",
                            transfer_id, ack_index,
                        )
                        for msg in window_messages:
                            await self._client.send_action(msg)
                        try:
                            ack = await self._client.wait_for_action(
                                "vai.fulfillment.ack", transfer_id, timeout=ACK_TIMEOUT_S
                            )
                            self._validate_ack(ack, ack_index, transfer_id)
                            logger.info("ACK received on retry")
                        except TimeoutError:
                            raise TimeoutError(
                                f"No ACK after retry for transfer_id={transfer_id} "
                                f"(expected ack through index {ack_index})"
                            )

        return chunks_sent

    def _validate_ack(self, ack: Dict[str, Any], expected_ack_index: int, transfer_id: str) -> None:
        """Validate an ACK response: status must be 'continue' and acked_through_index >= expected."""
        status = ack.get("status", "")
        if status != "continue":
            raise ConnectionError(f"ACK status={status}, aborting transfer {transfer_id}")
        acked_index = ack.get("acked_through_index", -1)
        if acked_index < expected_ack_index:
            raise ConnectionError(
                f"ACK acked_through_index={acked_index} < expected {expected_ack_index} "
                f"for transfer_id={transfer_id}"
            )

    async def _send_error(
        self, transfer_id: str, order_id: str, error_code: str, error_message: str
    ) -> None:
        """Send vai.fulfillment.error to ai.market."""
        try:
            await self._client.send_action({
                "action": "vai.fulfillment.error",
                "transfer_id": transfer_id,
                "order_id": order_id,
                "parameters": {
                    "status": "failed",
                    "error_code": error_code,
                    "error_message": error_message,
                },
            })
        except Exception as e:
            logger.error("Failed to send error message: %s", e)

    def _find_dataset(self, listing_id: str) -> tuple[Optional[DatasetRecord], Optional[str]]:
        """
        Look up a dataset by its marketplace listing_id.
        Returns (DatasetRecord, file_path) or (None, None).

        Checks:
          1. dataset_records.listing_id column
          2. Fallback: scan publish_result.json files in /data/processed/
        """
        with get_session_context() as session:
            # Primary: look up by listing_id column
            stmt = select(DatasetRecord).where(DatasetRecord.listing_id == listing_id)
            dataset = session.exec(stmt).first()

            if dataset:
                file_path = self._resolve_file_path(dataset)
                return dataset, file_path

            # Fallback: scan publish_result.json files
            processed_dir = Path(settings.processed_directory)
            if processed_dir.exists():
                for result_file in processed_dir.glob("*/publish_result.json"):
                    try:
                        with open(result_file) as f:
                            result = json.load(f)
                        if result.get("listing_id") == listing_id:
                            dataset_id = result_file.parent.name
                            stmt2 = select(DatasetRecord).where(
                                DatasetRecord.id == dataset_id
                            )
                            dataset = session.exec(stmt2).first()
                            if dataset:
                                # Backfill listing_id for future lookups
                                dataset.listing_id = listing_id
                                session.add(dataset)
                                session.commit()
                                session.refresh(dataset)
                                file_path = self._resolve_file_path(dataset)
                                return dataset, file_path
                    except (json.JSONDecodeError, OSError):
                        continue

        return None, None

    def _resolve_file_path(self, dataset: DatasetRecord) -> Optional[str]:
        """
        Resolve the actual file path for a dataset.
        Prefers processed Parquet, falls back to original upload.
        """
        # Try processed path first
        if dataset.processed_path and os.path.isfile(dataset.processed_path):
            return dataset.processed_path

        # Try standard processed location
        parquet_path = os.path.join(
            settings.processed_directory, f"{dataset.id}.parquet"
        )
        if os.path.isfile(parquet_path):
            return parquet_path

        # Fall back to original upload
        upload_path = os.path.join(
            settings.upload_directory, dataset.storage_filename
        )
        if os.path.isfile(upload_path):
            return upload_path

        return None

    @staticmethod
    def _compute_sha256(file_path: str) -> str:
        """Compute SHA-256 hash of a file in streaming fashion."""
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def _save_log(entry: FulfillmentLog) -> None:
        """Insert or update a fulfillment log entry."""
        with get_session_context() as session:
            session.merge(entry)
            session.commit()

    @staticmethod
    def _update_log(
        entry: FulfillmentLog,
        status: str,
        chunks_sent: Optional[int] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Update a fulfillment log entry with final state."""
        entry.status = status
        entry.completed_at = datetime.now(timezone.utc)
        if chunks_sent is not None:
            entry.chunks_sent = chunks_sent
        if error_code is not None:
            entry.error_code = error_code
        if error_message is not None:
            entry.error_message = error_message
        with get_session_context() as session:
            session.merge(entry)
            session.commit()


# Module-level singleton
_service: Optional[FulfillmentService] = None


def get_fulfillment_service() -> FulfillmentService:
    """Get or create the FulfillmentService singleton."""
    global _service
    if _service is None:
        client = get_trust_channel_client()
        _service = FulfillmentService(client)
        _service.register()
        _service.start()
    return _service
