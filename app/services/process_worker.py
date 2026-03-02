"""
Subprocess entry point for large file processing.

BQ-VZ-LARGE-FILES Phase 1 (M1): Process isolation via multiprocessing.Process.
Worker crash must not take down the API server.

IPC:
- Queue: carries serialized RecordBatch (Arrow IPC) or TextBlock dicts
- Pipe (progress): lightweight JSON progress messages
- Pipe (control): cancel signal from parent

Memory limits:
- Parent-side MemoryMonitor (psutil RSS) — RLIMIT_AS not used (see _set_memory_limit)
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import signal
import sys
import threading
import time
from dataclasses import asdict
from multiprocessing import Queue
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any, Dict, Optional

import psutil
import pyarrow as pa

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Memory limit enforcement
# ---------------------------------------------------------------------------


def _set_memory_limit(limit_mb: int) -> None:
    """Configure per-process memory awareness.

    RLIMIT_AS is intentionally NOT used: forked subprocesses inherit the
    parent's virtual address space (typically 4-5GB with PyTorch loaded),
    which already exceeds any reasonable RLIMIT_AS value.  Setting it
    causes immediate MemoryError on any new allocation — even small ones.

    Instead, the parent-side MemoryMonitor (psutil-based) watches RSS and
    sends SIGTERM then SIGKILL if the subprocess exceeds limit_mb * 2.
    """
    logger.info(
        "Worker memory budget: %dMB (enforced by parent MemoryMonitor, not RLIMIT_AS)",
        limit_mb,
    )


class MemoryMonitor:
    """Monitors a worker subprocess RSS from the parent process.

    Runs in a daemon thread; polls RSS every ``poll_interval_s`` seconds.
    If RSS exceeds ``limit_mb * 2`` → SIGTERM.  If still alive after
    ``grace_s`` seconds → SIGKILL.  Logs the high-water mark on stop.
    """

    def __init__(
        self,
        pid: int,
        limit_mb: int,
        poll_interval_s: float = 5.0,
        grace_s: float = 60.0,
    ):
        self._pid = pid
        self._limit_bytes = limit_mb * 1024 * 1024
        self._hard_limit_bytes = limit_mb * 2 * 1024 * 1024
        self._poll_interval = poll_interval_s
        self._grace_s = grace_s
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._high_water_bytes = 0

    # -- public API --------------------------------------------------------

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, daemon=True, name=f"mem-mon-{self._pid}",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._poll_interval + 2)
        logger.info(
            "MemoryMonitor for pid %d stopped — high-water RSS: %.1f MB",
            self._pid,
            self._high_water_bytes / (1024 * 1024),
        )

    # -- internals ---------------------------------------------------------

    def _run(self) -> None:
        try:
            proc = psutil.Process(self._pid)
        except psutil.NoSuchProcess:
            return

        sigterm_sent_at: Optional[float] = None

        while not self._stop_event.is_set():
            try:
                rss = proc.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break

            if rss > self._high_water_bytes:
                self._high_water_bytes = rss

            if sigterm_sent_at is not None:
                # Already sent SIGTERM — check grace period
                if time.monotonic() - sigterm_sent_at >= self._grace_s:
                    logger.error(
                        "Worker pid %d still alive after SIGTERM + %.0fs grace — sending SIGKILL",
                        self._pid, self._grace_s,
                    )
                    try:
                        os.kill(self._pid, signal.SIGKILL)
                    except OSError:
                        pass
                    break
            elif rss > self._hard_limit_bytes:
                logger.warning(
                    "Worker pid %d RSS %.1f MB exceeds hard limit %.1f MB — sending SIGTERM",
                    self._pid,
                    rss / (1024 * 1024),
                    self._hard_limit_bytes / (1024 * 1024),
                )
                try:
                    os.kill(self._pid, signal.SIGTERM)
                except OSError:
                    break
                sigterm_sent_at = time.monotonic()

            self._stop_event.wait(self._poll_interval)


# ---------------------------------------------------------------------------
# Serialization helpers for IPC via Queue
# ---------------------------------------------------------------------------


def serialize_record_batch(batch: pa.RecordBatch) -> bytes:
    """Serialize a RecordBatch to Arrow IPC bytes for Queue transport."""
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


def deserialize_record_batch(data: bytes) -> pa.RecordBatch:
    """Deserialize Arrow IPC bytes back to a RecordBatch."""
    reader = pa.ipc.open_stream(data)
    return reader.read_all().to_batches()[0]


# ---------------------------------------------------------------------------
# Worker entry points (called inside subprocess)
# ---------------------------------------------------------------------------


def _safe_progress_send(conn: Connection, msg: Dict[str, Any]) -> None:
    """Send a progress message, ignoring BrokenPipeError (parent died)."""
    try:
        conn.send(msg)
    except (BrokenPipeError, OSError):
        pass


def _safe_queue_put(
    queue: Queue,
    item: Any,
    control_conn: Connection,
    timeout: float = 30.0,
    max_retries: int = 10,
) -> bool:
    """Put an item on the queue with timeout and cancel-check between retries.

    Returns True if the item was placed, False if cancelled.
    """
    import queue as _queue_mod

    for _ in range(max_retries):
        try:
            queue.put(item, timeout=timeout)
            return True
        except _queue_mod.Full:
            # Check if parent asked us to cancel
            if control_conn.poll(0):
                msg = control_conn.recv()
                if msg == "cancel":
                    return False
    # Exhausted retries — parent is likely dead
    return False


def run_tabular_worker(
    filepath: str,
    file_type: str,
    data_queue: Queue,
    progress_conn: Connection,
    control_conn: Connection,
    memory_limit_mb: int,
    batch_target_rows: int,
) -> None:
    """Subprocess entry point for streaming tabular file processing.

    Yields RecordBatch chunks into data_queue, sends progress via progress_conn.
    Checks control_conn for cancel signals between chunks.
    """
    _set_memory_limit(memory_limit_mb)

    # Import here to avoid loading heavy libs in parent until needed
    from app.services.streaming_processor import (
        StreamingTabularProcessor,
        check_file_size,
        check_zip_bomb,
    )

    try:
        fp = Path(filepath)
        check_file_size(fp)
        check_zip_bomb(fp)

        total_bytes = fp.stat().st_size
        processor = StreamingTabularProcessor(fp, file_type)

        chunks_sent = 0
        rows_sent = 0
        last_progress = time.monotonic()

        for batch in processor:
            # Check for cancel signal (non-blocking)
            if control_conn.poll(0):
                msg = control_conn.recv()
                if msg == "cancel":
                    _safe_progress_send(progress_conn, {
                        "status": "cancelled",
                        "chunks_processed": chunks_sent,
                        "rows_processed": rows_sent,
                    })
                    _safe_queue_put(data_queue, None, control_conn)
                    return

            # Serialize and send batch (with timeout + cancel check)
            if not _safe_queue_put(data_queue, serialize_record_batch(batch), control_conn):
                _safe_progress_send(progress_conn, {
                    "status": "cancelled",
                    "chunks_processed": chunks_sent,
                    "rows_processed": rows_sent,
                })
                return

            chunks_sent += 1
            rows_sent += batch.num_rows

            # Send progress at most every 5 seconds
            now = time.monotonic()
            if now - last_progress >= 5:
                _safe_progress_send(progress_conn, {
                    "status": "processing",
                    "phase": "extracting",
                    "chunks_processed": chunks_sent,
                    "rows_processed": rows_sent,
                    "total_bytes": total_bytes,
                })
                last_progress = now

        # Send completion
        _safe_queue_put(data_queue, None, control_conn)  # sentinel
        _safe_progress_send(progress_conn, {
            "status": "completed",
            "chunks_processed": chunks_sent,
            "rows_processed": rows_sent,
            "total_bytes": total_bytes,
        })

    except Exception as e:
        logger.exception("Tabular worker failed: %s", e)
        _safe_progress_send(progress_conn, {
            "status": "error",
            "error": str(e),
            "error_type": type(e).__name__,
        })
        _safe_queue_put(data_queue, None, control_conn)  # sentinel so parent doesn't hang


def run_document_worker(
    filepath: str,
    file_type: str,
    data_queue: Queue,
    progress_conn: Connection,
    control_conn: Connection,
    memory_limit_mb: int,
) -> None:
    """Subprocess entry point for streaming document file processing.

    Yields TextBlock dicts into data_queue, sends progress via progress_conn.
    """
    _set_memory_limit(memory_limit_mb)

    from app.services.streaming_processor import (
        StreamingDocumentProcessor,
        check_file_size,
    )

    try:
        fp = Path(filepath)
        check_file_size(fp)

        total_bytes = fp.stat().st_size
        processor = StreamingDocumentProcessor(fp, file_type)

        chunks_sent = 0
        last_progress = time.monotonic()

        for text_block in processor:
            # Check for cancel
            if control_conn.poll(0):
                msg = control_conn.recv()
                if msg == "cancel":
                    _safe_progress_send(progress_conn, {
                        "status": "cancelled",
                        "chunks_processed": chunks_sent,
                    })
                    _safe_queue_put(data_queue, None, control_conn)
                    return

            # Send text block as dict (picklable, with timeout)
            if not _safe_queue_put(data_queue, asdict(text_block), control_conn):
                _safe_progress_send(progress_conn, {
                    "status": "cancelled",
                    "chunks_processed": chunks_sent,
                })
                return

            chunks_sent += 1

            now = time.monotonic()
            if now - last_progress >= 5:
                _safe_progress_send(progress_conn, {
                    "status": "processing",
                    "phase": "extracting",
                    "chunks_processed": chunks_sent,
                    "total_bytes": total_bytes,
                })
                last_progress = now

        _safe_queue_put(data_queue, None, control_conn)  # sentinel
        _safe_progress_send(progress_conn, {
            "status": "completed",
            "chunks_processed": chunks_sent,
            "total_bytes": total_bytes,
        })

    except Exception as e:
        logger.exception("Document worker failed: %s", e)
        _safe_progress_send(progress_conn, {
            "status": "error",
            "error": str(e),
            "error_type": type(e).__name__,
        })
        _safe_queue_put(data_queue, None, control_conn)


def run_indexing_worker(
    dataset_id: str,
    parquet_path: str,
    progress_conn: Connection,
    control_conn: Connection,
    memory_limit_mb: int,
) -> None:
    """Subprocess entry point for streaming indexing vectors to Qdrant.
    
    Reads from the Parquet file and indices batches via IndexingService.
    Sends progress updates back to the parent and handles cancellation.
    """
    _set_memory_limit(memory_limit_mb)

    import pyarrow.parquet as pq
    from app.config import settings
    from app.services.indexing_service import get_indexing_service

    # Path validation: prevent traversal and symlink attacks
    fp = Path(parquet_path).resolve()
    processed_dir = Path(settings.processed_directory).resolve()
    if not str(fp).startswith(str(processed_dir)):
        raise ValueError(f"Path traversal detected: {parquet_path} resolves outside {processed_dir}")
    if fp.is_symlink():
        raise ValueError(f"Symlink detected: {parquet_path}")
    if not fp.exists() or not fp.is_file():
        raise ValueError(f"Not a regular file: {parquet_path}")

    try:
        indexing_service = get_indexing_service()
        pf = pq.ParquetFile(parquet_path)
        total_rows = pf.metadata.num_rows if pf.metadata else 0
        chunk_iter = pf.iter_batches(batch_size=1000)

        def _indexing_progress(rows_done: int) -> None:
            # Check for cancel signal from parent
            if control_conn.poll(0):
                msg = control_conn.recv()
                if msg == "cancel":
                    raise InterruptedError("cancelled")
                    
            pct = min((rows_done / max(total_rows, 1)) * 100, 99) if total_rows else 50
            _safe_progress_send(progress_conn, {
                "status": "processing",
                "phase": "indexing",
                "rows_done": rows_done,
                "total_rows": total_rows,
                "pct": pct
            })

        result = indexing_service.index_streaming(
            dataset_id=dataset_id,
            chunk_iterator=chunk_iter,
            recreate_collection=True,
            progress_callback=_indexing_progress,
        )
        
        _safe_progress_send(progress_conn, {
            "status": "completed",
            "result": result
        })
        
    except Exception as e:
        status = "cancelled" if isinstance(e, InterruptedError) else "error"
        logger.exception("Indexing worker failed: %s", e)
        _safe_progress_send(progress_conn, {
            "status": status,
            "error": str(e),
            "error_type": type(e).__name__,
        })


# ---------------------------------------------------------------------------
# Parent-side: ProcessWorkerManager
# ---------------------------------------------------------------------------


class ProcessWorkerManager:
    """Manages subprocess workers for large file processing.

    Provides:
    - Bounded concurrency via semaphore
    - Queue-based data channel with backpressure
    - Pipe-based progress/control channels
    - Timeout, memory monitoring, and cancel escalation
    """

    def __init__(self):
        self._max_workers = self._get_max_workers()
        self._semaphore = threading.Semaphore(self._max_workers)
        self._active_processes: list[multiprocessing.Process] = []

    @staticmethod
    def _get_max_workers() -> int:
        from app.config import settings
        return settings.process_worker_max_concurrent

    def submit_tabular(
        self,
        filepath: Path,
        file_type: str,
    ) -> WorkerHandle:
        """Submit a tabular file for streaming processing in a subprocess."""
        from app.config import settings

        self._semaphore.acquire()

        data_queue = multiprocessing.Queue(maxsize=settings.streaming_queue_maxsize)
        progress_parent, progress_child = multiprocessing.Pipe(duplex=False)
        control_parent, control_child = multiprocessing.Pipe(duplex=False)

        proc = multiprocessing.Process(
            target=run_tabular_worker,
            args=(
                str(filepath),
                file_type,
                data_queue,
                progress_child,
                control_parent,   # read end for poll/recv
                settings.process_worker_memory_limit_mb,
                settings.streaming_batch_target_rows,
            ),
            daemon=True,
        )
        proc.start()
        self._active_processes.append(proc)

        mem_monitor = MemoryMonitor(
            pid=proc.pid,
            limit_mb=settings.process_worker_memory_limit_mb,
        )
        mem_monitor.start()

        return WorkerHandle(
            future=proc,
            data_queue=data_queue,
            progress_conn=progress_parent,
            control_conn=control_child,    # write end for send
            timeout_s=settings.process_worker_timeout_s,
            grace_period_s=settings.process_worker_grace_period_s,
            memory_monitor=mem_monitor,
            semaphore=self._semaphore,
        )

    def submit_document(
        self,
        filepath: Path,
        file_type: str,
    ) -> WorkerHandle:
        """Submit a document file for streaming processing in a subprocess."""
        from app.config import settings

        self._semaphore.acquire()

        data_queue = multiprocessing.Queue(maxsize=settings.streaming_queue_maxsize)
        progress_parent, progress_child = multiprocessing.Pipe(duplex=False)
        control_parent, control_child = multiprocessing.Pipe(duplex=False)

        proc = multiprocessing.Process(
            target=run_document_worker,
            args=(
                str(filepath),
                file_type,
                data_queue,
                progress_child,
                control_parent,   # read end for poll/recv
                settings.process_worker_memory_limit_mb,
            ),
            daemon=True,
        )
        proc.start()
        self._active_processes.append(proc)

        mem_monitor = MemoryMonitor(
            pid=proc.pid,
            limit_mb=settings.process_worker_memory_limit_mb,
        )
        mem_monitor.start()

        return WorkerHandle(
            future=proc,
            data_queue=data_queue,
            progress_conn=progress_parent,
            control_conn=control_child,    # write end for send
            timeout_s=settings.process_worker_timeout_s,
            grace_period_s=settings.process_worker_grace_period_s,
            memory_monitor=mem_monitor,
            semaphore=self._semaphore,
        )

    def submit_indexing(
        self,
        dataset_id: str,
        parquet_path: Path,
    ) -> WorkerHandle:
        """Submit an indexing job for streaming processing in a subprocess."""
        from app.config import settings

        self._semaphore.acquire()

        data_queue = multiprocessing.Queue(maxsize=1) # Unused for indexing, but matching handle signature
        progress_parent, progress_child = multiprocessing.Pipe(duplex=False)
        control_parent, control_child = multiprocessing.Pipe(duplex=False)

        proc = multiprocessing.Process(
            target=run_indexing_worker,
            args=(
                dataset_id,
                str(parquet_path),
                progress_child,
                control_parent,   # read end for poll/recv
                settings.process_worker_memory_limit_mb,
            ),
            daemon=True,
        )
        proc.start()
        self._active_processes.append(proc)

        mem_monitor = MemoryMonitor(
            pid=proc.pid,
            limit_mb=settings.process_worker_memory_limit_mb,
        )
        mem_monitor.start()

        return WorkerHandle(
            future=proc,
            data_queue=data_queue,
            progress_conn=progress_parent,
            control_conn=control_child,    # write end for send
            timeout_s=settings.process_worker_timeout_s * 2, # Indexing takes longer generally
            grace_period_s=settings.process_worker_grace_period_s,
            memory_monitor=mem_monitor,
            semaphore=self._semaphore,
        )

    def shutdown(self, wait: bool = True) -> None:
        for proc in self._active_processes:
            if proc.is_alive():
                proc.terminate()
                if wait:
                    proc.join(timeout=10)
        self._active_processes.clear()


class WorkerHandle:
    """Handle to a running subprocess worker.

    Provides iteration over data chunks from the Queue,
    progress polling, and cancellation.
    """

    def __init__(
        self,
        future,
        data_queue: Queue,
        progress_conn: Connection,
        control_conn: Connection,
        timeout_s: int = 1800,
        grace_period_s: int = 60,
        memory_monitor: Optional[MemoryMonitor] = None,
        semaphore: Optional[threading.Semaphore] = None,
    ):
        self.future = future
        self.data_queue = data_queue
        self.progress_conn = progress_conn
        self.control_conn = control_conn
        self.timeout_s = timeout_s
        self.grace_period_s = grace_period_s
        self._start_time = time.monotonic()
        self._memory_monitor = memory_monitor
        self._semaphore = semaphore

    def _worker_pid(self) -> Optional[int]:
        """Return the worker PID if available."""
        return getattr(self.future, "pid", None)

    def _worker_alive(self) -> bool:
        """Return True if the worker process is still running."""
        return getattr(self.future, "is_alive", lambda: False)()

    def _cleanup(self) -> None:
        """Stop the memory monitor and release the concurrency semaphore."""
        if self._memory_monitor is not None:
            self._memory_monitor.stop()
            self._memory_monitor = None
        if self._semaphore is not None:
            self._semaphore.release()
            self._semaphore = None

    def iter_data(self):
        """Iterate over data items from the worker queue.

        Yields raw items (bytes for RecordBatch, dict for TextBlock).
        Raises TimeoutError if worker exceeds timeout.
        """
        try:
            while True:
                elapsed = time.monotonic() - self._start_time
                remaining = max(1, self.timeout_s - elapsed)

                if elapsed > self.timeout_s:
                    self.cancel()
                    raise TimeoutError(
                        f"Worker exceeded {self.timeout_s}s timeout"
                    )

                try:
                    item = self.data_queue.get(timeout=min(remaining, 30))
                except Exception:
                    # Check if worker exited
                    if not self._worker_alive():
                        exitcode = getattr(self.future, "exitcode", None)
                        if exitcode and exitcode != 0:
                            raise RuntimeError(
                                f"Worker crashed with exit code {exitcode}"
                            )
                        return
                    continue

                if item is None:  # sentinel
                    return
                yield item
        finally:
            self._cleanup()

    def get_progress(self) -> Optional[Dict[str, Any]]:
        """Non-blocking poll for latest progress from worker."""
        latest = None
        try:
            while self.progress_conn.poll(0):
                latest = self.progress_conn.recv()
        except (EOFError, BrokenPipeError):
            pass
        return latest

    def cancel(self) -> None:
        """Send cancel signal to worker, then escalate SIGTERM → SIGKILL."""
        # 1. Ask nicely via control pipe
        try:
            self.control_conn.send("cancel")
        except Exception:
            pass

        pid = self._worker_pid()
        if pid is None:
            return

        # 2. Wait grace_period_s for cooperative shutdown
        deadline = time.monotonic() + self.grace_period_s
        while self._worker_alive() and time.monotonic() < deadline:
            time.sleep(0.5)

        if not self._worker_alive():
            self._cleanup()
            return

        # 3. SIGTERM
        logger.warning("Worker pid %d did not exit in %ds — sending SIGTERM", pid, self.grace_period_s)
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            self._cleanup()
            return

        sigterm_deadline = time.monotonic() + 5
        while self._worker_alive() and time.monotonic() < sigterm_deadline:
            time.sleep(0.5)

        if not self._worker_alive():
            self._cleanup()
            return

        # 4. SIGKILL
        logger.error("Worker pid %d still alive after SIGTERM — sending SIGKILL", pid)
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
        self._cleanup()

    def wait(self, timeout: Optional[float] = None) -> None:
        """Wait for worker to complete."""
        self.future.join(timeout=timeout)


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_worker_manager: Optional[ProcessWorkerManager] = None


def get_worker_manager() -> ProcessWorkerManager:
    global _worker_manager
    if _worker_manager is None:
        _worker_manager = ProcessWorkerManager()
    return _worker_manager
