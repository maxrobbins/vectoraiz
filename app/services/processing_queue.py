"""
Bounded-concurrency file processing queue.

Auto-detects optimal concurrency from available CPU cores and memory,
then spawns N worker_loop tasks with an N-sized semaphore. Override via
``VECTORAIZ_MAX_CONCURRENT_PROCESSING`` env var.

All file processing requests are routed through this queue instead of
running as unbounded concurrent background tasks.
"""

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Auto-detect optimal concurrency from system resources
# ---------------------------------------------------------------------------

def auto_detect_concurrency() -> int:
    """Detect optimal processing queue concurrency from CPU and memory.

    - Reserve 1.5 GB for the base system (API server, Postgres, Qdrant, OS).
    - Each processing worker needs ~500 MB (embedding model + DuckDB + buffers).
    - Hard cap at 8, floor at 1.
    - ``VECTORAIZ_MAX_CONCURRENT_PROCESSING`` env var overrides when set.
    """
    env_key = "VECTORAIZ_MAX_CONCURRENT_PROCESSING"
    env_val = os.environ.get(env_key)

    if env_val is not None:
        try:
            n = max(1, int(env_val))
        except (ValueError, TypeError):
            n = 2
        logger.info(
            "Processing concurrency override: %d workers (env %s=%s)",
            n, env_key, env_val,
        )
        return n

    # --- CPU-based estimate ---
    cores = os.cpu_count() or 2
    cpu_based = max(1, cores // 2)

    # --- Memory-based estimate ---
    available_gb = _get_available_memory_gb()
    memory_based = max(1, int((available_gb - 1.5) / 0.5)) if available_gb > 1.5 else 1

    optimal = max(1, min(memory_based, cpu_based, 8))

    logger.info(
        "Auto-detected concurrency: %d workers "
        "(cpus=%d, memory=%.1fGB, env_override=%s)",
        optimal, cores, available_gb, env_val,
    )
    return optimal


def _get_available_memory_gb() -> float:
    """Return available system memory in GB.

    Reads ``/proc/meminfo`` (Docker / Linux cgroup-aware), then falls back
    to ``os.sysconf`` page-size heuristic, then ``psutil``.
    """
    # 1) /proc/meminfo — works inside Docker (respects cgroup limits)
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / (1024 * 1024)  # kB → GB
            # MemAvailable missing (old kernels) — fall back to MemTotal
            f.seek(0)
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) / (1024 * 1024)
    except (OSError, ValueError):
        pass

    # 2) os.sysconf (macOS / other POSIX)
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if pages > 0 and page_size > 0:
            return (pages * page_size) / (1024 ** 3)
    except (ValueError, OSError, AttributeError):
        pass

    # 3) psutil last resort
    try:
        import psutil
        return psutil.virtual_memory().total / (1024 ** 3)
    except Exception:
        return 4.0  # conservative fallback


def _friendly_error(exc: Exception) -> str:
    """Convert an exception to a human-readable error message (no stack traces)."""
    msg = str(exc)
    if isinstance(exc, TimeoutError):
        return "Processing timed out"
    if isinstance(exc, MemoryError):
        return "File too large to process in available memory"
    if isinstance(exc, FileNotFoundError):
        return "Upload file not found"
    # Strip multi-line tracebacks, keep first meaningful line
    first_line = msg.split("\n")[0].strip()
    return first_line[:300] if first_line else type(exc).__name__


@dataclass
class _QueueItem:
    dataset_id: str
    skip_indexing: bool = False
    index_only: bool = False
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ProcessingQueue:
    """Singleton queue that processes datasets with bounded concurrency."""

    def __init__(self):
        self._concurrency = auto_detect_concurrency()
        self._queue: asyncio.Queue[_QueueItem] = asyncio.Queue()
        self._semaphore = asyncio.Semaphore(self._concurrency)
        self._current: Optional[_QueueItem] = None
        self._worker_tasks: List[asyncio.Task] = []
        self._progress: Dict[str, Dict[str, Any]] = {}

    async def submit(
        self,
        dataset_id: str,
        skip_indexing: bool = False,
        index_only: bool = False,
    ) -> int:
        """Enqueue a dataset for processing. Returns queue depth."""
        item = _QueueItem(
            dataset_id=dataset_id,
            skip_indexing=skip_indexing,
            index_only=index_only,
        )
        await self._queue.put(item)
        depth = self._queue.qsize()
        logger.info(
            "Queued %s (queue_depth=%d, skip_indexing=%s, index_only=%s)",
            dataset_id, depth, skip_indexing, index_only,
        )
        self.update_progress(dataset_id, "queued", 0, f"Queue position #{depth}")
        return depth

    # ------------------------------------------------------------------
    # Progress tracking (in-memory only, no DB writes)
    # ------------------------------------------------------------------

    def update_progress(
        self, dataset_id: str, phase: str, progress_pct: float, detail: str = "",
    ) -> None:
        self._progress[dataset_id] = {
            "phase": phase,
            "progress_pct": min(progress_pct, 100),
            "detail": detail,
        }

    def get_progress(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        return self._progress.get(dataset_id)

    def clear_progress(self, dataset_id: str) -> None:
        self._progress.pop(dataset_id, None)

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    @property
    def current_dataset_id(self) -> Optional[str]:
        return self._current.dataset_id if self._current else None

    def get_position(self, dataset_id: str) -> Optional[int]:
        """Queue position: 0 = processing now, 1+ = waiting, None = not queued."""
        if self._current and self._current.dataset_id == dataset_id:
            return 0
        pos = 1
        for item in list(self._queue._queue):
            if item.dataset_id == dataset_id:
                return pos
            pos += 1
        return None

    async def worker_loop(self):
        """Pull items from the queue and process with bounded concurrency.

        Individual file failures MUST NOT stop the queue — each item is
        wrapped in its own try/except so the loop always continues.
        """
        logger.info("Processing queue worker started (max_concurrent=%d)", self._concurrency)
        while True:
            item = await self._queue.get()
            self._current = item
            self.update_progress(item.dataset_id, "extracting", 0, "Starting…")
            async with self._semaphore:
                try:
                    if item.index_only:
                        logger.info("Indexing dataset %s", item.dataset_id)
                        self.update_progress(item.dataset_id, "indexing", 0, "Starting indexing…")
                        await self._run_index(item.dataset_id)
                    else:
                        logger.info(
                            "Processing dataset %s (skip_indexing=%s)",
                            item.dataset_id, item.skip_indexing,
                        )
                        await self._run_process(item.dataset_id, item.skip_indexing)
                    logger.info("Completed dataset %s", item.dataset_id)
                except Exception as exc:
                    logger.exception("Failed dataset %s", item.dataset_id)
                    # Belt-and-suspenders: ensure ERROR status is persisted
                    # even if process_file failed to set it.
                    await self._ensure_error_status(item.dataset_id, exc)
                finally:
                    self.clear_progress(item.dataset_id)
                    self._current = None
                    self._queue.task_done()
                    await asyncio.sleep(0)  # yield to event loop

    # ------------------------------------------------------------------
    # Internal helpers (lazy imports to avoid circular dependencies)
    # ------------------------------------------------------------------

    async def _ensure_error_status(self, dataset_id: str, exc: Exception) -> None:
        """Ensure a dataset is marked ERROR with a human-readable message.

        Called as a safety net when worker_loop catches an unhandled exception.
        """
        try:
            from app.services.processing_service import get_processing_service
            from app.models.dataset import DatasetStatus

            svc = get_processing_service()
            rec = svc.get_dataset(dataset_id)
            if rec is None:
                return
            # Only override if not already in a terminal state
            status_val = rec.status.value if hasattr(rec.status, "value") else str(rec.status)
            if status_val in ("error", "ready", "cancelled", "deleted"):
                return
            rec.status = DatasetStatus.ERROR
            rec.error = _friendly_error(exc)
            storage_fn = rec.upload_path.name if rec.upload_path else f"{dataset_id}"
            svc._save_record(rec, storage_fn)
            logger.info("Set ERROR status for %s: %s", dataset_id, rec.error)
        except Exception:
            logger.exception("Failed to set ERROR status for %s", dataset_id)

    async def _run_process(self, dataset_id: str, skip_indexing: bool):
        """Process a dataset (extract + optionally index)."""
        from app.services.processing_service import get_processing_service
        from app.models.dataset import DatasetStatus

        processing = get_processing_service()
        await processing.process_file(dataset_id, skip_indexing=skip_indexing)

        if not skip_indexing:
            return

        # Auto-index if batch was confirmed during extraction
        from app.core.database import get_session_context
        from app.models.dataset import DatasetRecord as DBDatasetRecord

        should_index = False
        with get_session_context() as session:
            db_row = session.get(DBDatasetRecord, dataset_id)
            if (
                db_row
                and db_row.confirmed_at
                and db_row.status == DatasetStatus.PREVIEW_READY.value
            ):
                logger.info(
                    "Auto-indexing dataset %s (batch confirmed during extraction)",
                    dataset_id,
                )
                db_row.status = DatasetStatus.INDEXING.value
                db_row.updated_at = datetime.now(timezone.utc)
                session.add(db_row)
                session.commit()
                should_index = True

        if should_index:
            await self._run_index(dataset_id)

    async def _run_index(self, dataset_id: str):
        """Run index phase for a dataset."""
        from app.services.processing_service import get_processing_service

        processing = get_processing_service()
        await processing.run_index_phase(dataset_id)

    def start(self, wrapper=None) -> List[asyncio.Task]:
        """Start worker tasks matching auto-detected concurrency.

        Args:
            wrapper: Optional async wrapper(name, coro) for error isolation.
        """
        # Clean up finished tasks
        self._worker_tasks = [t for t in self._worker_tasks if not t.done()]
        while len(self._worker_tasks) < self._concurrency:
            idx = len(self._worker_tasks)
            coro = self.worker_loop()
            if wrapper:
                coro = wrapper(f"processing_queue_{idx}", coro)
            self._worker_tasks.append(asyncio.create_task(coro))
        return self._worker_tasks

    async def shutdown(self):
        """Cancel all worker tasks."""
        for task in self._worker_tasks:
            task.cancel()
        for task in self._worker_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._worker_tasks = []


_instance: Optional[ProcessingQueue] = None


def get_processing_queue() -> ProcessingQueue:
    """Get the singleton ProcessingQueue."""
    global _instance
    if _instance is None:
        _instance = ProcessingQueue()
    return _instance
