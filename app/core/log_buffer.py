"""
BQ-123B: In-memory ring buffer for recent structured log entries.

Thread-safe deque storing the last N log entries for inclusion in
diagnostic bundles. Wired as a structlog processor that captures
every formatted log entry.
"""
from __future__ import annotations

import threading
from collections import deque


class LogRingBuffer:
    """Thread-safe ring buffer storing the last N structured log entries."""

    def __init__(self, max_size: int = 1000):
        self._buffer: deque[dict] = deque(maxlen=max_size)
        self._lock = threading.Lock()

    def append(self, entry: dict) -> None:
        with self._lock:
            self._buffer.append(entry)

    def get_entries(self, limit: int = 1000) -> list[dict]:
        with self._lock:
            entries = list(self._buffer)
        return entries[-limit:]

    def clear(self) -> None:
        with self._lock:
            self._buffer.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._buffer)


# Module-level singleton
log_ring_buffer = LogRingBuffer(max_size=1000)


def structlog_buffer_processor(logger, method_name, event_dict):
    """Structlog processor that copies each log entry into the ring buffer.

    This must be inserted into the ProcessorFormatter's processor chain
    (before JSONRenderer) so it captures the fully-enriched event dict.
    """
    try:
        # Make a shallow copy so the buffer entry doesn't get mutated
        log_ring_buffer.append(dict(event_dict))
    except Exception:
        pass  # Never break logging due to buffer issues
    return event_dict
