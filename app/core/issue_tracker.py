"""
BQ-123A: Non-critical issue tracker — in-memory ring buffer.

Tracks recent warnings/degradations by error code. Persists to
logs/issues.json on shutdown and reloads on startup.
Auto-clears issues that haven't recurred in 1 hour.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

MAX_ISSUES = 100
AUTO_CLEAR_SECONDS = 3600  # 1 hour


@dataclass
class TrackedIssue:
    code: str
    component: str
    count: int = 1
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "component": self.component,
            "count": self.count,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }


class IssueTracker:
    """Ring buffer of recent non-critical issues."""

    def __init__(self, persist_path: str = "logs/issues.json", max_size: int = MAX_ISSUES):
        self._issues: OrderedDict[str, TrackedIssue] = OrderedDict()
        self._lock = threading.Lock()
        self._max_size = max_size
        self._persist_path = persist_path

    def record(self, code: str, component: str | None = None) -> None:
        """Record an issue occurrence."""
        if component is None:
            # Derive component from code domain: VAI-QDR-001 → qdrant
            parts = code.split("-")
            component = parts[1].lower() if len(parts) >= 3 else "unknown"

        with self._lock:
            if code in self._issues:
                issue = self._issues[code]
                issue.count += 1
                issue.last_seen = time.time()
                # Move to end (most recent)
                self._issues.move_to_end(code)
            else:
                self._issues[code] = TrackedIssue(
                    code=code, component=component,
                )
                # Evict oldest if over capacity
                while len(self._issues) > self._max_size:
                    self._issues.popitem(last=False)

    def get_active_issues(self) -> list[dict]:
        """Return issues that have recurred within the last hour."""
        cutoff = time.time() - AUTO_CLEAR_SECONDS
        with self._lock:
            return [
                issue.to_dict()
                for issue in self._issues.values()
                if issue.last_seen >= cutoff
            ]

    def persist(self) -> None:
        """Save current issues to disk."""
        try:
            os.makedirs(os.path.dirname(self._persist_path), exist_ok=True)
            with self._lock:
                data = [issue.to_dict() for issue in self._issues.values()]
            with open(self._persist_path, "w") as f:
                json.dump(data, f, indent=2)
            logger.info("issue_tracker_persisted", extra={"count": len(data)})
        except Exception as e:
            logger.warning("issue_tracker_persist_failed", extra={"error": str(e)})

    def reload(self) -> None:
        """Reload issues from disk."""
        if not os.path.exists(self._persist_path):
            return
        try:
            with open(self._persist_path, "r") as f:
                data = json.load(f)
            with self._lock:
                for item in data:
                    code = item["code"]
                    self._issues[code] = TrackedIssue(
                        code=code,
                        component=item.get("component", "unknown"),
                        count=item.get("count", 1),
                        first_seen=item.get("first_seen", time.time()),
                        last_seen=item.get("last_seen", time.time()),
                    )
            logger.info("issue_tracker_reloaded", extra={"count": len(self._issues)})
        except Exception as e:
            logger.warning("issue_tracker_reload_failed", extra={"error": str(e)})

    def clear(self) -> None:
        """Clear all tracked issues."""
        with self._lock:
            self._issues.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._issues)


# Module-level singleton
issue_tracker = IssueTracker()
