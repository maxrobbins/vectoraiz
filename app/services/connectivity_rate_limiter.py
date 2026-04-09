"""
Connectivity Rate Limiter — Multi-layer rate limiting for external queries.

Layers (§4.4, M21):
  1. Per-token:   30 req/min (configurable CONNECTIVITY_RATE_LIMIT_RPM)
  2. Per-IP auth: 5 failures/min → block IP for 5 minutes
  3. Global:      120 req/min
  4. Per-tool SQL: 10 req/min per token
  5. Concurrency:  max 3 in-flight per token

Implementation: In-memory sliding window. Resets on restart (acceptable for local).

Phase: BQ-MCP-RAG — Universal LLM Connectivity
Created: S136
"""
from __future__ import annotations

import logging
import time
from collections import defaultdict
from threading import Lock
from typing import Optional

logger = logging.getLogger(__name__)


class _SlidingWindow:
    """Thread-safe sliding-window counter for a single key."""

    __slots__ = ("_timestamps", "_lock")

    def __init__(self):
        self._timestamps: list[float] = []
        self._lock = Lock()

    def count_in_window(self, window_s: float, now: float) -> int:
        """Return how many events occurred in the last *window_s* seconds."""
        cutoff = now - window_s
        with self._lock:
            self._timestamps = [t for t in self._timestamps if t > cutoff]
            return len(self._timestamps)

    def record(self, now: float) -> None:
        with self._lock:
            self._timestamps.append(now)

    def count_and_record(self, window_s: float, now: float) -> int:
        """Prune, record new event, return count AFTER recording."""
        cutoff = now - window_s
        with self._lock:
            self._timestamps = [t for t in self._timestamps if t > cutoff]
            self._timestamps.append(now)
            return len(self._timestamps)


class ConnectivityRateLimiter:
    """Multi-layer rate limiter for external connectivity."""

    def __init__(
        self,
        per_token_rpm: int = 30,
        per_ip_auth_fail_limit: int = 5,
        ip_block_duration_s: int = 300,
        global_rpm: int = 120,
        per_tool_sql_rpm: int = 10,
        max_concurrent_per_token: int = 3,
    ):
        self.per_token_rpm = per_token_rpm
        self.per_ip_auth_fail_limit = per_ip_auth_fail_limit
        self.ip_block_duration_s = ip_block_duration_s
        self.global_rpm = global_rpm
        self.per_tool_sql_rpm = per_tool_sql_rpm
        self.max_concurrent_per_token = max_concurrent_per_token

        # Per-token sliding windows
        self._token_windows: dict[str, _SlidingWindow] = defaultdict(_SlidingWindow)
        # Per-token SQL sliding windows
        self._token_sql_windows: dict[str, _SlidingWindow] = defaultdict(_SlidingWindow)
        # Per-IP auth failure windows
        self._ip_fail_windows: dict[str, _SlidingWindow] = defaultdict(_SlidingWindow)
        # Per-IP block timestamps (IP → block_until)
        self._ip_blocks: dict[str, float] = {}
        # Global sliding window
        self._global_window = _SlidingWindow()
        # Per-token concurrency counters
        self._concurrency: dict[str, int] = defaultdict(int)
        self._concurrency_lock = Lock()

    def check_ip_blocked(self, client_ip: str) -> Optional[str]:
        """Check if an IP is currently blocked. Returns error code or None."""
        now = time.time()
        block_until = self._ip_blocks.get(client_ip)
        if block_until and now < block_until:
            return "ip_blocked"
        elif block_until and now >= block_until:
            # Block expired, clean up
            self._ip_blocks.pop(client_ip, None)
        return None

    def record_auth_failure(self, client_ip: str) -> Optional[str]:
        """Record an auth failure for an IP. Returns 'ip_blocked' if threshold reached."""
        now = time.time()
        count = self._ip_fail_windows[client_ip].count_and_record(60.0, now)
        if count >= self.per_ip_auth_fail_limit:
            self._ip_blocks[client_ip] = now + self.ip_block_duration_s
            logger.warning(
                "IP blocked due to auth failures: ip=%s failures=%d block_until=%s",
                client_ip, count, self._ip_blocks[client_ip],
            )
            return "ip_blocked"
        return None

    def check_rate_limits(
        self,
        token_id: str,
        tool_name: str,
        client_ip: str,
    ) -> Optional[str]:
        """Check all rate limit layers. Returns error code or None if allowed.

        Order: IP block → global → per-token → per-tool SQL → concurrency.
        """
        now = time.time()

        # Layer 1: IP block check
        ip_result = self.check_ip_blocked(client_ip)
        if ip_result:
            return ip_result

        # Layer 2: Global rate limit
        global_count = self._global_window.count_in_window(60.0, now)
        if global_count >= self.global_rpm:
            return "rate_limited"

        # Layer 3: Per-token rate limit
        token_count = self._token_windows[token_id].count_in_window(60.0, now)
        if token_count >= self.per_token_rpm:
            return "rate_limited"

        # Layer 4: Per-tool SQL rate limit
        if tool_name in ("vectoraiz_sql", "sql", "execute_sql"):
            sql_count = self._token_sql_windows[token_id].count_in_window(60.0, now)
            if sql_count >= self.per_tool_sql_rpm:
                return "rate_limited"

        # Layer 5: Concurrency
        with self._concurrency_lock:
            if self._concurrency[token_id] >= self.max_concurrent_per_token:
                return "rate_limited"

        return None

    def record_request(self, token_id: str, tool_name: str) -> None:
        """Record a successful request (called after check passes)."""
        now = time.time()
        self._global_window.record(now)
        self._token_windows[token_id].record(now)
        if tool_name in ("vectoraiz_sql", "sql", "execute_sql"):
            self._token_sql_windows[token_id].record(now)

    def acquire_concurrency(self, token_id: str) -> bool:
        """Acquire a concurrency slot. Returns False if at capacity."""
        with self._concurrency_lock:
            if self._concurrency[token_id] >= self.max_concurrent_per_token:
                return False
            self._concurrency[token_id] += 1
            return True

    def release_concurrency(self, token_id: str) -> None:
        """Release a concurrency slot."""
        with self._concurrency_lock:
            self._concurrency[token_id] = max(0, self._concurrency[token_id] - 1)

    def get_ip_block_remaining(self, client_ip: str) -> float:
        """Return seconds remaining on an IP block, or 0 if not blocked."""
        block_until = self._ip_blocks.get(client_ip)
        if block_until:
            remaining = block_until - time.time()
            return max(0.0, remaining)
        return 0.0
