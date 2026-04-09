"""
Connectivity Metrics — In-memory counters for external request monitoring.

Exposed to allAI for diagnostics and to the health endpoint (§3.3, M17).
In-memory only — resets on restart (acceptable for local single-instance).

Phase: BQ-MCP-RAG — Universal LLM Connectivity
Created: S136
"""
from __future__ import annotations

from collections import defaultdict
from threading import Lock
from typing import Any, Dict, Optional


class ConnectivityMetrics:
    """Thread-safe in-memory metrics for external connectivity."""

    def __init__(self):
        self._lock = Lock()
        # Counters: {key: count}
        self._requests_total: Dict[str, int] = defaultdict(int)   # by tool
        self._requests_errors: Dict[str, int] = defaultdict(int)   # by error code
        self._auth_failures: Dict[str, int] = defaultdict(int)     # by IP
        # Latency: {tool: [ms, ms, ...]}  (kept capped to avoid memory growth)
        self._latency_samples: Dict[str, list] = defaultdict(list)
        self._max_latency_samples = 1000
        # Gauge
        self._active_connections: int = 0

    def record_request(self, tool_name: str, duration_ms: int) -> None:
        with self._lock:
            self._requests_total[tool_name] += 1
            samples = self._latency_samples[tool_name]
            samples.append(duration_ms)
            if len(samples) > self._max_latency_samples:
                self._latency_samples[tool_name] = samples[-self._max_latency_samples:]

    def record_error(self, error_code: str) -> None:
        with self._lock:
            self._requests_errors[error_code] += 1

    def record_auth_failure(self, client_ip: str) -> None:
        with self._lock:
            self._auth_failures[client_ip] += 1

    def increment_connections(self) -> None:
        with self._lock:
            self._active_connections += 1

    def decrement_connections(self) -> None:
        with self._lock:
            self._active_connections = max(0, self._active_connections - 1)

    def get_snapshot(self) -> Dict[str, Any]:
        """Return a point-in-time snapshot of all metrics."""
        with self._lock:
            # Compute latency stats per tool
            latency_stats = {}
            for tool, samples in self._latency_samples.items():
                if samples:
                    sorted_s = sorted(samples)
                    latency_stats[tool] = {
                        "count": len(sorted_s),
                        "avg_ms": round(sum(sorted_s) / len(sorted_s), 1),
                        "p50_ms": sorted_s[len(sorted_s) // 2],
                        "p95_ms": sorted_s[int(len(sorted_s) * 0.95)],
                        "max_ms": sorted_s[-1],
                    }

            return {
                "ext_requests_total": dict(self._requests_total),
                "ext_requests_errors": dict(self._requests_errors),
                "ext_latency_ms": latency_stats,
                "ext_active_connections": self._active_connections,
                "ext_auth_failures": dict(self._auth_failures),
            }


# Singleton
_metrics: Optional[ConnectivityMetrics] = None


def get_connectivity_metrics() -> ConnectivityMetrics:
    global _metrics
    if _metrics is None:
        _metrics = ConnectivityMetrics()
    return _metrics
