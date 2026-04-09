"""
EventBus — In-Process Pub/Sub for VZ State Events

Multi-tab safe: maintains a Set of queues per session_id so multiple
browser tabs can independently subscribe to the same session's events.

PHASE: BQ-VZ-CONTROL-PLANE Step 2 — Security Foundation
CREATED: 2026-03-05
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Set

logger = logging.getLogger(__name__)


@dataclass
class VZEvent:
    """A structured event for the VZ event stream."""
    event_type: str  # approval_request, approval_result, tool_executed
    session_id: str
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = 0.0

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = time.time()

    def to_sse(self) -> str:
        """Format as Server-Sent Event."""
        payload = {
            "type": self.event_type,
            "data": self.data,
            "timestamp": self.timestamp,
        }
        return f"event: {self.event_type}\ndata: {json.dumps(payload)}\n\n"


class EventBus:
    """In-process pub/sub for VZ state events. Single-worker safe."""

    def __init__(self) -> None:
        self._subscribers: Dict[str, Set[asyncio.Queue]] = {}

    async def emit(self, session_id: str, event: VZEvent) -> None:
        """Emit event to ALL subscribers for a session."""
        for queue in self._subscribers.get(session_id, set()).copy():
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass  # Drop event for slow consumers

    def subscribe(self, session_id: str) -> asyncio.Queue:
        """Subscribe to events. Returns an async queue."""
        queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        if session_id not in self._subscribers:
            self._subscribers[session_id] = set()
        self._subscribers[session_id].add(queue)
        return queue

    def unsubscribe(self, session_id: str, queue: asyncio.Queue) -> None:
        """Remove a specific subscriber queue."""
        if session_id in self._subscribers:
            self._subscribers[session_id].discard(queue)
            if not self._subscribers[session_id]:
                del self._subscribers[session_id]


# Module-level singleton
event_bus = EventBus()
