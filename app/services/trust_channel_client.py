"""
Trust Channel WebSocket Client
==============================

Maintains a persistent WebSocket connection to ai.market's Trust Channel.
Dispatches incoming actions to registered handler functions.

BQ-D1: Fulfillment Listener (vectorAIz side)

The Trust Channel is the encrypted bidirectional communication channel
between vectorAIz instances and the ai.market platform. Messages are
JSON-encoded actions identified by an "action" field.

Connection lifecycle:
  1. Connect to ws://{ai_market_url}/ws/trust-channel
  2. Authenticate with internal API key
  3. Listen for incoming actions, dispatch to registered handlers
  4. Reconnect with exponential backoff on disconnect
"""

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable, Dict, Optional

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError

try:
    from websockets.exceptions import InvalidStatusCode
except ImportError:
    # websockets >= 14 moved this; fall back to generic
    InvalidStatusCode = ConnectionClosedError  # type: ignore[misc,assignment]

from app.config import settings

logger = logging.getLogger(__name__)

# Type alias for action handlers: async fn(params: dict) -> None
ActionHandler = Callable[[Dict[str, Any]], Awaitable[None]]

# Reconnect backoff
_INITIAL_BACKOFF_S = 2.0
_MAX_BACKOFF_S = 60.0
_BACKOFF_MULTIPLIER = 2.0


class TrustChannelClient:
    """
    WebSocket client that connects to ai.market's Trust Channel
    and dispatches incoming actions to registered handlers.
    """

    def __init__(self) -> None:
        self._handlers: Dict[str, ActionHandler] = {}
        self._ws: Optional[Any] = None  # websockets connection
        self._running = False
        self._send_lock = asyncio.Lock()
        self._waiters: Dict[str, asyncio.Future] = {}
        # Build WS URL from ai_market_url (http → ws, https → wss)
        base = settings.ai_market_url.rstrip("/")
        if base.startswith("https://"):
            self._ws_url = base.replace("https://", "wss://") + "/ws/trust-channel"
        elif base.startswith("http://"):
            self._ws_url = base.replace("http://", "ws://") + "/ws/trust-channel"
        else:
            self._ws_url = "wss://" + base + "/ws/trust-channel"

    def register_handler(self, action: str, handler: ActionHandler) -> None:
        """Register a handler for a specific action type."""
        if action in self._handlers:
            logger.warning("Overwriting existing handler for action: %s", action)
        self._handlers[action] = handler
        logger.info("Registered Trust Channel handler: %s", action)

    async def send_action(self, message: Dict[str, Any]) -> None:
        """Send a JSON message over the Trust Channel WebSocket."""
        if self._ws is None:
            raise ConnectionError("Trust Channel not connected")
        async with self._send_lock:
            await self._ws.send(json.dumps(message))

    async def wait_for_action(
        self, action: str, transfer_id: str, timeout: float = 30.0
    ) -> Dict[str, Any]:
        """
        Wait for a specific action+transfer_id message from the server.
        Used for ACK waiting during chunk streaming.

        Returns the parsed message dict, or raises TimeoutError.
        """
        waiter_key = f"{action}:{transfer_id}"
        future: asyncio.Future[Dict[str, Any]] = asyncio.get_event_loop().create_future()
        self._waiters[waiter_key] = future
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Timed out waiting for {action} (transfer_id={transfer_id})")
        finally:
            self._waiters.pop(waiter_key, None)

    async def run(self) -> None:
        """
        Main connection loop with automatic reconnection.
        Call this as an asyncio task during app lifespan.
        """
        self._running = True
        backoff = _INITIAL_BACKOFF_S

        while self._running:
            try:
                await self._connect_and_listen()
                # If _connect_and_listen returns normally, reset backoff
                backoff = _INITIAL_BACKOFF_S
            except (ConnectionClosed, ConnectionClosedError, OSError) as e:
                logger.warning("Trust Channel disconnected: %s", e)
            except InvalidStatusCode as e:
                logger.error("Trust Channel connection rejected (HTTP %s)", e.status_code)
                if e.status_code in (401, 403):
                    logger.error("Auth failure — check VECTORAIZ_INTERNAL_API_KEY")
                    # Don't retry rapidly on auth failures
                    backoff = min(backoff * _BACKOFF_MULTIPLIER, _MAX_BACKOFF_S)
            except Exception as e:
                logger.error("Trust Channel unexpected error: %s", e, exc_info=True)

            if not self._running:
                break

            logger.info("Reconnecting to Trust Channel in %.0fs...", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * _BACKOFF_MULTIPLIER, _MAX_BACKOFF_S)

    async def _connect_and_listen(self) -> None:
        """Connect, authenticate, and enter the message dispatch loop."""
        api_key = settings.internal_api_key
        if not api_key:
            logger.error("Cannot connect to Trust Channel — no VECTORAIZ_INTERNAL_API_KEY")
            raise ConnectionError("No API key for Trust Channel")

        headers = {"X-API-Key": api_key}
        logger.info("Connecting to Trust Channel: %s", self._ws_url)

        async with websockets.connect(
            self._ws_url,
            additional_headers=headers,
            ping_interval=30,
            ping_timeout=10,
            max_size=2 * 1024 * 1024,  # 2MB max message (base64 chunks)
        ) as ws:
            self._ws = ws
            logger.info("Trust Channel connected")

            async for raw_message in ws:
                try:
                    message = json.loads(raw_message)
                except (json.JSONDecodeError, TypeError):
                    logger.warning("Non-JSON message on Trust Channel: %s", raw_message[:200])
                    continue

                action = message.get("action", "")

                # Check if any waiter is waiting for this specific message
                transfer_id = message.get("transfer_id", "")
                waiter_key = f"{action}:{transfer_id}"
                if waiter_key in self._waiters and not self._waiters[waiter_key].done():
                    self._waiters[waiter_key].set_result(message)
                    continue

                # Dispatch to registered handler
                handler = self._handlers.get(action)
                if handler:
                    # Run handler as a task so we don't block the receive loop
                    asyncio.create_task(self._safe_handle(action, handler, message))
                else:
                    logger.debug("No handler for Trust Channel action: %s", action)

        # Connection closed normally
        self._ws = None

    async def _safe_handle(
        self, action: str, handler: ActionHandler, message: Dict[str, Any]
    ) -> None:
        """Run a handler with error isolation."""
        try:
            await handler(message)
        except Exception as e:
            logger.error(
                "Handler for %s raised: %s", action, e, exc_info=True
            )

    async def stop(self) -> None:
        """Gracefully stop the client."""
        self._running = False
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        # Cancel any pending waiters
        for future in self._waiters.values():
            if not future.done():
                future.cancel()
        logger.info("Trust Channel client stopped")


# Module-level singleton
_client: Optional[TrustChannelClient] = None


def get_trust_channel_client() -> TrustChannelClient:
    """Get or create the Trust Channel client singleton."""
    global _client
    if _client is None:
        _client = TrustChannelClient()
    return _client
