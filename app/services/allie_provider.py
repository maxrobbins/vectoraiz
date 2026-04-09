"""
Allie LLM Provider — Mock + Real (BQ-128)
==========================================

Two-path architecture for Allie's LLM calls:
1. MockAllieProvider — deterministic responses with simulated streaming
   (used in Phase 1, tests, and dev)
2. AiMarketAllieProvider — real ai.market proxy → Claude (SSE streaming)

The active provider is selected via VECTORAIZ_ALLIE_PROVIDER env var:
- "mock" (default) — MockAllieProvider
- "aimarket" — AiMarketAllieProvider (requires VECTORAIZ_INTERNAL_API_KEY)

Usage:
    from app.services.allie_provider import get_allie_provider

    provider = get_allie_provider()
    async for chunk in provider.stream("Hello"):
        print(chunk.text)  # token-by-token
    # final chunk has .usage set
"""

import asyncio
import json
import os
import logging
import tempfile
import uuid
from dataclasses import dataclass
from typing import AsyncIterator, Optional

import httpx

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AllieUsage:
    """Token usage from an Allie LLM call."""
    input_tokens: int
    output_tokens: int
    cost_cents: int
    provider: str
    model: str


@dataclass
class AllieStreamChunk:
    """A single chunk from an Allie streaming response."""
    text: str
    done: bool = False
    usage: Optional[AllieUsage] = None


class AllieDisabledError(Exception):
    """Raised when Allie is called in standalone mode."""


class AllieTimeoutError(Exception):
    """Raised when an Allie LLM call times out (distinct from connection errors)."""


class InsufficientBalanceError(Exception):
    """Raised when user's balance is insufficient for Allie."""


class RateLimitExceededError(Exception):
    """Raised when user exceeds rate limit."""
    def __init__(self, reset_at: Optional[str] = None):
        self.reset_at = reset_at
        super().__init__(f"Rate limit exceeded, resets at {reset_at}")


class DuplicateMessageError(Exception):
    """Raised when a duplicate client_message_id is detected."""
    def __init__(self, client_message_id: str):
        self.client_message_id = client_message_id
        super().__init__(f"Duplicate message: {client_message_id}")


class BaseAllieProvider:
    """Abstract base for Allie LLM providers."""

    @property
    def supports_vision(self) -> bool:
        """Whether this provider's model supports image/vision input."""
        return False

    async def stream(
        self, message: str, context: Optional[str] = None, attachments: Optional[list] = None,
        chat_history: Optional[list] = None,
    ) -> AsyncIterator[AllieStreamChunk]:
        """Stream a response token-by-token. Final chunk has done=True and usage set."""
        raise NotImplementedError
        yield  # make it an async generator  # noqa: unreachable


class MockAllieProvider(BaseAllieProvider):
    """
    Mock provider for Phase 1 development and testing.
    Returns deterministic responses with simulated streaming delay.
    """

    MOCK_RESPONSES = {
        "default": "I can help you explore your data. I can see your datasets and help you understand patterns, run queries, and find insights. What would you like to know?",
        "hello": "Hello! I'm Allie, your AI data assistant. I can help you explore datasets, run queries, and find insights in your data. How can I help?",
        "help": "Here's what I can help with:\n- **Data exploration**: Browse and understand your datasets\n- **SQL queries**: Write and run queries against your data\n- **Semantic search**: Find relevant information using natural language\n- **Data quality**: Check for PII, missing values, and anomalies",
    }

    MODEL = "mock-allie-v1"
    PROVIDER = "mock"
    CHUNK_DELAY = 0.02  # 20ms between tokens (simulates streaming)

    def _pick_response(self, message: str) -> str:
        msg_lower = message.lower().strip()
        if any(g in msg_lower for g in ("hello", "hi", "hey")):
            return self.MOCK_RESPONSES["hello"]
        if any(h in msg_lower for h in ("help", "what can you")):
            return self.MOCK_RESPONSES["help"]
        return self.MOCK_RESPONSES["default"]

    async def stream(
        self, message: str, context: Optional[str] = None, attachments: Optional[list] = None,
        chat_history: Optional[list] = None,
    ) -> AsyncIterator[AllieStreamChunk]:
        """Stream mock response word-by-word."""
        response = self._pick_response(message)
        words = response.split(" ")
        input_tokens = max(1, len(message) // 4)
        output_tokens = max(1, len(response) // 4)

        for i, word in enumerate(words):
            text = word if i == 0 else " " + word
            await asyncio.sleep(self.CHUNK_DELAY)
            yield AllieStreamChunk(text=text)

        # Final chunk with usage
        yield AllieStreamChunk(
            text="",
            done=True,
            usage=AllieUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost_cents=max(1, (input_tokens + output_tokens) // 100),
                provider=self.PROVIDER,
                model=self.MODEL,
            ),
        )


class AiMarketAllieProvider(BaseAllieProvider):
    """Real provider: proxies through ai.market to Claude via SSE."""

    PROVIDER = "aimarket"

    @property
    def supports_vision(self) -> bool:
        return True

    def __init__(
        self,
        *,
        serial: Optional[str] = None,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
    ):
        from app.config import settings
        self.base_url = (base_url or settings.ai_market_url).rstrip("/")
        self.api_key = api_key or settings.internal_api_key
        if not self.api_key:
            raise ValueError("API key required for AiMarketAllieProvider")
        self.serial = serial or settings.serial
        self.timeout = httpx.Timeout(180, connect=10)  # generous timeout for streaming responses

    async def stream(
        self, message: str, context: Optional[str] = None, attachments: Optional[list] = None,
        chat_history: Optional[list] = None,
    ) -> AsyncIterator[AllieStreamChunk]:
        """Stream response from ai.market Allie proxy via SSE."""
        messages = []
        if context:
            messages.append({"role": "user", "content": context})

        # Inject conversation history before the current message
        if chat_history:
            for msg in chat_history:
                messages.append({"role": msg["role"], "content": msg["content"]})

        # BQ-ALLAI-FILES: Build user content with attachment blocks
        if attachments:
            from app.services.attachment_blocks import build_user_content
            user_content = build_user_content(message, attachments, supports_vision=self.supports_vision)
            messages.append({"role": "user", "content": user_content})
        else:
            messages.append({"role": "user", "content": message})

        request_id = f"req_{uuid.uuid4().hex[:12]}"
        url = f"{self.base_url}/api/v1/allie/chat"
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key,
            "Accept": "text/event-stream",
        }
        if self.serial:
            headers["X-Serial"] = self.serial
        body = {
            "messages": messages,
            "request_id": request_id,
        }

        model = ""

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                async with client.stream("POST", url, json=body, headers=headers) as response:
                    if response.status_code == 401:
                        raise AllieDisabledError("ai.market authentication failed (invalid API key)")
                    elif response.status_code == 402:
                        raise InsufficientBalanceError("Insufficient balance on ai.market")
                    elif response.status_code == 403:
                        raise AllieDisabledError("API key missing allie:chat scope")
                    elif response.status_code == 429:
                        raise RateLimitExceededError()
                    elif response.status_code != 200:
                        text = ""
                        async for chunk in response.aiter_text():
                            text += chunk
                        raise AllieDisabledError(f"ai.market error ({response.status_code}): {text[:200]}")

                    event_type = ""
                    async for line in response.aiter_lines():
                        line = line.strip()
                        if not line:
                            event_type = ""
                            continue
                        if line.startswith("event: "):
                            event_type = line[7:]
                            continue
                        if not line.startswith("data: "):
                            continue

                        try:
                            data = json.loads(line[6:])
                        except json.JSONDecodeError:
                            continue

                        if event_type == "start":
                            model = data.get("model", "unknown")
                        elif event_type == "delta":
                            yield AllieStreamChunk(text=data.get("text", ""))
                        elif event_type == "done":
                            usage_data = data.get("usage", {})
                            yield AllieStreamChunk(
                                text="",
                                done=True,
                                usage=AllieUsage(
                                    input_tokens=usage_data.get("input_tokens", 0),
                                    output_tokens=usage_data.get("output_tokens", 0),
                                    cost_cents=data.get("cost_cents", 0),
                                    provider=self.PROVIDER,
                                    model=model,
                                ),
                            )
                        elif event_type == "error":
                            error_msg = data.get("message", "Unknown ai.market error")
                            if data.get("retryable"):
                                raise AllieDisabledError(f"ai.market error (retryable): {error_msg}")
                            else:
                                raise AllieDisabledError(f"ai.market error: {error_msg}")
        except httpx.TimeoutException as e:
            raise AllieTimeoutError(
                "Request timed out — the query may be too complex. Try a simpler question."
            ) from e


_provider_instance: Optional[BaseAllieProvider] = None

ALLIE_CONFIG_FILENAME = "allie_config.json"


def _allie_config_path() -> str:
    """Path to the allie credentials config file."""
    from app.config import settings
    return os.path.join(settings.serial_data_dir, ALLIE_CONFIG_FILENAME)


def write_allie_config(serial_number: str, install_token: str, ai_market_url: str) -> None:
    """Persist allAI credentials after serial activation or token refresh.

    Uses atomic write (tmp → fsync → rename) with chmod 600.
    """
    path = _allie_config_path()
    data = {
        "serial_number": serial_number,
        "install_token": install_token,
        "ai_market_url": ai_market_url,
        "provider": "aimarket",
    }
    dir_path = os.path.dirname(path)
    os.makedirs(dir_path, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp_path, 0o600)
        os.rename(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    logger.info(
        "Wrote allie config: provider=aimarket, serial=%s...",
        serial_number[:16] if serial_number else "?",
    )


def read_allie_config() -> Optional[dict]:
    """Read allie config from disk. Returns None if missing or invalid."""
    path = _allie_config_path()
    try:
        with open(path) as f:
            data = json.load(f)
        if (
            data.get("provider") == "aimarket"
            and data.get("install_token")
            and data.get("serial_number")
        ):
            return data
        return None
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return None


def get_allie_provider() -> BaseAllieProvider:
    """Get the configured Allie LLM provider (singleton).

    Resolution order:
    1. VECTORAIZ_ALLIE_PROVIDER env var (explicit override)
    2. /data/allie_config.json from serial activation
    3. MockProvider fallback
    """
    global _provider_instance
    if _provider_instance is None:
        provider_type = os.environ.get("VECTORAIZ_ALLIE_PROVIDER", "").strip()

        if provider_type == "aimarket":
            _provider_instance = AiMarketAllieProvider()
            logger.info("Allie provider: AiMarketAllieProvider (env) → %s", _provider_instance.base_url)
        elif provider_type == "mock":
            _provider_instance = MockAllieProvider()
            logger.info("Allie provider: MockAllieProvider (env)")
        elif not provider_type:
            # No explicit env var — check config file written by serial activation
            config = read_allie_config()
            if config:
                _provider_instance = AiMarketAllieProvider(
                    serial=config["serial_number"],
                    api_key=config["install_token"],
                    base_url=config.get("ai_market_url"),
                )
                logger.info("Allie provider: AiMarketAllieProvider (config) → %s", _provider_instance.base_url)
            else:
                _provider_instance = MockAllieProvider()
                logger.info("Allie provider: MockAllieProvider (default)")
        else:
            logger.warning("Unknown ALLIE_PROVIDER=%s, falling back to mock", provider_type)
            _provider_instance = MockAllieProvider()
    return _provider_instance


def reset_provider() -> None:
    """Reset provider singleton so next get_allie_provider() re-evaluates config."""
    global _provider_instance
    _provider_instance = None
