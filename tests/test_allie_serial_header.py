"""
Tests for AiMarketAllieProvider X-Serial header support.

Verifies that:
- X-Serial header is sent when settings.serial is configured
- X-Serial header is omitted when settings.serial is None (gateway mode)
"""

import json
import pytest
from unittest.mock import patch, MagicMock


from app.services.allie_provider import (
    AiMarketAllieProvider,
    reset_provider,
)


@pytest.fixture(autouse=True)
def _reset():
    reset_provider()
    yield
    reset_provider()


def _make_settings(**overrides):
    """Return a mock settings object with sensible defaults."""
    defaults = {
        "ai_market_url": "https://test.example.com",
        "internal_api_key": "vzit_test_key_123",
        "serial": None,
    }
    defaults.update(overrides)
    s = MagicMock()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


def _sse_lines(events):
    """Build raw SSE text lines from a list of (event_type, data_dict)."""
    lines = []
    for evt, data in events:
        lines.append(f"event: {evt}")
        lines.append(f"data: {json.dumps(data)}")
        lines.append("")
    return lines


class _FakeResponse:
    """Minimal async-context-manager stand-in for httpx streaming response."""

    def __init__(self, status_code=200, events=None):
        self.status_code = status_code
        self._events = events or [
            ("start", {"model": "claude-test"}),
            ("delta", {"text": "hello"}),
            ("done", {"usage": {"input_tokens": 10, "output_tokens": 5}, "cost_cents": 1}),
        ]

    async def aiter_lines(self):
        for line in _sse_lines(self._events):
            yield line

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


class _FakeClient:
    """Captures the headers passed to client.stream()."""

    def __init__(self, response=None):
        self.response = response or _FakeResponse()
        self.captured_headers = None

    def stream(self, method, url, *, json=None, headers=None):
        self.captured_headers = headers
        return self.response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


# ---- Tests ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_x_serial_header_sent_when_serial_configured():
    """When settings.serial is set, X-Serial header must appear in the request."""
    settings = _make_settings(serial="SN-ABC-123")
    fake_client = _FakeClient()

    with patch("app.config.settings", settings):
        provider = AiMarketAllieProvider()

    with patch("httpx.AsyncClient", return_value=fake_client):
        chunks = []
        async for chunk in provider.stream("test message"):
            chunks.append(chunk)

    assert fake_client.captured_headers is not None
    assert fake_client.captured_headers["X-Serial"] == "SN-ABC-123"
    assert fake_client.captured_headers["X-API-Key"] == "vzit_test_key_123"
    # Should have received delta + done chunks
    assert any(c.done for c in chunks)


@pytest.mark.asyncio
async def test_x_serial_header_omitted_when_serial_none():
    """When settings.serial is None (gateway mode), X-Serial must NOT be in headers."""
    settings = _make_settings(serial=None)
    fake_client = _FakeClient()

    with patch("app.config.settings", settings):
        provider = AiMarketAllieProvider()

    with patch("httpx.AsyncClient", return_value=fake_client):
        async for _ in provider.stream("test message"):
            pass

    assert fake_client.captured_headers is not None
    assert "X-Serial" not in fake_client.captured_headers
    assert fake_client.captured_headers["X-API-Key"] == "vzit_test_key_123"


@pytest.mark.asyncio
async def test_x_serial_header_omitted_when_serial_empty_string():
    """Empty string serial should also be treated as unset."""
    settings = _make_settings(serial="")
    fake_client = _FakeClient()

    with patch("app.config.settings", settings):
        provider = AiMarketAllieProvider()

    with patch("httpx.AsyncClient", return_value=fake_client):
        async for _ in provider.stream("test message"):
            pass

    assert fake_client.captured_headers is not None
    assert "X-Serial" not in fake_client.captured_headers


def test_provider_stores_serial_from_settings():
    """AiMarketAllieProvider.__init__ should store serial from settings."""
    settings = _make_settings(serial="SN-XYZ-789")
    with patch("app.config.settings", settings):
        provider = AiMarketAllieProvider()
    assert provider.serial == "SN-XYZ-789"


def test_provider_stores_none_serial():
    """AiMarketAllieProvider.__init__ should store None when serial not set."""
    settings = _make_settings(serial=None)
    with patch("app.config.settings", settings):
        provider = AiMarketAllieProvider()
    assert provider.serial is None
