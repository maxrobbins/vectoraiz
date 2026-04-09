"""
Tests for SerialClient — HTTP mocking, retries.

BQ-VZ-SERIAL-CLIENT
"""

import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, patch, MagicMock

import httpx

from app.services.serial_client import (
    SerialClient,
)


@pytest.fixture
def client():
    return SerialClient(base_url="https://test.ai.market", timeout=2.0)


class TestActivate:
    @pytest.mark.asyncio
    async def test_activate_success(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"install_token": "vzit_new_token"}

        with patch("httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.request = AsyncMock(return_value=mock_resp)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            result = await client.activate(
                serial="VZ-test1234-test5678",
                bootstrap_token="vzbt_boot",
                instance_id="vz-testhost",
                hostname="testhost",
                version="1.0.0",
            )

        assert result.success is True
        assert result.install_token == "vzit_new_token"

    @pytest.mark.asyncio
    async def test_activate_401(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.json.return_value = {"detail": "Invalid bootstrap token"}

        with patch("httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.request = AsyncMock(return_value=mock_resp)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            result = await client.activate(
                serial="VZ-test", bootstrap_token="vzbt_bad",
                instance_id="vz-test", hostname="test", version="1.0.0",
            )

        assert result.success is False
        assert result.status_code == 401

    @pytest.mark.asyncio
    async def test_activate_network_retry(self, client):
        """Network error should retry and eventually fail."""
        with patch("httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.request = AsyncMock(side_effect=httpx.ConnectError("refused"))
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            result = await client.activate(
                serial="VZ-test", bootstrap_token="vzbt_test",
                instance_id="vz-test", hostname="test", version="1.0.0",
            )

        assert result.success is False
        # Should have retried (3 total attempts: 1 + 2 retries)
        assert mock_instance.request.call_count == 3


class TestMeter:
    @pytest.mark.asyncio
    async def test_meter_allowed(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "allowed": True,
            "category": "data",
            "cost_usd": "0.0300",
            "remaining_usd": "3.9700",
            "reason": None,
            "payment_enabled": False,
            "migrated": False,
        }

        with patch("httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.request = AsyncMock(return_value=mock_resp)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            result = await client.meter(
                serial="VZ-test", install_token="vzit_test",
                category="data", cost_usd=Decimal("0.03"),
                request_id="vz:test:abc:123",
            )

        assert result.allowed is True
        assert result.remaining_usd == "3.9700"

    @pytest.mark.asyncio
    async def test_meter_denied(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 402
        mock_resp.json.return_value = {
            "allowed": False,
            "category": "data",
            "cost_usd": "0.0300",
            "remaining_usd": "0.0000",
            "reason": "insufficient_data_credits",
            "payment_enabled": False,
            "migrated": False,
        }

        with patch("httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.request = AsyncMock(return_value=mock_resp)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            result = await client.meter(
                serial="VZ-test", install_token="vzit_test",
                category="data", cost_usd=Decimal("0.03"),
                request_id="vz:test:abc:456",
            )

        assert result.allowed is False
        assert result.reason == "insufficient_data_credits"


class TestStatus:
    @pytest.mark.asyncio
    async def test_status_success(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "setup_remaining_usd": "8.00",
            "data_remaining_usd": "3.50",
            "migrated": False,
        }

        with patch("httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.request = AsyncMock(return_value=mock_resp)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            result = await client.status("VZ-test", "vzit_test")

        assert result.success is True
        assert result.data["setup_remaining_usd"] == "8.00"

    @pytest.mark.asyncio
    async def test_status_migrated(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "migrated": True,
            "gateway_user_id": "gw_user_123",
        }

        with patch("httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.request = AsyncMock(return_value=mock_resp)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            result = await client.status("VZ-test", "vzit_test")

        assert result.migrated is True


class TestRefresh:
    @pytest.mark.asyncio
    async def test_refresh_success(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"install_token": "vzit_refreshed"}

        with patch("httpx.AsyncClient") as MockClient:
            mock_instance = AsyncMock()
            mock_instance.request = AsyncMock(return_value=mock_resp)
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_instance

            result = await client.refresh("VZ-test", "vzit_old", "vz-host")

        assert result.success is True
        assert result.install_token == "vzit_refreshed"
