"""
Tests for ActivationManager — boot from each state.

BQ-VZ-SERIAL-CLIENT
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.services.activation_manager import ActivationManager
from app.services.serial_store import (
    ACTIVE,
    MIGRATED,
    PROVISIONED,
    UNPROVISIONED,
    SerialStore,
)
from app.services.serial_client import (
    ActivateResult,
    RefreshResult,
    SerialClient,
)


@pytest.fixture
def mock_store(tmp_path):
    path = str(tmp_path / "serial.json")
    return SerialStore(path=path)


@pytest.fixture
def mock_client():
    client = AsyncMock(spec=SerialClient)
    return client


class TestStartupUnprovisioned:
    @pytest.mark.asyncio
    async def test_unprovisioned_standalone_does_nothing(self, mock_store, mock_client):
        """In standalone mode, auto-provision is skipped."""
        mock_store.state.state = UNPROVISIONED
        mgr = ActivationManager(store=mock_store, client=mock_client)

        with patch("app.services.activation_manager.settings") as mock_settings:
            mock_settings.mode = "standalone"
            mock_settings.app_version = "dev"
            await mgr.startup()
        await mgr.shutdown()

        mock_client.activate.assert_not_called()
        mock_client.refresh.assert_not_called()

    @pytest.mark.asyncio
    async def test_unprovisioned_connected_auto_provisions(self, mock_store, mock_client):
        """In connected mode, auto-provision is attempted."""
        mock_store.state.state = UNPROVISIONED
        mgr = ActivationManager(store=mock_store, client=mock_client)

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            "serial": "VZ-auto1234-auto5678",
            "bootstrap_token": "vzbt_auto",
        }

        mock_http_client = AsyncMock()
        mock_http_client.__aenter__ = AsyncMock(return_value=mock_http_client)
        mock_http_client.__aexit__ = AsyncMock(return_value=False)
        mock_http_client.post = AsyncMock(return_value=mock_response)

        mock_client.activate = AsyncMock(return_value=ActivateResult(
            success=True, install_token="vzit_auto", status_code=200,
        ))

        with patch("app.services.activation_manager.settings") as mock_settings, \
             patch("httpx.AsyncClient", return_value=mock_http_client):
            mock_settings.mode = "connected"
            mock_settings.aimarket_url = "https://ai-market-backend-production.up.railway.app"
            mock_settings.app_version = "dev"
            await mgr.startup()
        await mgr.shutdown()

        assert mock_store.state.serial == "VZ-auto1234-auto5678"
        assert mock_store.state.state == ACTIVE


class TestStartupProvisioned:
    @pytest.mark.asyncio
    async def test_activates_on_boot(self, mock_store, mock_client):
        mock_store.state.state = PROVISIONED
        mock_store.state.serial = "VZ-test1234-test5678"
        mock_store.state.bootstrap_token = "vzbt_boot"
        mock_store.save()

        mock_client.activate = AsyncMock(return_value=ActivateResult(
            success=True, install_token="vzit_new", status_code=200,
        ))

        mgr = ActivationManager(store=mock_store, client=mock_client)
        await mgr.startup()
        await mgr.shutdown()

        mock_client.activate.assert_called_once()
        assert mock_store.state.state == ACTIVE
        assert mock_store.state.install_token == "vzit_new"
        assert mock_store.state.bootstrap_token is None

    @pytest.mark.asyncio
    async def test_activation_failure_stays_provisioned(self, mock_store, mock_client):
        mock_store.state.state = PROVISIONED
        mock_store.state.serial = "VZ-test"
        mock_store.state.bootstrap_token = "vzbt_boot"
        mock_store.save()

        mock_client.activate = AsyncMock(return_value=ActivateResult(
            success=False, error="network error", status_code=0,
        ))

        mgr = ActivationManager(store=mock_store, client=mock_client)
        await mgr.startup()
        await mgr.shutdown()

        assert mock_store.state.state == PROVISIONED

    @pytest.mark.asyncio
    async def test_activation_401_goes_unprovisioned(self, mock_store, mock_client):
        mock_store.state.state = PROVISIONED
        mock_store.state.serial = "VZ-test"
        mock_store.state.bootstrap_token = "vzbt_bad"
        mock_store.save()

        mock_client.activate = AsyncMock(return_value=ActivateResult(
            success=False, error="invalid token", status_code=401,
        ))

        mgr = ActivationManager(store=mock_store, client=mock_client)
        await mgr.startup()
        await mgr.shutdown()

        assert mock_store.state.state == UNPROVISIONED


class TestStartupActive:
    @pytest.mark.asyncio
    async def test_no_refresh_if_same_version(self, mock_store, mock_client):
        mock_store.state.state = ACTIVE
        mock_store.state.serial = "VZ-test"
        mock_store.state.install_token = "vzit_existing"
        mock_store.state.last_app_version = "1.0.0"
        mock_store.save()

        with patch("app.services.activation_manager.settings") as mock_settings:
            mock_settings.app_version = "1.0.0"
            mgr = ActivationManager(store=mock_store, client=mock_client)
            await mgr.startup()
            await mgr.shutdown()

        mock_client.refresh.assert_not_called()

    @pytest.mark.asyncio
    async def test_refresh_on_version_change(self, mock_store, mock_client):
        mock_store.state.state = ACTIVE
        mock_store.state.serial = "VZ-test"
        mock_store.state.install_token = "vzit_old"
        mock_store.state.last_app_version = "1.0.0"
        mock_store.save()

        mock_client.refresh = AsyncMock(return_value=RefreshResult(
            success=True, install_token="vzit_new_v2", status_code=200,
        ))

        with patch("app.services.activation_manager.settings") as mock_settings:
            mock_settings.app_version = "2.0.0"
            mgr = ActivationManager(store=mock_store, client=mock_client)
            await mgr.startup()
            await mgr.shutdown()

        mock_client.refresh.assert_called_once()
        assert mock_store.state.install_token == "vzit_new_v2"


class TestStartupMigrated:
    @pytest.mark.asyncio
    async def test_migrated_skips_all(self, mock_store, mock_client):
        mock_store.state.state = MIGRATED
        mock_store.state.serial = "VZ-test"
        mock_store.save()

        mgr = ActivationManager(store=mock_store, client=mock_client)
        await mgr.startup()
        await mgr.shutdown()

        mock_client.activate.assert_not_called()
        mock_client.refresh.assert_not_called()
        mock_client.status.assert_not_called()
