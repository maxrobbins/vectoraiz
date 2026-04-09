"""
Tests for SerialStore — persistence, atomic writes, state transitions.

BQ-VZ-SERIAL-CLIENT
"""

import json
import os

import pytest

from app.services.serial_store import (
    ACTIVE,
    DEGRADED,
    FAILURE_THRESHOLD,
    MIGRATED,
    PROVISIONED,
    UNPROVISIONED,
    SerialStore,
)


@pytest.fixture
def tmp_serial_dir(tmp_path):
    """Create a temp directory for serial.json."""
    return str(tmp_path)


@pytest.fixture
def store(tmp_serial_dir):
    """Create a SerialStore with a temp path."""
    path = os.path.join(tmp_serial_dir, "serial.json")
    return SerialStore(path=path)


class TestSerialStoreInit:
    def test_starts_unprovisioned(self, store):
        assert store.state.state == UNPROVISIONED
        assert store.state.serial == ""
        assert store.state.install_token is None

    def test_loads_existing_state(self, tmp_serial_dir):
        path = os.path.join(tmp_serial_dir, "serial.json")
        data = {
            "serial": "VZ-abcd1234-efgh5678",
            "install_token": "vzit_test123",
            "bootstrap_token": None,
            "state": "active",
            "last_app_version": "1.0.0",
            "last_status_cache": {"setup_remaining_usd": "8.00"},
            "last_status_at": "2026-02-24T00:00:00Z",
            "consecutive_failures": 0,
        }
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f)

        store = SerialStore(path=path)
        assert store.state.serial == "VZ-abcd1234-efgh5678"
        assert store.state.state == ACTIVE
        assert store.state.install_token == "vzit_test123"

    def test_invalid_state_resets_to_unprovisioned(self, tmp_serial_dir):
        path = os.path.join(tmp_serial_dir, "serial.json")
        data = {"serial": "VZ-test", "state": "bogus"}
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f)

        store = SerialStore(path=path)
        assert store.state.state == UNPROVISIONED


class TestAtomicWrites:
    def test_save_creates_file(self, store, tmp_serial_dir):
        store.state.serial = "VZ-test1234-test5678"
        store.state.state = PROVISIONED
        store.save()

        path = os.path.join(tmp_serial_dir, "serial.json")
        assert os.path.exists(path)
        with open(path) as f:
            data = json.load(f)
        assert data["serial"] == "VZ-test1234-test5678"
        assert data["state"] == PROVISIONED

    def test_save_chmod_600(self, store, tmp_serial_dir):
        store.save()
        path = os.path.join(tmp_serial_dir, "serial.json")
        mode = oct(os.stat(path).st_mode)[-3:]
        assert mode == "600"

    def test_save_survives_reload(self, tmp_serial_dir):
        path = os.path.join(tmp_serial_dir, "serial.json")
        store1 = SerialStore(path=path)
        store1.state.serial = "VZ-persist-test1234"
        store1.state.state = ACTIVE
        store1.state.install_token = "vzit_abc"
        store1.save()

        store2 = SerialStore(path=path)
        assert store2.state.serial == "VZ-persist-test1234"
        assert store2.state.state == ACTIVE
        assert store2.state.install_token == "vzit_abc"


class TestStateTransitions:
    def test_transition_to_active(self, store):
        store.state.serial = "VZ-test"
        store.state.bootstrap_token = "vzbt_boot"
        store.state.state = PROVISIONED
        store.transition_to_active("vzit_install")

        assert store.state.state == ACTIVE
        assert store.state.install_token == "vzit_install"
        assert store.state.bootstrap_token is None  # Deleted after activation

    def test_record_failure_triggers_degraded(self, store):
        store.state.state = ACTIVE
        store.state.serial = "VZ-test"
        store.save()

        for _ in range(FAILURE_THRESHOLD):
            store.record_failure()

        assert store.state.state == DEGRADED
        assert store.state.consecutive_failures == FAILURE_THRESHOLD

    def test_record_success_clears_degraded(self, store):
        store.state.state = DEGRADED
        store.state.serial = "VZ-test"
        store.state.consecutive_failures = 5
        store.save()

        store.record_success()
        assert store.state.state == ACTIVE
        assert store.state.consecutive_failures == 0

    def test_transition_to_migrated(self, store):
        store.state.state = ACTIVE
        store.state.serial = "VZ-test"
        store.state.last_status_cache = {}
        store.save()

        store.transition_to_migrated("user_123")
        assert store.state.state == MIGRATED
        assert store.state.last_status_cache.get("gateway_user_id") == "user_123"

    def test_transition_to_unprovisioned(self, store):
        store.state.state = ACTIVE
        store.state.serial = "VZ-test"
        store.state.install_token = "vzit_test"
        store.save()

        store.transition_to_unprovisioned()
        assert store.state.state == UNPROVISIONED
        assert store.state.install_token is None
        assert store.state.bootstrap_token is None
