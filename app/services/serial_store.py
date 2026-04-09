"""
Serial Store — Persistent local state for serial activation.
=============================================================

Stores serial activation state in /data/serial.json with atomic writes
(tmp + fsync + rename) and chmod 600 for security.

BQ-VZ-SERIAL-CLIENT
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

from app.config import settings

logger = logging.getLogger(__name__)

# Valid states
UNPROVISIONED = "unprovisioned"
PROVISIONED = "provisioned"
ACTIVE = "active"
DEGRADED = "degraded"
MIGRATED = "migrated"

VALID_STATES = {UNPROVISIONED, PROVISIONED, ACTIVE, DEGRADED, MIGRATED}
FAILURE_THRESHOLD = 3


@dataclass
class SerialState:
    serial: str = ""
    install_token: Optional[str] = None
    bootstrap_token: Optional[str] = None
    state: str = UNPROVISIONED
    last_app_version: Optional[str] = None
    last_status_cache: Optional[dict] = None
    last_status_at: Optional[str] = None
    consecutive_failures: int = 0


class SerialStore:
    """Persistent serial state backed by a JSON file with atomic writes."""

    def __init__(self, path: Optional[str] = None):
        self._path = Path(path or os.path.join(settings.serial_data_dir, "serial.json"))
        self._state: SerialState = SerialState()
        self._load()

    @property
    def state(self) -> SerialState:
        return self._state

    def _load(self) -> None:
        if not self._path.exists():
            # Env var fallback: if VECTORAIZ_SERIAL + VECTORAIZ_BOOTSTRAP_TOKEN
            # are set (e.g. via .env / compose), create serial.json from them.
            env_serial = os.environ.get("VECTORAIZ_SERIAL", "").strip()
            env_bootstrap = os.environ.get("VECTORAIZ_BOOTSTRAP_TOKEN", "").strip()
            if env_serial and env_bootstrap:
                logger.info("No serial.json found — creating from env vars VECTORAIZ_SERIAL / VECTORAIZ_BOOTSTRAP_TOKEN")
                self._state = SerialState(
                    serial=env_serial,
                    bootstrap_token=env_bootstrap,
                    state=PROVISIONED,
                )
                self.save()
                return
            logger.info("No serial.json found at %s — starting unprovisioned", self._path)
            return
        try:
            raw = json.loads(self._path.read_text())
            self._state = SerialState(
                serial=raw.get("serial", ""),
                install_token=raw.get("install_token"),
                bootstrap_token=raw.get("bootstrap_token"),
                state=raw.get("state", UNPROVISIONED),
                last_app_version=raw.get("last_app_version"),
                last_status_cache=raw.get("last_status_cache"),
                last_status_at=raw.get("last_status_at"),
                consecutive_failures=raw.get("consecutive_failures", 0),
            )
            if self._state.state not in VALID_STATES:
                logger.warning("Invalid state '%s' in serial.json — resetting to unprovisioned", self._state.state)
                self._state.state = UNPROVISIONED
            logger.info("Loaded serial state: serial=%s state=%s", self._state.serial[:16] if self._state.serial else "?", self._state.state)
        except Exception as e:
            logger.error("Failed to load serial.json: %s — starting unprovisioned", e)

    def save(self) -> None:
        """Atomic write: tmp → fsync → rename. chmod 600."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = asdict(self._state)
        fd, tmp_path = tempfile.mkstemp(dir=str(self._path.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.chmod(tmp_path, 0o600)
            os.rename(tmp_path, str(self._path))
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def record_success(self) -> None:
        """Record a successful server call. Transition DEGRADED → ACTIVE."""
        self._state.consecutive_failures = 0
        if self._state.state == DEGRADED:
            self._state.state = ACTIVE
            logger.info("Serial state: DEGRADED → ACTIVE (server reachable)")
        self.save()

    def record_failure(self) -> None:
        """Record a failed server call. Transition ACTIVE → DEGRADED after threshold."""
        self._state.consecutive_failures += 1
        if (
            self._state.state == ACTIVE
            and self._state.consecutive_failures >= FAILURE_THRESHOLD
        ):
            self._state.state = DEGRADED
            logger.warning(
                "Serial state: ACTIVE → DEGRADED (%d consecutive failures)",
                self._state.consecutive_failures,
            )
        self.save()

    def transition_to_active(self, install_token: str) -> None:
        """PROVISIONED → ACTIVE after successful activation."""
        self._state.install_token = install_token
        self._state.bootstrap_token = None  # Security: delete bootstrap token
        self._state.state = ACTIVE
        self._state.consecutive_failures = 0
        self.save()
        logger.info("Serial state: PROVISIONED → ACTIVE")

    def transition_to_migrated(self, gateway_user_id: Optional[str] = None) -> None:
        """ACTIVE → MIGRATED when billing_mode=ledger."""
        self._state.state = MIGRATED
        if gateway_user_id and self._state.last_status_cache is not None:
            self._state.last_status_cache["gateway_user_id"] = gateway_user_id
        self.save()
        logger.info("Serial state: → MIGRATED")

    def transition_to_unprovisioned(self) -> None:
        """Any → UNPROVISIONED (token revoked)."""
        self._state.install_token = None
        self._state.bootstrap_token = None
        self._state.state = UNPROVISIONED
        self._state.consecutive_failures = 0
        self.save()
        logger.warning("Serial state: → UNPROVISIONED (token revoked)")

    def update_status_cache(self, status_data: dict, timestamp: str) -> None:
        """Cache the latest status response for UI display."""
        self._state.last_status_cache = status_data
        self._state.last_status_at = timestamp
        self.save()

    def update_app_version(self, version: str) -> None:
        self._state.last_app_version = version
        self.save()


# ---------------------------------------------------------------------------
# Module-level singleton + FastAPI dependency
# ---------------------------------------------------------------------------
_store: Optional[SerialStore] = None


def get_serial_store() -> SerialStore:
    global _store
    if _store is None:
        _store = SerialStore()
    return _store
