"""
BQ-VZ-SHARED-SEARCH: Portal Models — Access Code Validator & Config Store
==========================================================================

Mandate M5: Access code hardening (min 6 chars alphanumeric, bcrypt, rate limiting).
Config persisted to /data/portal_config.json (same pattern as serial_store.py).
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List

import bcrypt

from app.config import settings
from app.schemas.portal import PortalConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate limiting (in-memory, resets on restart — acceptable for local VZ)
# ---------------------------------------------------------------------------
_access_attempts: Dict[str, List[datetime]] = {}
MAX_ATTEMPTS = 5
WINDOW_MINUTES = 15


class AccessCodeValidator:
    """Shared access code validation and hashing (Mandate M5)."""

    @staticmethod
    def validate_strength(code: str) -> bool:
        """Min 6 chars, alphanumeric. No pure-numeric PINs."""
        if len(code) < 6:
            return False
        if code.isdigit():
            return False  # Must include at least one letter
        return code.isalnum()

    @staticmethod
    def hash_code(code: str) -> str:
        return bcrypt.hashpw(code.encode(), bcrypt.gensalt()).decode()

    @staticmethod
    def verify_code(code: str, hashed: str) -> bool:
        return bcrypt.checkpw(code.encode(), hashed.encode())

    @staticmethod
    def check_rate_limit(ip: str) -> bool:
        """5 attempts per 15 minutes per IP. Returns True if allowed."""
        now = datetime.utcnow()
        cutoff = now - timedelta(minutes=WINDOW_MINUTES)
        attempts = _access_attempts.get(ip, [])
        attempts = [t for t in attempts if t > cutoff]
        _access_attempts[ip] = attempts
        return len(attempts) < MAX_ATTEMPTS

    @staticmethod
    def record_attempt(ip: str):
        now = datetime.utcnow()
        _access_attempts.setdefault(ip, []).append(now)

    @staticmethod
    def clear_rate_limits():
        """Clear all rate limit state (for testing)."""
        _access_attempts.clear()

    @staticmethod
    def invalidate_sessions_on_rotation(config: PortalConfig) -> PortalConfig:
        """Increment portal_session_version, clear active sessions (SS-C1)."""
        config.portal_session_version += 1
        config.active_sessions.clear()
        return config


# ---------------------------------------------------------------------------
# Portal Config Store — persisted JSON file
# ---------------------------------------------------------------------------
_PORTAL_CONFIG_PATH = Path(settings.data_directory) / "portal_config.json"
_cached_config: Optional[PortalConfig] = None


def get_portal_config() -> PortalConfig:
    """Load portal config from disk, or return defaults."""
    global _cached_config
    if _cached_config is not None:
        return _cached_config

    if _PORTAL_CONFIG_PATH.exists():
        try:
            data = json.loads(_PORTAL_CONFIG_PATH.read_text())
            _cached_config = PortalConfig(**data)
            logger.info("Loaded portal config from %s", _PORTAL_CONFIG_PATH)
            return _cached_config
        except Exception as e:
            logger.warning("Failed to load portal config: %s", e)

    _cached_config = PortalConfig()
    return _cached_config


def save_portal_config(config: PortalConfig) -> None:
    """Persist portal config to disk."""
    global _cached_config
    _cached_config = config
    try:
        _PORTAL_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        _PORTAL_CONFIG_PATH.write_text(
            json.dumps(config.model_dump(), indent=2, default=str)
        )
        logger.info("Saved portal config to %s", _PORTAL_CONFIG_PATH)
    except OSError as e:
        logger.error("Failed to save portal config: %s", e)
        raise


def reset_portal_config_cache():
    """Reset in-memory cache (for testing)."""
    global _cached_config
    _cached_config = None
