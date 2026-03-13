"""
Database Credential Service
============================

Fernet encrypt/decrypt for database connection passwords.
Reuses the same SECRET_KEY infrastructure as LLM API keys (BQ-125).

Phase: BQ-VZ-DB-CONNECT
Created: 2026-02-25
"""

import base64
import hashlib
import logging
from cryptography.fernet import Fernet

from app.config import settings

logger = logging.getLogger(__name__)


def _ensure_fernet_key(raw_key: str) -> str:
    """Ensure we have a valid Fernet key, deriving one if necessary."""
    try:
        Fernet(raw_key.encode() if isinstance(raw_key, str) else raw_key)
        return raw_key
    except (ValueError, Exception):
        pass

    logger.warning(
        "VECTORAIZ_SECRET_KEY is not a valid Fernet key — deriving one. "
        "For best practice, generate a proper key: "
        'python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"'
    )
    derived = hashlib.sha256(raw_key.encode()).digest()
    return base64.urlsafe_b64encode(derived).decode()


def _get_fernet() -> Fernet:
    """Return a Fernet instance using the app's SECRET_KEY."""
    key = _ensure_fernet_key(settings.get_secret_key())
    return Fernet(key.encode())


def encrypt_password(plaintext: str) -> str:
    """Encrypt a database password. Returns a URL-safe base64 token string."""
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt_password(token: str) -> str:
    """Decrypt a database password from its Fernet token."""
    return _get_fernet().decrypt(token.encode()).decode()
