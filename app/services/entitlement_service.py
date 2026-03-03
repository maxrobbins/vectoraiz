"""
Entitlement Service
===================

Validates signed entitlement tokens for raw file downloads.
Tokens are HMAC-SHA256 signed by ai.market, validated by VZ.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-03
"""

import base64
import hashlib
import hmac
import json
import logging
import threading
import time
from typing import Dict, Optional

from app.config import settings

logger = logging.getLogger(__name__)

# Nonce TTL: 1 hour (matching token expiry)
_NONCE_TTL_SECONDS = 3600


def _derive_shared_secret() -> bytes:
    """
    Derive shared secret from install token via HKDF-SHA256.

    Uses the internal_api_key (install token) as input key material.
    Falls back to keystore_passphrase if internal_api_key is not set.
    """
    ikm = getattr(settings, "internal_api_key", None) or getattr(settings, "keystore_passphrase", None)
    if not ikm:
        raise RuntimeError("No install token or keystore passphrase configured for entitlement signing")

    # HKDF-Extract: PRK = HMAC-SHA256(salt, IKM)
    salt = b"vectoraiz-entitlement-v1"
    prk = hmac.new(salt, ikm.encode("utf-8"), hashlib.sha256).digest()

    # HKDF-Expand: OKM = HMAC-SHA256(PRK, info || 0x01)
    info = b"raw-file-download"
    okm = hmac.new(prk, info + b"\x01", hashlib.sha256).digest()
    return okm


class EntitlementService:
    """Validates signed entitlement tokens for raw file downloads."""

    def __init__(self):
        self._nonce_store: Dict[str, float] = {}  # nonce → expiry_timestamp
        self._lock = threading.Lock()

    def _cleanup_expired_nonces(self):
        """Remove expired nonces from the in-memory store."""
        now = time.time()
        with self._lock:
            expired = [n for n, exp in self._nonce_store.items() if exp < now]
            for n in expired:
                del self._nonce_store[n]

    def _check_and_store_nonce(self, nonce: str, expires_at: float) -> bool:
        """
        Check if nonce has been used; if not, store it.

        Returns True if nonce is fresh (not replayed).
        Returns False if nonce was already used.
        """
        self._cleanup_expired_nonces()
        with self._lock:
            if nonce in self._nonce_store:
                return False  # Replay detected
            self._nonce_store[nonce] = expires_at
            return True

    def validate_entitlement(self, authorization_header: str) -> dict:
        """
        Parse and validate an entitlement token from the Authorization header.

        Expected format: Bearer base64(<json_payload>).<base64_signature>

        The JSON payload contains:
            order_id, listing_id, file_hash, buyer_id, issued_at, expires_at, nonce

        Returns the decoded payload dict on success.

        Raises:
            ValueError: On any validation failure (expired, replayed, bad signature, etc.)
        """
        if not authorization_header or not authorization_header.startswith("Bearer "):
            raise ValueError("Missing or invalid Authorization header")

        token = authorization_header[len("Bearer "):]

        # Split payload.signature
        parts = token.split(".")
        if len(parts) != 2:
            raise ValueError("Malformed entitlement token")

        payload_b64, sig_b64 = parts

        # Verify HMAC-SHA256 signature
        try:
            secret = _derive_shared_secret()
        except RuntimeError as e:
            raise ValueError(f"Server configuration error: {e}")

        expected_sig = hmac.new(secret, payload_b64.encode("utf-8"), hashlib.sha256).digest()
        try:
            provided_sig = base64.urlsafe_b64decode(sig_b64 + "==")  # Pad for safety
        except Exception:
            raise ValueError("Invalid signature encoding")

        if not hmac.compare_digest(expected_sig, provided_sig):
            raise ValueError("Invalid entitlement token signature")

        # Decode payload
        try:
            payload_json = base64.urlsafe_b64decode(payload_b64 + "==")
            payload = json.loads(payload_json)
        except Exception:
            raise ValueError("Invalid entitlement token payload")

        # Validate required fields
        required = ("order_id", "listing_id", "file_hash", "buyer_id", "issued_at", "expires_at", "nonce")
        for field in required:
            if field not in payload:
                raise ValueError(f"Missing required field: {field}")

        # Check expiry
        now = time.time()
        try:
            expires_at = float(payload["expires_at"])
        except (ValueError, TypeError):
            raise ValueError("Invalid expires_at value")

        if now > expires_at:
            raise ValueError("Entitlement token has expired")

        # Check nonce (replay protection)
        nonce = payload["nonce"]
        if not self._check_and_store_nonce(nonce, expires_at):
            raise ValueError("Entitlement token has already been used (replay detected)")

        return payload


# Module-level singleton
_entitlement_service: Optional[EntitlementService] = None


def get_entitlement_service() -> EntitlementService:
    """Get or create singleton EntitlementService."""
    global _entitlement_service
    if _entitlement_service is None:
        _entitlement_service = EntitlementService()
    return _entitlement_service
