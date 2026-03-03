"""
Tests for entitlement token validation.
========================================

Covers: valid token, expired, replayed, wrong hash, missing auth,
        tampered signature, malformed token, missing fields.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-03
"""

import base64
import hashlib
import hmac
import json
import time
import uuid

from typing import Optional

import pytest

from app.services.entitlement_service import EntitlementService, _derive_shared_secret


def _make_token(payload_dict: dict, secret: Optional[bytes] = None) -> str:
    """Helper: create a signed entitlement token."""
    if secret is None:
        secret = _derive_shared_secret()

    payload_json = json.dumps(payload_dict).encode("utf-8")
    payload_b64 = base64.urlsafe_b64encode(payload_json).decode("utf-8").rstrip("=")
    sig = hmac.new(secret, payload_b64.encode("utf-8"), hashlib.sha256).digest()
    sig_b64 = base64.urlsafe_b64encode(sig).decode("utf-8").rstrip("=")
    return f"{payload_b64}.{sig_b64}"


def _valid_payload(file_hash: str = "a" * 64) -> dict:
    """Helper: return a valid entitlement payload."""
    return {
        "order_id": str(uuid.uuid4()),
        "listing_id": str(uuid.uuid4()),
        "file_hash": file_hash,
        "buyer_id": str(uuid.uuid4()),
        "issued_at": time.time(),
        "expires_at": time.time() + 3600,
        "nonce": str(uuid.uuid4()),
    }


@pytest.fixture(autouse=True)
def _set_secret(monkeypatch):
    """Ensure a deterministic secret is available for testing."""
    monkeypatch.setattr("app.config.settings.internal_api_key", "test-install-token-for-entitlement")


@pytest.fixture
def svc():
    return EntitlementService()


class TestEntitlementValid:
    """Test valid entitlement tokens."""

    def test_valid_token(self, svc):
        """A properly signed, fresh token validates successfully."""
        payload = _valid_payload()
        token = _make_token(payload)
        result = svc.validate_entitlement(f"Bearer {token}")
        assert result["order_id"] == payload["order_id"]
        assert result["file_hash"] == payload["file_hash"]
        assert result["nonce"] == payload["nonce"]

    def test_valid_token_returns_all_fields(self, svc):
        """All required fields are present in the validated payload."""
        payload = _valid_payload()
        token = _make_token(payload)
        result = svc.validate_entitlement(f"Bearer {token}")
        for key in ("order_id", "listing_id", "file_hash", "buyer_id", "issued_at", "expires_at", "nonce"):
            assert key in result


class TestEntitlementExpired:
    """Test expired tokens."""

    def test_expired_token(self, svc):
        """An expired token is rejected."""
        payload = _valid_payload()
        payload["expires_at"] = time.time() - 100  # Already expired
        token = _make_token(payload)
        with pytest.raises(ValueError, match="expired"):
            svc.validate_entitlement(f"Bearer {token}")


class TestEntitlementReplay:
    """Test nonce replay protection."""

    def test_replayed_token(self, svc):
        """Using the same token twice triggers replay detection."""
        payload = _valid_payload()
        token = _make_token(payload)
        svc.validate_entitlement(f"Bearer {token}")  # First use OK
        with pytest.raises(ValueError, match="replay"):
            svc.validate_entitlement(f"Bearer {token}")  # Second use fails

    def test_different_nonces_ok(self, svc):
        """Two tokens with different nonces both validate."""
        p1 = _valid_payload()
        p2 = _valid_payload()
        t1 = _make_token(p1)
        t2 = _make_token(p2)
        svc.validate_entitlement(f"Bearer {t1}")
        svc.validate_entitlement(f"Bearer {t2}")  # No error


class TestEntitlementWrongHash:
    """Test file_hash from token is returned correctly for verification."""

    def test_hash_mismatch_detected_downstream(self, svc):
        """Token with a specific file_hash returns it for caller to verify."""
        payload = _valid_payload(file_hash="b" * 64)
        token = _make_token(payload)
        result = svc.validate_entitlement(f"Bearer {token}")
        assert result["file_hash"] == "b" * 64


class TestEntitlementMissingAuth:
    """Test missing or malformed Authorization header."""

    def test_missing_header(self, svc):
        """Empty authorization header is rejected."""
        with pytest.raises(ValueError, match="Missing"):
            svc.validate_entitlement("")

    def test_no_bearer_prefix(self, svc):
        """Authorization without 'Bearer ' prefix is rejected."""
        with pytest.raises(ValueError, match="Missing"):
            svc.validate_entitlement("Token abc123")


class TestEntitlementTampered:
    """Test tampered/invalid signatures."""

    def test_tampered_signature(self, svc):
        """Token with wrong signature is rejected."""
        payload = _valid_payload()
        # Sign with wrong secret
        wrong_secret = b"totally-wrong-secret-key-that-is-32b"
        token = _make_token(payload, secret=wrong_secret)
        with pytest.raises(ValueError, match="signature"):
            svc.validate_entitlement(f"Bearer {token}")

    def test_malformed_token(self, svc):
        """A token without proper format is rejected."""
        with pytest.raises(ValueError, match="Malformed"):
            svc.validate_entitlement("Bearer not-a-valid-token-at-all")

    def test_missing_required_field(self, svc):
        """Token missing a required field is rejected."""
        payload = _valid_payload()
        del payload["buyer_id"]
        token = _make_token(payload)
        with pytest.raises(ValueError, match="Missing required field"):
            svc.validate_entitlement(f"Bearer {token}")
