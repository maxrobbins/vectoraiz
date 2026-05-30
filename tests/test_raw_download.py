"""
Tests for raw file download endpoint.
======================================

Covers: successful stream, hash mismatch, missing file, invalid token.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-03
"""

import base64
import hashlib
import hmac
import json
import os
import time
import uuid

from typing import Optional

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routers.raw_listings import router as raw_listings_router, download_router
from app.services.entitlement_service import _derive_shared_secret


def _make_token(payload_dict: dict, secret: Optional[bytes] = None) -> str:
    """Helper: create a signed entitlement token."""
    if secret is None:
        secret = _derive_shared_secret()
    payload_json = json.dumps(payload_dict).encode("utf-8")
    payload_b64 = base64.urlsafe_b64encode(payload_json).decode("utf-8").rstrip("=")
    sig = hmac.new(secret, payload_b64.encode("utf-8"), hashlib.sha256).digest()
    sig_b64 = base64.urlsafe_b64encode(sig).decode("utf-8").rstrip("=")
    return f"{payload_b64}.{sig_b64}"


def _entitlement_payload(file_hash: str) -> dict:
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
def app():
    _app = FastAPI()
    _app.include_router(raw_listings_router, prefix="/api/raw")
    _app.include_router(download_router, prefix="/api/raw")
    return _app


@pytest.fixture
def client(app):
    return TestClient(app)


@pytest.fixture
def sample_file(tmp_path):
    """Create a sample file and return its path."""
    f = tmp_path / "download_test.txt"
    f.write_text("Hello, buyer! This is your purchased data.\n")
    return str(f)


@pytest.fixture
def registered_file(client, sample_file):
    """Register a file and return its full response data."""
    resp = client.post("/api/raw/files", json={"file_path": sample_file})
    assert resp.status_code == 201
    return resp.json()


class TestRawDownload:
    """Test GET /api/raw/download/{id} — entitlement-gated file download."""

    def test_successful_download(self, client, registered_file):
        """Valid entitlement token enables file download."""
        file_id = registered_file["id"]
        content_hash = registered_file["content_hash"]

        payload = _entitlement_payload(content_hash)
        token = _make_token(payload)

        resp = client.get(
            f"/api/raw/download/{file_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 200
        assert b"Hello, buyer!" in resp.content

    def test_hash_mismatch(self, client, registered_file):
        """Download with wrong file_hash in token returns 409."""
        file_id = registered_file["id"]

        payload = _entitlement_payload("wrong" + "a" * 59)  # Wrong hash
        token = _make_token(payload)

        resp = client.get(
            f"/api/raw/download/{file_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 409

    def test_missing_file_id(self, client, registered_file):
        """Download for nonexistent file_id returns 404."""
        payload = _entitlement_payload(registered_file["content_hash"])
        token = _make_token(payload)

        resp = client.get(
            "/api/raw/download/00000000-0000-0000-0000-000000000000",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 404

    def test_invalid_token(self, client, registered_file):
        """Download with invalid token returns 401."""
        file_id = registered_file["id"]
        resp = client.get(
            f"/api/raw/download/{file_id}",
            headers={"Authorization": "Bearer invalid.token"},
        )
        assert resp.status_code == 401

    def test_missing_auth_header(self, client, registered_file):
        """Download without Authorization header returns 401."""
        file_id = registered_file["id"]
        resp = client.get(f"/api/raw/download/{file_id}")
        assert resp.status_code == 401

    def test_expired_token(self, client, registered_file):
        """Download with expired token returns 401."""
        file_id = registered_file["id"]
        content_hash = registered_file["content_hash"]

        payload = _entitlement_payload(content_hash)
        payload["expires_at"] = time.time() - 10  # Already expired
        token = _make_token(payload)

        resp = client.get(
            f"/api/raw/download/{file_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 401
        assert "expired" in resp.json()["detail"].lower()

    def test_replayed_nonce(self, client, registered_file):
        """Using the same nonce twice returns 401 on the second attempt."""
        file_id = registered_file["id"]
        content_hash = registered_file["content_hash"]

        payload = _entitlement_payload(content_hash)
        token = _make_token(payload)

        # First request succeeds
        resp1 = client.get(
            f"/api/raw/download/{file_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp1.status_code == 200

        # Same token (same nonce) fails
        resp2 = client.get(
            f"/api/raw/download/{file_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp2.status_code == 401
        assert "replay" in resp2.json()["detail"].lower()

    def test_file_missing_from_disk(self, client, registered_file, sample_file):
        """Download when file has been deleted from disk returns 404."""
        file_id = registered_file["id"]
        content_hash = registered_file["content_hash"]

        # Delete the actual file from disk
        os.remove(sample_file)

        payload = _entitlement_payload(content_hash)
        token = _make_token(payload)

        resp = client.get(
            f"/api/raw/download/{file_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 404
        assert "missing" in resp.json()["detail"].lower()

    def test_wrong_secret_signature(self, client, registered_file):
        """Token signed with wrong secret is rejected."""
        file_id = registered_file["id"]
        content_hash = registered_file["content_hash"]

        payload = _entitlement_payload(content_hash)
        wrong_secret = b"wrong-secret-key-not-the-real-one-padding"
        token = _make_token(payload, secret=wrong_secret)

        resp = client.get(
            f"/api/raw/download/{file_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 401
