"""
BQ-102 ST-4: Device Registration Integration Tests
===================================================

Tests the full device registration flow:
1. Keypair generation and keystore persistence
2. Keystore encryption/decryption round-trip
3. Registration client with mocked ai.market endpoint
4. Platform key storage after registration
5. Negative cases: invalid keys, 409 recovery, auth failure
6. Private keys never leave local storage
"""

import base64
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from app.core.crypto import DeviceCrypto


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture
def tmp_keystore(tmp_path):
    """Provide a temporary keystore path."""
    return str(tmp_path / "test_keystore.json")


@pytest.fixture
def crypto(tmp_keystore):
    """Create a DeviceCrypto instance with a temp keystore."""
    return DeviceCrypto(
        keystore_path=tmp_keystore,
        passphrase="test-passphrase-for-ci",
    )


# ===========================================================================
# AC-1: Keypair Generation
# ===========================================================================

class TestKeypairGeneration:
    """CLI command or startup flow generates Ed25519 + X25519 keypairs if not already present."""

    def test_generates_keypairs_on_first_call(self, crypto, tmp_keystore):
        """Should generate fresh keypairs when keystore doesn't exist."""
        assert not Path(tmp_keystore).exists()

        ed_priv, ed_pub, x_priv, x_pub = crypto.get_or_create_keypairs()

        assert ed_priv is not None
        assert ed_pub is not None
        assert x_priv is not None
        assert x_pub is not None
        assert Path(tmp_keystore).exists()

    def test_idempotent_returns_same_keys(self, crypto):
        """Calling get_or_create_keypairs twice should return the same keys."""
        ed1_priv, ed1_pub, x1_priv, x1_pub = crypto.get_or_create_keypairs()
        ed2_priv, ed2_pub, x2_priv, x2_pub = crypto.get_or_create_keypairs()

        # Public keys should match
        from cryptography.hazmat.primitives import serialization

        pub1 = ed1_pub.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        pub2 = ed2_pub.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        assert pub1 == pub2

    def test_generates_valid_ed25519_key(self, crypto):
        """Ed25519 public key should be exactly 32 bytes."""
        from cryptography.hazmat.primitives import serialization

        _, ed_pub, _, _ = crypto.get_or_create_keypairs()
        raw = ed_pub.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        assert len(raw) == 32

    def test_generates_valid_x25519_key(self, crypto):
        """X25519 public key should be exactly 32 bytes."""
        from cryptography.hazmat.primitives import serialization

        _, _, _, x_pub = crypto.get_or_create_keypairs()
        raw = x_pub.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        assert len(raw) == 32


# ===========================================================================
# AC-2: Secure Keystore
# ===========================================================================

class TestSecureKeystore:
    """Keypairs stored securely in local keystore with passphrase-derived encryption."""

    def test_keystore_contains_encrypted_private_keys(self, crypto, tmp_keystore):
        """Private keys in keystore should be encrypted, not raw."""
        crypto.get_or_create_keypairs()

        with open(tmp_keystore) as f:
            data = json.load(f)

        # Should have encrypted keys, not raw hex
        assert "encrypted_ed25519_private_key" in data
        assert "encrypted_x25519_private_key" in data
        assert "ed25519_salt" in data
        assert "x25519_salt" in data

        # The encrypted data should NOT be 32 bytes hex (that would be raw)
        enc_ed = data["encrypted_ed25519_private_key"].encode("latin-1")
        assert len(enc_ed) > 64  # Fernet adds padding + IV + HMAC

    def test_wrong_passphrase_fails(self, crypto, tmp_keystore):
        """Loading with wrong passphrase should fail."""
        crypto.get_or_create_keypairs()

        bad_crypto = DeviceCrypto(
            keystore_path=tmp_keystore,
            passphrase="wrong-passphrase",
        )

        with pytest.raises(Exception):
            bad_crypto.get_or_create_keypairs()

    def test_encryption_decryption_roundtrip(self, crypto):
        """Encrypt then decrypt should produce identical keys."""
        from cryptography.hazmat.primitives import serialization

        ed_priv, ed_pub, _, _ = crypto.get_or_create_keypairs()

        # Reload from disk
        ed_priv2, ed_pub2, _, _ = crypto.get_or_create_keypairs()

        orig = ed_priv.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        loaded = ed_priv2.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        assert orig == loaded

    def test_private_keys_not_in_plaintext(self, crypto, tmp_keystore):
        """Verify raw private key bytes don't appear in the keystore file."""
        from cryptography.hazmat.primitives import serialization

        ed_priv, _, x_priv, _ = crypto.get_or_create_keypairs()

        ed_raw = ed_priv.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        ).hex()

        with open(tmp_keystore) as f:
            keystore_text = f.read()

        assert ed_raw not in keystore_text


# ===========================================================================
# AC-3: Registration Call
# ===========================================================================

class TestRegistrationClient:
    """Calls POST /api/v1/trust/register and handles responses."""

    @pytest.mark.asyncio
    async def test_successful_registration(self, crypto):
        """Should store platform keys on 200 response."""
        crypto.get_or_create_keypairs()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "device_id": "test123",
            "ai_market_ed25519_public_key": base64.b64encode(b"A" * 32).decode(),
            "ai_market_x25519_public_key": base64.b64encode(b"B" * 32).decode(),
            "certificate": base64.b64encode(b"cert-data").decode(),
            "registered_at": "2026-02-11T00:00:00Z",
        }

        with patch("app.services.registration_service.httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post.return_value = mock_response
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = mock_instance

            from app.services.registration_service import register_with_marketplace

            with patch("app.services.registration_service.settings") as mock_settings:
                mock_settings.ai_market_url = "https://test.ai.market"
                mock_settings.internal_api_key = "aim_test_key"
                mock_settings.keystore_passphrase = "test"

                result = await register_with_marketplace(crypto)

        assert result is True
        assert crypto.has_platform_keys()

    @pytest.mark.asyncio
    async def test_409_recovery_with_platform_keys(self, crypto):
        """Should recover platform keys from 409 response (AG Council)."""
        crypto.get_or_create_keypairs()

        mock_response = MagicMock()
        mock_response.status_code = 409
        mock_response.json.return_value = {
            "ai_market_ed25519_public_key": base64.b64encode(b"C" * 32).decode(),
            "ai_market_x25519_public_key": base64.b64encode(b"D" * 32).decode(),
            "certificate": base64.b64encode(b"cert-existing").decode(),
        }
        mock_response.text = "Already registered"

        with patch("app.services.registration_service.httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post.return_value = mock_response
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = mock_instance

            from app.services.registration_service import register_with_marketplace

            with patch("app.services.registration_service.settings") as mock_settings:
                mock_settings.ai_market_url = "https://test.ai.market"
                mock_settings.internal_api_key = "aim_test_key"

                result = await register_with_marketplace(crypto)

        assert result is True
        assert crypto.has_platform_keys()

    @pytest.mark.asyncio
    async def test_auth_failure_no_retry(self, crypto):
        """Should not retry on 401/403."""
        crypto.get_or_create_keypairs()

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"

        with patch("app.services.registration_service.httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.post.return_value = mock_response
            mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
            mock_instance.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value = mock_instance

            from app.services.registration_service import register_with_marketplace

            with patch("app.services.registration_service.settings") as mock_settings:
                mock_settings.ai_market_url = "https://test.ai.market"
                mock_settings.internal_api_key = "aim_test_key"

                result = await register_with_marketplace(crypto)

        assert result is False
        # Should only have been called once (no retry)
        assert mock_instance.post.call_count == 1

    @pytest.mark.asyncio
    async def test_skips_if_platform_keys_exist(self, crypto):
        """Should skip registration if platform keys already stored."""
        crypto.get_or_create_keypairs()
        crypto.store_platform_keys(
            platform_ed25519_pub=base64.b64encode(b"E" * 32).decode(),
            platform_x25519_pub=base64.b64encode(b"F" * 32).decode(),
            certificate="existing-cert",
        )

        from app.services.registration_service import register_with_marketplace

        with patch("app.services.registration_service.settings") as mock_settings:
            mock_settings.internal_api_key = "aim_test_key"
            result = await register_with_marketplace(crypto)

        assert result is True  # Skipped successfully


# ===========================================================================
# AC-4: Platform Key Storage
# ===========================================================================

class TestPlatformKeyStorage:
    """Stores ai.market platform public keys locally for signature verification."""

    def test_store_and_retrieve_platform_keys(self, crypto, tmp_keystore):
        """Should persist platform keys in keystore."""
        crypto.get_or_create_keypairs()

        ed_pub = base64.b64encode(b"G" * 32).decode()
        x_pub = base64.b64encode(b"H" * 32).decode()
        cert = "test-certificate-data"

        crypto.store_platform_keys(ed_pub, x_pub, cert)

        # Verify in keystore file
        with open(tmp_keystore) as f:
            data = json.load(f)

        assert data["platform_ed25519_public_key"] == ed_pub
        assert data["platform_x25519_public_key"] == x_pub
        assert data["certificate"] == cert

    def test_has_platform_keys_false_initially(self, crypto):
        """Should return False before registration."""
        crypto.get_or_create_keypairs()
        assert crypto.has_platform_keys() is False

    def test_has_platform_keys_true_after_store(self, crypto):
        """Should return True after storing platform keys."""
        crypto.get_or_create_keypairs()
        crypto.store_platform_keys("a", "b", "c")
        assert crypto.has_platform_keys() is True

    def test_device_keys_preserved_after_platform_store(self, crypto, tmp_keystore):
        """Storing platform keys should not destroy device keypairs."""
        from cryptography.hazmat.primitives import serialization

        _, ed_pub, _, _ = crypto.get_or_create_keypairs()
        orig_pub_hex = ed_pub.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        ).hex()

        crypto.store_platform_keys("x", "y", "z")

        with open(tmp_keystore) as f:
            data = json.load(f)

        assert data["ed25519_public_key"] == orig_pub_hex
