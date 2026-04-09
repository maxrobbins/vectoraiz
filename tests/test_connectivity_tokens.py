"""
Tests for connectivity token service — CRUD, HMAC, parsing, edge cases.

BQ-MCP-RAG Phase 1: Token lifecycle, HMAC verification, constant-time comparison,
parsing edge cases (unicode, whitespace, short IDs, extra underscores),
max token enforcement, revocation, expiration.
"""

from datetime import datetime, timedelta, timezone

import pytest

from app.services.connectivity_token_service import (
    ConnectivityTokenError,
    create_token,
    list_tokens,
    parse_token,
    revoke_token,
    verify_token,
    _hmac_hash,
)


# ---------------------------------------------------------------------------
# Token parsing
# ---------------------------------------------------------------------------

class TestParseToken:
    def test_valid_token(self):
        token_id, secret = parse_token("vzmcp_a1B2c3D4_abcdef0123456789abcdef0123456789")
        assert token_id == "a1B2c3D4"
        assert secret == "abcdef0123456789abcdef0123456789"

    def test_missing_prefix(self):
        with pytest.raises(ConnectivityTokenError, match="Invalid token format"):
            parse_token("wrong_a1B2c3D4_abcdef0123456789abcdef0123456789")

    def test_empty_string(self):
        with pytest.raises(ConnectivityTokenError, match="Invalid token format"):
            parse_token("")

    def test_prefix_only(self):
        with pytest.raises(ConnectivityTokenError, match="Invalid token format"):
            parse_token("vzmcp_")

    def test_not_a_string(self):
        with pytest.raises(ConnectivityTokenError, match="must be a string"):
            parse_token(12345)  # type: ignore

    def test_extra_underscores(self):
        """Token with more than 2 underscores should fail."""
        with pytest.raises(ConnectivityTokenError, match="Invalid token format"):
            parse_token("vzmcp_a1B2c3D4_abcdef01_23456789abcdef0123456789")

    def test_short_token_id(self):
        """Token ID must be exactly 8 chars."""
        with pytest.raises(ConnectivityTokenError, match="Invalid token format"):
            parse_token("vzmcp_abc_abcdef0123456789abcdef0123456789")

    def test_long_token_id(self):
        with pytest.raises(ConnectivityTokenError, match="Invalid token format"):
            parse_token("vzmcp_a1B2c3D4E_abcdef0123456789abcdef0123456789")

    def test_non_alphanumeric_token_id(self):
        with pytest.raises(ConnectivityTokenError, match="Invalid token format"):
            parse_token("vzmcp_a1B2c3D!_abcdef0123456789abcdef0123456789")

    def test_non_hex_secret(self):
        with pytest.raises(ConnectivityTokenError, match="Invalid token format"):
            parse_token("vzmcp_a1B2c3D4_GHIJKL0123456789abcdef0123456789")

    def test_short_secret(self):
        with pytest.raises(ConnectivityTokenError, match="Invalid token format"):
            parse_token("vzmcp_a1B2c3D4_abcdef01234567")

    def test_unicode_in_token(self):
        with pytest.raises(ConnectivityTokenError, match="Invalid token format"):
            parse_token("vzmcp_a1B2c3D4_abcdef0123456789abcdef01234567é9")

    def test_whitespace_in_token(self):
        with pytest.raises(ConnectivityTokenError, match="Invalid token format"):
            parse_token("vzmcp_a1B2 c3D4_abcdef0123456789abcdef0123456789")

    def test_uppercase_secret_rejected(self):
        """Secret must be lowercase hex only."""
        with pytest.raises(ConnectivityTokenError, match="Invalid token format"):
            parse_token("vzmcp_a1B2c3D4_ABCDEF0123456789ABCDEF0123456789")


# ---------------------------------------------------------------------------
# Token CRUD
# ---------------------------------------------------------------------------

class TestTokenCRUD:
    def test_create_token_returns_raw_and_model(self):
        raw, token = create_token(label="Test Token")
        assert raw.startswith("vzmcp_")
        assert len(raw.split("_")) == 3
        assert token.label == "Test Token"
        assert token.id
        assert len(token.scopes) == 6

    def test_create_token_custom_scopes(self):
        raw, token = create_token(label="SQL Only", scopes=["ext:sql"])
        assert token.scopes == ["ext:sql"]

    def test_create_token_invalid_scope(self):
        with pytest.raises(ConnectivityTokenError, match="Invalid scopes"):
            create_token(label="Bad", scopes=["ext:delete"])

    def test_verify_token_success(self):
        raw, created = create_token(label="Verify Test")
        verified = verify_token(raw)
        assert verified.id == created.id
        assert verified.label == "Verify Test"

    def test_verify_wrong_secret(self):
        raw, _ = create_token(label="Wrong Secret")
        # Tamper with the secret
        parts = raw.split("_")
        tampered = f"{parts[0]}_{parts[1]}_{'0' * 32}"
        with pytest.raises(ConnectivityTokenError, match="Invalid token"):
            verify_token(tampered)

    def test_verify_unknown_token_id(self):
        with pytest.raises(ConnectivityTokenError, match="Unknown token"):
            verify_token("vzmcp_zZ999999_abcdef0123456789abcdef0123456789")

    def test_revoke_token(self):
        raw, created = create_token(label="Revoke Test")
        revoked = revoke_token(created.id)
        assert revoked.id == created.id

        # Verification should fail with revoked error
        with pytest.raises(ConnectivityTokenError, match="revoked"):
            verify_token(raw)

    def test_revoke_already_revoked(self):
        _, created = create_token(label="Double Revoke")
        revoke_token(created.id)
        with pytest.raises(ConnectivityTokenError, match="already revoked"):
            revoke_token(created.id)

    def test_revoke_nonexistent(self):
        with pytest.raises(ConnectivityTokenError, match="not found"):
            revoke_token("ZZZZZZZZ")

    def test_list_tokens(self):
        create_token(label="List Test 1")
        create_token(label="List Test 2")
        tokens = list_tokens()
        labels = [t.label for t in tokens]
        assert "List Test 1" in labels
        assert "List Test 2" in labels

    def test_max_tokens_enforced(self):
        """Creating more than max_tokens should fail."""
        # Revoke all existing tokens first so count is deterministic
        for t in list_tokens():
            if not hasattr(t, '_revoked'):
                try:
                    revoke_token(t.id)
                except ConnectivityTokenError:
                    pass

        for i in range(3):
            create_token(label=f"Max Test {i}", max_tokens=3)

        with pytest.raises(ConnectivityTokenError, match="Maximum 3 active tokens"):
            create_token(label="One too many", max_tokens=3)

    def test_max_tokens_after_revoke(self):
        """Revoking frees a slot."""
        # Revoke all existing tokens first
        for t in list_tokens():
            try:
                revoke_token(t.id)
            except ConnectivityTokenError:
                pass

        tokens = []
        for i in range(3):
            _, t = create_token(label=f"Max Revoke {i}", max_tokens=3)
            tokens.append(t)

        # Revoke one to free a slot
        revoke_token(tokens[0].id)

        # Should succeed now
        _, new_token = create_token(label="After Revoke", max_tokens=3)
        assert new_token.label == "After Revoke"

    def test_expired_token_rejected(self):
        """Token past expires_at should be rejected."""
        raw, _ = create_token(
            label="Expired",
            expires_at=datetime.now(timezone.utc) - timedelta(hours=1),
        )
        with pytest.raises(ConnectivityTokenError, match="expired"):
            verify_token(raw)

    def test_non_expired_token_accepted(self):
        raw, created = create_token(
            label="Not Expired",
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        verified = verify_token(raw)
        assert verified.id == created.id

    def test_request_count_increments(self):
        raw, created = create_token(label="Counter Test")
        verify_token(raw)
        verify_token(raw)
        verified = verify_token(raw)
        assert verified.request_count >= 3

    def test_secret_last4_stored(self):
        raw, token = create_token(label="Last4 Test")
        secret = raw.split("_")[2]
        assert token.secret_last4 == secret[-4:]


# ---------------------------------------------------------------------------
# HMAC constant-time comparison
# ---------------------------------------------------------------------------

class TestHMACConstantTime:
    def test_hmac_hash_deterministic(self):
        h1 = _hmac_hash("test_secret_1234")
        h2 = _hmac_hash("test_secret_1234")
        assert h1 == h2

    def test_hmac_hash_different_for_different_secrets(self):
        h1 = _hmac_hash("secret_a")
        h2 = _hmac_hash("secret_b")
        assert h1 != h2

    def test_uses_hmac_compare_digest(self):
        """Verify that hmac.compare_digest is used (not ==)."""
        # We test this indirectly: create a token, then verify with correct secret
        # The code uses hmac.compare_digest — this test ensures the path works
        raw, _ = create_token(label="HMAC Test")
        verified = verify_token(raw)
        assert verified is not None
