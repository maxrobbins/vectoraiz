"""
ApprovalTokenService — Server-Enforced Approval Gate for Tool Execution

Replaces ConfirmationService with:
- 3-category tool classification (READ_ONLY, AUTO_APPROVE, MUTATION)
- Risk-based TTL for approval tokens
- Atomic CAS status transitions (prevents double-approve)
- SHA-256 args hash integrity verification
- Default-deny for unclassified tools

PHASE: BQ-VZ-CONTROL-PLANE Step 2 — Security Foundation
CREATED: 2026-03-05
"""

import hashlib
import json
import logging
import secrets
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# ======================================================================
# Tool Classification — THREE Categories
# ======================================================================

READ_ONLY_TOOLS = {
    "list_datasets", "get_dataset_detail", "preview_rows",
    "run_sql_query", "search_vectors", "get_system_status",
    "get_dataset_statistics", "connectivity_status",
    "get_notifications", "get_tunnel_status",
    "generate_diagnostic_bundle",
    "connectivity_test",
    "connectivity_generate_setup",
}

AUTO_APPROVE_TOOLS = {
    "log_feedback",
    "submit_feedback",
    "create_notification",
    "prepare_support_bundle",
    "create_artifact",
    "create_artifact_from_query",
}

MUTATION_TOOLS: Dict[str, Dict[str, str]] = {
    "delete_dataset":            {"risk": "high",   "desc": "Delete dataset '{dataset_name}'"},
    "connectivity_enable":       {"risk": "medium", "desc": "Enable ai.market connectivity"},
    "connectivity_disable":      {"risk": "medium", "desc": "Disable ai.market connectivity"},
    "connectivity_create_token": {"risk": "medium", "desc": "Create connectivity token"},
    "connectivity_revoke_token": {"risk": "medium", "desc": "Revoke connectivity token"},
    "start_public_tunnel":       {"risk": "medium", "desc": "Start public tunnel"},
    "stop_public_tunnel":        {"risk": "low",    "desc": "Stop public tunnel"},
}

ALL_CLASSIFIED_TOOLS = READ_ONLY_TOOLS | AUTO_APPROVE_TOOLS | set(MUTATION_TOOLS.keys())


# ======================================================================
# Risk-Based TTL
# ======================================================================

RISK_TTL: Dict[str, int] = {
    "low": 120,
    "medium": 60,
    "high": 30,
    "critical": 15,
}


# ======================================================================
# Capability-Based Authorization (Phase D)
# ======================================================================

TOOL_CAPABILITIES: Dict[str, set] = {
    # READ_ONLY
    "list_datasets": {"data:read"},
    "get_dataset_detail": {"data:read"},
    "preview_rows": {"data:read"},
    "run_sql_query": {"data:read"},
    "search_vectors": {"data:read"},
    "get_system_status": {"system:read"},
    "get_dataset_statistics": {"data:read"},
    "connectivity_status": {"connectivity:read"},
    "get_notifications": {"system:read"},
    "get_tunnel_status": {"connectivity:read"},
    "generate_diagnostic_bundle": {"system:read"},
    "connectivity_test": {"connectivity:read"},
    "connectivity_generate_setup": {"connectivity:read"},
    # AUTO_APPROVE
    "log_feedback": {"feedback:write"},
    "submit_feedback": {"feedback:write"},
    "create_notification": {"system:write"},
    "prepare_support_bundle": {"system:read"},
    # AUTO_APPROVE — Artifacts
    "create_artifact": {"data:read"},
    "create_artifact_from_query": {"data:read"},
    # MUTATION
    "delete_dataset": {"data:delete"},
    "connectivity_enable": {"connectivity:write"},
    "connectivity_disable": {"connectivity:write"},
    "connectivity_create_token": {"connectivity:write"},
    "connectivity_revoke_token": {"connectivity:write"},
    "start_public_tunnel": {"connectivity:write"},
    "stop_public_tunnel": {"connectivity:write"},
}

ALL_CAPABILITIES = {cap for caps in TOOL_CAPABILITIES.values() for cap in caps}


def get_user_capabilities(user: Any) -> set:
    """V1: single-user — all capabilities granted."""
    return ALL_CAPABILITIES


def check_capabilities(user: Any, tool_name: str) -> Optional[str]:
    """Check if user has required capabilities. Returns error string or None."""
    if tool_name not in TOOL_CAPABILITIES:
        return f"Tool '{tool_name}' has no capability mapping — denied by default"

    required = TOOL_CAPABILITIES[tool_name]
    user_caps = get_user_capabilities(user)
    missing = required - user_caps
    if missing:
        return f"Missing capabilities: {missing}"
    return None


# ======================================================================
# Token Model
# ======================================================================

@dataclass
class ApprovalToken:
    id: str
    user_id: str
    session_id: str
    tool_name: str
    tool_input: dict
    tool_args_hash: str
    nonce: str
    description: str
    risk_level: str
    status: str  # pending, claimed, approved, denied, expired, error
    created_at: float
    expires_at: float


@dataclass
class ApprovalResult:
    success: bool
    reason: str = ""
    tool_name: str = ""
    tool_input: dict = field(default_factory=dict)


# ======================================================================
# ApprovalTokenService
# ======================================================================

class ApprovalTokenService:
    """
    Server-enforced approval gate for mutation tools.

    Uses in-memory dict with CAS status transitions. Single-worker safe
    (Python GIL guarantees atomicity of status assignment).
    """

    def __init__(self) -> None:
        self._pending: Dict[str, ApprovalToken] = {}

    def create_token(
        self,
        user_id: str,
        session_id: str,
        tool_name: str,
        tool_input: dict,
        description: str = "",
    ) -> ApprovalToken:
        """Create an approval token for a mutation tool call."""
        self._cleanup_expired()

        mutation_info = MUTATION_TOOLS.get(tool_name, {})
        risk_level = mutation_info.get("risk", "medium")
        ttl = RISK_TTL.get(risk_level, 60)

        args_hash = hashlib.sha256(
            json.dumps(tool_input, sort_keys=True).encode()
        ).hexdigest()

        now = time.time()
        token = ApprovalToken(
            id=str(uuid.uuid4()),
            user_id=user_id,
            session_id=session_id,
            tool_name=tool_name,
            tool_input=tool_input,
            tool_args_hash=args_hash,
            nonce=secrets.token_hex(16),
            description=description or mutation_info.get("desc", f"Execute {tool_name}"),
            risk_level=risk_level,
            status="pending",
            created_at=now,
            expires_at=now + ttl,
        )

        self._pending[token.id] = token

        logger.info(
            "Approval token created: id=%s user=%s tool=%s risk=%s ttl=%ds",
            token.id[:8], user_id, tool_name, risk_level, ttl,
        )
        return token

    def validate_and_consume(
        self,
        token_id: str,
        user_id: str,
        session_id: str,
    ) -> ApprovalResult:
        """
        Validate and consume an approval token. Atomic CAS transition.

        Returns ApprovalResult with success=True and tool details on success,
        or success=False with reason on failure.

        IMPORTANT: wrong_user/wrong_session do NOT consume the token —
        it stays pending so the correct user can still approve.
        """
        token = self._pending.get(token_id)
        if not token or token.status != "pending":
            return ApprovalResult(success=False, reason="not_found_or_already_used")

        # Atomic CAS: pending → claimed (prevents concurrent approve)
        token.status = "claimed"

        # Expiry check
        if time.time() > token.expires_at:
            token.status = "expired"
            self._pending.pop(token_id, None)
            return ApprovalResult(success=False, reason="expired")

        # User match — do NOT consume on mismatch
        if token.user_id != user_id:
            token.status = "pending"  # restore to pending
            logger.warning(
                "Approval token user mismatch: token=%s expected=%s got=%s",
                token_id[:8], token.user_id, user_id,
            )
            return ApprovalResult(success=False, reason="wrong_user")

        # Session match — do NOT consume on mismatch
        if token.session_id != session_id:
            token.status = "pending"  # restore to pending
            logger.warning(
                "Approval token session mismatch: token=%s expected=%s got=%s",
                token_id[:8], token.session_id, session_id,
            )
            return ApprovalResult(success=False, reason="wrong_session")

        # Args hash integrity
        expected_hash = hashlib.sha256(
            json.dumps(token.tool_input, sort_keys=True).encode()
        ).hexdigest()
        if token.tool_args_hash != expected_hash:
            token.status = "denied"
            self._pending.pop(token_id, None)
            return ApprovalResult(success=False, reason="args_tampered")

        # Success — mark approved, remove from pending (single-use)
        token.status = "approved"
        self._pending.pop(token_id, None)

        return ApprovalResult(
            success=True,
            tool_name=token.tool_name,
            tool_input=token.tool_input,
        )

    def deny_token(self, token_id: str) -> bool:
        """Explicitly deny a pending token."""
        token = self._pending.get(token_id)
        if not token or token.status != "pending":
            return False
        token.status = "denied"
        self._pending.pop(token_id, None)
        return True

    def _cleanup_expired(self) -> None:
        """Remove expired tokens."""
        now = time.time()
        expired = [
            tid for tid, t in self._pending.items()
            if now > t.expires_at
        ]
        for tid in expired:
            t = self._pending.pop(tid, None)
            if t:
                t.status = "expired"


# Module-level singleton
approval_token_service = ApprovalTokenService()
