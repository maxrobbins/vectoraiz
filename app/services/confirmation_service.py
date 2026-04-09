"""
DEPRECATED — Replaced by ApprovalTokenService (BQ-VZ-CONTROL-PLANE Step 2).

This module re-exports symbols for backward compatibility with existing tests.
All new code should import from app.services.approval_token_service.
"""

import warnings

from app.services.approval_token_service import (
    MUTATION_TOOLS,
    approval_token_service,
)

warnings.warn(
    "confirmation_service is deprecated. Use approval_token_service instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Backward-compat aliases
CONFIRMATION_TTL_SECONDS = 60
DESTRUCTIVE_TOOLS = set(MUTATION_TOOLS.keys())


class ConfirmationService:
    """Deprecated — use ApprovalTokenService."""
    pass


confirmation_service = approval_token_service
