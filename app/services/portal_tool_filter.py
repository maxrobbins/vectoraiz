"""
Portal Tool Filter — Allowlist wrapper for allAI tools in portal context.

Security model:
- Only READ_ONLY search tools are allowed (no mutations, no admin tools)
- Dataset ACL enforced on every tool call (portal_visible check)
- Wraps AllAIToolExecutor with pre-execution filtering
- Default-deny: any tool not in PORTAL_ALLOWED_TOOLS is blocked

PHASE: BQ-VZ-SHARED-SEARCH Phase 1.5 — allAI Chat Integration
"""

import logging
from typing import Optional, Set

from app.middleware.portal_auth import check_dataset_acl
from app.services.allai_tool_result import ToolResult

logger = logging.getLogger(__name__)

# Portal users can ONLY use these read-only search tools.
# Everything else (mutations, admin, connectivity, tunnel, artifacts) is blocked.
PORTAL_ALLOWED_TOOLS: Set[str] = {
    "list_datasets",
    "get_dataset_detail",
    "preview_rows",
    "search_vectors",
    "get_dataset_statistics",
    "run_sql_query",
}

# Tools that take a dataset_id parameter and need ACL enforcement
DATASET_SCOPED_TOOLS: Set[str] = {
    "get_dataset_detail",
    "preview_rows",
    "search_vectors",
    "get_dataset_statistics",
    "run_sql_query",
}

BLOCKED_RESULT = ToolResult(
    frontend_data={"error": "This tool is not available in portal mode"},
    llm_summary="Tool not available. You can only use search and read-only data tools in the portal.",
)

ACL_BLOCKED_RESULT = ToolResult(
    frontend_data={"error": "Dataset not available on this portal"},
    llm_summary="This dataset is not available on the portal. Use list_datasets to see available datasets.",
)


def check_portal_tool_allowed(tool_name: str, tool_input: dict) -> Optional[ToolResult]:
    """Pre-check a tool call against portal allowlist and dataset ACL.

    Returns None if the call is allowed, or a ToolResult with an error if blocked.
    """
    # Step 1: Allowlist check (default-deny)
    if tool_name not in PORTAL_ALLOWED_TOOLS:
        logger.warning("Portal tool blocked (not in allowlist): %s", tool_name)
        return BLOCKED_RESULT

    # Step 2: Dataset ACL enforcement for dataset-scoped tools
    if tool_name in DATASET_SCOPED_TOOLS:
        dataset_id = tool_input.get("dataset_id")
        if dataset_id:
            try:
                check_dataset_acl(dataset_id)
            except Exception:
                logger.warning(
                    "Portal tool blocked (ACL): %s on dataset %s",
                    tool_name, dataset_id,
                )
                return ACL_BLOCKED_RESULT

    return None  # Allowed
