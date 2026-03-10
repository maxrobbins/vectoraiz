"""
CoPilotContextManager — Runtime State Injection
=================================================

Builds the AllieContext payload from current system state.
Called on every BRAIN_MESSAGE before prompt assembly.

Aggregates:
- StateSnapshot (last received from frontend via WS)
- User preferences (tone mode, quiet mode from session/env)
- System state (LLM config, Qdrant health, connected mode)
- Dataset metadata (if active dataset in snapshot)
- Recent events (from event log — placeholder for Phase 3)

PHASE: BQ-128 Phase 2 — Personality + Context Engine (Task 2.2)
CREATED: 2026-02-14
UPDATED: BQ-128 Phase 2 Audit — Sanitize form_state/selection
SPEC: ALLAI-PERSONALITY-SPEC-v2.1 Section 5
"""

import logging
import os
from typing import Any, Dict, List, Optional

from app.models.copilot import StateSnapshot
from app.services.prompt_factory import AllieContext, ToneMode, resolve_tone_mode
from app.core.local_only_guard import is_local_only

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Context sanitization constants (audit gate: prompt injection surface)
# ---------------------------------------------------------------------------
_FORM_STATE_BLOCKED_KEYS = {"system", "assistant", "instructions", "prompt", "role"}
_MAX_FIELD_LENGTH = 500
_MAX_SELECTION_TOTAL = 2000


def _sanitize_form_state(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Whitelist form_state keys and cap string lengths."""
    sanitized: Dict[str, Any] = {}
    for k, v in raw.items():
        if k.lower() in _FORM_STATE_BLOCKED_KEYS:
            continue
        if isinstance(v, str):
            sanitized[k] = v[:_MAX_FIELD_LENGTH]
        elif isinstance(v, (int, float, bool)):
            sanitized[k] = v
        elif isinstance(v, dict):
            sanitized[k] = _sanitize_form_state(v)
        elif isinstance(v, list):
            sanitized[k] = str(v)[:_MAX_FIELD_LENGTH]
        # skip other types
    return sanitized


def _cap_selection_total(selection: Dict[str, Any]) -> Dict[str, Any]:
    """Cap total serialized size of selection dict."""
    import json
    serialized = json.dumps(selection, default=str)
    if len(serialized) > _MAX_SELECTION_TOTAL:
        # Truncate to fit — drop form_state first, it's largest
        selection.pop("form_state", None)
        serialized = json.dumps(selection, default=str)
        if len(serialized) > _MAX_SELECTION_TOTAL:
            return {"_truncated": True}
    return selection


class CoPilotContextManager:
    """Builds runtime context for Allie's prompt."""

    async def build_context(
        self,
        state_snapshot: Optional[StateSnapshot] = None,
        user_id: Optional[str] = None,
        user_preferences: Optional[Dict[str, Any]] = None,
        session_metadata: Optional[Dict[str, Any]] = None,
    ) -> AllieContext:
        """
        Assemble AllieContext from:
        - StateSnapshot (last received from frontend via WS)
        - User preferences (tone mode, quiet mode)
        - System state (LLM config, Qdrant health, etc.)
        - Session metadata (has_seen_intro, etc.)

        Returns a fully populated AllieContext ready for PromptFactory.
        """
        prefs = user_preferences or {}

        # UI state from snapshot
        screen = "unknown"
        route = "/"
        selection: Dict[str, Any] = {}
        active_dataset_id = None

        if state_snapshot:
            route = state_snapshot.current_route or "/"
            screen = self._route_to_screen(route)
            if state_snapshot.active_dataset_id:
                active_dataset_id = state_snapshot.active_dataset_id
                selection["dataset_id"] = str(active_dataset_id)[:_MAX_FIELD_LENGTH]
            if state_snapshot.form_state:
                selection["form_state"] = _sanitize_form_state(state_snapshot.form_state)
            selection = _cap_selection_total(selection)

        # System state
        connected_mode = not is_local_only()
        vectorization_enabled = _check_vectorization_enabled()
        qdrant_status = _check_qdrant_status()
        local_only = is_local_only()

        # Dataset list: prefer frontend-provided summary, fallback to DB
        dataset_list: List[Dict[str, Any]] = []
        if state_snapshot and state_snapshot.dataset_summary:
            dataset_list = [
                item.model_dump() for item in state_snapshot.dataset_summary
            ]
        else:
            dataset_list = self._get_all_datasets_summary()

        # Active dataset detail (full schema for focused dataset)
        dataset_summary = None
        if active_dataset_id:
            dataset_summary = self._get_dataset_detail(active_dataset_id)

        # Tone mode resolution
        tone_str = prefs.get("tone_mode")
        tone_mode = resolve_tone_mode(user_preference=tone_str)

        # Quiet mode
        quiet_mode = prefs.get("quiet_mode", False)
        if os.environ.get("ALLAI_QUIET_MODE", "").lower() == "true":
            quiet_mode = True

        # Rate limits (placeholder — real values come from metering service)
        remaining_tokens = None
        daily_limit = None
        if connected_mode:
            remaining_tokens = 100_000  # Placeholder
            daily_limit = 100_000

        # Capabilities based on deployment
        capabilities = self._resolve_capabilities(connected_mode, local_only)

        return AllieContext(
            screen=screen,
            route=route,
            selection=selection,
            dataset_summary=dataset_summary,
            dataset_list=dataset_list,
            connected_mode=connected_mode,
            vectorization_enabled=vectorization_enabled,
            qdrant_status=qdrant_status,
            capabilities=capabilities,
            recent_events=[],  # Populated in Phase 3 (proactive triggers)
            triggers={},  # Populated in Phase 3
            remaining_tokens_today=remaining_tokens,
            daily_token_limit=daily_limit,
            tone_mode=tone_mode.value,
            quiet_mode=quiet_mode,
            local_only=local_only,
        )

    @staticmethod
    def _route_to_screen(route: str) -> str:
        """Map a frontend route to a screen identifier."""
        route_lower = route.lower().rstrip("/")

        route_map = {
            "/datasets": "datasets_list",
            "/settings": "settings",
            "/dashboard": "dashboard",
            "/earnings": "earnings",
            "/marketplace": "marketplace",
            "/data-requests": "data_request_board",
            "/data-requests/new": "data_request_create",
            "/dashboard/requests": "data_request_dashboard",
        }

        # Exact match first
        if route_lower in route_map:
            return route_map[route_lower]

        # Pattern matches
        if "/data-requests/" in route_lower:
            return "data_request_detail"

        if "/datasets/" in route_lower:
            if "/preview" in route_lower:
                return "data_preview"
            if "/query" in route_lower:
                return "query_builder"
            if "/upload" in route_lower:
                return "upload_wizard"
            return "dataset_detail"

        return "unknown"

    @staticmethod
    def _resolve_capabilities(connected_mode: bool, local_only: bool) -> Dict[str, bool]:
        """Resolve available capabilities based on deployment mode."""
        caps = {
            "can_preview_rows": True,
            "can_convert_encoding": True,
            "can_run_pii_scan": True,
            "can_generate_diagnostic_bundle": True,
            "can_run_query": True,
            "can_modify_settings": False,
            "can_push_to_marketplace": connected_mode and not local_only,
        }
        return caps

    @staticmethod
    def _get_all_datasets_summary() -> List[Dict[str, Any]]:
        """Fetch all non-deleted datasets with metadata from DB."""
        try:
            from app.services.processing_service import get_processing_service
            svc = get_processing_service()
            records = svc.list_datasets()
            return [
                {
                    "id": r.id,
                    "filename": r.original_filename,
                    "file_type": r.file_type,
                    "status": r.status.value if hasattr(r.status, "value") else str(r.status),
                    "rows": r.metadata.get("row_count"),
                    "columns": r.metadata.get("column_count"),
                    "size_bytes": r.file_size_bytes,
                }
                for r in records
            ]
        except Exception as e:
            logger.warning("Failed to fetch dataset list for context: %s", e)
            return []

    @staticmethod
    def _get_dataset_detail(dataset_id: str) -> Optional[Dict[str, Any]]:
        """Get full metadata for the active dataset (schema, types, stats)."""
        try:
            from app.services.processing_service import get_processing_service
            svc = get_processing_service()
            record = svc.get_dataset(dataset_id)
            if not record:
                return {"dataset_id": dataset_id, "status": "not_found"}
            return {
                "dataset_id": record.id,
                "filename": record.original_filename,
                "file_type": record.file_type,
                "status": record.status.value if hasattr(record.status, "value") else str(record.status),
                "rows": record.metadata.get("row_count"),
                "columns": record.metadata.get("column_count"),
                "column_names": record.metadata.get("column_names", []),
                "dtypes": record.metadata.get("dtypes", {}),
                "size_bytes": record.file_size_bytes,
                "created_at": record.created_at.isoformat() if record.created_at else None,
            }
        except Exception as e:
            logger.warning("Failed to fetch dataset detail for %s: %s", dataset_id, e)
            return {"dataset_id": dataset_id, "status": "error", "note": str(e)}


def _check_vectorization_enabled() -> bool:
    """Check if vectorization is enabled in this deployment."""
    return os.environ.get("VECTORAIZ_VECTORIZATION_ENABLED", "true").lower() == "true"


def _check_qdrant_status() -> str:
    """Check Qdrant status. Placeholder — real health check in Phase 3."""
    return os.environ.get("VECTORAIZ_QDRANT_STATUS", "healthy")



# Module-level singleton
context_manager = CoPilotContextManager()
