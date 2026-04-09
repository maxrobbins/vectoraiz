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
import re
from typing import Any, Dict, List, Optional

from app.models.copilot import StateSnapshot
from app.services.prompt_factory import AllieContext, resolve_tone_mode
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

        full_schema_graph = self._build_full_schema_graph(active_dataset_id=active_dataset_id)

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
            full_schema_graph=full_schema_graph,
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
            column_names, dtypes = CoPilotContextManager._extract_column_metadata(record)
            return {
                "dataset_id": record.id,
                "filename": record.original_filename,
                "file_type": record.file_type,
                "status": record.status.value if hasattr(record.status, "value") else str(record.status),
                "rows": record.metadata.get("row_count"),
                "columns": record.metadata.get("column_count"),
                "column_names": column_names,
                "dtypes": dtypes,
                "size_bytes": record.file_size_bytes,
                "created_at": record.created_at.isoformat() if record.created_at else None,
            }
        except Exception as e:
            logger.warning("Failed to fetch dataset detail for %s: %s", dataset_id, e)
            return {"dataset_id": dataset_id, "status": "error", "note": str(e)}

    @staticmethod
    def _build_full_schema_graph(active_dataset_id: Optional[str] = None) -> Dict[str, Any]:
        """Collect schemas for all SQL-ready datasets and infer likely joins."""
        try:
            from app.services.processing_service import get_processing_service

            svc = get_processing_service()
            records = svc.list_datasets()
            ready_records = [
                record
                for record in records
                if CoPilotContextManager._is_queryable_dataset(record)
            ]

            tables = [
                CoPilotContextManager._build_table_schema(record)
                for record in ready_records
            ]
            joins = CoPilotContextManager._detect_likely_joins(tables)

            if active_dataset_id:
                tables.sort(
                    key=lambda table: (
                        table.get("dataset_id") != active_dataset_id,
                        table.get("display_name", ""),
                    )
                )

            return {
                "active_dataset_id": active_dataset_id,
                "table_count": len(tables),
                "tables": tables,
                "joins": joins,
            }
        except Exception as e:
            logger.warning("Failed to build full schema graph: %s", e)
            return {
                "active_dataset_id": active_dataset_id,
                "table_count": 0,
                "tables": [],
                "joins": [],
                "status": "error",
                "note": str(e),
            }

    @staticmethod
    def _is_queryable_dataset(record: Any) -> bool:
        """Return True when the dataset can be queried as a DuckDB view."""
        status = record.status.value if hasattr(record.status, "value") else str(record.status)
        return status == "ready" and bool(record.processed_path)

    @staticmethod
    def _build_table_schema(record: Any) -> Dict[str, Any]:
        """Convert a dataset record into prompt-friendly table schema metadata."""
        column_names, dtypes = CoPilotContextManager._extract_column_metadata(record)
        return {
            "dataset_id": record.id,
            "table_name": f"dataset_{record.id}",
            "display_name": CoPilotContextManager._dataset_display_name(record.original_filename, record.id),
            "filename": record.original_filename,
            "status": record.status.value if hasattr(record.status, "value") else str(record.status),
            "row_count": record.metadata.get("row_count"),
            "column_count": record.metadata.get("column_count"),
            "columns": [
                {"name": column_name, "type": dtypes.get(column_name)}
                for column_name in column_names
            ],
        }

    @staticmethod
    def _extract_column_metadata(record: Any) -> tuple[List[str], Dict[str, Any]]:
        """Extract column names and dtypes from available dataset metadata."""
        column_names = record.metadata.get("column_names", []) or []
        dtypes = dict(record.metadata.get("dtypes", {}) or {})
        columns_info = record.metadata.get("columns", []) or []

        if not column_names and columns_info:
            column_names = [c.get("name", "?") for c in columns_info if c.get("name")]

        if columns_info:
            for column in columns_info:
                name = column.get("name")
                if name and not dtypes.get(name):
                    dtypes[name] = column.get("type")

        return column_names, dtypes

    @staticmethod
    def _detect_likely_joins(tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Infer likely foreign-key joins across loaded dataset tables."""
        joins: List[Dict[str, Any]] = []
        seen: set[tuple[str, str, str, str]] = set()
        table_index = {
            table["dataset_id"]: {
                "table_name": table["table_name"],
                "display_name": table["display_name"],
                "columns": [col.get("name") for col in table.get("columns", []) if col.get("name")],
                "column_set": {col.get("name") for col in table.get("columns", []) if col.get("name")},
                "tokens": CoPilotContextManager._dataset_name_tokens(table["display_name"]),
            }
            for table in tables
        }

        for source in tables:
            source_meta = table_index[source["dataset_id"]]
            for column_name in source_meta["columns"]:
                normalized_column = column_name.lower()
                if normalized_column == "id":
                    continue

                if normalized_column.endswith("_id"):
                    fk_tokens = CoPilotContextManager._column_fk_tokens(normalized_column)
                    for target in tables:
                        if target["dataset_id"] == source["dataset_id"]:
                            continue
                        target_meta = table_index[target["dataset_id"]]
                        if "id" not in target_meta["column_set"]:
                            continue
                        if CoPilotContextManager._tokens_match_target(fk_tokens, target_meta["tokens"]):
                            CoPilotContextManager._append_join(
                                joins,
                                seen,
                                source_table=source_meta["table_name"],
                                source_display=source_meta["display_name"],
                                source_column=column_name,
                                target_table=target_meta["table_name"],
                                target_display=target_meta["display_name"],
                                target_column="id",
                                reason="fk_name_match",
                            )

                for target in tables:
                    if target["dataset_id"] == source["dataset_id"]:
                        continue
                    target_meta = table_index[target["dataset_id"]]
                    if column_name not in target_meta["column_set"]:
                        continue
                    if normalized_column in {"id", "created_at", "updated_at"}:
                        continue
                    if source_meta["table_name"] >= target_meta["table_name"]:
                        continue
                    CoPilotContextManager._append_join(
                        joins,
                        seen,
                        source_table=source_meta["table_name"],
                        source_display=source_meta["display_name"],
                        source_column=column_name,
                        target_table=target_meta["table_name"],
                        target_display=target_meta["display_name"],
                        target_column=column_name,
                        reason="shared_column_name",
                    )

        return joins

    @staticmethod
    def _append_join(
        joins: List[Dict[str, Any]],
        seen: set[tuple[str, str, str, str]],
        *,
        source_table: str,
        source_display: str,
        source_column: str,
        target_table: str,
        target_display: str,
        target_column: str,
        reason: str,
    ) -> None:
        key = (source_table, source_column, target_table, target_column)
        if key in seen:
            return
        seen.add(key)
        joins.append(
            {
                "from_table": source_table,
                "from_display_name": source_display,
                "from_column": source_column,
                "to_table": target_table,
                "to_display_name": target_display,
                "to_column": target_column,
                "reason": reason,
            }
        )

    @staticmethod
    def _dataset_display_name(filename: str, dataset_id: str) -> str:
        """Use the filename stem as the human-readable dataset label."""
        stem = str(filename or "").strip().rsplit(".", 1)[0].strip()
        return stem or dataset_id

    @staticmethod
    def _dataset_name_tokens(name: str) -> set[str]:
        """Normalize a dataset label into singular/plural-insensitive tokens."""
        raw_tokens = [token for token in re.split(r"[^a-z0-9]+", name.lower()) if token]
        expanded: set[str] = set()
        for token in raw_tokens:
            expanded.add(token)
            singular = CoPilotContextManager._singularize_token(token)
            if singular:
                expanded.add(singular)
        return expanded

    @staticmethod
    def _column_fk_tokens(column_name: str) -> set[str]:
        """Extract tokens from a foreign-key-looking column name."""
        base = column_name[:-3] if column_name.endswith("_id") else column_name
        raw_tokens = [token for token in base.split("_") if token and token not in {"issuer", "debtor", "source", "target"}]
        expanded: set[str] = set()
        for token in raw_tokens:
            expanded.add(token)
            singular = CoPilotContextManager._singularize_token(token)
            if singular:
                expanded.add(singular)
        return expanded

    @staticmethod
    def _tokens_match_target(source_tokens: set[str], target_tokens: set[str]) -> bool:
        """Match FK-ish column tokens against target dataset name tokens."""
        if not source_tokens or not target_tokens:
            return False
        overlap = source_tokens & target_tokens
        if len(overlap) >= 2:
            return True
        if len(overlap) == 1:
            shared = next(iter(overlap))
            return len(shared) >= 5
        return False

    @staticmethod
    def _singularize_token(token: str) -> str:
        """Basic singularization heuristic for dataset/column matching."""
        if token.endswith("ies") and len(token) > 3:
            return token[:-3] + "y"
        if token.endswith("ses") and len(token) > 3:
            return token[:-2]
        if token.endswith("s") and not token.endswith("ss") and len(token) > 3:
            return token[:-1]
        return token


def _check_vectorization_enabled() -> bool:
    """Check if vectorization is enabled in this deployment."""
    return os.environ.get("VECTORAIZ_VECTORIZATION_ENABLED", "true").lower() == "true"


def _check_qdrant_status() -> str:
    """Check Qdrant status. Placeholder — real health check in Phase 3."""
    return os.environ.get("VECTORAIZ_QDRANT_STATUS", "healthy")



# Module-level singleton
context_manager = CoPilotContextManager()
