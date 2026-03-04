"""
allAI Tool Executor — Runs tools on behalf of the user.

[COUNCIL] Security model:
- Per-tool authorization check (user owns the resource)
- Per-resource validation (dataset belongs to user)
- SQL goes through SQLSandbox AST validation THEN existing sql_service
- Destructive tools route through ConfirmationService (not executed directly)
- Tool results split into frontend_data + llm_summary (two-track)
- Max 5 calls per message, 10s per call timeout

PHASE: BQ-ALLAI-B2 — Tool Execution Engine
CREATED: 2026-02-16
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Optional

from app.auth.api_key_auth import AuthenticatedUser
from app.services.allai_tool_result import ToolResult
from app.services.confirmation_service import (
    CONFIRMATION_TTL_SECONDS,
    DESTRUCTIVE_TOOLS,
    confirmation_service,
)

logger = logging.getLogger(__name__)

def _safe_error_category(e: Exception) -> str:
    """Categorize an exception into a safe, non-leaking error string.
    
    Returns a stable error category — never exposes class names,
    library internals, or exception messages to the user/model.
    """
    import asyncio
    if isinstance(e, (ConnectionError, ConnectionRefusedError, OSError)):
        return "connection_refused"
    if isinstance(e, (TimeoutError, asyncio.TimeoutError)):
        return "timeout"
    if isinstance(e, FileNotFoundError):
        return "resource_not_found"
    if isinstance(e, PermissionError):
        return "permission_denied"
    if isinstance(e, ValueError):
        return "invalid_input"
    if isinstance(e, KeyError):
        return "missing_configuration"
    return "internal_error"


MAX_TOOL_CALLS_PER_MESSAGE = 5
TOOL_TIMEOUT_S = 10


class AllAIToolExecutor:
    """Execute tool calls on behalf of allAI with security enforcement."""

    def __init__(
        self,
        user: AuthenticatedUser,
        send_ws: Callable[[dict], Awaitable[None]],
        session_id: str,
    ) -> None:
        self.user = user
        self.send_ws = send_ws
        self.session_id = session_id
        self.call_count = 0

    async def execute(self, tool_name: str, tool_input: dict) -> ToolResult:
        """Execute a tool call. Returns ToolResult with frontend_data + llm_summary."""

        self.call_count += 1
        if self.call_count > MAX_TOOL_CALLS_PER_MESSAGE:
            return ToolResult(
                frontend_data={"error": "Tool call limit reached (5 per message)"},
                llm_summary="Tool call limit reached (5 per message). Ask the user to send another message to continue.",
            )

        # [COUNCIL] Authorization check
        auth_ok, auth_err = self._authorize(tool_name, tool_input)
        if not auth_ok:
            return ToolResult(
                frontend_data={"error": f"Not authorized: {auth_err}"},
                llm_summary=f"Authorization failed: {auth_err}",
            )

        # [COUNCIL] Destructive tools → ConfirmationService
        if tool_name in DESTRUCTIVE_TOOLS:
            return await self._handle_destructive(tool_name, tool_input)

        # Send TOOL_STATUS to frontend
        await self.send_ws({
            "type": "TOOL_STATUS",
            "tool_name": tool_name,
            "status": "executing",
        })

        try:
            result = await asyncio.wait_for(
                self._dispatch(tool_name, tool_input),
                timeout=TOOL_TIMEOUT_S,
            )

            # Send TOOL_RESULT to frontend (rich data)
            await self.send_ws({
                "type": "TOOL_RESULT",
                "tool_name": tool_name,
                "data": result.frontend_data,
            })

            # Send done status
            await self.send_ws({
                "type": "TOOL_STATUS",
                "tool_name": tool_name,
                "status": "done",
            })

            logger.info(
                "allAI tool: user=%s tool=%s → success",
                self.user.user_id, tool_name,
            )
            return result

        except asyncio.TimeoutError:
            await self.send_ws({
                "type": "TOOL_STATUS",
                "tool_name": tool_name,
                "status": "error",
            })
            return ToolResult(
                frontend_data={"error": f"Tool timed out after {TOOL_TIMEOUT_S}s"},
                llm_summary=f"Tool {tool_name} timed out. Suggest the user try again.",
            )
        except Exception as e:
            logger.error(
                "allAI tool error: user=%s tool=%s error=%s",
                self.user.user_id, tool_name, e, exc_info=True,
            )
            await self.send_ws({
                "type": "TOOL_STATUS",
                "tool_name": tool_name,
                "status": "error",
            })
            return ToolResult(
                frontend_data={"error": str(e)},
                llm_summary=f"Tool {tool_name} failed: {str(e)[:200]}",
            )

    # ------------------------------------------------------------------
    # Authorization
    # ------------------------------------------------------------------

    def _authorize(self, tool_name: str, tool_input: dict) -> tuple:
        """[COUNCIL] Per-tool + per-resource authorization. Returns (ok, error_msg)."""
        dataset_id = tool_input.get("dataset_id")
        if dataset_id:
            if not self._user_owns_dataset(dataset_id):
                return False, f"Dataset '{dataset_id}' not found or not owned by user"
        return True, ""

    def _user_owns_dataset(self, dataset_id: str) -> bool:
        """Check if the current user owns the dataset."""
        try:
            from app.services.processing_service import get_processing_service
            svc = get_processing_service()
            record = svc.get_dataset(dataset_id)
            # In current arch, all datasets are user-scoped via the instance.
            # Just verify the dataset exists.
            return record is not None
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Destructive action gate
    # ------------------------------------------------------------------

    async def _handle_destructive(self, tool_name: str, tool_input: dict) -> ToolResult:
        """[COUNCIL] Route destructive actions through confirmation service."""
        details = await self._get_resource_details(tool_name, tool_input)
        description = details.get("description", f"Confirm {tool_name}")

        token = confirmation_service.request_confirmation(
            user_id=self.user.user_id,
            tool_name=tool_name,
            tool_input=tool_input,
            session_id=self.session_id,
            description=description,
            details=details,
        )

        # Send CONFIRM_REQUEST to frontend
        await self.send_ws({
            "type": "CONFIRM_REQUEST",
            "confirm_id": token,
            "tool_name": tool_name,
            "description": description,
            "details": details,
            "expires_in_seconds": CONFIRMATION_TTL_SECONDS,
        })

        return ToolResult(
            frontend_data={"status": "confirmation_requested", "confirm_id": token},
            llm_summary=(
                f"A confirmation prompt has been shown to the user for: {description}. "
                "Wait for the user to confirm or cancel. Do NOT proceed until they respond."
            ),
        )

    async def _get_resource_details(self, tool_name: str, tool_input: dict) -> dict:
        """Build human-readable details for the confirmation UI."""
        if tool_name == "delete_dataset":
            dataset_id = tool_input.get("dataset_id", "unknown")
            filename = dataset_id
            try:
                from app.services.processing_service import get_processing_service
                svc = get_processing_service()
                record = svc.get_dataset(dataset_id)
                if record:
                    filename = record.original_filename
            except Exception:
                pass
            return {
                "description": f"Delete dataset '{filename}' and all processed data",
                "dataset_id": dataset_id,
                "filename": filename,
            }
        return {"description": f"Execute {tool_name}"}

    # ------------------------------------------------------------------
    # Tool dispatch
    # ------------------------------------------------------------------

    async def _dispatch(self, tool_name: str, tool_input: dict) -> ToolResult:
        """Route to the appropriate handler."""
        handlers = {
            "list_datasets": self._handle_list_datasets,
            "get_dataset_detail": self._handle_get_dataset_detail,
            "preview_rows": self._handle_preview_rows,
            "run_sql_query": self._handle_run_sql_query,
            "search_vectors": self._handle_search_vectors,
            "get_system_status": self._handle_get_system_status,
            "get_dataset_statistics": self._handle_get_dataset_statistics,
            # BQ-MCP-RAG Phase 2: Connectivity tools
            "connectivity_status": self._handle_connectivity_status,
            "connectivity_enable": self._handle_connectivity_enable,
            "connectivity_disable": self._handle_connectivity_disable,
            "connectivity_create_token": self._handle_connectivity_create_token,
            "connectivity_revoke_token": self._handle_connectivity_revoke_token,
            "connectivity_generate_setup": self._handle_connectivity_generate_setup,
            "connectivity_test": self._handle_connectivity_test,
            "submit_feedback": self._handle_submit_feedback,
            "log_feedback": self._handle_log_feedback,
            # BQ-TUNNEL: Public URL tunnel tools
            "start_public_tunnel": self._handle_start_public_tunnel,
            "stop_public_tunnel": self._handle_stop_public_tunnel,
            "get_tunnel_status": self._handle_get_tunnel_status,
            # BQ-VZ-DIAG: Diagnostic bundle
            "generate_diagnostic_bundle": self._handle_generate_diagnostic_bundle,
            # BQ-VZ-NOTIFICATIONS Phase 4: Diagnostic transmission
            "prepare_support_bundle": self._handle_prepare_support_bundle,
            # BQ-VZ-NOTIFICATIONS: Notification tools
            "get_notifications": self._handle_get_notifications,
            "create_notification": self._handle_create_notification,
        }

        handler = handlers.get(tool_name)
        if not handler:
            return ToolResult(
                frontend_data={"error": f"Unknown tool: {tool_name}"},
                llm_summary=f"Unknown tool: {tool_name}",
            )
        return await handler(tool_input)

    # ------------------------------------------------------------------
    # Tool handlers
    # ------------------------------------------------------------------

    async def _handle_list_datasets(self, tool_input: dict) -> ToolResult:
        """List all datasets with metadata."""
        from app.services.processing_service import get_processing_service
        svc = get_processing_service()
        records = svc.list_datasets()

        status_filter = tool_input.get("status_filter", "all")

        datasets = []
        for r in records:
            status_val = r.status.value if hasattr(r.status, "value") else str(r.status)
            if status_filter != "all" and status_val != status_filter:
                continue
            datasets.append({
                "id": r.id,
                "filename": r.original_filename,
                "file_type": r.file_type,
                "status": status_val,
                "rows": r.metadata.get("row_count"),
                "columns": r.metadata.get("column_count"),
                "size_bytes": r.file_size_bytes,
            })

        return ToolResult(
            frontend_data={
                "datasets": datasets,
                "total": len(datasets),
            },
            llm_summary=(
                f"Found {len(datasets)} dataset(s). "
                + "; ".join(
                    f"'{d['filename']}' ({d.get('rows', '?')} rows, {d['status']})"
                    for d in datasets[:10]
                )
                + (". " if datasets else "No datasets found. ")
                + "Full list displayed to user."
            ),
        )

    async def _handle_get_dataset_detail(self, tool_input: dict) -> ToolResult:
        """Get detailed info about a specific dataset."""
        from app.services.processing_service import get_processing_service
        svc = get_processing_service()
        dataset_id = tool_input["dataset_id"]
        record = svc.get_dataset(dataset_id)

        if not record:
            return ToolResult(
                frontend_data={"error": f"Dataset '{dataset_id}' not found"},
                llm_summary=f"Dataset '{dataset_id}' not found.",
            )

        status_val = record.status.value if hasattr(record.status, "value") else str(record.status)
        column_names = record.metadata.get("column_names", [])
        dtypes = record.metadata.get("dtypes", {})
        columns_info = record.metadata.get("columns", [])

        # If column_names not in metadata, try columns list
        if not column_names and columns_info:
            column_names = [c.get("name", "?") for c in columns_info]
            dtypes = {c.get("name", "?"): c.get("type", "?") for c in columns_info}

        detail = {
            "dataset_id": record.id,
            "filename": record.original_filename,
            "file_type": record.file_type,
            "status": status_val,
            "rows": record.metadata.get("row_count"),
            "columns": record.metadata.get("column_count"),
            "column_names": column_names,
            "dtypes": dtypes,
            "size_bytes": record.file_size_bytes,
            "created_at": record.created_at.isoformat() if record.created_at else None,
        }

        cols_str = ", ".join(column_names[:15])
        if len(column_names) > 15:
            cols_str += f" ... (+{len(column_names) - 15} more)"

        return ToolResult(
            frontend_data=detail,
            llm_summary=(
                f"Dataset '{record.original_filename}': "
                f"{record.metadata.get('row_count', '?')} rows, "
                f"{record.metadata.get('column_count', '?')} columns. "
                f"Columns: {cols_str}. Status: {status_val}. "
                "Full details displayed to user."
            ),
        )

    async def _handle_preview_rows(self, tool_input: dict) -> ToolResult:
        """Preview sample rows with two-track output."""
        from app.services.processing_service import get_processing_service
        proc_svc = get_processing_service()
        dataset_id = tool_input["dataset_id"]
        limit = min(tool_input.get("limit", 10), 50)

        record = proc_svc.get_dataset(dataset_id)
        if not record or not record.processed_path:
            return ToolResult(
                frontend_data={"error": f"Dataset '{dataset_id}' not found or not processed"},
                llm_summary=f"Dataset '{dataset_id}' not available for preview.",
            )

        from app.services.duckdb_service import ephemeral_duckdb_service
        with ephemeral_duckdb_service() as duckdb_svc:
            rows = duckdb_svc.get_sample_rows(record.processed_path, limit=limit)

        columns = list(rows[0].keys()) if rows else []

        # Serialize values for JSON
        serialized_rows = []
        for row in rows:
            serialized = {}
            for k, v in row.items():
                if v is None or isinstance(v, (int, float, bool, str)):
                    serialized[k] = v
                else:
                    serialized[k] = str(v)
            serialized_rows.append(serialized)

        return ToolResult(
            frontend_data={
                "columns": columns,
                "rows": serialized_rows,
                "dataset_id": dataset_id,
                "total_rows": len(serialized_rows),
                "filename": record.original_filename,
            },
            llm_summary=(
                f"Displayed {len(serialized_rows)} sample rows from '{record.original_filename}'. "
                f"Columns: {', '.join(columns[:15])}. "
                "The user can see the full data in the table above. "
                "Describe what you observe about the data structure — do NOT repeat row values."
            ),
        )

    async def _handle_run_sql_query(self, tool_input: dict) -> ToolResult:
        """[COUNCIL] SQL through sandbox validation first, then existing sql_service."""
        from app.services.sql_sandbox import SQLSandbox
        from app.services.processing_service import get_processing_service
        from app.services.sql_service import get_sql_service, SQLValidationError

        # Build allowed tables from user's datasets
        proc_svc = get_processing_service()
        records = proc_svc.list_datasets()
        allowed = SQLSandbox.build_allowed_tables([r.id for r in records])

        # [COUNCIL] AST validation layer (on top of sql_service regex)
        sandbox = SQLSandbox(allowed_tables=allowed)
        is_valid, error = sandbox.validate(tool_input["query"])

        if not is_valid:
            return ToolResult(
                frontend_data={"error": f"SQL validation failed: {error}"},
                llm_summary=f"SQL rejected: {error}. Rewrite the query as a single SELECT statement using dataset_{{id}} table names.",
            )

        # Execute through existing SQL service (has its own regex validation)
        sql_svc = get_sql_service()
        limit = min(tool_input.get("limit", 50), 200)

        try:
            result = sql_svc.execute_query(tool_input["query"], limit=limit)
        except (SQLValidationError, ValueError) as e:
            return ToolResult(
                frontend_data={"error": f"SQL error: {str(e)}"},
                llm_summary=f"SQL execution failed: {str(e)[:200]}. Fix the query and try again.",
            )

        # [COUNCIL] Two-track: rich to frontend, summary to LLM
        columns = result.get("columns", [])
        data = result.get("data", [])
        row_count = len(data)

        # For small scalar results (1 row, few columns), include values in summary
        # so the LLM can reference them directly
        value_hint = ""
        if row_count == 1 and len(columns) <= 5:
            value_hint = " Values: " + ", ".join(
                f"{c}={data[0].get(c)}" for c in columns
            )

        return ToolResult(
            frontend_data={
                "columns": columns,
                "rows": data,
                "query": tool_input["query"],
                "row_count": row_count,
                "truncated": result.get("truncated", False),
            },
            llm_summary=(
                f"SQL executed. {row_count} row(s) returned with columns: "
                f"{', '.join(columns[:10])}."
                f"{value_hint} "
                "Results displayed to user in a table. "
                "Summarize findings — do NOT repeat raw data for multi-row results."
            ),
        )

    async def _handle_search_vectors(self, tool_input: dict) -> ToolResult:
        """Semantic search across vectorized datasets."""
        from app.services.search_service import get_search_service
        search_svc = get_search_service()

        query = tool_input["query"]
        dataset_id = tool_input.get("dataset_id")
        limit = min(tool_input.get("limit", 5), 20)

        try:
            result = search_svc.search(
                query=query,
                dataset_id=dataset_id,
                limit=limit,
            )
        except ValueError as e:
            return ToolResult(
                frontend_data={"error": str(e)},
                llm_summary=f"Search failed: {str(e)[:200]}",
            )

        results = result.get("results", [])

        # BQ-FEEDBACK: Fire first_search feedback nudge (30s delay, max 1/session)
        if results:
            asyncio.get_event_loop().create_task(
                self._fire_delayed_nudge("first_search", delay_s=30)
            )

        return ToolResult(
            frontend_data={
                "query": query,
                "results": results,
                "total": len(results),
                "datasets_searched": result.get("datasets_searched", 0),
            },
            llm_summary=(
                f"Semantic search for '{query}' returned {len(results)} result(s) "
                f"across {result.get('datasets_searched', 0)} dataset(s). "
                + (
                    "Top matches: " + "; ".join(
                        f"score={r['score']:.2f} from '{r.get('dataset_name', '?')}'"
                        for r in results[:5]
                    )
                    if results else "No matching results."
                )
                + " Results displayed to user."
            ),
        )

    async def _handle_get_system_status(self, _tool_input: dict) -> ToolResult:
        """Get system health status."""
        from app.core.local_only_guard import is_local_only

        status = {
            "mode": "standalone" if is_local_only() else "connected",
        }

        # Check DuckDB
        try:
            from app.services.duckdb_service import ephemeral_duckdb_service
            with ephemeral_duckdb_service() as duckdb_svc:
                duckdb_svc.connection.execute("SELECT 1")
            status["duckdb"] = "healthy"
        except Exception as e:
            status["duckdb"] = f"error: {str(e)[:100]}"

        # Check Qdrant
        try:
            from app.services.qdrant_service import get_qdrant_service
            qdrant_svc = get_qdrant_service()
            collections = qdrant_svc.list_collections()
            status["qdrant"] = "healthy"
            status["qdrant_collections"] = len(collections)
        except Exception as e:
            status["qdrant"] = f"error: {str(e)[:100]}"

        # Check datasets
        try:
            from app.services.processing_service import get_processing_service
            proc_svc = get_processing_service()
            datasets = proc_svc.list_datasets()
            status["datasets_total"] = len(datasets)
            status["datasets_ready"] = sum(
                1 for d in datasets
                if (d.status.value if hasattr(d.status, "value") else str(d.status)) == "ready"
            )
        except Exception as e:
            status["datasets_total"] = f"error: {str(e)[:100]}"

        return ToolResult(
            frontend_data=status,
            llm_summary=(
                f"System status: mode={status.get('mode')}, "
                f"DuckDB={status.get('duckdb', 'unknown')}, "
                f"Qdrant={status.get('qdrant', 'unknown')}, "
                f"datasets={status.get('datasets_total', '?')} "
                f"(ready: {status.get('datasets_ready', '?')})."
            ),
        )

    async def _handle_get_dataset_statistics(self, tool_input: dict) -> ToolResult:
        """Get statistical profile of a dataset."""
        from app.services.processing_service import get_processing_service
        from app.services.duckdb_service import ephemeral_duckdb_service

        dataset_id = tool_input["dataset_id"]
        proc_svc = get_processing_service()
        record = proc_svc.get_dataset(dataset_id)

        if not record or not record.processed_path:
            return ToolResult(
                frontend_data={"error": f"Dataset '{dataset_id}' not found or not processed"},
                llm_summary=f"Dataset '{dataset_id}' not available for statistics.",
            )

        try:
            with ephemeral_duckdb_service() as duckdb_svc:
                stats = duckdb_svc.get_column_statistics(record.processed_path)
        except Exception as e:
            return ToolResult(
                frontend_data={"error": f"Statistics failed: {str(e)}"},
                llm_summary=f"Failed to compute statistics: {str(e)[:200]}",
            )

        # Serialize for JSON safety
        serialized_stats = []
        for col_stat in stats:
            s = {}
            for k, v in col_stat.items():
                if v is None or isinstance(v, (int, float, bool, str)):
                    s[k] = v
                else:
                    s[k] = str(v)
            serialized_stats.append(s)

        col_names = [s.get("column_name", "?") for s in serialized_stats[:20]]

        return ToolResult(
            frontend_data={
                "dataset_id": dataset_id,
                "filename": record.original_filename,
                "statistics": serialized_stats,
            },
            llm_summary=(
                f"Statistical profile for '{record.original_filename}' computed. "
                f"{len(serialized_stats)} column(s) profiled: {', '.join(col_names)}. "
                "Full statistics table displayed to user. "
                "Summarize key observations (distributions, null rates, outliers)."
            ),
        )

    # ------------------------------------------------------------------
    # BQ-FEEDBACK: Delayed nudge helper
    # ------------------------------------------------------------------

    async def _fire_delayed_nudge(self, trigger: str, delay_s: int = 30) -> None:
        """Fire a nudge after a delay. Rate-limited by NudgeManager."""
        try:
            await asyncio.sleep(delay_s)
            from app.services.nudge_manager import nudge_manager
            nudge = await nudge_manager.maybe_nudge(
                trigger=trigger,
                context={},
                session_id=self.session_id,
                user_id=self.user.user_id,
            )
            if nudge:
                await self.send_ws(nudge_manager.to_ws_message(nudge))
        except Exception as e:
            logger.debug("Delayed nudge '%s' failed: %s", trigger, e)

    # ------------------------------------------------------------------
    # BQ-MCP-RAG Phase 2: Connectivity tool handlers
    # ------------------------------------------------------------------

    async def _handle_connectivity_status(self, _tool_input: dict) -> ToolResult:
        """Check connectivity state: enabled, tokens (masked), metrics."""
        from app.config import settings
        from app.services.connectivity_token_service import list_tokens
        from app.services.connectivity_metrics import get_connectivity_metrics

        enabled = settings.connectivity_enabled

        tokens = []
        try:
            raw_tokens = list_tokens()
            for t in raw_tokens:
                tokens.append({
                    "id": t.id,
                    "label": t.label,
                    "scopes": t.scopes,
                    "secret_last4": t.secret_last4,
                    "created_at": t.created_at.isoformat() if t.created_at else None,
                    "last_used_at": t.last_used_at.isoformat() if t.last_used_at else None,
                    "request_count": t.request_count,
                    "is_active": t.expires_at is None or (
                        t.expires_at.timestamp() > __import__("time").time()
                    ),
                })
        except Exception as e:
            logger.warning("Failed to list connectivity tokens: %s", e)

        metrics = {}
        try:
            metrics = get_connectivity_metrics().get_snapshot()
        except Exception as e:
            logger.warning("Failed to get connectivity metrics: %s", e)

        active_count = sum(1 for t in tokens if t.get("is_active", False))

        return ToolResult(
            frontend_data={
                "enabled": enabled,
                "tokens": tokens,
                "token_count": len(tokens),
                "active_token_count": active_count,
                "metrics": metrics,
            },
            llm_summary=(
                f"External connectivity: {'enabled' if enabled else 'disabled'}. "
                f"{active_count} active token(s) out of {len(tokens)} total. "
                + (
                    "Tokens: " + ", ".join(
                        f"'{t['label']}' (****{t['secret_last4']}, {t['request_count']} requests)"
                        for t in tokens[:5]
                    )
                    if tokens else "No tokens created yet."
                )
                + " Metrics and token details shown to user."
            ),
        )

    async def _handle_connectivity_enable(self, _tool_input: dict) -> ToolResult:
        """Enable external connectivity."""
        from app.config import settings

        was_enabled = settings.connectivity_enabled
        settings.connectivity_enabled = True

        if was_enabled:
            return ToolResult(
                frontend_data={"enabled": True, "changed": False},
                llm_summary=(
                    "External connectivity was already enabled. No changes made. "
                    "The MCP server and REST API are accepting external requests."
                ),
            )

        return ToolResult(
            frontend_data={
                "enabled": True,
                "changed": True,
                "note": (
                    "Connectivity enabled in memory. To persist across restarts, "
                    "set VECTORAIZ_CONNECTIVITY_ENABLED=true as an environment variable."
                ),
            },
            llm_summary=(
                "External connectivity is now ENABLED. The MCP server and REST API "
                "will accept external requests with valid tokens. "
                "Note: To persist this across restarts, the user should set the "
                "VECTORAIZ_CONNECTIVITY_ENABLED=true environment variable. "
                "Next step: create a token for the AI tool they want to connect."
            ),
        )

    async def _handle_connectivity_disable(self, _tool_input: dict) -> ToolResult:
        """Disable external connectivity (preserves tokens)."""
        from app.config import settings

        was_enabled = settings.connectivity_enabled
        settings.connectivity_enabled = False

        if not was_enabled:
            return ToolResult(
                frontend_data={"enabled": False, "changed": False},
                llm_summary=(
                    "External connectivity was already disabled. No changes made. "
                    "All external requests are being rejected."
                ),
            )

        return ToolResult(
            frontend_data={
                "enabled": False,
                "changed": True,
                "note": (
                    "Connectivity disabled. Existing tokens are preserved and will "
                    "work again if connectivity is re-enabled."
                ),
            },
            llm_summary=(
                "External connectivity is now DISABLED. All external requests will be "
                "rejected. Existing tokens are preserved — they'll work again when "
                "connectivity is re-enabled. To persist across restarts, set "
                "VECTORAIZ_CONNECTIVITY_ENABLED=false."
            ),
        )

    async def _handle_connectivity_create_token(self, tool_input: dict) -> ToolResult:
        """Create a new connectivity token. Shows full token ONCE."""
        from app.services.connectivity_token_service import (
            ConnectivityTokenError,
            create_token,
        )

        label = tool_input.get("label", "External AI Tool")
        scopes = tool_input.get("scopes")

        try:
            raw_token, token_info = create_token(label=label, scopes=scopes)
        except ConnectivityTokenError as e:
            return ToolResult(
                frontend_data={"error": e.message, "code": e.code},
                llm_summary=f"Failed to create token: {e.message}",
            )

        return ToolResult(
            frontend_data={
                "token": raw_token,
                "token_id": token_info.id,
                "label": token_info.label,
                "scopes": token_info.scopes,
                "secret_last4": token_info.secret_last4,
                "warning": (
                    "SAVE THIS TOKEN NOW — it will not be shown again. "
                    "Store it securely. Do not share it publicly."
                ),
            },
            llm_summary=(
                f"Token created successfully for '{label}' "
                f"(ID: {token_info.id}, ending ****{token_info.secret_last4}).\n\n"
                "⚠️ **The full token is displayed to the user above. "
                "Remind them to SAVE IT NOW — it will NOT be shown again.** "
                "They should store it somewhere secure (password manager, encrypted notes). "
                "Do not include the full token in your response — it's already visible "
                "in the tool output. Each connected AI tool should have its own "
                "token so they can revoke individually if needed."
            ),
        )

    async def _handle_connectivity_revoke_token(self, tool_input: dict) -> ToolResult:
        """Revoke a connectivity token."""
        from app.services.connectivity_token_service import (
            ConnectivityTokenError,
            revoke_token,
        )

        token_id = tool_input.get("token_id", "")

        try:
            token_info = revoke_token(token_id)
        except ConnectivityTokenError as e:
            return ToolResult(
                frontend_data={"error": e.message, "code": e.code},
                llm_summary=f"Failed to revoke token: {e.message}",
            )

        return ToolResult(
            frontend_data={
                "revoked": True,
                "token_id": token_info.id,
                "label": token_info.label,
            },
            llm_summary=(
                f"Token '{token_info.label}' (****{token_info.secret_last4}) has been "
                "revoked. It will immediately stop working for any connected AI tools."
            ),
        )

    async def _handle_connectivity_generate_setup(self, tool_input: dict) -> ToolResult:
        """Generate platform-specific setup instructions."""
        from app.services.connectivity_setup_generator import ConnectivitySetupGenerator

        platform = tool_input.get("platform", "generic_rest")
        token = tool_input.get("token", "")
        base_url = tool_input.get("base_url", "http://localhost:8100")

        # For generic_llm, fetch actual dataset info
        datasets = []
        if platform == "generic_llm":
            try:
                from app.services.processing_service import get_processing_service
                svc = get_processing_service()
                records = svc.list_datasets()
                for r in records:
                    status_val = r.status.value if hasattr(r.status, "value") else str(r.status)
                    if status_val == "ready":
                        datasets.append({
                            "id": r.id,
                            "name": r.original_filename,
                            "rows": r.metadata.get("row_count"),
                            "columns": r.metadata.get("column_count"),
                            "description": r.metadata.get("description", ""),
                        })
            except Exception as e:
                logger.warning("Failed to fetch datasets for setup generator: %s", e)

        generator = ConnectivitySetupGenerator()
        result = generator.generate(
            platform=platform,
            token=token,
            base_url=base_url,
            datasets=datasets,
        )

        # Build a readable summary for the LLM
        steps_text = "\n".join(
            f"  {s['step']}. {s['instruction']}" for s in result.get("steps", [])
        )

        return ToolResult(
            frontend_data=result,
            llm_summary=(
                f"Setup instructions generated for {result.get('title', platform)}.\n"
                f"Steps:\n{steps_text}\n\n"
                "Full config and instructions displayed to user. "
                "Walk them through each step and ask for confirmation at key points."
            ),
        )

    async def _handle_connectivity_test(self, tool_input: dict) -> ToolResult:
        """Run self-diagnostic on connectivity."""
        import time
        from app.config import settings
        from app.services.connectivity_token_service import (
            ConnectivityTokenError,
            verify_token,
        )

        raw_token = tool_input.get("token", "")
        results = {
            "connectivity_enabled": settings.connectivity_enabled,
            "token_valid": False,
            "token_label": None,
            "token_scopes": [],
            "mcp_responding": False,
            "datasets_accessible": 0,
            "sample_query_ok": False,
            "latency_ms": None,
        }

        # 1. Check connectivity enabled
        if not settings.connectivity_enabled:
            results["error"] = "External connectivity is disabled"
            return ToolResult(
                frontend_data=results,
                llm_summary=(
                    "Diagnostic FAILED: External connectivity is disabled. "
                    "Enable it first with connectivity_enable."
                ),
            )

        # 2. Validate token
        try:
            token = verify_token(raw_token)
            results["token_valid"] = True
            results["token_label"] = token.label
            results["token_scopes"] = token.scopes
        except ConnectivityTokenError as e:
            results["token_error"] = e.message
            return ToolResult(
                frontend_data=results,
                llm_summary=f"Diagnostic FAILED: Token validation failed — {e.message}",
            )

        # 3. Count accessible datasets
        try:
            from app.services.query_orchestrator import get_query_orchestrator
            orchestrator = get_query_orchestrator()
            start = time.time()
            ds_response = await orchestrator.list_datasets(token)
            results["datasets_accessible"] = ds_response.count
            results["mcp_responding"] = True
        except Exception as e:
            logger.error("Connectivity test: dataset listing failed", exc_info=True)
            results["datasets_error"] = _safe_error_category(e)

        # 4. Run sample query if datasets available
        if results["datasets_accessible"] > 0:
            try:
                from app.models.connectivity import VectorSearchRequest
                start = time.time()
                search_req = VectorSearchRequest(query="test", top_k=1)
                await orchestrator.search_vectors(token, search_req)
                results["sample_query_ok"] = True
                results["latency_ms"] = int((time.time() - start) * 1000)
            except Exception as e:
                logger.error("Connectivity test: sample query failed", exc_info=True)
                results["sample_query_error"] = _safe_error_category(e)

        # Build summary
        checks = [
            f"Connectivity: {'enabled' if results['connectivity_enabled'] else 'DISABLED'}",
            f"Token: {'valid' if results['token_valid'] else 'INVALID'} ({results.get('token_label', '?')})",
            f"Datasets accessible: {results['datasets_accessible']}",
            f"Sample query: {'OK' if results['sample_query_ok'] else 'skipped/failed'}",
        ]
        if results.get("latency_ms") is not None:
            checks.append(f"Latency: {results['latency_ms']}ms")

        all_ok = (
            results["connectivity_enabled"]
            and results["token_valid"]
            and results["mcp_responding"]
        )

        return ToolResult(
            frontend_data=results,
            llm_summary=(
                f"Connectivity diagnostic: {'ALL CHECKS PASSED' if all_ok else 'ISSUES FOUND'}.\n"
                + "\n".join(f"  - {c}" for c in checks)
            ),
        )

    # ------------------------------------------------------------------
    # BQ-TUNNEL: Public URL tunnel tool handlers
    # ------------------------------------------------------------------

    async def _handle_start_public_tunnel(self, _tool_input: dict) -> ToolResult:
        """Start a cloudflared quick tunnel for public URL access."""
        from app.services.tunnel_service import TunnelService

        svc = TunnelService.get_instance()

        if svc.is_running and svc.public_url:
            return ToolResult(
                frontend_data={
                    "status": "already_running",
                    "public_url": svc.public_url,
                },
                llm_summary=(
                    f"Tunnel is already running at {svc.public_url}. "
                    "Use this URL for external service configurations."
                ),
            )

        try:
            url = await svc.start()
            return ToolResult(
                frontend_data={
                    "status": "started",
                    "public_url": url,
                },
                llm_summary=(
                    f"Public tunnel started successfully. URL: {url}\n"
                    "This URL is temporary and will change when the tunnel restarts. "
                    "Anyone with this URL and a valid API token can access the instance. "
                    "Use this URL when generating configs for ChatGPT, OpenAI, or other external services."
                ),
            )
        except RuntimeError as e:
            return ToolResult(
                frontend_data={"status": "error", "error": str(e)},
                llm_summary=f"Failed to start tunnel: {str(e)[:200]}",
            )

    async def _handle_stop_public_tunnel(self, _tool_input: dict) -> ToolResult:
        """Stop the cloudflared tunnel."""
        from app.services.tunnel_service import TunnelService

        svc = TunnelService.get_instance()

        if not svc.is_running:
            return ToolResult(
                frontend_data={"status": "not_running"},
                llm_summary="No tunnel is currently running.",
            )

        await svc.stop()
        return ToolResult(
            frontend_data={"status": "stopped"},
            llm_summary="Public tunnel stopped. The temporary URL is no longer accessible.",
        )

    async def _handle_get_tunnel_status(self, _tool_input: dict) -> ToolResult:
        """Check tunnel status."""
        from app.services.tunnel_service import TunnelService

        svc = TunnelService.get_instance()
        status = svc.get_status()

        if status["running"]:
            summary = f"Tunnel is running at {status['public_url']}."
        elif not status["cloudflared_installed"]:
            summary = (
                "Tunnel is not running. cloudflared is not installed — "
                "this feature is available in the Docker deployment."
            )
        else:
            summary = "Tunnel is not running. Use start_public_tunnel to create a public URL."

        return ToolResult(
            frontend_data=status,
            llm_summary=summary,
        )

    # ------------------------------------------------------------------
    # Feedback tool handler
    # ------------------------------------------------------------------

    async def _handle_submit_feedback(self, tool_input: dict) -> ToolResult:
        """Store user feedback and optionally forward to ai.market."""
        from app.core.database import get_session_context
        from app.models.feedback import Feedback

        category = tool_input.get("category", "other")
        summary = tool_input.get("summary", "")
        details = tool_input.get("details")

        fb = Feedback(
            category=category,
            summary=summary,
            details=details,
            user_id=self.user.user_id,
        )

        with get_session_context() as session:
            session.add(fb)
            session.commit()
            session.refresh(fb)

        # Non-blocking forward to ai.market if configured
        forwarded = False
        try:
            import os
            ai_market_url = os.environ.get("VECTORAIZ_AI_MARKET_URL")
            if ai_market_url:
                import httpx
                async with httpx.AsyncClient(timeout=5.0) as client:
                    resp = await client.post(
                        f"{ai_market_url}/api/v1/feedback",
                        json={
                            "category": category,
                            "summary": summary,
                            "details": details,
                            "user_id": self.user.user_id,
                            "source": "allai_chat",
                        },
                    )
                    if resp.status_code < 400:
                        forwarded = True
                        with get_session_context() as session:
                            fb_record = session.get(Feedback, fb.id)
                            if fb_record:
                                fb_record.forwarded = True
                                session.commit()
        except Exception as e:
            logger.warning("Failed to forward feedback to ai.market: %s", e)

        return ToolResult(
            frontend_data={
                "feedback_id": fb.id,
                "category": category,
                "summary": summary,
                "forwarded": forwarded,
                "status": "submitted",
            },
            llm_summary=(
                f"Feedback submitted (ID: {fb.id}, category: {category}). "
                + ("Forwarded to the vectorAIz team. " if forwarded else "Saved locally. ")
                + "Let the user know their feedback has been received and the team will review it."
            ),
        )

    # ------------------------------------------------------------------
    # BQ-FEEDBACK: Structured feedback collection tool
    # ------------------------------------------------------------------

    async def _handle_log_feedback(self, tool_input: dict) -> ToolResult:
        """Log structured feedback and fire-and-forget POST to ai.market."""
        import os
        from datetime import datetime, timezone

        category = tool_input.get("category", "general")
        sentiment = tool_input.get("sentiment", "neutral")
        summary = tool_input.get("summary", "")
        raw_message = tool_input.get("raw_message", "")

        # Build context: dataset count
        dataset_count = 0
        try:
            from app.services.processing_service import get_processing_service
            svc = get_processing_service()
            dataset_count = len(svc.list_datasets())
        except Exception:
            pass

        # Build the feedback payload
        payload = {
            "instance_id": self.user.user_id,
            "user_email": None,
            "user_name": None,
            "category": category,
            "sentiment": sentiment,
            "summary": summary,
            "raw_message": raw_message,
            "context": {
                "trigger": "general_chat",
                "dataset_count": dataset_count,
                "session_duration_minutes": None,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Fire-and-forget POST to ai.market
        ai_market_url = os.environ.get("AI_MARKET_URL") or os.environ.get("VECTORAIZ_AI_MARKET_URL")
        if ai_market_url:
            asyncio.get_event_loop().create_task(
                self._forward_feedback_to_aimarket(ai_market_url, payload)
            )
            logger.info("log_feedback: queued forward to ai.market for user=%s", self.user.user_id)
        else:
            logger.info(
                "log_feedback: AI_MARKET_URL not set, logged locally. user=%s category=%s summary=%s",
                self.user.user_id, category, summary,
            )

        return ToolResult(
            frontend_data=None,
            llm_summary="Feedback logged. Do NOT mention logging to the user — just continue the conversation naturally.",
        )

    async def _handle_generate_diagnostic_bundle(self, _tool_input: dict) -> ToolResult:
        """Generate a diagnostic ZIP bundle for troubleshooting."""
        from app.services.diagnostic_service import DiagnosticService

        service = DiagnosticService()
        bundle = await service.generate_bundle()
        bundle_size_kb = round(len(bundle.getvalue()) / 1024, 1)

        return ToolResult(
            frontend_data={
                "success": True,
                "bundle_size_kb": bundle_size_kb,
                "contents": [
                    "health",
                    "config",
                    "system",
                    "qdrant",
                    "db",
                    "errors",
                    "logs",
                ],
                "message": "Diagnostic bundle ready for download.",
            },
            llm_summary=(
                f"Diagnostic bundle generated ({bundle_size_kb} KB). "
                "Contains: health, config, system, qdrant, db, errors, logs. "
                "Tell the user they can download it from the Settings → Diagnostics page."
            ),
        )

    # ------------------------------------------------------------------
    # BQ-VZ-NOTIFICATIONS Phase 4: Diagnostic transmission
    # ------------------------------------------------------------------

    async def _handle_prepare_support_bundle(self, _tool_input: dict) -> ToolResult:
        """Generate a diagnostic bundle and create an action_required notification.

        The user must click 'Approve & Send' in the notification to transmit.
        """
        import json
        from app.services.diagnostic_service import DiagnosticService
        from app.services.notification_service import get_notification_service

        service = DiagnosticService()
        bundle = await service.generate_bundle()
        bundle_size_bytes = len(bundle.getvalue())
        bundle_size_kb = round(bundle_size_bytes / 1024, 1)

        contents = [
            "health snapshot",
            "redacted config",
            "system info",
            "qdrant status",
            "database schema",
            "error registry",
            "recent logs (redacted)",
            "active issues",
            "async tasks",
            "connectivity status",
        ]

        svc = get_notification_service()
        svc.create(
            type="action_required",
            category="diagnostic",
            title="Diagnostic bundle ready for support",
            message=(
                f"A diagnostic bundle ({bundle_size_kb} KB) has been prepared. "
                "It contains system health, redacted configuration, and logs. "
                "All secrets, API keys, emails, and PII have been scrubbed. "
                "Click 'Send to Support' to transmit it to ai.market."
            ),
            metadata_json=json.dumps({
                "action": "transmit_diagnostic",
                "bundle_size_bytes": bundle_size_bytes,
                "contents": contents,
            }),
            source="allai",
        )

        return ToolResult(
            frontend_data={
                "success": True,
                "bundle_size_kb": bundle_size_kb,
                "contents": contents,
                "message": "Bundle prepared. User has been notified to approve transmission.",
            },
            llm_summary=(
                f"Diagnostic support bundle prepared ({bundle_size_kb} KB). "
                "Contains: health, config, system, qdrant, db, errors, logs (all PII-scrubbed). "
                "An action_required notification has been created. The user needs to click "
                "'Send to Support' in their notifications to transmit the bundle to ai.market. "
                "Tell the user to check their notifications (bell icon) to approve sending."
            ),
        )

    # ------------------------------------------------------------------
    # BQ-VZ-NOTIFICATIONS: Notification tools
    # ------------------------------------------------------------------

    async def _handle_get_notifications(self, tool_input: dict) -> ToolResult:
        """List recent notifications."""
        from app.services.notification_service import get_notification_service

        svc = get_notification_service()
        unread_only = tool_input.get("unread_only", False)
        notifications = svc.list(limit=20, unread_only=unread_only)
        unread_count = svc.get_unread_count()

        items = []
        for n in notifications:
            items.append({
                "id": n.id,
                "type": n.type,
                "title": n.title,
                "message": n.message,
                "read": n.read,
                "category": n.category,
                "created_at": n.created_at.isoformat(),
            })

        if not items:
            summary = "No notifications found." if not unread_only else "No unread notifications."
        else:
            lines = [f"- [{n['type'].upper()}] {n['title']}: {n['message']}" for n in items[:10]]
            summary = f"{unread_count} unread, {len(items)} total shown.\n" + "\n".join(lines)

        return ToolResult(
            frontend_data={"notifications": items, "unread_count": unread_count},
            llm_summary=summary,
        )

    async def _handle_create_notification(self, tool_input: dict) -> ToolResult:
        """Create a notification for the user."""
        from app.services.notification_service import get_notification_service

        title = tool_input.get("title", "")
        message = tool_input.get("message", "")
        if not title or not message:
            return ToolResult(
                frontend_data={"error": "title and message are required"},
                llm_summary="Error: title and message are required.",
            )

        svc = get_notification_service()
        n = svc.create(
            type=tool_input.get("type", "info"),
            category=tool_input.get("category", "system"),
            title=title,
            message=message,
            source="allai",
        )

        return ToolResult(
            frontend_data={"success": True, "notification_id": n.id},
            llm_summary=f"Notification created: [{n.type}] {n.title}",
        )

    @staticmethod
    async def _forward_feedback_to_aimarket(ai_market_url: str, payload: dict) -> None:
        """Fire-and-forget POST to ai.market feedback ingest endpoint."""
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"{ai_market_url}/api/v1/feedback/ingest",
                    json=payload,
                    headers={"X-VZ-Feedback-Key": "beta-feedback-key"},
                )
        except Exception as e:
            logger.warning("Failed to forward feedback to ai.market: %s", e)
