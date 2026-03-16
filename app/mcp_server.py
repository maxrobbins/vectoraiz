"""
MCP Server — Standalone stdio server for external LLM connectivity.

Invocation:
    docker exec -i vectoraiz python -m app.mcp_server --token vzmcp_...

Uses FastMCP from the `mcp` SDK. 6 tools delegate to QueryOrchestrator.

Phase: BQ-MCP-RAG — Universal LLM Connectivity
Created: S136
"""

import argparse
import os
os.environ["TQDM_DISABLE"] = "1"  # Suppress progress bars on stdio
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
import asyncio
import json
import logging
import sys
from typing import Any, Dict, Optional

try:
    from mcp.server.fastmcp import FastMCP
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False

from app.models.connectivity import (
    DatasetIdInput,
    VectorSearchRequest,
    SQLQueryRequest,
    validate_dataset_id,
)
from app.services.connectivity_token_service import ConnectivityTokenError
from app.services.query_orchestrator import ConnectivityError, QueryOrchestrator

logger = logging.getLogger(__name__)

# Global state set during startup
_token_raw: str = ""
_orchestrator: Optional[QueryOrchestrator] = None

# Create MCP server only if SDK available
mcp_server = None
if MCP_AVAILABLE:
    mcp_server = FastMCP(name="vectoraiz", json_response=True)


def _noop_decorator(*args, **kwargs):
    """No-op decorator when MCP is not available."""
    def wrapper(fn):
        return fn
    if args and callable(args[0]):
        return args[0]
    return wrapper


def _tool():
    """Return the mcp_server.tool() decorator or a no-op if MCP unavailable."""
    if mcp_server is not None:
        return mcp_server.tool()
    return _noop_decorator


def _format_error(code: str, message: str, details: Optional[Dict[str, Any]] = None) -> str:
    """Format a structured error as JSON string for MCP isError responses."""
    return json.dumps({
        "error": {
            "code": code,
            "message": message,
            "details": details or {},
        },
    })


def _get_orchestrator() -> QueryOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = QueryOrchestrator()
    return _orchestrator


def _validate_token():
    """Validate the global token. Raises on failure."""
    orch = _get_orchestrator()
    return orch.validate_token(_token_raw)


def _validate_dataset_id(dataset_id: str) -> str:
    """Validate dataset_id via Pydantic model. Raises ConnectivityError on failure."""
    try:
        validated = DatasetIdInput(dataset_id=dataset_id)
        return validated.dataset_id
    except Exception:
        raise ConnectivityError(
            "invalid_input",
            "Invalid dataset_id: must be alphanumeric/dash/underscore, max 64 chars",
        )


def _try_meter(tool_name: str) -> None:
    """Best-effort metering for MCP tool calls (connected mode only)."""
    try:
        from app.config import settings
        if settings.mode == "standalone":
            return
        from app.services.serial_store import get_serial_store, ACTIVE, MIGRATED
        store = get_serial_store()
        state = store.state
        if state.state not in (ACTIVE, MIGRATED):
            return
        from app.services.serial_metering import (
            SerialMeteringStrategy,
            LedgerMeteringStrategy,
            DEFAULT_SETUP_COST,
        )
        import time, hashlib
        if state.state == MIGRATED:
            strategy = LedgerMeteringStrategy()
        else:
            strategy = SerialMeteringStrategy(store)
        serial_short = state.serial[3:11] if state.serial.startswith("VZ-") else state.serial[:8]
        endpoint_hash = hashlib.md5(f"MCP:{tool_name}".encode()).hexdigest()[:8]
        ts_ms = int(time.time() * 1000)
        request_id = f"vz:{serial_short}:{endpoint_hash}:{ts_ms}"
        asyncio.get_event_loop().create_task(
            strategy.check_and_meter("setup", DEFAULT_SETUP_COST, request_id)
        )
    except Exception:
        pass  # Metering is best-effort; never block tool execution


@_tool()
async def vectoraiz_list_datasets() -> str:
    """List all externally-queryable datasets in vectorAIz with metadata including name, type, row count, and whether vectors are available."""
    try:
        _try_meter("list_datasets")
        token = _validate_token()
        orch = _get_orchestrator()
        result = await orch.list_datasets(token)
        return result.model_dump_json()
    except ConnectivityError as e:
        raise ValueError(_format_error(e.code, e.message, e.details))
    except Exception as e:
        logger.exception("Unexpected error in vectoraiz_list_datasets")
        raise ValueError(_format_error("internal_error", "An internal error occurred. Check vectorAIz logs for details."))


@_tool()
async def vectoraiz_get_schema(dataset_id: str) -> str:
    """Get column definitions for a specific dataset. Returns column names, types, nullable status, and sample values. Use dataset IDs from vectoraiz_list_datasets."""
    try:
        _try_meter("get_schema")
        token = _validate_token()
        dataset_id = _validate_dataset_id(dataset_id)
        orch = _get_orchestrator()
        result = await orch.get_schema(token, dataset_id)
        return result.model_dump_json()
    except ConnectivityError as e:
        raise ValueError(_format_error(e.code, e.message, e.details))
    except Exception as e:
        logger.exception("Unexpected error in vectoraiz_get_schema")
        raise ValueError(_format_error("internal_error", "An internal error occurred. Check vectorAIz logs for details."))


@_tool()
async def vectoraiz_search(query: str, dataset_id: str = "", top_k: int = 5) -> str:
    """Semantic vector search across indexed documents and data chunks. Use natural language queries. Optionally limit to a specific dataset."""
    try:
        _try_meter("search_vectors")
        token = _validate_token()
        # Validate dataset_id if provided
        validated_id = None
        if dataset_id:
            validated_id = _validate_dataset_id(dataset_id)
        orch = _get_orchestrator()
        req = VectorSearchRequest(
            query=query,
            dataset_id=validated_id,
            top_k=max(1, min(top_k, 20)),
        )
        result = await orch.search_vectors(token, req)
        return result.model_dump_json()
    except ConnectivityError as e:
        raise ValueError(_format_error(e.code, e.message, e.details))
    except Exception as e:
        logger.exception("Unexpected error in vectoraiz_search")
        raise ValueError(_format_error("internal_error", "An internal error occurred. Check vectorAIz logs for details."))


@_tool()
async def vectoraiz_sql(sql: str, dataset_id: str = "") -> str:
    """Execute a read-only SQL SELECT query against structured data. Tables are named dataset_{id}. Only SELECT queries are allowed. Use vectoraiz_get_schema to discover column names first."""
    try:
        _try_meter("execute_sql")
        token = _validate_token()
        # Validate dataset_id if provided
        validated_id = None
        if dataset_id:
            validated_id = _validate_dataset_id(dataset_id)
        orch = _get_orchestrator()
        req = SQLQueryRequest(
            sql=sql,
            dataset_id=validated_id,
        )
        result = await orch.execute_sql(token, req)
        return result.model_dump_json()
    except ConnectivityError as e:
        raise ValueError(_format_error(e.code, e.message, e.details))
    except Exception as e:
        logger.exception("Unexpected error in vectoraiz_sql")
        raise ValueError(_format_error("internal_error", "An internal error occurred. Check vectorAIz logs for details."))


@_tool()
async def vectoraiz_profile_dataset(dataset_id: str) -> str:
    """Profile a dataset: get row count, column count, column types, null rates, and 5 sample rows. Use dataset IDs from vectoraiz_list_datasets."""
    try:
        _try_meter("profile_dataset")
        token = _validate_token()
        dataset_id = _validate_dataset_id(dataset_id)
        orch = _get_orchestrator()
        result = await orch.profile_dataset(token, dataset_id)
        return result.model_dump_json()
    except ConnectivityError as e:
        raise ValueError(_format_error(e.code, e.message, e.details))
    except Exception as e:
        logger.exception("Unexpected error in vectoraiz_profile_dataset")
        raise ValueError(_format_error("internal_error", "An internal error occurred. Check vectorAIz logs for details."))


@_tool()
async def vectoraiz_get_pii_report(dataset_id: str) -> str:
    """Get cached PII (Personally Identifiable Information) scan results for a dataset. Returns the PII report from the last pipeline run. Does not trigger a new scan."""
    try:
        _try_meter("get_pii_report")
        token = _validate_token()
        dataset_id = _validate_dataset_id(dataset_id)
        orch = _get_orchestrator()
        result = await orch.get_pii_report(token, dataset_id)
        return result.model_dump_json()
    except ConnectivityError as e:
        raise ValueError(_format_error(e.code, e.message, e.details))
    except Exception as e:
        logger.exception("Unexpected error in vectoraiz_get_pii_report")
        raise ValueError(_format_error("internal_error", "An internal error occurred. Check vectorAIz logs for details."))


def main():
    # Force all logging to stderr so stdout stays clean for JSON-RPC
    logging.basicConfig(stream=sys.stderr, level=logging.WARNING)
    global _token_raw

    if not MCP_AVAILABLE:
        print("Error: MCP SDK not installed. Run: pip install 'mcp>=1.8.0,<1.9'", file=sys.stderr)
        sys.exit(1)

    parser = argparse.ArgumentParser(description="vectorAIz MCP Server")
    parser.add_argument("--token", required=True, help="Connectivity token (vzmcp_...)")
    args = parser.parse_args()

    _token_raw = args.token

    # Validate token before starting
    try:
        _validate_token()
    except (ConnectivityError, ConnectivityTokenError) as e:
        print(f"Token validation failed: {e}", file=sys.stderr)
        sys.exit(1)

    mcp_server.run(transport="stdio")


if __name__ == "__main__":
    main()
