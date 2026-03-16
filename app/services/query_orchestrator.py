"""
Query Orchestrator — Central gateway for all external LLM connectivity.

Both MCP tools and REST endpoints call into this. Single enforcement point
for auth, rate limits, sandboxing, and audit logging (§3.2).

Phase: BQ-MCP-RAG — Universal LLM Connectivity
Created: S136
"""

import logging
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb

from app.config import settings
from app.models.connectivity import (
    ColumnInfo,
    ConnectivityErrorDetail,
    ConnectivityErrorResponse,
    ConnectivityToken,
    DatasetInfo,
    DatasetListResponse,
    HealthResponse,
    PIIReportResponse,
    ProfileColumnInfo,
    ProfileResponse,
    SchemaResponse,
    SearchMatch,
    SearchResponse,
    SQLLimits,
    SQLQueryRequest,
    SQLResponse,
    VectorSearchRequest,
)
from app.services.connectivity_audit import audit_log
from app.services.connectivity_metrics import get_connectivity_metrics
from app.services.connectivity_rate_limiter import ConnectivityRateLimiter
from app.services.connectivity_token_service import (
    ConnectivityTokenError,
    verify_token,
)

logger = logging.getLogger(__name__)


class ConnectivityError(Exception):
    """Raised when an external query fails with a structured error."""

    def __init__(self, code: str, message: str, details: Optional[Dict[str, Any]] = None):
        self.code = code
        self.message = message
        self.details = details or {}
        super().__init__(message)


class QueryOrchestrator:
    """Central gateway for all external LLM connectivity (§3.2)."""

    def __init__(self):
        self.rate_limiter = ConnectivityRateLimiter(
            per_token_rpm=settings.connectivity_rate_limit_rpm,
            per_ip_auth_fail_limit=settings.connectivity_rate_limit_auth_fail,
            global_rpm=settings.connectivity_rate_limit_global_rpm,
            per_tool_sql_rpm=settings.connectivity_rate_limit_sql_rpm,
            max_concurrent_per_token=settings.connectivity_max_concurrent,
        )
        self.metrics = get_connectivity_metrics()

    def _check_enabled(self) -> None:
        """Raise if connectivity is disabled."""
        if not settings.connectivity_enabled:
            raise ConnectivityError("service_unavailable", "External connectivity is disabled")

    def validate_token(self, raw_token: str) -> ConnectivityToken:
        """Parse, HMAC verify, check revoked/expired/scope."""
        try:
            return verify_token(raw_token)
        except ConnectivityTokenError as e:
            raise ConnectivityError(e.code, e.message)

    def _enforce_scope(self, token: ConnectivityToken, required_scope: str) -> None:
        """Raise if token lacks the required scope."""
        if required_scope not in token.scopes:
            raise ConnectivityError(
                "scope_denied",
                f"Token lacks required scope: {required_scope}",
            )

    def _enforce_rate_limit(
        self, token: ConnectivityToken, tool_name: str, client_ip: str
    ) -> None:
        """Check all rate limit layers."""
        result = self.rate_limiter.check_rate_limits(token.id, tool_name, client_ip)
        if result:
            self.metrics.record_error(result)
            raise ConnectivityError(result, "Rate limit exceeded" if result == "rate_limited" else "IP blocked due to repeated auth failures")

    def _make_request_id(self) -> str:
        return f"ext-{uuid.uuid4().hex[:12]}"

    # ------------------------------------------------------------------
    # Tool: list_datasets
    # ------------------------------------------------------------------

    async def list_datasets(
        self, token: ConnectivityToken, client_ip: str = "127.0.0.1"
    ) -> DatasetListResponse:
        """List externally-queryable datasets (§5.2)."""
        self._check_enabled()
        self._enforce_scope(token, "ext:datasets")
        self._enforce_rate_limit(token, "list_datasets", client_ip)

        request_id = self._make_request_id()
        start = time.time()

        try:
            self.rate_limiter.record_request(token.id, "list_datasets")

            from app.services.processing_service import get_processing_service, ProcessingStatus
            from app.core.database import get_session_context
            from app.models.dataset import DatasetRecord as DBDatasetRecord
            from sqlmodel import select

            processing = get_processing_service()

            # Query only externally_queryable datasets
            with get_session_context() as session:
                stmt = select(DBDatasetRecord).where(
                    DBDatasetRecord.externally_queryable == True,  # noqa: E712
                    DBDatasetRecord.status == "ready",
                )
                db_records = session.exec(stmt).all()

            # Check which have vectors
            from app.services.qdrant_service import get_qdrant_service
            qdrant = get_qdrant_service()
            collections = {c["name"] for c in qdrant.list_collections()}

            datasets = []
            for rec in db_records:
                import json as _json
                try:
                    meta = _json.loads(rec.metadata_json) if rec.metadata_json else {}
                except (ValueError, TypeError):
                    meta = {}

                collection_name = f"dataset_{rec.id}"
                datasets.append(DatasetInfo(
                    id=rec.id,
                    name=rec.original_filename,
                    description=meta.get("description"),
                    type=rec.file_type,
                    row_count=meta.get("row_count", 0),
                    column_count=meta.get("column_count", 0),
                    created_at=rec.created_at.isoformat() if rec.created_at else "",
                    has_vectors=collection_name in collections,
                ))

            duration_ms = int((time.time() - start) * 1000)
            self.metrics.record_request("list_datasets", duration_ms)
            audit_log(
                tool_name="list_datasets",
                token_id=token.id,
                dataset_id=None,
                duration_ms=duration_ms,
                row_count=len(datasets),
                error_code=None,
                request_id=request_id,
            )

            return DatasetListResponse(datasets=datasets, count=len(datasets))

        except ConnectivityError:
            raise
        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            self.metrics.record_error("internal_error")
            audit_log(
                tool_name="list_datasets", token_id=token.id, dataset_id=None,
                duration_ms=duration_ms, row_count=None, error_code="internal_error",
                request_id=request_id,
            )
            logger.exception("list_datasets failed: %s", e)
            raise ConnectivityError("internal_error", "Internal error listing datasets")

    # ------------------------------------------------------------------
    # Tool: get_schema
    # ------------------------------------------------------------------

    async def get_schema(
        self, token: ConnectivityToken, dataset_id: str, client_ip: str = "127.0.0.1"
    ) -> SchemaResponse:
        """Get column definitions for a dataset (§5.2)."""
        self._check_enabled()
        self._enforce_scope(token, "ext:schema")
        self._enforce_rate_limit(token, "get_schema", client_ip)

        request_id = self._make_request_id()
        start = time.time()

        try:
            self.rate_limiter.record_request(token.id, "get_schema")

            record = self._get_queryable_dataset(dataset_id)

            import json as _json
            meta = _json.loads(record.metadata_json) if record.metadata_json else {}

            # Get detailed column info from DuckDB
            from app.services.duckdb_service import ephemeral_duckdb_service
            processed_path = Path(record.processed_path) if record.processed_path else None

            columns = []
            if processed_path and processed_path.exists():
                try:
                    with ephemeral_duckdb_service() as duckdb_svc:
                        profiles = duckdb_svc.get_column_profile(processed_path, max_rows=100)
                    for p in profiles:
                        columns.append(ColumnInfo(
                            name=p["name"],
                            type=p["type"],
                            nullable=p["null_count"] > 0,
                            sample_values=[str(v) for v in p.get("sample_values", [])[:3]],
                        ))
                except Exception:
                    # Fallback to metadata columns
                    for col in meta.get("columns", []):
                        columns.append(ColumnInfo(
                            name=col["name"],
                            type=col.get("type", "VARCHAR"),
                            nullable=col.get("nullable", True),
                        ))
            else:
                for col in meta.get("columns", []):
                    columns.append(ColumnInfo(
                        name=col["name"],
                        type=col.get("type", "VARCHAR"),
                        nullable=col.get("nullable", True),
                    ))

            table_name = f"dataset_{dataset_id}"
            row_count = meta.get("row_count", 0)

            duration_ms = int((time.time() - start) * 1000)
            self.metrics.record_request("get_schema", duration_ms)
            audit_log(
                tool_name="get_schema", token_id=token.id, dataset_id=dataset_id,
                duration_ms=duration_ms, row_count=None, error_code=None,
                request_id=request_id,
            )

            return SchemaResponse(
                dataset_id=dataset_id,
                table_name=table_name,
                row_count=row_count,
                columns=columns,
            )

        except ConnectivityError:
            raise
        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            self.metrics.record_error("internal_error")
            audit_log(
                tool_name="get_schema", token_id=token.id, dataset_id=dataset_id,
                duration_ms=duration_ms, row_count=None, error_code="internal_error",
                request_id=request_id,
            )
            logger.exception("get_schema failed: %s", e)
            raise ConnectivityError("internal_error", "Internal error getting schema")

    # ------------------------------------------------------------------
    # Tool: search_vectors
    # ------------------------------------------------------------------

    async def search_vectors(
        self, token: ConnectivityToken, req: VectorSearchRequest, client_ip: str = "127.0.0.1"
    ) -> SearchResponse:
        """Semantic vector search (§5.2)."""
        self._check_enabled()
        self._enforce_scope(token, "ext:search")
        self._enforce_rate_limit(token, "search_vectors", client_ip)

        request_id = self._make_request_id()
        start = time.time()

        try:
            self.rate_limiter.record_request(token.id, "search_vectors")

            # If dataset_id specified, verify it's queryable
            if req.dataset_id:
                self._get_queryable_dataset(req.dataset_id)

            from app.services.search_service import get_search_service
            search_svc = get_search_service()

            result = search_svc.search(
                query=req.query,
                dataset_id=req.dataset_id,
                limit=req.top_k,
            )

            matches = []
            for r in result.get("results", []):
                text_content = r.get("text_content") or ""
                if len(text_content) > 2000:
                    text_content = text_content[:2000]

                matches.append(SearchMatch(
                    id=str(r.get("row_index", "")),
                    score=r.get("score", 0.0),
                    text=text_content,
                    metadata={
                        "source_file": r.get("dataset_name", ""),
                        "dataset_id": r.get("dataset_id", ""),
                        "dataset_name": r.get("dataset_name", ""),
                    },
                ))

            truncated = len(matches) >= req.top_k

            duration_ms = int((time.time() - start) * 1000)
            self.metrics.record_request("search_vectors", duration_ms)
            audit_log(
                tool_name="search_vectors", token_id=token.id,
                dataset_id=req.dataset_id, duration_ms=duration_ms,
                row_count=len(matches), error_code=None, request_id=request_id,
            )

            return SearchResponse(
                matches=matches,
                count=len(matches),
                truncated=truncated,
                request_id=request_id,
            )

        except ConnectivityError:
            raise
        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            self.metrics.record_error("internal_error")
            audit_log(
                tool_name="search_vectors", token_id=token.id,
                dataset_id=req.dataset_id, duration_ms=duration_ms,
                row_count=None, error_code="internal_error", request_id=request_id,
            )
            logger.exception("search_vectors failed: %s", e)
            raise ConnectivityError("internal_error", "Internal error during search")

    # ------------------------------------------------------------------
    # Tool: execute_sql
    # ------------------------------------------------------------------

    async def execute_sql(
        self, token: ConnectivityToken, req: SQLQueryRequest, client_ip: str = "127.0.0.1"
    ) -> SQLResponse:
        """Execute read-only SQL (§5.2)."""
        self._check_enabled()
        self._enforce_scope(token, "ext:sql")
        self._enforce_rate_limit(token, "execute_sql", client_ip)

        request_id = self._make_request_id()
        start = time.time()

        # Concurrency control
        if not self.rate_limiter.acquire_concurrency(token.id):
            raise ConnectivityError("rate_limited", "Too many concurrent queries")

        try:
            self.rate_limiter.record_request(token.id, "execute_sql")

            max_length = settings.connectivity_sql_max_length
            max_rows = settings.connectivity_sql_max_rows
            timeout_s = settings.connectivity_sql_timeout_s
            memory_mb = settings.connectivity_sql_memory_mb

            # Validate SQL length (M28)
            if len(req.sql) > max_length:
                raise ConnectivityError(
                    "sql_too_long",
                    f"SQL exceeds maximum length of {max_length} characters",
                )

            # Get queryable dataset IDs for sandbox
            queryable_ids = self._get_all_queryable_dataset_ids()
            if req.dataset_id:
                self._get_queryable_dataset(req.dataset_id)
                queryable_ids = [req.dataset_id]

            # Validate through enhanced SQL sandbox (external mode)
            from app.services.sql_sandbox import SQLSandbox
            allowed_tables = SQLSandbox.build_allowed_tables(queryable_ids)
            sandbox = SQLSandbox(allowed_tables)

            # External-mode validation
            is_valid, error = sandbox.validate_external(req.sql)
            if not is_valid:
                raise ConnectivityError("forbidden_sql", error)

            # Wrap with enforced LIMIT (M27)
            clean_sql = req.sql.strip().rstrip(";").strip()
            wrapped_sql = f"SELECT * FROM ({clean_sql}) AS __ext_q LIMIT {max_rows}"

            # Execute on ephemeral connection with tighter limits (M30)
            from app.services.duckdb_service import ephemeral_duckdb_service
            with ephemeral_duckdb_service() as duckdb_svc:
                conn = duckdb_svc.create_ephemeral_connection(
                    memory_limit=f"{memory_mb}MB",
                    threads=2,
                )

            try:
                # Create views for queryable datasets
                from app.services.processing_service import get_processing_service, ProcessingStatus
                processing = get_processing_service()
                from app.utils.sanitization import sql_quote_literal

                for ds_id in queryable_ids:
                    record = processing.get_dataset(ds_id)
                    if record and record.status == ProcessingStatus.READY and record.processed_path:
                        escaped = sql_quote_literal(str(record.processed_path))
                        conn.execute(
                            f"CREATE OR REPLACE VIEW dataset_{ds_id} "
                            f"AS SELECT * FROM read_parquet('{escaped}')"
                        )

                # Execute with timeout
                import asyncio
                result = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, lambda: conn.execute(wrapped_sql)
                    ),
                    timeout=timeout_s,
                )

                rows_raw = result.fetchall()
                columns = [desc[0] for desc in result.description]

                # Serialize values
                rows = []
                for row in rows_raw:
                    serialized = []
                    for val in row:
                        if val is None:
                            serialized.append(None)
                        elif isinstance(val, (int, float, bool, str)):
                            serialized.append(val)
                        else:
                            serialized.append(str(val))
                    rows.append(serialized)

                truncated = len(rows) >= max_rows

            except asyncio.TimeoutError:
                raise ConnectivityError("query_timeout", f"Query exceeded {timeout_s}s timeout")
            except duckdb.Error as e:
                error_msg = str(e)
                # Redact internal paths
                if settings.processed_directory in error_msg:
                    error_msg = error_msg.replace(settings.processed_directory, "[data]")
                raise ConnectivityError("forbidden_sql", f"Query failed: {error_msg}")
            finally:
                conn.close()

            duration_ms = int((time.time() - start) * 1000)
            self.metrics.record_request("execute_sql", duration_ms)
            audit_log(
                tool_name="execute_sql", token_id=token.id,
                dataset_id=req.dataset_id, duration_ms=duration_ms,
                row_count=len(rows), error_code=None, request_id=request_id,
                sql=req.sql,
            )

            return SQLResponse(
                columns=columns,
                rows=rows,
                row_count=len(rows),
                truncated=truncated,
                execution_ms=duration_ms,
                limits_applied=SQLLimits(
                    max_rows=max_rows,
                    max_runtime_ms=timeout_s * 1000,
                    max_memory_mb=memory_mb,
                ),
                request_id=request_id,
            )

        except ConnectivityError:
            raise
        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            self.metrics.record_error("internal_error")
            audit_log(
                tool_name="execute_sql", token_id=token.id,
                dataset_id=req.dataset_id, duration_ms=duration_ms,
                row_count=None, error_code="internal_error", request_id=request_id,
                sql=req.sql,
            )
            logger.exception("execute_sql failed: %s", e)
            raise ConnectivityError("internal_error", "Internal error executing SQL")
        finally:
            self.rate_limiter.release_concurrency(token.id)

    # ------------------------------------------------------------------
    # Tool: profile_dataset
    # ------------------------------------------------------------------

    async def profile_dataset(
        self, token: ConnectivityToken, dataset_id: str, client_ip: str = "127.0.0.1"
    ) -> ProfileResponse:
        """Profile a dataset: row count, column types, null rates, sample rows."""
        self._check_enabled()
        self._enforce_scope(token, "ext:profile")
        self._enforce_rate_limit(token, "profile_dataset", client_ip)

        request_id = self._make_request_id()
        start = time.time()

        try:
            self.rate_limiter.record_request(token.id, "profile_dataset")

            record = self._get_queryable_dataset(dataset_id)

            import json as _json
            meta = _json.loads(record.metadata_json) if record.metadata_json else {}

            from app.services.duckdb_service import ephemeral_duckdb_service
            processed_path = Path(record.processed_path) if record.processed_path else None

            columns = []
            sample_rows = []
            row_count = meta.get("row_count", 0)

            if processed_path and processed_path.exists():
                with ephemeral_duckdb_service() as duckdb_svc:
                    # Get column profiles
                    profiles = duckdb_svc.get_column_profile(processed_path, max_rows=100)
                    for p in profiles:
                        null_count = p.get("null_count", 0)
                        total = p.get("total_count", row_count) or 1
                        columns.append(ProfileColumnInfo(
                            name=p["name"],
                            type=p["type"],
                            null_count=null_count,
                            null_rate=round(null_count / total, 4) if total > 0 else 0.0,
                            sample_values=[str(v) for v in p.get("sample_values", [])[:5]],
                        ))

                    # Get 5 sample rows
                    try:
                        from app.utils.sanitization import sql_quote_literal
                        escaped = sql_quote_literal(str(processed_path))
                        conn = duckdb_svc.create_ephemeral_connection(
                            memory_limit="128MB", threads=1,
                        )
                        try:
                            result = conn.execute(
                                f"SELECT * FROM read_parquet('{escaped}') LIMIT 5"
                            )
                            raw_rows = result.fetchall()
                            for row in raw_rows:
                                sample_rows.append([
                                    v if isinstance(v, (int, float, bool, str, type(None)))
                                    else str(v) for v in row
                                ])
                        finally:
                            conn.close()
                    except Exception:
                        pass  # sample rows are best-effort

            duration_ms = int((time.time() - start) * 1000)
            self.metrics.record_request("profile_dataset", duration_ms)
            audit_log(
                tool_name="profile_dataset", token_id=token.id,
                dataset_id=dataset_id, duration_ms=duration_ms,
                row_count=row_count, error_code=None, request_id=request_id,
            )

            return ProfileResponse(
                dataset_id=dataset_id,
                row_count=row_count,
                column_count=len(columns),
                columns=columns,
                sample_rows=sample_rows,
            )

        except ConnectivityError:
            raise
        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            self.metrics.record_error("internal_error")
            audit_log(
                tool_name="profile_dataset", token_id=token.id,
                dataset_id=dataset_id, duration_ms=duration_ms,
                row_count=None, error_code="internal_error", request_id=request_id,
            )
            logger.exception("profile_dataset failed: %s", e)
            raise ConnectivityError("internal_error", "Internal error profiling dataset")

    # ------------------------------------------------------------------
    # Tool: get_pii_report
    # ------------------------------------------------------------------

    async def get_pii_report(
        self, token: ConnectivityToken, dataset_id: str, client_ip: str = "127.0.0.1"
    ) -> PIIReportResponse:
        """Get cached PII scan results for a dataset (read-only)."""
        self._check_enabled()
        self._enforce_scope(token, "ext:pii")
        self._enforce_rate_limit(token, "get_pii_report", client_ip)

        request_id = self._make_request_id()
        start = time.time()

        try:
            self.rate_limiter.record_request(token.id, "get_pii_report")

            # Validate dataset exists and is queryable (prevents path traversal)
            record = self._get_queryable_dataset(dataset_id)

            # Read cached PII scan from the known processed directory
            import json as _json
            pii_path = Path(settings.processed_directory) / dataset_id / "pii_scan.json"

            if not pii_path.exists():
                duration_ms = int((time.time() - start) * 1000)
                self.metrics.record_request("get_pii_report", duration_ms)
                audit_log(
                    tool_name="get_pii_report", token_id=token.id,
                    dataset_id=dataset_id, duration_ms=duration_ms,
                    row_count=None, error_code=None, request_id=request_id,
                )
                return PIIReportResponse(
                    dataset_id=dataset_id,
                    status="not_available",
                    message="Run dataset processing first",
                )

            pii_data = _json.loads(pii_path.read_text())

            duration_ms = int((time.time() - start) * 1000)
            self.metrics.record_request("get_pii_report", duration_ms)
            audit_log(
                tool_name="get_pii_report", token_id=token.id,
                dataset_id=dataset_id, duration_ms=duration_ms,
                row_count=None, error_code=None, request_id=request_id,
            )

            return PIIReportResponse(
                dataset_id=dataset_id,
                status="available",
                report=pii_data,
            )

        except ConnectivityError:
            raise
        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            self.metrics.record_error("internal_error")
            audit_log(
                tool_name="get_pii_report", token_id=token.id,
                dataset_id=dataset_id, duration_ms=duration_ms,
                row_count=None, error_code="internal_error", request_id=request_id,
            )
            logger.exception("get_pii_report failed: %s", e)
            raise ConnectivityError("internal_error", "Internal error reading PII report")

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    async def health_check(self) -> HealthResponse:
        """Minimal health check — no auth required, no dataset/token details."""
        return HealthResponse(
            status="ok",
            connectivity_enabled=settings.connectivity_enabled,
            version="1.0",
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_queryable_dataset(self, dataset_id: str):
        """Fetch a dataset record, verifying it exists and is externally_queryable."""
        from app.core.database import get_session_context
        from app.models.dataset import DatasetRecord as DBDatasetRecord

        with get_session_context() as session:
            record = session.get(DBDatasetRecord, dataset_id)
            if record is None:
                raise ConnectivityError("dataset_not_found", f"Dataset '{dataset_id}' not found")
            if not record.externally_queryable:
                raise ConnectivityError("dataset_not_found", f"Dataset '{dataset_id}' is not available for external queries")
            if record.status != "ready":
                raise ConnectivityError("dataset_not_found", f"Dataset '{dataset_id}' is not ready")
            return record

    def _get_all_queryable_dataset_ids(self) -> List[str]:
        """Get IDs of all externally queryable, ready datasets."""
        from app.core.database import get_session_context
        from app.models.dataset import DatasetRecord as DBDatasetRecord
        from sqlmodel import select

        with get_session_context() as session:
            stmt = select(DBDatasetRecord.id).where(
                DBDatasetRecord.externally_queryable == True,  # noqa: E712
                DBDatasetRecord.status == "ready",
            )
            return [row for row in session.exec(stmt).all()]

    @staticmethod
    def format_error(
        code: str, message: str, details: Optional[Dict[str, Any]] = None, request_id: Optional[str] = None
    ) -> ConnectivityErrorResponse:
        """Format a structured error response."""
        return ConnectivityErrorResponse(
            error=ConnectivityErrorDetail(
                code=code,
                message=message,
                details=details or {},
            ),
            request_id=request_id or f"ext-{uuid.uuid4().hex[:12]}",
        )


# Singleton
_orchestrator: Optional[QueryOrchestrator] = None


def get_query_orchestrator() -> QueryOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = QueryOrchestrator()
    return _orchestrator
