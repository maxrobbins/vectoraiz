"""
Database Connectivity Router
=============================

CRUD for database connections, test connectivity, schema introspection,
and table extraction to dataset pipeline.

Phase: BQ-VZ-DB-CONNECT
Created: 2026-02-25
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

from app.config import settings
from app.core.database import get_session_context
from app.models.database_connection import DatabaseConnection
from app.models.dataset import DatasetRecord as DBDatasetRecord, DatasetStatus
from app.services.db_connector import get_db_connector
from app.services.db_credential_service import encrypt_password

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class ConnectionCreate(BaseModel):
    name: str = Field(..., max_length=255)
    db_type: str = Field(..., pattern="^(postgresql|mysql)$")
    host: str = Field(..., max_length=512)
    port: int = Field(..., gt=0, le=65535)
    database: str = Field(..., max_length=255)
    username: str = Field(..., max_length=255)
    password: str = Field(..., min_length=1)
    ssl_mode: str = Field(default="prefer", pattern="^(disable|prefer|require)$")
    extra_options: Optional[str] = None


class ConnectionUpdate(BaseModel):
    name: Optional[str] = Field(default=None, max_length=255)
    host: Optional[str] = Field(default=None, max_length=512)
    port: Optional[int] = Field(default=None, gt=0, le=65535)
    database: Optional[str] = Field(default=None, max_length=255)
    username: Optional[str] = Field(default=None, max_length=255)
    password: Optional[str] = None
    ssl_mode: Optional[str] = Field(default=None, pattern="^(disable|prefer|require)$")
    extra_options: Optional[str] = None


class ConnectionResponse(BaseModel):
    id: str
    name: str
    db_type: str
    host: str
    port: int
    database: str
    username: str
    ssl_mode: str
    extra_options: Optional[str] = None
    status: str
    error_message: Optional[str] = None
    last_connected_at: Optional[str] = None
    last_sync_at: Optional[str] = None
    table_count: Optional[int] = None
    created_at: str
    updated_at: str


class ExtractTableSpec(BaseModel):
    table: str
    schema_name: Optional[str] = Field(default=None, alias="schema")
    row_limit: Optional[int] = None


class ExtractRequest(BaseModel):
    tables: Optional[List[ExtractTableSpec]] = None
    custom_sql: Optional[str] = None
    dataset_name: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _conn_to_response(conn: DatabaseConnection) -> ConnectionResponse:
    """Convert DB model to API response (password masked)."""
    return ConnectionResponse(
        id=conn.id,
        name=conn.name,
        db_type=conn.db_type,
        host=conn.host,
        port=conn.port,
        database=conn.database,
        username=conn.username,
        ssl_mode=conn.ssl_mode,
        extra_options=conn.extra_options,
        status=conn.status,
        error_message=conn.error_message,
        last_connected_at=conn.last_connected_at.isoformat() if conn.last_connected_at else None,
        last_sync_at=conn.last_sync_at.isoformat() if conn.last_sync_at else None,
        table_count=conn.table_count,
        created_at=conn.created_at.isoformat(),
        updated_at=conn.updated_at.isoformat(),
    )


def _get_connection(connection_id: str) -> DatabaseConnection:
    """Fetch a connection by ID or raise 404."""
    with get_session_context() as session:
        conn = session.get(DatabaseConnection, connection_id)
        if not conn:
            raise HTTPException(status_code=404, detail="Connection not found")
        # Detach from session so we can use it outside the context
        session.expunge(conn)
        return conn


# ---------------------------------------------------------------------------
# CRUD endpoints
# ---------------------------------------------------------------------------

@router.post("/connections", status_code=201, summary="Create database connection")
async def create_connection(body: ConnectionCreate) -> ConnectionResponse:
    conn = DatabaseConnection(
        id=str(uuid.uuid4()),
        name=body.name,
        db_type=body.db_type,
        host=body.host,
        port=body.port,
        database=body.database,
        username=body.username,
        password_encrypted=encrypt_password(body.password),
        ssl_mode=body.ssl_mode,
        extra_options=body.extra_options,
    )
    with get_session_context() as session:
        session.add(conn)
        session.commit()
        session.refresh(conn)
        return _conn_to_response(conn)


@router.get("/connections", summary="List all database connections")
async def list_connections() -> List[ConnectionResponse]:
    from sqlmodel import select

    with get_session_context() as session:
        rows = session.exec(select(DatabaseConnection)).all()
        return [_conn_to_response(r) for r in rows]


@router.get("/connections/{connection_id}", summary="Get connection details")
async def get_connection(connection_id: str) -> ConnectionResponse:
    conn = _get_connection(connection_id)
    return _conn_to_response(conn)


@router.put("/connections/{connection_id}", summary="Update connection")
async def update_connection(connection_id: str, body: ConnectionUpdate) -> ConnectionResponse:
    with get_session_context() as session:
        conn = session.get(DatabaseConnection, connection_id)
        if not conn:
            raise HTTPException(status_code=404, detail="Connection not found")

        if body.name is not None:
            conn.name = body.name
        if body.host is not None:
            conn.host = body.host
        if body.port is not None:
            conn.port = body.port
        if body.database is not None:
            conn.database = body.database
        if body.username is not None:
            conn.username = body.username
        if body.password is not None:
            conn.password_encrypted = encrypt_password(body.password)
        if body.ssl_mode is not None:
            conn.ssl_mode = body.ssl_mode
        if body.extra_options is not None:
            conn.extra_options = body.extra_options

        conn.updated_at = datetime.now(timezone.utc)
        # Reset status when config changes
        conn.status = "configured"
        conn.error_message = None

        session.add(conn)
        session.commit()
        session.refresh(conn)

        # Dispose cached engine so next use picks up new config
        get_db_connector().dispose_engine(connection_id)

        return _conn_to_response(conn)


@router.delete("/connections/{connection_id}", status_code=204, summary="Delete connection")
async def delete_connection(connection_id: str):
    with get_session_context() as session:
        conn = session.get(DatabaseConnection, connection_id)
        if not conn:
            raise HTTPException(status_code=404, detail="Connection not found")
        session.delete(conn)
        session.commit()
    get_db_connector().dispose_engine(connection_id)


# ---------------------------------------------------------------------------
# Test connection
# ---------------------------------------------------------------------------

@router.post("/connections/{connection_id}/test", summary="Test database connectivity")
async def test_connection(connection_id: str) -> Dict[str, Any]:
    conn = _get_connection(connection_id)
    connector = get_db_connector()
    result = connector.test_connection(conn)

    # Update connection status in DB
    with get_session_context() as session:
        db_conn = session.get(DatabaseConnection, connection_id)
        if db_conn:
            now = datetime.now(timezone.utc)
            if result["ok"]:
                db_conn.status = "connected"
                db_conn.last_connected_at = now
                db_conn.error_message = None
            else:
                db_conn.status = "error"
                db_conn.error_message = result["error"]
            db_conn.updated_at = now
            session.add(db_conn)
            session.commit()

    return result


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------

@router.get("/connections/{connection_id}/schema", summary="Introspect database schema")
async def introspect_schema(
    connection_id: str,
    schema: Optional[str] = Query(default=None, description="Schema name (default: public for Postgres)"),
) -> Dict[str, Any]:
    conn = _get_connection(connection_id)
    connector = get_db_connector()

    timed_out = False
    try:
        tables = await asyncio.wait_for(
            asyncio.to_thread(connector.introspect_schema, conn, schema),
            timeout=15.0,
        )
    except asyncio.TimeoutError:
        # Timeout: return whatever partial results the connector gathered
        # The connector builds a list incrementally, so re-fetch table names
        # and return them without column details as a fallback.
        timed_out = True
        try:
            tables = await asyncio.wait_for(
                asyncio.to_thread(_partial_introspect, connector, conn, schema),
                timeout=5.0,
            )
        except Exception:
            tables = []
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Schema introspection failed: {e}")

    # Update table_count
    with get_session_context() as session:
        db_conn = session.get(DatabaseConnection, connection_id)
        if db_conn:
            db_conn.table_count = len(tables)
            db_conn.updated_at = datetime.now(timezone.utc)
            session.add(db_conn)
            session.commit()

    result: Dict[str, Any] = {
        "tables": [t.to_dict() for t in tables],
        "partial": timed_out,
    }
    if timed_out:
        result["warning"] = "Schema introspection timed out after 15s. Partial results returned."
    return result


def _partial_introspect(connector, conn, schema):
    """Return table names only (no column detail) as a fast fallback."""
    from app.services.db_connector import TableInfo
    from sqlalchemy import inspect as sa_inspect

    engine = connector.get_engine(conn)
    insp = sa_inspect(engine)
    resolved = schema or ("public" if conn.db_type == "postgresql" else None)
    try:
        table_names = insp.get_table_names(schema=resolved)
    except Exception:
        return []
    return [
        TableInfo(name=t, schema=resolved or "", columns=[], estimated_rows=0)
        for t in table_names
    ]


# ---------------------------------------------------------------------------
# Direct query on connected database
# ---------------------------------------------------------------------------

class DirectQueryRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=10000)
    limit: int = Field(default=1000, ge=1, le=10000)


@router.post("/connections/{connection_id}/query", summary="Run read-only SQL against connected database")
async def direct_query(connection_id: str, body: DirectQueryRequest) -> Dict[str, Any]:
    conn = _get_connection(connection_id)
    connector = get_db_connector()

    # Validate SQL is read-only
    from app.services.db_connector import DatabaseConnector
    try:
        DatabaseConnector.validate_readonly_sql(body.sql)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    # Apply LIMIT wrapper
    sql_with_limit = f"SELECT * FROM ({body.sql}) _q LIMIT {body.limit}"

    def _execute():
        engine = connector.get_engine(conn)
        from sqlalchemy import text
        with engine.connect() as db_conn:
            result = db_conn.execute(text(sql_with_limit))
            columns = list(result.keys())
            rows = [list(row) for row in result.fetchall()]
            return columns, rows

    try:
        columns, rows = await asyncio.wait_for(
            asyncio.to_thread(_execute),
            timeout=30.0,
        )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Query timed out after 30 seconds")
    except Exception as e:
        error_msg = str(e)
        # Don't leak internal connection details
        if "password" in error_msg.lower() or "connection" in error_msg.lower():
            error_msg = "Query execution failed. Check your SQL syntax and database connection."
        raise HTTPException(status_code=502, detail=error_msg)

    return {
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "truncated": len(rows) >= body.limit,
    }


# ---------------------------------------------------------------------------
# Extract → Pipeline
# ---------------------------------------------------------------------------

def _run_extraction_pipeline(
    connection_id: str,
    extractions: List[Dict[str, Any]],
):
    """Background task: extract tables and run pipeline for each."""
    import asyncio
    from app.services.pipeline_service import get_pipeline_service

    conn = _get_connection(connection_id)
    connector = get_db_connector()
    pipeline = get_pipeline_service()

    for spec in extractions:
        dataset_id = spec["dataset_id"]
        try:
            # Mandate M1: write raw parquet to {data_directory}/{dataset_id}.parquet
            output_path = Path(settings.data_directory) / f"{dataset_id}.parquet"
            connector.extract_table(
                connection=conn,
                table_name=spec.get("table", ""),
                output_path=output_path,
                schema=spec.get("schema"),
                custom_sql=spec.get("custom_sql"),
                row_limit=spec.get("row_limit"),
            )

            # Update dataset status
            with get_session_context() as session:
                rec = session.get(DBDatasetRecord, dataset_id)
                if rec:
                    rec.status = DatasetStatus.EXTRACTING.value
                    rec.file_size_bytes = output_path.stat().st_size if output_path.exists() else 0
                    rec.updated_at = datetime.now(timezone.utc)
                    session.add(rec)
                    session.commit()

            # Run pipeline (creates processed.parquet, PII scan, compliance)
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(pipeline.run_full_pipeline(dataset_id))
            finally:
                loop.close()

            # Mark ready
            with get_session_context() as session:
                rec = session.get(DBDatasetRecord, dataset_id)
                if rec:
                    rec.status = DatasetStatus.READY.value
                    rec.processed_path = str(
                        Path(settings.processed_directory) / dataset_id / "processed.parquet"
                    )
                    rec.updated_at = datetime.now(timezone.utc)
                    session.add(rec)
                    session.commit()

            # Update last_sync_at on connection
            with get_session_context() as session:
                db_conn = session.get(DatabaseConnection, connection_id)
                if db_conn:
                    db_conn.last_sync_at = datetime.now(timezone.utc)
                    session.add(db_conn)
                    session.commit()

            logger.info("Extraction + pipeline complete for dataset %s", dataset_id)

        except Exception as e:
            logger.error("Extraction failed for dataset %s: %s", dataset_id, e, exc_info=True)
            with get_session_context() as session:
                rec = session.get(DBDatasetRecord, dataset_id)
                if rec:
                    rec.status = DatasetStatus.ERROR.value
                    rec.metadata_json = json.dumps({"error": str(e)})
                    rec.updated_at = datetime.now(timezone.utc)
                    session.add(rec)
                    session.commit()


@router.post("/connections/{connection_id}/extract", status_code=202, summary="Extract tables to datasets")
async def extract_tables(
    connection_id: str,
    body: ExtractRequest,
    background_tasks: BackgroundTasks,
) -> Dict[str, Any]:
    conn = _get_connection(connection_id)

    if not body.tables and not body.custom_sql:
        raise HTTPException(status_code=422, detail="Provide 'tables' or 'custom_sql'")
    if body.custom_sql and not body.dataset_name:
        raise HTTPException(status_code=422, detail="'dataset_name' is required for custom SQL")

    # Validate custom SQL upfront (fail fast before creating records)
    if body.custom_sql:
        try:
            from app.services.db_connector import DatabaseConnector
            DatabaseConnector.validate_readonly_sql(body.custom_sql)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))

    extractions = []
    dataset_ids = []

    if body.custom_sql:
        dataset_id = str(uuid.uuid4())[:8]
        dataset_name = body.dataset_name or "Custom Query"

        # Create DatasetRecord
        with get_session_context() as session:
            rec = DBDatasetRecord(
                id=dataset_id,
                original_filename=f"{dataset_name}.parquet",
                storage_filename=f"{dataset_id}_{dataset_name}.parquet",
                file_type="parquet",
                file_size_bytes=0,
                status=DatasetStatus.UPLOADED.value,
                metadata_json=json.dumps({
                    "source_type": "database",
                    "source_connection_id": connection_id,
                    "source_table": "custom_query",
                    "custom_sql": body.custom_sql,
                }),
            )
            session.add(rec)
            session.commit()

        extractions.append({
            "dataset_id": dataset_id,
            "custom_sql": body.custom_sql,
        })
        dataset_ids.append(dataset_id)
    else:
        for spec in body.tables:
            dataset_id = str(uuid.uuid4())[:8]
            table_label = f"{spec.schema_name}.{spec.table}" if spec.schema_name else spec.table

            with get_session_context() as session:
                rec = DBDatasetRecord(
                    id=dataset_id,
                    original_filename=f"{table_label}.parquet",
                    storage_filename=f"{dataset_id}_{spec.table}.parquet",
                    file_type="parquet",
                    file_size_bytes=0,
                    status=DatasetStatus.UPLOADED.value,
                    metadata_json=json.dumps({
                        "source_type": "database",
                        "source_connection_id": connection_id,
                        "source_table": table_label,
                    }),
                )
                session.add(rec)
                session.commit()

            extractions.append({
                "dataset_id": dataset_id,
                "table": spec.table,
                "schema": spec.schema_name,
                "row_limit": spec.row_limit,
            })
            dataset_ids.append(dataset_id)

    background_tasks.add_task(_run_extraction_pipeline, connection_id, extractions)

    return {
        "status": "accepted",
        "dataset_ids": dataset_ids,
        "message": f"Extraction started for {len(dataset_ids)} dataset(s)",
    }
