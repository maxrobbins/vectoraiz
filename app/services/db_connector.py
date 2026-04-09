"""
Database Connector Service
===========================

Manages external database connections for data extraction.
Supports PostgreSQL and MySQL with read-only enforcement.

Phase: BQ-VZ-DB-CONNECT
Created: 2026-02-25
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import sqlglot
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Engine

from app.config import settings
from app.models.database_connection import DatabaseConnection
from app.services.db_credential_service import decrypt_password

logger = logging.getLogger(__name__)

# Type mapping: DB types → Arrow-compatible types (Mandate M2)
_PG_TYPE_MAP = {
    "jsonb": "TEXT",
    "json": "TEXT",
    "array": "TEXT",
    "hstore": "TEXT",
    "uuid": "TEXT",
    "enum": "TEXT",
}

# Columns with these types are skipped entirely
_SKIP_TYPES = {"bytea", "blob", "binary", "varbinary", "longblob", "mediumblob", "tinyblob"}

# SQL statement types that are absolutely forbidden
_BLOCKED_STATEMENT_TYPES = frozenset({
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "REPLACE", "MERGE", "GRANT", "REVOKE", "COPY",
    "CALL", "EXECUTE", "EXEC", "SET", "LOCK", "UNLOCK",
    "LOAD", "IMPORT", "EXPORT", "VACUUM", "ANALYZE", "REINDEX",
    "COMMENT", "SECURITY", "REASSIGN", "CLUSTER", "REFRESH",
    "NOTIFY", "LISTEN", "UNLISTEN", "DISCARD", "RESET",
    "PREPARE", "DEALLOCATE", "SAVEPOINT", "RELEASE", "ROLLBACK",
    "COMMIT", "BEGIN", "START", "ABORT", "END",
    "RENAME", "MOVE", "FETCH", "CLOSE", "DECLARE",
})

# Dangerous patterns in SQL text (defense-in-depth beyond AST)
_DANGEROUS_PATTERNS = [
    r"\bSELECT\b.*\bINTO\s+\w",  # SELECT ... INTO new_table (creates tables)
    r"\bINTO\s+OUTFILE\b",
    r"\bINTO\s+DUMPFILE\b",
    r"\bLOAD_FILE\s*\(",
    r"\bpg_sleep\s*\(",
    r"\bSLEEP\s*\(",
    r"\bBENCHMARK\s*\(",
    r"\bdblink\b",
    r"\bCOPY\b",
    r"\bpg_read_file\s*\(",
    r"\bpg_ls_dir\s*\(",
    r"\blo_import\s*\(",
    r"\blo_export\s*\(",
]


class TableInfo:
    """Schema introspection result for a single table."""

    def __init__(
        self,
        name: str,
        schema: str,
        columns: List[Dict[str, Any]],
        primary_key: Optional[Dict] = None,
        estimated_rows: int = 0,
    ):
        self.name = name
        self.schema = schema
        self.columns = columns
        self.primary_key = primary_key
        self.estimated_rows = estimated_rows

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "schema": self.schema,
            "columns": [
                {
                    "name": c.get("name"),
                    "type": str(c.get("type", "")),
                    "nullable": c.get("nullable", True),
                }
                for c in self.columns
            ],
            "primary_key": self.primary_key,
            "estimated_rows": self.estimated_rows,
        }


class DatabaseConnector:
    """Manages external database connections for data extraction."""

    SUPPORTED_TYPES = {"postgresql", "mysql"}

    def __init__(self):
        self._engines: Dict[str, Engine] = {}

    def _build_url(self, connection: DatabaseConnection) -> str:
        """Build SQLAlchemy connection URL from a DatabaseConnection."""
        password = decrypt_password(connection.password_encrypted)
        if connection.db_type == "mysql":
            driver = "mysql+pymysql"
        else:
            driver = "postgresql+psycopg2"
        # URL-encode password to handle special characters
        from urllib.parse import quote_plus
        encoded_password = quote_plus(password)
        return f"{driver}://{connection.username}:{encoded_password}@{connection.host}:{connection.port}/{connection.database}"

    def _connect_args(self, connection: DatabaseConnection) -> dict:
        """Build driver-specific connect_args."""
        args: dict = {}
        if connection.db_type == "postgresql":
            ssl_map = {
                "disable": "disable",
                "prefer": "prefer",
                "require": "require",
            }
            sslmode = ssl_map.get(connection.ssl_mode, "prefer")
            args["options"] = f"-c statement_timeout=30000"
            args["sslmode"] = sslmode
        elif connection.db_type == "mysql":
            if connection.ssl_mode == "require":
                args["ssl"] = {"ssl": True}
        return args

    def get_engine(self, connection: DatabaseConnection) -> Engine:
        """Create or return cached SQLAlchemy engine with read-only enforcement."""
        if connection.id in self._engines:
            return self._engines[connection.id]

        url = self._build_url(connection)
        connect_args = self._connect_args(connection)

        engine_kwargs: dict = {
            "pool_size": 2,
            "max_overflow": 1,
            "pool_timeout": 10,
            "pool_recycle": 300,
            "connect_args": connect_args,
        }

        if connection.db_type == "postgresql":
            engine_kwargs["execution_options"] = {"postgresql_readonly": True}

        engine = create_engine(url, **engine_kwargs)

        # Mandate M4: enforce read-only at connection level
        if connection.db_type == "postgresql":
            @event.listens_for(engine, "connect")
            def _pg_readonly(dbapi_conn, connection_record):
                dbapi_conn.set_session(readonly=True, autocommit=False)
        elif connection.db_type == "mysql":
            @event.listens_for(engine, "connect")
            def _mysql_readonly(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                cursor.execute("SET SESSION TRANSACTION READ ONLY")
                cursor.close()

        self._engines[connection.id] = engine
        return engine

    def dispose_engine(self, connection_id: str) -> None:
        """Dispose and remove a cached engine."""
        engine = self._engines.pop(connection_id, None)
        if engine:
            engine.dispose()

    def test_connection(self, connection: DatabaseConnection) -> Dict[str, Any]:
        """Test connectivity. Returns {ok, latency_ms, server_version, error}."""
        try:
            engine = self.get_engine(connection)
            start = time.monotonic()
            with engine.connect() as conn:
                if connection.db_type == "postgresql":
                    row = conn.execute(text("SELECT version()")).fetchone()
                else:
                    row = conn.execute(text("SELECT version()")).fetchone()
                latency_ms = round((time.monotonic() - start) * 1000, 1)
                version = row[0] if row else "unknown"
            return {
                "ok": True,
                "latency_ms": latency_ms,
                "server_version": version,
                "error": None,
            }
        except Exception as e:
            # Clean up broken engine
            self.dispose_engine(connection.id)
            return {
                "ok": False,
                "latency_ms": None,
                "server_version": None,
                "error": str(e),
            }

    def introspect_schema(
        self, connection: DatabaseConnection, schema: Optional[str] = None
    ) -> List[TableInfo]:
        """Return all tables with columns, types, and row count estimates.

        Uses bulk SQL queries (3 total) instead of per-table inspector calls
        to avoid N*3 round-trips on remote databases.
        """
        if schema is None:
            schema = "public" if connection.db_type == "postgresql" else None

        try:
            if connection.db_type == "postgresql":
                return self._bulk_introspect_pg(connection, schema or "public")
            else:
                return self._bulk_introspect_mysql(connection, schema)
        except Exception as e:
            logger.warning("Bulk introspection failed, falling back to per-table: %s", e)
            return self._per_table_introspect(connection, schema)

    def _per_table_introspect(
        self, connection: DatabaseConnection, schema: Optional[str]
    ) -> List[TableInfo]:
        """Legacy per-table introspection as fallback."""
        engine = self.get_engine(connection)
        insp = inspect(engine)

        tables = []
        try:
            table_names = insp.get_table_names(schema=schema)
        except Exception as e:
            logger.error("Failed to list tables for connection %s: %s", connection.id, e)
            raise

        for table_name in table_names:
            try:
                columns = insp.get_columns(table_name, schema=schema)
                pk = insp.get_pk_constraint(table_name, schema=schema)
                row_count = self._estimate_row_count(engine, connection.db_type, schema, table_name)
                tables.append(
                    TableInfo(
                        name=table_name,
                        schema=schema or "",
                        columns=columns,
                        primary_key=pk,
                        estimated_rows=row_count,
                    )
                )
            except Exception as e:
                logger.warning("Skipping table %s.%s: %s", schema, table_name, e)

        return tables

    def _bulk_introspect_pg(
        self, connection: DatabaseConnection, schema: str
    ) -> List[TableInfo]:
        """Bulk introspection for PostgreSQL — 3 queries total."""
        engine = self.get_engine(connection)
        with engine.connect() as conn:
            # 1. All columns
            cols_rows = conn.execute(
                text(
                    "SELECT table_name, column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_schema = :schema "
                    "ORDER BY table_name, ordinal_position"
                ),
                {"schema": schema},
            ).fetchall()

            # 2. All primary keys
            pk_rows = conn.execute(
                text(
                    "SELECT tc.table_name, kcu.column_name "
                    "FROM information_schema.table_constraints tc "
                    "JOIN information_schema.key_column_usage kcu "
                    "  ON tc.constraint_name = kcu.constraint_name "
                    "  AND tc.table_schema = kcu.table_schema "
                    "WHERE tc.constraint_type = 'PRIMARY KEY' "
                    "  AND tc.table_schema = :schema "
                    "ORDER BY tc.table_name, kcu.ordinal_position"
                ),
                {"schema": schema},
            ).fetchall()

            # 3. All row count estimates
            count_rows = conn.execute(
                text(
                    "SELECT c.relname, c.reltuples::bigint "
                    "FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = :schema AND c.relkind = 'r'"
                ),
                {"schema": schema},
            ).fetchall()

        # Group columns by table
        columns_by_table: Dict[str, List[Dict[str, Any]]] = {}
        for table_name, col_name, data_type, is_nullable in cols_rows:
            columns_by_table.setdefault(table_name, []).append({
                "name": col_name,
                "type": data_type,
                "nullable": is_nullable == "YES",
            })

        # Group PKs by table
        pks_by_table: Dict[str, List[str]] = {}
        for table_name, col_name in pk_rows:
            pks_by_table.setdefault(table_name, []).append(col_name)

        # Row counts by table
        counts_by_table = {name: max(int(count), 0) for name, count in count_rows}

        # Build TableInfo list for all tables that have columns
        tables = []
        for table_name, columns in columns_by_table.items():
            pk_cols = pks_by_table.get(table_name, [])
            pk = {"constrained_columns": pk_cols} if pk_cols else None
            tables.append(
                TableInfo(
                    name=table_name,
                    schema=schema,
                    columns=columns,
                    primary_key=pk,
                    estimated_rows=counts_by_table.get(table_name, 0),
                )
            )

        return tables

    def _bulk_introspect_mysql(
        self, connection: DatabaseConnection, schema: Optional[str]
    ) -> List[TableInfo]:
        """Bulk introspection for MySQL — 3 queries total."""
        engine = self.get_engine(connection)
        with engine.connect() as conn:
            # Resolve schema if not provided
            resolved_schema = schema or conn.execute(text("SELECT DATABASE()")).scalar()

            # 1. All columns
            cols_rows = conn.execute(
                text(
                    "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
                    "FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = :schema "
                    "ORDER BY TABLE_NAME, ORDINAL_POSITION"
                ),
                {"schema": resolved_schema},
            ).fetchall()

            # 2. All primary keys
            pk_rows = conn.execute(
                text(
                    "SELECT tc.TABLE_NAME, kcu.COLUMN_NAME "
                    "FROM information_schema.TABLE_CONSTRAINTS tc "
                    "JOIN information_schema.KEY_COLUMN_USAGE kcu "
                    "  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
                    "  AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA "
                    "  AND tc.TABLE_NAME = kcu.TABLE_NAME "
                    "WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
                    "  AND tc.TABLE_SCHEMA = :schema "
                    "ORDER BY tc.TABLE_NAME, kcu.ORDINAL_POSITION"
                ),
                {"schema": resolved_schema},
            ).fetchall()

            # 3. All row count estimates
            count_rows = conn.execute(
                text(
                    "SELECT TABLE_NAME, TABLE_ROWS "
                    "FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = :schema"
                ),
                {"schema": resolved_schema},
            ).fetchall()

        # Group columns by table
        columns_by_table: Dict[str, List[Dict[str, Any]]] = {}
        for table_name, col_name, data_type, is_nullable in cols_rows:
            columns_by_table.setdefault(table_name, []).append({
                "name": col_name,
                "type": data_type,
                "nullable": is_nullable == "YES",
            })

        # Group PKs by table
        pks_by_table: Dict[str, List[str]] = {}
        for table_name, col_name in pk_rows:
            pks_by_table.setdefault(table_name, []).append(col_name)

        # Row counts by table
        counts_by_table = {
            name: int(count) if count else 0 for name, count in count_rows
        }

        # Build TableInfo list
        tables = []
        for table_name, columns in columns_by_table.items():
            pk_cols = pks_by_table.get(table_name, [])
            pk = {"constrained_columns": pk_cols} if pk_cols else None
            tables.append(
                TableInfo(
                    name=table_name,
                    schema=resolved_schema or "",
                    columns=columns,
                    primary_key=pk,
                    estimated_rows=counts_by_table.get(table_name, 0),
                )
            )

        return tables

    @staticmethod
    def _estimate_row_count(
        engine: Engine, db_type: str, schema: Optional[str], table_name: str
    ) -> int:
        """Fast row count estimate (no COUNT(*))."""
        try:
            with engine.connect() as conn:
                if db_type == "postgresql":
                    row = conn.execute(
                        text(
                            "SELECT reltuples::bigint FROM pg_class c "
                            "JOIN pg_namespace n ON n.oid = c.relnamespace "
                            "WHERE n.nspname = :schema AND c.relname = :table"
                        ),
                        {"schema": schema or "public", "table": table_name},
                    ).fetchone()
                    return max(int(row[0]), 0) if row else 0
                else:
                    # MySQL: use information_schema
                    row = conn.execute(
                        text(
                            "SELECT TABLE_ROWS FROM information_schema.TABLES "
                            "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table"
                        ),
                        {"schema": schema or conn.execute(text("SELECT DATABASE()")).scalar(), "table": table_name},
                    ).fetchone()
                    return int(row[0]) if row and row[0] else 0
        except Exception as e:
            logger.warning("Row count estimate failed for %s.%s: %s", schema, table_name, e)
            return 0

    def extract_table(
        self,
        connection: DatabaseConnection,
        table_name: str,
        output_path: Path,
        schema: Optional[str] = None,
        custom_sql: Optional[str] = None,
        row_limit: Optional[int] = None,
    ) -> Path:
        """Extract table data to a Parquet file. Returns path to parquet."""
        max_rows = settings.db_extract_max_rows

        if custom_sql:
            self.validate_readonly_sql(custom_sql)
            query = custom_sql
        else:
            sch = schema or ("public" if connection.db_type == "postgresql" else None)
            if sch:
                query = f'SELECT * FROM "{sch}"."{table_name}"'
            else:
                query = f'SELECT * FROM `{table_name}`'

        # Apply row limit (user-specified or system max)
        effective_limit = min(row_limit, max_rows) if row_limit else max_rows
        if effective_limit:
            query = f"SELECT * FROM ({query}) _sub LIMIT {effective_limit}"

        engine = self.get_engine(connection)
        return self._stream_to_parquet(engine, connection.db_type, query, output_path)

    def _stream_to_parquet(
        self, engine: Engine, db_type: str, query: str, output_path: Path
    ) -> Path:
        """Execute query with server-side cursor, write Arrow batches to Parquet."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        batch_size = 10_000
        writer: Optional[pq.ParquetWriter] = None
        total_rows = 0

        try:
            with engine.connect() as conn:
                result = conn.execution_options(stream_results=True).execute(text(query))
                col_names = list(result.keys())

                while True:
                    rows = result.fetchmany(batch_size)
                    if not rows:
                        break

                    # Convert rows to column-oriented dict for Arrow
                    col_data: Dict[str, list] = {name: [] for name in col_names}
                    for row in rows:
                        for i, name in enumerate(col_names):
                            col_data[name].append(row[i])

                    # Build Arrow arrays with type mapping (M2)
                    arrays = []
                    fields = []
                    for name in col_names:
                        values = col_data[name]
                        arr, field = self._to_arrow_column(name, values)
                        if arr is not None:
                            arrays.append(arr)
                            fields.append(field)

                    if not arrays:
                        break

                    arrow_schema = pa.schema(fields)
                    batch = pa.RecordBatch.from_arrays(arrays, schema=arrow_schema)

                    if writer is None:
                        writer = pq.ParquetWriter(str(output_path), arrow_schema)
                    writer.write_batch(batch)
                    total_rows += len(rows)

        finally:
            if writer:
                writer.close()

        if total_rows == 0:
            # Write an empty parquet so pipeline can still process
            if not output_path.exists():
                empty_table = pa.table({"_empty": pa.array([], type=pa.string())})
                pq.write_table(empty_table, str(output_path))

        logger.info("Extracted %d rows to %s", total_rows, output_path)
        return output_path

    @staticmethod
    def _to_arrow_column(name: str, values: list):
        """Convert a column's values to an Arrow array, applying type mapping (M2).

        Returns (array, field) or (None, None) if the column should be skipped.
        """
        if not values:
            arr = pa.array([], type=pa.string())
            return arr, pa.field(name, pa.string())

        # Sample first non-None value for type detection
        sample = None
        for v in values:
            if v is not None:
                sample = v
                break

        if sample is None:
            arr = pa.array(values, type=pa.string())
            return arr, pa.field(name, pa.string())

        type(sample).__name__.lower()

        # Skip binary columns (M2: BYTEA/BLOB → skip)
        if isinstance(sample, (bytes, bytearray, memoryview)):
            logger.warning("Skipping binary column '%s'", name)
            return None, None

        # Handle specific Python types
        if isinstance(sample, bool):
            arr = pa.array(values, type=pa.bool_())
            return arr, pa.field(name, pa.bool_())

        if isinstance(sample, int):
            arr = pa.array(values, type=pa.int64())
            return arr, pa.field(name, pa.int64())

        if isinstance(sample, float):
            arr = pa.array(values, type=pa.float64())
            return arr, pa.field(name, pa.float64())

        # DECIMAL/Numeric → FLOAT64 (M2)
        import decimal
        if isinstance(sample, decimal.Decimal):
            float_values = [float(v) if v is not None else None for v in values]
            arr = pa.array(float_values, type=pa.float64())
            return arr, pa.field(name, pa.float64())

        # datetime → timestamp (TIMESTAMPTZ → UTC per M2)
        if isinstance(sample, datetime):
            # Normalize to UTC
            utc_values = []
            for v in values:
                if v is not None and isinstance(v, datetime):
                    if v.tzinfo is not None:
                        v = v.astimezone(timezone.utc).replace(tzinfo=None)
                    utc_values.append(v)
                else:
                    utc_values.append(v)
            arr = pa.array(utc_values, type=pa.timestamp("us"))
            return arr, pa.field(name, pa.timestamp("us"))

        import datetime as dt
        if isinstance(sample, dt.date):
            arr = pa.array(values, type=pa.date32())
            return arr, pa.field(name, pa.date32())

        if isinstance(sample, dt.time):
            str_values = [str(v) if v is not None else None for v in values]
            arr = pa.array(str_values, type=pa.string())
            return arr, pa.field(name, pa.string())

        # UUID → TEXT (M2)
        import uuid
        if isinstance(sample, uuid.UUID):
            str_values = [str(v) if v is not None else None for v in values]
            arr = pa.array(str_values, type=pa.string())
            return arr, pa.field(name, pa.string())

        # JSONB, JSON, ARRAY, HSTORE, ENUM, unknown → TEXT (M2)
        if isinstance(sample, (list, dict)):
            import json
            str_values = [json.dumps(v) if v is not None else None for v in values]
            arr = pa.array(str_values, type=pa.string())
            return arr, pa.field(name, pa.string())

        # Default: cast to string
        str_values = [str(v) if v is not None else None for v in values]
        arr = pa.array(str_values, type=pa.string())
        return arr, pa.field(name, pa.string())

    @staticmethod
    def validate_readonly_sql(sql: str) -> None:
        """Reject any non-SELECT statement using sqlglot AST validation (M4).

        Raises ValueError if the SQL contains forbidden operations.
        """
        import re

        if not sql or not sql.strip():
            raise ValueError("Empty SQL query")

        # Layer 1: Quick text-level pre-checks
        normalized = sql.strip()
        upper = normalized.upper()

        # Must start with SELECT or WITH (CTEs)
        if not (upper.startswith("SELECT") or upper.startswith("WITH") or upper.startswith("(")):
            raise ValueError("Only SELECT queries are allowed")

        # Layer 2: Dangerous function/pattern checks
        for pattern in _DANGEROUS_PATTERNS:
            if re.search(pattern, sql, re.IGNORECASE):
                raise ValueError("Blocked SQL pattern detected")

        # Layer 3: AST validation via sqlglot
        try:
            parsed = sqlglot.parse(sql)
        except sqlglot.errors.ParseError:
            raise ValueError("SQL parse error — only valid SELECT queries are allowed")

        if not parsed:
            raise ValueError("Empty SQL parse result")

        for statement in parsed:
            if statement is None:
                continue
            stmt_type = type(statement).__name__.upper()
            # sqlglot uses class names like Select, Insert, Update, etc.
            if stmt_type not in ("SELECT", "UNION", "INTERSECT", "EXCEPT", "PAREN", "SUBQUERY", "CTE"):
                raise ValueError(f"Only SELECT queries are allowed, got: {stmt_type}")

            # Walk the AST to check for subquery mutations
            for node in statement.walk():
                node_type = type(node).__name__.upper()
                if node_type in _BLOCKED_STATEMENT_TYPES:
                    raise ValueError(f"Blocked SQL operation: {node_type}")


# Singleton
_connector: Optional[DatabaseConnector] = None


def get_db_connector() -> DatabaseConnector:
    """Get the singleton DatabaseConnector instance."""
    global _connector
    if _connector is None:
        _connector = DatabaseConnector()
    return _connector
