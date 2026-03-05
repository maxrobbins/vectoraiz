"""
SQL query service with security hardening.
Allows safe SELECT queries against processed datasets.

Uses ephemeral DuckDB connections with views — no SQL rewriting.
"""

import re
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from pathlib import Path

import duckdb

from app.config import settings
from app.services.duckdb_service import ephemeral_duckdb_service
from app.services.processing_service import get_processing_service, ProcessingService, ProcessingStatus
from app.utils.sanitization import sql_quote_literal


# Dangerous SQL patterns to block (defense-in-depth)
BLOCKED_PATTERNS = [
    r'\bCOPY\b',
    r'\bCREATE\b',
    r'\bDROP\b',
    r'\bDELETE\b',
    r'\bINSERT\b',
    r'\bUPDATE\b',
    r'\bALTER\b',
    r'\bTRUNCATE\b',
    r'\bREPLACE\b',
    r'\bATTACH\b',
    r'\bDETACH\b',
    r'\bEXPORT\b',
    r'\bIMPORT\b',
    r'\bLOAD\b',
    r'\bINSTALL\b',
    r'\bCALL\b',
    r'\bEXECUTE\b',
    r'\bPRAGMA\b',
    r'\bSET\b',
    r'\bRESET\b',
    # Block filesystem access
    r'read_csv\s*\(',
    r'read_json\s*\(',
    r'read_parquet\s*\(',
    r'read_text\s*\(',
    r'st_read\s*\(',
    r'glob\s*\(',
    # Block system functions
    r'current_database\s*\(',
    r'current_schema\s*\(',
]

# Compiled regex for efficiency
BLOCKED_REGEX = re.compile('|'.join(BLOCKED_PATTERNS), re.IGNORECASE)

# Dataset ID validation pattern
DATASET_ID_REGEX = re.compile(r'^[a-zA-Z0-9_-]+$')

# Default limits
DEFAULT_ROW_LIMIT = 1000
MAX_ROW_LIMIT = 10000
class SQLValidationError(Exception):
    """Raised when SQL query fails validation."""
    pass


class QueryTimeoutError(Exception):
    """Raised when a SQL query exceeds the timeout."""
    pass


class SQLService:
    """
    Secure SQL query execution service.
    Only allows SELECT queries against registered datasets.

    Uses ephemeral DuckDB connections with CREATE VIEW for each dataset,
    then runs the user's SQL as-is. No regex-based SQL rewriting.
    """

    def __init__(self):
        self.processing: ProcessingService = get_processing_service()
        self.processed_dir = Path(settings.processed_directory)

    def validate_query(self, query: str) -> Tuple[bool, Optional[str]]:
        """
        Validate a SQL query for safety.

        Returns:
            (is_valid, error_message)
        """
        query_stripped = query.strip()

        # Must not be empty
        if not query_stripped:
            return False, "Query cannot be empty"

        # Must start with SELECT or WITH (allowlist)
        query_upper = query_stripped.upper()
        if not (query_upper.startswith('SELECT') or query_upper.startswith('WITH')):
            return False, "Only SELECT queries are allowed"

        # Check for blocked patterns (defense-in-depth)
        match = BLOCKED_REGEX.search(query)
        if match:
            return False, f"Query contains blocked operation: {match.group()}"

        # Check for file path patterns that might bypass restrictions
        if re.search(r'[\'"].*[/\\].*[\'"]', query):
            return False, "Direct file path access is not allowed"

        return True, None

    @staticmethod
    def _validate_dataset_id(dataset_id: str) -> None:
        """Validate that a dataset ID matches the safe pattern.

        Raises SQLValidationError with a 400-level message if invalid.
        """
        if not DATASET_ID_REGEX.match(dataset_id):
            raise SQLValidationError(
                f"Invalid dataset ID '{dataset_id}': must match [a-zA-Z0-9_-]+"
            )

    def get_dataset_table_name(self, dataset_id: str) -> str:
        """Get the virtual table name for a dataset."""
        return f"dataset_{dataset_id}"

    def get_available_tables(self) -> List[Dict[str, Any]]:
        """Get list of available dataset tables for querying."""
        datasets = self.processing.list_datasets()
        tables = []

        for record in datasets:
            if record.status == ProcessingStatus.READY and record.processed_path:
                tables.append({
                    "table_name": self.get_dataset_table_name(record.id),
                    "dataset_id": record.id,
                    "filename": record.original_filename,
                    "row_count": record.metadata.get("row_count", 0),
                    "columns": [c["name"] for c in record.metadata.get("columns", [])],
                })

        return tables

    def get_table_schema(self, dataset_id: str) -> Dict[str, Any]:
        """Get schema for a specific dataset table."""
        record = self.processing.get_dataset(dataset_id)
        if not record:
            raise ValueError(f"Dataset '{dataset_id}' not found")

        if record.status != ProcessingStatus.READY:
            raise ValueError(f"Dataset '{dataset_id}' is not ready")

        if not record.processed_path or not record.processed_path.exists():
            raise ValueError(f"Dataset '{dataset_id}' file not found")

        # Get column info
        with ephemeral_duckdb_service() as duckdb_svc:
            metadata = duckdb_svc.get_file_metadata(record.processed_path)

        return {
            "table_name": self.get_dataset_table_name(dataset_id),
            "dataset_id": dataset_id,
            "filename": record.original_filename,
            "columns": metadata.get("columns", []),
            "row_count": metadata.get("row_count", 0),
        }

    def _resolve_datasets(
        self, dataset_id: Optional[str]
    ) -> List[Tuple[str, Path]]:
        """Resolve which datasets to expose as views.

        Returns list of (dataset_id, parquet_path) tuples.
        Validates dataset IDs.
        """
        if dataset_id:
            self._validate_dataset_id(dataset_id)
            record = self.processing.get_dataset(dataset_id)
            if not record:
                raise ValueError(f"Dataset '{dataset_id}' not found")
            if record.status != ProcessingStatus.READY:
                raise ValueError(f"Dataset '{dataset_id}' is not ready")
            if not record.processed_path:
                raise ValueError(f"Dataset '{dataset_id}' has no processed file")
            return [(record.id, record.processed_path)]
        else:
            # All ready datasets
            results = []
            for record in self.processing.list_datasets():
                if record.status == ProcessingStatus.READY and record.processed_path:
                    self._validate_dataset_id(record.id)
                    results.append((record.id, record.processed_path))
            return results

    @staticmethod
    def _create_views(
        conn: "duckdb.DuckDBPyConnection",
        datasets: List[Tuple[str, Path]],
    ) -> None:
        """Create OR REPLACE VIEWs for each dataset on the given connection."""
        for ds_id, parquet_path in datasets:
            escaped_path = sql_quote_literal(str(parquet_path))
            conn.execute(
                f"CREATE OR REPLACE VIEW dataset_{ds_id} "
                f"AS SELECT * FROM read_parquet('{escaped_path}')"
            )

    @staticmethod
    def _strip_trailing_semicolons(query: str) -> str:
        """Strip trailing semicolons from the user query."""
        return query.rstrip().rstrip(';').rstrip()

    @staticmethod
    def _wrap_with_pagination(query: str, limit: int, offset: int) -> str:
        """Wrap user query with an outer LIMIT/OFFSET for pagination.

        Always applied — caps output regardless of user's own LIMIT.
        """
        return f"SELECT * FROM ({query}) AS _q LIMIT {limit} OFFSET {offset}"

    def execute_query(
        self,
        query: str,
        dataset_id: Optional[str] = None,
        limit: int = DEFAULT_ROW_LIMIT,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Execute a SQL query against dataset(s).

        Opens an ephemeral DuckDB connection, creates views for each
        available dataset, executes the user SQL with pagination wrapper,
        then closes the connection.

        Timeout is enforced by the caller via ``run_sync()``'s asyncio timeout.

        Args:
            query: SQL SELECT query
            dataset_id: If provided, query runs against this dataset only
            limit: Maximum rows to return (capped at MAX_ROW_LIMIT)
            offset: Row offset for pagination

        Returns:
            Query results with metadata

        Raises:
            SQLValidationError: Query fails validation (400)
            ValueError: Dataset not found or not ready (400)
        """
        start_time = datetime.utcnow()

        # Validate query
        is_valid, error = self.validate_query(query)
        if not is_valid:
            raise SQLValidationError(error)

        # Enforce limits
        limit = min(limit, MAX_ROW_LIMIT)

        # Resolve datasets to expose
        # NOTE: variable deliberately named _ds_views (not "datasets") to avoid
        # DuckDB replacement-scan picking up a Python local when a user query
        # references an unresolved table called "datasets".
        _ds_views = self._resolve_datasets(dataset_id)

        # Prepare user query: strip semicolons, wrap with pagination
        clean_query = self._strip_trailing_semicolons(query)
        wrapped_query = self._wrap_with_pagination(clean_query, limit, offset)

        # Execute on ephemeral connection
        with ephemeral_duckdb_service() as duckdb_svc:
            conn = duckdb_svc.create_ephemeral_connection()
            try:

                # Create views for each dataset
                self._create_views(conn, _ds_views)

                # Execute the user query
                result = conn.execute(wrapped_query)
                rows = result.fetchall()

                # Get column names
                columns = [desc[0] for desc in result.description]

                # Convert to list of dicts
                data = [dict(zip(columns, row)) for row in rows]

                # Serialize values
                data = self._serialize_results(data)

                end_time = datetime.utcnow()
                duration_ms = (end_time - start_time).total_seconds() * 1000

                return {
                    "query": query,
                    "columns": columns,
                    "data": data,
                    "row_count": len(data),
                    "limit": limit,
                    "offset": offset,
                    "duration_ms": round(duration_ms, 2),
                    "truncated": len(data) == limit,
                }

            except duckdb.InvalidInputException as e:
                error_msg = str(e)
                if str(self.processed_dir) in error_msg:
                    error_msg = error_msg.replace(str(self.processed_dir), "[data]")
                raise ValueError(f"Query execution failed: {error_msg}")
            except duckdb.Error as e:
                error_msg = str(e)
                if str(self.processed_dir) in error_msg:
                    error_msg = error_msg.replace(str(self.processed_dir), "[data]")
                raise ValueError(f"Query execution failed: {error_msg}")
            finally:
                conn.close()

    def _serialize_results(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Serialize query results to JSON-compatible format."""
        serialized = []
        for row in data:
            serialized_row = {}
            for key, value in row.items():
                if value is None:
                    serialized_row[key] = None
                elif isinstance(value, (int, float, bool, str)):
                    serialized_row[key] = value
                else:
                    # Convert other types to string
                    serialized_row[key] = str(value)
            serialized.append(serialized_row)
        return serialized


# Singleton instance
_sql_service: Optional[SQLService] = None


def get_sql_service() -> SQLService:
    """Get the singleton SQL service instance."""
    global _sql_service
    if _sql_service is None:
        _sql_service = SQLService()
    return _sql_service
