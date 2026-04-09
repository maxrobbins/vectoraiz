import duckdb
import logging
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime

import pandas as pd
import pyarrow.parquet as pq

from app.config import settings
from app.utils.sanitization import sql_quote_literal

_log = logging.getLogger(__name__)


class DuckDBService:
    """DuckDB connection manager with production settings."""
    
    SUPPORTED_EXTENSIONS = {'.csv', '.tsv', '.json', '.parquet', '.xlsx', '.xls'}
    
    def __init__(self):
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        self.data_dir = Path(settings.data_directory)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._metadata_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._excel_view_counter = 0
    
    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection with production settings."""
        if self._connection is None:
            self._connection = duckdb.connect(":memory:")
            # Production settings
            self._connection.execute(f"SET memory_limit = '{settings.duckdb_memory_limit}'")
            self._connection.execute(f"SET threads = {settings.duckdb_threads}")
            temp_dir = sql_quote_literal(f"{settings.data_directory}/temp")
            self._connection.execute(f"SET temp_directory = '{temp_dir}'")
            # Ensure temp directory exists
            Path(f"{settings.data_directory}/temp").mkdir(parents=True, exist_ok=True)
        return self._connection

    def create_ephemeral_connection(
        self,
        memory_limit: Optional[str] = None,
        threads: Optional[int] = None,
    ) -> duckdb.DuckDBPyConnection:
        """Create a fresh, isolated DuckDB connection.

        The caller is responsible for closing the returned connection.
        Used by SQLService so each query gets its own connection (thread-safe,
        independent timeout settings, no shared state).

        Args:
            memory_limit: Override memory limit (e.g. "256MB"). Defaults to settings.
            threads: Override thread count. Defaults to settings.

        BQ-MCP-RAG (M30): QueryOrchestrator passes tighter limits for external queries.
        """
        conn = duckdb.connect(":memory:")
        conn.execute(f"SET memory_limit = '{memory_limit or settings.duckdb_memory_limit}'")
        conn.execute(f"SET threads = {threads or settings.duckdb_threads}")
        temp_dir = sql_quote_literal(f"{settings.data_directory}/temp")
        conn.execute(f"SET temp_directory = '{temp_dir}'")
        # Ensure temp directory exists
        Path(f"{settings.data_directory}/temp").mkdir(parents=True, exist_ok=True)
        return conn
    
    def detect_file_type(self, filepath: Path) -> Optional[str]:
        """Detect file type from extension."""
        ext = filepath.suffix.lower()
        if ext in self.SUPPORTED_EXTENSIONS:
            return ext[1:]  # Remove the dot
        return None
    
    def _register_excel(self, filepath: str) -> str:
        """Read an Excel file via pandas and register it as a DuckDB view.

        Returns the view name to use in SQL queries.
        """
        self._excel_view_counter += 1
        view_name = f"_excel_view_{self._excel_view_counter}"
        ext = Path(filepath).suffix.lower()
        engine = "xlrd" if ext == ".xls" else "openpyxl"
        df = pd.read_excel(filepath, engine=engine)
        self.connection.register(view_name, df)
        return view_name

    # Safety invariant: ALLOWED_READ_TYPES is the exhaustive set of file types
    # that may be interpolated into SQL via get_read_function(). The filepath is
    # always escaped via sql_quote_literal AND must originate from our controlled
    # storage paths (record.upload_path / record.processed_path), never from
    # user-supplied strings directly.
    ALLOWED_READ_TYPES = frozenset({'csv', 'tsv', 'json', 'parquet', 'xlsx', 'xls'})

    def get_read_function(self, file_type: str, filepath: str) -> str:
        """Get the appropriate DuckDB read function for a file type.

        For Excel files, registers via pandas and returns the view name.
        For other types, returns the DuckDB reader function call with escaped path.

        The file_type MUST be in ALLOWED_READ_TYPES. The filepath MUST come from
        our controlled storage paths and is escaped via sql_quote_literal.
        """
        if file_type not in self.ALLOWED_READ_TYPES:
            raise ValueError(f"Unsupported file type for SQL read: {file_type}")
        if file_type in ('xlsx', 'xls'):
            return self._register_excel(filepath)
        escaped = sql_quote_literal(filepath)
        readers = {
            'csv': f"read_csv_auto('{escaped}')",
            'tsv': f"read_csv('{escaped}', delim='\\t', header=true, auto_detect=true)",
            'json': f"read_json_auto('{escaped}')",
            'parquet': f"read_parquet('{escaped}')",
        }
        return readers[file_type]
    
    def list_datasets(self) -> List[Dict[str, Any]]:
        """List all datasets in the data directory with metadata."""
        datasets = []
        
        if not self.data_dir.exists():
            return datasets
        
        for filepath in self.data_dir.iterdir():
            if filepath.is_file() and filepath.suffix.lower() in self.SUPPORTED_EXTENSIONS:
                try:
                    metadata = self.get_file_metadata(filepath)
                    datasets.append(metadata)
                except Exception as e:
                    # Include failed files with error status
                    datasets.append({
                        "id": filepath.stem,
                        "filename": filepath.name,
                        "filepath": str(filepath),
                        "file_type": self.detect_file_type(filepath),
                        "size_bytes": filepath.stat().st_size if filepath.exists() else 0,
                        "status": "error",
                        "error": str(e),
                        "created_at": datetime.fromtimestamp(filepath.stat().st_ctime).isoformat(),
                    })
        
        return sorted(datasets, key=lambda x: x.get("created_at", ""), reverse=True)
    
    # ------------------------------------------------------------------
    # M6: Arrow-based Parquet metadata (zero I/O for row count + schema)
    # ------------------------------------------------------------------

    def get_parquet_metadata_arrow(self, filepath: Path) -> Dict[str, Any]:
        """Extract metadata from a Parquet file using PyArrow metadata only.

        BQ-VZ-LARGE-FILES M6: Avoids full-scan COUNT(*) and DESCRIBE.
        - Row count from parquet_file.metadata.num_rows (zero I/O)
        - Schema from parquet_file.schema_arrow (metadata only)
        """
        pf = pq.ParquetFile(str(filepath))
        parquet_meta = pf.metadata
        arrow_schema = pf.schema_arrow

        row_count = parquet_meta.num_rows
        columns = [
            {
                "name": arrow_schema.field(i).name,
                "type": str(arrow_schema.field(i).type),
                "nullable": arrow_schema.field(i).nullable,
            }
            for i in range(len(arrow_schema))
        ]

        file_stat = filepath.stat()
        return {
            "id": filepath.stem,
            "filename": filepath.name,
            "filepath": str(filepath),
            "file_type": "parquet",
            "row_count": row_count,
            "column_count": len(columns),
            "columns": columns,
            "size_bytes": file_stat.st_size,
            "created_at": datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
            "modified_at": datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
            "status": "ready",
            "num_row_groups": parquet_meta.num_row_groups,
        }

    def get_parquet_sample_arrow(self, filepath: Path, limit: int = 10) -> List[Dict[str, Any]]:
        """Sample rows from Parquet using first row group only.

        BQ-VZ-LARGE-FILES M6: Never reads full file. Uses
        pf.read_row_group(0) for preview.
        """
        pf = pq.ParquetFile(str(filepath))
        if pf.metadata.num_row_groups == 0:
            return []

        table = pf.read_row_group(0)
        # Slice to limit
        if table.num_rows > limit:
            table = table.slice(0, limit)

        return table.to_pydict()  # Returns {col: [values]}

    def get_file_metadata(self, filepath: Path) -> Dict[str, Any]:
        """Extract metadata from a data file. Results are cached by filepath+mtime.

        BQ-VZ-LARGE-FILES M6: For Parquet files, uses Arrow metadata
        (zero I/O for row count + schema) instead of DuckDB COUNT(*)/DESCRIBE.
        """
        file_type = self.detect_file_type(filepath)
        if not file_type:
            raise ValueError(f"Unsupported file type: {filepath.suffix}")

        # Check cache by filepath + mtime
        cache_key = str(filepath)
        file_stat = filepath.stat()
        mtime = file_stat.st_mtime

        if cache_key in self._metadata_cache:
            cached_mtime, cached_result = self._metadata_cache[cache_key]
            if cached_mtime == mtime:
                return cached_result

        # M6: Use Arrow metadata for Parquet (avoids full scan)
        if file_type == "parquet":
            try:
                result = self.get_parquet_metadata_arrow(filepath)
                self._metadata_cache[cache_key] = (mtime, result)
                return result
            except Exception as e:
                _log.warning(
                    "Arrow metadata extraction failed for %s, falling back to DuckDB: %s",
                    filepath, e,
                )

        read_func = self.get_read_function(file_type, str(filepath))

        # Get row count
        count_result = self.connection.execute(f"SELECT COUNT(*) FROM {read_func}").fetchone()
        row_count = count_result[0] if count_result else 0

        # Get schema using DESCRIBE (wrap in subquery for table functions)
        schema_result = self.connection.execute(f"DESCRIBE SELECT * FROM {read_func}").fetchall()
        columns = [
            {
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == "YES" if len(row) > 2 else True,
            }
            for row in schema_result
        ]

        result = {
            "id": filepath.stem,
            "filename": filepath.name,
            "filepath": str(filepath),
            "file_type": file_type,
            "row_count": row_count,
            "column_count": len(columns),
            "columns": columns,
            "size_bytes": file_stat.st_size,
            "created_at": datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
            "modified_at": datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
            "status": "ready",
        }

        self._metadata_cache[cache_key] = (mtime, result)
        return result
    
    def get_dataset_by_id(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific dataset by its ID (filename without extension)."""
        for filepath in self.data_dir.iterdir():
            if filepath.is_file() and filepath.stem == dataset_id:
                if filepath.suffix.lower() in self.SUPPORTED_EXTENSIONS:
                    return self.get_file_metadata(filepath)
        return None
    
    def get_sample_rows(self, filepath: Path, limit: int = 10) -> List[Dict[str, Any]]:
        """Get sample rows from a dataset."""
        file_type = self.detect_file_type(filepath)
        if not file_type:
            raise ValueError(f"Unsupported file type: {filepath.suffix}")

        limit = int(limit)

        # For large files (>200MB), use an ephemeral connection to avoid
        # memory pressure on the shared connection that causes empty results.
        file_size = filepath.stat().st_size if filepath.exists() else 0
        use_ephemeral = file_size > 200 * 1024 * 1024

        if use_ephemeral:
            conn = self.create_ephemeral_connection()
        else:
            conn = self.connection

        try:
            # For ephemeral connections, build the read_func directly so it
            # doesn't reference a view registered on the shared connection.
            # get_read_function validates file_type against ALLOWED_READ_TYPES
            # and escapes filepath via sql_quote_literal.
            if use_ephemeral and file_type not in ('xlsx', 'xls'):
                read_func = self.get_read_function(file_type, str(filepath))
            else:
                read_func = self.get_read_function(file_type, str(filepath))

            # Get column names first
            schema = conn.execute(f"DESCRIBE SELECT * FROM {read_func}").fetchall()
            column_names = [row[0] for row in schema]

            result = conn.execute(
                f"SELECT * FROM {read_func} LIMIT {limit}"
            ).fetchall()

            # Convert to list of dicts, coercing non-serializable types to str
            rows = []
            for row in result:
                row_dict = {}
                for col_name, value in zip(column_names, row):
                    if isinstance(value, (bytes, bytearray)):
                        value = value.hex()
                    elif isinstance(value, memoryview):
                        value = bytes(value).hex()
                    elif hasattr(value, '__float__') and not isinstance(value, (int, float, bool)):
                        value = float(value)
                    row_dict[col_name] = value
                rows.append(row_dict)
            return rows
        finally:
            if use_ephemeral:
                conn.close()
    
    def get_column_statistics(self, filepath: Path) -> List[Dict[str, Any]]:
        """Get statistics for each column using SUMMARIZE."""
        file_type = self.detect_file_type(filepath)
        if not file_type:
            raise ValueError(f"Unsupported file type: {filepath.suffix}")
        
        read_func = self.get_read_function(file_type, str(filepath))
        
        # SUMMARIZE returns statistics for all columns
        cursor = self.connection.execute(f"SUMMARIZE SELECT * FROM {read_func}")
        result = cursor.fetchall()
        
        # Get column names from cursor description
        stat_columns = [desc[0] for desc in cursor.description]
        
        return [dict(zip(stat_columns, row)) for row in result]

    def get_column_profile(self, filepath: Path, max_rows: int = 100_000) -> List[Dict[str, Any]]:
        """
        Get detailed profile for each column including nulls, uniqueness, and sample values.

        Args:
            max_rows: Maximum rows to scan for profiling (default 100,000).
        """
        file_type = self.detect_file_type(filepath)
        if not file_type:
            raise ValueError(f"Unsupported file type: {filepath.suffix}")

        read_func = self.get_read_function(file_type, str(filepath))
        max_rows = int(max_rows)

        # Use a row-limited subquery to avoid full-scanning huge files
        source = f"(SELECT * FROM {read_func} LIMIT {max_rows})"

        # Get column names and types
        schema = self.connection.execute(f"DESCRIBE SELECT * FROM {read_func}").fetchall()

        profiles = []
        for col_info in schema:
            col_name = col_info[0]
            col_type = col_info[1]

            # Escape column name for SQL (double any embedded quotes)
            safe_name = col_name.replace('"', '""')
            escaped_col = f'"{safe_name}"'

            # Get column statistics in a single query
            stats_query = f"""
                SELECT
                    COUNT(*) as total_count,
                    COUNT({escaped_col}) as non_null_count,
                    COUNT(DISTINCT {escaped_col}) as distinct_count,
                    MIN({escaped_col}::VARCHAR) as min_value,
                    MAX({escaped_col}::VARCHAR) as max_value
                FROM {source}
            """
            stats = self.connection.execute(stats_query).fetchone()

            total_count = stats[0]
            non_null_count = stats[1]
            distinct_count = stats[2]
            min_value = stats[3]
            max_value = stats[4]

            null_count = total_count - non_null_count
            null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
            uniqueness_ratio = (distinct_count / non_null_count) if non_null_count > 0 else 0

            # Get sample values (up to 5 distinct non-null values)
            sample_query = f"""
                SELECT DISTINCT {escaped_col}::VARCHAR as val
                FROM {source}
                WHERE {escaped_col} IS NOT NULL
                LIMIT 5
            """
            sample_result = self.connection.execute(sample_query).fetchall()
            sample_values = [row[0] for row in sample_result]
            
            # Infer semantic type
            semantic_type = self._infer_semantic_type(col_name, col_type, sample_values)
            
            profiles.append({
                "name": col_name,
                "type": col_type,
                "semantic_type": semantic_type,
                "total_count": total_count,
                "non_null_count": non_null_count,
                "null_count": null_count,
                "null_percentage": round(null_percentage, 2),
                "distinct_count": distinct_count,
                "uniqueness_ratio": round(uniqueness_ratio, 4),
                "is_unique": uniqueness_ratio == 1.0 and non_null_count > 0,
                "is_potential_id": uniqueness_ratio > 0.95 and non_null_count > 0,
                "min_value": min_value,
                "max_value": max_value,
                "sample_values": sample_values,
            })
        
        return profiles

    def _infer_semantic_type(
        self, 
        col_name: str, 
        col_type: str, 
        sample_values: List[str]
    ) -> str:
        """
        Infer semantic type from column name, type, and sample values.
        Returns: email, url, phone, date, currency, percentage, id, text, numeric, boolean, unknown
        """
        col_name_lower = col_name.lower()
        
        # Check column name patterns
        if any(x in col_name_lower for x in ['email', 'e_mail', 'e-mail']):
            return "email"
        if any(x in col_name_lower for x in ['url', 'link', 'website', 'href']):
            return "url"
        if any(x in col_name_lower for x in ['phone', 'tel', 'mobile', 'fax']):
            return "phone"
        if any(x in col_name_lower for x in ['_id', 'id_', '_key', 'uuid', 'guid']):
            return "id"
        if col_name_lower in ['id', 'key', 'pk']:
            return "id"
        if any(x in col_name_lower for x in ['date', 'time', '_at', '_on', 'created', 'updated', 'timestamp']):
            return "datetime"
        if any(x in col_name_lower for x in ['price', 'cost', 'amount', 'revenue', 'salary', 'fee', 'total']):
            return "currency"
        if any(x in col_name_lower for x in ['percent', 'pct', 'rate', 'ratio']):
            return "percentage"
        
        # Check sample values for patterns
        if sample_values:
            email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
            url_pattern = r'^https?://|^www\.'
            phone_pattern = r'^[\+]?[(]?[0-9]{1,3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}$'
            
            matches = {'email': 0, 'url': 0, 'phone': 0}
            for val in sample_values:
                if val:
                    if re.match(email_pattern, str(val)):
                        matches['email'] += 1
                    if re.match(url_pattern, str(val)):
                        matches['url'] += 1
                    if re.match(phone_pattern, str(val).replace(' ', '')):
                        matches['phone'] += 1
            
            # If majority match a pattern
            threshold = len(sample_values) * 0.6
            if matches['email'] >= threshold:
                return "email"
            if matches['url'] >= threshold:
                return "url"
            if matches['phone'] >= threshold:
                return "phone"
        
        # Fall back to basic type inference
        col_type_lower = col_type.lower()
        if 'int' in col_type_lower or 'float' in col_type_lower or 'double' in col_type_lower or 'decimal' in col_type_lower:
            return "numeric"
        if 'bool' in col_type_lower:
            return "boolean"
        if 'date' in col_type_lower or 'time' in col_type_lower:
            return "datetime"
        if 'varchar' in col_type_lower or 'text' in col_type_lower or 'string' in col_type_lower:
            return "text"
        
        return "unknown"

    def calculate_searchability_score(self, filepath: Path) -> Dict[str, Any]:
        """
        Calculate a searchability score (0-100) indicating how well the dataset can be searched.
        Higher scores mean better semantic search potential.
        """
        profiles = self.get_column_profile(filepath)
        
        score = 0
        max_score = 100
        factors = []
        
        # Factor 1: Has text columns (30 points max)
        text_columns = [p for p in profiles if p['semantic_type'] in ['text', 'email', 'url']]
        text_score = min(30, len(text_columns) * 10)
        score += text_score
        factors.append({
            "name": "text_columns",
            "score": text_score,
            "max": 30,
            "detail": f"{len(text_columns)} searchable text columns"
        })
        
        # Factor 2: Data completeness (25 points max)
        avg_completeness = sum(100 - p['null_percentage'] for p in profiles) / len(profiles) if profiles else 0
        completeness_score = round(avg_completeness * 0.25)
        score += completeness_score
        factors.append({
            "name": "completeness",
            "score": completeness_score,
            "max": 25,
            "detail": f"{round(avg_completeness)}% average completeness"
        })
        
        # Factor 3: Column diversity (20 points max)
        semantic_types = set(p['semantic_type'] for p in profiles)
        diversity_score = min(20, len(semantic_types) * 4)
        score += diversity_score
        factors.append({
            "name": "diversity",
            "score": diversity_score,
            "max": 20,
            "detail": f"{len(semantic_types)} different data types"
        })
        
        # Factor 4: Has identifiable columns (15 points max)
        id_columns = [p for p in profiles if p['semantic_type'] == 'id' or p['is_potential_id']]
        id_score = min(15, len(id_columns) * 5)
        score += id_score
        factors.append({
            "name": "identifiers",
            "score": id_score,
            "max": 15,
            "detail": f"{len(id_columns)} identifier columns"
        })
        
        # Factor 5: Reasonable column count (10 points max)
        col_count = len(profiles)
        if 3 <= col_count <= 50:
            col_score = 10
        elif col_count < 3:
            col_score = col_count * 3
        else:
            col_score = max(0, 10 - (col_count - 50) // 10)
        score += col_score
        factors.append({
            "name": "column_count",
            "score": col_score,
            "max": 10,
            "detail": f"{col_count} columns"
        })
        
        # Determine grade
        if score >= 80:
            grade = "A"
        elif score >= 60:
            grade = "B"
        elif score >= 40:
            grade = "C"
        elif score >= 20:
            grade = "D"
        else:
            grade = "F"
        
        return {
            "score": min(score, max_score),
            "max_score": max_score,
            "grade": grade,
            "factors": factors,
        }

    def get_enhanced_metadata(self, filepath: Path) -> Dict[str, Any]:
        """
        Get comprehensive metadata including basic info, column profiles, and searchability.
        """
        # Basic metadata
        basic = self.get_file_metadata(filepath)
        
        # Column profiles
        profiles = self.get_column_profile(filepath)
        
        # Searchability score
        searchability = self.calculate_searchability_score(filepath)
        
        # Calculate estimated memory size (rough estimate)
        file_type = self.detect_file_type(filepath)
        read_func = self.get_read_function(file_type, str(filepath))
        
        # Get approximate memory usage
        try:
            mem_query = f"""
                SELECT
                    SUM(LENGTH(t.*::VARCHAR)) as total_bytes
                FROM (SELECT * FROM {read_func} LIMIT 1000) t
            """
            mem_result = self.connection.execute(mem_query).fetchone()
            sample_bytes = mem_result[0] if mem_result and mem_result[0] else 0
            estimated_memory_bytes = int(sample_bytes * basic['row_count'] / 1000) if basic['row_count'] > 1000 else sample_bytes
        except Exception:
            estimated_memory_bytes = basic.get('size_bytes', 0)
        
        return {
            **basic,
            "column_profiles": profiles,
            "searchability": searchability,
            "estimated_memory_bytes": estimated_memory_bytes,
            "estimated_memory_mb": round(estimated_memory_bytes / (1024 * 1024), 2),
        }

    def write_parquet(self, filepath: Path, data: List[tuple], columns: List[str]):
        """Write data to a Parquet file.

        Args:
            filepath: Path to the Parquet file
            data: List of tuples representing the data
            columns: List of column names
        """
        df = pd.DataFrame(data, columns=columns)
        self.connection.register('_tmp_parquet_write', df)
        escaped_path = sql_quote_literal(str(filepath))
        try:
            self.connection.execute(
                f"COPY _tmp_parquet_write TO '{escaped_path}' (FORMAT PARQUET)"
            )
        finally:
            self.connection.unregister('_tmp_parquet_write')
    
    # ------------------------------------------------------------------
    # M5: DuckDB disk-spill cleanup on dataset deletion
    # ------------------------------------------------------------------

    def cleanup_dataset_temp(self, dataset_id: str) -> None:
        """Remove temp/spill files for a deleted dataset.

        BQ-VZ-LARGE-FILES M5: Ensures disk-spill artifacts are cleaned up
        when a dataset is deleted.
        """
        temp_dir = Path(settings.data_directory) / "temp"
        if not temp_dir.exists():
            return
        # DuckDB spill files are named with connection-specific prefixes,
        # not dataset IDs. Clean up any orphaned files that are stale.
        try:
            import time
            stale_threshold = time.time() - 3600  # 1 hour
            for f in temp_dir.iterdir():
                if f.is_file() and f.stat().st_mtime < stale_threshold:
                    try:
                        f.unlink()
                        _log.debug("Cleaned up stale temp file: %s", f)
                    except OSError:
                        pass
        except Exception as e:
            _log.warning("Temp cleanup failed: %s", e)

    def close(self):
        """Close the DuckDB connection."""
        if self._connection:
            self._connection.close()
            self._connection = None


# Singleton instance
_duckdb_service: Optional[DuckDBService] = None


def get_duckdb_service() -> DuckDBService:
    """Get the singleton DuckDB service instance."""
    global _duckdb_service
    if _duckdb_service is None:
        _duckdb_service = DuckDBService()
    return _duckdb_service


@contextmanager
def ephemeral_duckdb_service():
    """Create an isolated DuckDBService with its own connection.

    Used by worker threads that run concurrently (e.g. processing_service)
    to avoid sharing the singleton connection, which is not thread-safe at
    the DuckDB C layer and causes segfaults under concurrent access.

    Usage::

        with ephemeral_duckdb_service() as duckdb:
            duckdb.get_file_metadata(path)
    """
    svc = DuckDBService()
    try:
        yield svc
    finally:
        svc.close()
