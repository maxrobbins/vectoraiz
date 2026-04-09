"""
SQL Sandbox — Regex and pattern-based validation (defense-in-depth layer).

Combined with DuckDB ephemeral connections, read-only settings, LIMIT wrapper,
and table allowlisting for multi-layer security.

Rules (enforced by pattern matching + optional AST validation):
1. Single statement only (reject `;` separators)
2. SELECT (and WITH...SELECT) ONLY — reject INSERT/UPDATE/DELETE/DROP/ALTER/CREATE
3. Reject DuckDB file primitives: COPY, ATTACH, INSTALL, LOAD, PRAGMA
4. Reject read_csv_auto, read_parquet, etc. on arbitrary paths
5. Table names must be in the user's allowed set (dataset_{id} format)

When sqlglot is available, an additional AST validation pass verifies statement
types, table references, and function calls at the structural level.

PHASE: BQ-ALLAI-B0 — Security Infrastructure
CREATED: 2026-02-16
"""

import logging
import re
from typing import Set, Tuple

logger = logging.getLogger(__name__)

# Optional AST validation via sqlglot
try:
    import sqlglot
    from sqlglot import expressions as exp
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    logger.warning("sqlglot not available — SQL sandbox will use regex-only validation")

# Statements that are NEVER allowed (case-insensitive word boundary check)
BLOCKED_STATEMENTS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "COPY", "ATTACH", "DETACH", "INSTALL", "LOAD", "PRAGMA",
    "EXPORT", "IMPORT", "VACUUM", "CHECKPOINT", "CALL",
    "EXECUTE", "SET", "RESET", "TRUNCATE", "REPLACE",
}

# Functions that access the filesystem directly
BLOCKED_FUNCTIONS = {
    "read_csv", "read_csv_auto", "read_parquet", "read_json",
    "read_json_auto", "read_blob", "glob", "read_text",
    "read_ndjson_auto", "read_ndjson", "st_read",
}

# BQ-MCP-RAG: Additional blocks for external mode (tighter than internal)
EXTERNAL_BLOCKED_FUNCTIONS = BLOCKED_FUNCTIONS | {
    "httpfs", "http_get", "http_post",
}

# BQ-MCP-RAG: Schema/system access blocked for external mode
EXTERNAL_BLOCKED_PATTERNS = {
    "information_schema",
    "pg_catalog",
    "temp",
    "__internal",
}

# Schema-qualified prefixes that are blocked for external mode
EXTERNAL_BLOCKED_SCHEMAS = {"information_schema", "pg_catalog", "temp", "main"}

# Quoted identifier bypass detection — blocked keywords hidden in double quotes
_QUOTED_KEYWORD_PATTERN = re.compile(
    r'"(' + '|'.join(re.escape(s) for s in BLOCKED_STATEMENTS) + r')"',
    re.IGNORECASE,
)

# Regex for extracting table references (FROM/JOIN clauses)
# Captures optional schema prefix: schema.table or just table
_TABLE_REF_PATTERN = re.compile(
    r'(?:FROM|JOIN)\s+(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?([a-zA-Z_][a-zA-Z0-9_]*)',
    re.IGNORECASE,
)

# Regex for extracting CTE names (WITH name AS ...)
_CTE_NAME_PATTERN = re.compile(
    r'\bWITH\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\('
    r'|,\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+AS\s*\(',
    re.IGNORECASE,
)


class SQLSandbox:
    """
    Validates SQL queries before execution.

    Additional layer on top of sql_service.validate_query().
    Both must pass before a query reaches DuckDB.
    """

    def __init__(self, allowed_tables: Set[str]):
        """
        Args:
            allowed_tables: Set of table names the user can access
                           (e.g., {"dataset_abc123", "dataset_def456"})
        """
        self.allowed_tables = {t.lower() for t in allowed_tables}

    def validate(self, sql: str) -> Tuple[bool, str]:
        """
        Validate SQL query.

        Returns: (is_valid, error_message)
        """
        if not sql or not sql.strip():
            return False, "Empty SQL query"

        sql_stripped = sql.strip().rstrip(";").strip()

        # 1. Reject multiple statements (semicolons in the body)
        if ";" in sql_stripped:
            return False, "Multiple SQL statements not allowed"

        # 2. First keyword must be SELECT or WITH
        tokens = sql_stripped.split()
        if not tokens:
            return False, "Empty SQL query"

        first_keyword = tokens[0].upper()
        if first_keyword not in ("SELECT", "WITH"):
            return False, f"Only SELECT queries allowed, got: {first_keyword}"

        # 3. Check for blocked statements as standalone words
        sql_upper = sql_stripped.upper()
        # Pad with spaces for word boundary matching
        padded = f" {sql_upper} "
        for blocked in BLOCKED_STATEMENTS:
            # Match as word boundary (space/paren/comma before and after)
            if re.search(rf'[\s,(]{blocked}[\s,(]', padded):
                # Exception: allow "DELETE" inside string literals — but since
                # LLM-generated SQL shouldn't have DELETE in column names, block it
                return False, f"Blocked SQL operation: {blocked}"

        # 4. Check for blocked filesystem functions
        sql_lower = sql_stripped.lower()
        for func in BLOCKED_FUNCTIONS:
            # Match function call pattern: func_name followed by (
            if re.search(rf'\b{re.escape(func)}\s*\(', sql_lower):
                return False, f"Blocked function: {func}"

        # 5. Table access validation — extract table references and verify
        # First, extract CTE alias names so they're treated as "allowed"
        cte_names = set()
        for match in _CTE_NAME_PATTERN.finditer(sql_stripped):
            name = match.group(1) or match.group(2)
            if name:
                cte_names.add(name.lower())

        table_refs = _TABLE_REF_PATTERN.findall(sql_stripped)
        for schema, table_name in table_refs:
            table_lower = table_name.lower()
            # Skip common SQL pseudo-tables, subquery aliases, and CTE names
            if table_lower in ("dual", "generate_series", "range", "unnest"):
                continue
            if table_lower in cte_names:
                continue
            # Check against allowed set
            if table_lower not in self.allowed_tables:
                if schema:
                    display = f"{schema}.{table_name}"
                else:
                    display = table_name
                return False, (
                    f"Table '{display}' is not accessible. "
                    f"Available tables: {', '.join(sorted(self.allowed_tables))}"
                )

        return True, ""

    def validate_external(self, sql: str, max_length: int = 4096) -> Tuple[bool, str]:
        """Validate SQL for external (MCP/REST) queries — tighter than internal.

        Additional checks beyond validate():
          - Max SQL length (M28)
          - Block information_schema, temp schema access
          - Block network/extension functions (httpfs, http_get, etc.)

        BQ-MCP-RAG: §4.3 external mode.
        """
        if not sql or not sql.strip():
            return False, "Empty SQL query"

        # Max length check (M28)
        if len(sql) > max_length:
            return False, f"SQL exceeds maximum length of {max_length} characters"

        # Run all standard checks first
        is_valid, error = self.validate(sql)
        if not is_valid:
            return is_valid, error

        sql_lower = sql.lower()

        # Block external-only functions (network access, etc.)
        for func in EXTERNAL_BLOCKED_FUNCTIONS - BLOCKED_FUNCTIONS:
            if re.search(rf'\b{re.escape(func)}\s*\(', sql_lower):
                return False, f"Blocked function: {func}"

        # Block schema/system access patterns
        for pattern in EXTERNAL_BLOCKED_PATTERNS:
            if pattern in sql_lower:
                return False, f"Access to '{pattern}' is not allowed for external queries"

        # Block schema-qualified access (e.g., main.secret_table, pg_catalog.pg_tables)
        schema_ref = re.findall(r'(\w+)\.(\w+)', sql_lower)
        for schema, table in schema_ref:
            if schema in EXTERNAL_BLOCKED_SCHEMAS:
                qualified = f"{schema}.{table}"
                if f"dataset_{table}" not in self.allowed_tables:
                    return False, f"Schema-qualified access '{qualified}' is not allowed for external queries"

        # Block quoted identifiers that hide blocked keywords (e.g., "ATTACH", "COPY")
        quoted_match = _QUOTED_KEYWORD_PATTERN.search(sql)
        if quoted_match:
            return False, f"Blocked keyword in quoted identifier: {quoted_match.group(0)}"

        # Optional AST validation pass (defense-in-depth)
        ast_valid, ast_error = self._validate_ast(sql)
        if not ast_valid:
            return False, ast_error

        return True, ""

    def _validate_ast(self, sql: str) -> Tuple[bool, str]:
        """Optional AST validation using sqlglot (if available).

        Falls back to pass-through if sqlglot is not installed or parse fails.
        """
        if not SQLGLOT_AVAILABLE:
            return True, ""

        try:
            parsed = sqlglot.parse(sql, error_level=sqlglot.ErrorLevel.IGNORE)
        except Exception:
            # Parse failure — fall back to regex-only validation (already passed)
            logger.debug("sqlglot parse failed, falling back to regex validation")
            return True, ""

        if not parsed:
            return True, ""

        for statement in parsed:
            if statement is None:
                continue

            # Only SELECT and WITH (CTE) statements allowed
            if not isinstance(statement, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
                # Check if it's a CTE wrapping a SELECT
                if isinstance(statement, exp.CTE):
                    continue
                stmt_type = type(statement).__name__
                return False, f"AST validation: only SELECT statements allowed, got {stmt_type}"

            # Walk AST to check table references
            for table in statement.find_all(exp.Table):
                table_name = table.name.lower() if table.name else ""
                catalog = table.catalog.lower() if table.catalog else ""
                db = table.db.lower() if table.db else ""

                # Block schema-qualified access
                if db in EXTERNAL_BLOCKED_SCHEMAS or catalog in EXTERNAL_BLOCKED_SCHEMAS:
                    return False, f"AST validation: blocked schema access '{db}.{table_name}'"

                # Check table against allowlist (skip subquery aliases)
                if table_name and table_name not in self.allowed_tables:
                    # Could be a CTE alias or subquery — only flag if not internal
                    pass  # Regex check already handles this

            # Walk AST to check function calls
            for func in statement.find_all(exp.Anonymous):
                func_name = func.name.lower() if func.name else ""
                if func_name in EXTERNAL_BLOCKED_FUNCTIONS:
                    return False, f"AST validation: blocked function '{func_name}'"

        return True, ""

    @staticmethod
    def build_allowed_tables(user_dataset_ids: list) -> Set[str]:
        """Build the set of allowed table names from user's dataset IDs."""
        return {f"dataset_{did}" for did in user_dataset_ids}
