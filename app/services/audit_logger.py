"""
AuditLogger — SQLite Audit Trail for Tool Execution

Every tool call (including denials) is logged with:
- tool_input_hash for integrity
- resource_id in plaintext for debuggability
- outcome, duration, error category

Storage: data/audit.db (WAL mode for concurrent reads)
Retention: 30 days, pruned on startup.

PHASE: BQ-VZ-CONTROL-PLANE Step 2 — Security Foundation
CREATED: 2026-03-05
"""

import hashlib
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

AUDIT_DB_PATH = os.path.join("data", "audit.db")
RETENTION_DAYS = 30

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    tool_input_hash TEXT NOT NULL,
    resource_id TEXT,
    outcome TEXT NOT NULL,
    duration_ms INTEGER DEFAULT 0,
    error_category TEXT,
    approval_token_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Resource ID extraction — which field from tool_input to store as plaintext
RESOURCE_ID_FIELDS = ["dataset_id", "token_id", "platform", "query"]


class AuditLogger:
    """SQLite-backed audit logger for tool execution."""

    def __init__(self, db_path: str = AUDIT_DB_PATH) -> None:
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def _ensure_db(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(CREATE_TABLE_SQL)
        self._conn.commit()
        self._prune_old()
        return self._conn

    def _prune_old(self) -> None:
        """Remove entries older than RETENTION_DAYS."""
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
            conn = self._ensure_db()
            conn.execute("DELETE FROM audit_log WHERE timestamp < ?", (cutoff,))
            conn.commit()
        except Exception as e:
            logger.warning("Audit prune failed: %s", e)

    def _extract_resource_id(self, tool_input: dict) -> Optional[str]:
        """Extract the primary resource identifier from tool_input."""
        for field_name in RESOURCE_ID_FIELDS:
            val = tool_input.get(field_name)
            if val is not None:
                return str(val)[:200]
        return None

    async def log(
        self,
        session_id: str,
        user_id: str,
        tool_name: str,
        tool_input: dict,
        outcome: str,
        duration_ms: int = 0,
        error_category: Optional[str] = None,
        approval_token_id: Optional[str] = None,
    ) -> None:
        """Log a tool execution event."""
        try:
            conn = self._ensure_db()
            tool_input_hash = hashlib.sha256(
                json.dumps(tool_input, sort_keys=True).encode()
            ).hexdigest()
            resource_id = self._extract_resource_id(tool_input)

            conn.execute(
                """INSERT INTO audit_log
                   (timestamp, session_id, user_id, tool_name, tool_input_hash,
                    resource_id, outcome, duration_ms, error_category, approval_token_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    session_id,
                    user_id,
                    tool_name,
                    tool_input_hash,
                    resource_id,
                    outcome,
                    duration_ms,
                    error_category,
                    approval_token_id,
                ),
            )
            conn.commit()
        except Exception as e:
            logger.error("Audit log write failed: %s", e)


# Module-level singleton
audit_logger = AuditLogger()
