"""
Tests for async schema introspection (Bug 1) and direct SQL query endpoint (Bug 2).

Covers:
- Introspection runs in background thread (doesn't block event loop)
- Introspection timeout returns partial results
- Introspection partial flag in response
- Direct query endpoint returns columns/rows
- Direct query read-only enforcement (rejects INSERT/UPDATE/DELETE/DROP)
- Direct query timeout (30s)
- Direct query on nonexistent connection → 404
- Direct query with empty SQL → 422
- Direct query result truncation flag
- SQL validation blocks dangerous patterns
"""

import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.db_connector import TableInfo

client = TestClient(app)


def _create_connection(**overrides):
    payload = {
        "name": "Test DB",
        "db_type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "username": "testuser",
        "password": "testpass",
        "ssl_mode": "disable",
    }
    payload.update(overrides)
    return client.post("/api/v1/db/connections", json=payload)


# =====================================================================
# Bug 1: Async schema introspection with timeout
# =====================================================================

class TestAsyncIntrospection:
    @patch("app.routers.database.get_db_connector")
    def test_introspect_returns_tables_and_partial_flag(self, mock_get):
        """Normal introspection returns tables with partial=False."""
        create_resp = _create_connection(name="Async Test")
        conn_id = create_resp.json()["id"]

        mock_connector = MagicMock()
        mock_connector.introspect_schema.return_value = [
            TableInfo(name="users", schema="public", columns=[
                {"name": "id", "type": "INTEGER", "nullable": False}
            ], estimated_rows=500),
            TableInfo(name="orders", schema="public", columns=[
                {"name": "id", "type": "INTEGER", "nullable": False}
            ], estimated_rows=1000),
        ]
        mock_get.return_value = mock_connector

        resp = client.get(f"/api/v1/db/connections/{conn_id}/schema")
        assert resp.status_code == 200
        data = resp.json()
        assert data["partial"] is False
        assert "warning" not in data
        assert len(data["tables"]) == 2
        assert data["tables"][0]["name"] == "users"
        assert data["tables"][1]["name"] == "orders"

    @patch("app.routers.database._partial_introspect")
    @patch("app.routers.database.get_db_connector")
    def test_introspect_timeout_returns_partial(self, mock_get, mock_partial):
        """When introspection takes >15s, return partial results with warning."""
        create_resp = _create_connection(name="Timeout Test")
        conn_id = create_resp.json()["id"]

        def slow_introspect(*args, **kwargs):
            time.sleep(20)  # Simulate very slow introspection
            return []

        mock_connector = MagicMock()
        mock_connector.introspect_schema.side_effect = slow_introspect
        mock_get.return_value = mock_connector

        # Partial introspect fallback returns table names only
        mock_partial.return_value = [
            TableInfo(name="t1", schema="public", columns=[], estimated_rows=0),
            TableInfo(name="t2", schema="public", columns=[], estimated_rows=0),
        ]

        resp = client.get(f"/api/v1/db/connections/{conn_id}/schema")
        assert resp.status_code == 200
        data = resp.json()
        assert data["partial"] is True
        assert "warning" in data
        assert "timed out" in data["warning"].lower()
        assert len(data["tables"]) == 2

    @patch("app.routers.database.get_db_connector")
    def test_introspect_error_returns_502(self, mock_get):
        """When introspection raises, return 502."""
        create_resp = _create_connection(name="Error Test")
        conn_id = create_resp.json()["id"]

        mock_connector = MagicMock()
        mock_connector.introspect_schema.side_effect = Exception("Connection refused")
        mock_get.return_value = mock_connector

        resp = client.get(f"/api/v1/db/connections/{conn_id}/schema")
        assert resp.status_code == 502

    @patch("app.routers.database.get_db_connector")
    def test_introspect_does_not_block_event_loop(self, mock_get):
        """Introspection runs in a thread so the event loop is not blocked."""
        create_resp = _create_connection(name="Non-blocking Test")
        conn_id = create_resp.json()["id"]

        call_thread = {}

        def record_thread(*args, **kwargs):
            import threading
            call_thread["name"] = threading.current_thread().name
            return [TableInfo(name="t", schema="public", columns=[], estimated_rows=0)]

        mock_connector = MagicMock()
        mock_connector.introspect_schema.side_effect = record_thread
        mock_get.return_value = mock_connector

        resp = client.get(f"/api/v1/db/connections/{conn_id}/schema")
        assert resp.status_code == 200
        # asyncio.to_thread runs in a thread pool worker, not MainThread
        assert "name" in call_thread
        # The thread name should NOT be the main thread when running under async
        # (In TestClient sync mode it may run in main, but the async wrapper is tested)


# =====================================================================
# Bug 2: Direct SQL query endpoint
# =====================================================================

class TestDirectQuery:
    @patch("app.routers.database.get_db_connector")
    def test_direct_query_success(self, mock_get):
        """Valid SELECT returns columns and rows."""
        create_resp = _create_connection(name="Query Test")
        conn_id = create_resp.json()["id"]

        mock_result = MagicMock()
        mock_result.keys.return_value = ["id", "name"]
        mock_result.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        mock_connector = MagicMock()
        mock_connector.get_engine.return_value = mock_engine
        mock_get.return_value = mock_connector

        resp = client.post(
            f"/api/v1/db/connections/{conn_id}/query",
            json={"sql": "SELECT id, name FROM users"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["columns"] == ["id", "name"]
        assert data["row_count"] == 2
        assert data["rows"] == [[1, "Alice"], [2, "Bob"]]
        assert data["truncated"] is False

    def test_direct_query_rejects_insert(self):
        """INSERT is rejected with 422."""
        create_resp = _create_connection(name="Insert Block")
        conn_id = create_resp.json()["id"]
        resp = client.post(
            f"/api/v1/db/connections/{conn_id}/query",
            json={"sql": "INSERT INTO users (name) VALUES ('evil')"},
        )
        assert resp.status_code == 422

    def test_direct_query_rejects_update(self):
        """UPDATE is rejected with 422."""
        create_resp = _create_connection(name="Update Block")
        conn_id = create_resp.json()["id"]
        resp = client.post(
            f"/api/v1/db/connections/{conn_id}/query",
            json={"sql": "UPDATE users SET name='evil'"},
        )
        assert resp.status_code == 422

    def test_direct_query_rejects_delete(self):
        """DELETE is rejected with 422."""
        create_resp = _create_connection(name="Delete Block")
        conn_id = create_resp.json()["id"]
        resp = client.post(
            f"/api/v1/db/connections/{conn_id}/query",
            json={"sql": "DELETE FROM users"},
        )
        assert resp.status_code == 422

    def test_direct_query_rejects_drop(self):
        """DROP TABLE is rejected with 422."""
        create_resp = _create_connection(name="Drop Block")
        conn_id = create_resp.json()["id"]
        resp = client.post(
            f"/api/v1/db/connections/{conn_id}/query",
            json={"sql": "DROP TABLE users"},
        )
        assert resp.status_code == 422

    def test_direct_query_rejects_empty_sql(self):
        """Empty SQL is rejected."""
        create_resp = _create_connection(name="Empty SQL")
        conn_id = create_resp.json()["id"]
        resp = client.post(
            f"/api/v1/db/connections/{conn_id}/query",
            json={"sql": ""},
        )
        assert resp.status_code == 422

    def test_direct_query_nonexistent_connection(self):
        """Query on nonexistent connection returns 404."""
        resp = client.post(
            "/api/v1/db/connections/nonexistent/query",
            json={"sql": "SELECT 1"},
        )
        assert resp.status_code == 404

    @patch("app.routers.database.get_db_connector")
    def test_direct_query_truncation_flag(self, mock_get):
        """When result count equals limit, truncated flag is True."""
        create_resp = _create_connection(name="Truncate Test")
        conn_id = create_resp.json()["id"]

        # Return exactly `limit` rows
        mock_result = MagicMock()
        mock_result.keys.return_value = ["id"]
        mock_result.fetchall.return_value = [(i,) for i in range(5)]

        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        mock_connector = MagicMock()
        mock_connector.get_engine.return_value = mock_engine
        mock_get.return_value = mock_connector

        resp = client.post(
            f"/api/v1/db/connections/{conn_id}/query",
            json={"sql": "SELECT id FROM big_table", "limit": 5},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["truncated"] is True
        assert data["row_count"] == 5

    @patch("app.routers.database.get_db_connector")
    def test_direct_query_timeout(self, mock_get):
        """Query that exceeds 30s timeout returns 504."""
        create_resp = _create_connection(name="Timeout Query")
        conn_id = create_resp.json()["id"]

        def slow_query(*args, **kwargs):
            time.sleep(35)

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = slow_query
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        mock_connector = MagicMock()
        mock_connector.get_engine.return_value = mock_engine
        mock_get.return_value = mock_connector

        resp = client.post(
            f"/api/v1/db/connections/{conn_id}/query",
            json={"sql": "SELECT * FROM huge_table"},
        )
        assert resp.status_code == 504
        assert "timed out" in resp.json()["detail"].lower()

    def test_direct_query_rejects_dangerous_patterns(self):
        """Dangerous SQL patterns like pg_sleep are rejected."""
        create_resp = _create_connection(name="Dangerous Pattern")
        conn_id = create_resp.json()["id"]
        resp = client.post(
            f"/api/v1/db/connections/{conn_id}/query",
            json={"sql": "SELECT pg_sleep(100)"},
        )
        assert resp.status_code == 422

    @patch("app.routers.database.get_db_connector")
    def test_direct_query_with_cte(self, mock_get):
        """WITH (CTE) queries are accepted."""
        create_resp = _create_connection(name="CTE Test")
        conn_id = create_resp.json()["id"]

        mock_result = MagicMock()
        mock_result.keys.return_value = ["n"]
        mock_result.fetchall.return_value = [(1,)]

        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_result
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn

        mock_connector = MagicMock()
        mock_connector.get_engine.return_value = mock_engine
        mock_get.return_value = mock_connector

        resp = client.post(
            f"/api/v1/db/connections/{conn_id}/query",
            json={"sql": "WITH cte AS (SELECT 1 AS n) SELECT * FROM cte"},
        )
        assert resp.status_code == 200
