import pytest
from pathlib import Path
import csv

from fastapi.testclient import TestClient
from app.main import app
from app.services.sql_service import SQLService, SQLValidationError
from app.services.processing_service import get_processing_service, ProcessingStatus
from app.services.duckdb_service import get_duckdb_service

client = TestClient(app)


@pytest.fixture
def sql_service():
    """Create SQL service instance."""
    return SQLService()


def test_validate_select_query(sql_service):
    """Test that SELECT queries are allowed."""
    is_valid, error = sql_service.validate_query("SELECT * FROM some_table")
    assert is_valid == True
    assert error is None


def test_validate_with_cte(sql_service):
    """Test that WITH (CTE) queries are allowed."""
    query = "WITH cte AS (SELECT * FROM table1) SELECT * FROM cte"
    is_valid, error = sql_service.validate_query(query)
    assert is_valid == True


def test_block_drop_query(sql_service):
    """Test that DROP queries are blocked."""
    is_valid, error = sql_service.validate_query("DROP TABLE users")
    assert is_valid == False
    # It fails the whitelist check first
    assert "only select" in error.lower() or "blocked" in error.lower()


def test_block_delete_query(sql_service):
    """Test that DELETE queries are blocked."""
    is_valid, error = sql_service.validate_query("DELETE FROM users WHERE id = 1")
    assert is_valid == False


def test_block_insert_query(sql_service):
    """Test that INSERT queries are blocked."""
    is_valid, error = sql_service.validate_query("INSERT INTO users VALUES (1, 'test')")
    assert is_valid == False


def test_block_update_query(sql_service):
    """Test that UPDATE queries are blocked."""
    is_valid, error = sql_service.validate_query("UPDATE users SET name = 'test'")
    assert is_valid == False


def test_block_copy_query(sql_service):
    """Test that COPY queries are blocked."""
    is_valid, error = sql_service.validate_query("COPY users TO '/tmp/data.csv'")
    assert is_valid == False


def test_block_file_read(sql_service):
    """Test that direct file read functions are blocked."""
    is_valid, error = sql_service.validate_query("SELECT * FROM read_csv('/etc/passwd')")
    assert is_valid == False


def test_block_file_path(sql_service):
    """Test that file paths in queries are blocked."""
    is_valid, error = sql_service.validate_query("SELECT '/etc/passwd' as path")
    assert is_valid == False


def test_empty_query(sql_service):
    """Test that empty queries are rejected."""
    is_valid, error = sql_service.validate_query("")
    assert is_valid == False
    assert "empty" in error.lower()


def test_list_tables_endpoint():
    """Test listing available tables."""
    response = client.get("/api/sql/tables")
    assert response.status_code == 200
    data = response.json()
    assert "tables" in data
    assert "count" in data


def test_sql_help_endpoint():
    """Test SQL help endpoint."""
    response = client.get("/api/sql/help")
    assert response.status_code == 200
    data = response.json()
    assert "allowed_operations" in data
    assert "blocked_operations" in data
    assert "examples" in data


def test_validate_endpoint():
    """Test query validation endpoint."""
    response = client.post("/api/sql/validate", json={
        "query": "SELECT * FROM test_table"
    })
    assert response.status_code == 200
    data = response.json()
    assert data["valid"] == True


def test_validate_blocked_endpoint():
    """Test validation catches blocked queries."""
    response = client.post("/api/sql/validate", json={
        "query": "DROP TABLE users"
    })
    assert response.status_code == 200
    data = response.json()
    assert data["valid"] == False
    assert data["error"] is not None


def test_query_execution_blocked():
    """Test that blocked queries don't execute."""
    response = client.post("/api/sql/query", json={
        "query": "DELETE FROM users"
    })
    assert response.status_code == 400
    assert "Invalid query" in response.json()["detail"]


def test_query_get_endpoint():
    """Test GET query endpoint exists."""
    response = client.get("/api/sql/query", params={"q": "SELECT 1"})
    # May succeed or fail based on table existence, but shouldn't error on validation
    assert response.status_code in [200, 400, 500]


def test_sql_integration(tmp_path):
    """Test full SQL execution against a created dataset."""
    from app.services.processing_service import ProcessingStatus, DatasetRecord
    
    # 1. Setup mock ProcessingService to return a record
    # Actually, we can just use the real one if we mock the filesystem or creaate a file
    # Let's create a real parquet file
    
    # Create sample CSV
    csv_file = tmp_path / "sql_test.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'value'])
        writer.writerow([1, 'Item A', 100])
        writer.writerow([2, 'Item B', 200])
    
    # Convert to Parquet manually via duckdb for the test
    duckdb_service = get_duckdb_service()
    parquet_path = tmp_path / "sql_test.parquet"
    duckdb_service.connection.execute(f"COPY (SELECT * FROM read_csv_auto('{csv_file}')) TO '{parquet_path}' (FORMAT PARQUET)")
    
    # We need to register this dataset in ProcessingService
    # But ProcessingService reads from JSON file.
    # We can perform a trick: Subclass or mock ProcessingService.get_dataset
    
    # Easier: Just use SQLService directly with a mocked ProcessingService get_dataset method
    sql = SQLService()
    
    # Mock get_dataset
    class MockRecord:
        id = "sql_test_123"
        status = ProcessingStatus.READY
        processed_path = parquet_path
        original_filename = "sql_test.csv"
        metadata = {"columns": [{"name": "id"}, {"name": "name"}, {"name": "value"}]}
        
    original_get = sql.processing.get_dataset
    sql.processing.get_dataset = lambda x: MockRecord() if x == "sql_test_123" else None
    
    try:
        # Test query
        result = sql.execute_query(
            query="SELECT * FROM dataset_sql_test_123 ORDER BY id DESC",
            dataset_id="sql_test_123"
        )
        
        assert result["row_count"] == 2
        assert result["data"][0]["name"] == "Item B"
        assert result["data"][1]["name"] == "Item A"
        
    finally:
        # Restore
        sql.processing.get_dataset = original_get


def test_no_replacement_scan_leak(tmp_path):
    """Querying non-existent table 'datasets' must NOT trigger DuckDB replacement scan
    against the Python local variable (regression test for the replacement-scan bug)."""
    import csv as csv_mod

    csv_file = tmp_path / "rs_test.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv_mod.writer(f)
        writer.writerow(['id', 'val'])
        writer.writerow([1, 'a'])

    duckdb_service = get_duckdb_service()
    parquet_path = tmp_path / "rs_test.parquet"
    duckdb_service.connection.execute(
        f"COPY (SELECT * FROM read_csv_auto('{csv_file}')) TO '{parquet_path}' (FORMAT PARQUET)"
    )

    sql = SQLService()

    class MockRecord:
        id = "rs_test_1"
        status = ProcessingStatus.READY
        processed_path = parquet_path
        original_filename = "rs_test.csv"
        metadata = {"columns": [{"name": "id"}, {"name": "val"}]}

    original_get = sql.processing.get_dataset
    sql.processing.get_dataset = lambda x: MockRecord() if x == "rs_test_1" else None

    try:
        with pytest.raises(ValueError, match="Query execution failed"):
            # Query references 'datasets' — should get a clean "table not found"
            # error, NOT a replacement-scan "Python Object" error.
            sql.execute_query(
                query="SELECT * FROM datasets",
                dataset_id="rs_test_1",
            )
    finally:
        sql.processing.get_dataset = original_get
