import pytest
from pathlib import Path
from unittest.mock import patch
import csv

import pandas as pd
import duckdb

from app.services.duckdb_service import DuckDBService


@pytest.fixture
def duckdb_service(tmp_path):
    """Create a DuckDB service with a temp data directory."""
    with patch("app.services.duckdb_service.settings") as mock_settings:
        mock_settings.data_directory = str(tmp_path / "data")
        mock_settings.duckdb_memory_limit = "256MB"
        mock_settings.duckdb_threads = 2
        service = DuckDBService()
        yield service
        service.close()


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file for testing."""
    csv_file = tmp_path / "test_data.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'value'])
        writer.writerow([1, 'Alice', 100])
        writer.writerow([2, 'Bob', 200])
        writer.writerow([3, 'Charlie', 300])
    return csv_file


@pytest.fixture
def sample_xlsx(tmp_path):
    """Create a sample Excel file for testing."""
    xlsx_file = tmp_path / "test_data.xlsx"
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [100, 200, 300],
    })
    df.to_excel(xlsx_file, index=False)
    return xlsx_file


@pytest.fixture
def csv_with_quotes(tmp_path):
    """Create a CSV with single quotes in the filename."""
    csv_file = tmp_path / "it's_a_test.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name'])
        writer.writerow([1, 'Alice'])
        writer.writerow([2, 'Bob'])
    return csv_file


def test_detect_file_type(duckdb_service):
    """Test file type detection."""
    assert duckdb_service.detect_file_type(Path("test.csv")) == "csv"
    assert duckdb_service.detect_file_type(Path("test.json")) == "json"
    assert duckdb_service.detect_file_type(Path("test.parquet")) == "parquet"
    assert duckdb_service.detect_file_type(Path("test.txt")) is None


def test_get_file_metadata(duckdb_service, sample_csv):
    """Test metadata extraction from CSV."""
    metadata = duckdb_service.get_file_metadata(sample_csv)

    assert metadata["filename"] == "test_data.csv"
    assert metadata["file_type"] == "csv"
    assert metadata["row_count"] == 3
    assert metadata["column_count"] == 3
    assert metadata["status"] == "ready"

    column_names = [c["name"] for c in metadata["columns"]]
    assert "id" in column_names
    assert "name" in column_names
    assert "value" in column_names


def test_get_sample_rows(duckdb_service, sample_csv):
    """Test sample row retrieval uses LIMIT (returns exact count)."""
    sample = duckdb_service.get_sample_rows(sample_csv, limit=2)

    assert len(sample) == 2
    assert "id" in sample[0]
    assert "name" in sample[0]
    assert "value" in sample[0]


def test_get_sample_rows_returns_correct_count(duckdb_service, sample_csv):
    """Requesting more rows than available returns all rows."""
    sample = duckdb_service.get_sample_rows(sample_csv, limit=100)
    assert len(sample) == 3


def test_connection_settings(duckdb_service):
    """Test that DuckDB connection has correct settings."""
    result = duckdb_service.connection.execute("SELECT current_setting('threads')").fetchone()
    assert result is not None


def test_excel_read_via_get_read_function(duckdb_service, sample_xlsx):
    """Excel files should be read via pandas, not st_read."""
    view_name = duckdb_service.get_read_function('xlsx', str(sample_xlsx))
    # The view name should be a registered view, not an st_read() call
    assert 'st_read' not in view_name
    assert view_name.startswith('_excel_view_')

    # Should be queryable
    result = duckdb_service.connection.execute(f"SELECT * FROM {view_name}").fetchall()
    assert len(result) == 3


def test_excel_get_file_metadata(duckdb_service, sample_xlsx):
    """Metadata extraction should work for Excel files."""
    metadata = duckdb_service.get_file_metadata(sample_xlsx)
    assert metadata["file_type"] == "xlsx"
    assert metadata["row_count"] == 3
    assert metadata["column_count"] == 3
    assert metadata["status"] == "ready"


def test_excel_get_sample_rows(duckdb_service, sample_xlsx):
    """Sample rows should work for Excel files."""
    rows = duckdb_service.get_sample_rows(sample_xlsx, limit=2)
    assert len(rows) == 2
    assert "name" in rows[0]


def test_write_parquet_produces_valid_file(duckdb_service, tmp_path):
    """write_parquet should produce a valid Parquet file readable by DuckDB."""
    output = tmp_path / "output.parquet"
    data = [(1, 'Alice', 100), (2, 'Bob', 200)]
    columns = ['id', 'name', 'value']

    duckdb_service.write_parquet(output, data, columns)

    assert output.exists()
    assert output.stat().st_size > 0

    # Verify it's readable
    result = duckdb.connect(":memory:").execute(
        f"SELECT * FROM read_parquet('{output}')"
    ).fetchall()
    assert len(result) == 2
    assert result[0][1] == 'Alice'


def test_filename_with_quotes_csv(duckdb_service, csv_with_quotes):
    """Filenames containing single quotes should not break SQL queries."""
    metadata = duckdb_service.get_file_metadata(csv_with_quotes)
    assert metadata["row_count"] == 2
    assert metadata["status"] == "ready"


def test_filename_with_quotes_sample_rows(duckdb_service, csv_with_quotes):
    """get_sample_rows should handle filenames with quotes."""
    rows = duckdb_service.get_sample_rows(csv_with_quotes, limit=5)
    assert len(rows) == 2


def test_column_profile_respects_max_rows(duckdb_service, sample_csv):
    """get_column_profile should respect the max_rows parameter."""
    # With max_rows=2, total_count in profile should be 2 (not 3)
    profiles = duckdb_service.get_column_profile(sample_csv, max_rows=2)
    assert len(profiles) == 3  # 3 columns
    for p in profiles:
        assert p["total_count"] == 2


def test_column_profile_default_works(duckdb_service, sample_csv):
    """get_column_profile should work with default max_rows."""
    profiles = duckdb_service.get_column_profile(sample_csv)
    assert len(profiles) == 3
    for p in profiles:
        assert p["total_count"] == 3


def test_metadata_caching(duckdb_service, sample_csv):
    """Repeated calls to get_file_metadata should use cache."""
    result1 = duckdb_service.get_file_metadata(sample_csv)
    result2 = duckdb_service.get_file_metadata(sample_csv)
    # Should be the exact same dict object from cache
    assert result1 is result2


def test_write_parquet_with_quoted_path(duckdb_service, tmp_path):
    """write_parquet should handle paths with special characters."""
    output = tmp_path / "it's_output.parquet"
    data = [(1, 'x')]
    columns = ['id', 'val']

    duckdb_service.write_parquet(output, data, columns)
    assert output.exists()
