import pytest
import csv

from app.services.duckdb_service import DuckDBService


@pytest.fixture
def duckdb_service():
    """Create a DuckDB service instance."""
    return DuckDBService()


@pytest.fixture
def sample_csv_with_types(tmp_path):
    """Create a sample CSV with various data types for testing."""
    csv_file = tmp_path / "test_types.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'email', 'revenue', 'created_at', 'website'])
        writer.writerow([1, 'Acme Corp', 'contact@acme.com', 1500000, '2024-01-15', 'https://acme.com'])
        writer.writerow([2, 'Globex Inc', 'info@globex.com', 2300000, '2024-02-20', 'https://globex.com'])
        writer.writerow([3, 'Initech', None, 890000, '2024-03-10', 'www.initech.com'])
        writer.writerow([4, 'Umbrella', 'sales@umbrella.com', None, '2024-04-05', None])
        writer.writerow([5, 'Stark Ind', 'tony@stark.com', 12000000, '2024-05-01', 'https://stark.com'])
    return csv_file


def test_column_profile(duckdb_service, sample_csv_with_types):
    """Test column profiling with null and uniqueness analysis."""
    profiles = duckdb_service.get_column_profile(sample_csv_with_types)
    
    assert len(profiles) == 6
    
    # Check ID column
    id_profile = next(p for p in profiles if p['name'] == 'id')
    assert id_profile['null_percentage'] == 0
    assert id_profile['is_unique'] == True
    assert id_profile['semantic_type'] == 'id'
    
    # Check email column (has one null)
    email_profile = next(p for p in profiles if p['name'] == 'email')
    assert email_profile['null_percentage'] == 20.0  # 1 out of 5
    assert email_profile['semantic_type'] == 'email'
    
    # Check revenue column (has one null)
    revenue_profile = next(p for p in profiles if p['name'] == 'revenue')
    assert revenue_profile['null_count'] == 1
    assert revenue_profile['semantic_type'] == 'currency'


def test_semantic_type_inference(duckdb_service):
    """Test semantic type inference from column names."""
    # Test via internal method
    assert duckdb_service._infer_semantic_type('user_email', 'VARCHAR', ['test@example.com']) == 'email'
    assert duckdb_service._infer_semantic_type('website_url', 'VARCHAR', ['https://example.com']) == 'url'
    assert duckdb_service._infer_semantic_type('created_at', 'TIMESTAMP', []) == 'datetime'
    assert duckdb_service._infer_semantic_type('total_price', 'DECIMAL', []) == 'currency'
    assert duckdb_service._infer_semantic_type('user_id', 'INTEGER', []) == 'id'


def test_searchability_score(duckdb_service, sample_csv_with_types):
    """Test searchability score calculation."""
    searchability = duckdb_service.calculate_searchability_score(sample_csv_with_types)
    
    assert 'score' in searchability
    assert 'grade' in searchability
    assert 'factors' in searchability
    assert 0 <= searchability['score'] <= 100
    assert searchability['grade'] in ['A', 'B', 'C', 'D', 'F']


def test_enhanced_metadata(duckdb_service, sample_csv_with_types):
    """Test comprehensive metadata extraction."""
    metadata = duckdb_service.get_enhanced_metadata(sample_csv_with_types)
    
    assert 'row_count' in metadata
    assert 'column_count' in metadata
    assert 'column_profiles' in metadata
    assert 'searchability' in metadata
    assert 'estimated_memory_mb' in metadata
    
    assert metadata['row_count'] == 5
    assert metadata['column_count'] == 6
    assert len(metadata['column_profiles']) == 6
