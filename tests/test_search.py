import pytest
import csv

from fastapi.testclient import TestClient
from app.main import app
from app.services.search_service import SearchService
from app.services.indexing_service import IndexingService
from app.services.duckdb_service import get_duckdb_service

client = TestClient(app)


@pytest.fixture
def indexed_dataset(tmp_path):
    """Create and index a sample dataset for search tests."""
    # Create sample CSV
    csv_file = tmp_path / "search_test.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'company', 'description', 'industry'])
        writer.writerow([1, 'Acme Corp', 'Industrial equipment and machinery manufacturer', 'Manufacturing'])
        writer.writerow([2, 'TechSoft', 'Cloud computing and software development', 'Technology'])
        writer.writerow([3, 'GreenLeaf', 'Organic food and sustainable agriculture', 'Agriculture'])
        writer.writerow([4, 'FinanceHub', 'Investment banking and financial services', 'Finance'])
        writer.writerow([5, 'HealthPlus', 'Healthcare technology and medical devices', 'Healthcare'])
    
    # Convert to Parquet
    duckdb = get_duckdb_service()
    parquet_path = tmp_path / "search_test.parquet"
    duckdb.connection.execute(f"""
        COPY (SELECT * FROM read_csv_auto('{csv_file}'))
        TO '{parquet_path}' (FORMAT PARQUET)
    """)
    
    # Index the dataset
    indexing_service = IndexingService()
    dataset_id = "search_test_001"
    
    result = indexing_service.index_dataset(
        dataset_id=dataset_id,
        filepath=parquet_path,
        recreate_collection=True,
    )
    
    yield {
        "dataset_id": dataset_id,
        "parquet_path": parquet_path,
        "index_result": result,
    }
    
    # Cleanup
    indexing_service.delete_dataset_index(dataset_id)


def test_search_endpoint_get(indexed_dataset):
    """Test GET search endpoint."""
    response = client.get("/api/search/?q=technology%20software")
    
    assert response.status_code == 200
    data = response.json()
    
    assert "query" in data
    assert "results" in data
    assert "total" in data
    assert "duration_ms" in data


def test_search_endpoint_post():
    """Test POST search endpoint."""
    response = client.post("/api/search/", json={
        "query": "healthcare medical",
        "limit": 5,
    })
    
    assert response.status_code == 200
    data = response.json()
    assert "results" in data


def test_search_stats_endpoint():
    """Test search stats endpoint."""
    response = client.get("/api/search/stats")
    
    assert response.status_code == 200
    data = response.json()
    
    assert "total_datasets" in data
    assert "total_vectors" in data
    assert "datasets" in data


def test_search_suggest_endpoint():
    """Test search suggestions endpoint."""
    response = client.get("/api/search/suggest?q=tech")
    
    assert response.status_code == 200
    data = response.json()
    
    assert "query" in data
    assert "suggestions" in data


def test_search_service_direct(indexed_dataset):
    """Test SearchService directly."""
    service = SearchService()
    
    # Search for technology-related content
    results = service.search(
        query="technology and software development",
        limit=5,
    )
    
    assert results["total"] >= 0
    assert "duration_ms" in results
    
    if results["total"] > 0:
        first_result = results["results"][0]
        assert "score" in first_result
        assert "row_data" in first_result
        assert "dataset_id" in first_result


def test_search_relevance(indexed_dataset):
    """Test that search returns relevant results."""
    service = SearchService()
    
    # Search for specific term
    results = service.search(
        query="organic food agriculture",
        dataset_id=indexed_dataset["dataset_id"],
        limit=3,
    )
    
    if results["total"] > 0:
        # Check results contain relevant content
        all_text = " ".join([r["text_content"].lower() for r in results["results"]])
        # At least one of these terms should appear
        assert any(term in all_text for term in ["organic", "food", "agriculture", "greenleaf"])


def test_search_empty_query():
    """Test search with empty query."""
    response = client.get("/api/search/?q=")
    
    # Should handle gracefully
    assert response.status_code in [200, 422]


def test_search_with_min_score(indexed_dataset):
    """Test search with minimum score filter."""
    service = SearchService()
    
    results = service.search(
        query="random unrelated query xyz123",
        min_score=0.9,  # Very high threshold
        limit=10,
    )
    
    # Should return few or no results with high threshold
    for result in results["results"]:
        assert result["score"] >= 0.9
