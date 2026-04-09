from fastapi.testclient import TestClient

from app.main import app
from app.services.qdrant_service import QdrantService, VECTOR_SIZE

client = TestClient(app)


def test_vector_health_endpoint():
    """Test vector health check endpoint."""
    response = client.get("/api/vectors/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "timestamp" in data


def test_list_collections_endpoint():
    """Test listing collections endpoint."""
    response = client.get("/api/vectors/collections")
    assert response.status_code == 200
    data = response.json()
    assert "collections" in data
    assert "count" in data


def test_create_and_delete_collection():
    """Test creating and deleting a collection."""
    test_collection = "test_collection_xyz"
    
    # Create collection
    response = client.post(f"/api/vectors/collections/{test_collection}")
    assert response.status_code == 200
    data = response.json()
    assert "collection" in data
    
    # Verify it exists
    response = client.get(f"/api/vectors/collections/{test_collection}")
    assert response.status_code == 200
    
    # Get count
    response = client.get(f"/api/vectors/collections/{test_collection}/count")
    assert response.status_code == 200
    assert response.json()["vector_count"] == 0
    
    # Delete collection
    response = client.delete(f"/api/vectors/collections/{test_collection}")
    assert response.status_code == 200
    
    # Verify it's gone
    response = client.get(f"/api/vectors/collections/{test_collection}")
    assert response.status_code == 404


def test_qdrant_service_direct():
    """Test QdrantService directly."""
    service = QdrantService()
    
    # Health check
    health = service.health_check()
    assert health["status"] == "healthy"
    
    # Create test collection
    test_name = "direct_test_collection"
    
    try:
        # Create
        info = service.create_collection(test_name)
        assert info["name"] == test_name
        assert info["vectors_count"] == 0
        
        # Check exists
        assert service.collection_exists(test_name) == True
        
        # Insert test vectors
        test_vectors = [[0.1] * VECTOR_SIZE, [0.2] * VECTOR_SIZE]
        test_payloads = [
            {"row_index": 0, "text_content": "hello world"},
            {"row_index": 1, "text_content": "goodbye world"},
        ]
        
        result = service.upsert_vectors(test_name, test_vectors, test_payloads)
        assert result["upserted"] == 2
        
        # Search
        results = service.search(test_name, [0.1] * VECTOR_SIZE, limit=5)
        assert len(results) > 0
        assert "score" in results[0]
        assert "payload" in results[0]
        
    finally:
        # Cleanup
        service.delete_collection(test_name)


def test_vector_size_constant():
    """Verify vector size matches embedding model."""
    assert VECTOR_SIZE == 384  # all-MiniLM-L6-v2 dimension
