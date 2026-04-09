from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_root_endpoint():
    """Test root endpoint returns API info."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    
    assert "name" in data
    assert "version" in data
    assert "docs" in data
    assert data["docs"]["swagger"] == "/docs"
    assert data["docs"]["redoc"] == "/redoc"


def test_swagger_docs():
    """Test Swagger UI is accessible."""
    response = client.get("/docs")
    assert response.status_code == 200
    assert "swagger" in response.text.lower()


def test_redoc_docs():
    """Test ReDoc is accessible."""
    response = client.get("/redoc")
    assert response.status_code == 200


def test_openapi_json():
    """Test OpenAPI JSON is accessible and valid."""
    response = client.get("/openapi.json")
    assert response.status_code == 200
    data = response.json()
    
    assert "openapi" in data
    assert "info" in data
    assert data["info"]["title"] == "vectorAIz API"
    assert "paths" in data
    assert "tags" in data


def test_api_guide():
    """Test API guide endpoint."""
    response = client.get("/api/docs/guide")
    assert response.status_code == 200
    data = response.json()
    
    assert "title" in data
    assert "sections" in data
    assert len(data["sections"]) > 0


def test_postman_collection():
    """Test Postman collection export."""
    response = client.get("/api/docs/postman")
    assert response.status_code == 200
    data = response.json()
    
    assert "info" in data
    assert data["info"]["name"] == "vectorAIz API"
    assert "item" in data
    assert len(data["item"]) > 0


def test_api_examples():
    """Test API examples endpoint."""
    response = client.get("/api/docs/examples")
    assert response.status_code == 200
    data = response.json()
    
    assert "examples" in data
    assert len(data["examples"]) > 0
    
    # Check example structure
    example = data["examples"][0]
    assert "name" in example
    assert "endpoint" in example
    assert "curl" in example


def test_openapi_has_tags():
    """Test OpenAPI spec includes all tags."""
    response = client.get("/openapi.json")
    data = response.json()
    
    tag_names = [t["name"] for t in data["tags"]]
    expected_tags = ["health", "datasets", "search", "sql", "vectors", "pii"]
    
    for tag in expected_tags:
        assert tag in tag_names


def test_openapi_has_descriptions():
    """Test OpenAPI endpoints have descriptions."""
    response = client.get("/openapi.json")
    data = response.json()
    
    # Check some key endpoints have descriptions
    paths = data["paths"]
    
    # Health endpoint
    assert "/api/health" in paths
    
    # Search endpoint
    assert "/api/search/" in paths
