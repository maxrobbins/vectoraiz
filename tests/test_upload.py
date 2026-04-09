from fastapi.testclient import TestClient
import io

from app.main import app

client = TestClient(app)


def test_upload_csv():
    """Test uploading a CSV file."""
    csv_content = b"id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300"
    files = {"file": ("test_upload.csv", io.BytesIO(csv_content), "text/csv")}
    
    response = client.post("/api/datasets/upload", files=files)
    
    assert response.status_code == 202
    data = response.json()
    assert "dataset_id" in data
    assert data["status"] == "uploaded"
    assert data["filename"] == "test_upload.csv"


def test_upload_unsupported_type():
    """Test uploading an unsupported file type."""
    files = {"file": ("test.xyz", io.BytesIO(b"hello"), "application/octet-stream")}

    response = client.post("/api/datasets/upload", files=files)

    assert response.status_code in (400, 422)  # VectorAIzError VAI-ING-001 → 422


def test_list_datasets():
    """Test listing datasets."""
    response = client.get("/api/datasets/")
    
    assert response.status_code == 200
    data = response.json()
    assert "datasets" in data
    assert "count" in data


def test_get_nonexistent_dataset():
    """Test getting a dataset that doesn't exist."""
    response = client.get("/api/datasets/nonexistent123")
    
    assert response.status_code == 404
