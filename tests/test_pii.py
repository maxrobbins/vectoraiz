import pytest
import csv

from fastapi.testclient import TestClient
from app.main import app
from app.services.pii_service import PIIService
from app.services.duckdb_service import get_duckdb_service

client = TestClient(app)


@pytest.fixture
def pii_service():
    """Create PII service instance."""
    return PIIService()


@pytest.fixture
def dataset_with_pii(tmp_path):
    """Create a dataset containing PII for testing."""
    csv_file = tmp_path / "pii_test.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'email', 'phone', 'ssn', 'notes'])
        writer.writerow([1, 'John Smith', 'john.smith@email.com', '555-123-4567', '123-45-6789', 'Regular customer'])
        writer.writerow([2, 'Jane Doe', 'jane.doe@company.org', '(555) 987-6543', '987-65-4321', 'VIP member'])
        writer.writerow([3, 'Bob Wilson', 'bob@test.net', '555.456.7890', '456-78-9012', 'New signup'])
    
    # Convert to parquet
    duckdb = get_duckdb_service()
    parquet_path = tmp_path / "pii_test.parquet"
    duckdb.connection.execute(f"""
        COPY (SELECT * FROM read_csv_auto('{csv_file}'))
        TO '{parquet_path}' (FORMAT PARQUET)
    """)
    
    return parquet_path


@pytest.fixture
def clean_dataset(tmp_path):
    """Create a dataset without PII."""
    csv_file = tmp_path / "clean_test.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['product_id', 'product_name', 'price', 'category'])
        writer.writerow([1, 'Widget A', 29.99, 'Electronics'])
        writer.writerow([2, 'Gadget B', 49.99, 'Electronics'])
        writer.writerow([3, 'Tool C', 19.99, 'Hardware'])
    
    duckdb = get_duckdb_service()
    parquet_path = tmp_path / "clean_test.parquet"
    duckdb.connection.execute(f"""
        COPY (SELECT * FROM read_csv_auto('{csv_file}'))
        TO '{parquet_path}' (FORMAT PARQUET)
    """)
    
    return parquet_path


def test_scan_text_with_email(pii_service):
    """Test email detection in text."""
    text = "Contact me at john.doe@example.com"
    results = pii_service.scan_text(text)
    
    assert len(results) > 0
    entity_types = [r.entity_type for r in results]
    assert "EMAIL_ADDRESS" in entity_types


def test_scan_text_with_phone(pii_service):
    """Test phone number detection."""
    # Use a more realistic looking number
    text = "Call me at 212-555-1234"
    results = pii_service.scan_text(text, score_threshold=0.3)
    
    assert len(results) > 0
    entity_types = [r.entity_type for r in results]
    assert "PHONE_NUMBER" in entity_types


def test_scan_text_with_ssn(pii_service):
    """Test SSN detection."""
    # Use a format that matches regex
    text = "My SSN is 111-22-3344"
    results = pii_service.scan_text(text, score_threshold=0.3)
    
    assert len(results) > 0
    entity_types = [r.entity_type for r in results]
    assert "US_SSN" in entity_types


def test_scan_clean_text(pii_service):
    """Test that clean text returns no PII."""
    text = "The product costs $29.99 and is available in blue."
    results = pii_service.scan_text(text, score_threshold=0.7)
    
    # Should have minimal or no high-confidence PII matches
    high_confidence = [r for r in results if r.score >= 0.7]
    assert len(high_confidence) == 0


def test_scan_dataset_with_pii(pii_service, dataset_with_pii):
    """Test dataset scanning detects PII."""
    result = pii_service.scan_dataset(dataset_with_pii)
    
    assert result["columns_with_pii"] > 0
    assert result["overall_risk"] in ["low", "medium", "high"]
    
    # Should detect PII in email, phone, ssn columns
    # Service returns 'column_results' with 'column' key per entry
    pii_columns = [f["column"] for f in result["column_results"]]
    assert any("email" in col.lower() for col in pii_columns)


def test_scan_clean_dataset(pii_service, clean_dataset):
    """Test clean dataset has no PII."""
    result = pii_service.scan_dataset(clean_dataset)
    
    # Clean dataset should have minimal or no PII
    assert result["overall_risk"] in ["none", "low"]


def test_list_entities_endpoint():
    """Test entity listing endpoint."""
    response = client.get("/api/pii/entities")
    assert response.status_code == 200
    data = response.json()
    assert "entities" in data
    assert len(data["entities"]) > 0


def test_analyze_text_endpoint():
    """Test text analysis endpoint."""
    response = client.post(
        "/api/pii/analyze-text",
        params={"text": "Email me at test@example.com"}
    )
    assert response.status_code == 200
    data = response.json()
    assert "findings" in data
    assert data["pii_found"] == True


def test_analyze_empty_text():
    """Test empty text handling."""
    response = client.post("/api/pii/analyze-text", params={"text": ""})
    assert response.status_code == 400


def test_recommendations(pii_service, dataset_with_pii):
    """Test recommendation generation."""
    result = pii_service.scan_dataset(dataset_with_pii)
    recommendations = pii_service.get_recommendations(result)
    
    assert len(recommendations) > 0
    assert all("severity" in r for r in recommendations)
    assert all("message" in r for r in recommendations)
