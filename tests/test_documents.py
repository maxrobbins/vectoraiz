import pytest
from pathlib import Path

from app.services.document_service import DocumentService


@pytest.fixture
def doc_service():
    """Create a document service instance."""
    return DocumentService()


def test_document_service_init(doc_service):
    """Test document service initializes with local processor."""
    assert doc_service.local_processor is not None
    assert doc_service.premium_processor is None  # No API key configured


def test_supported_types(doc_service):
    """Test supported file types."""
    processor = doc_service.get_processor()
    supported = processor.supported_types()
    
    assert '.pdf' in supported
    assert '.docx' in supported
    assert '.pptx' in supported


def test_is_document_type(doc_service):
    """Test document type detection."""
    assert doc_service.is_document_type(Path("test.pdf")) == True
    assert doc_service.is_document_type(Path("test.docx")) == True
    assert doc_service.is_document_type(Path("test.csv")) == False


def test_is_spreadsheet_type(doc_service):
    """Test spreadsheet type detection."""
    assert doc_service.is_spreadsheet_type(Path("test.xlsx")) == True
    assert doc_service.is_spreadsheet_type(Path("test.xls")) == True
    assert doc_service.is_spreadsheet_type(Path("test.csv")) == False
