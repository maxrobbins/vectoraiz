"""
Document processing service using Unstructured library.
Provides abstraction layer for local processing with optional upgrade path to paid API.

BQ-VZ-PERF Phase 3: Replaced Tika sidecar with NativeFormatProcessor (pure Python).
"""

import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from abc import ABC, abstractmethod
from datetime import datetime

from app.config import settings

logger = logging.getLogger(__name__)


class DocumentProcessor(ABC):
    """Abstract base class for document processors."""
    
    @abstractmethod
    def process(self, filepath: Path) -> Dict[str, Any]:
        """
        Process a document and return extracted content.
        
        Returns:
            {
                "text_content": [...],  # List of text blocks with metadata
                "tables": [...],        # List of extracted tables
                "metadata": {...},      # Document metadata
            }
        """
        pass
    
    @abstractmethod
    def supported_types(self) -> List[str]:
        """Return list of supported file extensions."""
        pass


class LocalDocumentProcessor(DocumentProcessor):
    """
    Local document processor using Unstructured open-source library.
    Handles PDF, Word, PowerPoint, and other document formats.
    """
    
    SUPPORTED_EXTENSIONS = {
        '.pdf', '.docx', '.doc', '.pptx', '.ppt', '.xlsx', '.xls',
        '.txt', '.md', '.html', '.htm',
        # Native lightweight extractors
        '.rtf', '.odt', '.ods', '.odp', '.epub',
        '.eml', '.msg', '.mbox',
        '.xml', '.rss', '.ics', '.vcf',
    }
    
    def __init__(self):
        self._check_dependencies()
    
    def _check_dependencies(self):
        """Verify required libraries are available."""
        try:
            from unstructured.partition.auto import partition
            self._partition = partition
        except ImportError:
            raise ImportError(
                "Unstructured library not found. Install with: pip install unstructured"
            )
    
    def supported_types(self) -> List[str]:
        return list(self.SUPPORTED_EXTENSIONS)
    
    def process(self, filepath: Path) -> Dict[str, Any]:
        """Process document using Unstructured library."""
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        ext = filepath.suffix.lower()
        if ext not in self.SUPPORTED_EXTENSIONS:
            raise ValueError(f"Unsupported file type: {ext}")
        
        # Use Unstructured to partition the document
        elements = self._partition(str(filepath))
        
        # Organize extracted content
        text_content = []
        tables = []
        
        for element in elements:
            element_dict = {
                "type": type(element).__name__,
                "text": str(element),
                "metadata": {}
            }
            
            # Extract metadata if available
            if hasattr(element, 'metadata'):
                meta = element.metadata
                if hasattr(meta, 'page_number'):
                    element_dict["metadata"]["page_number"] = meta.page_number
                if hasattr(meta, 'filename'):
                    element_dict["metadata"]["filename"] = meta.filename
                if hasattr(meta, 'coordinates'):
                    element_dict["metadata"]["has_coordinates"] = True
            
            # Separate tables from text
            if type(element).__name__ == "Table":
                tables.append({
                    "content": str(element),
                    "metadata": element_dict["metadata"]
                })
            else:
                text_content.append(element_dict)
        
        return {
            "text_content": text_content,
            "tables": tables,
            "metadata": {
                "filename": filepath.name,
                "file_type": ext[1:],  # Remove dot
                "element_count": len(elements),
                "text_blocks": len(text_content),
                "table_count": len(tables),
                "processed_at": datetime.utcnow().isoformat(),
                "processor": "local_unstructured"
            }
        }
    
    def extract_text_only(self, filepath: Path) -> str:
        """Extract just the text content as a single string."""
        result = self.process(filepath)
        text_parts = [item["text"] for item in result["text_content"]]
        return "\n\n".join(text_parts)
    
    def extract_tables_as_dicts(self, filepath: Path) -> List[Dict[str, Any]]:
        """Extract tables and attempt to parse them into structured data."""
        result = self.process(filepath)
        parsed_tables = []
        
        for i, table in enumerate(result["tables"]):
            parsed_tables.append({
                "table_index": i,
                "raw_content": table["content"],
                "metadata": table["metadata"]
            })
        
        return parsed_tables


class PremiumDocumentProcessor(DocumentProcessor):
    """
    Premium document processor using Unstructured hosted API.
    Provides better accuracy for complex layouts and scanned documents.
    """
    
    def __init__(self, api_key: str, api_url: str = "https://api.unstructured.io/general/v0/general"):
        self.api_key = api_key
        self.api_url = api_url
    
    def supported_types(self) -> List[str]:
        # Premium API supports more formats
        return ['.pdf', '.docx', '.doc', '.pptx', '.ppt', '.xlsx', '.xls', 
                '.jpg', '.jpeg', '.png', '.tiff', '.bmp', '.heic']
    
    def process(self, filepath: Path) -> Dict[str, Any]:
        """Process document using Unstructured hosted API."""
        import httpx
        
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        with open(filepath, 'rb') as f:
            files = {'files': (filepath.name, f)}
            headers = {'unstructured-api-key': self.api_key}
            
            response = httpx.post(
                self.api_url,
                files=files,
                headers=headers,
                timeout=300  # 5 minute timeout for large files
            )
        
        if response.status_code != 200:
            raise Exception(f"API error: {response.status_code} - {response.text}")
        
        elements = response.json()
        
        # Organize content similar to local processor
        text_content = []
        tables = []
        
        for element in elements:
            element_dict = {
                "type": element.get("type", "Unknown"),
                "text": element.get("text", ""),
                "metadata": element.get("metadata", {})
            }
            
            if element.get("type") == "Table":
                tables.append({
                    "content": element.get("text", ""),
                    "metadata": element.get("metadata", {})
                })
            else:
                text_content.append(element_dict)
        
        return {
            "text_content": text_content,
            "tables": tables,
            "metadata": {
                "filename": filepath.name,
                "file_type": filepath.suffix[1:],
                "element_count": len(elements),
                "text_blocks": len(text_content),
                "table_count": len(tables),
                "processed_at": datetime.utcnow().isoformat(),
                "processor": "premium_unstructured_api"
            }
        }


class DocumentService:
    """
    Main document service that manages processor selection.
    Uses local processor by default, premium if API key configured.
    NativeFormatProcessor handles formats previously routed to Tika.
    """

    def __init__(self):
        self.local_processor = None
        self.premium_processor = None
        self.native_processor = None
        self._init_processors()

    def _init_processors(self):
        """Initialize available processors."""
        # Always try to init local processor
        try:
            self.local_processor = LocalDocumentProcessor()
        except ImportError as e:
            print(f"Warning: Local document processor unavailable: {e}")

        # Init premium if API key available
        api_key = getattr(settings, 'unstructured_api_key', None)
        if api_key:
            self.premium_processor = PremiumDocumentProcessor(api_key)

        # BQ-VZ-PERF Phase 3: Native format processor (replaces Tika sidecar)
        from app.services.native_document_processors import NativeFormatProcessor
        self.native_processor = NativeFormatProcessor()

    def get_processor(
        self,
        prefer_premium: bool = False,
        *,
        filepath: Optional[Path] = None,
    ) -> DocumentProcessor:
        """Get the appropriate processor.

        Routes native-handled formats to NativeFormatProcessor.
        Keyword-only ``filepath`` param is backward-compatible.
        """
        ext = filepath.suffix.lower() if filepath else None
        file_type = ext[1:] if ext else None

        # Route native-handled formats
        if file_type and self.native_processor and file_type in self.native_processor.NATIVE_TYPES:
            return self.native_processor

        # Existing chain: premium → local
        if prefer_premium and self.premium_processor:
            return self.premium_processor
        if self.local_processor:
            return self.local_processor
        if self.premium_processor:
            return self.premium_processor

        raise RuntimeError("No document processor available")

    def process_document(
        self,
        filepath: Path,
        prefer_premium: bool = False
    ) -> Dict[str, Any]:
        """Process a document with the best available processor.

        Returns metadata-only on total failure (XAI recommendation).
        """
        processor = self.get_processor(prefer_premium, filepath=Path(filepath))
        try:
            return processor.process(Path(filepath))
        except Exception as e:
            # Graceful failure: return metadata-only (XAI recommendation)
            logger.error("All processors failed for %s: %s", filepath, e)
            return {
                "text_content": [],
                "tables": [],
                "metadata": {
                    "filename": Path(filepath).name,
                    "file_type": Path(filepath).suffix[1:],
                    "element_count": 0,
                    "text_blocks": 0,
                    "table_count": 0,
                    "processor": "failed",
                    "error": str(e),
                }
            }
    
    def is_document_type(self, filepath: Path) -> bool:
        """Check if file is a processable document type."""
        ext = filepath.suffix.lower()
        return ext in {'.pdf', '.docx', '.doc', '.pptx', '.ppt'}
    
    def is_spreadsheet_type(self, filepath: Path) -> bool:
        """Check if file is a spreadsheet type (handled differently)."""
        ext = filepath.suffix.lower()
        return ext in {'.xlsx', '.xls'}


# Singleton instance
_document_service: Optional[DocumentService] = None


def get_document_service() -> DocumentService:
    """Get the singleton document service instance."""
    global _document_service
    if _document_service is None:
        _document_service = DocumentService()
    return _document_service
