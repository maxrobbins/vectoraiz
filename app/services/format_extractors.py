"""
Lightweight format extractors — bridge between NativeFormatProcessor and
the streaming document pipeline (TextBlock format).

Converts the dict-based output of native_document_processors.py into
List[TextBlock] for use by StreamingDocumentProcessor in subprocess workers.
"""

import logging
from pathlib import Path
from typing import List

from app.services.streaming_processor import TextBlock

logger = logging.getLogger(__name__)

# File types handled by lightweight extractors (no Tika, no Unstructured)
EXTRACTABLE_TYPES = {
    'rtf', 'odt', 'ods', 'odp', 'epub',
    'eml', 'msg', 'mbox',
    'xml', 'rss',
    'ics', 'vcf',
}


def can_extract(file_type: str) -> bool:
    """Check if a file type is supported by lightweight extractors."""
    return file_type.lower() in EXTRACTABLE_TYPES


def extract_text_blocks(filepath: Path, file_type: str) -> List[TextBlock]:
    """Extract text blocks from a native format file.

    Returns List[TextBlock] compatible with StreamingDocumentProcessor output.
    Uses native_document_processors.py handlers under the hood.
    """
    from app.services.native_document_processors import NativeFormatProcessor

    processor = NativeFormatProcessor()
    result = processor.process(filepath)

    blocks = []
    for i, item in enumerate(result.get("text_content", [])):
        page_num = item.get("metadata", {}).get("page_number", 0)
        blocks.append(TextBlock(
            page_num=page_num + 1,
            text=item.get("text", ""),
            metadata=item.get("metadata", {}),
        ))

    # If no text was extracted, yield a single empty block so the caller
    # knows the file was processed (prevents silent empty-result confusion).
    if not blocks:
        blocks.append(TextBlock(
            page_num=1,
            text="",
            metadata={"empty": True},
        ))

    return blocks
