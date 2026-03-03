"""
Raw File Service
================

Handles registration, hashing, metadata generation, and serving of raw files
for marketplace listings.

Phase: BQ-VZ-RAW-LISTINGS
Created: 2026-03-03
"""

import hashlib
import logging
import mimetypes
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from sqlmodel import select

from app.models.raw_file import RawFile

logger = logging.getLogger(__name__)

# 8 KB chunks for streaming hash (per spec)
_HASH_CHUNK_SIZE = 8192


def _get_db_session():
    from app.core.database import get_session_context
    return get_session_context()


def _compute_sha256(file_path: str) -> str:
    """Compute SHA256 hex digest of a file using streaming 8KB chunks."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(_HASH_CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()


class RawFileService:
    """Manages raw file registration, lookup, and serving."""

    def register_file(self, file_path: str) -> RawFile:
        """
        Register a raw file: compute SHA256, detect MIME type, store metadata in DB.

        Args:
            file_path: Absolute path to the file on disk.

        Returns:
            RawFile record.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"File not found: {file_path}")

        content_hash = _compute_sha256(file_path)
        file_size = path.stat().st_size
        mime_type, _ = mimetypes.guess_type(file_path)
        filename = path.name

        raw_file = RawFile(
            id=str(uuid.uuid4()),
            filename=filename,
            file_path=str(path.resolve()),
            file_size_bytes=file_size,
            content_hash=content_hash,
            mime_type=mime_type,
        )

        with _get_db_session() as session:
            session.add(raw_file)
            session.commit()
            session.refresh(raw_file)
            logger.info(
                "Registered raw file: %s (hash=%s, size=%d)",
                filename, content_hash[:12], file_size,
            )
            return raw_file

    def get_file(self, file_id: str) -> Optional[RawFile]:
        """Look up a raw file by ID."""
        with _get_db_session() as session:
            return session.exec(
                select(RawFile).where(RawFile.id == file_id)
            ).first()

    def serve_file(self, file_id: str, expected_hash: str) -> RawFile:
        """
        Verify content hash and return the RawFile for streaming.

        Args:
            file_id: UUID of the raw file.
            expected_hash: SHA256 hash the entitlement token expects.

        Returns:
            RawFile record (caller streams via FileResponse).

        Raises:
            FileNotFoundError: If file_id not found.
            ValueError: If content_hash does not match expected_hash.
        """
        raw_file = self.get_file(file_id)
        if raw_file is None:
            raise FileNotFoundError(f"Raw file not found: {file_id}")

        if raw_file.content_hash != expected_hash:
            raise ValueError("File has changed since listing. Contact seller.")

        if not Path(raw_file.file_path).is_file():
            raise FileNotFoundError(f"File missing from disk: {raw_file.file_path}")

        return raw_file

    def generate_metadata(self, file_id: str) -> dict:
        """
        Use allAI copilot to auto-describe a file.

        Extracts first 4KB of the file and sends to allAI for description.
        Returns dict with title, description, tags, preview_snippet.

        Falls back to a basic stub if allAI is unavailable.
        """
        raw_file = self.get_file(file_id)
        if raw_file is None:
            raise FileNotFoundError(f"Raw file not found: {file_id}")

        # Extract sample for allAI
        sample = ""
        try:
            with open(raw_file.file_path, "r", errors="replace") as f:
                sample = f.read(4096)
        except Exception:
            sample = "(binary file — no text preview available)"

        # Try allAI copilot
        try:
            from app.services.allai_service import get_allai_service
            allai = get_allai_service()
            if allai and hasattr(allai, "generate_completion"):
                prompt = (
                    "Describe this dataset for a marketplace listing. "
                    "Generate: title (short), one-paragraph description, "
                    "up to 5 tags as a list, and a preview snippet.\n\n"
                    f"Filename: {raw_file.filename}\n"
                    f"MIME type: {raw_file.mime_type}\n"
                    f"Size: {raw_file.file_size_bytes} bytes\n\n"
                    f"Sample:\n{sample[:2048]}"
                )
                result = allai.generate_completion(prompt)
                if result:
                    metadata = {
                        "title": raw_file.filename,
                        "description": str(result),
                        "tags": [],
                        "preview_snippet": sample[:500],
                        "source": "allai",
                    }
                    logger.info("Generated allAI metadata for file %s", file_id)
                    return metadata
        except Exception as e:
            logger.warning("allAI auto-describe unavailable, using stub: %s", e)

        # Stub fallback
        metadata = {
            "title": raw_file.filename,
            "description": f"Raw data file: {raw_file.filename} ({raw_file.mime_type or 'unknown type'})",
            "tags": [raw_file.mime_type.split("/")[-1]] if raw_file.mime_type else [],
            "preview_snippet": sample[:500],
            "source": "stub",
        }
        logger.info("Generated stub metadata for file %s", file_id)
        return metadata


# Module-level singleton
_raw_file_service: Optional[RawFileService] = None


def get_raw_file_service() -> RawFileService:
    """Get or create singleton RawFileService."""
    global _raw_file_service
    if _raw_file_service is None:
        _raw_file_service = RawFileService()
    return _raw_file_service
