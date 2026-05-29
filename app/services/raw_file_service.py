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
from pathlib import Path
from typing import List, Optional

from sqlmodel import select

from app.config import settings
from app.models.raw_file import RawFile
from app.services.raw_file_metadata import MetadataExtractor

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

    def get_import_directory(self) -> Path:
        """Return the directory where browser-uploaded raw files are persisted."""
        return Path(settings.raw_file_import_directory)

    def register_file(self, file_path: str, filename: Optional[str] = None) -> RawFile:
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
        stored_filename = filename or path.name

        raw_file = RawFile(
            id=str(uuid.uuid4()),
            filename=stored_filename,
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
                stored_filename, content_hash[:12], file_size,
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

    def list_files(self) -> List[RawFile]:
        """Return all registered raw files, newest first."""
        with _get_db_session() as session:
            return list(
                session.exec(
                    select(RawFile).order_by(RawFile.created_at.desc())
                ).all()
            )

    def delete_file(self, file_id: str) -> bool:
        """
        Delete a raw file record and optionally the file on disk.

        Returns True if deleted, False if not found.
        """
        with _get_db_session() as session:
            raw_file = session.exec(
                select(RawFile).where(RawFile.id == file_id)
            ).first()
            if raw_file is None:
                return False

            file_path = raw_file.file_path
            session.delete(raw_file)
            session.commit()

            # Remove from disk if it exists
            if Path(file_path).is_file():
                try:
                    os.remove(file_path)
                except OSError as e:
                    logger.warning("Could not remove file %s from disk: %s", file_path, e)

            logger.info("Deleted raw file %s (%s)", file_id, file_path)
            return True

    def update_file_metadata(self, file_id: str, metadata: dict) -> Optional[RawFile]:
        """Update the metadata JSON field on a raw file."""
        with _get_db_session() as session:
            raw_file = session.exec(
                select(RawFile).where(RawFile.id == file_id)
            ).first()
            if raw_file is None:
                return None

            from datetime import datetime, timezone
            raw_file.metadata_ = metadata
            raw_file.updated_at = datetime.now(timezone.utc)
            session.add(raw_file)
            session.commit()
            session.refresh(raw_file)
            logger.info("Updated metadata for raw file %s", file_id)
            return raw_file

    async def generate_metadata(self, file_id: str) -> dict:
        """
        Generate and persist structured metadata for a raw file.
        """
        raw_file = self.get_file(file_id)
        if raw_file is None:
            raise FileNotFoundError(f"Raw file not found: {file_id}")

        extractor = MetadataExtractor()
        metadata = await extractor.extract(raw_file)

        with _get_db_session() as session:
            stored_raw_file = session.exec(
                select(RawFile).where(RawFile.id == file_id)
            ).first()
            if stored_raw_file is None:
                raise FileNotFoundError(f"Raw file not found: {file_id}")

            stored_raw_file.metadata_ = metadata
            session.add(stored_raw_file)
            session.commit()

        logger.info("Generated %s metadata for file %s", metadata.get("source", "unknown"), file_id)
        return metadata


# Module-level singleton
_raw_file_service: Optional[RawFileService] = None


def get_raw_file_service() -> RawFileService:
    """Get or create singleton RawFileService."""
    global _raw_file_service
    if _raw_file_service is None:
        _raw_file_service = RawFileService()
    return _raw_file_service
