"""
Ephemeral storage for chat attachments.

Files live in /data/chat/ with a 1-hour TTL.
A background task cleans up expired attachments periodically.
Attachments are NOT datasets — they don't appear in the catalog.

Single-worker assumption: in-memory index + disk metadata.
If multi-worker needed, migrate to SQLite or Redis.

BQ-ALLAI-FILES (S135)
"""

import json
import logging
import os
import re
import shutil
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from PIL import Image, ImageOps

logger = logging.getLogger(__name__)

CHAT_UPLOAD_DIR = Path("/data/chat")
ATTACHMENT_TTL_SECONDS = 3600  # 1 hour
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
MAX_ATTACHMENTS_PER_MESSAGE = 3
MAX_IMAGE_DIMENSION = 1600  # px, longest side
MAX_POST_RESIZE_BYTES = 4 * 1024 * 1024  # 4MB

# Pillow decompression bomb guard (M8)
Image.MAX_IMAGE_PIXELS = 25_000_000  # 25MP

ALLOWED_MIME_TYPES = {
    # Images (passthrough to multimodal LLM)
    "image/png", "image/jpeg", "image/webp", "image/gif",
    # Documents (text extraction)
    "application/pdf",
    # Data files (schema + sample extraction)
    "text/csv", "application/json", "text/plain",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


def sanitize_filename(filename: str) -> str:
    """Sanitize user-provided filename for safe storage (M9)."""
    # Normalize unicode
    filename = unicodedata.normalize("NFKD", filename)
    # Strip path components (both unix and windows)
    filename = filename.replace("\\", "/").split("/")[-1]
    # Remove dangerous chars — keep word chars, spaces, hyphens, dots
    filename = re.sub(r"[^\w\s\-.]", "_", filename)
    # Prevent hidden files
    filename = filename.lstrip(".")
    # Truncate
    name, ext = os.path.splitext(filename)
    return f"{name[:100]}{ext[:10]}" or "upload"


def validate_image(file_path: Path) -> tuple[bool, str]:
    """Validate image is safe to process (M8)."""
    try:
        with Image.open(file_path) as img:
            img.verify()  # Check for corruption/bombs
        # Re-open after verify (verify invalidates the object) and force full decode
        with Image.open(file_path) as img:
            img.load()  # Force decode under PIL guards
            # Reject animated GIFs
            if getattr(img, "is_animated", False):
                return False, "Animated GIFs not supported"
            w, h = img.size
            if w * h > 25_000_000:
                return False, f"Image too large: {w}x{h} ({w * h:,} pixels, max 25M)"
        return True, ""
    except Image.DecompressionBombError as e:
        return False, f"Image too large (decompression bomb detected): {e}"
    except Exception as e:
        return False, f"Invalid image: {e}"


def resize_if_needed(file_path: Path, max_dimension: int = MAX_IMAGE_DIMENSION) -> bool:
    """Resize image in-place if longest side exceeds max. Returns True if resized (M12)."""
    try:
        with Image.open(file_path) as img:
            # Reject animated images early
            if getattr(img, "is_animated", False):
                raise ValueError("Animated images not supported")
            # Capture format before transpose (transpose returns new image without .format)
            fmt = img.format or "PNG"
            # EXIF orientation fix
            img = ImageOps.exif_transpose(img)
            w, h = img.size
            if max(w, h) <= max_dimension:
                return False
            ratio = max_dimension / max(w, h)
            new_size = (int(w * ratio), int(h * ratio))
            if fmt == "JPEG":
                img = img.convert("RGB")
                img = img.resize(new_size, Image.LANCZOS)
                img.save(file_path, format="JPEG", quality=85)
            elif fmt == "WEBP":
                img = img.resize(new_size, Image.LANCZOS)
                try:
                    img.save(file_path, format="WEBP")
                except Exception:
                    img.save(file_path, format="PNG")
            else:
                # PNG/GIF static
                if img.mode == "P":
                    img = img.convert("RGBA")
                img = img.resize(new_size, Image.LANCZOS)
                img.save(file_path, format="PNG")
            return True
    except Image.DecompressionBombError:
        raise ValueError("Image too large (decompression bomb detected)")


@dataclass
class ChatAttachment:
    id: str
    user_id: str  # AuthZ binding (M10)
    filename: str
    mime_type: str
    size_bytes: int
    type: str  # "image" | "document" | "data"
    file_path: Path
    created_at: float
    expires_at: float
    extracted_text: Optional[str] = None

    @property
    def is_expired(self) -> bool:
        return time.time() > self.expires_at

    def to_response_dict(self) -> dict:
        return {
            "id": self.id,
            "filename": self.filename,
            "mime_type": self.mime_type,
            "size_bytes": self.size_bytes,
            "type": self.type,
            "expires_at": datetime.fromtimestamp(
                self.expires_at, tz=timezone.utc
            ).isoformat().replace("+00:00", "Z"),
        }


class ChatAttachmentService:
    """Manage ephemeral chat file uploads."""

    def __init__(self, skip_init: bool = False) -> None:
        self._attachments: dict[str, ChatAttachment] = {}
        if not skip_init:
            try:
                CHAT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
                self._rebuild_index()
            except OSError:
                # /data may not be writable (e.g., macOS dev, tests)
                pass

    def _rebuild_index(self) -> None:
        """Scan /data/chat/ on startup and rebuild in-memory index from meta.json files."""
        if not CHAT_UPLOAD_DIR.exists():
            return
        now = time.time()
        for att_dir in CHAT_UPLOAD_DIR.iterdir():
            if not att_dir.is_dir():
                continue
            meta_path = att_dir / "meta.json"
            if not meta_path.exists():
                # Orphaned dir — clean up
                shutil.rmtree(att_dir, ignore_errors=True)
                continue
            try:
                meta = json.loads(meta_path.read_text())
                if meta.get("expires_at", 0) < now:
                    # Expired — clean up
                    shutil.rmtree(att_dir, ignore_errors=True)
                    continue
                file_path = att_dir / meta["filename"]
                if not file_path.exists():
                    shutil.rmtree(att_dir, ignore_errors=True)
                    continue
                att = ChatAttachment(
                    id=meta["id"],
                    user_id=meta["user_id"],
                    filename=meta["filename"],
                    mime_type=meta["mime_type"],
                    size_bytes=meta["size_bytes"],
                    type=meta["type"],
                    file_path=file_path,
                    created_at=meta["created_at"],
                    expires_at=meta["expires_at"],
                    extracted_text=meta.get("extracted_text"),
                )
                self._attachments[att.id] = att
            except Exception as e:
                logger.warning("Failed to rebuild attachment from %s: %s", att_dir, e)
                shutil.rmtree(att_dir, ignore_errors=True)

        logger.info("Rebuilt chat attachment index: %d active", len(self._attachments))

    async def store(
        self,
        attachment_id: str,
        user_id: str,
        filename: str,
        mime_type: str,
        file_path: Path,
        size_bytes: int,
    ) -> ChatAttachment:
        """Register an uploaded file as a chat attachment."""
        now = time.time()
        att_type = self._classify(mime_type)

        extracted_text = None
        if att_type in ("document", "data"):
            extracted_text = await self._extract_text(file_path, mime_type)

        attachment = ChatAttachment(
            id=attachment_id,
            user_id=user_id,
            filename=filename,
            mime_type=mime_type,
            size_bytes=size_bytes,
            type=att_type,
            file_path=file_path,
            created_at=now,
            expires_at=now + ATTACHMENT_TTL_SECONDS,
            extracted_text=extracted_text,
        )
        self._attachments[attachment_id] = attachment

        # Write meta.json atomically (write temp then rename)
        att_dir = file_path.parent
        meta = {
            "id": attachment.id,
            "user_id": attachment.user_id,
            "filename": attachment.filename,
            "mime_type": attachment.mime_type,
            "size_bytes": attachment.size_bytes,
            "type": attachment.type,
            "created_at": attachment.created_at,
            "expires_at": attachment.expires_at,
            "extracted_text": attachment.extracted_text,
        }
        meta_tmp = att_dir / "meta.json.tmp"
        meta_final = att_dir / "meta.json"
        meta_tmp.write_text(json.dumps(meta))
        meta_tmp.rename(meta_final)

        return attachment

    def get(self, attachment_id: str) -> Optional[ChatAttachment]:
        """Retrieve attachment by ID. Returns None if expired or missing."""
        att = self._attachments.get(attachment_id)
        if att is None:
            return None
        if att.is_expired:
            self._delete(attachment_id)
            return None
        return att

    def cleanup_expired(self) -> None:
        """Remove expired attachments from memory and disk."""
        expired = [aid for aid, a in self._attachments.items() if a.is_expired]
        for aid in expired:
            self._delete(aid)
        if expired:
            logger.info("Cleaned up %d expired chat attachments", len(expired))

    def _delete(self, attachment_id: str) -> None:
        att = self._attachments.pop(attachment_id, None)
        if att:
            att_dir = att.file_path.parent
            shutil.rmtree(att_dir, ignore_errors=True)

    @staticmethod
    def _classify(mime_type: str) -> str:
        if mime_type.startswith("image/"):
            return "image"
        if mime_type == "application/pdf":
            return "document"
        return "data"

    async def _extract_text(self, file_path: Path, mime_type: str) -> Optional[str]:
        """Extract text from document/data files. Cap at 10,000 chars."""
        try:
            if mime_type == "application/pdf":
                return await self._extract_pdf(file_path)

            if mime_type in ("text/csv", "text/plain"):
                text = file_path.read_text(errors="replace")
                lines = text.splitlines()
                if len(lines) > 50:
                    return "\n".join(lines[:50]) + f"\n... ({len(lines)} total lines)"
                return text[:10_000]

            if mime_type == "application/json":
                raw = file_path.read_text(errors="replace")[:10_000]
                try:
                    import json as json_mod
                    parsed = json_mod.loads(raw)
                    return json_mod.dumps(parsed, indent=2)[:10_000]
                except (json.JSONDecodeError, ValueError):
                    return raw

            if "spreadsheet" in mime_type:
                return await self._extract_spreadsheet(file_path)

        except Exception as e:
            logger.warning("Attachment text extraction failed: %.30s %s", file_path.name, e)
            return f"[Could not extract text from {file_path.name[:30]}]"

        return None

    async def _extract_pdf(self, file_path: Path) -> Optional[str]:
        """Extract text from PDF. Hard cap at 50KB text.

        Tries DocumentService first (Unstructured), falls back to pdfminer.six.
        """
        import asyncio

        # Try DocumentService (Unstructured) first
        try:
            from app.services.document_service import DocumentService
            doc_svc = DocumentService()
            result = await asyncio.get_event_loop().run_in_executor(
                None, doc_svc.process_document, file_path
            )
            text_blocks = result.get("text_content", [])
            full_text = "\n".join(
                block.get("text", "") if isinstance(block, dict) else str(block)
                for block in text_blocks
            )
            if full_text:
                return full_text[:50_000]
        except Exception as e:
            logger.info("DocumentService PDF extraction unavailable, trying pdfminer: %s", e)

        # Fallback: pdfminer.six
        try:
            from pdfminer.high_level import extract_text as pdfminer_extract

            text = await asyncio.get_event_loop().run_in_executor(
                None, pdfminer_extract, str(file_path)
            )
            return text[:50_000] if text else None
        except Exception as e:
            logger.warning("PDF extraction failed (pdfminer): %s", e)
            return "[Could not extract text from PDF]"

    async def _extract_spreadsheet(self, file_path: Path) -> Optional[str]:
        """Extract schema + sample from spreadsheet using DuckDB."""
        import asyncio
        try:
            from app.services.duckdb_service import DuckDBService
            duckdb_svc = DuckDBService()
            profile = await asyncio.get_event_loop().run_in_executor(
                None, duckdb_svc.get_column_profile, file_path
            )
            sample = await asyncio.get_event_loop().run_in_executor(
                None, duckdb_svc.get_sample_rows, file_path, 20
            )
            profile_str = json.dumps(profile, indent=2, default=str)[:5_000]
            sample_str = json.dumps(sample, indent=2, default=str)[:5_000]
            return f"Schema:\n{profile_str}\n\nSample rows:\n{sample_str}"[:10_000]
        except Exception as e:
            logger.warning("Spreadsheet extraction failed: %s", e)
            return f"[Could not extract data from {file_path.name[:30]}]"


# Module-level singleton
chat_attachment_service = ChatAttachmentService()
