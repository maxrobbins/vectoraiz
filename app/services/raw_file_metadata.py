"""
Raw File Metadata Extraction
============================

Format-specific metadata extraction for raw marketplace files.
"""

import asyncio
import logging
import mimetypes
import wave
from pathlib import Path
from typing import Any, Dict, Optional

from app.config import settings

logger = logging.getLogger(__name__)


class MetadataExtractor:
    """Extract structured metadata from raw files using format-specific strategies + allAI."""

    async def extract(self, raw_file) -> dict:
        """Dispatch to format-specific extractor based on MIME type."""
        self._validate_allowed_path(raw_file.file_path)
        mime = raw_file.mime_type or ""
        if mime.startswith("image/"):
            return await self._extract_image(raw_file)
        if mime == "application/pdf":
            return await self._extract_pdf(raw_file)
        if mime.startswith("audio/"):
            return await self._extract_audio(raw_file)
        return await self._extract_generic(raw_file)

    async def _extract_image(self, raw_file) -> dict:
        file_path = self._validate_allowed_path(raw_file.file_path)
        technical_metadata: Dict[str, Any] = {
            "mime_type": raw_file.mime_type,
            "size_bytes": raw_file.file_size_bytes,
        }
        capability_notes = []

        try:
            from PIL import Image, ExifTags  # type: ignore

            def _read_image() -> Dict[str, Any]:
                with Image.open(file_path) as image:
                    exif_raw = image.getexif() or {}
                    exif = {}
                    for key, value in exif_raw.items():
                        label = ExifTags.TAGS.get(key, str(key))
                        exif[label] = str(value)
                    return {
                        "dimensions": {"width": image.width, "height": image.height},
                        "format": image.format,
                        "mode": image.mode,
                        "exif": exif,
                    }

            technical_metadata.update(await asyncio.to_thread(_read_image))
        except ImportError:
            capability_notes.append("Pillow not available for image parsing")
        except Exception as exc:
            capability_notes.append(f"image parsing failed: {exc}")

        if capability_notes:
            technical_metadata["capability_notes"] = capability_notes

        prompt = (
            "Describe this image for a data marketplace listing. Include: subject matter, "
            "content type, potential use cases. Technical info: "
            f"{technical_metadata.get('dimensions', 'unknown dimensions')}, "
            f"{technical_metadata.get('format', raw_file.mime_type or 'unknown format')}, "
            f"{raw_file.file_size_bytes} bytes"
        )
        return await self._build_metadata(raw_file, prompt, technical_metadata, fallback_kind="image")

    async def _extract_pdf(self, raw_file) -> dict:
        file_path = self._validate_allowed_path(raw_file.file_path)
        technical_metadata: Dict[str, Any] = {
            "mime_type": raw_file.mime_type,
            "size_bytes": raw_file.file_size_bytes,
        }
        capability_notes = []
        sample_text = ""

        try:
            from pypdf import PdfReader  # type: ignore

            def _read_pdf() -> Dict[str, Any]:
                reader = PdfReader(file_path)
                info = reader.metadata or {}
                first_page = ""
                if reader.pages:
                    try:
                        first_page = reader.pages[0].extract_text() or ""
                    except Exception:
                        first_page = ""
                return {
                    "page_count": len(reader.pages),
                    "title": getattr(info, "title", None) or info.get("/Title"),
                    "author": getattr(info, "author", None) or info.get("/Author"),
                    "sample_text": first_page[:2000],
                }

            pdf_data = await asyncio.to_thread(_read_pdf)
            sample_text = pdf_data.pop("sample_text", "")
            technical_metadata.update(pdf_data)
        except ImportError:
            capability_notes.append("pypdf not available for PDF parsing")
        except Exception as exc:
            capability_notes.append(f"PDF parsing failed: {exc}")

        if capability_notes:
            technical_metadata["capability_notes"] = capability_notes

        prompt = (
            "Summarize this document for a data marketplace listing. Include: topic, key "
            "content areas, potential use cases. Technical info: "
            f"{technical_metadata.get('page_count', 'unknown')} pages, "
            f"{technical_metadata.get('title') or raw_file.filename}, "
            f"{technical_metadata.get('author') or 'unknown author'}"
        )
        if sample_text:
            prompt += f"\n\nExtracted text sample:\n{sample_text[:1000]}"
        return await self._build_metadata(raw_file, prompt, technical_metadata, fallback_kind="document")

    async def _extract_audio(self, raw_file) -> dict:
        file_path = self._validate_allowed_path(raw_file.file_path)
        technical_metadata: Dict[str, Any] = {
            "mime_type": raw_file.mime_type,
            "size_bytes": raw_file.file_size_bytes,
        }
        capability_notes = []

        mutagen_available = False
        try:
            from mutagen import File as MutagenFile  # type: ignore

            mutagen_available = True

            def _read_audio() -> Dict[str, Any]:
                audio = MutagenFile(file_path)
                if audio is None or audio.info is None:
                    return {}
                info = audio.info
                return {
                    "duration_seconds": getattr(info, "length", None),
                    "sample_rate_hz": getattr(info, "sample_rate", None),
                    "channels": getattr(info, "channels", None),
                    "bitrate": getattr(info, "bitrate", None),
                    "format": audio.mime[0] if getattr(audio, "mime", None) else None,
                }

            technical_metadata.update(await asyncio.to_thread(_read_audio))
        except ImportError:
            capability_notes.append("mutagen not available for broad audio parsing")
        except Exception as exc:
            capability_notes.append(f"audio parsing failed: {exc}")

        if not technical_metadata.get("duration_seconds"):
            wav_details = await self._extract_wave_fallback(str(file_path))
            if wav_details:
                technical_metadata.update({k: v for k, v in wav_details.items() if v is not None})
                if not mutagen_available:
                    capability_notes.append("used stdlib wave fallback for WAV metadata only")

        inferred_format = technical_metadata.get("format")
        if not inferred_format:
            inferred_format = file_path.suffix.lstrip(".").lower() or None
        if not inferred_format:
            inferred_format = mimetypes.guess_type(str(file_path))[0]
        if inferred_format:
            technical_metadata["format"] = inferred_format

        if capability_notes:
            technical_metadata["capability_notes"] = capability_notes

        prompt = (
            "Describe this audio content for a data marketplace listing. Include: content "
            "type, potential use cases. Technical info: "
            f"{technical_metadata.get('duration_seconds', 'unknown duration')}, "
            f"{technical_metadata.get('format', raw_file.mime_type or 'unknown format')}, "
            f"{technical_metadata.get('sample_rate_hz', 'unknown sample rate')}"
        )
        return await self._build_metadata(raw_file, prompt, technical_metadata, fallback_kind="audio")

    async def _extract_generic(self, raw_file) -> dict:
        file_path = self._validate_allowed_path(raw_file.file_path)
        sample = await self._read_text_sample(str(file_path))
        technical_metadata: Dict[str, Any] = {
            "mime_type": raw_file.mime_type,
            "size_bytes": raw_file.file_size_bytes,
            "sample_available": sample != "(binary file - no text preview available)",
        }
        prompt = (
            f"Based on filename '{raw_file.filename}' (MIME: {raw_file.mime_type or 'unknown'}, "
            f"{raw_file.file_size_bytes} bytes), suggest a title, description, and up to 5 tags "
            f"for a data marketplace listing. Sample content:\n{sample[:2048]}"
        )
        metadata = await self._build_metadata(raw_file, prompt, technical_metadata, fallback_kind="file")
        metadata["preview_snippet"] = sample[:500]
        return metadata

    def _validate_allowed_path(self, file_path: str) -> Path:
        resolved_path = Path(file_path).expanduser().resolve(strict=True)
        allowed_dirs = [
            Path(allowed_dir).expanduser().resolve(strict=False)
            for allowed_dir in settings.allowed_raw_file_dirs
        ]

        for allowed_dir in allowed_dirs:
            if resolved_path == allowed_dir or resolved_path.is_relative_to(allowed_dir):
                return resolved_path

        allowed_display = ", ".join(str(path) for path in allowed_dirs) or "(none configured)"
        raise ValueError(
            f"File path '{resolved_path}' is outside allowed raw file directories: {allowed_display}"
        )

    async def _read_text_sample(self, file_path: str) -> str:
        def _read() -> str:
            try:
                with open(file_path, "r", errors="replace") as handle:
                    return handle.read(4096)
            except Exception:
                return "(binary file - no text preview available)"

        return await asyncio.to_thread(_read)

    async def _generate_ai_description(self, prompt: str) -> Optional[str]:
        try:
            from app.services.allai_service import get_allai_service

            allai = get_allai_service()
            if not allai or not hasattr(allai, "generate_completion"):
                return None

            result = allai.generate_completion(prompt)
            if asyncio.iscoroutine(result):
                result = await result
            return str(result) if result else None
        except Exception as exc:
            logger.warning("allAI metadata generation unavailable: %s", exc)
            return None

    async def _extract_wave_fallback(self, file_path: str) -> Dict[str, Any]:
        def _read_wave() -> Dict[str, Any]:
            try:
                with wave.open(file_path, "rb") as wav_file:
                    frame_rate = wav_file.getframerate()
                    frames = wav_file.getnframes()
                    duration = (frames / float(frame_rate)) if frame_rate else None
                    return {
                        "duration_seconds": duration,
                        "sample_rate_hz": frame_rate,
                        "channels": wav_file.getnchannels(),
                        "sample_width_bytes": wav_file.getsampwidth(),
                        "format": "wav",
                    }
            except Exception:
                return {}

        return await asyncio.to_thread(_read_wave)

    async def _build_metadata(
        self,
        raw_file,
        prompt: str,
        technical_metadata: Dict[str, Any],
        fallback_kind: str,
    ) -> Dict[str, Any]:
        description = await self._generate_ai_description(prompt)
        if description:
            return {
                "title": Path(raw_file.filename).stem or raw_file.filename,
                "description": description,
                "tags": self._build_default_tags(raw_file),
                "technical_metadata": technical_metadata,
                "source": "allai",
            }

        return {
            "title": Path(raw_file.filename).stem or raw_file.filename,
            "description": (
                f"Raw {fallback_kind} file for marketplace listing: {raw_file.filename} "
                f"({raw_file.mime_type or 'unknown type'})"
            ),
            "tags": self._build_default_tags(raw_file),
            "technical_metadata": technical_metadata,
            "source": "stub",
        }

    def _build_default_tags(self, raw_file) -> list:
        tags = []
        if raw_file.mime_type:
            tags.append(raw_file.mime_type.split("/")[-1])

        suffix = Path(raw_file.filename).suffix.lstrip(".").lower()
        if suffix and suffix not in tags:
            tags.append(suffix)

        return tags[:5]
