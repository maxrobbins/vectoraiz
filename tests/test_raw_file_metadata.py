"""
Tests for raw file metadata extraction.
=======================================
"""

import asyncio
import wave
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image
from pypdf import PdfWriter
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.config import settings
from app.routers.raw_listings import router as raw_listings_router
from app.services.raw_file_metadata import MetadataExtractor
from app.services.raw_file_service import RawFileService


def _make_raw_file(path: Path, mime_type: str):
    return SimpleNamespace(
        id="raw-file-1",
        filename=path.name,
        file_path=str(path),
        file_size_bytes=path.stat().st_size,
        mime_type=mime_type,
    )


@pytest.fixture
def allow_tmp_raw_dirs(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "allowed_raw_file_dirs", [str(tmp_path)])
    return tmp_path


class TestMetadataExtractorRouting:
    def test_extract_routes_by_mime_type(self, allow_tmp_raw_dirs, monkeypatch):
        extractor = MetadataExtractor()
        path = allow_tmp_raw_dirs / "sample.bin"
        path.write_bytes(b"data")
        raw_file = _make_raw_file(path, "image/png")
        calls = []

        async def _image(_raw_file):
            calls.append("image")
            return {"source": "image"}

        async def _pdf(_raw_file):
            calls.append("pdf")
            return {"source": "pdf"}

        async def _audio(_raw_file):
            calls.append("audio")
            return {"source": "audio"}

        async def _generic(_raw_file):
            calls.append("generic")
            return {"source": "generic"}

        monkeypatch.setattr(extractor, "_extract_image", _image)
        monkeypatch.setattr(extractor, "_extract_pdf", _pdf)
        monkeypatch.setattr(extractor, "_extract_audio", _audio)
        monkeypatch.setattr(extractor, "_extract_generic", _generic)

        assert asyncio.run(extractor.extract(raw_file)) == {"source": "image"}
        raw_file.mime_type = "application/pdf"
        assert asyncio.run(extractor.extract(raw_file)) == {"source": "pdf"}
        raw_file.mime_type = "audio/wav"
        assert asyncio.run(extractor.extract(raw_file)) == {"source": "audio"}
        raw_file.mime_type = "application/octet-stream"
        assert asyncio.run(extractor.extract(raw_file)) == {"source": "generic"}
        assert calls == ["image", "pdf", "audio", "generic"]


class TestMetadataExtractorFormats:
    def test_extract_image_returns_expected_structure(self, allow_tmp_raw_dirs, monkeypatch):
        path = allow_tmp_raw_dirs / "sample.png"
        Image.new("RGB", (10, 20), color="red").save(path)
        raw_file = _make_raw_file(path, "image/png")
        extractor = MetadataExtractor()

        async def _fake_ai(prompt: str):
            assert "Describe this image for a data marketplace listing" in prompt
            return "A red sample image."

        monkeypatch.setattr(extractor, "_generate_ai_description", _fake_ai)

        metadata = asyncio.run(extractor._extract_image(raw_file))

        assert metadata["title"] == "sample"
        assert metadata["description"] == "A red sample image."
        assert metadata["source"] == "allai"
        assert metadata["tags"] == ["png"]
        assert metadata["technical_metadata"]["dimensions"] == {"width": 10, "height": 20}
        assert metadata["technical_metadata"]["format"] == "PNG"

    def test_extract_pdf_returns_expected_structure(self, allow_tmp_raw_dirs, monkeypatch):
        path = allow_tmp_raw_dirs / "sample.pdf"
        writer = PdfWriter()
        writer.add_blank_page(width=72, height=72)
        writer.add_metadata({"/Title": "Sample PDF", "/Author": "VectorAIz"})
        with path.open("wb") as handle:
            writer.write(handle)

        raw_file = _make_raw_file(path, "application/pdf")
        extractor = MetadataExtractor()

        async def _fake_ai(prompt: str):
            assert "Summarize this document for a data marketplace listing" in prompt
            return "A one-page PDF document."

        monkeypatch.setattr(extractor, "_generate_ai_description", _fake_ai)

        metadata = asyncio.run(extractor._extract_pdf(raw_file))

        assert metadata["title"] == "sample"
        assert metadata["description"] == "A one-page PDF document."
        assert metadata["source"] == "allai"
        assert metadata["technical_metadata"]["page_count"] == 1
        assert metadata["technical_metadata"]["title"] == "Sample PDF"
        assert metadata["technical_metadata"]["author"] == "VectorAIz"

    def test_extract_audio_gracefully_degrades_without_mutagen(self, allow_tmp_raw_dirs, monkeypatch):
        path = allow_tmp_raw_dirs / "sample.wav"
        with wave.open(str(path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(8000)
            wav_file.writeframes(b"\x00\x00" * 8000)

        raw_file = _make_raw_file(path, "audio/wav")
        extractor = MetadataExtractor()
        real_import = __import__

        def _fake_import(name, *args, **kwargs):
            if name == "mutagen":
                raise ImportError("mutagen unavailable")
            return real_import(name, *args, **kwargs)

        async def _fake_ai(prompt: str):
            assert "Describe this audio content for a data marketplace listing" in prompt
            return "A short WAV recording."

        monkeypatch.setattr("builtins.__import__", _fake_import)
        monkeypatch.setattr(extractor, "_generate_ai_description", _fake_ai)

        metadata = asyncio.run(extractor._extract_audio(raw_file))

        assert metadata["source"] == "allai"
        assert metadata["technical_metadata"]["format"] == "wav"
        assert metadata["technical_metadata"]["sample_rate_hz"] == 8000
        assert metadata["technical_metadata"]["channels"] == 1
        assert metadata["technical_metadata"]["duration_seconds"] == 1.0
        assert "capability_notes" in metadata["technical_metadata"]

    def test_extract_generic_returns_preview(self, allow_tmp_raw_dirs, monkeypatch):
        path = allow_tmp_raw_dirs / "sample.txt"
        path.write_text("hello world\n" * 20)
        raw_file = _make_raw_file(path, "text/plain")
        extractor = MetadataExtractor()

        async def _fake_ai(prompt: str):
            assert "Based on filename 'sample.txt'" in prompt
            return "Plain text sample."

        monkeypatch.setattr(extractor, "_generate_ai_description", _fake_ai)

        metadata = asyncio.run(extractor._extract_generic(raw_file))

        assert metadata["source"] == "allai"
        assert metadata["preview_snippet"].startswith("hello world")
        assert metadata["technical_metadata"]["sample_available"] is True

    def test_extract_image_gracefully_degrades_when_pillow_missing(self, allow_tmp_raw_dirs, monkeypatch):
        path = allow_tmp_raw_dirs / "fallback.png"
        path.write_bytes(b"\x89PNG\r\n\x1a\nfallback")
        raw_file = _make_raw_file(path, "image/png")
        extractor = MetadataExtractor()
        real_import = __import__

        def _fake_import(name, *args, **kwargs):
            if name == "PIL" or name.startswith("PIL."):
                raise ImportError("Pillow unavailable")
            return real_import(name, *args, **kwargs)

        async def _no_ai(_prompt: str):
            return None

        monkeypatch.setattr("builtins.__import__", _fake_import)
        monkeypatch.setattr(extractor, "_generate_ai_description", _no_ai)

        metadata = asyncio.run(extractor._extract_image(raw_file))

        assert isinstance(metadata, dict)
        assert metadata["source"] == "stub"
        assert metadata["technical_metadata"]["mime_type"] == "image/png"
        assert metadata["technical_metadata"]["size_bytes"] == path.stat().st_size
        assert "capability_notes" in metadata["technical_metadata"]
        assert any(
            "Pillow" in note for note in metadata["technical_metadata"]["capability_notes"]
        )
        assert metadata["tags"] == ["png"]

    def test_extract_pdf_gracefully_degrades_when_pypdf_missing(self, allow_tmp_raw_dirs, monkeypatch):
        path = allow_tmp_raw_dirs / "fallback.pdf"
        path.write_bytes(b"%PDF-1.4\n%fallback")
        raw_file = _make_raw_file(path, "application/pdf")
        extractor = MetadataExtractor()
        real_import = __import__

        def _fake_import(name, *args, **kwargs):
            if name == "pypdf":
                raise ImportError("pypdf unavailable")
            return real_import(name, *args, **kwargs)

        async def _no_ai(_prompt: str):
            return None

        monkeypatch.setattr("builtins.__import__", _fake_import)
        monkeypatch.setattr(extractor, "_generate_ai_description", _no_ai)

        metadata = asyncio.run(extractor._extract_pdf(raw_file))

        assert metadata["source"] == "stub"
        assert metadata["technical_metadata"]["mime_type"] == "application/pdf"
        assert "capability_notes" in metadata["technical_metadata"]

    def test_extract_raises_when_file_outside_allowed_dirs(self, tmp_path, monkeypatch):
        allowed_dir = tmp_path / "allowed"
        blocked_dir = tmp_path / "blocked"
        allowed_dir.mkdir()
        blocked_dir.mkdir()
        monkeypatch.setattr(settings, "allowed_raw_file_dirs", [str(allowed_dir)])

        path = blocked_dir / "outside.txt"
        path.write_text("outside allowed dirs")

        extractor = MetadataExtractor()
        raw_file = _make_raw_file(path, "text/plain")

        with pytest.raises(ValueError, match="outside allowed raw file directories"):
            asyncio.run(extractor.extract(raw_file))


class TestRawFileServiceMetadataPersistence:
    def test_generate_metadata_endpoint_persists_and_returns_metadata(self, tmp_path, monkeypatch):
        app = FastAPI()
        app.include_router(raw_listings_router, prefix="/api/raw")
        client = TestClient(app)

        sample = tmp_path / "endpoint.txt"
        sample.write_text("endpoint metadata test")
        registered = client.post("/api/raw/files", json={"file_path": str(sample)})
        file_id = registered.json()["id"]

        expected = {
            "title": "endpoint",
            "description": "Endpoint metadata",
            "tags": ["txt"],
            "technical_metadata": {"size_bytes": sample.stat().st_size},
            "source": "allai",
        }

        async def _fake_extract(self, stored_raw_file):
            assert stored_raw_file.id == file_id
            return expected

        monkeypatch.setattr(MetadataExtractor, "extract", _fake_extract)

        metadata_response = client.post(f"/api/raw/files/{file_id}/metadata")
        file_response = client.get(f"/api/raw/files/{file_id}")

        assert metadata_response.status_code == 200
        assert metadata_response.json()["auto_metadata"] == expected
        assert file_response.status_code == 200
        assert file_response.json()["metadata"] == expected

    def test_generate_metadata_stores_on_raw_file(self, tmp_path, monkeypatch):
        service = RawFileService()
        sample = tmp_path / "stored.txt"
        sample.write_text("stored metadata test")
        raw_file = service.register_file(str(sample))
        expected = {
            "title": "stored",
            "description": "Stored metadata",
            "tags": ["txt"],
            "technical_metadata": {"size_bytes": sample.stat().st_size},
            "source": "allai",
        }

        async def _fake_extract(self, stored_raw_file):
            assert stored_raw_file.id == raw_file.id
            return expected

        monkeypatch.setattr(MetadataExtractor, "extract", _fake_extract)

        metadata = asyncio.run(service.generate_metadata(raw_file.id))
        refreshed = service.get_file(raw_file.id)

        assert metadata == expected
        assert refreshed is not None
        assert refreshed.metadata_ == expected

    def test_generate_metadata_raises_for_missing_file(self):
        service = RawFileService()

        try:
            asyncio.run(service.generate_metadata("missing-id"))
        except FileNotFoundError as exc:
            assert "missing-id" in str(exc)
        else:
            raise AssertionError("Expected FileNotFoundError")
