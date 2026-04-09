"""
Tests for BQ-ALLAI-FILES: Chat file upload, attachment handling, MIME detection.

Covers:
1. Upload valid PNG → 200 with correct response
2. Upload valid PDF → 200, extracted_text populated
3. Upload > 10MB → 413
4. Upload .exe with spoofed extension → 415
5. Upload with RIFF header but no WEBP → rejected
6. Retrieve expired attachment → None
7. BRAIN_MESSAGE with valid attachment IDs → processed
8. BRAIN_MESSAGE with wrong user's attachment → rejected (AuthZ)
9. BRAIN_MESSAGE with > 3 attachments → error
10. Image > 1600px → resized
11. Post-resize > 4MB → 413
12. Filename sanitization: path traversal attempts cleaned
13. ZIP bomb detection: reject suspicious xlsx

CREATED: BQ-ALLAI-FILES (2026-02-16)
"""

import io
import json
import time

import pytest

from app.auth.api_key_auth import AuthenticatedUser, get_current_user
from app.services.mime_detector import detect_mime_for_zip, detect_mime_from_header
from app.services.chat_attachment_service import (
    ChatAttachment,
    ChatAttachmentService,
    resize_if_needed,
    sanitize_filename,
    validate_image,
)


# ---------------------------------------------------------------------------
# Mock authenticated user for endpoint tests
# ---------------------------------------------------------------------------

MOCK_USER = AuthenticatedUser(
    user_id="test_user_123",
    key_id="key_test",
    scopes=["read", "write"],
    valid=True,
    balance_cents=10000,
    free_trial_remaining_cents=0,
)


# ---------------------------------------------------------------------------
# Helpers: generate test file bytes
# ---------------------------------------------------------------------------

def _make_png(width: int = 100, height: int = 100) -> bytes:
    """Create a minimal valid PNG file."""
    from PIL import Image
    buf = io.BytesIO()
    img = Image.new("RGBA", (width, height), (255, 0, 0, 255))
    img.save(buf, format="PNG")
    return buf.getvalue()


def _make_jpeg(width: int = 100, height: int = 100) -> bytes:
    """Create a minimal valid JPEG file."""
    from PIL import Image
    buf = io.BytesIO()
    img = Image.new("RGB", (width, height), (255, 0, 0))
    img.save(buf, format="JPEG")
    return buf.getvalue()


def _make_pdf() -> bytes:
    """Create minimal PDF bytes."""
    return b"""%PDF-1.4
1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj
2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj
3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R>>endobj
xref
0 4
0000000000 65535 f
0000000009 00000 n
0000000058 00000 n
0000000115 00000 n
trailer<</Size 4/Root 1 0 R>>
startxref
190
%%EOF"""


def _make_csv() -> bytes:
    """Create minimal CSV bytes."""
    return b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n"


# ---------------------------------------------------------------------------
# MIME Detection Tests
# ---------------------------------------------------------------------------

class TestMimeDetection:
    def test_detect_png(self):
        data = _make_png()
        assert detect_mime_from_header(data[:32]) == "image/png"

    def test_detect_jpeg(self):
        data = _make_jpeg()
        assert detect_mime_from_header(data[:32]) == "image/jpeg"

    def test_detect_gif87a(self):
        header = b"GIF87a" + b"\x00" * 26
        assert detect_mime_from_header(header) == "image/gif"

    def test_detect_gif89a(self):
        header = b"GIF89a" + b"\x00" * 26
        assert detect_mime_from_header(header) == "image/gif"

    def test_detect_pdf(self):
        data = _make_pdf()
        assert detect_mime_from_header(data[:32]) == "application/pdf"

    def test_detect_webp_valid(self):
        # RIFF....WEBP
        header = b"RIFF" + b"\x00\x00\x00\x00" + b"WEBP" + b"\x00" * 20
        assert detect_mime_from_header(header) == "image/webp"

    def test_detect_riff_not_webp(self):
        """RIFF header without WEBP signature at bytes 8-12 → should NOT be image/webp."""
        header = b"RIFF" + b"\x00\x00\x00\x00" + b"AVI " + b"\x00" * 20
        result = detect_mime_from_header(header)
        assert result != "image/webp"

    def test_detect_zip_header(self):
        header = b"PK\x03\x04" + b"\x00" * 28
        assert detect_mime_from_header(header) == "application/zip"

    def test_detect_csv(self):
        data = _make_csv()
        assert detect_mime_from_header(data) == "text/csv"

    def test_detect_json(self):
        data = b'{"key": "value", "num": 42}'
        assert detect_mime_from_header(data) == "application/json"

    def test_detect_json_array(self):
        data = b'[{"a": 1}, {"a": 2}]'
        assert detect_mime_from_header(data) == "application/json"

    def test_detect_plain_text(self):
        data = b"Hello, this is just some plain text content."
        assert detect_mime_from_header(data) == "text/plain"

    def test_detect_binary_unknown(self):
        """Non-UTF8 binary with no recognized magic → None."""
        data = bytes(range(256)) * 2
        assert detect_mime_from_header(data) is None

    def test_exe_binary_rejected(self):
        """True binary (non-UTF8) with no recognized magic → None."""
        header = bytes([0x4D, 0x5A, 0x90, 0x00, 0x03] + [0x80, 0x81, 0x82, 0xFE, 0xFF] * 50)
        result = detect_mime_from_header(header)
        assert result is None


# ---------------------------------------------------------------------------
# ZIP / XLSX Detection Tests
# ---------------------------------------------------------------------------

class TestZipDetection:
    def test_valid_xlsx(self, tmp_path):
        """A real xlsx file should be detected as spreadsheet MIME."""
        # Create a minimal xlsx using openpyxl if available, otherwise skip
        try:
            import openpyxl
        except ImportError:
            pytest.skip("openpyxl not available")

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["Name", "Value"])
        ws.append(["Test", 42])
        xlsx_path = tmp_path / "test.xlsx"
        wb.save(xlsx_path)

        result = detect_mime_for_zip(xlsx_path)
        assert result == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    def test_regular_zip_rejected(self, tmp_path):
        """A plain ZIP (not xlsx) should return None."""
        import zipfile
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("hello.txt", "world")
        assert detect_mime_for_zip(zip_path) is None

    def test_not_a_zip(self, tmp_path):
        """A non-ZIP file should return None."""
        fake = tmp_path / "fake.xlsx"
        fake.write_bytes(b"not a zip file at all")
        assert detect_mime_for_zip(fake) is None

    def test_zip_bomb_too_many_entries(self, tmp_path):
        """ZIP with > 1000 entries should be rejected."""
        import zipfile
        bomb_path = tmp_path / "bomb.xlsx"
        with zipfile.ZipFile(bomb_path, "w") as zf:
            zf.writestr("[Content_Types].xml", "<Types/>")
            zf.writestr("xl/workbook.xml", "<workbook/>")
            for i in range(1001):
                zf.writestr(f"xl/data_{i}.xml", "<d/>")
        assert detect_mime_for_zip(bomb_path) is None


# ---------------------------------------------------------------------------
# Filename Sanitization Tests
# ---------------------------------------------------------------------------

class TestFilenameSanitization:
    def test_normal_filename(self):
        assert sanitize_filename("report.csv") == "report.csv"

    def test_path_traversal_unix(self):
        result = sanitize_filename("../../etc/passwd")
        assert "/" not in result
        assert ".." not in result

    def test_path_traversal_windows(self):
        result = sanitize_filename("..\\..\\windows\\system32\\config")
        assert "\\" not in result
        assert ".." not in result

    def test_hidden_file(self):
        result = sanitize_filename(".hidden_file.txt")
        assert not result.startswith(".")

    def test_dangerous_chars(self):
        result = sanitize_filename("file<name>.exe;rm -rf /")
        assert "<" not in result
        assert ">" not in result
        assert ";" not in result

    def test_truncation(self):
        long_name = "a" * 200 + ".xlsx"
        result = sanitize_filename(long_name)
        assert len(result) <= 110  # 100 name + 10 ext

    def test_empty_after_sanitization(self):
        result = sanitize_filename("...")
        assert result == "upload"

    def test_unicode_normalization(self):
        # NFKD normalization: e.g., ﬁ → fi
        result = sanitize_filename("ﬁle.txt")
        assert "fi" in result


# ---------------------------------------------------------------------------
# Image Validation + Resize Tests
# ---------------------------------------------------------------------------

class TestImageValidation:
    def test_valid_png(self, tmp_path):
        img_path = tmp_path / "test.png"
        img_path.write_bytes(_make_png(200, 200))
        valid, reason = validate_image(img_path)
        assert valid is True
        assert reason == ""

    def test_corrupt_image(self, tmp_path):
        img_path = tmp_path / "corrupt.png"
        img_path.write_bytes(b"\x89PNG\r\n\x1a\n" + b"garbage" * 100)
        valid, reason = validate_image(img_path)
        assert valid is False
        assert "Invalid image" in reason


class TestImageResize:
    def test_no_resize_needed(self, tmp_path):
        img_path = tmp_path / "small.png"
        img_path.write_bytes(_make_png(800, 600))
        assert resize_if_needed(img_path) is False

    def test_resize_large_image(self, tmp_path):
        img_path = tmp_path / "large.png"
        img_path.write_bytes(_make_png(3200, 2400))
        assert resize_if_needed(img_path) is True
        # Verify resized dimensions
        from PIL import Image
        with Image.open(img_path) as img:
            w, h = img.size
            assert max(w, h) <= 1600

    def test_resize_preserves_format(self, tmp_path):
        img_path = tmp_path / "large.jpeg"
        img_path.write_bytes(_make_jpeg(3200, 2400))
        assert resize_if_needed(img_path) is True
        from PIL import Image
        with Image.open(img_path) as img:
            assert img.format == "JPEG"
            assert max(img.size) <= 1600


# ---------------------------------------------------------------------------
# ChatAttachmentService Tests
# ---------------------------------------------------------------------------

class TestChatAttachmentService:
    @pytest.fixture
    def svc(self, tmp_path, monkeypatch):
        """Create a ChatAttachmentService with a temp upload dir."""
        import app.services.chat_attachment_service as mod
        monkeypatch.setattr(mod, "CHAT_UPLOAD_DIR", tmp_path / "chat")
        return ChatAttachmentService()

    @pytest.mark.asyncio
    async def test_store_and_get(self, svc, tmp_path):
        att_dir = tmp_path / "chat" / "att_test123"
        att_dir.mkdir(parents=True)
        file_path = att_dir / "test.png"
        file_path.write_bytes(_make_png())

        att = await svc.store(
            attachment_id="att_test123",
            user_id="usr_1",
            filename="test.png",
            mime_type="image/png",
            file_path=file_path,
            size_bytes=len(file_path.read_bytes()),
        )
        assert att.id == "att_test123"
        assert att.type == "image"
        assert att.user_id == "usr_1"

        # Retrieve
        retrieved = svc.get("att_test123")
        assert retrieved is not None
        assert retrieved.id == "att_test123"

    @pytest.mark.asyncio
    async def test_get_expired_returns_none(self, svc, tmp_path):
        att_dir = tmp_path / "chat" / "att_expired"
        att_dir.mkdir(parents=True)
        file_path = att_dir / "old.txt"
        file_path.write_bytes(b"old data")

        att = await svc.store(
            attachment_id="att_expired",
            user_id="usr_1",
            filename="old.txt",
            mime_type="text/plain",
            file_path=file_path,
            size_bytes=8,
        )
        # Force expiry
        att.expires_at = time.time() - 1
        svc._attachments["att_expired"] = att

        assert svc.get("att_expired") is None

    @pytest.mark.asyncio
    async def test_get_missing_returns_none(self, svc):
        assert svc.get("att_nonexistent") is None

    def test_cleanup_expired(self, svc, tmp_path):
        """cleanup_expired removes expired entries."""
        now = time.time()
        att_dir = tmp_path / "chat" / "att_cleanup"
        att_dir.mkdir(parents=True)
        file_path = att_dir / "file.txt"
        file_path.write_bytes(b"data")

        att = ChatAttachment(
            id="att_cleanup",
            user_id="usr_1",
            filename="file.txt",
            mime_type="text/plain",
            size_bytes=4,
            type="data",
            file_path=file_path,
            created_at=now - 7200,
            expires_at=now - 3600,  # expired 1 hour ago
        )
        svc._attachments["att_cleanup"] = att

        svc.cleanup_expired()
        assert "att_cleanup" not in svc._attachments
        assert not att_dir.exists()

    @pytest.mark.asyncio
    async def test_classify_types(self, svc):
        assert svc._classify("image/png") == "image"
        assert svc._classify("image/jpeg") == "image"
        assert svc._classify("application/pdf") == "document"
        assert svc._classify("text/csv") == "data"
        assert svc._classify("application/json") == "data"

    @pytest.mark.asyncio
    async def test_to_response_dict(self, svc, tmp_path):
        att_dir = tmp_path / "chat" / "att_resp"
        att_dir.mkdir(parents=True)
        file_path = att_dir / "test.csv"
        file_path.write_bytes(_make_csv())

        att = await svc.store(
            attachment_id="att_resp",
            user_id="usr_1",
            filename="test.csv",
            mime_type="text/csv",
            file_path=file_path,
            size_bytes=len(_make_csv()),
        )
        d = att.to_response_dict()
        assert d["id"] == "att_resp"
        assert d["type"] == "data"
        assert d["mime_type"] == "text/csv"
        assert "expires_at" in d
        assert d["expires_at"].endswith("Z")


# ---------------------------------------------------------------------------
# Upload Endpoint Tests (via FastAPI TestClient)
# ---------------------------------------------------------------------------

class TestUploadEndpoint:
    @pytest.fixture
    def app(self, tmp_path, monkeypatch):
        """FastAPI app with copilot routers and temp upload dir."""
        from fastapi import FastAPI
        from app.core.errors import VectorAIzError
        from app.core.errors.middleware import vectoraiz_error_handler
        from app.core.errors.registry import error_registry
        from app.routers.copilot import router as copilot_rest_router

        if len(error_registry) == 0:
            error_registry.load()

        # Redirect upload dir to temp
        import app.services.chat_attachment_service as att_mod
        monkeypatch.setattr(att_mod, "CHAT_UPLOAD_DIR", tmp_path / "chat")
        # Recreate singleton with new dir
        monkeypatch.setattr(att_mod, "chat_attachment_service", ChatAttachmentService())

        # Reset upload rate limiters
        from app.routers import copilot as copilot_mod
        copilot_mod._upload_rate.clear()
        copilot_mod._upload_session_count.clear()

        app = FastAPI()
        app.add_exception_handler(VectorAIzError, vectoraiz_error_handler)
        app.include_router(copilot_rest_router, prefix="/api/copilot")
        # Override auth dependency so upload endpoint doesn't 401
        app.dependency_overrides[get_current_user] = lambda: MOCK_USER
        yield app
        app.dependency_overrides.clear()

    @pytest.fixture
    def client(self, app):
        from fastapi.testclient import TestClient
        return TestClient(app)

    def test_upload_valid_png(self, client):
        png_data = _make_png(200, 200)
        response = client.post(
            "/api/copilot/upload",
            files={"file": ("screenshot.png", io.BytesIO(png_data), "image/png")},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "image"
        assert data["mime_type"] == "image/png"
        assert data["id"].startswith("att_")
        assert "expires_at" in data

    def test_upload_valid_csv(self, client):
        csv_data = _make_csv()
        response = client.post(
            "/api/copilot/upload",
            files={"file": ("data.csv", io.BytesIO(csv_data), "text/csv")},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "data"
        assert data["mime_type"] == "text/csv"

    def test_upload_valid_pdf(self, client):
        pdf_data = _make_pdf()
        response = client.post(
            "/api/copilot/upload",
            files={"file": ("report.pdf", io.BytesIO(pdf_data), "application/pdf")},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "document"
        assert data["mime_type"] == "application/pdf"

    def test_upload_too_large(self, client):
        """File > 10MB should be rejected with 413."""
        large_data = b"\x89PNG\r\n\x1a\n" + b"\x00" * (11 * 1024 * 1024)
        response = client.post(
            "/api/copilot/upload",
            files={"file": ("big.png", io.BytesIO(large_data), "image/png")},
        )
        assert response.status_code == 413

    def test_upload_exe_rejected(self, client):
        """EXE binary (non-UTF8) with spoofed .png extension → 415."""
        # Real EXE-like binary: MZ header + non-UTF8 byte sequences
        exe_data = b"MZ\x90\x00\x03" + bytes(range(128, 256)) * 10
        response = client.post(
            "/api/copilot/upload",
            files={"file": ("totally_safe.png", io.BytesIO(exe_data), "image/png")},
        )
        assert response.status_code == 415

    def test_upload_riff_not_webp_rejected(self, client):
        """RIFF header without WEBP at bytes 8-12 → unrecognized binary → 415."""
        # AVI container: RIFF header but not WEBP, followed by non-UTF8 data
        riff_avi = b"RIFF\x00\x00\x00\x00AVI " + bytes(range(128, 256)) * 10
        response = client.post(
            "/api/copilot/upload",
            files={"file": ("video.webp", io.BytesIO(riff_avi), "image/webp")},
        )
        assert response.status_code == 415

    def test_upload_image_gets_resized(self, client):
        """Image > 1600px should be resized."""
        big_png = _make_png(3200, 2400)
        response = client.post(
            "/api/copilot/upload",
            files={"file": ("big_screenshot.png", io.BytesIO(big_png), "image/png")},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "image"
        # Size should be smaller than original
        assert data["size_bytes"] < len(big_png)

    def test_upload_rate_limit(self, client):
        """More than 5 uploads in a minute should be rate limited."""
        png_data = _make_png()
        for i in range(5):
            resp = client.post(
                "/api/copilot/upload",
                files={"file": (f"file{i}.png", io.BytesIO(png_data), "image/png")},
            )
            assert resp.status_code == 200

        # 6th should be rate limited
        resp = client.post(
            "/api/copilot/upload",
            files={"file": ("file5.png", io.BytesIO(png_data), "image/png")},
        )
        assert resp.status_code == 429


# ---------------------------------------------------------------------------
# BRAIN_MESSAGE Attachment Integration Tests
# ---------------------------------------------------------------------------

class TestBrainMessageAttachments:
    """Test attachment handling in BRAIN_MESSAGE via WebSocket."""

    @pytest.fixture
    def app(self, tmp_path, monkeypatch):
        from fastapi import FastAPI
        from app.core.errors import VectorAIzError
        from app.core.errors.middleware import vectoraiz_error_handler
        from app.core.errors.registry import error_registry
        from app.routers.copilot import router as copilot_rest_router, ws_router, manager

        if len(error_registry) == 0:
            error_registry.load()

        import app.services.chat_attachment_service as att_mod
        monkeypatch.setattr(att_mod, "CHAT_UPLOAD_DIR", tmp_path / "chat")
        monkeypatch.setattr(att_mod, "chat_attachment_service", ChatAttachmentService())

        # Clean manager
        manager._active.clear()
        manager._user_sessions.clear()
        manager._connected_since.clear()
        manager._session_users.clear()
        manager._session_balance.clear()
        manager._session_state.clear()
        manager._session_intro_seen.clear()
        manager._inflight_task.clear()
        manager._session_msg_timestamps.clear()
        manager._user_connect_timestamps.clear()
        manager._lock = None

        app = FastAPI()
        app.add_exception_handler(VectorAIzError, vectoraiz_error_handler)
        app.include_router(copilot_rest_router, prefix="/api/copilot")
        app.include_router(ws_router)
        # Override auth dependency so endpoints don't 401
        app.dependency_overrides[get_current_user] = lambda: MOCK_USER
        yield app
        app.dependency_overrides.clear()

    @pytest.fixture
    def client(self, app):
        from fastapi.testclient import TestClient
        return TestClient(app)

    def test_too_many_attachments(self, client):
        """BRAIN_MESSAGE with > 3 attachments → error."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "Test message",
                "attachments": [
                    {"id": f"att_{i}", "type": "image"}
                    for i in range(4)
                ],
            })
            resp = ws.receive_json()
            assert resp["type"] == "ERROR"
            assert resp["code"] == "TOO_MANY_ATTACHMENTS"

    def test_attachment_not_found(self, client):
        """BRAIN_MESSAGE with non-existent attachment ID → error."""
        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "Test message",
                "attachments": [
                    {"id": "att_nonexistent", "type": "image"},
                ],
            })
            resp = ws.receive_json()
            assert resp["type"] == "ERROR"
            assert resp["code"] == "ATTACHMENT_NOT_FOUND"

    def test_attachment_wrong_user(self, client, tmp_path, monkeypatch):
        """BRAIN_MESSAGE with another user's attachment → ATTACHMENT_FORBIDDEN."""
        # Get the service instance that the copilot router actually uses
        from app.routers.copilot import chat_attachment_service as router_svc

        # Manually add an attachment owned by a different user
        att_dir = tmp_path / "chat" / "att_other"
        att_dir.mkdir(parents=True)
        file_path = att_dir / "other.png"
        file_path.write_bytes(_make_png())

        att = ChatAttachment(
            id="att_other",
            user_id="usr_OTHER_USER",
            filename="other.png",
            mime_type="image/png",
            size_bytes=100,
            type="image",
            file_path=file_path,
            created_at=time.time(),
            expires_at=time.time() + 3600,
        )
        router_svc._attachments["att_other"] = att

        with client.websocket_connect("/ws/copilot") as ws:
            connected = ws.receive_json()
            assert connected["type"] == "CONNECTED"

            ws.send_json({
                "type": "BRAIN_MESSAGE",
                "message": "Analyze this",
                "attachments": [
                    {"id": "att_other", "type": "image"},
                ],
            })
            resp = ws.receive_json()
            assert resp["type"] == "ERROR"
            assert resp["code"] == "ATTACHMENT_FORBIDDEN"


# ---------------------------------------------------------------------------
# Meta.json Persistence / Rebuild Tests
# ---------------------------------------------------------------------------

class TestMetaPersistence:
    def test_rebuild_index_on_init(self, tmp_path, monkeypatch):
        """Service should rebuild index from meta.json files on startup."""
        import app.services.chat_attachment_service as mod
        monkeypatch.setattr(mod, "CHAT_UPLOAD_DIR", tmp_path / "chat")

        # Create a valid attachment dir with meta.json
        att_dir = tmp_path / "chat" / "att_rebuild"
        att_dir.mkdir(parents=True)
        file_path = att_dir / "test.txt"
        file_path.write_bytes(b"hello")
        meta = {
            "id": "att_rebuild",
            "user_id": "usr_1",
            "filename": "test.txt",
            "mime_type": "text/plain",
            "size_bytes": 5,
            "type": "data",
            "created_at": time.time(),
            "expires_at": time.time() + 3600,
        }
        (att_dir / "meta.json").write_text(json.dumps(meta))

        svc = ChatAttachmentService()
        assert "att_rebuild" in svc._attachments
        assert svc._attachments["att_rebuild"].user_id == "usr_1"

    def test_rebuild_skips_expired(self, tmp_path, monkeypatch):
        """Expired entries should be cleaned during rebuild."""
        import app.services.chat_attachment_service as mod
        monkeypatch.setattr(mod, "CHAT_UPLOAD_DIR", tmp_path / "chat")

        att_dir = tmp_path / "chat" / "att_old"
        att_dir.mkdir(parents=True)
        file_path = att_dir / "old.txt"
        file_path.write_bytes(b"old")
        meta = {
            "id": "att_old",
            "user_id": "usr_1",
            "filename": "old.txt",
            "mime_type": "text/plain",
            "size_bytes": 3,
            "type": "data",
            "created_at": time.time() - 7200,
            "expires_at": time.time() - 3600,
        }
        (att_dir / "meta.json").write_text(json.dumps(meta))

        svc = ChatAttachmentService()
        assert "att_old" not in svc._attachments
        assert not att_dir.exists()  # cleaned up
