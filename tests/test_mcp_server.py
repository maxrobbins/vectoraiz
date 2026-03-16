"""
Tests for MCP server tool calls, error responses, auth.

BQ-MCP-RAG Phase 1.
"""

import json

import pytest
from unittest.mock import patch, AsyncMock, MagicMock

from app.mcp_server import (
    _format_error,
    _validate_dataset_id,
    vectoraiz_list_datasets,
    vectoraiz_get_schema,
    vectoraiz_search,
    vectoraiz_sql,
    vectoraiz_profile_dataset,
    vectoraiz_get_pii_report,
)
from app.models.connectivity import (
    ConnectivityToken,
    DatasetIdInput,
    DatasetListResponse,
    PIIReportResponse,
    ProfileResponse,
    ProfileColumnInfo,
    SchemaResponse,
    SearchResponse,
    SQLResponse,
    SQLLimits,
    validate_dataset_id,
)
from app.services.query_orchestrator import ConnectivityError


@pytest.fixture
def mock_token():
    return ConnectivityToken(
        id="test1234",
        label="Test",
        scopes=["ext:search", "ext:sql", "ext:schema", "ext:datasets", "ext:profile", "ext:pii"],
        secret_last4="abcd",
        created_at="2026-01-01T00:00:00",
    )


@pytest.fixture
def mock_orchestrator(mock_token):
    """Patch the global orchestrator in mcp_server."""
    orch = MagicMock()
    orch.validate_token.return_value = mock_token
    return orch


# ---------------------------------------------------------------------------
# Error formatting
# ---------------------------------------------------------------------------

class TestErrorFormatting:
    def test_format_error_structure(self):
        result = _format_error("test_code", "test message", {"key": "val"})
        parsed = json.loads(result)
        assert parsed["error"]["code"] == "test_code"
        assert parsed["error"]["message"] == "test message"
        assert parsed["error"]["details"]["key"] == "val"

    def test_format_error_no_details(self):
        result = _format_error("code", "msg")
        parsed = json.loads(result)
        assert parsed["error"]["details"] == {}


# ---------------------------------------------------------------------------
# Tool: list_datasets
# ---------------------------------------------------------------------------

class TestListDatasets:
    @pytest.mark.asyncio
    async def test_list_datasets_success(self, mock_orchestrator, mock_token):
        import app.mcp_server as mcp_mod
        mcp_mod._token_raw = "vzmcp_test1234_abcdef0123456789abcdef0123456789"

        mock_orchestrator.list_datasets = AsyncMock(
            return_value=DatasetListResponse(datasets=[], count=0)
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orchestrator):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                result = await vectoraiz_list_datasets()
                data = json.loads(result)
                assert data["count"] == 0
                assert data["datasets"] == []

    @pytest.mark.asyncio
    async def test_list_datasets_auth_error(self):
        import app.mcp_server as mcp_mod
        mcp_mod._token_raw = "invalid"

        mock_orch = MagicMock()
        mock_orch.validate_token.side_effect = ConnectivityError("auth_invalid", "Bad token")

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with pytest.raises(ValueError) as exc_info:
                await vectoraiz_list_datasets()
            error_data = json.loads(str(exc_info.value))
            assert error_data["error"]["code"] == "auth_invalid"


# ---------------------------------------------------------------------------
# Tool: get_schema
# ---------------------------------------------------------------------------

class TestGetSchema:
    @pytest.mark.asyncio
    async def test_get_schema_success(self, mock_orchestrator, mock_token):
        import app.mcp_server as mcp_mod

        mock_orchestrator.get_schema = AsyncMock(
            return_value=SchemaResponse(
                dataset_id="abc123",
                table_name="dataset_abc123",
                row_count=100,
                columns=[],
            )
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orchestrator):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                result = await vectoraiz_get_schema("abc123")
                data = json.loads(result)
                assert data["dataset_id"] == "abc123"
                assert data["row_count"] == 100

    @pytest.mark.asyncio
    async def test_get_schema_not_found(self, mock_token):
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.get_schema = AsyncMock(
            side_effect=ConnectivityError("dataset_not_found", "Not found")
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_get_schema("nonexistent")
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "dataset_not_found"


# ---------------------------------------------------------------------------
# Tool: search
# ---------------------------------------------------------------------------

class TestSearch:
    @pytest.mark.asyncio
    async def test_search_success(self, mock_orchestrator, mock_token):
        import app.mcp_server as mcp_mod

        mock_orchestrator.search_vectors = AsyncMock(
            return_value=SearchResponse(
                matches=[], count=0, truncated=False, request_id="ext-test"
            )
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orchestrator):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                result = await vectoraiz_search("test query", top_k=5)
                data = json.loads(result)
                assert data["count"] == 0

    @pytest.mark.asyncio
    async def test_search_clamps_top_k(self, mock_orchestrator, mock_token):
        import app.mcp_server as mcp_mod

        mock_orchestrator.search_vectors = AsyncMock(
            return_value=SearchResponse(
                matches=[], count=0, truncated=False, request_id="ext-test"
            )
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orchestrator):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                # top_k > 20 should be clamped
                await vectoraiz_search("query", top_k=100)
                call_args = mock_orchestrator.search_vectors.call_args
                req = call_args[0][1]  # second positional arg
                assert req.top_k == 20


# ---------------------------------------------------------------------------
# Tool: sql
# ---------------------------------------------------------------------------

class TestSQL:
    @pytest.mark.asyncio
    async def test_sql_success(self, mock_orchestrator, mock_token):
        import app.mcp_server as mcp_mod

        mock_orchestrator.execute_sql = AsyncMock(
            return_value=SQLResponse(
                columns=["id", "name"],
                rows=[[1, "test"]],
                row_count=1,
                truncated=False,
                execution_ms=50,
                limits_applied=SQLLimits(max_rows=500, max_runtime_ms=10000, max_memory_mb=256),
                request_id="ext-test",
            )
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orchestrator):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                result = await vectoraiz_sql("SELECT * FROM dataset_abc123")
                data = json.loads(result)
                assert data["row_count"] == 1
                assert data["columns"] == ["id", "name"]

    @pytest.mark.asyncio
    async def test_sql_forbidden(self, mock_token):
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.execute_sql = AsyncMock(
            side_effect=ConnectivityError("forbidden_sql", "DROP not allowed")
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_sql("DROP TABLE users")
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "forbidden_sql"


# ---------------------------------------------------------------------------
# Error hardening — no raw exception strings (Fix 3 — Gate 3)
# ---------------------------------------------------------------------------

class TestErrorHardening:
    """Verify that unexpected exceptions don't leak internal details to clients."""

    @pytest.mark.asyncio
    async def test_list_datasets_hides_internal_error(self, mock_token):
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.list_datasets = AsyncMock(
            side_effect=RuntimeError("/internal/path/to/database.db: connection refused")
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_list_datasets()
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "internal_error"
                # Must NOT contain the raw exception message
                assert "/internal/path" not in error_data["error"]["message"]
                assert "connection refused" not in error_data["error"]["message"]
                assert "Check vectorAIz logs" in error_data["error"]["message"]

    @pytest.mark.asyncio
    async def test_get_schema_hides_internal_error(self, mock_token):
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.get_schema = AsyncMock(
            side_effect=FileNotFoundError("/secret/path/data.parquet not found")
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_get_schema("abc123")
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "internal_error"
                assert "/secret/path" not in error_data["error"]["message"]

    @pytest.mark.asyncio
    async def test_search_hides_internal_error(self, mock_token):
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.search_vectors = AsyncMock(
            side_effect=ConnectionError("qdrant://localhost:6333 unreachable")
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_search("test query")
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "internal_error"
                assert "qdrant" not in error_data["error"]["message"]

    @pytest.mark.asyncio
    async def test_sql_hides_internal_error(self, mock_token):
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.execute_sql = AsyncMock(
            side_effect=MemoryError("DuckDB out of memory at 0x7fff...")
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_sql("SELECT 1")
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "internal_error"
                assert "DuckDB" not in error_data["error"]["message"]
                assert "0x7fff" not in error_data["error"]["message"]


# ---------------------------------------------------------------------------
# Input validation — dataset_id
# ---------------------------------------------------------------------------

class TestInputValidation:
    """Validate dataset_id input: path traversal, injection, length."""

    def test_valid_dataset_id(self):
        assert validate_dataset_id("abc123") == "abc123"
        assert validate_dataset_id("my-dataset_01") == "my-dataset_01"
        assert validate_dataset_id("A" * 64) == "A" * 64

    def test_path_traversal_rejected(self):
        with pytest.raises(ValueError):
            validate_dataset_id("../../etc/passwd")

    def test_path_traversal_dot_dot(self):
        with pytest.raises(ValueError):
            validate_dataset_id("..%2F..%2Fetc%2Fpasswd")

    def test_path_traversal_slashes(self):
        with pytest.raises(ValueError):
            validate_dataset_id("foo/bar")

    def test_path_traversal_backslashes(self):
        with pytest.raises(ValueError):
            validate_dataset_id("foo\\bar")

    def test_sql_injection_rejected(self):
        with pytest.raises(ValueError):
            validate_dataset_id("'; DROP TABLE datasets; --")

    def test_spaces_rejected(self):
        with pytest.raises(ValueError):
            validate_dataset_id("has space")

    def test_too_long_rejected(self):
        with pytest.raises(ValueError):
            validate_dataset_id("A" * 65)

    def test_empty_rejected(self):
        with pytest.raises(ValueError):
            validate_dataset_id("")

    def test_special_chars_rejected(self):
        for char in [".", ",", "!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "=", "+"]:
            with pytest.raises(ValueError):
                validate_dataset_id(f"test{char}id")

    def test_pydantic_model_validates(self):
        m = DatasetIdInput(dataset_id="valid_id-123")
        assert m.dataset_id == "valid_id-123"

    def test_pydantic_model_rejects_traversal(self):
        with pytest.raises(Exception):
            DatasetIdInput(dataset_id="../../etc/passwd")

    def test_mcp_validate_dataset_id_rejects(self):
        """_validate_dataset_id in mcp_server raises ConnectivityError."""
        with pytest.raises(ConnectivityError) as exc_info:
            _validate_dataset_id("../../etc/passwd")
        assert exc_info.value.code == "invalid_input"

    def test_mcp_validate_dataset_id_accepts(self):
        assert _validate_dataset_id("valid_id") == "valid_id"


# ---------------------------------------------------------------------------
# Tool: profile_dataset
# ---------------------------------------------------------------------------

class TestProfileDataset:
    @pytest.mark.asyncio
    async def test_profile_success(self, mock_orchestrator, mock_token):
        import app.mcp_server as mcp_mod

        mock_orchestrator.profile_dataset = AsyncMock(
            return_value=ProfileResponse(
                dataset_id="abc123",
                row_count=500,
                column_count=3,
                columns=[
                    ProfileColumnInfo(name="id", type="INTEGER", null_count=0, null_rate=0.0, sample_values=["1", "2"]),
                    ProfileColumnInfo(name="name", type="VARCHAR", null_count=5, null_rate=0.01, sample_values=["Alice", "Bob"]),
                    ProfileColumnInfo(name="email", type="VARCHAR", null_count=10, null_rate=0.02, sample_values=["a@b.com"]),
                ],
                sample_rows=[[1, "Alice", "a@b.com"], [2, "Bob", "b@c.com"]],
            )
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orchestrator):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                result = await vectoraiz_profile_dataset("abc123")
                data = json.loads(result)
                assert data["dataset_id"] == "abc123"
                assert data["row_count"] == 500
                assert data["column_count"] == 3
                assert len(data["columns"]) == 3
                assert len(data["sample_rows"]) == 2

    @pytest.mark.asyncio
    async def test_profile_path_traversal_rejected(self, mock_token):
        """Path traversal in dataset_id is caught before reaching orchestrator."""
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.profile_dataset = AsyncMock()

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_profile_dataset("../../etc/passwd")
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "invalid_input"
                # Orchestrator should never have been called
                mock_orch.profile_dataset.assert_not_called()

    @pytest.mark.asyncio
    async def test_profile_hides_internal_error(self, mock_token):
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.profile_dataset = AsyncMock(
            side_effect=RuntimeError("/data/processed/secret/file.parquet crashed")
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_profile_dataset("abc123")
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "internal_error"
                assert "/data/processed" not in error_data["error"]["message"]


# ---------------------------------------------------------------------------
# Tool: get_pii_report
# ---------------------------------------------------------------------------

class TestGetPIIReport:
    @pytest.mark.asyncio
    async def test_pii_report_available(self, mock_orchestrator, mock_token):
        import app.mcp_server as mcp_mod

        mock_orchestrator.get_pii_report = AsyncMock(
            return_value=PIIReportResponse(
                dataset_id="abc123",
                status="available",
                report={"columns_scanned": 5, "pii_found": True, "findings": []},
            )
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orchestrator):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                result = await vectoraiz_get_pii_report("abc123")
                data = json.loads(result)
                assert data["dataset_id"] == "abc123"
                assert data["status"] == "available"
                assert data["report"]["pii_found"] is True

    @pytest.mark.asyncio
    async def test_pii_report_not_available(self, mock_orchestrator, mock_token):
        import app.mcp_server as mcp_mod

        mock_orchestrator.get_pii_report = AsyncMock(
            return_value=PIIReportResponse(
                dataset_id="abc123",
                status="not_available",
                message="Run dataset processing first",
            )
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orchestrator):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                result = await vectoraiz_get_pii_report("abc123")
                data = json.loads(result)
                assert data["status"] == "not_available"
                assert "processing" in data["message"]

    @pytest.mark.asyncio
    async def test_pii_report_path_traversal_rejected(self, mock_token):
        """Path traversal in dataset_id is caught before reaching orchestrator."""
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.get_pii_report = AsyncMock()

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_get_pii_report("../../etc/passwd")
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "invalid_input"
                mock_orch.get_pii_report.assert_not_called()

    @pytest.mark.asyncio
    async def test_pii_report_hides_internal_error(self, mock_token):
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.get_pii_report = AsyncMock(
            side_effect=PermissionError("/data/processed/abc123/pii_scan.json: permission denied")
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_get_pii_report("abc123")
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "internal_error"
                assert "/data/processed" not in error_data["error"]["message"]
                assert "permission denied" not in error_data["error"]["message"]


# ---------------------------------------------------------------------------
# Input validation on existing tools (path traversal via dataset_id)
# ---------------------------------------------------------------------------

class TestExistingToolsInputValidation:
    """Verify that existing tools now validate dataset_id input."""

    @pytest.mark.asyncio
    async def test_get_schema_rejects_traversal(self, mock_token):
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.get_schema = AsyncMock()

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_get_schema("../../etc/passwd")
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "invalid_input"
                mock_orch.get_schema.assert_not_called()

    @pytest.mark.asyncio
    async def test_search_rejects_traversal_in_dataset_id(self, mock_token):
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.search_vectors = AsyncMock()

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_search("test query", dataset_id="../../etc/passwd")
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "invalid_input"
                mock_orch.search_vectors.assert_not_called()

    @pytest.mark.asyncio
    async def test_sql_rejects_traversal_in_dataset_id(self, mock_token):
        import app.mcp_server as mcp_mod

        mock_orch = MagicMock()
        mock_orch.execute_sql = AsyncMock()

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orch):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                with pytest.raises(ValueError) as exc_info:
                    await vectoraiz_sql("SELECT 1", dataset_id="../../etc/passwd")
                error_data = json.loads(str(exc_info.value))
                assert error_data["error"]["code"] == "invalid_input"
                mock_orch.execute_sql.assert_not_called()

    @pytest.mark.asyncio
    async def test_search_allows_empty_dataset_id(self, mock_orchestrator, mock_token):
        """Empty dataset_id should still work (means 'search all')."""
        import app.mcp_server as mcp_mod

        mock_orchestrator.search_vectors = AsyncMock(
            return_value=SearchResponse(
                matches=[], count=0, truncated=False, request_id="ext-test"
            )
        )

        with patch.object(mcp_mod, "_get_orchestrator", return_value=mock_orchestrator):
            with patch.object(mcp_mod, "_validate_token", return_value=mock_token):
                result = await vectoraiz_search("test query", dataset_id="")
                data = json.loads(result)
                assert data["count"] == 0
