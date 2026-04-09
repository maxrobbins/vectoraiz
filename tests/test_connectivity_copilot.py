"""
Tests for BQ-MCP-RAG Phase 2: allAI Connectivity Concierge.

Covers:
1. Tool definitions — all 7 connectivity tools in ALLAI_TOOLS
2. Tool handlers — connectivity_status, create, revoke, enable, disable, setup, test
3. Security — secret masking, save warning on create
4. System prompt — connectivity guide present

PHASE: BQ-MCP-RAG Phase 2 Tests
CREATED: S136
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.allai_tools import ALLAI_TOOLS
from app.services.allai_tool_executor import AllAIToolExecutor


@pytest.fixture(autouse=True)
def _clear_connectivity_tokens():
    """Clear connectivity tokens before each test to avoid cross-test state."""
    from app.core.database import get_session_context
    from app.models.connectivity import ConnectivityTokenRecord
    with get_session_context() as session:
        session.query(ConnectivityTokenRecord).delete()
        session.commit()
    yield
    with get_session_context() as session:
        session.query(ConnectivityTokenRecord).delete()
        session.commit()


# =====================================================================
# Helpers
# =====================================================================

def _get_tool_names():
    return [t["name"] for t in ALLAI_TOOLS]


def _get_tool(name: str) -> dict:
    for t in ALLAI_TOOLS:
        if t["name"] == name:
            return t
    raise ValueError(f"Tool '{name}' not found")


def _make_executor() -> AllAIToolExecutor:
    """Create an executor with mocked user and websocket."""
    user = MagicMock()
    user.user_id = "test-user-001"
    send_ws = AsyncMock()
    return AllAIToolExecutor(user=user, send_ws=send_ws, session_id="test-session")


# =====================================================================
# Step 1: Tool Definition Tests
# =====================================================================

class TestConnectivityToolDefinitions:
    """All 7 connectivity tools should be in ALLAI_TOOLS with correct schemas."""

    CONNECTIVITY_TOOLS = [
        "connectivity_status",
        "connectivity_enable",
        "connectivity_disable",
        "connectivity_create_token",
        "connectivity_revoke_token",
        "connectivity_generate_setup",
        "connectivity_test",
    ]

    def test_all_7_tools_present(self):
        tool_names = _get_tool_names()
        for name in self.CONNECTIVITY_TOOLS:
            assert name in tool_names, f"Tool '{name}' missing from ALLAI_TOOLS"

    def test_tool_count_at_least_15(self):
        """8 original + 7 connectivity = 15 tools minimum."""
        assert len(ALLAI_TOOLS) >= 15

    def test_original_tools_preserved(self):
        """Original 8 tools must still exist."""
        original = [
            "list_datasets", "get_dataset_detail", "preview_rows",
            "run_sql_query", "search_vectors", "get_system_status",
            "get_dataset_statistics", "delete_dataset",
        ]
        tool_names = _get_tool_names()
        for name in original:
            assert name in tool_names, f"Original tool '{name}' missing"

    def test_each_tool_has_required_fields(self):
        for name in self.CONNECTIVITY_TOOLS:
            tool = _get_tool(name)
            assert "name" in tool
            assert "description" in tool
            assert "input_schema" in tool
            schema = tool["input_schema"]
            assert schema["type"] == "object"
            assert "properties" in schema
            assert "required" in schema

    def test_connectivity_status_no_required_params(self):
        tool = _get_tool("connectivity_status")
        assert tool["input_schema"]["required"] == []

    def test_connectivity_enable_no_required_params(self):
        tool = _get_tool("connectivity_enable")
        assert tool["input_schema"]["required"] == []

    def test_connectivity_disable_no_required_params(self):
        tool = _get_tool("connectivity_disable")
        assert tool["input_schema"]["required"] == []

    def test_connectivity_create_token_requires_label(self):
        tool = _get_tool("connectivity_create_token")
        assert "label" in tool["input_schema"]["required"]

    def test_connectivity_create_token_has_scopes_param(self):
        tool = _get_tool("connectivity_create_token")
        props = tool["input_schema"]["properties"]
        assert "scopes" in props
        assert props["scopes"]["type"] == "array"

    def test_connectivity_revoke_token_requires_token_id(self):
        tool = _get_tool("connectivity_revoke_token")
        assert "token_id" in tool["input_schema"]["required"]

    def test_connectivity_generate_setup_requires_platform_and_token(self):
        tool = _get_tool("connectivity_generate_setup")
        required = tool["input_schema"]["required"]
        assert "platform" in required
        assert "token" in required

    def test_connectivity_generate_setup_has_platform_enum(self):
        tool = _get_tool("connectivity_generate_setup")
        platforms = tool["input_schema"]["properties"]["platform"]["enum"]
        expected = [
            "claude_desktop", "chatgpt_desktop", "cursor", "gemini",
            "vscode", "openai_custom_gpt", "generic_rest", "generic_llm",
        ]
        assert platforms == expected

    def test_connectivity_test_requires_token(self):
        tool = _get_tool("connectivity_test")
        assert "token" in tool["input_schema"]["required"]

    def test_all_tools_have_descriptions(self):
        for name in self.CONNECTIVITY_TOOLS:
            tool = _get_tool(name)
            assert len(tool["description"]) > 20, f"Tool '{name}' has too short description"


# =====================================================================
# Step 2: Tool Handler Tests
# =====================================================================

class TestConnectivityStatusHandler:
    """connectivity_status returns tokens with masked secrets."""

    @pytest.mark.asyncio
    async def test_status_returns_enabled_state(self):
        executor = _make_executor()
        with patch("app.config.settings") as mock_settings:
            mock_settings.connectivity_enabled = True
            with patch(
                "app.services.connectivity_token_service.list_tokens", return_value=[]
            ):
                result = await executor._handle_connectivity_status({})

            assert result.frontend_data["enabled"] is True
            assert result.frontend_data["token_count"] == 0

    @pytest.mark.asyncio
    async def test_status_masks_secrets(self):
        """Tokens in status should have secret_last4 but NEVER full secret."""
        from app.services.connectivity_token_service import create_token

        raw_token, token_info = create_token(label="test-mask", scopes=["ext:search"])
        secret = raw_token.split("_")[2]  # full 32-char secret

        executor = _make_executor()
        result = await executor._handle_connectivity_status({})

        # Check frontend_data tokens
        tokens = result.frontend_data.get("tokens", [])
        assert len(tokens) >= 1

        found = False
        for t in tokens:
            if t["id"] == token_info.id:
                found = True
                assert t["secret_last4"] == secret[-4:]
                # Ensure full secret is NOT present anywhere in the result
                result_str = json.dumps(result.frontend_data)
                assert secret not in result_str
                break
        assert found, "Created token not found in status"

    @pytest.mark.asyncio
    async def test_status_never_exposes_full_secret_in_llm_summary(self):
        """LLM summary must not contain full token secret."""
        from app.services.connectivity_token_service import create_token

        raw_token, token_info = create_token(label="test-llm-mask", scopes=["ext:search"])
        secret = raw_token.split("_")[2]

        executor = _make_executor()
        result = await executor._handle_connectivity_status({})

        assert secret not in result.llm_summary

    @pytest.mark.asyncio
    async def test_status_includes_metrics(self):
        executor = _make_executor()
        result = await executor._handle_connectivity_status({})

        assert "metrics" in result.frontend_data


class TestConnectivityCreateTokenHandler:
    """connectivity_create_token returns full token with save warning."""

    @pytest.mark.asyncio
    async def test_create_returns_full_token(self):
        executor = _make_executor()
        result = await executor._handle_connectivity_create_token({
            "label": "Test Tool",
        })

        assert "token" in result.frontend_data
        assert result.frontend_data["token"].startswith("vzmcp_")
        assert len(result.frontend_data["token"]) > 40

    @pytest.mark.asyncio
    async def test_create_includes_save_warning_in_frontend(self):
        executor = _make_executor()
        result = await executor._handle_connectivity_create_token({
            "label": "Warning Test",
        })

        warning = result.frontend_data.get("warning", "")
        assert "SAVE" in warning.upper()
        assert "not be shown again" in warning.lower()

    @pytest.mark.asyncio
    async def test_create_includes_save_warning_in_llm_summary(self):
        executor = _make_executor()
        result = await executor._handle_connectivity_create_token({
            "label": "LLM Warning Test",
        })

        assert "SAVE" in result.llm_summary.upper()
        assert "NOT" in result.llm_summary.upper()

    @pytest.mark.asyncio
    async def test_create_returns_token_metadata(self):
        executor = _make_executor()
        result = await executor._handle_connectivity_create_token({
            "label": "Meta Test",
            "scopes": ["ext:search", "ext:sql"],
        })

        assert result.frontend_data["label"] == "Meta Test"
        assert "ext:search" in result.frontend_data["scopes"]
        assert "ext:sql" in result.frontend_data["scopes"]
        assert "token_id" in result.frontend_data
        assert "secret_last4" in result.frontend_data

    @pytest.mark.asyncio
    async def test_create_token_NOT_in_llm_summary(self):
        """LLM summary must NOT contain full token — only frontend_data shows it once."""
        executor = _make_executor()
        result = await executor._handle_connectivity_create_token({
            "label": "Full Token Test",
        })

        token = result.frontend_data["token"]
        secret = token.split("_")[2]  # 32-char hex secret
        assert secret not in result.llm_summary, "Full token secret must not appear in llm_summary"
        # But token_id and last4 should be present
        assert result.frontend_data["token_id"] in result.llm_summary
        assert result.frontend_data["secret_last4"] in result.llm_summary


class TestConnectivityRevokeTokenHandler:
    """connectivity_revoke_token actually revokes."""

    @pytest.mark.asyncio
    async def test_revoke_succeeds(self):
        from app.services.connectivity_token_service import create_token

        _, token_info = create_token(label="revoke-test")
        executor = _make_executor()
        result = await executor._handle_connectivity_revoke_token({
            "token_id": token_info.id,
        })

        assert result.frontend_data.get("revoked") is True
        assert result.frontend_data["token_id"] == token_info.id

    @pytest.mark.asyncio
    async def test_revoke_nonexistent_fails(self):
        executor = _make_executor()
        result = await executor._handle_connectivity_revoke_token({
            "token_id": "ZZZZZZZZ",
        })

        assert "error" in result.frontend_data

    @pytest.mark.asyncio
    async def test_revoke_double_fails(self):
        from app.services.connectivity_token_service import create_token

        _, token_info = create_token(label="double-revoke-test")
        executor = _make_executor()

        await executor._handle_connectivity_revoke_token({"token_id": token_info.id})
        result = await executor._handle_connectivity_revoke_token({"token_id": token_info.id})

        assert "error" in result.frontend_data


class TestConnectivityEnableDisableHandler:
    """Enable/disable toggles state correctly."""

    @pytest.mark.asyncio
    async def test_enable_sets_flag(self):
        executor = _make_executor()
        with patch("app.config.settings") as mock_settings:
            mock_settings.connectivity_enabled = False
            result = await executor._handle_connectivity_enable({})
            assert mock_settings.connectivity_enabled is True
            assert result.frontend_data["enabled"] is True
            assert result.frontend_data["changed"] is True

    @pytest.mark.asyncio
    async def test_enable_idempotent(self):
        executor = _make_executor()
        with patch("app.config.settings") as mock_settings:
            mock_settings.connectivity_enabled = True
            result = await executor._handle_connectivity_enable({})
            assert result.frontend_data["changed"] is False

    @pytest.mark.asyncio
    async def test_disable_sets_flag(self):
        executor = _make_executor()
        with patch("app.config.settings") as mock_settings:
            mock_settings.connectivity_enabled = True
            result = await executor._handle_connectivity_disable({})
            assert mock_settings.connectivity_enabled is False
            assert result.frontend_data["enabled"] is False
            assert result.frontend_data["changed"] is True

    @pytest.mark.asyncio
    async def test_disable_idempotent(self):
        executor = _make_executor()
        with patch("app.config.settings") as mock_settings:
            mock_settings.connectivity_enabled = False
            result = await executor._handle_connectivity_disable({})
            assert result.frontend_data["changed"] is False


class TestConnectivityGenerateSetupHandler:
    """connectivity_generate_setup for each platform."""

    @pytest.mark.asyncio
    async def test_generate_claude_desktop(self):
        executor = _make_executor()
        result = await executor._handle_connectivity_generate_setup({
            "platform": "claude_desktop",
            "token": "vzmcp_testtest_abcdef0123456789abcdef0123456789",
        })

        data = result.frontend_data
        assert data["platform"] == "claude_desktop"
        assert "mcpServers" in json.dumps(data.get("config", {}))
        assert len(data["steps"]) >= 3

    @pytest.mark.asyncio
    async def test_generate_chatgpt_desktop(self):
        executor = _make_executor()
        result = await executor._handle_connectivity_generate_setup({
            "platform": "chatgpt_desktop",
            "token": "vzmcp_testtest_abcdef0123456789abcdef0123456789",
        })

        data = result.frontend_data
        assert data["platform"] == "chatgpt_desktop"
        assert len(data["steps"]) >= 3

    @pytest.mark.asyncio
    async def test_generate_cursor(self):
        executor = _make_executor()
        result = await executor._handle_connectivity_generate_setup({
            "platform": "cursor",
            "token": "vzmcp_testtest_abcdef0123456789abcdef0123456789",
        })

        data = result.frontend_data
        assert data["platform"] == "cursor"
        assert "mcpServers" in json.dumps(data.get("config", {}))

    @pytest.mark.asyncio
    async def test_generate_generic_rest(self):
        executor = _make_executor()
        result = await executor._handle_connectivity_generate_setup({
            "platform": "generic_rest",
            "token": "vzmcp_testtest_abcdef0123456789abcdef0123456789",
        })

        data = result.frontend_data
        assert data["platform"] == "generic_rest"
        assert "curl" in json.dumps(data.get("steps", []))

    @pytest.mark.asyncio
    async def test_generate_generic_llm(self):
        executor = _make_executor()
        with patch("app.services.processing_service.get_processing_service") as mock_svc:
            mock_record = MagicMock()
            mock_record.id = "ds001"
            mock_record.original_filename = "sales_data.csv"
            mock_record.status.value = "ready"
            mock_record.metadata = {
                "row_count": 1000,
                "column_count": 10,
                "description": "Sales records",
            }
            mock_svc.return_value.list_datasets.return_value = [mock_record]

            result = await executor._handle_connectivity_generate_setup({
                "platform": "generic_llm",
                "token": "vzmcp_testtest_abcdef0123456789abcdef0123456789",
            })

        data = result.frontend_data
        assert data["platform"] == "generic_llm"
        system_prompt = data.get("config", {}).get("system_prompt", "")
        assert "sales_data.csv" in system_prompt

    @pytest.mark.asyncio
    async def test_generate_setup_llm_summary_has_steps(self):
        executor = _make_executor()
        result = await executor._handle_connectivity_generate_setup({
            "platform": "generic_rest",
            "token": "vzmcp_testtest_abcdef0123456789abcdef0123456789",
        })

        assert "Steps:" in result.llm_summary


class TestConnectivityTestHandler:
    """connectivity_test diagnostic."""

    @pytest.mark.asyncio
    async def test_diagnostic_disabled(self):
        executor = _make_executor()
        with patch("app.config.settings") as mock_settings:
            mock_settings.connectivity_enabled = False
            result = await executor._handle_connectivity_test({
                "token": "vzmcp_testtest_abcdef0123456789abcdef0123456789",
            })

            assert result.frontend_data["connectivity_enabled"] is False
            assert "disabled" in result.llm_summary.lower()

    @pytest.mark.asyncio
    async def test_diagnostic_invalid_token(self):
        executor = _make_executor()
        with patch("app.config.settings") as mock_settings:
            mock_settings.connectivity_enabled = True
            result = await executor._handle_connectivity_test({
                "token": "vzmcp_XXXXXXXX_00000000000000000000000000000000",
            })

            assert result.frontend_data["token_valid"] is False
            assert "FAILED" in result.llm_summary

    @pytest.mark.asyncio
    async def test_diagnostic_returns_structured_results(self):
        executor = _make_executor()
        with patch("app.config.settings") as mock_settings:
            mock_settings.connectivity_enabled = False
            result = await executor._handle_connectivity_test({
                "token": "vzmcp_testtest_abcdef0123456789abcdef0123456789",
            })

            data = result.frontend_data
            assert "connectivity_enabled" in data
            assert "token_valid" in data
            assert "mcp_responding" in data
            assert "datasets_accessible" in data
            assert "sample_query_ok" in data


# =====================================================================
# System Prompt Tests
# =====================================================================

class TestSystemPromptConnectivityGuide:
    """The connectivity guide must be in the system prompt."""

    def test_prompt_factory_includes_connectivity_guide(self):
        from app.services.prompt_factory import PromptFactory, AllieContext

        factory = PromptFactory()
        prompt = factory.build_system_prompt(AllieContext())

        assert "External Connectivity Guide" in prompt
        assert "connectivity_status" in prompt
        assert "Claude Desktop" in prompt
        assert "ChatGPT" in prompt
        assert "Cursor" in prompt
        assert "secret they should not share" in prompt

    def test_prompt_guide_in_layer_2(self):
        from app.services.prompt_factory import PromptFactory, AllieContext

        factory = PromptFactory()
        ctx = AllieContext()
        layer2 = factory._layer_2_role_domain(ctx.capabilities)

        assert "External Connectivity Guide" in layer2
        assert "connectivity_status" in layer2
