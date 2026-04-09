"""
Tests for anti-hallucination prompt rules and diagnostic bundle tool.

Covers:
1. Prompt anti-hallucination text in Layer 1 and Layer 2
2. generate_diagnostic_bundle exists in ALLAI_TOOLS
3. Tool executor handles generate_diagnostic_bundle (mocked DiagnosticService)
"""

import io
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.prompt_factory import PromptFactory
from app.services.allai_tools import ALLAI_TOOLS
from app.services.allai_tool_executor import AllAIToolExecutor


# =====================================================================
# Anti-hallucination prompt tests
# =====================================================================

class TestAntiHallucinationPrompt:
    """Verify anti-hallucination rules appear in the system prompt."""

    def test_layer1_has_tool_list_constraint(self):
        pf = PromptFactory()
        layer1 = pf._layer_1_safety()
        assert "Tool-list constraint" in layer1
        assert "NEVER offer, suggest, or claim you can perform capabilities" in layer1

    def test_layer2_has_no_unlisted_capabilities_rule(self):
        pf = PromptFactory()
        layer2 = pf._layer_2_role_domain({})
        assert "NEVER offer capabilities not in your tool list" in layer2


# =====================================================================
# Diagnostic bundle tool definition test
# =====================================================================

class TestDiagnosticBundleToolDef:
    """Verify generate_diagnostic_bundle is in ALLAI_TOOLS."""

    def test_tool_exists_in_allai_tools(self):
        names = [t["name"] for t in ALLAI_TOOLS]
        assert "generate_diagnostic_bundle" in names

    def test_tool_has_correct_schema(self):
        tool = next(t for t in ALLAI_TOOLS if t["name"] == "generate_diagnostic_bundle")
        assert tool["input_schema"]["type"] == "object"
        assert tool["input_schema"]["required"] == []


# =====================================================================
# Diagnostic bundle executor test
# =====================================================================

class TestDiagnosticBundleExecutor:
    """Verify executor handles generate_diagnostic_bundle via mocked DiagnosticService."""

    @pytest.mark.asyncio
    async def test_handle_generate_diagnostic_bundle(self):
        mock_user = MagicMock()
        mock_user.user_id = "test-user"
        mock_ws = AsyncMock()

        executor = AllAIToolExecutor(
            user=mock_user,
            send_ws=mock_ws,
            session_id="test-session",
        )

        # Mock DiagnosticService.generate_bundle to return a small ZIP-like buffer
        fake_bundle = io.BytesIO(b"x" * 2048)
        mock_service = MagicMock()
        mock_service.generate_bundle = AsyncMock(return_value=fake_bundle)

        with patch(
            "app.services.diagnostic_service.DiagnosticService",
            return_value=mock_service,
        ):
            result = await executor.execute("generate_diagnostic_bundle", {})

        assert result.frontend_data["success"] is True
        assert result.frontend_data["bundle_size_kb"] == 2.0
        assert "health" in result.frontend_data["contents"]
        assert "logs" in result.frontend_data["contents"]
        assert "Diagnostic bundle generated" in result.llm_summary
