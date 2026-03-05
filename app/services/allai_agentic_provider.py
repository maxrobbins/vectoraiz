"""
Agentic LLM Provider — Tool-use loop for allAI.

Architecture:
- All LLM calls go through ai.market proxy (POST /api/v1/allie/chat/agentic)
- No local Anthropic API key needed
- Max 5 tool iterations (hard cap)
- Two-track tool results (rich→frontend, summary→LLM)
- Heartbeat during tool execution (WS resilience)

PHASE: BQ-ALLAI-B3 — Agentic LLM Loop
CREATED: 2026-02-16
UPDATED: 2026-03-05 — Proxy-only path via ai.market /agentic endpoint
"""

import asyncio
import json
import logging
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import httpx

from app.services.allie_provider import AllieDisabledError, AllieUsage
from app.services.allai_tool_executor import AllAIToolExecutor

logger = logging.getLogger(__name__)

MAX_ITERATIONS = 5
HEARTBEAT_INTERVAL_S = 5


class AgenticAllieProvider:
    """
    Agentic LLM provider that supports tool-use loops via ai.market proxy.
    """

    async def run_agentic_loop(
        self,
        messages: List[dict],
        system_prompt: str,
        tools: List[dict],
        tool_executor: AllAIToolExecutor,
        send_chunk: Callable[[str], Awaitable[None]],
        send_heartbeat: Callable[[], Awaitable[None]],
    ) -> Tuple[str, Optional[AllieUsage]]:
        """
        Execute the agentic loop:

        1. Call LLM with messages + tools
        2. If response has tool_use blocks → execute tools → feed summaries back
        3. If response has text → stream to frontend
        4. Repeat until text-only response or max iterations

        Returns:
            (full_text, usage) tuple
        """
        iteration = 0
        total_usage = None
        full_text = ""

        while iteration < MAX_ITERATIONS:
            iteration += 1

            # Send heartbeat before LLM call
            await send_heartbeat()

            # Call LLM via ai.market proxy
            response = await self._call_llm(messages, system_prompt, tools)

            # Track usage
            if response.get("usage"):
                total_usage = self._merge_usage(total_usage, response["usage"], response.get("model", "unknown"))

            # Process response content blocks
            has_tool_use = False
            tool_results_for_next = []
            content_blocks = response.get("content", [])

            # Build assistant message content for message history
            assistant_content = []

            for block in content_blocks:
                block_type = block.get("type", "")

                if block_type == "text":
                    text = block.get("text", "")
                    if text:
                        await send_chunk(text)
                        full_text += text
                    assistant_content.append(block)

                elif block_type == "tool_use":
                    has_tool_use = True
                    assistant_content.append(block)

                    tool_name = block.get("name", "")
                    tool_input = block.get("input", {})
                    tool_use_id = block.get("id", "")

                    # Send heartbeat during tool execution
                    await send_heartbeat()

                    # Execute tool (handles auth, sandbox, confirmation, two-track)
                    result = await tool_executor.execute(tool_name, tool_input)

                    # Feed SUMMARY (not raw data) back to LLM
                    tool_results_for_next.append({
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": result.llm_summary,
                    })

            if not has_tool_use:
                # LLM responded with text only — we're done
                break

            # Append assistant response + tool results to messages for next iteration
            messages.append({"role": "assistant", "content": assistant_content})
            messages.append({"role": "user", "content": tool_results_for_next})

        return full_text, total_usage

    async def _call_llm(
        self,
        messages: List[dict],
        system_prompt: str,
        tools: List[dict],
    ) -> dict:
        """
        Call the LLM via ai.market proxy.

        Returns a dict with:
        - content: list of content blocks
        - usage: {input_tokens, output_tokens}
        - model: model name
        - stop_reason: e.g. "tool_use" or "end_turn"
        """
        try:
            return await self._call_proxy(messages, system_prompt, tools)
        except AllieDisabledError:
            raise
        except Exception as e:
            raise AllieDisabledError(
                f"ai.market agentic call failed: {str(e)[:200]}"
            ) from e

    async def _call_proxy(
        self,
        messages: List[dict],
        system_prompt: str,
        tools: List[dict],
    ) -> dict:
        """Call ai.market proxy for tool-enabled agentic LLM request."""
        from app.config import settings

        base_url = settings.ai_market_url.rstrip("/")
        api_key = settings.internal_api_key
        if not api_key:
            raise ValueError("VECTORAIZ_INTERNAL_API_KEY required for ai.market proxy")

        url = f"{base_url}/api/v1/allie/chat/agentic"
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": api_key,
            "X-Serial": settings.serial or "",
        }
        body = {
            "messages": messages,
            "system": system_prompt,
            "tools": tools,
            "max_tokens": 4096,
            "request_id": f"agentic_{uuid.uuid4().hex[:12]}",
        }

        timeout = httpx.Timeout(120, connect=10)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, json=body, headers=headers)

            if response.status_code == 401:
                raise AllieDisabledError("ai.market authentication failed")
            elif response.status_code == 402:
                raise AllieDisabledError("Insufficient balance on ai.market")
            elif response.status_code != 200:
                raise AllieDisabledError(
                    f"ai.market proxy error ({response.status_code}): {response.text[:200]}"
                )

            data = response.json()
            return data

    def _merge_usage(
        self,
        existing: Optional[AllieUsage],
        new_usage: dict,
        model: str,
    ) -> AllieUsage:
        """Merge usage from multiple iterations."""
        new_input = new_usage.get("input_tokens", 0)
        new_output = new_usage.get("output_tokens", 0)
        new_cost = new_usage.get("cost_cents", 0)

        if existing:
            return AllieUsage(
                input_tokens=existing.input_tokens + new_input,
                output_tokens=existing.output_tokens + new_output,
                cost_cents=existing.cost_cents + new_cost,
                provider=existing.provider,
                model=existing.model,
            )

        return AllieUsage(
            input_tokens=new_input,
            output_tokens=new_output,
            cost_cents=new_cost,
            provider="anthropic",
            model=model,
        )
