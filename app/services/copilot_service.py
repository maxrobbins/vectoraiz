"""
Co-Pilot Service — Metered LLM Interaction Layer
==================================================

Orchestrates Co-Pilot message processing with integrated credit metering:
1. Input sanitization (OWASP-grade — BQ-128 Phase 2)
2. Context assembly + 5-layer system prompt (BQ-128 Phase 2)
3. Pre-flight balance check (check_balance)
4. Claude API call (via AllieProvider — mock in Phase 1)
5. Post-flight usage reporting (report_usage)

Provides three methods:
- ``process_message()`` — Legacy method that raises 402 on insufficient balance.
- ``process_message_metered()`` — Returns (response_text, UsageReport) tuple
  so callers can inspect post-flight balance and handle mid-stream depletion.
- ``process_message_streaming()`` — BQ-128: Streams tokens via send_chunk callback,
  persists response + usage, returns (full_text, AllieUsage).
- ``process_message_agentic()`` — BQ-ALLAI-B: Agentic loop with tool use.

PHASE: BQ-073 — allAI Usage Metering & Prepaid Credits (Sub-tasks 7, 8)
CREATED: S94 (2026-02-06)
UPDATED: S120/BQ-113 (2026-02-12) — session_id/message_id for stable idempotency
UPDATED: BQ-128 Phase 1 (2026-02-14) — Streaming support via AllieProvider
UPDATED: BQ-128 Phase 2 (2026-02-14) — PromptFactory, InputSanitizer, ContextManager
UPDATED: BQ-ALLAI-B (2026-02-16) — Agentic loop with tool use
"""

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from app.services.metering_service import metering_service, UsageReport
from app.services.allie_provider import (
    AllieDisabledError,
    AllieUsage,
    get_allie_provider,
)
from app.services.prompt_factory import (
    RiskMode,
    prompt_factory,
    resolve_tone_mode,
)
from app.services.context_manager_copilot import context_manager
from app.core.input_sanitizer import input_sanitizer
from app.core.local_only_guard import is_local_only
from app.auth.api_key_auth import AuthenticatedUser
from app.models.copilot import StateSnapshot
from app.services.chat_attachment_service import ChatAttachment
from app.services.attachment_blocks import build_user_content as _build_user_content
from fastapi import HTTPException

logger = logging.getLogger(__name__)

# Deflection message per personality spec Section 6 rule 5
INJECTION_DEFLECTION = (
    "That looks like it might be trying to modify my behavior "
    "— I'll stick to helping with your data."
)


class CoPilotService:
    """
    Service for handling Co-Pilot interactions with metering.
    """

    async def process_message(
        self,
        user: AuthenticatedUser,
        api_key: str,
        message: str,
        session_id: str = "",
        message_id: Optional[str] = None,
    ) -> str:
        """
        Process a user message: balance gate → LLM call → usage report.

        Args:
            user: Authenticated user with cached balance from gateway/validate.
            api_key: The user's API key (for legacy compat; user_id used for deduct).
            message: The user's message to Co-Pilot.
            session_id: Session identifier for idempotency key generation.
            message_id: Optional message identifier for idempotency key generation.

        Returns:
            The LLM response text.

        Raises:
            HTTPException 402 if balance is insufficient.
        """
        # 1. Pre-flight balance check
        total_balance = (user.balance_cents or 0) + (user.free_trial_remaining_cents or 0)
        balance_check = metering_service.check_balance(total_balance)

        if not balance_check.allowed:
            raise HTTPException(
                status_code=402,
                detail="Purchase credits to use Co-Pilot",
            )

        # Legacy method — CoPilot integration not available in beta.
        # Streaming (process_message_streaming) and agentic
        # (process_message_agentic) paths are the active interfaces.
        logger.warning(
            "process_message() called but CoPilot legacy path is not implemented. "
            "user=%s session=%s", user.user_id, session_id,
        )
        raise NotImplementedError(
            "CoPilot integration pending — not available in beta"
        )

    async def process_message_metered(
        self,
        user: AuthenticatedUser,
        message: str,
        session_id: str = "",
        message_id: Optional[str] = None,
    ) -> Tuple[str, Optional[UsageReport]]:
        """
        Process a user message with full metering, returning the usage report.

        Unlike ``process_message()``, this does NOT raise HTTPException on
        insufficient balance — the caller is responsible for the pre-flight
        check. This method handles the LLM call and post-flight reporting.

        CancelledError handling (BQ-116): If cancelled before usage report,
        no credits are consumed. If cancelled after report but before return,
        usage was already reported (acceptable).

        Args:
            user: Authenticated user with cached balance from gateway/validate.
            message: The user's message to Co-Pilot.
            session_id: Session identifier for idempotency key generation.
            message_id: Optional message identifier for idempotency key generation.

        Returns:
            Tuple of (response_text, UsageReport or None).
            UsageReport is None only if the LLM call failed before producing
            a response (in which case no credits are consumed).

        Raises:
            asyncio.CancelledError: If the task is cancelled (e.g. STOP).
              Propagated to caller so no partial results are sent.
        """
        # Legacy metered method — CoPilot integration not available in beta.
        # Streaming (process_message_streaming) and agentic
        # (process_message_agentic) paths are the active interfaces.
        logger.warning(
            "process_message_metered() called but CoPilot legacy path is not "
            "implemented. user=%s session=%s", user.user_id, session_id,
        )
        raise NotImplementedError(
            "CoPilot integration pending — not available in beta"
        )

    async def process_message_streaming(
        self,
        user: AuthenticatedUser,
        message: str,
        session_id: str,
        message_id: str,
        send_chunk: Callable[[str], Awaitable[None]],
        state_snapshot: Optional[StateSnapshot] = None,
        user_preferences: Optional[Dict[str, Any]] = None,
        attachments: Optional[list[ChatAttachment]] = None,
        chat_history: Optional[List[Dict[str, str]]] = None,
    ) -> Tuple[str, Optional[AllieUsage]]:
        """
        Stream a metered Co-Pilot response (BQ-128 Phase 1+2).

        Pipeline:
        1. Check standalone guard
        2. Input sanitization (OWASP-grade)
        3. Build runtime context (CoPilotContextManager)
        4. Assemble 5-layer system prompt (PromptFactory)
        5. Handle intro behavior (first message only)
        6. Stream via AllieProvider.stream()
        7. Call send_chunk() for each token
        8. Return full response + usage

        CancelledError: propagated — no usage charged.
        """
        if is_local_only():
            raise AllieDisabledError(
                "Allie requires an ai.market connection. "
                "Switch to connected mode to use Allie."
            )

        # --- Step 2: Input sanitization ---
        sanitize_result = input_sanitizer.sanitize(message, user_id=user.user_id)

        if sanitize_result.injection_detected:
            # Allie deflects in-character per personality spec
            logger.warning(
                "Injection deflected: session=%s user=%s pattern=%s",
                session_id, user.user_id, sanitize_result.injection_pattern,
            )
            await send_chunk(INJECTION_DEFLECTION)
            return INJECTION_DEFLECTION, None

        clean_message = sanitize_result.clean_text

        # --- Step 3: Build runtime context ---
        allie_context = await context_manager.build_context(
            state_snapshot=state_snapshot,
            user_id=user.user_id,
            user_preferences=user_preferences or {},
        )

        # --- Step 4: Assemble system prompt ---
        tone_mode = resolve_tone_mode(
            user_preference=(user_preferences or {}).get("tone_mode"),
        )
        risk_mode = RiskMode.NORMAL  # Elevated/critical set by event triggers (Phase 3)

        system_prompt = prompt_factory.build_system_prompt(
            context=allie_context,
            tone_mode=tone_mode,
            risk_mode=risk_mode,
            tools_available=False,
        )

        # --- Step 5: Stream via provider ---
        # NOTE: Intro message is handled by the frontend (CoPilotContext.tsx).
        # Do not inject server-side intro to avoid double welcome.
        provider = get_allie_provider()
        full_text = ""
        usage: Optional[AllieUsage] = None

        try:
            async for chunk in provider.stream(clean_message, context=system_prompt, attachments=attachments, chat_history=chat_history):
                if chunk.text:
                    full_text += chunk.text
                    await send_chunk(chunk.text)
                if chunk.done and chunk.usage:
                    usage = chunk.usage
        except asyncio.CancelledError:
            # STOP — propagate, no usage charged
            logger.info(
                "Streaming cancelled: session=%s message=%s partial_len=%d",
                session_id, message_id, len(full_text),
            )
            raise

        return full_text, usage

    async def process_message_agentic(
        self,
        user: AuthenticatedUser,
        message: str,
        session_id: str,
        message_id: str,
        send_chunk: Callable[[str], Awaitable[None]],
        send_ws: Callable[[dict], Awaitable[None]],
        send_heartbeat: Callable[[], Awaitable[None]],
        state_snapshot: Optional[StateSnapshot] = None,
        user_preferences: Optional[Dict[str, Any]] = None,
        attachments: Optional[list[ChatAttachment]] = None,
        chat_history: Optional[List[Dict[str, str]]] = None,
    ) -> Tuple[str, Optional[AllieUsage]]:
        """
        Agentic Co-Pilot response with tool use (BQ-ALLAI-B).

        Pipeline:
        1. Check standalone guard
        2. Input sanitization (OWASP-grade)
        3. Build runtime context (CoPilotContextManager)
        4. Assemble 5-layer system prompt (PromptFactory) with tool instructions
        5. Handle intro behavior (first message only)
        6. Run agentic loop: LLM call → tool execution → feed results → repeat
        7. Stream text chunks to frontend
        8. Return full response + usage

        CancelledError: propagated — no usage charged.
        """
        if is_local_only():
            raise AllieDisabledError(
                "Allie requires an ai.market connection. "
                "Switch to connected mode to use Allie."
            )

        # --- Step 2: Input sanitization ---
        sanitize_result = input_sanitizer.sanitize(message, user_id=user.user_id)

        if sanitize_result.injection_detected:
            logger.warning(
                "Injection deflected: session=%s user=%s pattern=%s",
                session_id, user.user_id, sanitize_result.injection_pattern,
            )
            await send_chunk(INJECTION_DEFLECTION)
            return INJECTION_DEFLECTION, None

        clean_message = sanitize_result.clean_text

        # --- Step 3: Build runtime context ---
        allie_context = await context_manager.build_context(
            state_snapshot=state_snapshot,
            user_id=user.user_id,
            user_preferences=user_preferences or {},
        )

        # --- Step 4: Assemble system prompt ---
        tone_mode = resolve_tone_mode(
            user_preference=(user_preferences or {}).get("tone_mode"),
        )
        risk_mode = RiskMode.NORMAL

        system_prompt = prompt_factory.build_system_prompt(
            context=allie_context,
            tone_mode=tone_mode,
            risk_mode=risk_mode,
        )

        # --- Step 5: Agentic loop ---
        # NOTE: Intro message is handled by the frontend (CoPilotContext.tsx).
        # Do not inject server-side intro to avoid double welcome.
        full_text = ""
        from app.services.allai_agentic_provider import AgenticAllieProvider
        from app.services.allai_tools import ALLAI_TOOLS
        from app.services.allai_tool_executor import AllAIToolExecutor

        tool_executor = AllAIToolExecutor(
            user=user,
            send_ws=send_ws,
            session_id=session_id,
        )

        agentic_provider = AgenticAllieProvider()

        # BQ-ALLAI-FILES: Build user message content with attachments
        provider = get_allie_provider()
        user_content = _build_user_content(clean_message, attachments, supports_vision=provider.supports_vision)

        # Build messages from conversation history + current message
        messages = []
        if chat_history:
            for msg in chat_history:
                messages.append({"role": msg["role"], "content": msg["content"]})
        messages.append({"role": "user", "content": user_content})

        try:
            agentic_text, usage = await agentic_provider.run_agentic_loop(
                messages=messages,
                system_prompt=system_prompt,
                tools=ALLAI_TOOLS,
                tool_executor=tool_executor,
                send_chunk=send_chunk,
                send_heartbeat=send_heartbeat,
            )
            full_text += agentic_text
        except asyncio.CancelledError:
            logger.info(
                "Agentic loop cancelled: session=%s message=%s partial_len=%d",
                session_id, message_id, len(full_text),
            )
            raise

        return full_text, usage


copilot_service = CoPilotService()
