"""
Context Window Manager
======================

Manages context windows for multi-turn RAG conversations.
Handles token counting, sliding windows, and context summarization.

Phase: 3.W.3
Created: 2026-01-25
"""

import logging
from dataclasses import dataclass
from typing import List, Optional
from uuid import UUID

from app.models.state import Message, MessageRole
from app.services.session_service import SessionService
from app.services.allie_provider import BaseAllieProvider, get_allie_provider

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ContextWindow:
    """
    Represents a constructed context window ready for LLM consumption.
    """
    messages: List[Message]
    total_tokens: int
    truncated: bool = False
    summary: Optional[str] = None  # Summary of truncated context
    
    @property
    def message_count(self) -> int:
        return len(self.messages)
    
    def to_prompt_format(self) -> List[dict]:
        """Convert to format suitable for LLM API calls."""
        return [
            {"role": msg.role.value, "content": msg.content}
            for msg in self.messages
        ]


@dataclass
class ContextConfig:
    """Configuration for context window management."""
    max_tokens: int = 4000  # Conservative default for smaller models
    summarization_threshold: float = 0.8  # Trigger at 80% of max
    min_recent_messages: int = 2  # Always keep last N messages
    summary_buffer_tokens: int = 300  # Reserve space for summary
    chars_per_token: float = 4.0  # Estimation heuristic


# =============================================================================
# Context Window Manager
# =============================================================================

class ContextWindowManager:
    """
    Manages context windows for RAG conversations.
    
    Features:
    - Token estimation (when not provided)
    - Sliding window to fit budget
    - Automatic summarization of old context
    - Priority: system prompt > recent messages > middle context
    """

    def __init__(
        self,
        session_service: SessionService,
        config: Optional[ContextConfig] = None
    ):
        """
        Initialize context manager.

        Args:
            session_service: Service for message retrieval
            config: Context window configuration
        """
        self.session_service = session_service
        self._allie_provider: Optional[BaseAllieProvider] = None
        self.config = config or ContextConfig()

    @property
    def allie_provider(self) -> BaseAllieProvider:
        """Lazy-load Allie provider."""
        if self._allie_provider is None:
            self._allie_provider = get_allie_provider()
        return self._allie_provider

    def estimate_tokens(self, text: str) -> int:
        """
        Estimate token count from text.
        
        Uses simple heuristic: ~4 characters per token.
        This is approximate but sufficient for local use.
        
        Args:
            text: Input text
            
        Returns:
            Estimated token count
        """
        if not text:
            return 0
        return max(1, int(len(text) / self.config.chars_per_token))

    def calculate_message_tokens(self, message: Message) -> int:
        """
        Get or estimate token count for a message.
        
        Args:
            message: Message to calculate tokens for
            
        Returns:
            Token count (from message or estimated)
        """
        if message.token_count is not None:
            return message.token_count
        return self.estimate_tokens(message.content)

    def build_context(
        self, 
        session_id: UUID, 
        max_tokens: Optional[int] = None,
        include_system_prompt: Optional[str] = None
    ) -> ContextWindow:
        """
        Build a context window for a session.
        
        Args:
            session_id: Session to build context for
            max_tokens: Override max tokens (default from config)
            include_system_prompt: Optional system prompt to prepend
            
        Returns:
            ContextWindow with messages fitting in budget
        """
        max_tokens = max_tokens or self.config.max_tokens
        
        # Get all messages from session
        all_messages = self.session_service.get_messages(session_id, limit=1000)
        
        if not all_messages:
            messages = []
            if include_system_prompt:
                messages.append(Message(
                    session_id=session_id,
                    role=MessageRole.SYSTEM,
                    content=include_system_prompt,
                    token_count=self.estimate_tokens(include_system_prompt)
                ))
            return ContextWindow(
                messages=messages,
                total_tokens=sum(self.calculate_message_tokens(m) for m in messages),
                truncated=False
            )

        # Calculate tokens for all messages
        for msg in all_messages:
            if msg.token_count is None:
                msg.token_count = self.estimate_tokens(msg.content)

        total_tokens = sum(msg.token_count for msg in all_messages)
        
        # Add system prompt tokens if provided
        system_prompt_tokens = self.estimate_tokens(include_system_prompt) if include_system_prompt else 0
        
        # If everything fits, return all
        if total_tokens + system_prompt_tokens <= max_tokens:
            messages = list(all_messages)
            if include_system_prompt:
                messages.insert(0, Message(
                    session_id=session_id,
                    role=MessageRole.SYSTEM,
                    content=include_system_prompt,
                    token_count=system_prompt_tokens
                ))
            return ContextWindow(
                messages=messages,
                total_tokens=total_tokens + system_prompt_tokens,
                truncated=False
            )

        # Need to truncate - build sliding window
        return self._build_sliding_window(
            session_id=session_id,
            messages=all_messages,
            max_tokens=max_tokens,
            system_prompt=include_system_prompt,
            system_prompt_tokens=system_prompt_tokens
        )

    def _build_sliding_window(
        self,
        session_id: UUID,
        messages: List[Message],
        max_tokens: int,
        system_prompt: Optional[str],
        system_prompt_tokens: int
    ) -> ContextWindow:
        """
        Build a sliding window when messages exceed budget.
        
        Priority:
        1. System prompt (always keep)
        2. Recent messages (keep last N)
        3. Middle messages (fill remaining budget)
        4. Old messages (summarize if needed)
        """
        result_messages: List[Message] = []
        current_tokens = 0

        # 1. Add system prompt
        if system_prompt:
            result_messages.append(Message(
                session_id=session_id,
                role=MessageRole.SYSTEM,
                content=system_prompt,
                token_count=system_prompt_tokens
            ))
            current_tokens += system_prompt_tokens

        # 2. Separate existing system messages and conversation
        existing_system = [m for m in messages if m.role == MessageRole.SYSTEM]
        conversation = [m for m in messages if m.role != MessageRole.SYSTEM]
        
        # Add existing system messages
        for sys_msg in existing_system:
            if current_tokens + sys_msg.token_count <= max_tokens:
                result_messages.append(sys_msg)
                current_tokens += sys_msg.token_count

        # 3. Reserve space for recent messages (always keep)
        min_recent = min(self.config.min_recent_messages, len(conversation))
        recent_messages = conversation[-min_recent:] if min_recent > 0 else []
        recent_tokens = sum(m.token_count for m in recent_messages)

        # 4. Calculate remaining budget for middle messages
        budget_after_recent = max_tokens - current_tokens - recent_tokens - self.config.summary_buffer_tokens
        
        # Edge case: not enough room even for recent
        if budget_after_recent < 0:
            logger.warning(
                f"Context extremely tight: system={current_tokens}, recent={recent_tokens}, max={max_tokens}"
            )
            result_messages.extend(recent_messages)
            return ContextWindow(
                messages=result_messages,
                total_tokens=current_tokens + recent_tokens,
                truncated=True,
                summary=None
            )

        # 5. Fill middle with older messages (working backwards)
        middle_candidates = conversation[:-min_recent] if min_recent > 0 else conversation
        middle_selected: List[Message] = []
        
        for msg in reversed(middle_candidates):
            if budget_after_recent >= msg.token_count:
                middle_selected.insert(0, msg)
                budget_after_recent -= msg.token_count
            else:
                # No more room
                break

        # 6. Identify messages that didn't fit (need summarization)
        num_selected = len(middle_selected)
        messages_to_summarize = middle_candidates[:len(middle_candidates) - num_selected]
        
        # 7. Summarize if we dropped messages
        summary = None
        if messages_to_summarize:
            summary = self._summarize_messages_sync(messages_to_summarize)
            if summary:
                summary_msg = Message(
                    session_id=session_id,
                    role=MessageRole.SYSTEM,
                    content=f"[Previous conversation summary]\n{summary}",
                    token_count=self.estimate_tokens(summary) + 30  # overhead
                )
                result_messages.append(summary_msg)
                current_tokens += summary_msg.token_count

        # 8. Assemble final list
        result_messages.extend(middle_selected)
        result_messages.extend(recent_messages)
        
        final_tokens = sum(self.calculate_message_tokens(m) for m in result_messages)

        return ContextWindow(
            messages=result_messages,
            total_tokens=final_tokens,
            truncated=True,
            summary=summary
        )

    def _summarize_messages_sync(self, messages: List[Message]) -> Optional[str]:
        """
        Synchronously summarize messages (for use in sync context).
        
        Note: This blocks - for async contexts, use summarize_messages_async.
        """
        if not messages:
            return None

        # Build conversation text
        conversation_lines = []
        for msg in messages:
            role_label = msg.role.value.capitalize()
            conversation_lines.append(f"{role_label}: {msg.content}")
        
        conversation_text = "\n".join(conversation_lines)
        
        # Truncate if too long
        if len(conversation_text) > 4000:
            conversation_text = conversation_text[:4000] + "\n[truncated...]"


        # Always return a placeholder in sync context (use summarize_messages_async for real summaries)
        return f"(Older context from {len(messages)} messages was summarized for brevity)"

    async def summarize_messages_async(self, messages: List[Message]) -> Optional[str]:
        """
        Asynchronously summarize messages.
        
        Args:
            messages: Messages to summarize
            
        Returns:
            Summary text or None
        """
        if not messages:
            return None

        conversation_lines = []
        for msg in messages:
            role_label = msg.role.value.capitalize()
            conversation_lines.append(f"{role_label}: {msg.content}")
        
        conversation_text = "\n".join(conversation_lines)
        
        if len(conversation_text) > 4000:
            conversation_text = conversation_text[:4000] + "\n[truncated...]"

        prompt = f"""Summarize the following conversation history into a concise paragraph (2-3 sentences).
Retain key facts, user requests, and important decisions. Omit pleasantries.

Conversation:
{conversation_text}

Summary:"""

        try:
            parts: list[str] = []
            async for chunk in self.allie_provider.stream(message=prompt):
                if chunk.text:
                    parts.append(chunk.text)
            return "".join(parts).strip()
        except Exception as e:
            logger.error(f"Failed to summarize messages: {e}")
            return f"(Older context from {len(messages)} messages was truncated)"

    async def build_context_async(
        self,
        session_id: UUID,
        max_tokens: Optional[int] = None,
        include_system_prompt: Optional[str] = None
    ) -> ContextWindow:
        """
        Async version of build_context with proper async summarization.
        """
        # For now, delegate to sync version
        # Full async implementation would require async session_service
        return self.build_context(session_id, max_tokens, include_system_prompt)


# =============================================================================
# Factory Functions
# =============================================================================

def get_context_manager(session_service: SessionService) -> ContextWindowManager:
    """Create a ContextWindowManager with default config."""
    return ContextWindowManager(session_service=session_service)
