"""
RAG Service
===========

Core orchestrator for Retrieval-Augmented Generation in vectorAIz.
Coordinates Search, Prompting, Generation, and Parsing.

Flow (Stateless):
1. User Question → SearchService → Relevant chunks from Qdrant
2. Chunks → PromptRegistry → Grounded prompt with citations
3. Prompt → AllieProvider → Generated answer
4. Answer → CitationParser → Structured response with sources

Flow (Stateful - with session):
1. Load conversation history from session
2. User Question → SearchService → Relevant chunks
3. History + Chunks → ContextWindowManager → Fit in budget
4. Context → PromptRegistry → Grounded prompt
5. Prompt → AllieProvider → Generated answer
6. Persist user message + assistant response to session
7. Answer → CitationParser → Structured response

Phase: 3.V.5 (original), 3.W.4 (stateful upgrade)
Created: 2026-01-25
Updated: 2026-01-25 - Added stateful session support
"""

import json
import logging
import re
import time
from typing import Optional, AsyncGenerator, List, Dict, Any
from uuid import UUID

from sqlmodel import Session as DBSession

from app.core.async_utils import run_sync
from app.services.search_service import get_search_service, SearchService
from app.services.allie_provider import get_allie_provider, BaseAllieProvider, AllieDisabledError
from app.services.prompt_registry import get_prompt_registry, PromptRegistry
from app.services.citation_parser import get_citation_parser, CitationParser
from app.services.session_service import SessionService
from app.services.context_manager import ContextWindowManager, ContextConfig
from app.models.rag import (
    ParsedRAGResponse,
    SourceChunk,
    SourceMetadata,
    Citation
)
from app.models.state import Session, Message, MessageRole

logger = logging.getLogger(__name__)


class RAGService:
    """
    Orchestrates the RAG pipeline.
    
    Supports both:
    - Stateless queries (query method)
    - Stateful conversations (chat method with session_id)
    """

    def __init__(self, db: Optional[DBSession] = None):
        """
        Initialize RAG service.

        Args:
            db: Optional database session for stateful operations.
                If not provided, only stateless queries are available.
        """
        self.search_service: SearchService = get_search_service()
        self._allie: BaseAllieProvider = get_allie_provider()
        self.prompt_registry: PromptRegistry = get_prompt_registry()
        self.citation_parser: CitationParser = get_citation_parser()

        # Stateful components (initialized lazily if db provided)
        self._db = db
        self._session_service: Optional[SessionService] = None
        self._context_manager: Optional[ContextWindowManager] = None

    @property
    def session_service(self) -> SessionService:
        """Get session service (requires db)."""
        if self._db is None:
            raise RuntimeError("Database session required for stateful operations")
        if self._session_service is None:
            self._session_service = SessionService(self._db)
        return self._session_service

    @property
    def context_manager(self) -> ContextWindowManager:
        """Get context manager (requires db)."""
        if self._context_manager is None:
            self._context_manager = ContextWindowManager(
                session_service=self.session_service,
            )
        return self._context_manager

    # =========================================================================
    # STATELESS QUERY (Original Phase 3.V.5)
    # =========================================================================

    async def query(
        self,
        question: str,
        dataset_id: Optional[str] = None,
        top_k: int = 5,
        min_score: float = 0.3,
        template: str = "rag_qa",
        **llm_kwargs
    ) -> ParsedRAGResponse:
        """
        Execute a stateless RAG query (no conversation memory).

        Args:
            question: User's natural language question
            dataset_id: Optional filter for specific dataset
            top_k: Number of context chunks to retrieve
            min_score: Minimum similarity score for retrieval
            template: Name of the prompt template to use
            **llm_kwargs: Extra args passed to LLM (temperature, max_tokens)

        Returns:
            Structured response with answer, citations, and metrics.
        """
        t0 = time.perf_counter()
        
        # === STEP 1: RETRIEVAL ===
        logger.info(f"RAG Query: '{question[:50]}...' (dataset={dataset_id}, top_k={top_k})")
        
        search_result = await run_sync(
            self.search_service.search,
            question, dataset_id, top_k, min_score,
        )

        t1 = time.perf_counter()
        retrieval_time_ms = (t1 - t0) * 1000

        # Map results to SourceChunks
        source_chunks = self._map_results_to_chunks(search_result.get("results", []))

        logger.info(f"Retrieved {len(source_chunks)} chunks in {retrieval_time_ms:.2f}ms")

        # === SETUP QUESTION FALLBACK ===
        # If this is a setup/connection question, use the setup_guide template
        # regardless of whether we found dataset context.
        is_setup = self._is_setup_question(question)

        if is_setup:
            logger.info("Detected setup/connection question — using setup_guide template")
            setup_context = self._build_setup_context()
            try:
                prompt = self.prompt_registry.render(
                    template_name="setup_guide",
                    question=question,
                    setup_context=setup_context,
                    context_chunks=source_chunks,  # may be empty, that's fine
                )
            except Exception as e:
                logger.error(f"Setup guide prompt rendering failed: {e}")
                raise ValueError(f"Failed to render setup_guide template: {str(e)}")

        elif not source_chunks:
            # Handle no context found (non-setup question)
            logger.info("No relevant context found for query.")
            return ParsedRAGResponse(
                answer="I couldn't find any relevant information in the provided datasets to answer your question.",
                citations=[],
                unique_sources_cited=[],
                chunks_retrieved=0,
                retrieval_time_ms=retrieval_time_ms,
                generation_time_ms=0.0,
                total_time_ms=(time.perf_counter() - t0) * 1000,
                template_used=template
            )

        else:
            # === STEP 2: PROMPT ENGINEERING (normal path) ===
            try:
                prompt = self.prompt_registry.render(
                    template_name=template,
                    question=question,
                    context_chunks=source_chunks
                )
                logger.debug(f"Rendered prompt ({len(prompt)} chars) using template '{template}'")
            except Exception as e:
                logger.error(f"Prompt rendering failed: {e}")
                raise ValueError(f"Failed to render prompt template '{template}': {str(e)}")

        # === STEP 3: GENERATION ===
        t2 = time.perf_counter()

        try:
            parts: List[str] = []
            async for chunk in self._allie.stream(message=prompt):
                if chunk.text:
                    parts.append(chunk.text)
            raw_response = "".join(parts)
        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            raise RuntimeError(f"LLM generation failed: {str(e)}")
            
        t3 = time.perf_counter()
        generation_time_ms = (t3 - t2) * 1000
        
        logger.info(f"Generated response ({len(raw_response)} chars) in {generation_time_ms:.2f}ms")

        # === STEP 4: PARSING & VALIDATION ===
        parsed_response = self.citation_parser.parse(
            text=raw_response,
            source_chunks=source_chunks
        )

        # Add metrics to response
        parsed_response.retrieval_time_ms = retrieval_time_ms
        parsed_response.generation_time_ms = generation_time_ms
        parsed_response.total_time_ms = (t3 - t0) * 1000
        parsed_response.template_used = "setup_guide" if is_setup else template
        parsed_response.model_used = type(self._allie).__name__
        parsed_response.chunks_retrieved = len(source_chunks)

        logger.info(
            f"RAG complete: {len(parsed_response.citations)} citations, "
            f"{len(parsed_response.unique_sources_cited)} unique sources, "
            f"total {parsed_response.total_time_ms:.2f}ms"
        )

        return parsed_response

    # =========================================================================
    # STATEFUL CHAT (Phase 3.W.4)
    # =========================================================================

    async def chat(
        self,
        session_id: UUID,
        question: str,
        dataset_id: Optional[str] = None,
        top_k: int = 5,
        min_score: float = 0.3,
        template: str = "rag_chat",
        max_context_tokens: int = 4000,
        **llm_kwargs
    ) -> ParsedRAGResponse:
        """
        Execute a stateful RAG chat with conversation memory.
        
        This method:
        1. Loads conversation history from the session
        2. Retrieves relevant context from datasets
        3. Builds a context window (history + retrieval)
        4. Generates a response
        5. Persists the user message and assistant response
        
        Args:
            session_id: UUID of the chat session
            question: User's message
            dataset_id: Optional filter for specific dataset
            top_k: Number of context chunks to retrieve
            min_score: Minimum similarity score for retrieval
            template: Prompt template (default: rag_chat for conversations)
            max_context_tokens: Maximum tokens for context window
            **llm_kwargs: Extra args passed to LLM
            
        Returns:
            Structured response with answer, citations, and metrics.
        """
        if self._db is None:
            raise RuntimeError("Database session required for chat(). Use query() for stateless RAG.")
        
        t0 = time.perf_counter()
        
        # Verify session exists
        session = self.session_service.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")
        
        logger.info(f"RAG Chat: session={session_id}, question='{question[:50]}...'")
        
        # === STEP 1: PERSIST USER MESSAGE ===
        user_msg_tokens = self.context_manager.estimate_tokens(question)
        user_message = self.session_service.add_message(
            session_id=session_id,
            role=MessageRole.USER,
            content=question,
            token_count=user_msg_tokens
        )
        
        # === STEP 2: RETRIEVAL ===
        search_result = await run_sync(
            self.search_service.search,
            question, dataset_id or session.dataset_id, top_k, min_score,
        )

        t1 = time.perf_counter()
        retrieval_time_ms = (t1 - t0) * 1000

        source_chunks = self._map_results_to_chunks(search_result.get("results", []))
        logger.info(f"Retrieved {len(source_chunks)} chunks in {retrieval_time_ms:.2f}ms")
        
        # === STEP 3: BUILD CONTEXT WINDOW ===
        # Get conversation history that fits in budget (minus space for retrieval)
        retrieval_tokens = sum(self.context_manager.estimate_tokens(c.text) for c in source_chunks)
        history_budget = max(0, max_context_tokens - retrieval_tokens - 500)  # Reserve for prompt overhead

        context_window = self.context_manager.build_context(
            session_id=session_id,
            max_tokens=history_budget
        )

        logger.debug(
            f"Context window: {context_window.message_count} messages, "
            f"{context_window.total_tokens} tokens, truncated={context_window.truncated}"
        )
        
        # === STEP 4: PROMPT ENGINEERING ===
        # Build conversation history for prompt
        conversation_history = self._format_history_for_prompt(context_window.messages)
        
        try:
            prompt = self.prompt_registry.render(
                template_name=template,
                question=question,
                context_chunks=source_chunks,
                conversation_history=conversation_history,
                context_summary=context_window.summary
            )
        except Exception as e:
            logger.error(f"Prompt rendering failed: {e}")
            raise ValueError(f"Failed to render prompt template '{template}': {str(e)}")
        
        # === STEP 5: GENERATION ===
        t2 = time.perf_counter()

        try:
            parts_: List[str] = []
            async for chunk in self._allie.stream(message=prompt):
                if chunk.text:
                    parts_.append(chunk.text)
            raw_response = "".join(parts_)
        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            raise RuntimeError(f"LLM generation failed: {str(e)}")
        
        t3 = time.perf_counter()
        generation_time_ms = (t3 - t2) * 1000
        
        # === STEP 6: PERSIST ASSISTANT RESPONSE ===
        assistant_tokens = self.context_manager.estimate_tokens(raw_response)
        
        # Build metadata with citation info
        parsed_response = self.citation_parser.parse(
            text=raw_response,
            source_chunks=source_chunks
        )
        
        response_metadata = {
            "citations_count": len(parsed_response.citations),
            "sources_used": [c.metadata.source_id for c in parsed_response.unique_sources_cited],
            "retrieval_time_ms": retrieval_time_ms,
            "generation_time_ms": generation_time_ms,
        }
        
        assistant_message = self.session_service.add_message(
            session_id=session_id,
            role=MessageRole.ASSISTANT,
            content=raw_response,
            token_count=assistant_tokens,
            metadata=response_metadata
        )
        
        # === STEP 7: BUILD RESPONSE ===
        parsed_response.retrieval_time_ms = retrieval_time_ms
        parsed_response.generation_time_ms = generation_time_ms
        parsed_response.total_time_ms = (t3 - t0) * 1000
        parsed_response.template_used = template
        parsed_response.model_used = type(self._allie).__name__
        parsed_response.chunks_retrieved = len(source_chunks)

        # Add session info to response
        parsed_response.session_id = str(session_id)
        parsed_response.message_id = str(assistant_message.id)
        
        logger.info(
            f"Chat complete: session={session_id}, "
            f"{len(parsed_response.citations)} citations, "
            f"total {parsed_response.total_time_ms:.2f}ms"
        )
        
        return parsed_response

    async def chat_stream(
        self,
        session_id: UUID,
        question: str,
        dataset_id: Optional[str] = None,
        top_k: int = 5,
        min_score: float = 0.3,
        template: str = "rag_chat",
        max_context_tokens: int = 4000,
        **llm_kwargs
    ) -> AsyncGenerator[str, None]:
        """
        Stream a stateful RAG chat response.
        
        Note: Persists messages after streaming completes.
        Full response is collected internally for persistence.
        """
        if self._db is None:
            raise RuntimeError("Database session required for chat_stream()")
        
        session = self.session_service.get_session(session_id)
        if not session:
            raise ValueError(f"Session {session_id} not found")
        
        # Persist user message
        user_msg_tokens = self.context_manager.estimate_tokens(question)
        self.session_service.add_message(
            session_id=session_id,
            role=MessageRole.USER,
            content=question,
            token_count=user_msg_tokens
        )
        
        # Retrieval
        search_result = await run_sync(
            self.search_service.search,
            question, dataset_id or session.dataset_id, top_k, min_score,
        )
        source_chunks = self._map_results_to_chunks(search_result.get("results", []))

        # Context window
        retrieval_tokens = sum(self.context_manager.estimate_tokens(c.text) for c in source_chunks)
        history_budget = max(0, max_context_tokens - retrieval_tokens - 500)

        context_window = self.context_manager.build_context(
            session_id=session_id,
            max_tokens=history_budget
        )
        
        # Build prompt
        conversation_history = self._format_history_for_prompt(context_window.messages)
        prompt = self.prompt_registry.render(
            template_name=template,
            question=question,
            context_chunks=source_chunks,
            conversation_history=conversation_history,
            context_summary=context_window.summary
        )
        
        # Stream and collect response — persist even on disconnect (partial)
        full_response = []
        try:
            async for chunk in self._allie.stream(message=prompt):
                if chunk.text:
                    full_response.append(chunk.text)
                    yield chunk.text
        finally:
            response_text = "".join(full_response)
            if response_text:
                assistant_tokens = self.context_manager.estimate_tokens(response_text)
                self.session_service.add_message(
                    session_id=session_id,
                    role=MessageRole.ASSISTANT,
                    content=response_text,
                    token_count=assistant_tokens
                )

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    # Setup/connection question detection patterns
    _SETUP_PATTERNS = re.compile(
        r"(?i)\b("
        r"connect\s*(chatgpt|gpt|claude|llm|mcp|api|client)"
        r"|custom\s*gpt"
        r"|gpt\s*action"
        r"|mcp\s*(server|connector|setup|config)"
        r"|how\s*(do\s*i|to|can\s*i)\s*(connect|hook\s*up|link|integrate|use|set\s*up|configure)"
        r"|setup\s*guide"
        r"|connect.*to\s*(my\s*)?data"
        r"|openapi\s*spec"
        r"|api\s*key.*where"
        r"|where.*api\s*key"
        r"|action\s*spec"
        r")\b"
    )

    @staticmethod
    def _is_setup_question(question: str) -> bool:
        """Detect if the question is about connecting LLM clients to vectorAIz."""
        return bool(RAGService._SETUP_PATTERNS.search(question))

    @staticmethod
    def _build_setup_context() -> str:
        """Build the setup knowledge context string for the setup_guide template."""
        from app.config import settings
        base_url = settings.public_url.rstrip("/")
        spec_url = f"{base_url}/api/allai/openapi-action-spec"

        return f"""
vectorAIz Connection Guide
===========================

This vectorAIz instance is available at: {base_url}

OPTION 1: ChatGPT Custom GPT with Actions
------------------------------------------
1. Go to https://chatgpt.com → Profile → My GPTs → Create a GPT.
2. Name it something like "My Data Assistant".
3. In Instructions, write: "You help the user search and ask questions about their data stored in vectorAIz. Use the askVectoraiz action to answer questions and searchVectoraiz to find information."
4. Under Actions, click "Create new action".
5. Import the OpenAPI spec from: {spec_url}
   (Or copy the JSON from that URL and paste into the schema editor.)
6. Under Authentication, choose "API Key", set Auth Type to "Custom", Header Name to "X-API-Key", and paste your vectorAIz API key.
7. Save and test!

OPTION 2: Claude Desktop via MCP
---------------------------------
1. Open your Claude Desktop config file:
   - macOS: ~/Library/Application Support/Claude/claude_desktop_config.json
   - Windows: %APPDATA%\\Claude\\claude_desktop_config.json
2. Add under "mcpServers":
   "vectoraiz": {{
     "command": "npx",
     "args": ["-y", "@anthropic-ai/mcp-remote", "{base_url}/mcp/sse"],
     "env": {{ "API_KEY": "YOUR_VECTORAIZ_API_KEY_HERE" }}
   }}
3. Replace YOUR_VECTORAIZ_API_KEY_HERE with your real API key.
4. Restart Claude Desktop — vectorAIz tools will appear automatically.
Note: MCP requires the Connectivity feature to be enabled in Settings → Connectivity.

OPTION 3: Direct REST API
--------------------------
Ask a question:
  POST {base_url}/api/allai/generate
  Headers: X-API-Key: YOUR_API_KEY, Content-Type: application/json
  Body: {{"question": "What is the total revenue?", "top_k": 5}}

Search your data:
  GET {base_url}/api/search/?q=revenue&limit=10
  Headers: X-API-Key: YOUR_API_KEY

Where to Find Your API Key
----------------------------
Go to the vectorAIz dashboard → Settings → API Keys → Create New Key.
Your key looks like: vz_xxxxxxxx_xxxxxxxxxxxxxxxx...
Keep it secret — treat it like a password.

OpenAPI Spec URL (for Custom GPT Actions):
{spec_url}
"""

    def _format_history_for_prompt(self, messages: List[Message]) -> str:
        """Format conversation history for inclusion in prompt."""
        if not messages:
            return ""
        
        lines = []
        for msg in messages:
            if msg.role == MessageRole.SYSTEM:
                continue  # Skip system messages in history display
            role_label = "User" if msg.role == MessageRole.USER else "Assistant"
            lines.append(f"{role_label}: {msg.content}")
        
        return "\n".join(lines)

    async def query_stream(
        self,
        question: str,
        dataset_id: Optional[str] = None,
        top_k: int = 5,
        min_score: float = 0.3,
        template: str = "rag_qa",
        **llm_kwargs
    ) -> AsyncGenerator[str, None]:
        """
        Stream the RAG response text (stateless).

        Yields text chunks followed by a final JSON event with parsed citations:
            {"type": "citations", "citations": [...], "sources": [...]}
        """
        # Retrieval
        search_result = await run_sync(
            self.search_service.search,
            question, dataset_id, top_k, min_score,
        )

        source_chunks = self._map_results_to_chunks(search_result.get("results", []))

        # Setup question fallback (streaming)
        is_setup = self._is_setup_question(question)

        if is_setup:
            setup_context = self._build_setup_context()
            prompt = self.prompt_registry.render(
                template_name="setup_guide",
                question=question,
                setup_context=setup_context,
                context_chunks=source_chunks,
            )
        elif not source_chunks:
            yield "I couldn't find any relevant information in the provided datasets to answer your question."
            return
        else:
            # Prompt Engineering (normal path)
            prompt = self.prompt_registry.render(
                template_name=template,
                question=question,
                context_chunks=source_chunks
            )

        # Streaming Generation — collect full response for citation parsing
        full_response: List[str] = []
        async for chunk in self._allie.stream(message=prompt):
            if chunk.text:
                full_response.append(chunk.text)
                yield chunk.text

        # Final event: parsed citations
        response_text = "".join(full_response)
        parsed = self.citation_parser.parse(text=response_text, source_chunks=source_chunks)
        citations_event = {
            "type": "citations",
            "citations": [
                {"source_index": c.source_index, "is_valid": c.is_valid}
                for c in parsed.citations
            ],
            "sources": [
                {
                    "index": s.index,
                    "source_id": s.metadata.source_id,
                    "dataset_id": s.metadata.dataset_id,
                    "filename": s.metadata.filename,
                    "score": s.metadata.score,
                }
                for s in parsed.unique_sources_cited
            ],
        }
        yield json.dumps(citations_event)

    def _map_results_to_chunks(self, results: List[Dict[str, Any]]) -> List[SourceChunk]:
        """Convert raw search results to SourceChunk objects."""
        chunks = []
        for i, res in enumerate(results, 1):
            # Prefer stable row_id from payload; fall back to dataset_id + row_index
            source_id = res.get('row_id') or f"{res.get('dataset_id', 'unknown')}:{res.get('row_index', i)}"
            metadata = SourceMetadata(
                source_id=source_id,
                dataset_id=res.get('dataset_id'),
                filename=res.get('dataset_name') or res.get('filename'),
                row_index=res.get('row_index'),
                score=res.get('score'),
                extra=res.get('row_data', {})
            )

            chunk = SourceChunk(
                index=i,
                text=res.get('text_content', ''),
                metadata=metadata
            )
            chunks.append(chunk)

        return chunks

    def get_status(self) -> Dict[str, Any]:
        """Get service status including LLM provider info."""
        status = {
            "service": "RAGService",
            "status": "healthy",
            "llm": {"provider": type(self._allie).__name__},
            "templates": self.prompt_registry.list_templates(),
            "stateful_enabled": self._db is not None,
        }
        return status


# =============================================================================
# Factory Functions
# =============================================================================

# Singleton for stateless usage
_rag_service: Optional[RAGService] = None


def get_rag_service() -> RAGService:
    """Get the singleton RAG service instance (stateless)."""
    global _rag_service
    if _rag_service is None:
        _rag_service = RAGService()
    return _rag_service


def get_rag_service_with_db(db: DBSession) -> RAGService:
    """Get a RAG service instance with database support (stateful)."""
    return RAGService(db=db)


def reset_rag_service():
    """Reset the singleton (useful for testing or config changes)."""
    global _rag_service
    _rag_service = None
