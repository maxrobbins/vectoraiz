"""
Tests for BQ-118: RAG & LLM Reliability
========================================

Covers:
  AC1 — history_budget clamped to max(0, ...)
  AC2 — chat_stream persists assistant message in try/finally (even partial)
  AC3 — Indexing payload includes dataset_id, filename, stable row identifier
  AC4 — _map_results_to_chunks uses correct field names
  AC5 — query_stream returns final event with parsed citations
  AC6 — Type preservation in row_data (datetime→ISO, UUID→str with type tag)
"""

import json
import pytest
from datetime import datetime, date
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

from app.services.rag_service import RAGService
from app.services.indexing_service import IndexingService
from app.services.allie_provider import AllieStreamChunk
from app.models.rag import SourceChunk, SourceMetadata


# Helper: async version of run_sync that just calls the sync function directly
async def _fake_run_sync(fn, *args, **kwargs):
    return fn(*args, **kwargs)


# ============================================================================
# AC1 — history_budget clamped to max(0, ...)
# ============================================================================

class TestHistoryBudgetClamping:
    """AC1: history_budget must never go negative."""

    def _make_rag_service_with_mocks(self):
        """Build a RAGService with all dependencies mocked for chat tests."""
        svc = RAGService.__new__(RAGService)
        svc.search_service = MagicMock()
        svc._allie = MagicMock()
        svc.prompt_registry = MagicMock()
        svc.citation_parser = MagicMock()
        svc._db = MagicMock()
        svc._session_service = MagicMock()

        # Context manager mock
        cm = MagicMock()
        cm.estimate_tokens = MagicMock(side_effect=lambda t: len(t) // 4)
        cm.build_context = MagicMock()
        ctx_window = MagicMock()
        ctx_window.messages = []
        ctx_window.message_count = 0
        ctx_window.total_tokens = 0
        ctx_window.truncated = False
        ctx_window.summary = None
        cm.build_context.return_value = ctx_window
        svc._context_manager = cm
        return svc

    @pytest.mark.asyncio
    async def test_chat_history_budget_clamped_when_retrieval_exceeds_max(self):
        """When retrieval_tokens + 500 > max_context_tokens, budget should be 0, not negative."""
        svc = self._make_rag_service_with_mocks()

        # Session exists
        session_mock = MagicMock()
        session_mock.dataset_id = "ds1"
        svc._session_service.get_session.return_value = session_mock
        svc._session_service.add_message.return_value = MagicMock(id=uuid4())

        # Search returns results with huge text → tokens will exceed budget
        big_text = "x" * 20000  # 5000 tokens at 4 chars/token
        svc.search_service.search = MagicMock(return_value={
            "results": [
                {"dataset_id": "ds1", "dataset_name": "f.csv", "score": 0.9,
                 "row_index": 0, "text_content": big_text, "row_data": {}}
            ]
        })

        svc.prompt_registry.render.return_value = "prompt"

        async def fake_allie_stream(**kw):
            yield AllieStreamChunk(text="answer [1]")

        svc._allie.stream = fake_allie_stream
        svc.citation_parser.parse.return_value = MagicMock(
            citations=[], unique_sources_cited=[],
            retrieval_time_ms=None, generation_time_ms=None,
            total_time_ms=None, template_used=None, model_used=None,
            chunks_retrieved=0, session_id=None, message_id=None,
        )

        # max_context_tokens=1000 but retrieval alone is ~5000 tokens
        with patch("app.services.rag_service.run_sync", side_effect=_fake_run_sync):
            await svc.chat(
                session_id=uuid4(),
                question="test",
                max_context_tokens=1000,
            )

        # The build_context call should have received max_tokens >= 0
        call_kwargs = svc._context_manager.build_context.call_args
        budget = call_kwargs[1].get("max_tokens")
        assert budget is not None
        assert budget >= 0, f"history_budget was {budget}, expected >= 0"

    @pytest.mark.asyncio
    async def test_chat_stream_history_budget_clamped(self):
        """chat_stream must also clamp history_budget to >= 0."""
        svc = self._make_rag_service_with_mocks()

        session_mock = MagicMock()
        session_mock.dataset_id = "ds1"
        svc._session_service.get_session.return_value = session_mock
        svc._session_service.add_message.return_value = MagicMock(id=uuid4())

        big_text = "y" * 20000
        svc.search_service.search = MagicMock(return_value={
            "results": [
                {"dataset_id": "ds1", "dataset_name": "f.csv", "score": 0.9,
                 "row_index": 0, "text_content": big_text, "row_data": {}}
            ]
        })
        svc.prompt_registry.render.return_value = "prompt"

        async def fake_stream(**kw):
            yield AllieStreamChunk(text="chunk")

        svc._allie.stream = fake_stream

        with patch("app.services.rag_service.run_sync", side_effect=_fake_run_sync):
            chunks = []
            async for c in svc.chat_stream(
                session_id=uuid4(),
                question="test",
                max_context_tokens=1000,
            ):
                chunks.append(c)

        call_kwargs = svc._context_manager.build_context.call_args
        budget = call_kwargs[1].get("max_tokens")
        assert budget is not None
        assert budget >= 0, f"history_budget was {budget}, expected >= 0"


# ============================================================================
# AC2 — chat_stream persists assistant message in try/finally
# ============================================================================

class TestChatStreamPersistence:
    """AC2: chat_stream must persist partial responses on disconnect."""

    def _make_stream_service(self):
        svc = RAGService.__new__(RAGService)
        svc.search_service = MagicMock()
        svc._allie = MagicMock()
        svc.prompt_registry = MagicMock()
        svc.citation_parser = MagicMock()
        svc._db = MagicMock()
        svc._session_service = MagicMock()

        cm = MagicMock()
        cm.estimate_tokens = MagicMock(return_value=10)
        ctx_window = MagicMock()
        ctx_window.messages = []
        ctx_window.summary = None
        cm.build_context.return_value = ctx_window
        svc._context_manager = cm

        session_mock = MagicMock()
        session_mock.dataset_id = "ds1"
        svc._session_service.get_session.return_value = session_mock
        svc._session_service.add_message.return_value = MagicMock(id=uuid4())

        svc.search_service.search = MagicMock(return_value={
            "results": [
                {"dataset_id": "ds1", "dataset_name": "f.csv", "score": 0.9,
                 "row_index": 0, "text_content": "context", "row_data": {}}
            ]
        })
        svc.prompt_registry.render.return_value = "prompt"
        return svc

    @pytest.mark.asyncio
    async def test_chat_stream_persists_on_early_break(self):
        """Even if consumer breaks mid-stream, the partial response is persisted."""
        svc = self._make_stream_service()

        async def fake_stream(**kw):
            yield AllieStreamChunk(text="Hello ")
            yield AllieStreamChunk(text="World ")
            yield AllieStreamChunk(text="More")

        svc._allie.stream = fake_stream

        with patch("app.services.rag_service.run_sync", side_effect=_fake_run_sync):
            collected = []
            gen = svc.chat_stream(
                session_id=uuid4(),
                question="hi",
                max_context_tokens=4000,
            )
            # Only consume first chunk, then explicitly close (simulates disconnect)
            async for c in gen:
                collected.append(c)
                if len(collected) == 1:
                    await gen.aclose()
                    break

        # add_message should still have been called for the assistant (the finally block)
        # First call = user message, second call = assistant partial
        calls = svc._session_service.add_message.call_args_list
        assert len(calls) >= 2, f"Expected at least 2 add_message calls, got {len(calls)}"
        last_call = calls[-1]
        # Verify it was an ASSISTANT message
        from app.models.state import MessageRole
        assert last_call[1]["role"] == MessageRole.ASSISTANT
        # Content should be the partial text collected before break
        assert "Hello " in last_call[1]["content"]

    @pytest.mark.asyncio
    async def test_chat_stream_persists_full_response(self):
        """Normal completion also persists the full response."""
        svc = self._make_stream_service()

        async def fake_stream(**kw):
            yield AllieStreamChunk(text="Full ")
            yield AllieStreamChunk(text="response.")

        svc._allie.stream = fake_stream

        with patch("app.services.rag_service.run_sync", side_effect=_fake_run_sync):
            chunks = []
            async for c in svc.chat_stream(
                session_id=uuid4(),
                question="hi",
                max_context_tokens=4000,
            ):
                chunks.append(c)

        assert "".join(chunks) == "Full response."
        # Last add_message should contain full text
        calls = svc._session_service.add_message.call_args_list
        last_call = calls[-1]
        assert last_call[1]["content"] == "Full response."

    @pytest.mark.asyncio
    async def test_chat_stream_no_persist_when_empty(self):
        """If stream yields nothing, no assistant message should be persisted."""
        svc = self._make_stream_service()

        async def fake_stream(**kw):
            if False:  # make it an async generator
                yield

        svc._allie.stream = fake_stream

        with patch("app.services.rag_service.run_sync", side_effect=_fake_run_sync):
            chunks = []
            async for c in svc.chat_stream(
                session_id=uuid4(),
                question="hi",
                max_context_tokens=4000,
            ):
                chunks.append(c)

        assert chunks == []
        # Only the user message should have been persisted (1 call)
        calls = svc._session_service.add_message.call_args_list
        assert len(calls) == 1


# ============================================================================
# AC3 — Indexing payload includes dataset_id, filename, stable row identifier
# ============================================================================

class TestIndexingPayload:
    """AC3: Indexing payload must include dataset_id, filename, row_id."""

    def test_payload_includes_required_fields(self):
        """Payload built during indexing includes dataset_id, filename, row_id."""
        svc = IndexingService.__new__(IndexingService)
        svc.embedding_service = MagicMock()
        svc.qdrant_service = MagicMock()
        svc.duckdb_service = MagicMock()

        # Mock dependencies
        svc.qdrant_service.create_collection = MagicMock()
        svc.duckdb_service.get_file_metadata.return_value = {}
        svc.embedding_service.embed_texts.return_value = [[0.1] * 384]
        svc.qdrant_service.upsert_vectors.return_value = {"upserted": 1}

        from pathlib import Path
        svc._detect_text_columns = MagicMock(return_value=["name"])
        svc._extract_rows = MagicMock(return_value=[{"name": "Acme Corp", "id": 1}])

        result = svc.index_dataset(
            dataset_id="ds_abc",
            filepath=Path("/data/companies.parquet"),
        )

        assert result["status"] == "completed"

        # Inspect the payloads passed to upsert_vectors
        call_args = svc.qdrant_service.upsert_vectors.call_args
        payloads = call_args[1]["payloads"]
        assert len(payloads) == 1

        payload = payloads[0]
        assert payload["dataset_id"] == "ds_abc"
        assert payload["filename"] == "companies.parquet"
        assert payload["row_id"] == "ds_abc:0"
        assert payload["row_index"] == 0
        assert "text_content" in payload
        assert "row_data" in payload

    def test_payload_row_id_is_stable_across_rows(self):
        """row_id follows pattern dataset_id:row_index for each row."""
        svc = IndexingService.__new__(IndexingService)
        svc.embedding_service = MagicMock()
        svc.qdrant_service = MagicMock()
        svc.duckdb_service = MagicMock()

        svc.qdrant_service.create_collection = MagicMock()
        svc.duckdb_service.get_file_metadata.return_value = {}
        svc.embedding_service.embed_texts.return_value = [[0.1] * 384, [0.2] * 384]
        svc.qdrant_service.upsert_vectors.return_value = {"upserted": 2}

        from pathlib import Path
        svc._detect_text_columns = MagicMock(return_value=["name"])
        svc._extract_rows = MagicMock(return_value=[
            {"name": "Acme"},
            {"name": "Beta"},
        ])

        svc.index_dataset(dataset_id="ds_x", filepath=Path("/data/x.parquet"))

        payloads = svc.qdrant_service.upsert_vectors.call_args[1]["payloads"]
        assert payloads[0]["row_id"] == "ds_x:0"
        assert payloads[1]["row_id"] == "ds_x:1"


# ============================================================================
# AC4 — _map_results_to_chunks uses correct field names
# ============================================================================

class TestMapResultsToChunks:
    """AC4: _map_results_to_chunks must map all fields correctly."""

    def test_uses_row_id_when_available(self):
        """Prefers stable row_id from search result for source_id."""
        svc = RAGService.__new__(RAGService)
        results = [
            {
                "dataset_id": "ds1",
                "dataset_name": "data.csv",
                "score": 0.95,
                "row_index": 3,
                "text_content": "Some text",
                "row_data": {"col": "val"},
                "row_id": "ds1:3",
            }
        ]
        chunks = svc._map_results_to_chunks(results)
        assert len(chunks) == 1
        assert chunks[0].metadata.source_id == "ds1:3"
        assert chunks[0].metadata.dataset_id == "ds1"
        assert chunks[0].metadata.filename == "data.csv"
        assert chunks[0].metadata.row_index == 3
        assert chunks[0].metadata.score == 0.95
        assert chunks[0].text == "Some text"

    def test_fallback_source_id_without_row_id(self):
        """Falls back to dataset_id:row_index when row_id is missing."""
        svc = RAGService.__new__(RAGService)
        results = [
            {
                "dataset_id": "ds2",
                "dataset_name": "old.csv",
                "score": 0.8,
                "row_index": 7,
                "text_content": "Old text",
                "row_data": {},
            }
        ]
        chunks = svc._map_results_to_chunks(results)
        assert chunks[0].metadata.source_id == "ds2:7"

    def test_filename_fallback_to_payload_filename(self):
        """When dataset_name is absent, falls back to 'filename' field."""
        svc = RAGService.__new__(RAGService)
        results = [
            {
                "dataset_id": "ds3",
                "score": 0.7,
                "row_index": 0,
                "text_content": "text",
                "row_data": {},
                "filename": "from_payload.parquet",
            }
        ]
        chunks = svc._map_results_to_chunks(results)
        assert chunks[0].metadata.filename == "from_payload.parquet"

    def test_empty_results(self):
        """Empty results list returns empty chunks."""
        svc = RAGService.__new__(RAGService)
        assert svc._map_results_to_chunks([]) == []


# ============================================================================
# AC5 — query_stream returns final event with parsed citations
# ============================================================================

class TestQueryStreamCitations:
    """AC5: query_stream must yield a final JSON event with citations."""

    @pytest.mark.asyncio
    async def test_query_stream_emits_citations_event(self):
        """The last yielded chunk should be a JSON citations event."""
        svc = RAGService.__new__(RAGService)
        svc.search_service = MagicMock()
        svc._allie = MagicMock()
        svc.prompt_registry = MagicMock()
        svc.citation_parser = MagicMock()

        svc.search_service.search = MagicMock(return_value={
            "results": [
                {"dataset_id": "ds1", "dataset_name": "f.csv", "score": 0.9,
                 "row_index": 0, "text_content": "context about cats", "row_data": {}},
            ]
        })
        svc.prompt_registry.render.return_value = "prompt"

        async def fake_stream(**kw):
            yield AllieStreamChunk(text="Cats are great ")
            yield AllieStreamChunk(text="[1].")

        svc._allie.stream = fake_stream

        from app.models.rag import ParsedRAGResponse, Citation
        parsed = ParsedRAGResponse(
            answer="Cats are great [1].",
            citations=[Citation(source_index=1, is_valid=True)],
            unique_sources_cited=[
                SourceChunk(
                    index=1,
                    text="context about cats",
                    metadata=SourceMetadata(
                        source_id="ds1:0",
                        dataset_id="ds1",
                        filename="f.csv",
                        score=0.9,
                    ),
                )
            ],
        )
        svc.citation_parser.parse.return_value = parsed

        with patch("app.services.rag_service.run_sync", side_effect=_fake_run_sync):
            chunks = []
            async for c in svc.query_stream(question="tell me about cats"):
                chunks.append(c)

        # Should have text chunks + a final JSON event
        assert len(chunks) >= 3  # "Cats are great ", "[1].", json event

        # Last chunk should be valid JSON with type=citations
        last = json.loads(chunks[-1])
        assert last["type"] == "citations"
        assert len(last["citations"]) == 1
        assert last["citations"][0]["source_index"] == 1
        assert last["citations"][0]["is_valid"] is True
        assert len(last["sources"]) == 1
        assert last["sources"][0]["dataset_id"] == "ds1"

    @pytest.mark.asyncio
    async def test_query_stream_no_results_no_citation_event(self):
        """When no results found, should yield message without citations event."""
        svc = RAGService.__new__(RAGService)
        svc.search_service = MagicMock()
        svc._allie = MagicMock()
        svc.prompt_registry = MagicMock()
        svc.citation_parser = MagicMock()

        svc.search_service.search = MagicMock(return_value={"results": []})

        with patch("app.services.rag_service.run_sync", side_effect=_fake_run_sync):
            chunks = []
            async for c in svc.query_stream(question="test"):
                chunks.append(c)

        assert len(chunks) == 1
        assert "couldn't find" in chunks[0].lower()


# ============================================================================
# AC6 — Type preservation in row_data
# ============================================================================

class TestTypePreservation:
    """AC6: _serialize_value preserves datetime→ISO, UUID→str with type tag."""

    def test_datetime_serialized_with_type_tag(self):
        dt = datetime(2026, 1, 15, 10, 30, 0)
        result = IndexingService._serialize_value(dt)
        assert result == {"__type__": "datetime", "value": "2026-01-15T10:30:00"}

    def test_date_serialized_with_type_tag(self):
        d = date(2026, 1, 15)
        result = IndexingService._serialize_value(d)
        assert result == {"__type__": "date", "value": "2026-01-15"}

    def test_uuid_serialized_with_type_tag(self):
        uid = UUID("12345678-1234-5678-1234-567812345678")
        result = IndexingService._serialize_value(uid)
        assert result == {"__type__": "uuid", "value": "12345678-1234-5678-1234-567812345678"}

    def test_int_preserved(self):
        assert IndexingService._serialize_value(42) == 42

    def test_float_preserved(self):
        assert IndexingService._serialize_value(3.14) == 3.14

    def test_bool_preserved(self):
        assert IndexingService._serialize_value(True) is True

    def test_none_preserved(self):
        assert IndexingService._serialize_value(None) is None

    def test_string_passthrough(self):
        assert IndexingService._serialize_value("hello") == "hello"

    def test_other_types_become_str(self):
        """Non-primitive, non-datetime, non-UUID types become plain str."""
        assert IndexingService._serialize_value([1, 2, 3]) == "[1, 2, 3]"
