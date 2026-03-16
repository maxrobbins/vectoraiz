"""
Tests for BQ-VZ-HYBRID-SEARCH Phase 1A — Hybrid Search Pipeline.

Tests:
1. Hybrid search returns results when sparse+dense available
2. Dense-only fallback works when sparse unavailable
3. Reranker timeout triggers circuit breaker
4. FTS returns BM25 results for structured data
5. Facet counts correct
6. Search service pipeline stages logging
"""

import json
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


# ---------------------------------------------------------------------------
# 1. Reranker: circuit breaker on timeout
# ---------------------------------------------------------------------------

class TestRerankerCircuitBreaker:
    """Test that the reranker circuit breaker activates on consecutive timeouts."""

    def test_reranker_returns_results_on_success(self):
        """Reranker reranks documents when model succeeds."""
        from app.services.reranker_service import RerankerService

        service = RerankerService()

        # Mock the model to return predictable scores
        mock_model = MagicMock()
        mock_model.predict.return_value = [0.9, 0.1, 0.5]
        service._model = mock_model

        docs = [
            {"text_content": "first doc", "score": 0.8},
            {"text_content": "second doc", "score": 0.9},
            {"text_content": "third doc", "score": 0.7},
        ]

        with patch("app.services.reranker_service.settings") as mock_settings:
            mock_settings.reranker_top_k = 3
            mock_settings.reranker_timeout_ms = 5000  # generous timeout
            result = service.rerank("test query", docs, top_k=3)

        assert len(result) == 3
        # Should be sorted by rerank_score descending
        assert result[0]["rerank_score"] == 0.9
        assert result[1]["rerank_score"] == 0.5
        assert result[2]["rerank_score"] == 0.1

    def test_circuit_breaker_opens_after_consecutive_failures(self):
        """Circuit breaker opens after 3 consecutive timeouts."""
        from app.services.reranker_service import RerankerService

        service = RerankerService()
        service._circuit_threshold = 3

        # Mock model that always raises
        mock_model = MagicMock()
        mock_model.predict.side_effect = RuntimeError("Model failed")
        service._model = mock_model

        docs = [{"text_content": "doc", "score": 0.5}]

        with patch("app.services.reranker_service.settings") as mock_settings:
            mock_settings.reranker_top_k = 10
            mock_settings.reranker_timeout_ms = 200

            # 3 failures should open circuit
            for _ in range(3):
                result = service.rerank("query", docs, top_k=1)
                assert len(result) == 1  # Returns un-reranked

        assert service._circuit_open is True

        # 4th call should skip model entirely
        result = service.rerank("query", docs, top_k=1)
        assert len(result) == 1
        # Model should only have been called 3 times (not 4)
        assert mock_model.predict.call_count == 3

    def test_circuit_breaker_resets_on_manual_reset(self):
        """Circuit breaker can be reset manually."""
        from app.services.reranker_service import RerankerService

        service = RerankerService()
        service._circuit_open = True
        service._consecutive_timeouts = 5

        service.reset_circuit_breaker()

        assert service._circuit_open is False
        assert service._consecutive_timeouts == 0


# ---------------------------------------------------------------------------
# 2. FTS Service: BM25 results for structured data
# ---------------------------------------------------------------------------

class TestFTSService:
    """Test DuckDB FTS index creation and search."""

    def test_fts_build_and_search(self, tmp_path):
        """FTS index builds from parquet and returns BM25 results."""
        import duckdb
        from app.services.fts_service import (
            build_fts_index,
            search_fts,
            get_fts_status,
            _fts_status,
            _fts_lock,
        )

        # Create a test parquet file
        parquet_path = tmp_path / "test_data.parquet"
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE test AS SELECT * FROM (
                VALUES
                    ('Apple iPhone', 'smartphone with great camera'),
                    ('Samsung Galaxy', 'android phone with AMOLED display'),
                    ('Google Pixel', 'best camera phone for photography')
            ) AS t(name, description)
        """)
        con.execute(f"COPY test TO '{parquet_path}' (FORMAT PARQUET)")
        con.close()

        dataset_id = "fts_test_001"

        # Override processed directory to use tmp_path
        with patch("app.services.fts_service.settings") as mock_settings:
            mock_settings.processed_directory = str(tmp_path)

            # Build index (runs in background thread)
            build_fts_index(dataset_id, parquet_path)

            # Wait for background thread to complete
            import time
            for _ in range(50):  # max 5 seconds
                if get_fts_status(dataset_id) in ("ready", "unavailable"):
                    break
                time.sleep(0.1)

            status = get_fts_status(dataset_id)
            assert status == "ready", f"FTS build failed, status: {status}"

            # Search
            results = search_fts("camera phone", dataset_id, limit=5)
            assert len(results) > 0
            # Results should have score column
            assert "score" in results[0]

    def test_fts_returns_empty_when_unavailable(self):
        """FTS search returns empty list when index is not built."""
        from app.services.fts_service import search_fts

        results = search_fts("query", "nonexistent_dataset", limit=5)
        assert results == []


# ---------------------------------------------------------------------------
# 3. Facet Service: correct counts
# ---------------------------------------------------------------------------

class TestFacetService:
    """Test facet computation."""

    def test_facet_buckets(self):
        """Test row count and quality score bucketing."""
        from app.services.facet_service import _bucket_row_count, _bucket_quality_score

        assert _bucket_row_count(None) == "unknown"
        assert _bucket_row_count(50) == "<100"
        assert _bucket_row_count(500) == "100-1K"
        assert _bucket_row_count(5000) == "1K-10K"
        assert _bucket_row_count(50000) == "10K-100K"
        assert _bucket_row_count(500000) == "100K+"

        assert _bucket_quality_score(None) == "unknown"
        assert _bucket_quality_score(0.95) == "excellent"
        assert _bucket_quality_score(0.8) == "good"
        assert _bucket_quality_score(0.6) == "fair"
        assert _bucket_quality_score(0.3) == "poor"

    def test_rebuild_facets_produces_valid_structure(self, tmp_path):
        """Facet rebuild produces expected structure with mocked datasets."""
        from app.services import facet_service

        # Mock processing service to return fake datasets
        mock_record = MagicMock()
        mock_record.id = "test_001"
        mock_record.file_type = "csv"
        mock_record.metadata_json = json.dumps({"column_count": 5, "row_count": 1500})

        mock_processing = MagicMock()
        mock_processing.list_datasets.return_value = [mock_record]

        with patch.dict("sys.modules", {}), \
             patch("app.services.facet_service.settings") as mock_settings:
            mock_settings.data_directory = str(tmp_path)
            mock_settings.processed_directory = str(tmp_path / "processed")

            # Patch get_processing_service at the source module level
            with patch("app.services.processing_service.get_processing_service", return_value=mock_processing):
                facets = facet_service.rebuild_facets()

        assert "file_type" in facets
        assert "csv" in facets["file_type"]
        assert facets["file_type"]["csv"] == 1
        assert "row_count_bucket" in facets
        assert "1K-10K" in facets["row_count_bucket"]
        assert "has_pii" in facets
        assert "quality_score_bucket" in facets


# ---------------------------------------------------------------------------
# 4. Qdrant hybrid search methods
# ---------------------------------------------------------------------------

class TestQdrantHybridMethods:
    """Test qdrant_service hybrid method logic (mocked client)."""

    def test_collection_has_sparse_returns_false_for_legacy(self):
        """Legacy collections (single unnamed vector) return False for has_sparse."""
        from app.services.qdrant_service import QdrantService

        service = QdrantService()
        mock_client = MagicMock()

        # Simulate legacy collection (VectorParams, not dict)
        mock_info = MagicMock()
        mock_info.config.params.vectors = MagicMock()  # Not a dict
        mock_info.config.params.vectors.size = 384
        mock_client.get_collection.return_value = mock_info

        service._client = mock_client

        assert service.collection_has_sparse("test_collection") is False

    def test_hybrid_search_falls_back_to_dense(self):
        """Hybrid search falls back to dense-only when no sparse support."""
        from app.services.qdrant_service import QdrantService

        service = QdrantService()
        mock_client = MagicMock()
        service._client = mock_client

        # Mock collection_has_sparse to return False
        with patch.object(service, 'collection_has_sparse', return_value=False):
            # Mock the search method
            mock_hit = MagicMock()
            mock_hit.id = "point_1"
            mock_hit.score = 0.85
            mock_hit.payload = {"text_content": "test"}
            mock_client.search.return_value = [mock_hit]

            results = service.hybrid_search(
                collection_name="test_col",
                dense_vector=[0.1] * 384,
                sparse_vector=([1, 2, 3], [0.5, 0.3, 0.1]),
                limit=5,
            )

        assert len(results) == 1
        assert results[0]["score"] == 0.85
        # Should have called search, not query_points
        mock_client.search.assert_called_once()


# ---------------------------------------------------------------------------
# 5. Search service pipeline stages
# ---------------------------------------------------------------------------

class TestSearchServiceStages:
    """Test that search service reports active stages correctly."""

    def test_dense_only_stages(self):
        """Dense-only search reports correct stages."""
        from app.services.search_service import SearchService

        service = SearchService()

        # Mock everything
        mock_embedding = MagicMock()
        mock_embedding.embed_text.return_value = [0.1] * 384
        service.embedding_service = mock_embedding

        mock_qdrant = MagicMock()
        mock_qdrant.hybrid_search.return_value = [
            {"id": "1", "score": 0.9, "payload": {"text_content": "test", "row_index": 0, "row_data": {}}},
        ]
        mock_qdrant.collection_has_sparse.return_value = False
        service.qdrant_service = mock_qdrant
        service._sparse_encoder = False  # Disabled

        mock_processing = MagicMock()
        mock_processing.get_dataset.return_value = None
        service.processing_service = mock_processing

        with patch.object(service, '_get_searchable_collections', return_value=["dataset_test"]), \
             patch("app.services.search_service.settings") as mock_settings:
            mock_settings.hybrid_search_mode = "dense_only"
            mock_settings.reranker_enabled = False
            mock_settings.fts_enabled = False
            mock_settings.reranker_top_k = 30

            result = service.search("test query", limit=5)

        assert "dense_embedding" in result["stages_active"]
        assert result["total"] == 1

    def test_empty_query_returns_empty(self):
        """Empty query returns empty results."""
        from app.services.search_service import SearchService

        service = SearchService()
        result = service.search("   ", limit=5)

        assert result["total"] == 0
        assert result["results"] == []


# ---------------------------------------------------------------------------
# 6. Sparse encoder service
# ---------------------------------------------------------------------------

class TestSparseEncoder:
    """Test sparse encoder service interface."""

    def test_sparse_encoder_info(self):
        """Sparse encoder reports correct info when not loaded."""
        from app.services.sparse_encoder import SparseEncoder

        encoder = SparseEncoder()
        info = encoder.get_info()

        assert info["loaded"] is False
        assert "bm42" in info["model_name"].lower()


# ---------------------------------------------------------------------------
# 7. Config settings
# ---------------------------------------------------------------------------

class TestHybridSearchConfig:
    """Test hybrid search configuration defaults."""

    def test_default_config(self):
        """Default hybrid search config values are correct."""
        from app.config import settings

        assert settings.hybrid_search_mode in ("hybrid", "dense_only")
        assert settings.hybrid_rrf_k == 60
        assert settings.reranker_enabled is True
        assert settings.reranker_top_k == 30
        assert settings.reranker_timeout_ms == 200
        assert settings.fts_enabled is True
