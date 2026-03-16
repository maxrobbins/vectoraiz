"""
Search service that orchestrates semantic search across datasets.

BQ-VZ-HYBRID-SEARCH Phase 1A: Full hybrid search pipeline with:
  1. Qdrant hybrid search (dense + sparse with RRF fusion)
  2. DuckDB FTS for structured data (BM25)
  3. Cross-encoder reranking with circuit breaker
  4. Graceful degradation at every stage
"""

import logging
from typing import Optional, List, Dict, Any

from datetime import datetime

from app.config import settings
from app.services.embedding_service import get_embedding_service, EmbeddingService
from app.services.qdrant_service import get_qdrant_service, QdrantService
from app.services.processing_service import get_processing_service, ProcessingService

logger = logging.getLogger(__name__)


class SearchService:
    """
    Handles semantic search queries across indexed datasets.
    BQ-VZ-HYBRID-SEARCH: Orchestrates hybrid pipeline with graceful degradation.
    """

    def __init__(self):
        self.embedding_service: EmbeddingService = get_embedding_service()
        self.qdrant_service: QdrantService = get_qdrant_service()
        self.processing_service: ProcessingService = get_processing_service()
        self._sparse_encoder = None  # Lazy
        self._reranker = None  # Lazy

    @property
    def sparse_encoder(self):
        """Lazy-load sparse encoder for hybrid search."""
        if self._sparse_encoder is None and settings.hybrid_search_mode == "hybrid":
            try:
                from app.services.sparse_encoder import get_sparse_encoder
                self._sparse_encoder = get_sparse_encoder()
            except Exception as e:
                logger.warning("Sparse encoder unavailable: %s", e)
                self._sparse_encoder = False
        return self._sparse_encoder if self._sparse_encoder is not False else None

    @property
    def reranker(self):
        """Lazy-load reranker service."""
        if self._reranker is None and settings.reranker_enabled:
            try:
                from app.services.reranker_service import get_reranker_service
                self._reranker = get_reranker_service()
            except Exception as e:
                logger.warning("Reranker unavailable: %s", e)
                self._reranker = False
        return self._reranker if self._reranker is not False else None

    def search(
        self,
        query: str,
        dataset_id: Optional[str] = None,
        limit: int = 10,
        min_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Perform hybrid search with graceful degradation.

        Pipeline stages (each skipped if unavailable):
        1. Generate dense embedding (always)
        2. Generate sparse embedding (if hybrid mode + encoder loaded)
        3. Qdrant hybrid search with RRF fusion (falls back to dense-only)
        4. DuckDB FTS merge (if FTS index ready)
        5. Cross-encoder reranking (if enabled + within timeout)
        """
        start_time = datetime.utcnow()
        stages_active = []

        if not query.strip():
            return {
                "query": query,
                "results": [],
                "total": 0,
                "message": "Empty query",
            }

        # Stage 1: Dense embedding (always)
        query_vector = self.embedding_service.embed_text(query)
        stages_active.append("dense_embedding")

        # Stage 2: Sparse embedding (if available)
        sparse_vector = None
        if self.sparse_encoder is not None:
            try:
                sparse_vector = self.sparse_encoder.encode(query)
                stages_active.append("sparse_embedding")
            except Exception as e:
                logger.warning("Sparse encoding failed, skipping: %s", e)

        # Determine collections
        if dataset_id:
            collections = [f"dataset_{dataset_id}"]
        else:
            collections = self._get_searchable_collections()

        if not collections:
            return {
                "query": query,
                "results": [],
                "total": 0,
                "message": "No indexed datasets available",
            }

        # Stage 3: Qdrant search (hybrid or dense-only)
        all_results = []
        # For reranking, fetch more candidates
        fetch_limit = settings.reranker_top_k if self.reranker else limit

        for collection_name in collections:
            try:
                results = self.qdrant_service.hybrid_search(
                    collection_name=collection_name,
                    dense_vector=query_vector,
                    sparse_vector=sparse_vector,
                    limit=fetch_limit,
                )

                ds_id = collection_name.replace("dataset_", "")
                dataset_info = self._get_dataset_info(ds_id)

                for result in results:
                    all_results.append({
                        "dataset_id": ds_id,
                        "dataset_name": dataset_info.get("filename", ds_id),
                        "score": round(result["score"], 4),
                        "row_index": result["payload"].get("row_index"),
                        "text_content": result["payload"].get("text_content"),
                        "row_data": result["payload"].get("row_data", {}),
                    })

                if sparse_vector and self.qdrant_service.collection_has_sparse(collection_name):
                    if "hybrid_search" not in stages_active:
                        stages_active.append("hybrid_search")
                elif "dense_search" not in stages_active:
                    stages_active.append("dense_search")

            except Exception as e:
                logger.warning("Search failed for %s: %s", collection_name, e)
                continue

        # Stage 4: FTS merge (if enabled and index ready)
        if settings.fts_enabled and dataset_id:
            try:
                from app.services.fts_service import search_fts, get_fts_status
                if get_fts_status(dataset_id) == "ready":
                    fts_results = search_fts(query, dataset_id, limit=fetch_limit)
                    if fts_results:
                        stages_active.append("fts_bm25")
                        # Merge FTS results — deduplicate by row_index
                        existing_rows = {r.get("row_index") for r in all_results}
                        dataset_info = self._get_dataset_info(dataset_id)
                        for fts_row in fts_results:
                            row_idx = fts_row.get("rowid")
                            if row_idx not in existing_rows:
                                all_results.append({
                                    "dataset_id": dataset_id,
                                    "dataset_name": dataset_info.get("filename", dataset_id),
                                    "score": round(fts_row.get("score", 0.0), 4),
                                    "row_index": row_idx,
                                    "text_content": str(fts_row),
                                    "row_data": fts_row,
                                })
            except Exception as e:
                logger.warning("FTS merge failed: %s", e)

        # Sort by score before reranking
        all_results.sort(key=lambda x: x["score"], reverse=True)

        # Stage 5: Cross-encoder reranking (if enabled)
        if self.reranker and len(all_results) > 1:
            try:
                all_results = self.reranker.rerank(
                    query=query,
                    documents=all_results,
                    text_key="text_content",
                    top_k=limit,
                )
                stages_active.append("reranker")
            except Exception as e:
                logger.warning("Reranker failed: %s", e)

        # Final limit
        all_results = all_results[:limit]

        end_time = datetime.utcnow()
        duration_ms = (end_time - start_time).total_seconds() * 1000

        return {
            "query": query,
            "results": all_results,
            "total": len(all_results),
            "datasets_searched": len(collections),
            "duration_ms": round(duration_ms, 2),
            "stages_active": stages_active,
        }

    def search_dataset(
        self,
        dataset_id: str,
        query: str,
        limit: int = 10,
        min_score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Search within a specific dataset."""
        record = self.processing_service.get_dataset(dataset_id)
        if not record:
            raise ValueError(f"Dataset '{dataset_id}' not found")

        collection_name = f"dataset_{dataset_id}"
        if not self.qdrant_service.collection_exists(collection_name):
            raise ValueError(f"Dataset '{dataset_id}' is not indexed for search")

        return self.search(
            query=query,
            dataset_id=dataset_id,
            limit=limit,
            min_score=min_score,
        )

    def _get_searchable_collections(self) -> List[str]:
        """Get all dataset collections available for search."""
        collections = self.qdrant_service.list_collections()
        return [
            c["name"] for c in collections
            if c["name"].startswith("dataset_") and (c.get("vectors_count", 0) > 0 or c.get("points_count", 0) > 0)
        ]

    def _get_dataset_info(self, dataset_id: str) -> Dict[str, Any]:
        """Get basic dataset info for search results."""
        record = self.processing_service.get_dataset(dataset_id)
        if record:
            return {
                "filename": record.original_filename,
                "file_type": record.file_type,
            }
        return {}

    def get_search_stats(self) -> Dict[str, Any]:
        """Get statistics about searchable datasets."""
        collections = self._get_searchable_collections()

        total_vectors = 0
        datasets = []

        for collection_name in collections:
            try:
                info = self.qdrant_service.get_collection_info(collection_name)
                dataset_id = collection_name.replace("dataset_", "")
                dataset_info = self._get_dataset_info(dataset_id)

                datasets.append({
                    "dataset_id": dataset_id,
                    "filename": dataset_info.get("filename", dataset_id),
                    "vectors_count": info.get("vectors_count", 0),
                })
                total_vectors += info.get("vectors_count", 0)
            except Exception:
                continue

        return {
            "total_datasets": len(datasets),
            "total_vectors": total_vectors,
            "datasets": datasets,
        }


# Singleton instance
_search_service: Optional[SearchService] = None


def get_search_service() -> SearchService:
    """Get the singleton search service instance."""
    global _search_service
    if _search_service is None:
        _search_service = SearchService()
    return _search_service
