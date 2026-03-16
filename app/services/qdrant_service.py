"""
Qdrant vector database service for semantic search.
Manages collections and vector operations.

BQ-VZ-HYBRID-SEARCH Phase 1A: Added hybrid collection support with
named vectors (dense + sparse) and RRF fusion search.
"""

import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import uuid

from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.http.exceptions import UnexpectedResponse

from app.config import settings

logger = logging.getLogger(__name__)

# Vector configuration for all-MiniLM-L6-v2 model
VECTOR_SIZE = 384
DISTANCE_METRIC = models.Distance.COSINE

# HNSW index configuration for optimal search performance
HNSW_CONFIG = models.HnswConfigDiff(
    m=16,                    # Number of edges per node (higher = better recall, more memory)
    ef_construct=100,        # Size of dynamic candidate list during index construction
    full_scan_threshold=10000,  # Use brute force below this threshold
)

# Optimized for memory efficiency with larger collections
OPTIMIZERS_CONFIG = models.OptimizersConfigDiff(
    memmap_threshold=50000,  # Switch to memory-mapped storage above 50k vectors
    indexing_threshold=20000,  # Start indexing after 20k vectors
)


class QdrantService:
    """Manages Qdrant vector database operations."""
    
    def __init__(self):
        self._client: Optional[QdrantClient] = None
    
    @property
    def client(self) -> QdrantClient:
        """Get or create Qdrant client connection."""
        if self._client is None:
            self._client = QdrantClient(
                host=settings.qdrant_host,
                port=settings.qdrant_port,
                timeout=30,
            )
        return self._client
    
    def health_check(self) -> Dict[str, Any]:
        """Check Qdrant connection health."""
        try:
            collections = self.client.get_collections()
            return {
                "status": "healthy",
                "collections_count": len(collections.collections),
                "timestamp": datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }
    
    def collection_exists(self, collection_name: str) -> bool:
        """Check if a collection exists."""
        try:
            self.client.get_collection(collection_name)
            return True
        except UnexpectedResponse:
            return False
    
    def create_collection(
        self, 
        collection_name: str,
        recreate_if_exists: bool = False
    ) -> Dict[str, Any]:
        """
        Create a new vector collection optimized for semantic search.
        
        Args:
            collection_name: Name of the collection (typically dataset_id)
            recreate_if_exists: If True, delete and recreate existing collection
        
        Returns:
            Collection info dict
        """
        if self.collection_exists(collection_name):
            if recreate_if_exists:
                self.delete_collection(collection_name)
            else:
                return self.get_collection_info(collection_name)
        
        self.client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=VECTOR_SIZE,
                distance=DISTANCE_METRIC,
            ),
            hnsw_config=HNSW_CONFIG,
            optimizers_config=OPTIMIZERS_CONFIG,
            on_disk_payload=True,  # Store payloads on disk for memory efficiency
        )
        
        # Create payload indexes for common filter fields
        self._create_payload_indexes(collection_name)
        
        return self.get_collection_info(collection_name)
    
    def _create_payload_indexes(self, collection_name: str):
        """Create indexes on common payload fields for efficient filtering."""
        # Index for row_index (integer filtering)
        try:
            self.client.create_payload_index(
                collection_name=collection_name,
                field_name="row_index",
                field_schema=models.PayloadSchemaType.INTEGER,
            )
        except Exception:
            pass  # Index may already exist
        
        # Index for text content (full-text search support)
        try:
            self.client.create_payload_index(
                collection_name=collection_name,
                field_name="text_content",
                field_schema=models.TextIndexParams(
                    type="text",
                    tokenizer=models.TokenizerType.WORD,
                    min_token_len=2,
                    max_token_len=20,
                    lowercase=True,
                ),
            )
        except Exception:
            pass  # Index may already exist
    
    def delete_collection(self, collection_name: str) -> bool:
        """Delete a collection."""
        try:
            self.client.delete_collection(collection_name)
            return True
        except UnexpectedResponse:
            return False
    
    def get_collection_info(self, collection_name: str) -> Dict[str, Any]:
        """Get detailed information about a collection."""
        try:
            info = self.client.get_collection(collection_name)
            return {
                "name": collection_name,
                "status": info.status.value if info.status else "unknown",
                "vectors_count": info.vectors_count or 0,
                "points_count": info.points_count or 0,
                "indexed_vectors_count": info.indexed_vectors_count or 0,
                "config": {
                    "vector_size": info.config.params.vectors.size if info.config.params.vectors else VECTOR_SIZE,
                    "distance": info.config.params.vectors.distance.value if info.config.params.vectors else "cosine",
                },
                "segments_count": info.segments_count,
                "optimizer_status": str(info.optimizer_status) if info.optimizer_status else "unknown",
            }
        except UnexpectedResponse as e:
            raise ValueError(f"Collection '{collection_name}' not found")
    
    def list_collections(self) -> List[Dict[str, Any]]:
        """List all collections with basic info."""
        collections = self.client.get_collections()
        result = []
        for col in collections.collections:
            try:
                info = self.get_collection_info(col.name)
                result.append(info)
            except Exception:
                result.append({"name": col.name, "status": "error"})
        return result
    
    def upsert_vectors(
        self,
        collection_name: str,
        vectors: List[List[float]],
        payloads: List[Dict[str, Any]],
        ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Insert or update vectors in a collection.
        
        Args:
            collection_name: Target collection
            vectors: List of embedding vectors (384-dim each)
            payloads: List of payload dicts (one per vector)
            ids: Optional list of point IDs (auto-generated if not provided)
        
        Returns:
            Operation result with count of upserted vectors
        """
        if len(vectors) != len(payloads):
            raise ValueError("Vectors and payloads must have same length")
        
        if not vectors:
            return {"upserted": 0}
        
        # Generate IDs if not provided
        if ids is None:
            ids = [str(uuid.uuid4()) for _ in vectors]
        
        # Create points
        points = [
            models.PointStruct(
                id=point_id,
                vector=vector,
                payload=payload,
            )
            for point_id, vector, payload in zip(ids, vectors, payloads)
        ]
        
        # Upsert in batches of 100
        batch_size = 100
        total_upserted = 0
        
        for i in range(0, len(points), batch_size):
            batch = points[i:i + batch_size]
            is_last_batch = (i + batch_size >= len(points))
            self.client.upsert(
                collection_name=collection_name,
                points=batch,
                wait=is_last_batch,
            )
            total_upserted += len(batch)
        
        return {
            "upserted": total_upserted,
            "collection": collection_name,
        }
    
    def search(
        self,
        collection_name: str,
        query_vector: List[float],
        limit: int = 10,
        score_threshold: Optional[float] = None,
        filter_conditions: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search for similar vectors.
        
        Args:
            collection_name: Collection to search
            query_vector: Query embedding (384-dim)
            limit: Max results to return
            score_threshold: Minimum similarity score (0-1 for cosine)
            filter_conditions: Optional Qdrant filter dict
        
        Returns:
            List of results with id, score, and payload
        """
        # Build filter if conditions provided
        query_filter = None
        if filter_conditions:
            query_filter = models.Filter(**filter_conditions)
        
        results = self.client.search(
            collection_name=collection_name,
            query_vector=query_vector,
            limit=limit,
            score_threshold=score_threshold,
            query_filter=query_filter,
            with_payload=True,
            with_vectors=False,  # Don't return vectors to save bandwidth
        )
        
        return [
            {
                "id": str(hit.id),
                "score": hit.score,
                "payload": hit.payload,
            }
            for hit in results
        ]
    
    def delete_vectors(
        self,
        collection_name: str,
        ids: Optional[List[str]] = None,
        filter_conditions: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Delete vectors by ID or filter.
        
        Args:
            collection_name: Target collection
            ids: List of point IDs to delete
            filter_conditions: Qdrant filter to match points for deletion
        
        Returns:
            Operation result
        """
        if ids:
            self.client.delete(
                collection_name=collection_name,
                points_selector=models.PointIdsList(points=ids),
            )
            return {"deleted_ids": len(ids)}
        
        if filter_conditions:
            self.client.delete(
                collection_name=collection_name,
                points_selector=models.FilterSelector(
                    filter=models.Filter(**filter_conditions)
                ),
            )
            return {"deleted_by_filter": True}
        
        raise ValueError("Must provide either ids or filter_conditions")
    
    def get_vector_count(self, collection_name: str) -> int:
        """Get the number of vectors in a collection."""
        info = self.client.get_collection(collection_name)
        return info.vectors_count or 0

    # ==================================================================
    # BQ-VZ-HYBRID-SEARCH Phase 1A: Hybrid collection + search methods
    # ==================================================================

    def create_hybrid_collection(
        self,
        collection_name: str,
        recreate_if_exists: bool = False,
    ) -> Dict[str, Any]:
        """
        Create a collection with named dense + sparse vectors for hybrid search.

        Uses named vectors: "dense" (384-dim cosine) and "sparse" (sparse).
        Falls back to existing collection if already present and recreate=False.
        """
        if self.collection_exists(collection_name):
            if recreate_if_exists:
                self.delete_collection(collection_name)
            else:
                return self.get_collection_info(collection_name)

        self.client.create_collection(
            collection_name=collection_name,
            vectors_config={
                "dense": models.VectorParams(
                    size=VECTOR_SIZE,
                    distance=DISTANCE_METRIC,
                ),
            },
            sparse_vectors_config={
                "sparse": models.SparseVectorParams(),
            },
            hnsw_config=HNSW_CONFIG,
            optimizers_config=OPTIMIZERS_CONFIG,
            on_disk_payload=True,
        )

        self._create_payload_indexes(collection_name)
        logger.info("Created hybrid collection: %s", collection_name)
        return self.get_collection_info(collection_name)

    def collection_has_sparse(self, collection_name: str) -> bool:
        """Check if a collection has sparse vector support."""
        try:
            info = self.client.get_collection(collection_name)
            # Named vectors config is a dict when using named vectors
            vectors_config = info.config.params.vectors
            if isinstance(vectors_config, dict) and "dense" in vectors_config:
                return True
            return False
        except Exception:
            return False

    def upsert_hybrid_vectors(
        self,
        collection_name: str,
        dense_vectors: List[List[float]],
        sparse_vectors: List[Tuple[List[int], List[float]]],
        payloads: List[Dict[str, Any]],
        ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Upsert points with both dense and sparse vectors.

        Args:
            collection_name: Target collection (must be hybrid).
            dense_vectors: Dense embeddings (384-dim each).
            sparse_vectors: List of (indices, values) tuples for sparse vectors.
            payloads: Payload dicts.
            ids: Optional point IDs.
        """
        if len(dense_vectors) != len(sparse_vectors) or len(dense_vectors) != len(payloads):
            raise ValueError("dense_vectors, sparse_vectors, and payloads must have same length")

        if not dense_vectors:
            return {"upserted": 0}

        if ids is None:
            ids = [str(uuid.uuid4()) for _ in dense_vectors]

        points = []
        for point_id, dense, (sp_indices, sp_values), payload in zip(
            ids, dense_vectors, sparse_vectors, payloads
        ):
            points.append(
                models.PointStruct(
                    id=point_id,
                    vector={
                        "dense": dense,
                        "sparse": models.SparseVector(
                            indices=sp_indices,
                            values=sp_values,
                        ),
                    },
                    payload=payload,
                )
            )

        batch_size = 100
        total_upserted = 0

        for i in range(0, len(points), batch_size):
            batch = points[i:i + batch_size]
            is_last_batch = (i + batch_size >= len(points))
            self.client.upsert(
                collection_name=collection_name,
                points=batch,
                wait=is_last_batch,
            )
            total_upserted += len(batch)

        return {"upserted": total_upserted, "collection": collection_name}

    def hybrid_search(
        self,
        collection_name: str,
        dense_vector: List[float],
        sparse_vector: Optional[Tuple[List[int], List[float]]] = None,
        limit: int = 10,
        filter_conditions: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Hybrid search using Qdrant Query API with Prefetch + RRF fusion.

        Falls back to dense-only search if sparse_vector is None or if the
        collection doesn't support sparse vectors.
        """
        query_filter = None
        if filter_conditions:
            query_filter = models.Filter(**filter_conditions)

        has_sparse = sparse_vector is not None and self.collection_has_sparse(collection_name)

        if has_sparse:
            sp_indices, sp_values = sparse_vector
            try:
                results = self.client.query_points(
                    collection_name=collection_name,
                    prefetch=[
                        models.Prefetch(
                            query=dense_vector,
                            using="dense",
                            limit=100,
                        ),
                        models.Prefetch(
                            query=models.SparseVector(
                                indices=sp_indices,
                                values=sp_values,
                            ),
                            using="sparse",
                            limit=100,
                        ),
                    ],
                    query=models.FusionQuery(fusion=models.Fusion.RRF),
                    limit=limit,
                    query_filter=query_filter,
                    with_payload=True,
                )

                return [
                    {
                        "id": str(point.id),
                        "score": point.score if point.score is not None else 0.0,
                        "payload": point.payload,
                    }
                    for point in results.points
                ]
            except Exception as e:
                logger.warning(
                    "Hybrid search failed for %s, falling back to dense-only: %s",
                    collection_name, e,
                )
                # Fall through to dense-only

        # Dense-only fallback
        if self.collection_has_sparse(collection_name):
            # Collection uses named vectors — search the "dense" named vector
            results = self.client.search(
                collection_name=collection_name,
                query_vector=models.NamedVector(name="dense", vector=dense_vector),
                limit=limit,
                query_filter=query_filter,
                with_payload=True,
                with_vectors=False,
            )
        else:
            # Legacy collection with unnamed single vector
            results = self.client.search(
                collection_name=collection_name,
                query_vector=dense_vector,
                limit=limit,
                query_filter=query_filter,
                with_payload=True,
                with_vectors=False,
            )

        return [
            {
                "id": str(hit.id),
                "score": hit.score,
                "payload": hit.payload,
            }
            for hit in results
        ]

    def close(self):
        """Close the Qdrant client connection."""
        if self._client:
            self._client.close()
            self._client = None


# Singleton instance
_qdrant_service: Optional[QdrantService] = None


def get_qdrant_service() -> QdrantService:
    """Get the singleton Qdrant service instance."""
    global _qdrant_service
    if _qdrant_service is None:
        _qdrant_service = QdrantService()
    return _qdrant_service
