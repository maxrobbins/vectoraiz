"""
Indexing service that combines embedding and vector storage.
Handles automatic indexing of datasets for semantic search.

BQ-VZ-HYBRID-SEARCH Phase 1A: Added sparse vector generation alongside
dense vectors, hybrid collection creation, and FTS index building.
"""

from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date
from uuid import UUID
import uuid

import logging

from app.config import settings
from app.services.embedding_service import get_embedding_service, EmbeddingService
from app.services.qdrant_service import get_qdrant_service, QdrantService
from app.services.duckdb_service import ephemeral_duckdb_service

logger = logging.getLogger(__name__)


# Indexing configuration
DEFAULT_ROW_LIMIT = 10000  # Max rows to index per dataset
DEFAULT_BATCH_SIZE = 32


class IndexingService:
    """
    Orchestrates dataset indexing: extracts text, generates embeddings, stores in Qdrant.
    BQ-VZ-HYBRID-SEARCH: Now generates sparse vectors alongside dense for hybrid search.
    """

    def __init__(self):
        self.embedding_service: EmbeddingService = get_embedding_service()
        self.qdrant_service: QdrantService = get_qdrant_service()
        self._sparse_encoder = None  # Lazy-loaded

    @property
    def sparse_encoder(self):
        """Lazy-load sparse encoder only when hybrid mode is enabled."""
        if self._sparse_encoder is None and settings.hybrid_search_mode == "hybrid":
            try:
                from app.services.sparse_encoder import get_sparse_encoder
                self._sparse_encoder = get_sparse_encoder()
            except Exception as e:
                logger.warning("Failed to load sparse encoder, falling back to dense-only: %s", e)
                self._sparse_encoder = False  # Sentinel: tried and failed
        return self._sparse_encoder if self._sparse_encoder is not False else None
    
    def index_dataset(
        self,
        dataset_id: str,
        filepath: Path,
        row_limit: int = DEFAULT_ROW_LIMIT,
        text_columns: Optional[List[str]] = None,
        recreate_collection: bool = False,
    ) -> Dict[str, Any]:
        """
        Index a dataset for semantic search.
        
        Args:
            dataset_id: Unique identifier for the dataset (used as collection name)
            filepath: Path to the Parquet file
            row_limit: Maximum rows to index (default: 10000)
            text_columns: Specific columns to use for text (auto-detect if None)
            recreate_collection: Delete existing collection first
            
        Returns:
            Indexing result with statistics
        """
        start_time = datetime.utcnow()

        collection_name = f"dataset_{dataset_id}"
        use_hybrid = settings.hybrid_search_mode == "hybrid" and self.sparse_encoder is not None

        # Create hybrid or standard collection
        if use_hybrid:
            self.qdrant_service.create_hybrid_collection(
                collection_name,
                recreate_if_exists=recreate_collection,
            )
        else:
            self.qdrant_service.create_collection(
                collection_name,
                recreate_if_exists=recreate_collection,
            )

        # Get dataset metadata to identify text columns
        with ephemeral_duckdb_service() as duckdb:
            metadata = duckdb.get_file_metadata(filepath)

        # Auto-detect text columns if not specified
        if text_columns is None:
            text_columns = self._detect_text_columns(filepath)

        if not text_columns:
            return {
                "dataset_id": dataset_id,
                "status": "skipped",
                "reason": "No text columns found for indexing",
                "collection": collection_name,
            }

        # Extract rows for indexing
        rows = self._extract_rows(filepath, row_limit)

        if not rows:
            return {
                "dataset_id": dataset_id,
                "status": "skipped",
                "reason": "No rows to index",
                "collection": collection_name,
            }

        # Generate text representations and embeddings
        texts = []
        payloads = []

        filename = filepath.name

        for i, row in enumerate(rows):
            # Combine text columns into single string
            text_parts = []
            for col in text_columns:
                if col in row and row[col] is not None:
                    text_parts.append(f"{col}: {row[col]}")

            if text_parts:
                text = " | ".join(text_parts)
                texts.append(text)
                payloads.append({
                    "dataset_id": dataset_id,
                    "filename": filename,
                    "row_index": i,
                    "row_id": f"{dataset_id}:{i}",  # stable row identifier
                    "text_content": text,
                    "row_data": row,  # Store full row for retrieval
                })

        if not texts:
            return {
                "dataset_id": dataset_id,
                "status": "skipped",
                "reason": "No text content to index",
                "collection": collection_name,
            }

        # Generate dense embeddings
        embeddings = self.embedding_service.embed_texts(
            texts,
            batch_size=DEFAULT_BATCH_SIZE,
            show_progress=len(texts) > 100
        )

        # Generate sparse vectors and upsert hybrid, or dense-only
        if use_hybrid:
            sparse_vectors = self.sparse_encoder.encode_batch(texts)
            result = self.qdrant_service.upsert_hybrid_vectors(
                collection_name=collection_name,
                dense_vectors=embeddings,
                sparse_vectors=sparse_vectors,
                payloads=payloads,
            )
            logger.info("Hybrid indexed %d vectors for dataset %s", result["upserted"], dataset_id)
        else:
            result = self.qdrant_service.upsert_vectors(
                collection_name=collection_name,
                vectors=embeddings,
                payloads=payloads,
            )

        # Trigger FTS index build in background
        self._trigger_fts_build(dataset_id, filepath)

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        return {
            "dataset_id": dataset_id,
            "status": "completed",
            "collection": collection_name,
            "rows_indexed": result["upserted"],
            "text_columns_used": text_columns,
            "hybrid_mode": use_hybrid,
            "duration_seconds": round(duration, 2),
            "rows_per_second": round(result["upserted"] / duration, 1) if duration > 0 else 0,
        }
    
    def _trigger_fts_build(self, dataset_id: str, filepath: Path) -> None:
        """Trigger async FTS index build if FTS is enabled."""
        if not settings.fts_enabled:
            return
        try:
            from app.services.fts_service import build_fts_index
            build_fts_index(dataset_id, filepath)
            logger.info("FTS index build triggered for dataset %s", dataset_id)
        except Exception as e:
            logger.warning("FTS index build trigger failed for %s: %s", dataset_id, e)

    def _detect_text_columns(self, filepath: Path) -> List[str]:
        """
        Auto-detect columns suitable for text search.
        Prefers text/varchar columns with reasonable content.
        """
        with ephemeral_duckdb_service() as duckdb:
            profiles = duckdb.get_column_profile(filepath)
        
        text_columns = []
        for profile in profiles:
            # Include text-like columns
            if profile["semantic_type"] in ["text", "email", "url"]:
                # Skip columns that are mostly unique (likely IDs)
                if profile["uniqueness_ratio"] < 0.95:
                    text_columns.append(profile["name"])
            
            # Include columns with "name", "description", "title" in name
            col_lower = profile["name"].lower()
            if any(x in col_lower for x in ["name", "description", "title", "content", "text", "comment", "note"]):
                if profile["name"] not in text_columns:
                    text_columns.append(profile["name"])
        
        return text_columns
    
    def _extract_rows(self, filepath: Path, limit: int) -> List[Dict[str, Any]]:
        """Extract rows from dataset for indexing."""
        with ephemeral_duckdb_service() as duckdb:
            file_type = duckdb.detect_file_type(filepath)
            read_func = duckdb.get_read_function(file_type, str(filepath))

            query = f"SELECT * FROM {read_func} LIMIT {limit}"
            result = duckdb.connection.execute(query).fetchall()

            # Get column names
            schema = duckdb.connection.execute(f"DESCRIBE SELECT * FROM {read_func}").fetchall()
            column_names = [row[0] for row in schema]
        
        # Convert to list of dicts
        rows = []
        for row in result:
            row_dict = {}
            for col_name, value in zip(column_names, row):
                row_dict[col_name] = self._serialize_value(value)
            rows.append(row_dict)

        return rows

    @staticmethod
    def _serialize_value(value: Any) -> Any:
        """Convert a single value to a JSON-serializable form with type preservation."""
        if value is None:
            return None
        if isinstance(value, (int, float, bool)):
            return value
        if isinstance(value, datetime):
            return {"__type__": "datetime", "value": value.isoformat()}
        if isinstance(value, date):
            return {"__type__": "date", "value": value.isoformat()}
        if isinstance(value, UUID):
            return {"__type__": "uuid", "value": str(value)}
        return str(value)
    
    # ------------------------------------------------------------------
    # BQ-VZ-LARGE-FILES R5: Chunked streaming indexing
    # ------------------------------------------------------------------

    def index_streaming(
        self,
        dataset_id: str,
        chunk_iterator,
        text_columns: Optional[List[str]] = None,
        recreate_collection: bool = False,
        progress_callback: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Index a dataset from a streaming chunk iterator.

        BQ-VZ-LARGE-FILES R5: Processes one chunk at a time — never loads
        the full dataset into memory.

        Per chunk: extract text → chunk_text() → embed_batch() → upsert_batch()
        Batch size: 500 points per Qdrant upsert.
        Stable point IDs: {dataset_id}:{chunk_index}:{row_index}

        Args:
            dataset_id: Dataset identifier.
            chunk_iterator: Iterator yielding pyarrow.RecordBatch or dict items.
            text_columns: Columns to embed. Auto-detected from first batch if None.
            recreate_collection: Delete existing collection first.
        """
        import pyarrow as pa

        logger.info("index_streaming: dataset_id=%s — streaming mode", dataset_id)
        start_time = datetime.utcnow()
        collection_name = f"dataset_{dataset_id}"
        use_hybrid = settings.hybrid_search_mode == "hybrid" and self.sparse_encoder is not None

        if use_hybrid:
            self.qdrant_service.create_hybrid_collection(
                collection_name,
                recreate_if_exists=recreate_collection,
            )
        else:
            self.qdrant_service.create_collection(
                collection_name,
                recreate_if_exists=recreate_collection,
            )

        QDRANT_BATCH_SIZE = 500
        total_indexed = 0
        chunk_index = 0

        texts_buf: List[str] = []
        payloads_buf: List[Dict[str, Any]] = []

        for chunk in chunk_iterator:
            # Convert RecordBatch to list of row dicts
            if isinstance(chunk, pa.RecordBatch):
                row_dicts = chunk.to_pylist()
            elif isinstance(chunk, dict):
                row_dicts = [chunk]
            else:
                row_dicts = [chunk] if not isinstance(chunk, list) else chunk

            # Auto-detect text columns from first batch
            if text_columns is None and row_dicts:
                text_columns = self._detect_text_columns_from_rows(row_dicts[0])

            if not text_columns:
                chunk_index += 1
                continue

            for row_idx, row in enumerate(row_dicts):
                text_parts = []
                for col in text_columns:
                    val = row.get(col)
                    if val is not None:
                        text_parts.append(f"{col}: {val}")

                if not text_parts:
                    continue

                text = " | ".join(text_parts)
                point_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{dataset_id}:{chunk_index}:{row_idx}"))

                texts_buf.append(text)
                payloads_buf.append({
                    "dataset_id": dataset_id,
                    "row_id": point_id,
                    "chunk_index": chunk_index,
                    "row_index": row_idx,
                    "text_content": text,
                    "row_data": {k: self._serialize_value(v) for k, v in row.items()},
                })

                if len(texts_buf) >= QDRANT_BATCH_SIZE:
                    total_indexed += self._flush_index_batch(
                        collection_name, texts_buf, payloads_buf,
                    )
                    texts_buf = []
                    payloads_buf = []
                    # Release memory pages to OS — prevents RSS ratchet on large datasets
                    from app.services.process_worker import _release_memory
                    _release_memory()
                    if progress_callback:
                        progress_callback(total_indexed)

            chunk_index += 1

        # Flush remaining
        if texts_buf:
            total_indexed += self._flush_index_batch(
                collection_name, texts_buf, payloads_buf,
            )
            # Release memory after final flush
            from app.services.process_worker import _release_memory
            _release_memory()
            if progress_callback:
                progress_callback(total_indexed)

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        logger.info(
            "index_streaming: dataset_id=%s done — total_indexed=%d, chunks_processed=%d, duration=%.2fs",
            dataset_id, total_indexed, chunk_index, duration,
        )

        return {
            "dataset_id": dataset_id,
            "status": "completed",
            "collection": collection_name,
            "rows_indexed": total_indexed,
            "chunks_processed": chunk_index,
            "text_columns_used": text_columns or [],
            "duration_seconds": round(duration, 2),
            "rows_per_second": round(total_indexed / duration, 1) if duration > 0 else 0,
        }

    def _flush_index_batch(
        self,
        collection_name: str,
        texts: List[str],
        payloads: List[Dict[str, Any]],
    ) -> int:
        """Embed texts and upsert to Qdrant. Returns count upserted.

        Uses the ``row_id`` from each payload as the Qdrant point ID so
        that streaming-indexed points have stable, deterministic IDs
        (format: ``{dataset_id}:{chunk_index}:{row_index}``).

        BQ-VZ-HYBRID-SEARCH: Generates sparse vectors alongside dense when
        hybrid mode is enabled.
        """
        if not texts:
            return 0
        embeddings = self.embedding_service.embed_texts(
            texts, batch_size=DEFAULT_BATCH_SIZE, show_progress=False,
        )
        ids = [p["row_id"] for p in payloads]

        use_hybrid = settings.hybrid_search_mode == "hybrid" and self.sparse_encoder is not None

        if use_hybrid and self.qdrant_service.collection_has_sparse(collection_name):
            sparse_vectors = self.sparse_encoder.encode_batch(texts)
            result = self.qdrant_service.upsert_hybrid_vectors(
                collection_name=collection_name,
                dense_vectors=embeddings,
                sparse_vectors=sparse_vectors,
                payloads=payloads,
                ids=ids,
            )
        else:
            result = self.qdrant_service.upsert_vectors(
                collection_name=collection_name,
                vectors=embeddings,
                payloads=payloads,
                ids=ids,
            )
        return result.get("upserted", len(texts))

    def _detect_text_columns_from_rows(self, sample_row: Dict[str, Any]) -> List[str]:
        """Detect text columns from a sample row dict (no DuckDB needed).

        Used by index_streaming where we don't have a Parquet file to profile.
        """
        text_cols = []
        text_keywords = {"name", "description", "title", "content", "text", "comment", "note"}
        for col, val in sample_row.items():
            col_lower = col.lower()
            if isinstance(val, str) and len(val) > 10:
                text_cols.append(col)
            elif any(kw in col_lower for kw in text_keywords):
                if col not in text_cols:
                    text_cols.append(col)
        return text_cols

    def delete_dataset_index(self, dataset_id: str) -> bool:
        """Delete the vector index for a dataset."""
        collection_name = f"dataset_{dataset_id}"
        return self.qdrant_service.delete_collection(collection_name)
    
    def get_index_status(self, dataset_id: str) -> Dict[str, Any]:
        """Get indexing status for a dataset."""
        collection_name = f"dataset_{dataset_id}"
        
        if not self.qdrant_service.collection_exists(collection_name):
            return {
                "dataset_id": dataset_id,
                "indexed": False,
                "collection": None,
            }
        
        info = self.qdrant_service.get_collection_info(collection_name)
        return {
            "dataset_id": dataset_id,
            "indexed": True,
            "collection": collection_name,
            "vectors_count": info["vectors_count"],
            "status": info["status"],
        }


# Singleton instance
_indexing_service: Optional[IndexingService] = None


def get_indexing_service() -> IndexingService:
    """Get the singleton indexing service instance."""
    global _indexing_service
    if _indexing_service is None:
        _indexing_service = IndexingService()
    return _indexing_service
