"""
Sparse Encoder Service (BQ-VZ-HYBRID-SEARCH Phase 1A)
=====================================================
BM42 sparse encoding via fastembed for hybrid search.
Lazy-loaded, singleton pattern matching existing services.
"""

import logging
import sys
import time
from typing import Optional, List, Dict, Any, Tuple

logger = logging.getLogger(__name__)

# BM42 model for sparse encoding
SPARSE_MODEL_NAME = "Qdrant/bm42-all-minilm-l6-v2-attentions"


class SparseEncoder:
    """Generates sparse (BM42) vectors via fastembed for hybrid search."""

    def __init__(self):
        self._model = None
        self._load_time: Optional[float] = None

    @property
    def model(self):
        """Lazy-load the sparse embedding model."""
        if self._model is None:
            self._load_model()
        return self._model

    def _load_model(self):
        """Load the BM42 sparse model via fastembed."""
        from fastembed import SparseTextEmbedding

        start = time.time()
        logger.info("Loading sparse model: %s ...", SPARSE_MODEL_NAME)
        print(f"Loading sparse model: {SPARSE_MODEL_NAME}...", file=sys.stderr)

        self._model = SparseTextEmbedding(model_name=SPARSE_MODEL_NAME)

        self._load_time = time.time() - start
        logger.info("Sparse model loaded in %.2fs", self._load_time)
        print(f"Sparse model loaded in {self._load_time:.2f}s", file=sys.stderr)

    def encode(self, text: str) -> Tuple[List[int], List[float]]:
        """Encode a single text into sparse vector (indices, values)."""
        results = list(self.model.embed([text]))
        if not results:
            return [], []
        sparse = results[0]
        return sparse.indices.tolist(), sparse.values.tolist()

    def encode_batch(self, texts: List[str]) -> List[Tuple[List[int], List[float]]]:
        """Encode multiple texts into sparse vectors."""
        if not texts:
            return []
        results = list(self.model.embed(texts))
        return [
            (r.indices.tolist(), r.values.tolist())
            for r in results
        ]

    def is_loaded(self) -> bool:
        return self._model is not None

    def get_info(self) -> Dict[str, Any]:
        return {
            "model_name": SPARSE_MODEL_NAME,
            "loaded": self.is_loaded(),
            "load_time_seconds": self._load_time,
        }


# Singleton
_sparse_encoder: Optional[SparseEncoder] = None


def get_sparse_encoder() -> SparseEncoder:
    """Get the singleton sparse encoder instance."""
    global _sparse_encoder
    if _sparse_encoder is None:
        _sparse_encoder = SparseEncoder()
    return _sparse_encoder
