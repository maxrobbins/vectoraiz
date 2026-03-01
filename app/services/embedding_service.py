"""
Embedding service using sentence-transformers.
Loads model once at startup for efficient inference.

Prevention hardening (BQ-???): constrain native threadpools BEFORE importing
sentence-transformers/torch/tokenizers. In some environments (notably
certain container/QEMU builds), post-load initialization of OpenMP/BLAS/
interop pools can spawn many threads and allocate large per-thread stacks,
triggering MemoryError shortly AFTER the model reports "loaded".
"""

from typing import Optional, List, Dict, Any
import time
import sys

from app.config import settings


# Model configuration
MODEL_NAME = "all-MiniLM-L6-v2"
VECTOR_SIZE = 384
DEFAULT_BATCH_SIZE = 32


def _set_thread_env_hard_limits() -> None:
    """Set env vars that control native thread pools.

    Must run BEFORE importing torch/numpy/sentence_transformers to reliably
    take effect.
    """
    import os

    # OpenMP / BLAS family
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
    os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")

    # HuggingFace tokenizers (Rust/Rayon). "false" should disable parallelism.
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")


class EmbeddingService:
    """Generates text embeddings using sentence-transformers.

    Model is loaded once and reused for all requests.
    """

    def __init__(self):
        self._model = None
        self._model_name = MODEL_NAME
        self._load_time: Optional[float] = None

    @property
    def model(self):
        """Lazy load the embedding model."""
        if self._model is None:
            self._load_model()
        return self._model

    def _load_model(self):
        """Load the sentence-transformers model."""
        _set_thread_env_hard_limits()

        # Must happen BEFORE SentenceTransformer import — PyTorch initializes
        # interop/intraop thread pools on first import. Under QEMU emulation,
        # default pool size × 8MB stack per thread exhausts address space.
        try:
            import torch
            torch.set_num_threads(1)
            torch.set_num_interop_threads(1)
        except Exception:
            pass

        # Import AFTER thread limits are locked in
        from sentence_transformers import SentenceTransformer

        start_time = time.time()
        print(f"Loading embedding model: {self._model_name}...", file=sys.stderr)

        # Fallback keyword argument to force PyTorch over ONNX
        self._model = SentenceTransformer(self._model_name, backend="torch")

        self._load_time = time.time() - start_time
        print(f"Model loaded in {self._load_time:.2f}s", file=sys.stderr)

    def embed_text(self, text: str) -> List[float]:
        """Generate embedding for a single text string."""
        embedding = self.model.encode(text, convert_to_numpy=True)
        return embedding.tolist()

    def embed_texts(
        self,
        texts: List[str],
        batch_size: int = DEFAULT_BATCH_SIZE,
        show_progress: bool = False,
    ) -> List[List[float]]:
        """Generate embeddings for multiple texts in batches."""
        if not texts:
            return []

        embeddings = self.model.encode(
            texts,
            batch_size=batch_size,
            convert_to_numpy=True,
            show_progress_bar=show_progress,
        )

        return embeddings.tolist()

    def get_model_info(self) -> Dict[str, Any]:
        """Get information about the loaded model."""
        return {
            "model_name": self._model_name,
            "vector_size": VECTOR_SIZE,
            "loaded": self._model is not None,
            "load_time_seconds": self._load_time,
        }

    def preload(self):
        """Explicitly preload the model (call at startup)."""
        _ = self.model
        return self.get_model_info()


# Singleton instance
_embedding_service: Optional[EmbeddingService] = None


def get_embedding_service() -> EmbeddingService:
    """Get the singleton embedding service instance."""
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = EmbeddingService()
    return _embedding_service
