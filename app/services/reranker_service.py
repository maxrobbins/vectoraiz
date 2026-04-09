"""
Cross-Encoder Reranker Service (BQ-VZ-HYBRID-SEARCH Phase 1A)
=============================================================
Lazy-loaded cross-encoder model for reranking search results.
Includes circuit breaker: if reranking exceeds timeout, returns un-reranked results.
"""

import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Optional, List, Dict, Any

from app.config import settings

logger = logging.getLogger(__name__)

RERANKER_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"


class RerankerService:
    """Reranks search results using a cross-encoder model with timeout protection."""

    def __init__(self):
        self._model = None
        self._load_time: Optional[float] = None
        self._consecutive_timeouts: int = 0
        self._circuit_open: bool = False
        # Circuit breaker: open after 3 consecutive timeouts, reset on success
        self._circuit_threshold: int = 3

    @property
    def model(self):
        """Lazy-load the cross-encoder model."""
        if self._model is None:
            self._load_model()
        return self._model

    def _load_model(self):
        """Load the cross-encoder model."""
        from sentence_transformers import CrossEncoder

        start = time.time()
        logger.info("Loading reranker model: %s ...", RERANKER_MODEL)
        print(f"Loading reranker model: {RERANKER_MODEL}...", file=sys.stderr)

        self._model = CrossEncoder(RERANKER_MODEL)

        self._load_time = time.time() - start
        logger.info("Reranker model loaded in %.2fs", self._load_time)
        print(f"Reranker model loaded in {self._load_time:.2f}s", file=sys.stderr)

    def rerank(
        self,
        query: str,
        documents: List[Dict[str, Any]],
        text_key: str = "text_content",
        top_k: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Rerank documents by cross-encoder relevance score.

        Args:
            query: The search query.
            documents: List of result dicts, each containing text at text_key.
            text_key: Key in each doc dict containing the text to score.
            top_k: Return top-k after reranking (default: settings.reranker_top_k).

        Returns:
            Reranked documents with 'rerank_score' added. Falls back to
            original order if circuit breaker is open or timeout occurs.
        """
        if not documents:
            return documents

        if top_k is None:
            top_k = settings.reranker_top_k

        # Circuit breaker check
        if self._circuit_open:
            logger.warning("Reranker circuit breaker OPEN — returning un-reranked results")
            return documents[:top_k]

        timeout_ms = settings.reranker_timeout_ms

        try:
            # Build query-document pairs
            pairs = []
            for doc in documents:
                text = doc.get(text_key, "")
                if not text:
                    text = str(doc.get("row_data", ""))
                pairs.append((query, text))

            start = time.time()
            timeout_sec = timeout_ms / 1000.0

            # Run predict() in a thread with hard timeout.
            # Don't use context manager — __exit__ calls shutdown(wait=True)
            # which blocks until the worker finishes, defeating the timeout.
            pool = ThreadPoolExecutor(max_workers=1)
            future = pool.submit(self.model.predict, pairs)
            try:
                scores = future.result(timeout=timeout_sec)
            except FuturesTimeoutError:
                # Fire-and-forget: let the worker finish on its own
                pool.shutdown(wait=False, cancel_futures=True)
                logger.warning(
                    "Reranker hard timeout after %dms — returning un-reranked results",
                    timeout_ms,
                )
                self._consecutive_timeouts += 1
                if self._consecutive_timeouts >= self._circuit_threshold:
                    self._circuit_open = True
                    logger.error("Reranker circuit breaker OPENED after %d consecutive timeouts", self._circuit_threshold)
                return documents[:top_k]
            else:
                pool.shutdown(wait=False)

            elapsed_ms = (time.time() - start) * 1000

            if elapsed_ms > timeout_ms:
                logger.warning(
                    "Reranker exceeded timeout: %.0fms > %dms — results still used but timeout counted",
                    elapsed_ms, timeout_ms,
                )
                self._consecutive_timeouts += 1
                if self._consecutive_timeouts >= self._circuit_threshold:
                    self._circuit_open = True
                    logger.error("Reranker circuit breaker OPENED after %d consecutive timeouts", self._circuit_threshold)
            else:
                # Reset on success within timeout
                self._consecutive_timeouts = 0

            # Attach scores and sort
            scored_docs = []
            for doc, score in zip(documents, scores):
                doc_copy = dict(doc)
                doc_copy["rerank_score"] = float(score)
                scored_docs.append(doc_copy)

            scored_docs.sort(key=lambda x: x["rerank_score"], reverse=True)

            logger.debug("Reranked %d docs in %.0fms", len(documents), elapsed_ms)
            return scored_docs[:top_k]

        except Exception as e:
            logger.error("Reranker failed: %s — returning un-reranked results", e)
            self._consecutive_timeouts += 1
            if self._consecutive_timeouts >= self._circuit_threshold:
                self._circuit_open = True
            return documents[:top_k]

    def reset_circuit_breaker(self):
        """Manually reset the circuit breaker."""
        self._circuit_open = False
        self._consecutive_timeouts = 0

    def is_loaded(self) -> bool:
        return self._model is not None

    def get_info(self) -> Dict[str, Any]:
        return {
            "model_name": RERANKER_MODEL,
            "loaded": self.is_loaded(),
            "load_time_seconds": self._load_time,
            "circuit_open": self._circuit_open,
            "consecutive_timeouts": self._consecutive_timeouts,
        }


# Singleton
_reranker_service: Optional[RerankerService] = None


def get_reranker_service() -> RerankerService:
    """Get the singleton reranker service instance."""
    global _reranker_service
    if _reranker_service is None:
        _reranker_service = RerankerService()
    return _reranker_service
