"""
Request Match Engine
====================

Scores how well local datasets match a cached buyer request.
Uses category overlap, freshness, size, and text similarity.

Phase: BQ-VZ-REQUEST-ENGINE Slice B
Created: 2026-04-02
"""

import json
import logging
import math
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.models.cached_requests import CachedRequest
from app.models.dataset import DatasetRecord
from app.models.listing_metadata_schemas import ListingMetadata
from app.schemas.request_engine import RequestMatchSummary

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Weights for composite score
# ---------------------------------------------------------------------------
W_CATEGORY = 0.35
W_TEXT = 0.35
W_FRESHNESS = 0.15
W_SIZE = 0.15


def match_request(
    cached_request: CachedRequest,
    local_datasets: List[DatasetRecord],
) -> List[RequestMatchSummary]:
    """
    Score each local dataset against the cached request.
    Returns matches sorted by score descending.
    """
    request_categories = _parse_json_list(cached_request.categories)
    request_text = f"{cached_request.title} {cached_request.description}".lower()
    request_tokens = _tokenize(request_text)

    results: List[RequestMatchSummary] = []

    for ds in local_datasets:
        metadata = _parse_metadata(ds.metadata_json)
        reasons: Dict[str, Any] = {}

        # --- Category overlap ---
        ds_tags = _get_dataset_tags(metadata)
        cat_score = _category_score(request_categories, ds_tags)
        reasons["category_overlap"] = {
            "score": round(cat_score, 3),
            "request_categories": request_categories,
            "dataset_tags": ds_tags,
        }

        # --- Text similarity (TF-IDF-lite keyword matching) ---
        ds_text = _get_dataset_text(ds, metadata)
        ds_tokens = _tokenize(ds_text)
        text_score = _text_similarity(request_tokens, ds_tokens)
        reasons["text_similarity"] = {"score": round(text_score, 3)}

        # --- Freshness ---
        freshness_score, freshness_cat = _freshness_score(ds, metadata)
        reasons["freshness"] = {
            "score": round(freshness_score, 3),
            "category": freshness_cat,
        }

        # --- Size appropriateness ---
        row_count = _get_row_count(metadata)
        size_score = _size_score(row_count)
        row_range = _row_count_range(row_count)
        reasons["size"] = {
            "score": round(size_score, 3),
            "row_count": row_count,
            "range": row_range,
        }

        # --- Composite ---
        final_score = (
            W_CATEGORY * cat_score
            + W_TEXT * text_score
            + W_FRESHNESS * freshness_score
            + W_SIZE * size_score
        )
        final_score = round(min(max(final_score, 0.0), 1.0), 3)

        title = metadata.title if metadata else ds.original_filename

        results.append(RequestMatchSummary(
            dataset_id=ds.id,
            dataset_title=title,
            score=final_score,
            score_reasons=reasons,
            row_count_range=row_range,
            freshness_category=freshness_cat,
            require_review=final_score < 0.7,
        ))

    results.sort(key=lambda m: m.score, reverse=True)
    return results


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def _category_score(request_cats: List[str], dataset_tags: List[str]) -> float:
    """Jaccard-like overlap between request categories and dataset tags."""
    if not request_cats or not dataset_tags:
        return 0.0
    req_set = {c.lower().strip() for c in request_cats}
    ds_set = {t.lower().strip() for t in dataset_tags}
    overlap = req_set & ds_set
    union = req_set | ds_set
    return len(overlap) / len(union) if union else 0.0


def _text_similarity(request_tokens: Counter, dataset_tokens: Counter) -> float:
    """Cosine similarity between token frequency vectors."""
    if not request_tokens or not dataset_tokens:
        return 0.0
    common = set(request_tokens.keys()) & set(dataset_tokens.keys())
    if not common:
        return 0.0
    dot = sum(request_tokens[t] * dataset_tokens[t] for t in common)
    mag_r = math.sqrt(sum(v * v for v in request_tokens.values()))
    mag_d = math.sqrt(sum(v * v for v in dataset_tokens.values()))
    if mag_r == 0 or mag_d == 0:
        return 0.0
    return dot / (mag_r * mag_d)


def _freshness_score(ds: DatasetRecord, metadata: Optional[ListingMetadata]) -> tuple:
    """Score based on dataset age. Returns (score, category_label)."""
    if metadata and metadata.freshness_score > 0:
        score = metadata.freshness_score
    else:
        age_days = (datetime.now(timezone.utc) - ds.created_at).days
        if age_days <= 7:
            score = 1.0
        elif age_days <= 30:
            score = 0.8
        elif age_days <= 90:
            score = 0.5
        else:
            score = 0.2

    if score >= 0.8:
        cat = "fresh"
    elif score >= 0.5:
        cat = "recent"
    else:
        cat = "stale"
    return score, cat


def _size_score(row_count: int) -> float:
    """Datasets with 100-100K rows score highest. Very small or huge get penalized."""
    if row_count <= 0:
        return 0.3  # unknown size — neutral-low
    if row_count < 10:
        return 0.2
    if row_count < 100:
        return 0.5
    if row_count <= 100_000:
        return 1.0
    if row_count <= 1_000_000:
        return 0.7
    return 0.5


def _row_count_range(row_count: int) -> str:
    if row_count <= 0:
        return "unknown"
    if row_count < 100:
        return "<100"
    if row_count < 1_000:
        return "100-1K"
    if row_count < 10_000:
        return "1K-10K"
    if row_count < 100_000:
        return "10K-100K"
    if row_count < 1_000_000:
        return "100K-1M"
    return "1M+"


# ---------------------------------------------------------------------------
# Data extraction helpers
# ---------------------------------------------------------------------------

_STOP_WORDS = frozenset(
    "a an the is are was were be been being have has had do does did "
    "will would shall should may might can could of in to for on with "
    "at by from as into through during before after above below between "
    "and or but not no nor so yet both either neither each every all "
    "any few more most other some such than too very it its this that "
    "these those i me my we our you your he him his she her they them their".split()
)


def _tokenize(text: str) -> Counter:
    """Simple whitespace + punctuation tokenizer with stop-word removal."""
    words = re.findall(r"[a-z0-9]+", text.lower())
    return Counter(w for w in words if w not in _STOP_WORDS and len(w) > 1)


def _parse_json_list(raw: Any) -> List[str]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except (json.JSONDecodeError, TypeError):
            pass
    return []


def _parse_metadata(metadata_json: str) -> Optional[ListingMetadata]:
    try:
        data = json.loads(metadata_json) if metadata_json else {}
        if data and "title" in data:
            return ListingMetadata(**data)
    except (json.JSONDecodeError, TypeError, Exception):
        pass
    return None


def _get_dataset_tags(metadata: Optional[ListingMetadata]) -> List[str]:
    if metadata:
        return list(metadata.tags) + list(metadata.data_categories)
    return []


def _get_dataset_text(ds: DatasetRecord, metadata: Optional[ListingMetadata]) -> str:
    parts = [ds.original_filename]
    if metadata:
        parts.append(metadata.title)
        parts.append(metadata.description)
        parts.extend(metadata.tags)
    return " ".join(parts).lower()


def _get_row_count(metadata: Optional[ListingMetadata]) -> int:
    if metadata:
        return metadata.row_count
    return 0
