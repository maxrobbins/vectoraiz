"""
Semantic search API endpoints.

BQ-110: All sync SearchService calls wrapped via run_sync().
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional
from pydantic import BaseModel

from app.core.async_utils import run_sync
from app.core.errors import VectorAIzError
from app.services.search_service import get_search_service, SearchService
from app.auth.api_key_auth import get_current_user, AuthenticatedUser


router = APIRouter()


class SearchRequest(BaseModel):
    """Search request body for POST endpoint."""
    query: str
    dataset_id: Optional[str] = None
    limit: int = 10
    min_score: Optional[float] = None


@router.get("")
async def search_get(
    q: str = Query(..., description="Search query"),
    dataset_id: Optional[str] = Query(None, description="Search within specific dataset"),
    limit: int = Query(10, ge=1, le=100, description="Max results"),
    min_score: Optional[float] = Query(None, ge=0, le=1, description="Minimum relevance score"),
    search_service: SearchService = Depends(get_search_service),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """
    Semantic search across datasets (GET method).

    Returns results ranked by relevance with full row data.
    Requires X-API-Key header.
    """
    try:
        results = await run_sync(
            search_service.search,
            q, dataset_id, limit, min_score,
        )
        return results
    except ValueError as e:
        # Return empty results instead of 404 — let frontend show "no results" state
        return {
            "query": q,
            "results": [],
            "total": 0,
            "datasets_searched": 0,
            "duration_ms": 0,
            "message": str(e),
        }
    except ConnectionError:
        raise VectorAIzError("VAI-QDR-001", detail="Qdrant connection refused during search")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("")
async def search_post(
    request: SearchRequest,
    search_service: SearchService = Depends(get_search_service),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """
    Semantic search across datasets (POST method).

    Use this for longer queries or when embedding query in request body.
    Requires X-API-Key header.
    """
    try:
        results = await run_sync(
            search_service.search,
            request.query, request.dataset_id, request.limit, request.min_score,
        )
        return results
    except ValueError as e:
        return {
            "query": request.query,
            "results": [],
            "total": 0,
            "datasets_searched": 0,
            "duration_ms": 0,
            "message": str(e),
        }
    except ConnectionError:
        raise VectorAIzError("VAI-QDR-001", detail="Qdrant connection refused during search")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dataset/{dataset_id}")
async def search_dataset(
    dataset_id: str,
    q: str = Query(..., description="Search query"),
    limit: int = Query(10, ge=1, le=100, description="Max results"),
    min_score: Optional[float] = Query(None, ge=0, le=1, description="Minimum relevance score"),
    search_service: SearchService = Depends(get_search_service),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """
    Search within a specific dataset.

    Returns results only from the specified dataset.
    Requires X-API-Key header.
    """
    try:
        results = await run_sync(
            search_service.search_dataset,
            dataset_id, q, limit, min_score,
        )
        return results
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ConnectionError:
        raise HTTPException(status_code=503, detail="Search service unavailable")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def search_stats(
    search_service: SearchService = Depends(get_search_service),
):
    """
    Get statistics about searchable datasets.

    Returns count of indexed datasets and total vectors.
    """
    return await run_sync(search_service.get_search_stats)


@router.get("/suggest")
async def search_suggest(
    q: str = Query(..., min_length=2, description="Partial query for suggestions"),
    dataset_id: Optional[str] = Query(None, description="Limit to specific dataset"),
    limit: int = Query(5, ge=1, le=20, description="Max suggestions"),
    search_service: SearchService = Depends(get_search_service),
):
    """
    Get search suggestions based on partial query.

    Returns top matches that can be used for autocomplete.
    """
    try:
        results = await run_sync(
            search_service.search,
            q, dataset_id, limit, 0.3,
        )

        # Return simplified suggestions
        suggestions = [
            {
                "text": r["text_content"][:100] + "..." if len(r["text_content"]) > 100 else r["text_content"],
                "dataset": r["dataset_name"],
                "score": r["score"],
            }
            for r in results["results"]
        ]

        return {
            "query": q,
            "suggestions": suggestions,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
