"""
Vector collection management endpoints.

BQ-110: All sync QdrantService calls wrapped via run_sync() to avoid
blocking the event loop. Upgrade path: migrate QdrantService to
AsyncQdrantClient (available in qdrant_client) for native async.
"""

from fastapi import APIRouter, HTTPException, Depends

from app.core.async_utils import run_sync
from app.services.qdrant_service import get_qdrant_service, QdrantService
from app.services.embedding_service import get_embedding_service, EmbeddingService
from app.auth.api_key_auth import get_current_user, AuthenticatedUser

router = APIRouter()


@router.get("/health")
async def vector_health(
    qdrant: QdrantService = Depends(get_qdrant_service)
):
    """Check Qdrant vector database health."""
    try:
        return await run_sync(qdrant.health_check)
    except ConnectionError:
        raise HTTPException(status_code=503, detail="Qdrant service unavailable")


@router.get("/collections")
async def list_collections(
    qdrant: QdrantService = Depends(get_qdrant_service)
):
    """List all vector collections."""
    try:
        collections = await run_sync(qdrant.list_collections)
        return {
            "collections": collections,
            "count": len(collections)
        }
    except ConnectionError:
        raise HTTPException(status_code=503, detail="Qdrant service unavailable")


@router.post("/collections/{collection_name}")
async def create_collection(
    collection_name: str,
    recreate: bool = False,
    user: AuthenticatedUser = Depends(get_current_user),
    qdrant: QdrantService = Depends(get_qdrant_service)
):
    """
    Create a new vector collection.

    Args:
        collection_name: Name for the collection (typically dataset_id)
        recreate: If true, delete existing collection first

    Requires: X-API-Key header
    """
    try:
        info = await run_sync(qdrant.create_collection, collection_name, recreate)
        return {
            "message": f"Collection '{collection_name}' created successfully",
            "collection": info
        }
    except ConnectionError:
        raise HTTPException(status_code=503, detail="Qdrant service unavailable")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/collections/{collection_name}")
async def get_collection(
    collection_name: str,
    qdrant: QdrantService = Depends(get_qdrant_service)
):
    """Get information about a specific collection."""
    try:
        info = await run_sync(qdrant.get_collection_info, collection_name)
        return info
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ConnectionError:
        raise HTTPException(status_code=503, detail="Qdrant service unavailable")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/collections/{collection_name}")
async def delete_collection(
    collection_name: str,
    user: AuthenticatedUser = Depends(get_current_user),
    qdrant: QdrantService = Depends(get_qdrant_service)
):
    """
    Delete a vector collection.

    Requires: X-API-Key header
    """
    try:
        success = await run_sync(qdrant.delete_collection, collection_name)
    except ConnectionError:
        raise HTTPException(status_code=503, detail="Qdrant service unavailable")
    if success:
        return {"message": f"Collection '{collection_name}' deleted successfully"}
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Collection '{collection_name}' not found"
        )


@router.get("/collections/{collection_name}/count")
async def get_collection_count(
    collection_name: str,
    qdrant: QdrantService = Depends(get_qdrant_service)
):
    """Get the number of vectors in a collection."""
    try:
        count = await run_sync(qdrant.get_vector_count, collection_name)
        return {
            "collection": collection_name,
            "vector_count": count
        }
    except ConnectionError:
        raise HTTPException(status_code=503, detail="Qdrant service unavailable")
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/embedding/info")
async def get_embedding_info(
    embedding: EmbeddingService = Depends(get_embedding_service)
):
    """Get information about the embedding model."""
    return await run_sync(embedding.get_model_info)


@router.post("/embedding/test")
async def test_embedding(
    text: str,
    user: AuthenticatedUser = Depends(get_current_user),
    embedding: EmbeddingService = Depends(get_embedding_service)
):
    """
    Test embedding generation with a sample text.

    Requires: X-API-Key header
    """
    vector = await run_sync(embedding.embed_text, text)
    return {
        "text": text,
        "vector_size": len(vector),
        "vector_preview": vector[:10],  # First 10 dimensions
    }
