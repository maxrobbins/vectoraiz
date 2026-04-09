"""
allAI Router - Local RAG Generation API
=======================================

Provides the `/api/allai/generate` endpoint for RAG-powered Q&A.
This is the core intelligence interface for vectorAIz.

Phase: 3.V.6
Created: 2026-01-25
"""

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from typing import Dict, Any
import logging

from app.config import settings
from app.core.errors import VectorAIzError
from app.services.rag_service import get_rag_service, RAGService
from app.models.rag import RAGRequest, RAGResponse
from app.auth.api_key_auth import get_current_user, AuthenticatedUser
from app.services.serial_metering import metered, MeterDecision

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/generate", response_model=RAGResponse)
async def generate(
    request: RAGRequest,
    rag_service: RAGService = Depends(get_rag_service),
    user: AuthenticatedUser = Depends(get_current_user),
    _meter: MeterDecision = Depends(metered("data")),
):
    """
    Generate an AI response grounded in your indexed datasets.
    
    This is the core RAG (Retrieval-Augmented Generation) endpoint.
    It retrieves relevant context from your datasets and generates
    a factually grounded response using your configured LLM.
    
    **How it works:**
    1. Your question is embedded using the local embedding model
    2. Qdrant finds the top_k most relevant chunks from your data
    3. Retrieved context is injected into a grounded prompt
    4. Your LLM generates a cited, factual response
    
    **Parameters:**
    - question: The question or prompt to answer
    - dataset_id: Optional - search specific dataset only
    - top_k: Number of context chunks (1-20, default: 5)
    - min_score: Minimum similarity (0-1, default: 0.3)
    - template: Prompt template to use (default: rag_qa)
    
    **Requires:** X-API-Key header validated against ai.market
    """
    try:
        # Build optional LLM kwargs from request
        llm_kwargs = {}
        if request.temperature is not None:
            llm_kwargs["temperature"] = request.temperature
        if request.max_tokens is not None:
            llm_kwargs["max_tokens"] = request.max_tokens
        
        result = await rag_service.query(
            question=request.question,
            dataset_id=request.dataset_id,
            top_k=request.top_k,
            min_score=request.min_score or 0.3,
            template=request.template,
            **llm_kwargs
        )
        
        # Transform to API response format
        sources = [
            {
                "index": src.index,
                "text": src.text[:500] + "..." if len(src.text) > 500 else src.text,
                "filename": src.metadata.filename,
                "dataset_id": src.metadata.dataset_id,
                "row_index": src.metadata.row_index,
                "score": src.metadata.score,
            }
            for src in result.unique_sources_cited
        ]
        
        return RAGResponse(
            answer=result.answer,
            sources=sources,
            citations_count=len(result.citations),
            model_info={
                "model": result.model_used,
                "template": result.template_used,
            },
            retrieval_time_ms=result.retrieval_time_ms or 0,
            generation_time_ms=result.generation_time_ms or 0,
            total_time_ms=result.total_time_ms or 0,
        )
        
    except ValueError as e:
        raise VectorAIzError("VAI-RAG-001", detail=str(e))
    except RuntimeError as e:
        raise VectorAIzError("VAI-RAG-003", detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected RAG error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error during generation")


@router.post("/generate/stream")
async def generate_stream(
    request: RAGRequest,
    rag_service: RAGService = Depends(get_rag_service),
    user: AuthenticatedUser = Depends(get_current_user),
    _meter: MeterDecision = Depends(metered("data")),
):
    """
    Stream a RAG response for real-time display.
    
    Returns a Server-Sent Events (SSE) stream of text chunks.
    Citations [N] will appear in the text but structured parsing
    is not available in streaming mode.
    
    **Requires:** X-API-Key header validated against ai.market
    """
    try:
        llm_kwargs = {}
        if request.temperature is not None:
            llm_kwargs["temperature"] = request.temperature
        if request.max_tokens is not None:
            llm_kwargs["max_tokens"] = request.max_tokens
        
        async def event_generator():
            async for chunk in rag_service.query_stream(
                question=request.question,
                dataset_id=request.dataset_id,
                top_k=request.top_k,
                min_score=request.min_score or 0.3,
                template=request.template,
                **llm_kwargs
            ):
                yield f"data: {chunk}\n\n"
            yield "data: [DONE]\n\n"
        
        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            }
        )
        
    except Exception as e:
        logger.error(f"RAG streaming error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Streaming generation failed")


@router.get("/status")
async def allai_status(
    rag_service: RAGService = Depends(get_rag_service),
):
    """
    Get allAI service status including LLM configuration.
    
    Returns information about:
    - Current LLM provider and model
    - Available prompt templates
    - Service health
    
    **No authentication required.**
    """
    return rag_service.get_status()


@router.get("/templates")
async def list_templates(
    rag_service: RAGService = Depends(get_rag_service),
):
    """
    List available prompt templates for RAG generation.
    
    Templates control how context is presented to the LLM
    and can be optimized for different use cases.
    
    **No authentication required.**
    """
    return {
        "templates": rag_service.prompt_registry.list_templates(),
        "default": "rag_qa"
    }


# =============================================================================
# LLM Client Connection Endpoints
# =============================================================================

def _build_openapi_action_spec() -> Dict[str, Any]:
    """Build a ChatGPT Custom GPT Actions-compatible OpenAPI 3.1 spec."""
    base_url = settings.public_url.rstrip("/")
    return {
        "openapi": "3.1.0",
        "info": {
            "title": "vectorAIz - Search & Ask Your Data",
            "description": (
                "Search and ask questions about your indexed datasets using "
                "vectorAIz's RAG (Retrieval-Augmented Generation) engine. "
                "Upload data to vectorAIz first, then use these actions to "
                "query it from ChatGPT."
            ),
            "version": "1.0.0",
        },
        "servers": [{"url": base_url}],
        "paths": {
            "/api/allai/generate": {
                "post": {
                    "operationId": "askVectoraiz",
                    "summary": "Ask a question about your indexed data",
                    "description": (
                        "Send a natural-language question and get an AI-generated answer "
                        "grounded in your vectorAIz datasets. The response includes "
                        "cited sources so you can verify the information."
                    ),
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["question"],
                                    "properties": {
                                        "question": {
                                            "type": "string",
                                            "description": "The question to answer using your indexed data.",
                                        },
                                        "dataset_id": {
                                            "type": "string",
                                            "description": "Optional: restrict search to a specific dataset ID.",
                                        },
                                        "top_k": {
                                            "type": "integer",
                                            "default": 5,
                                            "minimum": 1,
                                            "maximum": 20,
                                            "description": "Number of context chunks to retrieve.",
                                        },
                                    },
                                },
                            },
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "AI-generated answer with sources",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "answer": {
                                                "type": "string",
                                                "description": "The AI-generated answer.",
                                            },
                                            "sources": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "text": {"type": "string"},
                                                        "filename": {"type": "string"},
                                                        "score": {"type": "number"},
                                                    },
                                                },
                                                "description": "Source chunks that were cited.",
                                            },
                                            "citations_count": {
                                                "type": "integer",
                                            },
                                        },
                                    },
                                },
                            },
                        },
                        "401": {"description": "Missing or invalid API key"},
                    },
                    "security": [{"apiKeyHeader": []}],
                },
            },
            "/api/search/": {
                "get": {
                    "operationId": "searchVectoraiz",
                    "summary": "Semantic search across your datasets",
                    "description": (
                        "Search your indexed datasets using natural language. "
                        "Returns the most relevant results ranked by similarity."
                    ),
                    "parameters": [
                        {
                            "name": "q",
                            "in": "query",
                            "required": True,
                            "schema": {"type": "string"},
                            "description": "Search query in natural language.",
                        },
                        {
                            "name": "dataset_id",
                            "in": "query",
                            "required": False,
                            "schema": {"type": "string"},
                            "description": "Optional: restrict to a specific dataset.",
                        },
                        {
                            "name": "limit",
                            "in": "query",
                            "required": False,
                            "schema": {"type": "integer", "default": 10},
                            "description": "Max number of results (1-100).",
                        },
                    ],
                    "responses": {
                        "200": {
                            "description": "Search results ranked by relevance",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "results": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "text_content": {"type": "string"},
                                                        "dataset_name": {"type": "string"},
                                                        "score": {"type": "number"},
                                                    },
                                                },
                                            },
                                            "total": {"type": "integer"},
                                        },
                                    },
                                },
                            },
                        },
                        "401": {"description": "Missing or invalid API key"},
                    },
                    "security": [{"apiKeyHeader": []}],
                },
            },
        },
        "components": {
            "securitySchemes": {
                "apiKeyHeader": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-API-Key",
                    "description": (
                        "Your vectorAIz API key. Find it in Settings > API Keys "
                        "in the vectorAIz dashboard."
                    ),
                },
            },
        },
    }


@router.get("/openapi-action-spec")
async def openapi_action_spec():
    """
    Get a ready-to-paste OpenAPI 3.1 spec for ChatGPT Custom GPT Actions.

    Copy the JSON response and paste it directly into ChatGPT's
    "Create Action" dialog to connect ChatGPT to your vectorAIz data.

    **No authentication required** (the spec itself is not sensitive).
    """
    spec = _build_openapi_action_spec()
    return JSONResponse(content=spec, media_type="application/json")


@router.get("/connect-guide")
async def connect_guide():
    """
    Get a structured connection guide for hooking up external LLM clients
    (ChatGPT Custom GPTs, Claude MCP, etc.) to this vectorAIz instance.

    **No authentication required.**
    """
    base_url = settings.public_url.rstrip("/")
    spec_url = f"{base_url}/api/allai/openapi-action-spec"

    return {
        "vectoraiz_url": base_url,
        "openapi_spec_url": spec_url,
        "api_key_location": (
            "Go to the vectorAIz dashboard → Settings → API Keys → "
            "Create New Key. Your key will look like vz_xxxxxxxx_xxxxxxxx..."
        ),
        "chatgpt_custom_gpt": {
            "title": "Connect ChatGPT (Custom GPT with Actions)",
            "steps": [
                "1. Go to https://chatgpt.com and click your profile → My GPTs → Create a GPT.",
                "2. In the 'Configure' tab, give your GPT a name like 'My Data Assistant'.",
                "3. Set the instructions to something like: 'You help the user search and ask questions about their data stored in vectorAIz. Use the askVectoraiz action to answer questions and searchVectoraiz to find information.'",
                "4. Scroll down to 'Actions' and click 'Create new action'.",
                f"5. Under 'Import from URL', paste: {spec_url}",
                "6. Alternatively, switch the schema box to 'Import from URL' and paste the URL, or copy the JSON from that URL and paste it into the schema editor.",
                "7. Under 'Authentication', select 'API Key', set Auth Type to 'Custom', Header Name to 'X-API-Key', and paste your vectorAIz API key.",
                "8. Click 'Save' and test by asking your GPT a question about your data!",
            ],
        },
        "claude_mcp": {
            "title": "Connect Claude Desktop (MCP)",
            "steps": [
                "1. Open your Claude Desktop config file:",
                "   - macOS: ~/Library/Application Support/Claude/claude_desktop_config.json",
                "   - Windows: %APPDATA%\\Claude\\claude_desktop_config.json",
                "2. Add a new MCP server entry under 'mcpServers':",
                f'   "vectoraiz": {{',
                f'     "command": "npx",',
                f'     "args": ["-y", "@anthropic-ai/mcp-remote", "{base_url}/mcp/sse"],',
                f'     "env": {{',
                f'       "API_KEY": "YOUR_VECTORAIZ_API_KEY_HERE"',
                f'     }}',
                f'   }}',
                "3. Replace YOUR_VECTORAIZ_API_KEY_HERE with your actual vectorAIz API key.",
                "4. Restart Claude Desktop. You should see vectorAIz tools available.",
                "5. Ask Claude to search or query your data — it will use the MCP tools automatically.",
            ],
            "note": (
                "MCP connectivity requires the vectorAIz Connectivity feature to be enabled. "
                "Go to Settings → Connectivity in the vectorAIz dashboard to enable it."
            ),
        },
        "generic_rest_api": {
            "title": "Connect via REST API (any client)",
            "endpoints": {
                "ask": {
                    "method": "POST",
                    "url": f"{base_url}/api/allai/generate",
                    "headers": {"X-API-Key": "YOUR_API_KEY", "Content-Type": "application/json"},
                    "body": {"question": "What is the total revenue?", "top_k": 5},
                },
                "search": {
                    "method": "GET",
                    "url": f"{base_url}/api/search/?q=revenue&limit=10",
                    "headers": {"X-API-Key": "YOUR_API_KEY"},
                },
            },
        },
    }
