"""
RAG Data Models
===============

Pydantic models for the RAG pipeline including sources, citations, and responses.

Phase: 3.V.4 (original), 3.W.4 (stateful additions)
Created: 2026-01-25
Updated: 2026-01-25 - Added session/chat models
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class SourceMetadata(BaseModel):
    """Metadata describing the origin of a context chunk."""
    source_id: str
    filename: Optional[str] = None
    page_number: Optional[int] = None
    row_index: Optional[int] = None
    chunk_id: Optional[str] = None
    score: Optional[float] = None
    dataset_id: Optional[str] = None
    extra: Dict[str, Any] = Field(default_factory=dict)


class SourceChunk(BaseModel):
    """A chunk of text provided to the LLM as context."""
    index: int  # The [N] number presented to the LLM
    text: str
    metadata: SourceMetadata


class Citation(BaseModel):
    """A specific reference found in the LLM response."""
    source_index: int
    is_valid: bool = True
    source: Optional[SourceChunk] = None  # Hydrated if valid


class ParsedRAGResponse(BaseModel):
    """The final structured response from the RAG system."""
    answer: str
    citations: List[Citation]
    unique_sources_cited: List[SourceChunk]
    model_used: Optional[str] = None
    
    # Timing metrics
    retrieval_time_ms: Optional[float] = None
    generation_time_ms: Optional[float] = None
    total_time_ms: Optional[float] = None
    
    # Debug info
    chunks_retrieved: int = 0
    template_used: Optional[str] = None
    
    # Stateful session info (Phase 3.W.4)
    session_id: Optional[str] = None
    message_id: Optional[str] = None


# =============================================================================
# API Request/Response Models
# =============================================================================

class RAGRequest(BaseModel):
    """Request model for stateless RAG generation endpoint."""
    question: str
    dataset_id: Optional[str] = None  # Search specific dataset or all
    top_k: int = Field(default=5, ge=1, le=20)
    min_score: Optional[float] = Field(default=0.3, ge=0, le=1)
    template: str = "rag_qa"  # Template to use
    
    # Optional overrides
    temperature: Optional[float] = Field(default=None, ge=0, le=1)
    max_tokens: Optional[int] = Field(default=None, ge=1, le=4096)


class ChatRequest(BaseModel):
    """Request model for stateful chat endpoint."""
    session_id: Optional[str] = None  # If None, creates new session
    question: str
    dataset_id: Optional[str] = None  # Scope to specific dataset
    top_k: int = Field(default=5, ge=1, le=20)
    min_score: Optional[float] = Field(default=0.3, ge=0, le=1)
    template: str = "rag_chat"
    max_context_tokens: int = Field(default=4000, ge=500, le=16000)
    
    # Optional overrides
    temperature: Optional[float] = Field(default=None, ge=0, le=1)
    max_tokens: Optional[int] = Field(default=None, ge=1, le=4096)


class RAGResponse(BaseModel):
    """Response model for stateless RAG generation endpoint."""
    answer: str
    sources: List[Dict[str, Any]]  # Simplified source info for API response
    citations_count: int
    model_info: Dict[str, Any]
    
    # Timing
    retrieval_time_ms: float
    generation_time_ms: float
    total_time_ms: float


class ChatResponse(BaseModel):
    """Response model for stateful chat endpoint."""
    session_id: str
    message_id: str
    answer: str
    sources: List[Dict[str, Any]]
    citations_count: int
    model_info: Dict[str, Any]
    
    # Timing
    retrieval_time_ms: float
    generation_time_ms: float
    total_time_ms: float
    
    # Context info
    context_truncated: bool = False
    history_messages_used: int = 0


class NewSessionResponse(BaseModel):
    """Response when a new session is created."""
    session_id: str
    title: Optional[str] = None
    created_at: str
