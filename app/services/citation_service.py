"""
Citation Service for vectorAIz RAG.
Parses, validates, and extracts citations from LLM responses.

PHASE: 3.V.4
CREATED: 2026-01-25 (S31)
"""

import re
import logging
from typing import List, Optional
from app.models.rag import Citation, SourceChunk, ValidationResult

logger = logging.getLogger(__name__)


class CitationService:
    """
    Service to parse, validate, and extract citations from LLM responses.
    Handles the [source_N] notation used in vectorAIz RAG.
    
    Usage:
        service = get_citation_service()
        
        # Parse citations from text
        citations = service.parse_citations("Based on [source_1], the data shows...")
        
        # Validate against context
        result = service.validate_citations(citations, context_chunks)
        
        # Extract which sources were actually used
        used_sources = service.extract_cited_content(text, sources)
    """
    
    # Regex to find [source_1], [source_12], etc.
    CITATION_PATTERN = re.compile(r'\[source_(\d+)\]')

    def parse_citations(self, text: str) -> List[Citation]:
        """
        Extract all citation tags from the text.
        
        Note: Does NOT validate if they exist in context.
        Use validate_citations() for that.
        
        Args:
            text: LLM-generated text containing citations
            
        Returns:
            List of Citation objects (is_valid defaults to False)
        """
        citations = []
        seen_indices = set()  # Track unique citations
        matches = self.CITATION_PATTERN.finditer(text)
        
        for match in matches:
            try:
                source_index = int(match.group(1))
                
                # Only add unique citations
                if source_index not in seen_indices:
                    citations.append(Citation(
                        source_index=source_index,
                        text_reference=match.group(0),
                        is_valid=False  # Default to False until validated
                    ))
                    seen_indices.add(source_index)
                    
            except ValueError:
                continue
                
        return citations

    def validate_citations(
        self, 
        citations: List[Citation], 
        context_chunks: List[str]
    ) -> ValidationResult:
        """
        Validate citations against the provided context chunks.
        
        [source_1] maps to context_chunks[0] (1-indexed to 0-indexed).
        
        Args:
            citations: List of parsed citations
            context_chunks: List of context strings used in the prompt
            
        Returns:
            ValidationResult with validation statistics
        """
        total_chunks = len(context_chunks)
        valid_count = 0
        invalid_count = 0
        
        for citation in citations:
            # Check bounds: source_1 corresponds to index 0
            if 1 <= citation.source_index <= total_chunks:
                citation.is_valid = True
                valid_count += 1
            else:
                citation.is_valid = False
                invalid_count += 1
                logger.warning(
                    f"Invalid citation: {citation.text_reference} "
                    f"(Max valid: [source_{total_chunks}])"
                )
        
        return ValidationResult(
            total_citations=len(citations),
            valid_citations=valid_count,
            invalid_citations=invalid_count,
            citations=citations
        )

    def extract_cited_content(
        self, 
        text: str, 
        sources: List[SourceChunk]
    ) -> List[SourceChunk]:
        """
        Identify which source chunks were actually cited in the text.
        
        Args:
            text: LLM-generated response
            sources: List of SourceChunk objects provided as context
            
        Returns:
            Unique list of SourceChunk objects that were referenced
        """
        citations = self.parse_citations(text)
        
        # Map source_index to SourceChunk
        # sources[0] corresponds to [source_1]
        source_map = {i + 1: source for i, source in enumerate(sources)}
        
        used_sources = {}
        
        for citation in citations:
            if citation.source_index in source_map:
                source = source_map[citation.source_index]
                used_sources[source.chunk_id] = source
                
        return list(used_sources.values())

    def enrich_citations(
        self, 
        citations: List[Citation], 
        sources: List[SourceChunk]
    ) -> List[Citation]:
        """
        Add metadata from sources to the citation objects.
        
        Args:
            citations: List of parsed citations
            sources: List of SourceChunk objects
            
        Returns:
            Citations enriched with chunk_id and metadata
        """
        source_map = {i + 1: source for i, source in enumerate(sources)}
        
        for citation in citations:
            if citation.source_index in source_map:
                source = source_map[citation.source_index]
                citation.chunk_id = source.chunk_id
                citation.metadata = source.metadata
                citation.is_valid = True
            else:
                citation.is_valid = False
                
        return citations

    def count_citations(self, text: str) -> int:
        """Quick count of citations in text."""
        matches = self.CITATION_PATTERN.findall(text)
        return len(set(matches))  # Unique citations only

    def strip_citations(self, text: str) -> str:
        """Remove all citation markers from text."""
        return self.CITATION_PATTERN.sub('', text).strip()


# Singleton instance
_citation_service: Optional[CitationService] = None


def get_citation_service() -> CitationService:
    """Get the singleton CitationService instance."""
    global _citation_service
    if _citation_service is None:
        _citation_service = CitationService()
    return _citation_service
