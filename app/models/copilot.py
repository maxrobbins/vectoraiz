"""
Co-Pilot SCI Command Schema
============================

Pydantic models for the Structured Command Interface (SCI) protocol.
Used by the /ws/copilot WebSocket endpoint and REST companion endpoints.

Defines command types, risk levels, and state snapshot structures
for AI-to-frontend communication.

CREATED: S94/BQ-069 (2026-02-06)
SPEC: BQ-CP-01 section 3.2
"""

from pydantic import BaseModel, Field
from typing import Any, List, Optional
from enum import IntEnum, Enum


class RiskLevel(IntEnum):
    """Risk classification for SCI commands.

    SAFE (0): Navigate, search, highlight — auto-execute
    REVERSIBLE (1): Fill forms, run scans — optimistic + undo
    CRITICAL (2): Delete, publish, spend money — explicit approval
    """
    SAFE = 0
    REVERSIBLE = 1
    CRITICAL = 2


class SCICommandType(str, Enum):
    """Types of commands the Co-Pilot can issue to the frontend."""
    NAVIGATE = "NAVIGATE"
    MUTATE_STATE = "MUTATE_STATE"
    EXECUTE = "EXECUTE"
    HIGHLIGHT = "HIGHLIGHT"
    QUERY_STATE = "QUERY_STATE"


class UIHints(BaseModel):
    """Visual hints for the frontend HUD overlay."""
    highlight_element: Optional[str] = None
    explanation: str


class SCICommand(BaseModel):
    """A single command from allAI to the frontend via SCI protocol."""
    id: str = Field(description="Unique command ID, format: cmd_<nanoid>")
    type: SCICommandType
    requires_approval: bool = False
    risk_level: RiskLevel = RiskLevel.SAFE
    action: str = Field(description="Action registry key, e.g. NAVIGATE_TO_DATASETS")
    payload: dict[str, Any] = Field(default_factory=dict)
    ui_hints: UIHints
    timestamp: str


class CommandResult(BaseModel):
    """Result returned by the frontend after executing a command."""
    command_id: str
    success: bool
    error: Optional[str] = None
    state_after: Optional[dict[str, Any]] = None


class DatasetSummaryItem(BaseModel):
    """Lightweight dataset summary sent from frontend in STATE_SNAPSHOT."""
    id: str
    filename: str
    file_type: str = ""
    status: str = "unknown"
    rows: Optional[int] = None
    columns: Optional[int] = None
    size_bytes: Optional[int] = None


class StateSnapshot(BaseModel):
    """Current frontend state, sent periodically or on request."""
    current_route: str
    page_title: str = ""
    active_dataset_id: Optional[str] = None
    form_state: Optional[dict[str, Any]] = None
    dataset_summary: Optional[List[DatasetSummaryItem]] = None
    timestamp: str = ""
