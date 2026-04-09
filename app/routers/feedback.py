"""
Feedback Router
===============

Admin endpoint for listing user-submitted feedback.

Phase: Feedback/Support Tool
Created: 2026-02-19
"""

import logging

from fastapi import APIRouter, Depends
from sqlmodel import select

from app.auth.api_key_auth import AuthenticatedUser, get_current_user
from app.core.database import get_session_context
from app.models.feedback import Feedback

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/feedback")
async def list_feedback(
    _user: AuthenticatedUser = Depends(get_current_user),
):
    """List all feedback entries (requires API key auth)."""
    with get_session_context() as session:
        results = session.exec(
            select(Feedback).order_by(Feedback.created_at.desc())
        ).all()

        return {
            "feedback": [
                {
                    "id": fb.id,
                    "category": fb.category,
                    "summary": fb.summary,
                    "details": fb.details,
                    "user_id": fb.user_id,
                    "created_at": fb.created_at.isoformat() if fb.created_at else None,
                    "forwarded": fb.forwarded,
                }
                for fb in results
            ],
            "total": len(results),
        }
