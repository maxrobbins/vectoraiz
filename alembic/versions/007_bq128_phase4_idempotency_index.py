"""BQ-128 Phase 4: Partial unique index for message idempotency

Originally created to add a partial unique index on messages(session_id,
client_message_id). However, the messages table lives in vai_state.db
(legacy engine), NOT in vectoraiz.db (Alembic-managed). The index is now
created in _migrate_legacy_bq128() in app/core/database.py instead.

This migration is kept as a no-op to maintain the Alembic revision chain.

Revision ID: 007_bq128_phase4_idempotency_index
Revises: 006_bq128_nudge_dismissals
Create Date: 2026-02-14
"""
from typing import Sequence, Union


revision: str = "007_bq128_p4_idempotency"
down_revision: Union[str, None] = "006_bq128_nudge_dismissals"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # No-op: index created in _migrate_legacy_bq128() on vai_state.db
    pass


def downgrade() -> None:
    pass
