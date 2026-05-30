"""Add ai_market_user_id link column to users table.

Revision ID: 019_aim_user_link
Revises: 018_raw_files_metadata
Create Date: 2026-05-25
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "019_aim_user_link"
down_revision: Union[str, None] = "018_raw_files_metadata"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("ai_market_user_id", sa.String(36), nullable=True))
    op.create_index(
        "ix_users_ai_market_user_id",
        "users",
        ["ai_market_user_id"],
        unique=False,
    )
    op.alter_column(
        "users",
        "pw_hash",
        existing_type=sa.String(255),
        nullable=True,
    )


def downgrade() -> None:
    op.drop_index("ix_users_ai_market_user_id", table_name="users")
    op.drop_column("users", "ai_market_user_id")
    # Do not force pw_hash back to NOT NULL; ai.market-linked rows may have NULL.
