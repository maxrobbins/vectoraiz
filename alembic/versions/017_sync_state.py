"""Add request_engine_sync_state table and matched_dataset_id FK

Revision ID: 017_sync_state
Revises: 016_request_engine
Create Date: 2026-04-02
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = "017_sync_state"
down_revision = "016_request_engine"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "request_engine_sync_state",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("last_cursor", sa.String(), nullable=True),
        sa.Column("last_synced_at", sa.DateTime(), nullable=True),
    )

    # SQLite doesn't support ADD CONSTRAINT for FKs on existing tables,
    # so we use batch mode to recreate the table with the FK.
    with op.batch_alter_table("response_drafts") as batch_op:
        batch_op.create_foreign_key(
            "fk_response_drafts_dataset",
            "dataset_records",
            ["matched_dataset_id"],
            ["id"],
        )

    # Drop redundant index (unique constraint already covers it)
    try:
        op.drop_index("ix_cached_requests_marketplace_id", table_name="cached_requests")
    except Exception:
        pass  # Index may not exist if 016 was applied after the fix


def downgrade() -> None:
    with op.batch_alter_table("response_drafts") as batch_op:
        batch_op.drop_constraint("fk_response_drafts_dataset", type_="foreignkey")

    op.create_index(
        "ix_cached_requests_marketplace_id",
        "cached_requests",
        ["marketplace_request_id"],
    )

    op.drop_table("request_engine_sync_state")
