"""BQ-VZ-REQUEST-ENGINE: Create cached_requests and response_drafts tables

Revision ID: 016_request_engine
Revises: 015_notifications
Create Date: 2026-04-02
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = "016_request_engine"
down_revision = "015_notifications"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cached_requests",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("marketplace_request_id", sa.String(36), nullable=False, unique=True),
        sa.Column("title", sa.String(512), nullable=False),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.Column("categories", sa.Text(), nullable=False, server_default="[]"),
        sa.Column("urgency", sa.String(32), nullable=True),
        sa.Column("status", sa.String(32), nullable=False, server_default="open"),
        sa.Column("published_at", sa.DateTime(), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("synced_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("match_run_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_cached_requests_marketplace_id", "cached_requests", ["marketplace_request_id"])
    op.create_index("ix_cached_requests_status", "cached_requests", ["status"])

    op.create_table(
        "response_drafts",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("cached_request_id", sa.String(36), sa.ForeignKey("cached_requests.id"), nullable=False),
        sa.Column("matched_dataset_id", sa.String(36), nullable=False),
        sa.Column("title", sa.String(512), nullable=False),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.Column("score", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("score_reasons", sa.Text(), nullable=False, server_default="{}"),
        sa.Column("require_review", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("status", sa.String(16), nullable=False, server_default="draft"),
        sa.Column("internal_notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_response_drafts_cached_request_id", "response_drafts", ["cached_request_id"])
    op.create_index("ix_response_drafts_matched_dataset_id", "response_drafts", ["matched_dataset_id"])
    op.create_index("ix_response_drafts_status", "response_drafts", ["status"])


def downgrade() -> None:
    op.drop_table("response_drafts")
    op.drop_table("cached_requests")
