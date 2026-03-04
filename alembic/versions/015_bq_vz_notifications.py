"""BQ-VZ-NOTIFICATIONS: Create notifications table for persistent notification system

Revision ID: 015_notifications
Revises: 014_multi_user
Create Date: 2026-03-04
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = "015_notifications"
down_revision = "014_multi_user"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "notifications",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("type", sa.String(32), nullable=False),
        sa.Column("category", sa.String(32), nullable=False),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("metadata_json", sa.Text(), nullable=True),
        sa.Column("read", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("batch_id", sa.String(64), nullable=True),
        sa.Column("source", sa.String(32), nullable=False, server_default="system"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("idx_notifications_type", "notifications", ["type"])
    op.create_index("idx_notifications_category", "notifications", ["category"])
    op.create_index("idx_notifications_read", "notifications", ["read"])
    op.create_index("idx_notifications_batch_id", "notifications", ["batch_id"])
    op.create_index("idx_notifications_created_at", "notifications", ["created_at"])


def downgrade() -> None:
    op.drop_index("idx_notifications_created_at", table_name="notifications")
    op.drop_index("idx_notifications_batch_id", table_name="notifications")
    op.drop_index("idx_notifications_read", table_name="notifications")
    op.drop_index("idx_notifications_category", table_name="notifications")
    op.drop_index("idx_notifications_type", table_name="notifications")
    op.drop_table("notifications")
