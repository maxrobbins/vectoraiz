"""BQ-VZ-MULTI-USER: Create users table for multi-user JWT auth

Revision ID: 014_multi_user
Revises: 013_raw_listings
Create Date: 2026-03-03
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = "014_multi_user"
down_revision = "013_raw_listings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("username", sa.String(64), nullable=False, unique=True),
        sa.Column("display_name", sa.String(128), nullable=True),
        sa.Column("pw_hash", sa.String(255), nullable=False),
        sa.Column(
            "role",
            sa.String(16),
            nullable=False,
            server_default="user",
        ),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("last_login_at", sa.DateTime(), nullable=True),
    )
    op.create_index("idx_users_username", "users", ["username"])


def downgrade() -> None:
    op.drop_index("idx_users_username", table_name="users")
    op.drop_table("users")
