"""BQ-VZ-RAW-LISTINGS: raw_files + raw_listings tables

Revision ID: 013_raw_listings
Revises: 012_file_size_bytes_bigint
Create Date: 2026-03-03
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "013_raw_listings"
down_revision: Union[str, None] = "012_file_size_bytes_bigint"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- raw_files table ---
    op.create_table(
        "raw_files",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("filename", sa.String(512), nullable=False),
        sa.Column("file_path", sa.String(1024), nullable=False),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("mime_type", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
    )

    # --- raw_listings table ---
    op.create_table(
        "raw_listings",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "raw_file_id",
            sa.String(36),
            sa.ForeignKey("raw_files.id"),
            nullable=False,
        ),
        sa.Column("marketplace_listing_id", sa.String(36), nullable=True),
        sa.Column("title", sa.String(256), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("tags", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("auto_metadata", sa.JSON(), nullable=True),
        sa.Column("price_cents", sa.Integer(), nullable=True),
        sa.Column(
            "status",
            sa.String(32),
            nullable=False,
            server_default="draft",
        ),
        sa.Column("published_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("status IN ('draft', 'listed', 'delisted')", name="ck_raw_listings_status"),
    )
    op.create_index("idx_raw_listings_status", "raw_listings", ["status"])


def downgrade() -> None:
    op.drop_index("idx_raw_listings_status", table_name="raw_listings")
    op.drop_table("raw_listings")
    op.drop_table("raw_files")
