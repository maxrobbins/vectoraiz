"""file_size_bytes Integer → BigInteger for files >2GB

Revision ID: 012_file_size_bytes_bigint
Revises: 011_database_connections
Create Date: 2026-03-02

The original migration (001) created dataset_records.file_size_bytes as
Integer (max ~2.1GB).  Commit 300bd00 updated the SQLAlchemy model to
BigInteger, but a fresh install still creates the column as Integer,
causing NumericValueOutOfRange for files >2GB.

fulfillment_log.file_size_bytes was already created as BigInteger in
migration 010; this migration only touches dataset_records.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "012_file_size_bytes_bigint"
down_revision: Union[str, None] = "011_database_connections"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "dataset_records",
        "file_size_bytes",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=False,
        existing_server_default="0",
    )


def downgrade() -> None:
    op.alter_column(
        "dataset_records",
        "file_size_bytes",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=False,
        existing_server_default="0",
    )
