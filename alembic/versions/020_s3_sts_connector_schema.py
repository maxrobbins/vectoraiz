"""S3 STS connector schema.

Revision ID: 020_s3_sts_connector_schema
Revises: 019_aim_user_link
Create Date: 2026-05-26
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "020_s3_sts_connector_schema"
down_revision: Union[str, None] = "019_aim_user_link"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "s3_connection",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("bucket", sa.String(255), nullable=False),
        sa.Column("region", sa.String(64), nullable=False),
        sa.Column("role_arn", sa.String(512), nullable=True),
        sa.Column("external_id", sa.String(128), nullable=True),
        sa.Column("prefix", sa.String(512), nullable=True),
        sa.Column("status", sa.String(32), nullable=False, server_default="configured"),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("last_scanned_at", sa.DateTime, nullable=True),
        sa.Column("continuation_token", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
        sa.CheckConstraint(
            "(status != 'configured') OR (role_arn IS NOT NULL AND external_id IS NOT NULL)",
            name="ck_s3_connection_configured_creds_required",
        ),
    )
    op.create_table(
        "s3_scan_job",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("connection_id", sa.String(36), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("started_at", sa.DateTime, nullable=False),
        sa.Column("completed_at", sa.DateTime, nullable=True),
        sa.Column("continuation_token", sa.Text, nullable=True),
        sa.Column("error_message", sa.Text, nullable=True),
        sa.Column("objects_enumerated", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
        sa.ForeignKeyConstraint(["connection_id"], ["s3_connection.id"], ondelete="CASCADE"),
    )
    op.create_table(
        "s3_object_metadata",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("connection_id", sa.String(36), nullable=False),
        sa.Column("scan_job_id", sa.String(36), nullable=False),
        sa.Column("object_key", sa.String(1024), nullable=False),
        sa.Column("size_bytes", sa.Integer, nullable=False),
        sa.Column("content_type", sa.String(128), nullable=False),
        sa.Column("last_modified", sa.DateTime, nullable=False),
        sa.Column("etag", sa.String(128), nullable=False),
        sa.Column("dataset_id", sa.String(36), nullable=True),
        sa.Column("metadata_extracted_at", sa.DateTime, nullable=True),
        sa.Column("extraction_status", sa.String(32), nullable=True),
        sa.Column("extraction_skip_reason", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("updated_at", sa.DateTime, nullable=False),
        sa.ForeignKeyConstraint(["connection_id"], ["s3_connection.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["scan_job_id"], ["s3_scan_job.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dataset_id"], ["dataset_records.id"], ondelete="SET NULL"),
    )
    op.create_index(
        "idx_s3_object_metadata_connection_object_key",
        "s3_object_metadata",
        ["connection_id", "object_key"],
    )
    op.create_index(
        "idx_s3_object_metadata_connection_scan_job",
        "s3_object_metadata",
        ["connection_id", "scan_job_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_s3_object_metadata_connection_scan_job", table_name="s3_object_metadata")
    op.drop_index("idx_s3_object_metadata_connection_object_key", table_name="s3_object_metadata")
    op.drop_table("s3_object_metadata")
    op.drop_table("s3_scan_job")
    op.drop_table("s3_connection")
