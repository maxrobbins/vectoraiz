"""
Alembic Environment Configuration
==================================

Runs migrations against the vectoraiz.db (BQ-111) database.
The sqlalchemy.url is overridden at runtime by app.core.database
so the value in alembic.ini is only a fallback for CLI usage.
"""

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlmodel import SQLModel

# Import all BQ-111 models so their tables are registered on metadata
from app.models.dataset import DatasetRecord  # noqa: F401
from app.models.billing import BillingUsage, BillingSubscription  # noqa: F401
from app.models.api_key import APIKey  # noqa: F401
from app.models.local_auth import LocalUser, LocalAPIKey  # noqa: F401  BQ-127
from app.models.connectivity import ConnectivityTokenRecord  # noqa: F401  BQ-MCP-RAG
from app.models.raw_file import RawFile  # noqa: F401  BQ-VZ-RAW-LISTINGS
from app.models.raw_listing import RawListing  # noqa: F401  BQ-VZ-RAW-LISTINGS
from app.services.deduction_queue import deductions_metadata

config = context.config

# Override sqlalchemy.url from DATABASE_URL env var (used in Docker deployments)
database_url = os.environ.get("DATABASE_URL")
if database_url:
    config.set_main_option("sqlalchemy.url", database_url)

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Combine SQLModel metadata (ORM models) with deductions_metadata (Core table).
# Alembic autogenerate needs all table definitions in a single MetaData to diff
# against the live database.  We mirror the Core table into SQLModel.metadata so
# that `target_metadata` stays a single MetaData instance.
for table in deductions_metadata.tables.values():
    if table.name not in SQLModel.metadata.tables:
        table.to_metadata(SQLModel.metadata)

target_metadata = SQLModel.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (emit SQL to stdout)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        version_num_width=128,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (connect to DB)."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, version_num_width=128)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
