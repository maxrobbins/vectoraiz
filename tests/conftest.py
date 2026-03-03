"""
Pytest configuration for vectorAIz tests.
Sets environment variables to disable auth during testing.
"""

import os
import tempfile

# Disable auth for all tests - must be set before any imports
# Auth disable now requires debug=True AND ENVIRONMENT=development
os.environ["VECTORAIZ_AUTH_ENABLED"] = "false"
os.environ["VECTORAIZ_DEBUG"] = "true"
os.environ["ENVIRONMENT"] = "development"

# Set temp data directories so tests don't need /data (read-only on macOS)
_test_data_dir = tempfile.mkdtemp(prefix="vectoraiz_test_")
os.environ.setdefault("VECTORAIZ_DATA_DIRECTORY", _test_data_dir)
os.environ.setdefault("VECTORAIZ_UPLOAD_DIRECTORY", os.path.join(_test_data_dir, "uploads"))
os.environ.setdefault("VECTORAIZ_PROCESSED_DIRECTORY", os.path.join(_test_data_dir, "processed"))
os.environ.setdefault("DATABASE_URL", f"sqlite:///{_test_data_dir}/test.db")

import pytest

# Ensure DB tables exist for all tests (create via SQLModel metadata)
from sqlmodel import SQLModel
from app.core.database import get_engine

# Import all models so their tables are registered on SQLModel.metadata
from app.models.dataset import DatasetRecord  # noqa: F401
from app.models.billing import BillingUsage, BillingSubscription  # noqa: F401
from app.models.api_key import APIKey  # noqa: F401
from app.models.local_auth import LocalUser, LocalAPIKey  # noqa: F401  BQ-127
from app.models.connectivity import ConnectivityTokenRecord  # noqa: F401  BQ-MCP-RAG
from app.models.state import Session, Message, UserPreferences  # noqa: F401  BQ-128
from app.models.fulfillment import FulfillmentLog  # noqa: F401  BQ-D1
from app.models.database_connection import DatabaseConnection  # noqa: F401  BQ-VZ-DB-CONNECT
from app.models.raw_file import RawFile  # noqa: F401  BQ-VZ-RAW-LISTINGS
from app.models.raw_listing import RawListing  # noqa: F401  BQ-VZ-RAW-LISTINGS

SQLModel.metadata.create_all(get_engine())

# Also create tables on the legacy engine (sessions, messages, user_preferences)
from app.core.database import get_legacy_engine
SQLModel.metadata.create_all(get_legacy_engine())

# Load error registry so VectorAIzError returns correct HTTP status codes
from app.core.errors.registry import error_registry
error_registry.load()


@pytest.fixture
def auth_headers():
    """Provide auth headers for tests that need them explicitly."""
    return {"X-API-Key": "test_api_key_for_testing"}
