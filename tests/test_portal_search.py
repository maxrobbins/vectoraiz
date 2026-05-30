"""
BQ-VZ-SHARED-SEARCH: Portal search tests

Tests: results only contain display_columns, max_results enforced.
"""

import pytest
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient

from app.models.portal import save_portal_config, reset_portal_config_cache
from app.schemas.portal import PortalConfig, DatasetPortalConfig, PortalTier


@pytest.fixture(autouse=True)
def reset_portal(tmp_path, monkeypatch):
    monkeypatch.setattr("app.models.portal._PORTAL_CONFIG_PATH", tmp_path / "portal_config.json")
    monkeypatch.setattr("app.middleware.portal_auth._PORTAL_JWT_SECRET_PATH", tmp_path / "portal_jwt.key")
    reset_portal_config_cache()
    yield
    reset_portal_config_cache()


@pytest.fixture
def client():
    from app.main import app
    with TestClient(app) as c:
        yield c


def _make_search_results(columns, n=5):
    """Build fake search results with the given columns."""
    return {
        "results": [
            {
                "score": 0.9 - i * 0.05,
                "row_data": {col: f"val_{col}_{i}" for col in columns},
                "text_content": f"text content {i}",
            }
            for i in range(n)
        ],
        "total": n,
    }


@pytest.fixture
def open_portal_with_datasets():
    """Open tier portal with a dataset that has restricted columns."""
    config = PortalConfig(
        enabled=True,
        tier=PortalTier.open,
        base_url="http://localhost:8100",
        datasets={
            "ds-restricted": DatasetPortalConfig(
                portal_visible=True,
                display_columns=["name", "category"],
                search_columns=["name", "description"],
                max_results=3,
            ),
            "ds-all-columns": DatasetPortalConfig(
                portal_visible=True,
                display_columns=[],
                max_results=100,
            ),
        },
    )
    save_portal_config(config)
    return config


def test_search_results_only_contain_display_columns(client, open_portal_with_datasets):
    """Search results for restricted dataset only include display_columns."""
    fake_results = _make_search_results(["name", "category", "secret_field", "internal_id"])

    with patch("app.services.portal_service.get_search_service") as mock_search, \
         patch("app.services.portal_service.get_processing_service") as mock_proc:
        mock_search.return_value.search.return_value = fake_results
        mock_record = MagicMock()
        mock_record.original_filename = "test.csv"
        mock_proc.return_value.get_dataset.return_value = mock_record

        resp = client.post(
            "/api/portal/search",
            json={"dataset_id": "ds-restricted", "query": "test"},
        )

    assert resp.status_code == 200
    data = resp.json()
    for result in data["results"]:
        row_keys = set(result["row_data"].keys())
        assert row_keys <= {"name", "category"}, f"Unexpected columns: {row_keys}"
        assert "secret_field" not in row_keys
        assert "internal_id" not in row_keys


def test_search_all_columns_when_display_empty(client, open_portal_with_datasets):
    """When display_columns is empty, all columns are returned."""
    fake_results = _make_search_results(["col_a", "col_b", "col_c"], n=2)

    with patch("app.services.portal_service.get_search_service") as mock_search, \
         patch("app.services.portal_service.get_processing_service") as mock_proc:
        mock_search.return_value.search.return_value = fake_results
        mock_record = MagicMock()
        mock_record.original_filename = "all_cols.csv"
        mock_proc.return_value.get_dataset.return_value = mock_record

        resp = client.post(
            "/api/portal/search",
            json={"dataset_id": "ds-all-columns", "query": "test"},
        )

    assert resp.status_code == 200
    data = resp.json()
    for result in data["results"]:
        row_keys = set(result["row_data"].keys())
        assert row_keys == {"col_a", "col_b", "col_c"}


def test_max_results_caps_search_limit(client, open_portal_with_datasets):
    """Dataset max_results=3 caps the limit even if client requests more."""
    with patch("app.services.portal_service.get_search_service") as mock_search, \
         patch("app.services.portal_service.get_processing_service") as mock_proc:
        mock_search.return_value.search.return_value = _make_search_results(["name"], n=3)
        mock_record = MagicMock()
        mock_record.original_filename = "test.csv"
        mock_proc.return_value.get_dataset.return_value = mock_record

        resp = client.post(
            "/api/portal/search",
            json={"dataset_id": "ds-restricted", "query": "test", "limit": 50},
        )

    assert resp.status_code == 200
    call_kwargs = mock_search.return_value.search.call_args
    called_limit = call_kwargs.kwargs.get("limit") if call_kwargs.kwargs else call_kwargs[1].get("limit")
    assert called_limit is not None
    assert called_limit <= 3


def test_get_search_single_dataset(client, open_portal_with_datasets):
    """GET /api/portal/search/{dataset_id}?q=... works."""
    fake_results = _make_search_results(["name", "category"], n=1)

    with patch("app.services.portal_service.get_search_service") as mock_search, \
         patch("app.services.portal_service.get_processing_service") as mock_proc:
        mock_search.return_value.search.return_value = fake_results
        mock_record = MagicMock()
        mock_record.original_filename = "test.csv"
        mock_proc.return_value.get_dataset.return_value = mock_record

        resp = client.get("/api/portal/search/ds-restricted?q=hello&limit=10")

    assert resp.status_code == 200
    data = resp.json()
    assert data["query"] == "hello"
    assert len(data["results"]) == 1


def test_search_non_visible_dataset_returns_403(client, open_portal_with_datasets):
    """Searching a dataset not marked portal_visible returns 403."""
    resp = client.post(
        "/api/portal/search",
        json={"dataset_id": "nonexistent-dataset", "query": "test"},
    )
    assert resp.status_code == 403
