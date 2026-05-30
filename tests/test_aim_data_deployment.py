"""
Deployment reconciliation tests for the aim-data channel.
"""

from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from app.core.channel_config import ChannelType
from app.main import app

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = REPO_ROOT / "docker-compose.aim-data.yml"
FORBIDDEN_KEYS = {"AIM_DATA_MODE", "AIM_DATA_MARKETPLACE_ENABLED"}
REQUIRED_ENV = {
    "VECTORAIZ_CHANNEL=aim-data",
    "VECTORAIZ_VERSION=${VECTORAIZ_VERSION:-latest}",
    "VECTORAIZ_MODE=${VECTORAIZ_MODE:-standalone}",
    "VECTORAIZ_SECRET_KEY=${VECTORAIZ_SECRET_KEY:-}",
}


def test_aim_data_compose_yaml_is_valid():
    """docker-compose.aim-data.yml parses as valid YAML."""
    data = yaml.safe_load(COMPOSE_FILE.read_text())

    assert isinstance(data, dict)
    assert "services" in data
    assert "volumes" in data


def test_aim_data_compose_uses_standard_vectoraiz_image_and_env():
    """AIM Data deploys the shared vectoraiz image with channel config."""
    data = yaml.safe_load(COMPOSE_FILE.read_text())

    assert "vectoraiz" in data["services"]
    assert "aim-data" not in data["services"]

    service = data["services"]["vectoraiz"]
    env = set(service["environment"])

    assert service["image"] == "ghcr.io/aidotmarket/vectoraiz:${VECTORAIZ_VERSION:-latest}"
    assert REQUIRED_ENV.issubset(env)
    assert all(not item.startswith("AIM_DATA_VERSION=") for item in env)
    assert all(not any(item.startswith(f"{key}=") for key in FORBIDDEN_KEYS) for item in env)
    assert service["ports"] == ["${AIM_DATA_PORT:-8080}:80"]
    assert service["volumes"] == [
        "aim-data-data:/data",
        "${HOST_IMPORT_DIR:-./import}:/data/import:ro",
        "/var/run/docker.sock:/var/run/docker.sock",
    ]
    assert set(service["depends_on"]) == {"postgres", "qdrant"}


def test_no_runtime_aim_data_mode_or_marketplace_enabled_refs_remain():
    """Legacy AIM_DATA_* gating vars are removed from non-spec runtime files."""
    violations = []
    skip_roots = {".git", ".venv", ".pytest_cache", "__pycache__", "specs"}
    skip_files = {Path(__file__).name}

    for path in REPO_ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in skip_roots for part in path.parts):
            continue
        if path.name in skip_files:
            continue
        if path.suffix in {".pyc", ".png", ".jpg", ".jpeg", ".gif", ".pdf"}:
            continue

        content = path.read_text(errors="ignore")
        if any(key in content for key in FORBIDDEN_KEYS):
            violations.append(str(path.relative_to(REPO_ROOT)))

    assert not violations, f"Found forbidden AIM_DATA_* refs in: {violations}"


def test_system_info_reports_aim_data_channel(monkeypatch):
    """GET /api/system/info returns aim-data when VECTORAIZ_CHANNEL is set accordingly."""
    import app.core.channel_config as channel_config

    monkeypatch.setattr(channel_config, "CHANNEL", ChannelType.aim_data)

    client = TestClient(app)
    response = client.get("/api/system/info")

    assert response.status_code == 200
    assert response.json()["channel"] == "aim-data"
