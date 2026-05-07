"""
ArtifactsService — allAI Output Files (Artifacts)

Manages creation, storage, retrieval, and lifecycle of artifacts
(tangible output files) created by allAI for the user.

Storage: filesystem under {data_dir}/artifacts/{artifact_id}/
  - metadata.json (schema_version: 1)
  - content.{ext} (ext from format enum, NEVER from user input)

Security:
  - Filename is display-only, NEVER used in filesystem paths
  - User-scoping enforced on every operation
  - Atomic writes (temp dir -> rename)
  - Content validation (UTF-8, no NUL, HTML sanitized)
  - Quotas: 50MB/file, 100/user, 5 creates/min rate limit

PHASE: BQ-VZ-ARTIFACTS Phase 1
CREATED: 2026-03-06
"""

import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
import time
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from app.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_ARTIFACT_SIZE_BYTES = 50 * 1024 * 1024  # 50 MB
MAX_ARTIFACTS_PER_USER = 100
MAX_CREATES_PER_MIN = 5
MAX_FILENAME_LENGTH = 255
ARTIFACT_TTL_DAYS = 7
SCHEMA_VERSION = 1
EXPORT_DIR = Path("/data/export")

FILENAME_CHARSET = re.compile(r'^[a-zA-Z0-9._-]+$')
SCRIPT_TAG_RE = re.compile(r'<script\b[^>]*>.*?</script>', re.IGNORECASE | re.DOTALL)
EVENT_HANDLER_RE = re.compile(r'\s+on\w+\s*=', re.IGNORECASE)


class ArtifactFormat(str, Enum):
    TXT = "txt"
    CSV = "csv"
    XLSX = "xlsx"
    JSON = "json"
    MD = "md"
    HTML = "html"
    PARQUET = "parquet"


MIME_TYPES = {
    ArtifactFormat.TXT: "text/plain",
    ArtifactFormat.CSV: "text/csv",
    ArtifactFormat.XLSX: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ArtifactFormat.JSON: "application/json",
    ArtifactFormat.MD: "text/markdown",
    ArtifactFormat.HTML: "text/html",
    ArtifactFormat.PARQUET: "application/vnd.apache.parquet",
}

QUERY_EXPORT_FORMATS = {
    ArtifactFormat.CSV,
    ArtifactFormat.XLSX,
    ArtifactFormat.JSON,
    ArtifactFormat.PARQUET,
}


# ---------------------------------------------------------------------------
# Artifact Model (JSON sidecar)
# ---------------------------------------------------------------------------

@dataclass
class Artifact:
    id: str
    schema_version: int
    filename: str
    format: str
    size_bytes: int
    content_hash: str
    created_at: str
    source: str
    source_ref: Optional[str]
    description: Optional[str]
    dataset_refs: List[str]
    user_id: str
    starred: bool
    expired: bool

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Artifact":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ---------------------------------------------------------------------------
# ArtifactsService
# ---------------------------------------------------------------------------

class ArtifactsService:
    def __init__(self):
        self._artifacts_dir = Path(settings.data_directory) / "artifacts"
        self._artifacts_dir.mkdir(parents=True, exist_ok=True)
        self._rate_limits: Dict[str, List[float]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_filename(filename: str) -> str:
        """Validate and sanitize display filename. NEVER used in paths."""
        if not filename or not filename.strip():
            raise ValueError("Filename cannot be empty")
        filename = os.path.basename(filename.strip())
        if len(filename) > MAX_FILENAME_LENGTH:
            raise ValueError(f"Filename exceeds {MAX_FILENAME_LENGTH} characters")
        if not FILENAME_CHARSET.match(filename):
            raise ValueError("Filename contains invalid characters (allowed: a-z A-Z 0-9 . _ -)")
        if ".." in filename:
            raise ValueError("Filename cannot contain '..'")
        if filename.startswith("."):
            raise ValueError("Filename cannot start with '.'")
        return filename

    @staticmethod
    def _validate_content(content: str, fmt: ArtifactFormat) -> str:
        """Validate content: UTF-8, no NUL, format-specific checks."""
        if not isinstance(content, str):
            raise ValueError("Content must be a string")
        if "\x00" in content:
            raise ValueError("Content contains NUL bytes")
        content_bytes = content.encode("utf-8")
        if len(content_bytes) > MAX_ARTIFACT_SIZE_BYTES:
            raise ValueError(f"Content exceeds {MAX_ARTIFACT_SIZE_BYTES // (1024*1024)}MB limit")
        if fmt == ArtifactFormat.HTML:
            content = SCRIPT_TAG_RE.sub("", content)
            content = EVENT_HANDLER_RE.sub(" ", content)
        if fmt == ArtifactFormat.JSON:
            try:
                json.loads(content)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON content: {e}")
        return content

    def _check_rate_limit(self, user_id: str) -> None:
        """Enforce 5 creates/min rate limit."""
        now = time.time()
        window = [t for t in self._rate_limits[user_id] if now - t < 60]
        self._rate_limits[user_id] = window
        if len(window) >= MAX_CREATES_PER_MIN:
            raise PermissionError("Rate limit exceeded: max 5 artifact creates per minute")

    def _record_create(self, user_id: str) -> None:
        self._rate_limits[user_id].append(time.time())

    def _check_quota(self, user_id: str) -> None:
        """Enforce max artifacts per user."""
        count = sum(
            1 for a in self._iter_artifacts(user_id)
            if not a.expired
        )
        if count >= MAX_ARTIFACTS_PER_USER:
            raise PermissionError(f"Artifact quota exceeded: max {MAX_ARTIFACTS_PER_USER} per user")

    def _iter_artifacts(self, user_id: str) -> List[Artifact]:
        """Iterate over all artifacts for a user by scanning the artifacts directory."""
        artifacts = []
        if not self._artifacts_dir.exists():
            return artifacts
        for entry in self._artifacts_dir.iterdir():
            if not entry.is_dir():
                continue
            meta_path = entry / "metadata.json"
            if not meta_path.exists():
                continue
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                artifact = Artifact.from_dict(data)
                if artifact.user_id == user_id:
                    artifacts.append(artifact)
            except Exception:
                logger.warning("Skipping corrupt artifact metadata: %s", meta_path)
        return artifacts

    def _get_artifact_dir(self, artifact_id: str) -> Path:
        """Get the directory for an artifact. Validates UUID format."""
        # Prevent path traversal
        safe_id = os.path.basename(artifact_id)
        if safe_id != artifact_id:
            raise ValueError("Invalid artifact ID")
        return self._artifacts_dir / safe_id

    def _load_artifact(self, artifact_id: str, user_id: str) -> Artifact:
        """Load an artifact, enforcing user scoping."""
        artifact_dir = self._get_artifact_dir(artifact_id)
        meta_path = artifact_dir / "metadata.json"
        if not meta_path.exists():
            raise FileNotFoundError(f"Artifact '{artifact_id}' not found")
        with open(meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        artifact = Artifact.from_dict(data)
        if artifact.user_id != user_id:
            raise FileNotFoundError(f"Artifact '{artifact_id}' not found")
        return artifact

    def _content_path(self, artifact_dir: Path, fmt: ArtifactFormat) -> Path:
        """Get content file path using format enum extension."""
        return artifact_dir / f"content.{fmt.value}"

    @classmethod
    def _canonicalize_export_destination(cls, filename: str, fmt: str) -> Path:
        """Return a real export path under /data/export for a sanitized filename."""
        export_root = EXPORT_DIR
        if not os.environ.get("HOST_EXPORT_DIR"):
            raise ValueError(
                "Export directory is not configured: HOST_EXPORT_DIR is unset"
            )
        if not export_root.exists():
            raise FileNotFoundError(
                "Export directory is not mounted: /data/export is missing"
            )
        if not export_root.is_dir():
            raise ValueError("Export directory is invalid: /data/export is not a directory")
        if not os.access(export_root, os.W_OK):
            raise PermissionError(
                "Export directory is read-only: /data/export is not writable"
            )

        raw_filename = filename.strip() if filename else ""
        if os.path.basename(raw_filename) != raw_filename:
            raise ValueError("Export filename cannot contain path separators")
        safe_filename = cls._validate_filename(raw_filename)
        expected_suffix = f".{fmt}"
        if Path(safe_filename).suffix.lower() != expected_suffix:
            stem = safe_filename.rsplit(".", 1)[0] if "." in safe_filename else safe_filename
            safe_filename = f"{stem}{expected_suffix}"

        root_real = export_root.resolve(strict=True)
        dest = export_root / safe_filename
        dest_parent_real = dest.parent.resolve(strict=True)
        if dest_parent_real != root_real:
            raise ValueError("Export destination escapes /data/export")

        dest_real = dest.resolve(strict=False)
        if root_real != dest_real and root_real not in dest_real.parents:
            raise ValueError("Export destination escapes /data/export")
        if dest.exists() and dest.is_symlink():
            raise ValueError("Export destination cannot be a symlink")
        return dest_real

    @staticmethod
    def _write_export(records: Iterable[dict], fmt: ArtifactFormat, dest: Path) -> None:
        """Write query records in the requested export format."""
        rows = list(records)
        columns = list(rows[0].keys()) if rows else []

        if fmt == ArtifactFormat.CSV:
            import csv
            with open(dest, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
            return

        if fmt == ArtifactFormat.XLSX:
            from openpyxl import Workbook
            wb = Workbook()
            ws = wb.active
            ws.append(columns)
            for row in rows:
                ws.append([row.get(column) for column in columns])
            wb.save(dest)
            return

        if fmt == ArtifactFormat.JSON:
            with open(dest, "w", encoding="utf-8") as f:
                json.dump(rows, f, ensure_ascii=False, default=str)
            return

        if fmt == ArtifactFormat.PARQUET:
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = pa.Table.from_pylist(rows)
            pq.write_table(table, dest)
            return

        raise ValueError(f"Unsupported export format: {fmt.value}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_artifact(
        self,
        filename: str,
        content: str,
        format: str,
        description: Optional[str],
        dataset_refs: Optional[List[str]],
        user_id: str,
        source: str = "allai-copilot",
        source_ref: Optional[str] = None,
    ) -> Artifact:
        """Create an artifact from content string. Atomic write."""
        if not user_id:
            raise ValueError("user_id is required (NEVER null)")

        fmt = ArtifactFormat(format)
        filename = self._validate_filename(filename)
        content = self._validate_content(content, fmt)
        self._check_rate_limit(user_id)
        self._check_quota(user_id)

        artifact_id = str(uuid.uuid4())
        temp_dir = Path(tempfile.mkdtemp(
            dir=self._artifacts_dir, prefix=f".tmp_{artifact_id}_"
        ))

        try:
            # Write content
            content_file = temp_dir / f"content.{fmt.value}"
            content_file.write_text(content, encoding="utf-8")

            # Measure actual size and hash
            content_bytes = content_file.read_bytes()
            size_bytes = len(content_bytes)
            content_hash = hashlib.sha256(content_bytes).hexdigest()

            # Build metadata
            artifact = Artifact(
                id=artifact_id,
                schema_version=SCHEMA_VERSION,
                filename=filename,
                format=fmt.value,
                size_bytes=size_bytes,
                content_hash=content_hash,
                created_at=datetime.now(timezone.utc).isoformat(),
                source=source,
                source_ref=source_ref,
                description=description,
                dataset_refs=dataset_refs or [],
                user_id=user_id,
                starred=False,
                expired=False,
            )

            # Write metadata
            meta_path = temp_dir / "metadata.json"
            meta_path.write_text(
                json.dumps(artifact.to_dict(), indent=2),
                encoding="utf-8",
            )

            # Atomic rename
            final_dir = self._get_artifact_dir(artifact_id)
            os.rename(str(temp_dir), str(final_dir))

            self._record_create(user_id)
            logger.info("Artifact created: id=%s user=%s file=%s", artifact_id, user_id, filename)
            return artifact

        except Exception:
            # Cleanup temp dir on failure
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
            raise

    def create_artifact_from_query(
        self,
        filename: str,
        query: str,
        description: Optional[str],
        user_id: str,
        source_ref: Optional[str] = None,
        fmt: str = "csv",
    ) -> Artifact:
        """Create an artifact by executing a SQL query and writing results to an export file."""
        if not user_id:
            raise ValueError("user_id is required (NEVER null)")
        if settings.mode != "standalone":
            raise PermissionError("Query file exports are only available in standalone mode")

        from app.services.sql_service import get_sql_service

        fmt = ArtifactFormat(fmt)
        if fmt not in QUERY_EXPORT_FORMATS:
            raise ValueError("Unsupported query export format")
        export_dest = self._canonicalize_export_destination(filename, fmt.value)
        filename = export_dest.name
        self._check_rate_limit(user_id)
        self._check_quota(user_id)

        sql_svc = get_sql_service()

        # Validate SQL
        is_valid, error = sql_svc.validate_query(query)
        if not is_valid:
            raise ValueError(f"SQL validation failed: {error}")

        artifact_id = str(uuid.uuid4())
        temp_dir = Path(tempfile.mkdtemp(
            dir=self._artifacts_dir, prefix=f".tmp_{artifact_id}_"
        ))

        try:
            content_file = temp_dir / f"content.{fmt.value}"

            # Execute query and stream to CSV
            from app.services.duckdb_service import ephemeral_duckdb_service
            from app.services.sql_sandbox import SQLSandbox
            from app.services.processing_service import get_processing_service

            proc_svc = get_processing_service()
            records = proc_svc.list_datasets()
            allowed = SQLSandbox.build_allowed_tables([r.id for r in records])
            sandbox = SQLSandbox(allowed_tables=allowed)
            is_valid, error = sandbox.validate(query)
            if not is_valid:
                raise ValueError(f"SQL validation failed: {error}")

            with ephemeral_duckdb_service() as duckdb_svc:
                conn = duckdb_svc.create_ephemeral_connection()
                try:
                    # Create views
                    from app.services.sql_service import SQLService
                    datasets = sql_svc._resolve_datasets(None)
                    SQLService._create_views(conn, datasets)

                    result = conn.execute(query)
                    columns = [desc[0] for desc in result.description]
                    records = []
                    while True:
                        batch = result.fetchmany(1000)
                        if not batch:
                            break
                        records.extend(dict(zip(columns, row)) for row in batch)

                    self._write_export(records, fmt, export_dest)
                    if export_dest.stat().st_size > MAX_ARTIFACT_SIZE_BYTES:
                        raise ValueError(
                            f"Query result exceeds {MAX_ARTIFACT_SIZE_BYTES // (1024*1024)}MB limit"
                        )
                    shutil.copyfile(export_dest, content_file)
                finally:
                    conn.close()

            # Measure actual size and hash
            content_bytes = content_file.read_bytes()
            size_bytes = len(content_bytes)
            if size_bytes == 0:
                raise ValueError("Query returned no results")
            content_hash = hashlib.sha256(content_bytes).hexdigest()

            # Build metadata
            artifact = Artifact(
                id=artifact_id,
                schema_version=SCHEMA_VERSION,
                filename=filename,
                format=fmt.value,
                size_bytes=size_bytes,
                content_hash=content_hash,
                created_at=datetime.now(timezone.utc).isoformat(),
                source="allai-query",
                source_ref=source_ref,
                description=description,
                dataset_refs=[],
                user_id=user_id,
                starred=False,
                expired=False,
            )

            meta_path = temp_dir / "metadata.json"
            meta_path.write_text(
                json.dumps(artifact.to_dict(), indent=2),
                encoding="utf-8",
            )

            final_dir = self._get_artifact_dir(artifact_id)
            os.rename(str(temp_dir), str(final_dir))

            self._record_create(user_id)
            logger.info(
                "Artifact from query created: id=%s user=%s file=%s size=%d",
                artifact_id, user_id, filename, size_bytes,
            )
            return artifact

        except Exception:
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
            raise

    def list_artifacts(
        self,
        user_id: str,
        include_expired: bool = False,
        offset: int = 0,
        limit: int = 50,
    ) -> List[Artifact]:
        """List artifacts for a user, sorted by created_at desc."""
        artifacts = self._iter_artifacts(user_id)
        if not include_expired:
            artifacts = [a for a in artifacts if not a.expired]
        artifacts.sort(key=lambda a: a.created_at, reverse=True)
        return artifacts[offset:offset + limit]

    def get_artifact(self, artifact_id: str, user_id: str) -> Artifact:
        """Get artifact metadata, enforcing user scoping."""
        return self._load_artifact(artifact_id, user_id)

    def download_artifact(self, artifact_id: str, user_id: str) -> tuple:
        """Get artifact content for download. Returns (path, filename, mime_type)."""
        artifact = self._load_artifact(artifact_id, user_id)
        fmt = ArtifactFormat(artifact.format)
        artifact_dir = self._get_artifact_dir(artifact_id)
        content_path = self._content_path(artifact_dir, fmt)
        if not content_path.exists():
            raise FileNotFoundError(f"Artifact content file missing: {artifact_id}")
        mime_type = MIME_TYPES.get(fmt, "application/octet-stream")
        return content_path, artifact.filename, mime_type

    def delete_artifact(self, artifact_id: str, user_id: str) -> bool:
        """Delete an artifact and its files."""
        self._load_artifact(artifact_id, user_id)  # Validates ownership
        artifact_dir = self._get_artifact_dir(artifact_id)
        shutil.rmtree(artifact_dir, ignore_errors=True)
        logger.info("Artifact deleted: id=%s user=%s", artifact_id, user_id)
        return True

    def star_artifact(self, artifact_id: str, user_id: str, starred: bool) -> Artifact:
        """Toggle star status on an artifact."""
        artifact = self._load_artifact(artifact_id, user_id)
        artifact.starred = starred
        artifact_dir = self._get_artifact_dir(artifact_id)
        meta_path = artifact_dir / "metadata.json"
        meta_path.write_text(
            json.dumps(artifact.to_dict(), indent=2),
            encoding="utf-8",
        )
        return artifact

    def cleanup_expired(self) -> int:
        """Remove unstarred artifacts older than TTL. Returns count removed."""
        now = datetime.now(timezone.utc)
        removed = 0
        if not self._artifacts_dir.exists():
            return 0
        for entry in self._artifacts_dir.iterdir():
            if not entry.is_dir() or entry.name.startswith("."):
                continue
            meta_path = entry / "metadata.json"
            if not meta_path.exists():
                continue
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                artifact = Artifact.from_dict(data)
                if artifact.starred:
                    continue
                created = datetime.fromisoformat(artifact.created_at)
                if created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                age_days = (now - created).total_seconds() / 86400
                if age_days > ARTIFACT_TTL_DAYS:
                    shutil.rmtree(entry, ignore_errors=True)
                    removed += 1
                    logger.info("Expired artifact cleaned up: %s (age=%.1fd)", artifact.id, age_days)
            except Exception:
                logger.warning("Error checking artifact expiry: %s", entry.name)
        return removed


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_artifacts_service: Optional[ArtifactsService] = None


def get_artifacts_service() -> ArtifactsService:
    global _artifacts_service
    if _artifacts_service is None:
        _artifacts_service = ArtifactsService()
    return _artifacts_service
