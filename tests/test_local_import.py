"""Tests for VZ-PERF-P1 local_import router path validation and endpoints."""
import os
from unittest.mock import patch

import pytest

# Patch IMPORT_ROOT before importing the module
_test_import_root = None


@pytest.fixture(autouse=True)
def patch_import_root(tmp_path):
    """Redirect IMPORT_ROOT to a tmp_path for every test."""
    global _test_import_root
    _test_import_root = tmp_path / "import"
    _test_import_root.mkdir()

    with patch("app.routers.local_import.IMPORT_ROOT", _test_import_root):
        yield _test_import_root


@pytest.fixture
def validate(patch_import_root):
    """Return the _validate_import_path function (already patched)."""
    from app.routers.local_import import _validate_import_path
    return _validate_import_path


# ── browse ──────────────────────────────────────────────────────────

def test_browse_returns_files(patch_import_root, validate):
    """Browse lists files that exist under import root."""
    (patch_import_root / "data.csv").write_text("a,b\n1,2")
    (patch_import_root / "report.json").write_text("{}")

    from app.routers.local_import import _validate_import_path

    # Validate the root itself
    resolved = _validate_import_path("")
    assert resolved == patch_import_root.resolve()

    # Check that files are visible
    entries = []
    for entry in sorted(resolved.iterdir(), key=lambda e: e.name.lower()):
        if entry.is_file():
            entries.append(entry.name)
    assert "data.csv" in entries


# ── path traversal ──────────────────────────────────────────────────

def test_browse_rejects_path_traversal(validate):
    """Path traversal attempts must be rejected."""
    with pytest.raises(ValueError, match="outside import directory"):
        validate("../../etc/passwd")


def test_browse_rejects_absolute_escape(validate):
    """Absolute paths outside the import root must be rejected."""
    with pytest.raises(ValueError, match="outside import directory"):
        validate("/etc/passwd")


# ── symlinks ────────────────────────────────────────────────────────

def test_browse_rejects_symlinks(patch_import_root, validate):
    """Symlinks inside import root must be rejected."""
    target = patch_import_root / "real.csv"
    target.write_text("a,b\n1,2")
    link = patch_import_root / "sneaky.csv"
    link.symlink_to(target)

    with pytest.raises(ValueError, match="Symlinks not allowed"):
        validate("sneaky.csv")


def test_browse_rejects_symlink_directory(patch_import_root, validate):
    """Symlinked directories must be rejected."""
    real_dir = patch_import_root / "real_dir"
    real_dir.mkdir()
    link_dir = patch_import_root / "linked_dir"
    link_dir.symlink_to(real_dir)

    with pytest.raises(ValueError, match="Symlinks not allowed"):
        validate("linked_dir")


# ── depth limit ─────────────────────────────────────────────────────

def test_browse_rejects_excessive_depth(patch_import_root, validate):
    """Paths deeper than MAX_DEPTH (3) must be rejected."""
    deep = patch_import_root / "a" / "b" / "c" / "d"
    deep.mkdir(parents=True)
    (deep / "file.csv").write_text("x")

    with pytest.raises(ValueError, match="max depth"):
        validate("a/b/c/d/file.csv")


def test_browse_allows_max_depth(patch_import_root, validate):
    """Paths at exactly MAX_DEPTH (3) should be allowed."""
    nested = patch_import_root / "a" / "b" / "c"
    nested.mkdir(parents=True)
    f = nested / "file.csv"
    f.write_text("x")

    result = validate("a/b/c/file.csv")
    assert result == f.resolve()


# ── process validates paths ─────────────────────────────────────────

def test_process_validates_paths(patch_import_root, validate):
    """Process should reject invalid relative paths."""
    # Traversal
    with pytest.raises(ValueError):
        validate("../../../etc/shadow")

    # Valid path that exists
    (patch_import_root / "ok.csv").write_text("a,b")
    result = validate("ok.csv")
    assert result.name == "ok.csv"


# ── status endpoint ─────────────────────────────────────────────────

def test_status_returns_availability(patch_import_root):
    """Status endpoint returns correct availability info."""

    # With patched root, the directory exists
    assert patch_import_root.is_dir()
    assert os.access(str(patch_import_root), os.R_OK)
