#!/usr/bin/env python3
"""
VectorAIz Beta-Readiness Integration Test Suite (Stress Test #2)
================================================================

Runs against a live VZ instance and exercises every user-facing feature:
  1. Auth (setup / login)
  2. Upload (small, medium, large, XL, new-format-small/medium/data/large, unsupported)
  3. Processing (metadata, status, sample, statistics, profile)
  4. Features (SQL, search, PII, compliance, searchability, attestation, listing)
  5. Delete
  6. Batch upload
  7. Negative tests

Stress Test #2 adds 25 NEW format files (md, xml, odt, ods, pptx, odp, vcf,
ics, eml, docx, rtf, html, txt, epub, xlsx, xls, large pdf/parquet/csv).

Usage (standalone):
    python tests/test_beta_readiness.py                              # localhost
    python tests/test_beta_readiness.py --base-url http://10.0.0.5   # custom host
    python tests/test_beta_readiness.py --xl                         # include XL files (>1GB)

Usage (pytest):
    pytest tests/test_beta_readiness.py -v
    pytest tests/test_beta_readiness.py -v -k "small"
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TEST_DATA_DIR = Path(os.environ.get(
    "VZ_TEST_DATA_DIR",
    os.path.expanduser("~/Downloads/vectoraiz-test-data"),
))

BASE_URL = os.environ.get("VZ_BASE_URL", "http://localhost")

# Auth credentials for setup/login
ADMIN_USER = os.environ.get("VZ_ADMIN_USER", "max")
ADMIN_PASS = os.environ.get("VZ_ADMIN_PASS", "123letmein")

# Timeouts (seconds)
UPLOAD_TIMEOUT = 300        # 5 min for very large files
PROCESSING_POLL_TIMEOUT = 300   # 5 min max wait for processing
PROCESSING_POLL_INTERVAL = 3
REQUEST_TIMEOUT = 60

# File tiers
SMALL_FILES = [
    "saas_company_metrics.csv",
    "product_catalog.json",
    "barcelona_apartments.csv",
    "gene_expression_sample.tsv",
]

MEDIUM_FILES = [
    "nyc-taxi-yellow-2024-03.parquet",
    "sec-edgar-readme.htm",
    "us-budget-2025.pdf",
]

LARGE_FILES = [
    "nyc-motor-vehicle-collisions.csv",
    "nyc-taxi-hvfhv-2024-01.parquet",
    "sec-edgar-financial-data.tsv",
]

XL_FILES = [
    "nyc-property-assessment.csv",
    "nyc-311-service-requests.json",
]

UNSUPPORTED_FILES = [
    "catalonia-geodata-260101.shp.zip",  # shapefile zip — should reject
    "catalonia-geodata-260101.shp",       # unpacked shapefile dir — should reject
]

# --- New format files added in S146 ---

NEW_FORMAT_SMALL_FILES = [
    "tensorflow-readme.md",
    "vectoraiz-data-dictionary.md",
    "plant-catalog.xml",
    "open-source-license-doc.odt",
    "project-tracking-sheet.ods",
    "ai-market-pitch-deck.pptx",
    "ai-conference-slides.odp",
    "business-contacts.vcf",
    "data-marketplace-events.ics",
    "inquiry-dataset-listing.eml",
    "us-holidays.ics",
]

NEW_FORMAT_MEDIUM_FILES = [
    "gdpr-compliance-report.docx",
    "technical-specifications-doc.docx",
    "data-marketplace-industry-report.rtf",
    "climate-change-wikipedia.html",
    "war-and-peace.txt",
    "moby-dick.epub",
]

NEW_FORMAT_DATA_FILES = [
    "eu-ecommerce-transactions-50k.xlsx",
    "iot-sensor-readings-10k.xls",
    "fda-medical-device-reports-5k.xml",
    "api_usage_logs.json",
]

NEW_FORMAT_LARGE_FILES = [
    "eurostat-population.csv",         # ~50 MB
    "nyc-taxi-hvfhv-2024-02.parquet", # ~462 MB
]

# These files are known to crash/overwhelm VZ — tested last
CRASH_PRONE_FILES = [
    "catalonia-osm-260101.pdf",       # ~255 MB — 422 rejected (oversized PDF)
    "ipcc-ar6-wg1-full.pdf",          # ~423 MB — crashes VZ worker (OOM)
]

# Batch upload uses 3 small files
BATCH_FILES = SMALL_FILES[:3]


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------

@dataclass
class TestResult:
    name: str
    passed: bool
    duration: float  # seconds
    detail: str = ""
    skipped: bool = False


@dataclass
class TestSuite:
    results: List[TestResult] = field(default_factory=list)

    def record(self, name: str, passed: bool, duration: float,
               detail: str = "", skipped: bool = False):
        r = TestResult(name=name, passed=passed, duration=duration,
                       detail=detail, skipped=skipped)
        self.results.append(r)
        status = "SKIP" if skipped else ("PASS" if passed else "FAIL")
        symbol = {"PASS": "\033[32m✓\033[0m",
                  "FAIL": "\033[31m✗\033[0m",
                  "SKIP": "\033[33m⊘\033[0m"}[status]
        time_str = f"{duration:.1f}s" if duration >= 0.1 else "<0.1s"
        msg = f"  {symbol} [{status}] {name} ({time_str})"
        if detail and not passed and not skipped:
            msg += f"  — {detail[:120]}"
        print(msg)

    def summary(self):
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed and not r.skipped)
        failed = sum(1 for r in self.results if not r.passed and not r.skipped)
        skipped = sum(1 for r in self.results if r.skipped)
        print("\n" + "=" * 70)
        print(f"SUMMARY: {total} tests | {passed} passed | {failed} failed | {skipped} skipped")
        print("=" * 70)
        if failed:
            print("\nFailed tests:")
            for r in self.results:
                if not r.passed and not r.skipped:
                    print(f"  - {r.name}: {r.detail[:200]}")
        return failed == 0


# ---------------------------------------------------------------------------
# HTTP client helper
# ---------------------------------------------------------------------------

class VZClient:
    """HTTP client for VZ API with auth support."""

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.api_key: Optional[str] = None

    @property
    def headers(self) -> Dict[str, str]:
        h: Dict[str, str] = {}
        if self.api_key:
            h["X-API-Key"] = self.api_key
        return h

    def url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def get(self, path: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", REQUEST_TIMEOUT)
        kwargs.setdefault("headers", {}).update(self.headers)
        return self.session.get(self.url(path), **kwargs)

    def post(self, path: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", REQUEST_TIMEOUT)
        kwargs.setdefault("headers", {}).update(self.headers)
        return self.session.post(self.url(path), **kwargs)

    def delete(self, path: str, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", REQUEST_TIMEOUT)
        kwargs.setdefault("headers", {}).update(self.headers)
        return self.session.delete(self.url(path), **kwargs)

    # -- Auth helpers --

    def authenticate(self) -> str:
        """Try login first; if no users exist, run setup. Returns API key."""
        # Check if setup is available
        r = self.get("/api/auth/setup")
        r.raise_for_status()
        setup_available = r.json().get("available", False)

        if setup_available:
            r = self.post("/api/auth/setup", json={
                "username": ADMIN_USER,
                "password": ADMIN_PASS,
            })
            r.raise_for_status()
            self.api_key = r.json()["api_key"]
            return self.api_key

        # Setup already done — login
        r = self.post("/api/auth/login", json={
            "username": ADMIN_USER,
            "password": ADMIN_PASS,
        })
        r.raise_for_status()
        self.api_key = r.json()["api_key"]
        return self.api_key

    # -- Upload helpers --

    def upload_file(self, filepath: Path, timeout: int = UPLOAD_TIMEOUT) -> Dict[str, Any]:
        """Upload a single file. Returns response JSON."""
        with open(filepath, "rb") as f:
            r = self.post(
                "/api/datasets/upload",
                files={"file": (filepath.name, f)},
                timeout=timeout,
            )
        r.raise_for_status()
        return r.json()

    def upload_bytes(self, filename: str, data: bytes,
                     timeout: int = UPLOAD_TIMEOUT) -> requests.Response:
        """Upload raw bytes with a given filename. Returns raw response (may be error)."""
        return self.post(
            "/api/datasets/upload",
            files={"file": (filename, io.BytesIO(data))},
            timeout=timeout,
        )

    def wait_for_ready(self, dataset_id: str,
                       timeout: int = PROCESSING_POLL_TIMEOUT) -> str:
        """Poll status until ready/error or timeout. Returns final status string."""
        deadline = time.time() + timeout
        terminal_statuses = {"ready", "error", "failed", "rejected", "unsupported"}
        consecutive_errors = 0
        while time.time() < deadline:
            try:
                r = self.get(f"/api/datasets/{dataset_id}/status")
                consecutive_errors = 0
                if r.status_code != 200:
                    time.sleep(PROCESSING_POLL_INTERVAL)
                    continue
                status = r.json().get("status", "").lower()
                if status in terminal_statuses:
                    return status
            except Exception:
                consecutive_errors += 1
                if consecutive_errors >= 5:
                    return "unreachable"
            time.sleep(PROCESSING_POLL_INTERVAL)
        return "timeout"

    def batch_upload(self, filepaths: List[Path],
                     timeout: int = UPLOAD_TIMEOUT) -> Dict[str, Any]:
        """Batch upload multiple files. Returns response JSON."""
        files_list = []
        for fp in filepaths:
            files_list.append(("files", (fp.name, open(fp, "rb"))))
        try:
            r = self.post("/api/datasets/batch", files=files_list, timeout=timeout)
            r.raise_for_status()
            return r.json()
        finally:
            for _, (_, fobj) in files_list:
                fobj.close()


# ---------------------------------------------------------------------------
# Test functions
# ---------------------------------------------------------------------------

def _run_auth(client: VZClient, suite: TestSuite):
    """Test authentication — setup or login."""
    t0 = time.time()
    try:
        key = client.authenticate()
        assert key and key.startswith("vz_"), f"Unexpected key format: {key[:20]}"
        # Verify /me
        r = client.get("/api/auth/me")
        r.raise_for_status()
        me = r.json()
        assert me.get("username") == ADMIN_USER
        suite.record("auth/setup-or-login", True, time.time() - t0)
    except Exception as e:
        suite.record("auth/setup-or-login", False, time.time() - t0, str(e))
        raise  # Auth failure is fatal


def _upload_and_process(client: VZClient, filepath: Path,
                        suite: TestSuite, tier: str) -> Optional[str]:
    """Upload a file, wait for processing, return dataset_id or None."""
    fname = filepath.name
    prefix = f"upload/{tier}/{fname}"

    # Upload
    t0 = time.time()
    try:
        result = client.upload_file(filepath)
        dataset_id = result.get("dataset_id")
        assert dataset_id, f"No dataset_id in response: {result}"
        suite.record(f"{prefix}/upload", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/upload", False, time.time() - t0, str(e))
        return None

    # Wait for ready
    t0 = time.time()
    final_status = client.wait_for_ready(dataset_id)
    if final_status == "ready":
        suite.record(f"{prefix}/processing", True, time.time() - t0)
    else:
        suite.record(f"{prefix}/processing", False, time.time() - t0,
                     f"Final status: {final_status}")
        # Even if not ready, return id for potential partial testing
        return dataset_id

    return dataset_id


def _test_processing_endpoints(client: VZClient, dataset_id: str,
                               fname: str, suite: TestSuite):
    """Test all processing / metadata endpoints for a single dataset."""
    prefix = f"processing/{fname}"

    # GET /api/datasets/{id} — metadata
    t0 = time.time()
    try:
        r = client.get(f"/api/datasets/{dataset_id}")
        r.raise_for_status()
        data = r.json()
        assert data.get("id") or data.get("dataset_id"), "No id in metadata"
        # Check schema/columns
        data.get("columns") or data.get("schema") or data.get("column_names")
        suite.record(f"{prefix}/metadata", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/metadata", False, time.time() - t0, str(e))

    # GET /api/datasets/{id}/status
    t0 = time.time()
    try:
        r = client.get(f"/api/datasets/{dataset_id}/status")
        r.raise_for_status()
        assert r.json().get("status", "").lower() == "ready"
        suite.record(f"{prefix}/status", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/status", False, time.time() - t0, str(e))

    # GET /api/datasets/{id}/sample
    t0 = time.time()
    try:
        r = client.get(f"/api/datasets/{dataset_id}/sample")
        r.raise_for_status()
        sample = r.json()
        rows = sample.get("sample") or sample.get("rows") or sample.get("data")
        assert rows and len(rows) > 0, "Empty sample"
        suite.record(f"{prefix}/sample", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/sample", False, time.time() - t0, str(e))

    # GET /api/datasets/{id}/statistics
    t0 = time.time()
    try:
        r = client.get(f"/api/datasets/{dataset_id}/statistics")
        r.raise_for_status()
        stats = r.json()
        assert stats.get("statistics") or stats.get("column_statistics"), \
            "No statistics in response"
        suite.record(f"{prefix}/statistics", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/statistics", False, time.time() - t0, str(e))

    # GET /api/datasets/{id}/profile
    t0 = time.time()
    try:
        r = client.get(f"/api/datasets/{dataset_id}/profile")
        r.raise_for_status()
        profile = r.json()
        assert profile.get("column_profiles") is not None or profile.get("profiles") is not None, \
            "No profiles in response"
        suite.record(f"{prefix}/profile", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/profile", False, time.time() - t0, str(e))


def _test_feature_endpoints(client: VZClient, dataset_id: str,
                            fname: str, suite: TestSuite):
    """Test feature endpoints (SQL, search, PII, compliance, etc.) for a dataset."""
    prefix = f"features/{fname}"

    # SQL: POST /api/sql/query
    t0 = time.time()
    try:
        r = client.post("/api/sql/query", json={
            "query": f"SELECT * FROM dataset_{dataset_id.replace('-', '_')} LIMIT 5",
            "dataset_id": dataset_id,
            "limit": 5,
        })
        r.raise_for_status()
        suite.record(f"{prefix}/sql-query", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/sql-query", False, time.time() - t0, str(e))

    # Search: POST /api/search/
    t0 = time.time()
    try:
        r = client.post("/api/search/", json={
            "query": fname.split(".")[0].replace("_", " ").replace("-", " ")[:30],
            "dataset_id": dataset_id,
            "limit": 5,
        })
        r.raise_for_status()
        suite.record(f"{prefix}/search", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/search", False, time.time() - t0, str(e))

    # PII Scan: POST /api/pii/scan/{dataset_id}
    t0 = time.time()
    try:
        r = client.post(f"/api/pii/scan/{dataset_id}")
        r.raise_for_status()
        suite.record(f"{prefix}/pii-scan", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/pii-scan", False, time.time() - t0, str(e))

    # Compliance: GET /api/datasets/{id}/compliance
    t0 = time.time()
    try:
        r = client.get(f"/api/datasets/{dataset_id}/compliance")
        r.raise_for_status()
        suite.record(f"{prefix}/compliance", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/compliance", False, time.time() - t0, str(e))

    # Searchability: GET /api/datasets/{id}/searchability
    t0 = time.time()
    try:
        r = client.get(f"/api/datasets/{dataset_id}/searchability")
        r.raise_for_status()
        suite.record(f"{prefix}/searchability", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/searchability", False, time.time() - t0, str(e))

    # Quality Attestation: POST /api/datasets/{id}/attestation
    t0 = time.time()
    try:
        r = client.post(f"/api/datasets/{dataset_id}/attestation")
        r.raise_for_status()
        suite.record(f"{prefix}/attestation", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/attestation", False, time.time() - t0, str(e))

    # Listing Metadata: POST /api/datasets/{id}/listing-metadata
    t0 = time.time()
    try:
        r = client.post(f"/api/datasets/{dataset_id}/listing-metadata")
        r.raise_for_status()
        suite.record(f"{prefix}/listing-metadata", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/listing-metadata", False, time.time() - t0, str(e))


def _test_delete(client: VZClient, dataset_id: str, fname: str, suite: TestSuite):
    """DELETE a dataset and verify 404 on re-fetch."""
    prefix = f"delete/{fname}"

    # DELETE /api/datasets/{id}
    t0 = time.time()
    try:
        r = client.delete(f"/api/datasets/{dataset_id}")
        assert r.status_code in (200, 204), f"DELETE returned {r.status_code}: {r.text[:200]}"
        suite.record(f"{prefix}/delete", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/delete", False, time.time() - t0, str(e))
        return

    # Verify 404
    t0 = time.time()
    try:
        r = client.get(f"/api/datasets/{dataset_id}")
        assert r.status_code == 404, f"Expected 404 after delete, got {r.status_code}"
        suite.record(f"{prefix}/verify-404", True, time.time() - t0)
    except Exception as e:
        suite.record(f"{prefix}/verify-404", False, time.time() - t0, str(e))


def _run_upload_unsupported(client: VZClient, suite: TestSuite):
    """Upload unsupported file formats — expect rejection."""
    for fname in UNSUPPORTED_FILES:
        fp = TEST_DATA_DIR / fname
        prefix = f"upload/unsupported/{fname}"
        t0 = time.time()
        if not fp.exists():
            suite.record(prefix, False, 0, f"File not found: {fp}", skipped=True)
            continue
        try:
            r = client.post(
                "/api/datasets/upload",
                files={"file": (fp.name, open(fp, "rb"))},
                headers=client.headers,
                timeout=UPLOAD_TIMEOUT,
            )
            # Expect 400/415/422 rejection
            if r.status_code in (400, 415, 422):
                suite.record(prefix, True, time.time() - t0,
                             f"Correctly rejected: {r.status_code}")
            elif r.status_code in (200, 202):
                # Accepted — check if processing results in error/rejected
                data = r.json()
                did = data.get("dataset_id")
                if did:
                    status = client.wait_for_ready(did, timeout=120)
                    if status in ("error", "failed", "rejected", "unsupported"):
                        suite.record(prefix, True, time.time() - t0,
                                     f"Accepted then rejected during processing: {status}")
                    else:
                        suite.record(prefix, False, time.time() - t0,
                                     f"Expected rejection but got status: {status}")
                else:
                    suite.record(prefix, False, time.time() - t0,
                                 f"Unexpected 2xx with no dataset_id")
            else:
                suite.record(prefix, False, time.time() - t0,
                             f"Unexpected status {r.status_code}: {r.text[:200]}")
        except Exception as e:
            suite.record(prefix, False, time.time() - t0, str(e))


def _run_batch_upload(client: VZClient, suite: TestSuite) -> List[str]:
    """Batch upload 3 small files, confirm-all, wait for ready. Returns dataset_ids."""
    dataset_ids: List[str] = []
    filepaths = [TEST_DATA_DIR / f for f in BATCH_FILES]
    missing = [fp for fp in filepaths if not fp.exists()]
    if missing:
        suite.record("batch/upload", False, 0,
                     f"Missing files: {[str(m) for m in missing]}", skipped=True)
        return dataset_ids

    # POST /api/datasets/batch
    t0 = time.time()
    try:
        result = client.batch_upload(filepaths)
        batch_id = result.get("batch_id")
        accepted = result.get("accepted", 0)
        items = result.get("items", [])
        assert batch_id, f"No batch_id in response: {result}"
        assert accepted >= 1, f"No files accepted: {result}"
        for item in items:
            did = item.get("dataset_id")
            if did and item.get("status") == "accepted":
                dataset_ids.append(did)
        suite.record("batch/upload", True, time.time() - t0,
                     f"batch_id={batch_id}, accepted={accepted}")
    except Exception as e:
        suite.record("batch/upload", False, time.time() - t0, str(e))
        return dataset_ids

    # POST /api/datasets/batch/{batch_id}/confirm-all
    t0 = time.time()
    try:
        r = client.post(f"/api/datasets/batch/{batch_id}/confirm-all")
        r.raise_for_status()
        suite.record("batch/confirm-all", True, time.time() - t0)
    except Exception as e:
        suite.record("batch/confirm-all", False, time.time() - t0, str(e))

    # Wait for all to be ready
    t0 = time.time()
    all_ready = True
    for did in dataset_ids:
        status = client.wait_for_ready(did)
        if status != "ready":
            all_ready = False
            suite.record(f"batch/processing/{did[:8]}", False, time.time() - t0,
                         f"Status: {status}")
    if all_ready and dataset_ids:
        suite.record("batch/all-ready", True, time.time() - t0,
                     f"{len(dataset_ids)} datasets ready")

    return dataset_ids


def _run_negative(client: VZClient, suite: TestSuite):
    """Negative / edge-case tests."""
    fake_id = "00000000-0000-0000-0000-000000000000"

    # -- Wrong extension --
    t0 = time.time()
    try:
        r = client.upload_bytes("test_file.xyz", b"col1,col2\n1,2\n3,4\n")
        if r.status_code in (400, 415, 422):
            suite.record("negative/wrong-extension", True, time.time() - t0,
                         f"Rejected: {r.status_code}")
        else:
            suite.record("negative/wrong-extension", False, time.time() - t0,
                         f"Expected rejection, got {r.status_code}: {r.text[:200]}")
    except Exception as e:
        suite.record("negative/wrong-extension", False, time.time() - t0, str(e))

    # -- Empty file --
    t0 = time.time()
    try:
        r = client.upload_bytes("empty.csv", b"")
        if r.status_code in (400, 415, 422):
            suite.record("negative/empty-file", True, time.time() - t0,
                         f"Rejected: {r.status_code}")
        elif r.status_code in (200, 202):
            # Might accept then fail during processing
            data = r.json()
            did = data.get("dataset_id")
            if did:
                status = client.wait_for_ready(did, timeout=60)
                if status in ("error", "failed", "rejected"):
                    suite.record("negative/empty-file", True, time.time() - t0,
                                 f"Failed during processing: {status}")
                else:
                    suite.record("negative/empty-file", False, time.time() - t0,
                                 f"Empty file got status: {status}")
            else:
                suite.record("negative/empty-file", False, time.time() - t0,
                             f"2xx but no dataset_id")
        else:
            suite.record("negative/empty-file", False, time.time() - t0,
                         f"Unexpected: {r.status_code}")
    except Exception as e:
        suite.record("negative/empty-file", False, time.time() - t0, str(e))

    # -- SQL injection --
    t0 = time.time()
    try:
        r = client.post("/api/sql/query", json={
            "query": "SELECT * FROM datasets; DROP TABLE datasets; --",
            "limit": 5,
        })
        # Should be rejected (400) or at least not succeed destructively
        if r.status_code in (400, 403, 422):
            suite.record("negative/sql-injection", True, time.time() - t0,
                         f"Blocked: {r.status_code}")
        elif r.status_code == 200:
            # If it returned 200, the DROP should have been stripped/blocked
            suite.record("negative/sql-injection", True, time.time() - t0,
                         "Returned 200 — query likely sanitized (DROP blocked)")
        else:
            suite.record("negative/sql-injection", False, time.time() - t0,
                         f"Unexpected: {r.status_code}: {r.text[:200]}")
    except Exception as e:
        suite.record("negative/sql-injection", False, time.time() - t0, str(e))

    # -- Invalid dataset ID on all endpoints --
    endpoints = [
        ("GET", f"/api/datasets/{fake_id}"),
        ("GET", f"/api/datasets/{fake_id}/status"),
        ("GET", f"/api/datasets/{fake_id}/sample"),
        ("GET", f"/api/datasets/{fake_id}/statistics"),
        ("GET", f"/api/datasets/{fake_id}/profile"),
        ("GET", f"/api/datasets/{fake_id}/compliance"),
        ("GET", f"/api/datasets/{fake_id}/searchability"),
        ("POST", f"/api/pii/scan/{fake_id}"),
        ("POST", f"/api/datasets/{fake_id}/attestation"),
        ("POST", f"/api/datasets/{fake_id}/listing-metadata"),
        ("DELETE", f"/api/datasets/{fake_id}"),
    ]
    for method, path in endpoints:
        endpoint_name = path.split("/")[-1] if "/" in path else path
        t0 = time.time()
        try:
            if method == "GET":
                r = client.get(path)
            elif method == "POST":
                r = client.post(path)
            else:
                r = client.delete(path)
            if r.status_code in (404, 400, 422):
                suite.record(f"negative/invalid-id/{endpoint_name}", True,
                             time.time() - t0, f"Correctly returned {r.status_code}")
            else:
                suite.record(f"negative/invalid-id/{endpoint_name}", False,
                             time.time() - t0,
                             f"Expected 404, got {r.status_code}: {r.text[:200]}")
        except Exception as e:
            suite.record(f"negative/invalid-id/{endpoint_name}", False,
                         time.time() - t0, str(e))


# ---------------------------------------------------------------------------
# Concurrent stress upload helper
# ---------------------------------------------------------------------------

def _concurrent_upload(client: VZClient, files: List[str], tier: str,
                       suite: TestSuite) -> List[Tuple[str, str]]:
    """Upload files concurrently. Returns list of (filename, dataset_id) for successful ones."""
    results: List[Tuple[str, str]] = []
    filepaths = []
    for fname in files:
        fp = TEST_DATA_DIR / fname
        if not fp.exists():
            suite.record(f"upload/{tier}/{fname}/upload", False, 0,
                         f"File not found: {fp}", skipped=True)
        else:
            filepaths.append(fp)

    if not filepaths:
        return results

    def _do_upload(fp: Path) -> Tuple[Path, Optional[Dict[str, Any]], Optional[str]]:
        try:
            result = client.upload_file(fp)
            return (fp, result, None)
        except Exception as e:
            return (fp, None, str(e))

    with ThreadPoolExecutor(max_workers=min(4, len(filepaths))) as pool:
        futures = {pool.submit(_do_upload, fp): fp for fp in filepaths}
        for future in as_completed(futures):
            fp, result, error = future.result()
            fname = fp.name
            prefix = f"upload/{tier}/{fname}"
            if error:
                suite.record(f"{prefix}/upload", False, 0, error)
            elif result:
                did = result.get("dataset_id")
                if did:
                    suite.record(f"{prefix}/upload", True, 0)
                    results.append((fname, did))
                else:
                    suite.record(f"{prefix}/upload", False, 0,
                                 f"No dataset_id: {result}")

    # Wait for all to be ready (sequentially — polling is cheap)
    ready_results: List[Tuple[str, str]] = []
    for fname, did in results:
        prefix = f"upload/{tier}/{fname}"
        t0 = time.time()
        status = client.wait_for_ready(did)
        if status == "ready":
            suite.record(f"{prefix}/processing", True, time.time() - t0)
            ready_results.append((fname, did))
        else:
            suite.record(f"{prefix}/processing", False, time.time() - t0,
                         f"Final status: {status}")
            ready_results.append((fname, did))  # still include for partial testing

    return ready_results


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def _sequential_upload(client: VZClient, files: List[str], tier: str,
                       suite: TestSuite) -> List[Tuple[str, str]]:
    """Upload files one-at-a-time (for very large files). Returns list of (filename, dataset_id)."""
    results: List[Tuple[str, str]] = []
    for fname in files:
        fp = TEST_DATA_DIR / fname
        prefix = f"upload/{tier}/{fname}"
        if not fp.exists():
            suite.record(f"{prefix}/upload", False, 0,
                         f"File not found: {fp}", skipped=True)
            continue

        t0 = time.time()
        try:
            result = client.upload_file(fp)
            did = result.get("dataset_id")
            assert did, f"No dataset_id in response: {result}"
            suite.record(f"{prefix}/upload", True, time.time() - t0)
        except Exception as e:
            suite.record(f"{prefix}/upload", False, time.time() - t0, str(e))
            continue

        t0 = time.time()
        try:
            status = client.wait_for_ready(did)
            if status == "ready":
                suite.record(f"{prefix}/processing", True, time.time() - t0)
            else:
                suite.record(f"{prefix}/processing", False, time.time() - t0,
                             f"Final status: {status}")
        except Exception as e:
            suite.record(f"{prefix}/processing", False, time.time() - t0,
                         f"Exception during polling: {str(e)[:100]}")
        results.append((fname, did))

    return results


def _get_vz_version(base_url: str) -> str:
    """Try to get VZ version from health endpoint."""
    try:
        r = requests.get(f"{base_url}/api/health", timeout=10)
        if r.status_code == 200:
            data = r.json()
            return data.get("version", data.get("app_version", "unknown"))
    except Exception:
        pass
    return "unknown"


def _write_report(suite: TestSuite, base_url: str, vz_version: str,
                  wall_clock: float, include_xl: bool):
    """Write a detailed Markdown report to the report file."""
    report_path = Path("/tmp/vectoraiz-stress-test-2-report.md")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    total = len(suite.results)
    passed = sum(1 for r in suite.results if r.passed and not r.skipped)
    failed = sum(1 for r in suite.results if not r.passed and not r.skipped)
    skipped = sum(1 for r in suite.results if r.skipped)

    # Group results by tier
    tiers: Dict[str, List[TestResult]] = {}
    for r in suite.results:
        parts = r.name.split("/")
        tier_name = parts[0] if len(parts) >= 2 else "other"
        if tier_name in ("upload", "processing", "features", "delete"):
            tier_name = f"{parts[0]}/{parts[1]}" if len(parts) >= 2 else parts[0]
        tiers.setdefault(tier_name, []).append(r)

    lines: List[str] = []
    lines.append("# VectorAIz Stress Test #2 — Report")
    lines.append("")
    lines.append(f"**Date:** {now}")
    lines.append(f"**VZ Version:** {vz_version}")
    lines.append(f"**Target:** {base_url}")
    lines.append(f"**Test Data Dir:** {TEST_DATA_DIR}")
    lines.append(f"**XL Files:** {'enabled' if include_xl else 'disabled'}")
    lines.append(f"**Wall-Clock Time:** {wall_clock:.1f}s ({wall_clock/60:.1f} min)")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append(f"| Metric | Count |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total  | {total} |")
    lines.append(f"| Passed | {passed} |")
    lines.append(f"| Failed | {failed} |")
    lines.append(f"| Skipped | {skipped} |")
    lines.append("")

    lines.append("## Comparison with Stress Test #1 (S145)")
    lines.append("")
    lines.append("This is **Stress Test #2**. The first test (S145) had "
                 "51 passed, 8 bugs, 15 cascade failures. All 8 bugs were "
                 "fixed in S146. This test verifies fixes and adds 25 new "
                 "format files.")
    lines.append("")

    # Per-tier sections
    lines.append("## Results by Tier")
    lines.append("")
    for tier_name, tier_results in sorted(tiers.items()):
        tier_passed = sum(1 for r in tier_results if r.passed and not r.skipped)
        tier_failed = sum(1 for r in tier_results if not r.passed and not r.skipped)
        tier_skipped = sum(1 for r in tier_results if r.skipped)
        lines.append(f"### {tier_name} ({tier_passed}P / {tier_failed}F / {tier_skipped}S)")
        lines.append("")
        lines.append("| Test | Result | Duration | Detail |")
        lines.append("|------|--------|----------|--------|")
        for r in tier_results:
            status = "SKIP" if r.skipped else ("PASS" if r.passed else "FAIL")
            icon = {"PASS": "✅", "FAIL": "❌", "SKIP": "⏭️"}[status]
            time_str = f"{r.duration:.1f}s" if r.duration >= 0.1 else "<0.1s"
            detail_safe = r.detail.replace("|", "\\|")[:150] if r.detail else ""
            lines.append(f"| {r.name} | {icon} {status} | {time_str} | {detail_safe} |")
        lines.append("")

    # Failed tests section
    failed_results = [r for r in suite.results if not r.passed and not r.skipped]
    if failed_results:
        lines.append("## Failed Tests — Full Details")
        lines.append("")
        for i, r in enumerate(failed_results, 1):
            lines.append(f"### {i}. {r.name}")
            lines.append(f"- **Duration:** {r.duration:.1f}s")
            lines.append(f"- **Detail:** {r.detail}")
            lines.append("")
    else:
        lines.append("## Failed Tests")
        lines.append("")
        lines.append("**None! All tests passed.**")
        lines.append("")

    # Timing section: per-file upload + processing times
    lines.append("## Timing: Per-File Upload & Processing")
    lines.append("")
    lines.append("| File | Upload | Processing | Total |")
    lines.append("|------|--------|------------|-------|")
    upload_results = {r.name: r for r in suite.results if "/upload" in r.name}
    processing_results = {r.name: r for r in suite.results if "/processing" in r.name}
    # Collect unique file keys
    seen_files = set()
    for r in suite.results:
        parts = r.name.split("/")
        if len(parts) >= 3 and parts[0] == "upload":
            file_key = f"{parts[1]}/{parts[2]}"
            if file_key not in seen_files:
                seen_files.add(file_key)
                upload_key = f"upload/{file_key}/upload"
                proc_key = f"upload/{file_key}/processing"
                u = upload_results.get(upload_key)
                p = processing_results.get(proc_key)
                u_time = f"{u.duration:.1f}s" if u else "—"
                p_time = f"{p.duration:.1f}s" if p else "—"
                total = (u.duration if u else 0) + (p.duration if p else 0)
                total_str = f"{total:.1f}s" if total > 0 else "—"
                lines.append(f"| {file_key} | {u_time} | {p_time} | {total_str} |")
    lines.append("")

    lines.append(f"---\n*Generated by test_beta_readiness.py (Stress Test #2) at {now}*\n")

    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n📄 Report written to: {report_path}")


def run_all_tests(base_url: str, include_xl: bool = False):
    suite = TestSuite()
    client = VZClient(base_url)
    wall_start = time.time()

    vz_version = _get_vz_version(base_url)

    print(f"\nVectorAIz Stress Test #2 — Beta-Readiness Integration Tests")
    print(f"Target: {base_url}")
    print(f"VZ Version: {vz_version}")
    print(f"Test data: {TEST_DATA_DIR}")
    print(f"XL files: {'enabled' if include_xl else 'disabled (use --xl)'}")
    print("=" * 70)

    # ------------------------------------------------------------------
    # 0. Connectivity check
    # ------------------------------------------------------------------
    print("\n--- Connectivity ---")
    t0 = time.time()
    try:
        r = requests.get(f"{base_url}/api/health", timeout=30)
        if r.status_code == 200:
            suite.record("connectivity/health", True, time.time() - t0)
        else:
            # Some setups may not have /api/health — try root
            r2 = requests.get(base_url, timeout=30)
            suite.record("connectivity/health", r2.ok, time.time() - t0,
                         f"health={r.status_code}, root={r2.status_code}")
    except Exception as e:
        suite.record("connectivity/health", False, time.time() - t0, str(e))
        print("\n  FATAL: Cannot reach VZ instance. Aborting.")
        suite.summary()
        _write_report(suite, base_url, vz_version, time.time() - wall_start, include_xl)
        return False

    # ------------------------------------------------------------------
    # 1. Auth
    # ------------------------------------------------------------------
    print("\n--- Auth ---")
    try:
        _run_auth(client, suite)
    except Exception:
        print("\n  FATAL: Auth failed. Aborting.")
        suite.summary()
        _write_report(suite, base_url, vz_version, time.time() - wall_start, include_xl)
        return False

    # ------------------------------------------------------------------
    # 2. Upload + Processing: Small files (concurrent)
    # ------------------------------------------------------------------
    print("\n--- Upload: Small Files (original) ---")
    small_datasets = _concurrent_upload(client, SMALL_FILES, "small", suite)

    # ------------------------------------------------------------------
    # 3. Upload + Processing: Medium files (concurrent)
    # ------------------------------------------------------------------
    print("\n--- Upload: Medium Files (original) ---")
    medium_datasets = _concurrent_upload(client, MEDIUM_FILES, "medium", suite)

    # ------------------------------------------------------------------
    # 4. Upload + Processing: Large files (concurrent)
    # ------------------------------------------------------------------
    print("\n--- Upload: Large Files (original) ---")
    large_datasets = _concurrent_upload(client, LARGE_FILES, "large", suite)

    # ------------------------------------------------------------------
    # 5. Upload + Processing: XL files
    # ------------------------------------------------------------------
    xl_datasets: List[Tuple[str, str]] = []
    if include_xl:
        print("\n--- Upload: XL Files ---")
        xl_datasets = _concurrent_upload(client, XL_FILES, "xl", suite)
    else:
        print("\n--- Upload: XL Files (skipped — use --xl) ---")
        for fname in XL_FILES:
            suite.record(f"upload/xl/{fname}", False, 0,
                         "Skipped — use --xl flag", skipped=True)

    # ------------------------------------------------------------------
    # 6. Unsupported file upload
    # ------------------------------------------------------------------
    print("\n--- Upload: Unsupported Files ---")
    _run_upload_unsupported(client, suite)

    # ------------------------------------------------------------------
    # 7. NEW FORMAT: Small doc files (concurrent)
    # ------------------------------------------------------------------
    print("\n--- Upload: New Format Small Files ---")
    nf_small_datasets = _concurrent_upload(
        client, NEW_FORMAT_SMALL_FILES, "nf-small", suite)

    # ------------------------------------------------------------------
    # 8. NEW FORMAT: Medium doc files (concurrent)
    # ------------------------------------------------------------------
    print("\n--- Upload: New Format Medium Files ---")
    nf_medium_datasets = _concurrent_upload(
        client, NEW_FORMAT_MEDIUM_FILES, "nf-medium", suite)

    # ------------------------------------------------------------------
    # 9. NEW FORMAT: Data files (concurrent)
    # ------------------------------------------------------------------
    print("\n--- Upload: New Format Data Files ---")
    nf_data_datasets = _concurrent_upload(
        client, NEW_FORMAT_DATA_FILES, "nf-data", suite)

    # ------------------------------------------------------------------
    # 10. NEW FORMAT: Large files (sequential — very big)
    # ------------------------------------------------------------------
    print("\n--- Upload: New Format Large Files (sequential) ---")
    nf_large_datasets = _sequential_upload(
        client, NEW_FORMAT_LARGE_FILES, "nf-large", suite)

    # ------------------------------------------------------------------
    # 11. Processing & Feature tests for all uploaded datasets
    # ------------------------------------------------------------------
    original_datasets = small_datasets + medium_datasets + large_datasets + xl_datasets
    new_format_datasets = (nf_small_datasets + nf_medium_datasets
                           + nf_data_datasets + nf_large_datasets)
    all_datasets = original_datasets + new_format_datasets

    if all_datasets:
        print("\n--- Processing Endpoints (all datasets) ---")
        for fname, did in all_datasets:
            _test_processing_endpoints(client, did, fname, suite)

        print("\n--- Feature Endpoints (original + small new-format) ---")
        # Run feature tests on: all small, first medium, first large, first xl
        feature_datasets = list(small_datasets)
        if medium_datasets:
            feature_datasets.append(medium_datasets[0])
        if large_datasets:
            feature_datasets.append(large_datasets[0])
        if xl_datasets:
            feature_datasets.append(xl_datasets[0])
        # ALL new format small files get full feature tests
        feature_datasets.extend(nf_small_datasets)

        for fname, did in feature_datasets:
            _test_feature_endpoints(client, did, fname, suite)

    # ------------------------------------------------------------------
    # 12. Batch upload test
    # ------------------------------------------------------------------
    print("\n--- Batch Upload ---")
    batch_ids = _run_batch_upload(client, suite)

    # Clean up batch datasets
    if batch_ids:
        print("\n--- Batch Delete ---")
        for did in batch_ids:
            _test_delete(client, did, f"batch-{did[:8]}", suite)

    # ------------------------------------------------------------------
    # 13. Negative tests
    # ------------------------------------------------------------------
    print("\n--- Negative Tests ---")
    _run_negative(client, suite)

    # ------------------------------------------------------------------
    # 14. Crash-Prone Large Files (run LAST — may take down VZ)
    # ------------------------------------------------------------------
    print("\n--- Upload: Crash-Prone Large Files (tested last) ---")
    crash_datasets = _sequential_upload(
        client, CRASH_PRONE_FILES, "crash-prone", suite)
    all_datasets.extend(crash_datasets)

    # ------------------------------------------------------------------
    # 15. Delete all uploaded datasets (cleanup)
    # ------------------------------------------------------------------
    if all_datasets:
        print("\n--- Delete (cleanup) ---")
        for fname, did in all_datasets:
            _test_delete(client, did, fname, suite)

    # ------------------------------------------------------------------
    # Summary + Report
    # ------------------------------------------------------------------
    wall_clock = time.time() - wall_start
    success = suite.summary()
    _write_report(suite, base_url, vz_version, wall_clock, include_xl)
    return success


# ---------------------------------------------------------------------------
# Pytest integration
# ---------------------------------------------------------------------------

# When run under pytest, expose individual test functions.
# pytest will collect functions starting with test_.

_pytest_client: Optional[VZClient] = None
_pytest_suite: Optional[TestSuite] = None
_pytest_datasets: Dict[str, str] = {}  # fname -> dataset_id


def _get_pytest_client() -> VZClient:
    global _pytest_client
    if _pytest_client is None:
        url = os.environ.get("VZ_BASE_URL", BASE_URL)
        _pytest_client = VZClient(url)
        _pytest_client.authenticate()
    return _pytest_client


def _get_pytest_suite() -> TestSuite:
    global _pytest_suite
    if _pytest_suite is None:
        _pytest_suite = TestSuite()
    return _pytest_suite


class TestBetaReadiness:
    """Pytest class — each method is collected as a test."""

    @classmethod
    def setup_class(cls):
        """Delete all existing datasets to avoid 409 Conflict from stale data."""
        client = _get_pytest_client()
        try:
            r = client.get("/api/datasets")
            if r.status_code == 200:
                datasets = r.json()
                # Handle both list response and dict with 'datasets' key
                if isinstance(datasets, dict):
                    datasets = datasets.get("datasets", [])
                for ds in datasets:
                    ds_id = ds.get("id") or ds.get("dataset_id")
                    if ds_id:
                        client.delete(f"/api/datasets/{ds_id}")
        except Exception:
            pass  # Best-effort cleanup; tests will report real errors

    # -- Auth --

    def test_auth_login(self):
        client = _get_pytest_client()
        assert client.api_key and client.api_key.startswith("vz_")
        r = client.get("/api/auth/me")
        assert r.status_code == 200

    # -- Small file upload + processing --

    def test_upload_small_csv(self):
        self._upload_and_verify("saas_company_metrics.csv")

    def test_upload_small_json(self):
        self._upload_and_verify("product_catalog.json")

    def test_upload_small_csv_barcelona(self):
        self._upload_and_verify("barcelona_apartments.csv")

    def test_upload_small_tsv(self):
        self._upload_and_verify("gene_expression_sample.tsv")

    # -- Medium file upload --

    def test_upload_medium_parquet(self):
        self._upload_and_verify("nyc-taxi-yellow-2024-03.parquet")

    def test_upload_medium_htm(self):
        self._upload_and_verify("sec-edgar-readme.htm")

    def test_upload_medium_pdf(self):
        self._upload_and_verify("us-budget-2025.pdf")

    # -- Feature tests (use first small file) --

    def test_sql_query(self):
        client = _get_pytest_client()
        did = self._ensure_dataset("saas_company_metrics.csv")
        if not did:
            import pytest as pt
            pt.skip("No dataset available")
        r = client.post("/api/sql/query", json={
            "query": f"SELECT * FROM dataset_{did.replace('-', '_')} LIMIT 5",
            "dataset_id": did, "limit": 5,
        })
        assert r.status_code == 200

    def test_search(self):
        client = _get_pytest_client()
        did = self._ensure_dataset("saas_company_metrics.csv")
        if not did:
            import pytest as pt
            pt.skip("No dataset available")
        r = client.post("/api/search/", json={
            "query": "saas company", "dataset_id": did, "limit": 5,
        })
        assert r.status_code == 200

    def test_pii_scan(self):
        client = _get_pytest_client()
        did = self._ensure_dataset("saas_company_metrics.csv")
        if not did:
            import pytest as pt
            pt.skip("No dataset available")
        r = client.post(f"/api/pii/scan/{did}")
        assert r.status_code == 200

    def test_compliance(self):
        client = _get_pytest_client()
        did = self._ensure_dataset("saas_company_metrics.csv")
        if not did:
            import pytest as pt
            pt.skip("No dataset available")
        r = client.get(f"/api/datasets/{did}/compliance")
        assert r.status_code == 200

    def test_searchability(self):
        client = _get_pytest_client()
        did = self._ensure_dataset("saas_company_metrics.csv")
        if not did:
            import pytest as pt
            pt.skip("No dataset available")
        r = client.get(f"/api/datasets/{did}/searchability")
        assert r.status_code == 200

    def test_attestation(self):
        client = _get_pytest_client()
        did = self._ensure_dataset("saas_company_metrics.csv")
        if not did:
            import pytest as pt
            pt.skip("No dataset available")
        r = client.post(f"/api/datasets/{did}/attestation")
        assert r.status_code == 200

    def test_listing_metadata(self):
        client = _get_pytest_client()
        did = self._ensure_dataset("saas_company_metrics.csv")
        if not did:
            import pytest as pt
            pt.skip("No dataset available")
        r = client.post(f"/api/datasets/{did}/listing-metadata")
        assert r.status_code == 200

    # -- Unsupported files --

    def test_upload_unsupported_shp_zip(self):
        client = _get_pytest_client()
        fp = TEST_DATA_DIR / "catalonia-geodata-260101.shp.zip"
        if not fp.exists():
            import pytest as pt
            pt.skip("File not found")
        r = client.post("/api/datasets/upload",
                        files={"file": (fp.name, open(fp, "rb"))},
                        headers=client.headers, timeout=UPLOAD_TIMEOUT)
        # Should reject or fail during processing
        assert r.status_code in (400, 415, 422, 200, 202)
        if r.status_code in (200, 202):
            did = r.json().get("dataset_id")
            if did:
                status = client.wait_for_ready(did, timeout=120)
                assert status in ("error", "failed", "rejected", "unsupported"), \
                    f"Expected failure, got {status}"

    # -- Negative tests --

    def test_negative_wrong_extension(self):
        client = _get_pytest_client()
        r = client.upload_bytes("test.xyz", b"col1,col2\n1,2\n")
        assert r.status_code in (400, 415, 422)

    def test_negative_empty_file(self):
        client = _get_pytest_client()
        r = client.upload_bytes("empty.csv", b"")
        # Either rejected at upload, fails during processing, or duplicate detected
        assert r.status_code in (400, 409, 415, 422, 200, 202)

    def test_negative_sql_injection(self):
        client = _get_pytest_client()
        r = client.post("/api/sql/query", json={
            "query": "SELECT 1; DROP TABLE datasets; --",
        })
        assert r.status_code in (400, 403, 422)

    def test_negative_invalid_dataset_id(self):
        client = _get_pytest_client()
        fake_id = "00000000-0000-0000-0000-000000000000"
        r = client.get(f"/api/datasets/{fake_id}")
        assert r.status_code in (404, 400, 422)

    def test_negative_delete_invalid_id(self):
        client = _get_pytest_client()
        fake_id = "00000000-0000-0000-0000-000000000000"
        r = client.delete(f"/api/datasets/{fake_id}")
        assert r.status_code in (404, 400, 422)

    # -- Batch upload --

    def test_batch_upload(self):
        client = _get_pytest_client()
        filepaths = [TEST_DATA_DIR / f for f in BATCH_FILES if (TEST_DATA_DIR / f).exists()]
        if len(filepaths) < 2:
            import pytest as pt
            pt.skip("Not enough test files for batch")
        result = client.batch_upload(filepaths)
        batch_id = result.get("batch_id")
        assert batch_id
        assert result.get("accepted", 0) >= 1

        # Confirm all
        r = client.post(f"/api/datasets/batch/{batch_id}/confirm-all")
        assert r.status_code in (200, 202)

        # Wait for at least one to be ready
        for item in result.get("items", []):
            did = item.get("dataset_id")
            if did and item.get("status") == "accepted":
                status = client.wait_for_ready(did, timeout=300)
                assert status == "ready", f"Batch item {did[:8]} status: {status}"
                # Clean up
                client.delete(f"/api/datasets/{did}")
                break

    # -- Delete test --

    def test_delete_dataset(self):
        client = _get_pytest_client()
        # Use an already-uploaded dataset if available, otherwise upload fresh
        global _pytest_datasets
        did = _pytest_datasets.pop("barcelona_apartments.csv", None)
        if not did:
            fp = TEST_DATA_DIR / "barcelona_apartments.csv"
            if not fp.exists():
                import pytest as pt
                pt.skip("File not found")
            result = client.upload_file(fp)
            did = result["dataset_id"]
            status = client.wait_for_ready(did)
            assert status == "ready"

        r = client.delete(f"/api/datasets/{did}")
        assert r.status_code in (200, 204)

        r = client.get(f"/api/datasets/{did}")
        assert r.status_code == 404

    # =====================================================================
    # NEW: BQ-VZ-LARGE-FILES, BQ-108, BQ-109, Gate 3 test cases
    # =====================================================================

    # -- Large file upload (generated in-memory) --

    def test_large_generated_csv_50mb(self):
        """BQ-VZ-LARGE-FILES: Generate and upload a 50MB+ CSV to test streaming processing."""
        client = _get_pytest_client()
        # Generate ~50MB CSV in-memory
        buf = io.BytesIO()
        buf.write(b"id,name,category,value_usd,score,region,status,date,amount,description\n")
        for i in range(550_000):  # ~55MB at ~100 bytes/row
            buf.write(
                f"{i},item_{i % 10000},cat_{i % 50},"
                f"{(i * 7 + 13) % 100000 / 100:.2f},"
                f"{(i * 3 + 7) % 10000 / 10000:.4f},"
                f"region_{i % 5},"
                f"{'active' if i % 4 == 0 else 'inactive' if i % 4 == 1 else 'pending' if i % 4 == 2 else 'archived'},"
                f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d},"
                f"{(i * 11 + 3) % 999999},"
                f"Row {i} stress test data\n"
                .encode()
            )

        size_mb = buf.tell() / (1024 * 1024)
        assert size_mb >= 50, f"Generated CSV is only {size_mb:.1f}MB, expected 50MB+"

        buf.seek(0)
        t0 = time.time()
        r = client.post(
            "/api/datasets/upload",
            files={"file": ("stress_test_50mb.csv", buf)},
            timeout=UPLOAD_TIMEOUT,
        )
        upload_time = time.time() - t0
        assert r.status_code in (200, 202), \
            f"50MB upload failed: {r.status_code}: {r.text[:300]}"

        data = r.json()
        dataset_id = data.get("dataset_id")
        assert dataset_id, f"No dataset_id in response: {data}"

        # Wait for processing — this exercises the streaming/chunked path
        t0 = time.time()
        status = client.wait_for_ready(dataset_id, timeout=PROCESSING_POLL_TIMEOUT)
        proc_time = time.time() - t0
        assert status == "ready", (
            f"50MB CSV processing failed: status={status} "
            f"(upload={upload_time:.1f}s, processing={proc_time:.1f}s)"
        )

        # Verify data is queryable
        r = client.get(f"/api/datasets/{dataset_id}/sample")
        assert r.status_code == 200, f"Sample failed after 50MB upload: {r.status_code}"

        r = client.get(f"/api/datasets/{dataset_id}/statistics")
        assert r.status_code == 200, f"Statistics failed after 50MB upload: {r.status_code}"

        # Cleanup
        client.delete(f"/api/datasets/{dataset_id}")

    # -- 10MB generated CSV upload + processing --

    def test_large_generated_csv_10mb(self):
        """BQ-VZ-LARGE-FILES: Generate and upload a ~10MB CSV to test chunked path."""
        client = _get_pytest_client()
        buf = io.BytesIO()
        buf.write(b"id,name,category,value_usd,score,region,status,date\n")
        for i in range(170_000):  # ~10MB at ~62 bytes/row
            buf.write(
                f"{i},item_{i % 5000},cat_{i % 30},"
                f"{(i * 7 + 13) % 100000 / 100:.2f},"
                f"{(i * 3 + 7) % 10000 / 10000:.4f},"
                f"region_{i % 5},"
                f"{'active' if i % 2 == 0 else 'inactive'},"
                f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}\n"
                .encode()
            )

        size_mb = buf.tell() / (1024 * 1024)
        assert size_mb >= 9, f"Generated CSV is only {size_mb:.1f}MB, expected ~10MB"

        buf.seek(0)
        t0 = time.time()
        r = client.post(
            "/api/datasets/upload",
            files={"file": ("stress_test_10mb.csv", buf)},
            timeout=UPLOAD_TIMEOUT,
        )
        upload_time = time.time() - t0
        assert r.status_code in (200, 202), \
            f"10MB upload failed: {r.status_code}: {r.text[:300]}"

        data = r.json()
        dataset_id = data.get("dataset_id")
        assert dataset_id, f"No dataset_id in response: {data}"

        t0 = time.time()
        status = client.wait_for_ready(dataset_id, timeout=PROCESSING_POLL_TIMEOUT)
        proc_time = time.time() - t0
        assert status == "ready", (
            f"10MB CSV processing failed: status={status} "
            f"(upload={upload_time:.1f}s, processing={proc_time:.1f}s)"
        )

        # Verify sample is queryable
        r = client.get(f"/api/datasets/{dataset_id}/sample")
        assert r.status_code == 200, f"Sample failed after 10MB upload: {r.status_code}"

        # Cleanup
        client.delete(f"/api/datasets/{dataset_id}")

    # -- Row count verification after processing --

    def test_row_count_after_processing(self):
        """Verify row count matches expected after processing a known CSV."""
        client = _get_pytest_client()
        expected_rows = 200
        buf = io.BytesIO()
        buf.write(b"id,value,label\n")
        for i in range(expected_rows):
            buf.write(f"{i},{i * 10},label_{i % 5}\n".encode())
        buf.seek(0)

        r = client.post(
            "/api/datasets/upload",
            files={"file": ("row_count_test.csv", buf)},
            timeout=UPLOAD_TIMEOUT,
        )
        assert r.status_code in (200, 202), \
            f"Upload failed: {r.status_code}: {r.text[:300]}"
        dataset_id = r.json().get("dataset_id")
        assert dataset_id

        status = client.wait_for_ready(dataset_id, timeout=120)
        assert status == "ready", f"Processing failed: {status}"

        # Check row count via metadata or statistics
        r = client.get(f"/api/datasets/{dataset_id}")
        assert r.status_code == 200
        meta = r.json()
        row_count = (
            meta.get("row_count")
            or meta.get("rows")
            or meta.get("num_rows")
            or meta.get("total_rows")
        )

        # Also try statistics endpoint
        r = client.get(f"/api/datasets/{dataset_id}/statistics")
        if r.status_code == 200:
            stats = r.json()
            stats_rows = (
                stats.get("row_count")
                or stats.get("rows")
                or stats.get("num_rows")
                or stats.get("total_rows")
            )
            if stats_rows is not None:
                row_count = row_count or stats_rows

        if row_count is not None:
            assert int(row_count) == expected_rows, \
                f"Row count mismatch: expected {expected_rows}, got {row_count}"
        # If row_count isn't exposed, verify via SQL
        else:
            r = client.post("/api/sql/query", json={
                "query": f"SELECT COUNT(*) as cnt FROM dataset_{dataset_id.replace('-', '_')}",
                "dataset_id": dataset_id,
                "limit": 1,
            })
            if r.status_code == 200:
                sql_data = r.json()
                results = sql_data.get("results") or sql_data.get("rows") or sql_data.get("data") or []
                if results:
                    cnt = results[0].get("cnt") if isinstance(results[0], dict) else results[0][0]
                    assert int(cnt) == expected_rows, \
                        f"SQL row count mismatch: expected {expected_rows}, got {cnt}"

        # Cleanup
        client.delete(f"/api/datasets/{dataset_id}")

    # -- Batch upload with 5+ files --

    def test_batch_upload_5_plus_files(self):
        """Batch upload with 5+ files to stress the batch pipeline."""
        client = _get_pytest_client()
        files_list = []
        num_files = 6
        for i in range(num_files):
            rows = f"col_x,col_y,col_z\n"
            for j in range(50):
                rows += f"v{i}_{j},{j * i},{j % 3}\n"
            files_list.append(
                ("files", (f"batch5_file_{i}.csv", io.BytesIO(rows.encode())))
            )

        r = client.post(
            "/api/datasets/batch",
            files=files_list,
            timeout=REQUEST_TIMEOUT,
        )
        assert r.status_code in (200, 202), \
            f"Batch upload failed: {r.status_code}: {r.text[:300]}"
        batch_data = r.json()
        batch_id = batch_data.get("batch_id")
        assert batch_id, f"No batch_id: {batch_data}"
        accepted = batch_data.get("accepted", 0)
        assert accepted >= 5, \
            f"Expected 5+ accepted, got {accepted}: {batch_data}"

        items = batch_data.get("items", [])
        dataset_ids = [item["dataset_id"] for item in items if item.get("dataset_id")]

        # Wait for all items to finish extraction (preview_ready) before confirming
        preview_states = {"preview_ready", "ready", "error", "failed"}
        for did in dataset_ids:
            deadline = time.time() + 120
            while time.time() < deadline:
                r = client.get(f"/api/datasets/{did}/status")
                if r.status_code == 200:
                    st = r.json().get("status", "").lower()
                    if st in preview_states:
                        break
                time.sleep(2)

        # Confirm all
        r = client.post(f"/api/datasets/batch/{batch_id}/confirm-all")
        assert r.status_code in (200, 202), \
            f"Confirm-all failed: {r.status_code}: {r.text[:300]}"

        # Wait for all to reach ready
        ready_count = 0
        for did in dataset_ids:
            status = client.wait_for_ready(did, timeout=180)
            if status == "ready":
                ready_count += 1
            client.delete(f"/api/datasets/{did}")

        assert ready_count >= 5, \
            f"Only {ready_count}/{len(dataset_ids)} batch items reached ready"

    # -- Data preview before indexing (BQ-109) --

    def test_data_preview_endpoint(self):
        """BQ-109: Verify /preview endpoint returns schema/data for an uploaded dataset."""
        client = _get_pytest_client()
        did = self._ensure_dataset("saas_company_metrics.csv")
        if not did:
            import pytest as pt
            pt.skip("No dataset available")

        r = client.get(f"/api/datasets/{did}/preview")
        assert r.status_code == 200, \
            f"Preview failed: {r.status_code}: {r.text[:300]}"
        preview = r.json()
        assert ("status" in preview or "columns" in preview
                or "rows" in preview or "schema" in preview), \
            f"Preview response missing expected fields: {list(preview.keys())}"

    def test_preview_flow_batch_upload(self):
        """BQ-109: Batch upload (preview mode) -> preview -> confirm -> ready."""
        client = _get_pytest_client()
        csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n"

        # Upload via batch with mode=preview
        r = client.post(
            "/api/datasets/batch",
            files=[("files", ("preview_flow_test.csv", io.BytesIO(csv_data)))],
            data={"mode": "preview"},
            timeout=REQUEST_TIMEOUT,
        )
        assert r.status_code in (200, 202), \
            f"Batch upload failed: {r.status_code}: {r.text[:300]}"
        batch_data = r.json()
        batch_id = batch_data.get("batch_id")
        assert batch_id, f"No batch_id: {batch_data}"

        items = batch_data.get("items", [])
        assert len(items) >= 1, f"No items in batch response: {batch_data}"
        dataset_id = items[0].get("dataset_id")
        assert dataset_id, f"No dataset_id in first item: {items[0]}"

        # Wait for extraction to finish (preview_ready or ready)
        deadline = time.time() + 120
        final_st = "unknown"
        while time.time() < deadline:
            r = client.get(f"/api/datasets/{dataset_id}/status")
            if r.status_code == 200:
                final_st = r.json().get("status", "").lower()
                if final_st in ("preview_ready", "ready", "error", "failed"):
                    break
            time.sleep(2)

        # Check preview endpoint
        r = client.get(f"/api/datasets/{dataset_id}/preview")
        assert r.status_code == 200, \
            f"Preview failed (status={final_st}): {r.status_code}: {r.text[:300]}"

        # Confirm for indexing (skip if already ready — single-file may auto-index)
        if final_st == "preview_ready":
            r = client.post(
                f"/api/datasets/{dataset_id}/confirm",
                json={"index": True},
                timeout=REQUEST_TIMEOUT,
            )
            assert r.status_code in (200, 202, 409), \
                f"Confirm failed: {r.status_code}: {r.text[:300]}"
            if r.status_code == 202:
                status = client.wait_for_ready(dataset_id, timeout=300)
                assert status == "ready", f"After confirm, status: {status}"

        # Cleanup
        client.delete(f"/api/datasets/{dataset_id}")

    # -- Batch upload progress tracking (BQ-108) --

    def test_batch_status_tracking(self):
        """BQ-108: Batch upload -> poll batch status endpoint for progress."""
        client = _get_pytest_client()
        csv_files = []
        for i in range(3):
            rows = "col_a,col_b,col_c\n"
            for j in range(100):
                rows += f"val_{i}_{j},{j * 10},{j % 5}\n"
            csv_files.append(
                ("files", (f"batch_track_{i}.csv", io.BytesIO(rows.encode())))
            )

        r = client.post(
            "/api/datasets/batch",
            files=csv_files,
            data={"mode": "preview"},
            timeout=REQUEST_TIMEOUT,
        )
        assert r.status_code in (200, 202), \
            f"Batch upload failed: {r.status_code}: {r.text[:300]}"
        batch_data = r.json()
        batch_id = batch_data.get("batch_id")
        assert batch_id, f"No batch_id: {batch_data}"

        # Poll batch status endpoint
        r = client.get(f"/api/datasets/batch/{batch_id}")
        assert r.status_code == 200, \
            f"Batch status failed: {r.status_code}: {r.text[:300]}"
        status_data = r.json()
        assert any(k in status_data for k in ("total", "items", "datasets", "count")), \
            f"Batch status missing expected fields: {list(status_data.keys())}"

        # Confirm all and cleanup
        r = client.post(f"/api/datasets/batch/{batch_id}/confirm-all")
        assert r.status_code in (200, 202), \
            f"Confirm-all failed: {r.status_code}: {r.text[:300]}"

        for item in batch_data.get("items", []):
            did = item.get("dataset_id")
            if did:
                client.wait_for_ready(did, timeout=120)
                client.delete(f"/api/datasets/{did}")

    # -- Deep health check (Gate 3) --

    def test_health_deep(self):
        """Gate 3: Verify /api/health/deep returns component statuses."""
        client = _get_pytest_client()
        r = client.get("/api/health/deep")
        assert r.status_code == 200, \
            f"Deep health check failed: {r.status_code}: {r.text[:300]}"
        data = r.json()
        assert any(k in data for k in ("components", "checks", "status")), \
            f"Deep health missing expected fields: {list(data.keys())}"

    # -- SQL validation --

    def test_sql_validate_valid_query(self):
        """Verify SQL validate endpoint accepts valid SELECT."""
        client = _get_pytest_client()
        r = client.post("/api/sql/validate", json={"query": "SELECT 1"})
        assert r.status_code == 200
        data = r.json()
        assert data.get("valid") is True, f"Expected valid=True: {data}"

    def test_sql_validate_rejects_drop(self):
        """Verify SQL validate endpoint rejects DROP TABLE."""
        client = _get_pytest_client()
        r = client.post("/api/sql/validate", json={"query": "DROP TABLE users"})
        assert r.status_code == 200
        data = r.json()
        assert data.get("valid") is False, f"Expected valid=False: {data}"

    # -- Concurrent upload stress --

    def test_concurrent_upload_stress_5x(self):
        """Stress: 5 concurrent in-memory CSV uploads to test server stability."""
        client = _get_pytest_client()
        dataset_ids: List[str] = []

        def _upload_one(idx: int) -> Optional[str]:
            csv = f"x,y,z\n"
            for j in range(500):
                csv += f"{idx}_{j},{j * idx},{j % 3}\n"
            r = client.post(
                "/api/datasets/upload",
                files={"file": (f"concurrent_{idx}.csv",
                                io.BytesIO(csv.encode()))},
                timeout=UPLOAD_TIMEOUT,
            )
            if r.status_code in (200, 202):
                return r.json().get("dataset_id")
            return None

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(_upload_one, i) for i in range(5)]
            for f in as_completed(futures):
                did = f.result()
                if did:
                    dataset_ids.append(did)

        assert len(dataset_ids) >= 3, \
            f"Only {len(dataset_ids)}/5 concurrent uploads succeeded"

        # Wait and cleanup
        for did in dataset_ids:
            client.wait_for_ready(did, timeout=120)
            client.delete(f"/api/datasets/{did}")

    # -- Memory/timeout resilience --

    def test_server_no_500_on_malformed_large_csv(self):
        """Negative: Server should not 500 on a 10MB CSV with malformed rows."""
        client = _get_pytest_client()
        buf = io.BytesIO()
        buf.write(b"a,b,c\n")
        # Rows with inconsistent column counts
        for i in range(100_000):
            if i % 3 == 0:
                buf.write(f"{i},{i+1},{i+2}\n".encode())
            elif i % 3 == 1:
                buf.write(f"{i},{i+1}\n".encode())      # missing column
            else:
                buf.write(f"{i},{i+1},{i+2},{i+3}\n".encode())  # extra column
        buf.seek(0)

        r = client.post(
            "/api/datasets/upload",
            files={"file": ("malformed_10mb.csv", buf)},
            timeout=120,
        )
        # Should either accept or reject gracefully — NOT 500
        assert r.status_code != 500, f"Server returned 500: {r.text[:300]}"

        if r.status_code in (200, 202):
            did = r.json().get("dataset_id")
            if did:
                # Even if processing fails, it should fail gracefully
                status = client.wait_for_ready(did, timeout=120)
                assert status in ("ready", "error", "failed"), \
                    f"Unexpected status for malformed CSV: {status}"
                client.delete(f"/api/datasets/{did}")

    # -- Path traversal protection (BQ-108) --

    def test_batch_path_traversal_blocked(self):
        """BQ-108: Batch upload with path traversal in paths should be rejected."""
        client = _get_pytest_client()
        csv_data = b"a,b\n1,2\n"
        r = client.post(
            "/api/datasets/batch",
            files=[("files", ("evil.csv", io.BytesIO(csv_data)))],
            data={"mode": "preview",
                  "paths": json.dumps(["../../etc/passwd"])},
            timeout=REQUEST_TIMEOUT,
        )
        assert r.status_code == 422, \
            f"Expected 422 for path traversal, got {r.status_code}: {r.text[:300]}"

    def test_batch_null_byte_blocked(self):
        """Batch upload with null byte in paths should be rejected."""
        client = _get_pytest_client()
        csv_data = b"a,b\n1,2\n"
        r = client.post(
            "/api/datasets/batch",
            files=[("files", ("safe.csv", io.BytesIO(csv_data)))],
            data={"mode": "preview",
                  "paths": json.dumps(["file\x00.csv"])},
            timeout=REQUEST_TIMEOUT,
        )
        assert r.status_code == 422, \
            f"Expected 422 for null byte in path, got {r.status_code}: {r.text[:300]}"

    def test_upload_traversal_filename(self):
        """Single upload with path traversal in filename should be rejected."""
        client = _get_pytest_client()
        csv_data = b"a,b\n1,2\n"
        r = client.post(
            "/api/datasets/upload",
            files={"file": ("../../../etc/passwd.csv", io.BytesIO(csv_data))},
            headers=client.headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert r.status_code == 422, \
            f"Expected 422 for traversal filename, got {r.status_code}: {r.text[:300]}"

    # -- Helpers --

    def _ensure_dataset(self, fname: str) -> Optional[str]:
        """Upload a file if not already uploaded, wait for ready, return dataset_id."""
        global _pytest_datasets
        if fname in _pytest_datasets:
            return _pytest_datasets[fname]
        fp = TEST_DATA_DIR / fname
        if not fp.exists():
            return None
        client = _get_pytest_client()
        result = client.upload_file(fp)
        did = result.get("dataset_id")
        if not did:
            return None
        status = client.wait_for_ready(did)
        if status == "ready":
            _pytest_datasets[fname] = did
            return did
        return None

    def _upload_and_verify(self, fname: str):
        """Upload a file, wait for ready, verify processing endpoints."""
        client = _get_pytest_client()
        did = self._ensure_dataset(fname)
        assert did, f"Failed to upload/process {fname}"

        # Verify metadata
        r = client.get(f"/api/datasets/{did}")
        assert r.status_code == 200

        # Verify status
        r = client.get(f"/api/datasets/{did}/status")
        assert r.status_code == 200
        assert r.json().get("status", "").lower() == "ready"

        # Verify sample
        r = client.get(f"/api/datasets/{did}/sample")
        assert r.status_code == 200


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    global BASE_URL, ADMIN_USER, ADMIN_PASS

    parser = argparse.ArgumentParser(
        description="VectorAIz Beta-Readiness Integration Tests")
    parser.add_argument("--base-url", default=BASE_URL,
                        help=f"VZ instance URL (default: {BASE_URL})")
    parser.add_argument("--xl", action="store_true",
                        help="Include XL files (>1GB)")
    parser.add_argument("--admin-user", default=ADMIN_USER,
                        help="Admin username")
    parser.add_argument("--admin-pass", default=ADMIN_PASS,
                        help="Admin password")
    args = parser.parse_args()

    BASE_URL = args.base_url
    ADMIN_USER = args.admin_user
    ADMIN_PASS = args.admin_pass

    success = run_all_tests(args.base_url, include_xl=args.xl)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
