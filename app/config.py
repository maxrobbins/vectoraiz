"""
vectorAIz Application Configuration
=====================================

PURPOSE:
    Pydantic-Settings based configuration for the vectorAIz backend.
    All settings can be overridden via environment variables (VECTORAIZ_ prefix).

UPDATED:
    S94 (2026-02-07) - BQ-066 Sub-task 1: Added SECRET_KEY with Fernet
        auto-generation for API key encryption at rest.
    S130 (2026-02-13) - BQ-127: Air-Gap Architecture — added VECTORAIZ_MODE,
        local auth secrets, connected fallback, premium feature flags.
"""

import logging
import os
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List, Optional, Literal
from cryptography.fernet import Fernet
import psutil

logger = logging.getLogger(__name__)

# BQ-127: Default ai.market URL used for mode inference
_DEFAULT_AI_MARKET_URL = "https://ai-market-backend-production.up.railway.app"


# ---------------------------------------------------------------------------
# VZ-PERF-P1: Dynamic resource detection
# ---------------------------------------------------------------------------
def _detect_cpu_workers() -> int:
    """Auto-detect concurrent workers: max(2, min(cores // 4, 8))."""
    try:
        cores = os.cpu_count() or 4
        return max(2, min(cores // 4, 8))
    except Exception:
        return 2


def _detect_worker_memory_mb() -> int:
    """Auto-detect per-worker memory: max(2048, min(total_ram // 8, 16384))."""
    try:
        total_mb = psutil.virtual_memory().total // (1024 * 1024)
        return max(2048, min(total_mb // 8, 16384))
    except Exception:
        return 2048


def _detect_duckdb_memory_mb() -> int:
    """Auto-detect DuckDB memory budget: max(512, min(total_ram // 4, 32768))."""
    try:
        total_mb = psutil.virtual_memory().total // (1024 * 1024)
        return max(512, min(total_mb // 4, 32768))
    except Exception:
        return 512


_DETECTED_CPU_WORKERS = _detect_cpu_workers()
_DETECTED_WORKER_MEM = _detect_worker_memory_mb()
_DETECTED_DUCKDB_MEM = _detect_duckdb_memory_mb()

logger.info(
    "VZ-PERF: detected resources — workers=%d, worker_mem=%dMB, duckdb_mem=%dMB",
    _DETECTED_CPU_WORKERS, _DETECTED_WORKER_MEM, _DETECTED_DUCKDB_MEM,
)


def _generate_fernet_key() -> str:
    """Generate a Fernet-compatible key for encryption at rest.

    WARNING: Auto-generated keys are ephemeral — they change on each restart.
    In production, set VECTORAIZ_SECRET_KEY env var to a persistent Fernet key.
    Generate one with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    """
    return Fernet.generate_key().decode()


class Settings(BaseSettings):
    """BQ-127: Settings now include operating mode and local auth configuration."""

    app_name: str = "vectorAIz"
    debug: bool = False  # S100: Default OFF for production safety

    # BQ-127: Operating mode — standalone (air-gapped) or connected (ai.market)
    mode: Literal["standalone", "connected"] = "standalone"

    # BQ-127: Connected fallback behavior (C4) — what happens when ai.market is unreachable
    connected_fallback: Literal["standalone", "refuse"] = "standalone"

    # BQ-127: Local auth secrets (C1 — separate from SECRET_KEY)
    apikey_hmac_secret: Optional[str] = None   # HMAC for local API key hashing
    local_auth_secret: Optional[str] = None    # JWT signing key (Phase 2, not used yet)

    # BQ-127: Premium feature flags (only relevant in connected mode)
    allai_enabled: bool = True
    marketplace_enabled: bool = True

    # ai.market platform integration
    ai_market_url: str = _DEFAULT_AI_MARKET_URL
    auth_enabled: bool = True  # S100: Default ON. Set VECTORAIZ_AUTH_ENABLED=false only for local dev.
    auth_cache_ttl: int = 300 # 5 minutes in seconds

    # Service-to-service auth (for internal endpoints on ai-market-backend)
    internal_api_key: Optional[str] = None
    
    # Encryption key for API keys at rest (BQ-066)
    # If not set, auto-generates a Fernet key.
    # WARNING: Auto-generated keys are ephemeral — encrypted data is lost on restart.
    # In production, always set VECTORAIZ_SECRET_KEY to a persistent Fernet key.
    secret_key: Optional[str] = None

    # BQ-125: Previous SECRET_KEY for dual-decrypt during key rotation.
    # Set VECTORAIZ_PREVIOUS_SECRET_KEY during transition period, remove after re-encryption.
    previous_secret_key: Optional[str] = None

    # BQ-102: Device identity keystore
    # Passphrase for encrypting private keys in the local keystore.
    # REQUIRED in production — startup will fail without it.
    keystore_passphrase: Optional[str] = None  # SecretStr-equivalent via env var
    # Path to keystore file — defaults to persistent data volume for Docker.
    keystore_path: str = "/data/keystore.json"

    # Co-Pilot metering (BQ-073)
    # Markup rate applied to Anthropic wholesale cost.
    # 2.0 = 200% of wholesale → e.g. $0.01 wholesale → $0.02 customer cost.
    copilot_markup_rate: float = 2.0
    # Minimum cost per query in cents (ensures even tiny queries incur a charge)
    copilot_min_cost_cents: int = 1
    # Estimated cost of an average Co-Pilot query in cents (for pre-flight checks)
    copilot_estimated_query_cost_cents: int = 3
    
    # DuckDB settings
    duckdb_threads: int = 8
    data_directory: str = "/data"
    allowed_raw_file_dirs: List[str] = Field(default_factory=list)
    
    # Upload settings
    upload_directory: str = "/data/uploads"
    processed_directory: str = "/data/processed"
    chunk_size: int = 1024 * 1024  # 1MB chunks for streaming
    raw_file_import_directory: str = "/data/import"
    raw_file_upload_max_size_mb: int = 500
    
    # Qdrant settings
    qdrant_host: str = "qdrant"
    qdrant_port: int = 6333
    
    # Document processing (optional premium)
    unstructured_api_key: Optional[str] = None

    # Stripe billing (BQ-098)
    stripe_secret_key: Optional[str] = None
    stripe_price_id: Optional[str] = None
    stripe_webhook_secret: Optional[str] = None
    billing_markup_rate: float = 2.0
    
    # Public URL for this vectorAIz instance (used in OpenAPI specs for Custom GPT Actions)
    public_url: str = "https://vectoraiz-backend-production.up.railway.app"

    # BQ-MCP-RAG: External LLM Connectivity (§4.5)
    connectivity_enabled: bool = False          # Off by default — customer must opt in
    connectivity_bind_host: str = "127.0.0.1"  # Loopback only by default
    connectivity_max_tokens: int = 10
    connectivity_rate_limit_rpm: int = 30       # Per-token requests/min
    connectivity_rate_limit_sql_rpm: int = 10   # Per-token SQL requests/min
    connectivity_rate_limit_global_rpm: int = 120
    connectivity_rate_limit_auth_fail: int = 5  # Auth failures/min per IP before block
    connectivity_max_concurrent: int = 3        # Per-token concurrency cap
    connectivity_sql_timeout_s: int = 10
    connectivity_sql_max_rows: int = 500
    connectivity_sql_memory_mb: int = 256
    connectivity_sql_max_length: int = 4096

    # BQ-VZ-LARGE-FILES: Streaming/chunked processing for large files
    large_file_threshold_mb: int = 50             # Files above this use streaming path
    fallback_max_size_mb: int = 200              # Max file size (MB) for in-memory fallback on streaming failure
    process_worker_memory_limit_mb: int = _DETECTED_WORKER_MEM  # Per-worker memory cap (auto-detected)
    process_worker_timeout_s: int = 300          # 5 min per file (indexing path doubles to 10 min)
    process_worker_grace_period_s: int = 60      # Seconds for checkpoint flush after SIGTERM
    process_worker_max_concurrent: int = _DETECTED_CPU_WORKERS  # Max parallel workers (auto-detected)
    duckdb_memory_limit_mb: int = _DETECTED_DUCKDB_MEM  # DuckDB in-memory budget (auto-detected)
    max_upload_size_gb: int = 1000               # Safety valve only — local app, disk is the real limit
    streaming_queue_maxsize: int = 32            # Backpressure queue depth
    streaming_batch_target_rows: int = 10000     # Target rows per RecordBatch
    parquet_row_group_size_mb: int = 64           # Target row group size for ParquetWriter

    # BQ-VZ-DB-CONNECT: Database extraction limits
    db_extract_max_rows: int = 5_000_000  # Max rows per extraction (M3)

    # BQ-VZ-SERIAL-CLIENT: Serial activation & metering
    serial: Optional[str] = None  # Device serial number for X-Serial header
    aimarket_url: str = _DEFAULT_AI_MARKET_URL  # ai-market serial authority base URL
    app_version: str = os.environ.get("VECTORAIZ_VERSION", "dev")
    serial_data_dir: str = "/data"  # Directory for serial.json + pending_usage.jsonl

    # BQ-VZ-HYBRID-SEARCH Phase 1A: Hybrid search pipeline config
    hybrid_search_mode: Literal["hybrid", "dense_only"] = "hybrid"
    hybrid_rrf_k: int = 60
    reranker_enabled: bool = True
    reranker_top_k: int = 30
    reranker_timeout_ms: int = 200
    fts_enabled: bool = True

    # CORS
    cors_origins: List[str] = ["http://localhost:5173", "http://localhost:3000", "http://localhost:8080", "https://vectoraiz-frontend-production.up.railway.app", "https://dev.vectoraiz.com", "https://vectoraiz.com", "https://www.vectoraiz.com", "https://vectoraiz-website-production.up.railway.app"]
    
    class Config:
        env_file = ".env"
        env_prefix = "VECTORAIZ_"

    def model_post_init(self, __context) -> None:
        if not self.allowed_raw_file_dirs:
            default_raw_dir = os.environ.get("VECTORAIZ_DATA_DIR") or self.data_directory
            self.allowed_raw_file_dirs = [default_raw_dir]

    @property
    def duckdb_memory_limit(self) -> str:
        """Derive DuckDB memory limit string from duckdb_memory_limit_mb."""
        mb = self.duckdb_memory_limit_mb
        if mb >= 1024 and mb % 1024 == 0:
            return f"{mb // 1024}GB"
        return f"{mb}MB"

    def get_secret_key(self) -> str:
        """Return the SECRET_KEY, auto-generating if not set.
        
        Uses Fernet.generate_key() for auto-generation so the key is always
        valid for Fernet encryption/decryption. Logs a warning when auto-generating
        since the key won't survive restarts.
        
        Returns:
            A Fernet-compatible key string.
        """
        if self.secret_key:
            return self.secret_key
        
        # Auto-generate and cache on instance
        logger.warning(
            "SECRET_KEY not set — auto-generating ephemeral Fernet key. "
            "Encrypted data will be LOST on restart. "
            "Set VECTORAIZ_SECRET_KEY in production: "
            "python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
        )
        self.secret_key = _generate_fernet_key()
        return self.secret_key


settings = Settings()

logger.info(
    "Resource detection: %d CPU cores, %.1f GB RAM -> %d workers @ %d MB, DuckDB %d MB, batch %d rows",
    os.cpu_count() or 0,
    psutil.virtual_memory().total / (1024**3),
    settings.process_worker_max_concurrent,
    settings.process_worker_memory_limit_mb,
    settings.duckdb_memory_limit_mb,
    settings.streaming_batch_target_rows,
)

# ---------------------------------------------------------------------------
# BQ-127: Mode inference for backward compatibility (C6)
# If VECTORAIZ_MODE is NOT explicitly set but VECTORAIZ_AI_MARKET_URL IS set
# to a non-default value, infer connected mode and log a deprecation warning.
# ---------------------------------------------------------------------------
import os as _os

if not _os.environ.get("VECTORAIZ_MODE") and _os.environ.get("VECTORAIZ_AI_MARKET_URL"):
    if settings.ai_market_url != _DEFAULT_AI_MARKET_URL:
        settings.mode = "connected"
        logger.warning(
            "VECTORAIZ_MODE not set but AI_MARKET_URL detected — defaulting to connected. "
            "Set VECTORAIZ_MODE=connected explicitly. This inference will be removed in v2.0."
        )

logger.info("vectorAIz operating mode: %s", settings.mode)
