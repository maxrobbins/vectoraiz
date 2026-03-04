from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
import faulthandler
import logging
import asyncio
import os
import sys
import threading

# Enable faulthandler for native crash tracebacks (all threads)
faulthandler.enable(file=sys.stderr, all_threads=True)

from app.config import settings

# BQ-127: Stock routers — always imported regardless of mode
from app.routers import health, datasets, search, sql, vectors, pii, docs, diagnostics, imports
from app.routers import auth as auth_router_module
from app.auth.api_key_auth import get_current_user
from app.core.database import init_db, close_db
from app.core.structured_logging import setup_logging
from app.core.errors import VectorAIzError
from app.core.errors.registry import error_registry
from app.core.errors.middleware import vectoraiz_error_handler
from app.core.log_middleware import CorrelationMiddleware
from app.core.issue_tracker import issue_tracker
from app.core.resource_guards import resource_monitor_loop, ensure_log_fallback
from app.services.deduction_queue import deduction_queue
from app.services.serial_metering import (
    CreditExhaustedException,
    ActivationRequiredException,
    UnprovisionedException,
)

# BQ-127 (C5): Premium modules are NOT imported at module level.
# DeviceCrypto, register_with_marketplace, stripe_connect_proxy,
# allai, billing, integrations, webhooks are lazy-imported in connected mode only.

def _custom_thread_excepthook(args):
    """BQ-URGENT: Capture unhandled exceptions in background threads."""
    logger = logging.getLogger("vectoraiz.thread_crash")
    if issubclass(args.exc_type, MemoryError):
        logger.critical("Fatal MemoryError in background thread '%s'", args.thread.name if args.thread else 'unknown', exc_info=(args.exc_type, args.exc_value, args.exc_traceback))
    else:
        logger.error("Unhandled exception in background thread '%s'", args.thread.name if args.thread else 'unknown', exc_info=(args.exc_type, args.exc_value, args.exc_traceback))

threading.excepthook = _custom_thread_excepthook

# BQ-123A: Initialize structured logging before any logger calls
setup_logging()

logger = logging.getLogger(__name__)

# API metadata
API_TITLE = "vectorAIz API"
API_VERSION = os.environ.get("VECTORAIZ_VERSION", "dev")

# BQ-127 (§7): Mode-aware API descriptions
API_DESCRIPTION_STANDALONE = """
## vectorAIz - Data Processing & Semantic Search

Upload, process, vectorize, and search your data using your own LLM.
Runs entirely on your infrastructure with no internet required.

### Quick Start
1. Access the web interface at http://your-hostname
2. Complete the setup wizard (create admin account)
3. Upload data files
4. Configure your LLM provider (Settings > LLM)
5. Search and query your data

### Authentication

All data endpoints require authentication via API key.
Include in requests: `X-API-Key: vz_your_key_here`

### Premium Features
Set VECTORAIZ_MODE=connected to enable ai.market integration:
- allAI intelligent data assistant
- Premium data connectors
- Marketplace listing & discovery
"""

API_DESCRIPTION_CONNECTED = """
## vectorAIz - Data Processing & Semantic Search (Connected)

Full-featured mode with ai.market integration for premium features,
billing, and marketplace access.

### Authentication

All data endpoints require authentication via API key.
- Local keys: `X-API-Key: vz_your_key_here`
- Marketplace keys: `X-API-Key: aim_your_key_here`

### Additional Features
- allAI intelligent assistant for data exploration
- Premium data connectors
- List your data on ai.market for discovery and sale
- Usage-based billing via ai.market
"""

API_DESCRIPTION = (
    API_DESCRIPTION_CONNECTED if settings.mode == "connected"
    else API_DESCRIPTION_STANDALONE
)

# Tag metadata for organizing endpoints
TAGS_METADATA = [
    {
        "name": "health",
        "description": "Health check and readiness endpoints for monitoring. No authentication required.",
    },
    {
        "name": "datasets",
        "description": "Dataset upload, processing, and management. Supports CSV, JSON, Parquet, PDF, Word, Excel, and PowerPoint files. **Requires API Key.**",
    },
    {
        "name": "search",
        "description": "Semantic search using natural language queries. Powered by sentence-transformers embeddings and Qdrant vector database. This is a read-only, public endpoint.",
    },
    {
        "name": "allai",
        "description": "RAG (Retrieval-Augmented Generation) powered Q&A. Ask questions and get AI-generated answers grounded in your indexed datasets. **Requires API Key for generation.**",
    },
    {
        "name": "sql",
        "description": "SQL query interface for power users. Execute SELECT queries directly against your processed datasets. **Requires API Key.**",
    },
    {
        "name": "vectors",
        "description": "Vector database management. Create, inspect, and delete Qdrant collections. **Requires API Key.**",
    },
    {
        "name": "pii",
        "description": "PII (Personally Identifiable Information) detection using Microsoft Presidio. Scan datasets for sensitive data. **Requires API Key.**",
    },
    {
        "name": "documentation",
        "description": "API documentation, usage guides, and exportable collections. No authentication required.",
    },
    {
        "name": "External Connectivity",
        "description": "External LLM connectivity endpoints (MCP + REST). Authenticated via Bearer token (vzmcp_...). Allows external AI tools to search, query, and explore your datasets.",
    },
    {
        "name": "Connectivity Management",
        "description": "Internal management endpoints for the Settings > Connectivity page. Authenticated via session/API key.",
    },
]


async def _safe_background_task(name: str, coro):
    """Run a coroutine with error isolation — never let it crash the API."""
    try:
        await coro
    except asyncio.CancelledError:
        raise  # Allow cancellation to propagate for clean shutdown
    except MemoryError:
        logger.critical(
            "%s failed with MemoryError — task stopped but API continues serving", name,
        )
    except Exception:
        logger.exception(
            "%s failed — task stopped but API continues serving", name,
        )


async def queue_processor_loop():
    while True:
        processed = await deduction_queue.process_all_pending()
        logger.debug(f"Queue processor: processed {processed} items")
        await asyncio.sleep(30)  # every 30 seconds


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    Handles startup and shutdown events.
    """
    # Startup
    logger.info(
        "Starting vectorAIz API v%s in %s mode...",
        API_VERSION, settings.mode.upper(),
    )

    # BQ-123A: Load error registry + issue tracker
    error_registry.load()
    issue_tracker.reload()
    ensure_log_fallback()

    # BQ-116: Co-Pilot requires single-worker mode
    web_concurrency = int(os.environ.get("WEB_CONCURRENCY", "1"))
    uvicorn_workers = int(os.environ.get("UVICORN_WORKERS", "1"))
    if web_concurrency > 1 or uvicorn_workers > 1:
        logger.critical(
            f"Co-Pilot requires single-worker mode but "
            f"WEB_CONCURRENCY={web_concurrency}, UVICORN_WORKERS={uvicorn_workers}"
        )
        raise RuntimeError("Co-Pilot requires single-worker mode")

    # BQ-116: File lock to prevent multiple processes
    import fcntl
    _lock_path = "/var/tmp/vectoraiz_copilot.lock"
    _lock_file = open(_lock_path, "w")
    try:
        fcntl.flock(_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        logger.info("Acquired single-worker lock: %s", _lock_path)
    except OSError:
        _lock_file.close()
        logger.critical("Another vectoraiz process is already running (lock: %s)", _lock_path)
        raise RuntimeError("Co-Pilot requires single-worker mode")

    # BQ-110: Configure thread pool for run_sync() / asyncio.to_thread()
    executor = ThreadPoolExecutor(max_workers=4)
    loop = asyncio.get_running_loop()
    loop.set_default_executor(executor)
    logger.info("ThreadPoolExecutor configured (max_workers=4)")

    try:
        init_db()  # Initialize SQLite databases + run Alembic migrations
        logger.info("Database initialized")
    except Exception as e:
        logger.critical("Database initialization failed: %s", e, exc_info=True)

    # BQ-111: Auto-migrate datasets.json → SQL on first startup
    try:
        from app.scripts.migrate_json import migrate_datasets_json
        from app.core.database import get_engine
        migrate_datasets_json(get_engine())
    except Exception as e:
        logger.error("datasets.json migration error (will retry next startup): %s", e)

    # BQ-127: Device registration and marketplace connect only in connected mode
    if settings.mode == "connected":
        # BQ-102: Initialize device cryptographic identity
        from app.core.crypto import DeviceCrypto
        from app.services.registration_service import register_with_marketplace

        if settings.keystore_passphrase:
            try:
                crypto = DeviceCrypto(
                    keystore_path=settings.keystore_path,
                    passphrase=settings.keystore_passphrase,
                )
                crypto.get_or_create_keypairs()
                logger.info("Device keypairs initialized (Ed25519 + X25519)")

                # BQ-102 ST-3: Register with ai.market (non-blocking background task)
                async def _register_background():
                    try:
                        await register_with_marketplace(crypto)
                    except Exception as e:
                        logger.warning(f"Background registration failed: {e}")

                asyncio.create_task(_register_background())
            except Exception as e:
                logger.error(f"Failed to initialize device keypairs: {e}")
        else:
            logger.warning(
                "VECTORAIZ_KEYSTORE_PASSPHRASE not set — device keypair generation skipped. "
                "Set this env var to enable Trust Channel device registration."
            )
    else:
        logger.info("Standalone mode — skipping device registration and marketplace connect.")

    # BQ-D1: Trust Channel client + fulfillment service (connected mode only)
    trust_channel_task = None
    if settings.mode == "connected" and settings.internal_api_key:
        from app.services.trust_channel_client import get_trust_channel_client
        from app.services.fulfillment_service import get_fulfillment_service

        tc_client = get_trust_channel_client()
        get_fulfillment_service()  # Registers handler on creation
        trust_channel_task = asyncio.create_task(
            _safe_background_task("trust_channel", tc_client.run())
        )
        logger.info("BQ-D1: Trust Channel client + fulfillment handler started")
    elif settings.mode == "connected":
        logger.warning("BQ-D1: Trust Channel skipped — no VECTORAIZ_INTERNAL_API_KEY")

    # BQ-110: Start queue processor with cancellation support
    queue_task = asyncio.create_task(
        _safe_background_task("queue_processor", queue_processor_loop())
    )

    # BQ-123A: Start resource monitor (disk/memory checks every 60s)
    resource_task = asyncio.create_task(
        _safe_background_task("resource_monitor", resource_monitor_loop())
    )

    # BQ-ALLAI-FILES: Start chat attachment cleanup (every 10 min)
    async def _attachment_cleanup_loop():
        from app.services.chat_attachment_service import chat_attachment_service
        while True:
            await asyncio.sleep(600)
            chat_attachment_service.cleanup_expired()

    attachment_cleanup_task = asyncio.create_task(
        _safe_background_task("attachment_cleanup", _attachment_cleanup_loop())
    )

    # BQ-VZ-AUTO-UPDATE: Background update check (startup + every 6h)
    from app.services.update_service import background_update_check_loop
    update_check_task = asyncio.create_task(
        _safe_background_task("update_checker", background_update_check_loop())
    )

    # BQ-VZ-SERIAL-CLIENT: Serial activation lifecycle
    # Non-blocking — run in background so API starts serving immediately.
    # Previously this was `await _activation_mgr.startup()` which blocked
    # the lifespan for up to 34s on HTTP retries, making the API unresponsive.
    from app.services.activation_manager import get_activation_manager
    _activation_mgr = get_activation_manager()
    activation_task = asyncio.create_task(
        _safe_background_task("activation_manager", _activation_mgr.startup())
    )

    # BQ-VZ-LARGE-FILES: Recover records stuck in processing states (OOM crash recovery)
    try:
        from app.services.processing_service import get_processing_service
        _ps = get_processing_service()
        _recovered = _ps.recover_stuck_records()
        if _recovered:
            logger.info("Recovered %d stuck processing records on startup", _recovered)
    except Exception as e:
        logger.error("Failed to recover stuck records: %s", e)

    # BQ-VZ-QUEUE: File processing queue (concurrency=2)
    from app.services.processing_queue import get_processing_queue
    _processing_queue = get_processing_queue()
    processing_queue_tasks = _processing_queue.start(wrapper=_safe_background_task)

    # RC#22-F3: Re-queue files with status='uploaded' so crash-recovered records get processed
    try:
        from app.models.dataset import DatasetRecord as DBDatasetRecord
        from sqlmodel import select
        from app.core.database import get_engine
        from sqlmodel import Session

        with Session(get_engine()) as _session:
            _uploaded = _session.exec(
                select(DBDatasetRecord).where(DBDatasetRecord.status == "uploaded")
            ).all()
            for _rec in _uploaded:
                await _processing_queue.submit(_rec.id)
            if _uploaded:
                logger.info("Re-queued %d uploaded records for processing", len(_uploaded))
    except Exception as e:
        logger.error("Failed to re-queue uploaded records: %s", e)

    # Preload embedding model to avoid MemoryError during first indexing
    from app.services.embedding_service import get_embedding_service
    try:
        _emb_info = await asyncio.to_thread(get_embedding_service().preload)
        logger.info("Embedding model preloaded: %s", _emb_info)
    except Exception as e:
        logger.error("Embedding model preload failed (will retry on first use): %s", e)

    logger.info("API ready — all background tasks launched")

    yield

    # Shutdown
    logger.info("Shutting down vectorAIz API...")

    # BQ-123A: Persist issue tracker state
    issue_tracker.persist()

    # BQ-123A: Cancel resource monitor
    resource_task.cancel()
    try:
        await resource_task
    except asyncio.CancelledError:
        pass

    # BQ-ALLAI-FILES: Cancel attachment cleanup
    attachment_cleanup_task.cancel()
    try:
        await attachment_cleanup_task
    except asyncio.CancelledError:
        pass

    # BQ-VZ-AUTO-UPDATE: Cancel background update checker
    update_check_task.cancel()
    try:
        await update_check_task
    except asyncio.CancelledError:
        pass

    # BQ-VZ-SERIAL-CLIENT: Shutdown activation manager
    activation_task.cancel()
    try:
        await activation_task
    except asyncio.CancelledError:
        pass
    await _activation_mgr.shutdown()

    # BQ-VZ-QUEUE: Stop processing queue workers
    await _processing_queue.shutdown()

    # BQ-110: Cancel queue processor gracefully
    queue_task.cancel()
    try:
        await queue_task
    except asyncio.CancelledError:
        logger.info("Queue processor cancelled")

    # BQ-D1: Stop Trust Channel client
    if trust_channel_task is not None:
        from app.services.trust_channel_client import get_trust_channel_client
        await get_trust_channel_client().stop()
        trust_channel_task.cancel()
        try:
            await trust_channel_task
        except asyncio.CancelledError:
            pass
        logger.info("Trust Channel client stopped")

    # BQ-127: Only close stripe proxy in connected mode
    if settings.mode == "connected":
        from app.services.stripe_connect_proxy import close_proxy_client
        await close_proxy_client()
    close_db()
    executor.shutdown(wait=False)

    # BQ-116: Release single-worker lock
    try:
        import fcntl
        fcntl.flock(_lock_file, fcntl.LOCK_UN)
        _lock_file.close()
    except Exception:
        pass

    logger.info("Database connection closed")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    
    app = FastAPI(
        redirect_slashes=False,
        title=API_TITLE,
        description=API_DESCRIPTION,
        version=API_VERSION,
        openapi_tags=TAGS_METADATA,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,  # Use lifespan for startup/shutdown
        contact={
            "name": "AI.Market Support",
            "url": "https://ai.market/support",
            "email": "support@ai.market",
        },
        license_info={
            "name": "Proprietary",
            "url": "https://ai.market/license",
        },
    )
    
    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # BQ-123A: Correlation ID middleware (request_id + correlation_id in every log)
    app.add_middleware(CorrelationMiddleware)

    # BQ-123A: Structured error handler for VectorAIzError
    app.add_exception_handler(VectorAIzError, vectoraiz_error_handler)

    # BQ-VZ-SERIAL-CLIENT: Credit wall exception handlers
    @app.exception_handler(CreditExhaustedException)
    async def _credit_exhausted_handler(request: Request, exc: CreditExhaustedException):
        register_url = f"https://ai.market/register?serial={exc.serial}" if exc.serial else "https://ai.market/register"
        return JSONResponse(
            status_code=402,
            content={
                "error": "data_credits_exhausted" if exc.category == "data" else "setup_credits_exhausted",
                "message": f"You've used your free {exc.category} credits.",
                "setup_remaining_usd": exc.setup_remaining_usd,
                "data_remaining_usd": exc.remaining_usd,
                "payment_enabled": exc.payment_enabled,
                "register_url": register_url,
            },
        )

    @app.exception_handler(ActivationRequiredException)
    async def _activation_required_handler(request: Request, exc: ActivationRequiredException):
        return JSONResponse(
            status_code=403,
            content={"error": "activation_required", "message": str(exc)},
        )

    @app.exception_handler(UnprovisionedException)
    async def _unprovisioned_handler(request: Request, exc: UnprovisionedException):
        return JSONResponse(
            status_code=403,
            content={"error": "serial_required", "message": str(exc)},
        )

    # Catch-all handler so unhandled exceptions return JSON (not bare text)
    @app.exception_handler(Exception)
    async def _unhandled_exception_handler(request: Request, exc: Exception):
        logger.error("Unhandled exception on %s %s: %s", request.method, request.url.path, exc, exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal Server Error"},
        )

    # ------------------------------------------------------------------
    # BQ-VZ-MULTI-USER: Role-based route dependencies
    # ------------------------------------------------------------------
    from app.middleware.auth import require_admin, require_any

    # Backward compat: protected_route_dependency now uses require_any
    # so both admin and user roles can access (endpoints that need admin-only
    # use admin_route_dependency instead)
    any_user_dependency = [Depends(require_any)]
    admin_route_dependency = [Depends(require_admin)]

    # Legacy alias — still works for existing code that references it
    protected_route_dependency = any_user_dependency

    # ------------------------------------------------------------------
    # BQ-127: Register routers — stock (always) vs connected (conditional)
    # ------------------------------------------------------------------

    # PUBLIC — no auth required
    app.include_router(health.router, prefix="/api", tags=["health"])
    app.include_router(diagnostics.router, prefix="/api", tags=["diagnostics"])
    app.include_router(docs.router, prefix="/api/docs", tags=["documentation"])
    app.include_router(
        auth_router_module.router,
        prefix="/api/auth",
        tags=["auth"],
    )
    app.include_router(
        search.router,
        prefix="/api/search",
        tags=["search"],
    )

    # ADMIN + USER — datasets (GET = any, POST/PUT/DELETE = admin enforced per-endpoint)
    app.include_router(
        datasets.router,
        prefix="/api/datasets",
        tags=["datasets"],
        dependencies=any_user_dependency,
    )

    # ADMIN ONLY — upload/import/processing-related
    app.include_router(
        imports.router,
        prefix="/api/datasets/import",
        tags=["import"],
        dependencies=admin_route_dependency,
    )

    # ADMIN ONLY — vectors, sql, pii
    app.include_router(
        vectors.router,
        prefix="/api/vectors",
        tags=["vectors"],
        dependencies=admin_route_dependency,
    )
    app.include_router(
        sql.router,
        prefix="/api/sql",
        tags=["sql"],
        dependencies=admin_route_dependency,
    )
    app.include_router(
        pii.router,
        prefix="/api/pii",
        tags=["pii"],
        dependencies=admin_route_dependency,
    )

    # ADMIN ONLY — local directory import
    from app.routers.local_import import router as local_import_router
    app.include_router(
        local_import_router,
        prefix="/api/v1/import",
        tags=["import"],
        dependencies=admin_route_dependency,
    )

    # ADMIN ONLY — database connectivity
    from app.routers.database import router as database_router
    app.include_router(
        database_router,
        prefix="/api/v1/db",
        tags=["database"],
        dependencies=admin_route_dependency,
    )

    # ADMIN + USER — Co-Pilot (REST + WebSocket)
    from app.routers.copilot import router as copilot_rest_router, ws_router as copilot_ws_router
    app.include_router(
        copilot_rest_router,
        prefix="/api/copilot",
        tags=["copilot"],
        dependencies=any_user_dependency,
    )
    app.include_router(copilot_ws_router)  # WebSocket at /ws/copilot (no prefix, own auth)

    # ADMIN + USER — allAI features
    if settings.allai_enabled:
        from app.routers import allai

        app.include_router(
            allai.router,
            prefix="/api/allai",
            tags=["allai"],
            dependencies=any_user_dependency,
        )
        logger.info("allAI router mounted (allai_enabled=True)")

    # CONNECTED MODE — billing/marketplace
    if settings.mode == "connected":
        from app.routers import billing, integrations, webhooks

        app.include_router(
            webhooks.router,
            prefix="/api/webhooks",
            tags=["webhooks"],
        )
        app.include_router(
            billing.router,
            prefix="/api",
            tags=["billing", "api-keys"],
            dependencies=admin_route_dependency,
        )
        app.include_router(
            integrations.router,
            prefix="/api/integrations",
            tags=["integrations"],
            dependencies=admin_route_dependency,
        )
        logger.info("Connected mode: premium routers mounted (billing, integrations, webhooks)")
    elif not settings.allai_enabled:
        logger.info("Standalone mode: premium routers NOT mounted")

    # PUBLIC — website chat widget (no auth)
    from app.routers.website_chat import router as website_chat_router
    app.include_router(
        website_chat_router,
        prefix="/api/website-chat",
        tags=["website-chat"],
    )

    # ADMIN ONLY — connectivity management (Settings UI)
    from app.routers.connectivity_mgmt import router as connectivity_mgmt_router
    app.include_router(
        connectivity_mgmt_router,
        prefix="/api/connectivity",
        tags=["Connectivity Management"],
        dependencies=admin_route_dependency,
    )

    # ADMIN ONLY — feedback
    from app.routers.feedback import router as feedback_router
    app.include_router(
        feedback_router,
        prefix="/api",
        tags=["feedback"],
        dependencies=admin_route_dependency,
    )

    # PUBLIC — version check
    from app.routers.version import router as version_router
    app.include_router(
        version_router,
        prefix="/api",
        tags=["version"],
    )

    # ADMIN ONLY — tunnel management
    from app.routers.tunnel import router as tunnel_router
    app.include_router(
        tunnel_router,
        prefix="/api/tunnel",
        tags=["tunnel"],
        dependencies=admin_route_dependency,
    )

    # ADMIN + USER — notifications
    from app.routers.notifications import router as notifications_router
    app.include_router(
        notifications_router,
        prefix="/api/notifications",
        tags=["notifications"],
        dependencies=any_user_dependency,
    )

    # ADMIN ONLY — raw file listings for marketplace
    from app.routers.raw_listings import router as raw_listings_router
    app.include_router(
        raw_listings_router,
        prefix="/api/raw",
        tags=["raw-listings"],
        dependencies=admin_route_dependency,
    )

    # BQ-MCP-RAG: External LLM Connectivity — conditionally mount
    if settings.connectivity_enabled:
        from app.routers.ext import router as ext_router
        from app.routers.mcp import mount_mcp_sse

        app.include_router(ext_router, tags=["External Connectivity"])
        mount_mcp_sse(app)
        logger.info("BQ-MCP-RAG: External connectivity routers mounted (REST + MCP SSE)")
    else:
        logger.info("BQ-MCP-RAG: External connectivity disabled (CONNECTIVITY_ENABLED=false)")

    # Root endpoint
    @app.get("/", tags=["health"], summary="API Root", description="Returns basic API information and links to documentation.")
    async def root():
        return {
            "name": API_TITLE,
            "version": API_VERSION,
            "mode": settings.mode,
            "status": "running",
            "docs": {
                "swagger": "/docs",
                "redoc": "/redoc",
                "openapi": "/openapi.json",
                "postman": "/api/docs/postman",
                "guide": "/api/docs/guide",
            },
        }
    
    return app


# Create the app instance
app = create_app()
