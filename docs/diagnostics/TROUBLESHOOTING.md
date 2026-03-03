# VectorAIz — Active Diagnostic Reference

**Last updated:** S206 (3 March 2026)
**Platform:** Mac Studio M3 Ultra (Titan-1) — ARM64 via Docker Desktop + QEMU
**Current version:** v1.20.11 (DuckDB thread-safety fix)

---

## 1. Active Issues

### RC#18: Unsupported Format Types Freeze Pipeline (CRITICAL)
- **Status:** BUILD DISPATCHED — CC task `1e7f0847`
- **Trigger:** Uploading RTF, ICS, VCF, ODT, EPUB, or other "Tika-powered" formats
- **Mechanism:** `DOCUMENT_TYPES` in `processing_service.py` includes 15+ types tagged "Tika-powered" but neither `StreamingDocumentProcessor` (PDF/DOCX/PPTX only), `LocalDocumentProcessor` (Unstructured, not installed), nor any fallback can process them. Files accepted at upload → fail at streaming → fail at in-memory fallback → error handling blocks the processing queue → `/api/datasets` hangs → **entire UI freezes**
- **Fix (in build):** 4-part: (1) Pre-validation at upload rejecting unsupported types, (2) Pure Python extractors for RTF/ICS/VCF/ODT/EPUB/EML/MBOX/XML, (3) Pipeline hardening so individual failures don't kill the queue, (4) Human-readable error messages
- **Workaround:** Restart container: `docker restart vectoraiz-monorepo-vectoraiz-1`

### RC#19: allAI LLM Status Shows "not_configured"
- **Status:** INVESTIGATING
- **Symptom:** `GET /api/allai/status` returns `"llm": {"provider": "not_configured", "model": "none"}`
- **Context:** Env vars correct. The "not_configured" status is stale — LLM is now provided via allAI (ai.market proxy). Status endpoint may not yet reflect the allAI provider migration.
- **Next step:** Test copilot chat directly to confirm Allie proxy works

### RC#15: No Re-Queue on Startup Recovery
- **Status:** UNFIXED (not blocking beta)
- **Fix:** Lifespan requeue after `recover_stuck_records()` in `app/main.py`

### RC#16: WorkerHandle Cleanup Not Guaranteed
- **Status:** FIX IN BUILD `1e7f0847`
- **Fix:** `try/finally` with idempotent `_cleanup()`

---

## 2. Recently Fixed

### RC#20: DuckDB Thread-Safety (v1.20.11, commit 758d58e)
- All 12 services converted from singleton to ephemeral context manager

### RC#21: Release Pipeline — GitHub Release Not Created (v1.20.11)
- Added `step5_create_release()` with `gh release create --latest`
- Commits: 21292ca, 037d439, 30228fc

### RC#17: iter_data() Pipe Deadlock (v1.20.8, build f3c8e439)
- Subprocess exit detection + timeout in `iter_data()`

### RC#14: Indexing Subprocess Hang — fixed by same mechanism as RC#17
### RC#13: Event Loop Deadlock — fixed with asyncio.to_thread()

---

## 3. Monitoring Quick Reference

```bash
# Health check
curl -s http://localhost:8080/api/health | python3 -m json.tool

# Container resources
docker stats vectoraiz-monorepo-vectoraiz-1

# Filtered logs
docker compose -f docker-compose.customer.yml logs -f vectoraiz 2>&1 | grep -E "error|failed|worker|timeout|MemoryError"

# Restart after freeze
docker restart vectoraiz-monorepo-vectoraiz-1

# Full reset
docker compose -f docker-compose.customer.yml down --volumes --rmi all
docker compose -f docker-compose.customer.yml pull
docker compose -f docker-compose.customer.yml up -d
```

---

## 4. Key Files

| File | Role |
|------|------|
| `app/services/processing_service.py` | DOCUMENT_TYPES, queue consumer, extraction routing |
| `app/services/streaming_processor.py` | StreamingDocumentProcessor (PDF/DOCX/PPTX only) |
| `app/services/format_extractors.py` | NEW — lightweight extractors for RTF/ICS/VCF/etc |
| `app/services/process_worker.py` | Subprocess + WorkerHandle + IPC |
| `app/services/allie_provider.py` | allAI proxy to ai.market |
| `app/services/llm_service.py` | RAG LLM provider (separate from Allie) |
| `app/routers/datasets.py` | Upload endpoint, ALLOWED_EXTENSIONS |
| `scripts/release.sh` | Release pipeline |

---

## 5. Root Cause Catalog

| # | Root Cause | Status |
|---|-----------|--------|
| 1-12 | Historical (in-memory loading, nginx, threads, BigInteger, etc.) | ALL FIXED/CLOSED |
| 13 | Sync processing blocks event loop | FIXED |
| 14 | Indexing subprocess hang | FIXED (v1.20.8) |
| 15 | No re-queue on startup recovery | UNFIXED (not blocking) |
| 16 | WorkerHandle cleanup not in finally | FIX IN BUILD |
| 17 | iter_data() pipe deadlock | FIXED (v1.20.8) |
| 18 | Unsupported formats freeze pipeline | **FIX IN BUILD** |
| 19 | allAI LLM shows not_configured | **INVESTIGATING** |
| 20 | DuckDB thread-safety (singleton) | FIXED (v1.20.11) |
| 21 | Release pipeline missing GitHub Releases | FIXED (v1.20.11) |
