# BQ-VZ-LARGE-FILES — Streaming/Chunked Processing for Large Files
## Spec v1.1 (Incorporating Gate 1 Mandates)

### Problem
vectorAIz currently loads entire files into memory for processing. Files >500MB crash workers, >200MB cause significant latency and memory pressure. Enterprise customers routinely have multi-GB CSV/Parquet/PDF files. This is a beta blocker for any serious enterprise deployment.

### Current Architecture (What Changes)
- `processing_service.py` (849 lines): Loads files fully into memory, converts to DuckDB tables
- `document_service.py` (410 lines): PDF/DOCX/PPTX parsing via PyPDF/python-docx/python-pptx — all in-memory
- `indexing_service.py` (243 lines): Reads processed data, chunks text, sends to Qdrant
- `duckdb_service.py`: In-memory DuckDB for SQL queries on tabular data
- `text_processor.py` (73 lines): Text chunking for embeddings

### Gate 1 Council Mandates (10 items, unanimously agreed)

**M1: Process Isolation** (P0)
- File processing MUST run in a separate subprocess, NOT inside uvicorn workers
- Use `multiprocessing.Process` or `concurrent.futures.ProcessPoolExecutor`
- Worker crash must not take down the API server
- Memory limit per worker: configurable, default 2GB
- Timeout per file: configurable, default 30 minutes

**M2: Generator/Iterator Pattern** (P0)
- All processors must yield chunks, not return complete results
- `process_tabular()` → yields `RecordBatch` or `DataFrame` chunks
- `process_document()` → yields `TextBlock` chunks  
- Backpressure: if consumer is slow, producer pauses (bounded queue)

**M3: Incremental PyArrow ParquetWriter** (P1)
- Replace current temp-CSV pipeline with `pyarrow.parquet.ParquetWriter`
- Write row groups incrementally as chunks arrive
- No intermediate CSV materialization
- Target row group size: 64MB (configurable)

**M4: PyMuPDF for Large PDFs** (P0)
- Replace current PDF processing with PyMuPDF (fitz) for streaming page extraction
- Process page-by-page, never load full PDF into memory
- Extract text + tables per page, yield immediately
- Fallback to current PyPDF for simple text-only extraction if PyMuPDF unavailable

**M5: DuckDB Disk-Spill Configuration** (P1)
- Configure DuckDB with `SET temp_directory = '/tmp/vectoraiz-duckdb'`
- Set `SET memory_limit = '512MB'` (configurable)
- Enable disk-based spill for large SQL queries
- Clean up temp files on dataset deletion

**M6: Arrow-Based Parquet Sampling** (P1)
- Fix current Parquet preview to use PyArrow's `read_row_group(0)` instead of loading entire file
- For preview/sampling, read only first N rows via Arrow, not full file
- Metadata extraction via `parquet_file.metadata` without reading data

**M7: Progress Reporting** (P2)
- Emit progress events during processing (bytes processed / total bytes)
- Use existing WebSocket infrastructure for real-time updates
- SSE fallback for environments without WebSocket

**M8: Resumable Processing** (P2)
- Track last successfully processed chunk offset
- On crash/restart, resume from last checkpoint
- Store checkpoint in dataset metadata

**M9: Memory Monitoring** (P1)
- Track per-worker RSS memory usage
- Auto-kill worker if exceeds 2x configured limit
- Log memory high-water mark per file for capacity planning

**M10: Graceful Degradation** (P1)
- If streaming processor fails, fall back to current in-memory path for files <100MB
- Log degraded-mode usage for tracking
- Never silently fall back — always record in dataset metadata

### Architecture

```
Upload API (uvicorn)
    │
    ├─ Small files (<100MB): Direct in-process (existing path, unchanged)
    │
    └─ Large files (≥100MB): 
         │
         └─ ProcessPoolExecutor (max_workers=2)
              │
              ├─ Worker subprocess
              │    ├─ StreamingTabularProcessor (yields RecordBatch)
              │    ├─ StreamingDocumentProcessor (yields TextBlock)
              │    └─ Progress reporter (→ parent via pipe)
              │
              └─ Parent process
                   ├─ PyArrow ParquetWriter (incremental write)
                   ├─ Qdrant indexer (batch upsert per chunk)
                   └─ WebSocket progress relay
```

### Implementation Plan (Phased)

**Phase 1: Foundation (20h)** — M1 + M2 + M10
- New file: `app/services/streaming_processor.py`
  - `StreamingTabularProcessor`: CSV/TSV/JSON via chunked pandas reader, Parquet via row groups
  - `StreamingDocumentProcessor`: PDF page-by-page, DOCX paragraph-by-paragraph
  - Both implement `__iter__` yielding typed chunks
- New file: `app/services/process_worker.py`
  - Subprocess entry point
  - Memory limit enforcement via `resource.setrlimit()`
  - Communicates with parent via `multiprocessing.Queue`
- Modify: `processing_service.py`
  - Route to streaming path when `file_size >= LARGE_FILE_THRESHOLD`
  - Fallback to existing path for small files (M10)

**Phase 2: Optimized I/O (16h)** — M3 + M4 + M5 + M6
- PyArrow ParquetWriter integration in streaming pipeline
- PyMuPDF page-by-page PDF extraction
- DuckDB disk-spill configuration in `duckdb_service.py`
- Arrow-based Parquet sampling in `preview_service.py`

**Phase 3: Observability (12h)** — M7 + M8 + M9
- Progress events via existing WebSocket/SSE
- Checkpoint storage in dataset metadata
- Memory monitoring with auto-kill

**Phase 4: Testing (12h)**
- Unit tests for each streaming processor
- Integration tests with 500MB+ test files
- Memory profiling tests (verify RSS stays bounded)
- Crash recovery tests (kill worker mid-processing)
- Fallback path tests (streaming fails → in-memory succeeds)

### New Dependencies
- `pymupdf` (fitz) — PDF streaming extraction
- No other new dependencies (pyarrow already installed)

### Configuration (environment variables)
```
LARGE_FILE_THRESHOLD_MB=100        # Files above this use streaming
PROCESS_WORKER_MEMORY_LIMIT_MB=2048 # Per-worker memory cap
PROCESS_WORKER_TIMEOUT_S=1800       # 30 min per file
PROCESS_WORKER_MAX_CONCURRENT=2     # Max parallel workers
DUCKDB_MEMORY_LIMIT_MB=512          # DuckDB in-memory budget
DUCKDB_TEMP_DIR=/tmp/vectoraiz-duckdb
PARQUET_ROW_GROUP_SIZE_MB=64        # Target row group size
```

### Acceptance Criteria
1. 1GB CSV file processes without exceeding 2GB worker RSS
2. 500MB Parquet file processes using only 2 row groups in memory at a time
3. 200-page PDF processes page-by-page (memory flat, not growing)
4. Worker crash does not affect API server health
5. Files <100MB use existing path unchanged (regression-safe)
6. Progress events emitted every 5 seconds during large file processing
7. DuckDB queries on large datasets spill to disk instead of OOM
8. Preview/sampling for large Parquet reads only metadata + first row group

### Estimate
- **Total: 60 hours** (Council consensus from Gate 1)
- Phase 1: 20h, Phase 2: 16h, Phase 3: 12h, Phase 4: 12h
- Recommended: 4 build dispatches, each ~15h

### Risk Assessment
- **PyMuPDF licensing**: Check AGPL compatibility with customer deployment model
- **multiprocessing on Docker**: Verify `/dev/shm` sizing in customer Docker Compose
- **DuckDB temp storage**: Customer disk space must accommodate spill files
