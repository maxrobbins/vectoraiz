# BQ-VZ-DATA-CHANNEL — AIM Data Channel for vectorAIz

**Status:** Gate 1 — Needs Determination
**Author:** Vulcan (S415)
**Estimate:** 40–60h
**Repo:** aidotmarket/vectoraiz (monorepo)

---

## 1. Problem Statement

vectorAIz currently serves two personas via the channel system (`direct` and `marketplace`). A third persona — the **data seller** — needs a purpose-built experience focused on getting data listed and sold on ai.market. This is the "AIM Data" product: **same codebase, same Docker image, different channel configuration**.

Key gap: vectorAIz currently mandates vectorization for all ingested data. Data sellers often have raw files (images, audio, PDFs, CSVs, proprietary formats) that should be listable on ai.market without mandatory embedding. The current pipeline forces all data through chunking → embedding → Qdrant, which is unnecessary and counterproductive for raw file listings.

## 2. Solution: `aim-data` Channel

### 2.1 Architecture Decision

**One codebase, one Docker image, channel-driven menu configuration.** NOT a separate product, repo, or Docker image. `VECTORAIZ_CHANNEL=aim-data` activates the seller-oriented experience.

This follows the existing pattern established by BQ-VZ-CHANNEL (direct/marketplace). The `docker-compose.aim-data.yml` already exists with the correct service name and image reference.

### 2.2 Channel Behavior

| Aspect | `direct` | `marketplace` | `aim-data` |
|--------|----------|---------------|------------|
| Primary persona | Data engineer | Data seller (from ai.market) | Data seller (standalone) |
| Primary flow | Ingest → Vectorize → Query | Upload → Enrich → Publish | Upload → Manage → Publish |
| Vectorization | Required | Required | Optional |
| allAI focus | Data processing copilot | Listing creation copilot | Data management + publishing copilot |
| Default landing | Dashboard | ai.market page | My Data |

### 2.3 Menu Order (`aim-data` channel)

**Primary section (seller workflow):**
1. My Data (dashboard of uploaded files/datasets)
2. Upload (drag-drop any format)
3. Manage (metadata editing, preview, organization)
4. Publish (ai.market listing wizard)

**Secondary section (optional processing):**
5. Vectorize (opt-in embedding for RAG-enabled listings)
6. Search (semantic search over vectorized data)
7. SQL Query
8. allAI Chat

**Bottom section:**
9. Earnings
10. Settings
11. Billing

### 2.4 Route Mapping

Some menu items map to existing routes with different labels:
- "My Data" → `/datasets` (relabeled)
- "Upload" → `/datasets` with upload modal auto-open, or new `/upload` route
- "Manage" → `/datasets/:id` (relabeled detail view)
- "Publish" → `/ai-market` (relabeled)
- "Vectorize" → existing vectorization flow, but now opt-in

New routes needed: potentially `/upload` as a dedicated page (TBD in Gate 2).

## 3. Backend Changes

### 3.1 Channel Config Extension

Add `aim_data` to `ChannelType` enum in `app/core/channel_config.py`. Parse `VECTORAIZ_CHANNEL=aim-data` → `ChannelType.aim_data`. Add channel prompt templates.

### 3.2 Optional Vectorization Pipeline

**Current:** All datasets go through: upload → parse → chunk → embed → store in Qdrant.
**New:** Datasets can exist in two states:
- **Raw**: File uploaded, metadata extracted, no vectorization. Listable on ai.market as `direct_download` or `api_access` fulfillment.
- **Vectorized**: Full pipeline completed. Listable as `ai_queryable` fulfillment.

Implementation approach:
- Add `vectorization_status` field to dataset model: `none | pending | processing | complete | failed`
- Upload endpoint stores raw file + extracts basic metadata (filename, size, format, row count if tabular)
- Vectorization becomes an explicit user action ("Vectorize this dataset") rather than automatic
- Existing pipeline remains unchanged; only the trigger changes from automatic to manual
- Datasets with `vectorization_status=none` are still fully functional for: metadata editing, preview, marketplace publishing (as raw/download listings), export

### 3.3 allAI Metadata Extraction

For non-tabular formats (images, audio, PDFs, proprietary), use allAI to extract structured metadata:
- PDF: title, author, page count, topic extraction, language detection
- Images: dimensions, format, EXIF data, allAI-generated description
- Audio: duration, format, sample rate, allAI-generated transcription summary
- Generic: file size, MIME type, allAI-generated description from filename + any extractable text

This powers the listing enrichment wizard — sellers get pre-populated metadata fields without manual entry.

### 3.4 Any-Format Acceptance

Both `aim-data` and other channels must accept ANY file format. Currently VZ has format-specific parsers (CSV, JSON, Parquet, PDF, etc.). For unsupported formats:
- Store the raw file
- Extract what metadata is possible (size, MIME type)
- Use allAI for description/categorization
- Allow manual metadata entry
- Support marketplace listing as `direct_download`

## 4. Frontend Changes

### 4.1 Channel-Aware Sidebar

Extend `Sidebar.tsx` with `NAV_ORDER_AIM_DATA` array and `SEPARATOR_INDEX_AIM_DATA`. Add new nav items for relabeled routes. No feature gating — channel is presentation-only (Condition C2 preserved).

### 4.2 useChannel Hook

Add `"aim-data"` to the `Channel` type union. Propagate through `ModeContext`.

### 4.3 Upload Experience

The `aim-data` channel should emphasize drag-and-drop simplicity:
- Large drop zone on the main dashboard
- Progress tracking with allAI metadata extraction status
- Post-upload: immediate preview + "Publish to ai.market" CTA
- Optional "Vectorize for AI queries" as secondary action

### 4.4 Dataset Detail View (Manage)

Enhanced for non-vectorized datasets:
- File preview (image thumbnails, PDF first page, audio player)
- Metadata editor (allAI-suggested fields, manual override)
- Listing readiness indicator (what's needed before publishing)
- "Vectorize" button (opt-in, shows what it enables)

## 5. Conditions & Constraints

- **C1:** Channel is presentation-only — no feature gating, auth, or billing changes (extends existing C2 from BQ-VZ-CHANNEL)
- **C2:** All existing `direct` and `marketplace` channel behavior unchanged
- **C3:** Vectorization pipeline untouched — only the trigger changes (auto → manual for aim-data)
- **C4:** No new Docker image — `aim-data` is a channel of the existing VZ image
- **C5:** `docker-compose.aim-data.yml` already exists and should be updated to set `VECTORAIZ_CHANNEL=aim-data`
- **C6:** allAI metadata extraction uses existing allAI infrastructure (proxied Gemini key), no new LLM integrations
- **C7:** Files without vectorization can be published to ai.market as `direct_download` fulfillment type

## 6. Slices (Proposed)

| Slice | Scope | Estimate | Dependencies |
|-------|-------|----------|-------------|
| A | Channel config: add `aim_data` to enum, prompts, frontend hook, sidebar order | 6-8h | None |
| B | Optional vectorization: `vectorization_status` field, manual trigger, upload without auto-vectorize | 12-16h | A |
| C | Any-format acceptance: raw file storage, basic metadata extraction, unsupported format handling | 8-12h | B |
| D | allAI metadata extraction: PDF/image/audio metadata, description generation | 8-10h | C |
| E | Frontend: upload UX, dataset detail view enhancements, listing readiness indicator | 8-12h | A, B |
| F | docker-compose.aim-data.yml update, installer integration, smoke tests | 4-6h | A-E |

**Total: 46-64h** (aligns with 40-60h estimate)

## 7. Risks

- **R1:** Optional vectorization may introduce inconsistent dataset states. Mitigation: clear status indicators in UI, allAI guidance.
- **R2:** allAI metadata extraction quality varies by format. Mitigation: always allow manual override, show confidence scores.
- **R3:** Existing tests assume vectorization is mandatory. Mitigation: Slice B includes test updates.

## 8. Success Criteria

1. `VECTORAIZ_CHANNEL=aim-data` activates seller-focused menu ordering
2. Files can be uploaded and listed on ai.market without vectorization
3. allAI extracts useful metadata from common non-tabular formats
4. Existing direct/marketplace channel behavior unaffected
5. docker-compose.aim-data.yml works end-to-end for a new user
