# BQ-VZ-DATA-CHANNEL — Gate 2: Detailed Build Plan

**Author:** Vulcan (S415, R2 S416)
**Gate 1:** APPROVED_WITH_MANDATES (MP R2 + AG R1)
**Remaining Mandates:** MP-R2-M1 (DatasetDetail.tsx fan-out), AG-M1/M2 (docker-compose reconciliation)
**R2 Fixes:** Corrected endpoints (/api/raw/*), added frontend API client, fixed dependency graph, reworked metadata architecture, updated estimates

---

## Slice A: Channel Config (10-14h)

All changes are presentation-only. No feature gating.

### A1. Backend — `app/core/channel_config.py`

```python
# Add to ChannelType enum (line 18):
aim_data = "aim-data"

# Update parse_channel() to handle "aim-data":
elif raw == "aim-data":
    return ChannelType.aim_data

# Update warning message valid values list
```

### A2. Backend — `app/prompts/channel_prompts.py`

Add `ChannelType.aim_data` entries:

```python
CHANNEL_GREETINGS[ChannelType.aim_data] = (
    "Hi! I'm your data management copilot. I'll help you upload, "
    "organize, and publish your data on ai.market. What would you "
    "like to work on?"
)

CHANNEL_SYSTEM_CONTEXTS[ChannelType.aim_data] = (
    "The user is using vectorAIz as AIM Data — focused on uploading "
    "data files, managing metadata, and publishing listings to ai.market. "
    "Vectorization and RAG are optional enhancements, not required. "
    "Help with file upload, metadata editing, listing creation, and "
    "marketplace publishing. Mention vectorization as an option that "
    "enables AI-queryable listings when relevant."
)
```

### A3. Frontend — `frontend/src/hooks/useChannel.ts`

```typescript
export type Channel = "direct" | "marketplace" | "aim-data";
```

### A4. Frontend — `frontend/src/contexts/ModeContext.tsx`

Update `Channel` type (line 6) and channel parsing (line 44):
```typescript
type Channel = "direct" | "marketplace" | "aim-data";
// ...
setChannel(
  data.channel === "marketplace" ? "marketplace"
  : data.channel === "aim-data" ? "aim-data"
  : "direct"
);
```

### A5. Frontend — `frontend/src/components/layout/Sidebar.tsx`

Add nav order array and separator:
```typescript
const NAV_ORDER_AIM_DATA = [
  "/", "/datasets", "/ai-market", "/data-requests",
  "/search", "/sql", "/artifacts", "/databases",
  "/earnings", "/billing", "/data-types", "/settings",
];
const SEPARATOR_INDEX_AIM_DATA = 4;

// Update getOrderedItems():
function getOrderedItems(channel: Channel) {
  const order = channel === "marketplace" ? NAV_ORDER_MARKETPLACE
    : channel === "aim-data" ? NAV_ORDER_AIM_DATA
    : NAV_ORDER_DIRECT;
  const sepIdx = channel === "marketplace" ? SEPARATOR_INDEX_MARKETPLACE
    : channel === "aim-data" ? SEPARATOR_INDEX_AIM_DATA
    : SEPARATOR_INDEX_DIRECT;
  // ... rest unchanged
}
```

### A6. Frontend — `frontend/src/App.tsx`

Update `ChannelLanding` (line 74):
```typescript
const ChannelLanding = () => {
  const channel = useChannel();
  const target = channel === "marketplace" ? "/ai-market"
    : channel === "aim-data" ? "/datasets"
    : "/datasets";
  return <Navigate to={target} replace />;
};
```

### A7. Frontend — `frontend/src/components/onboarding/OnboardingWizard.tsx`

Add `AIM_DATA_STEPS` array with seller-oriented onboarding:
- Step 1: "Welcome to AIM Data" (explain data marketplace concept)
- Step 2: "Upload your first file" (any format accepted)
- Step 3: "Publish to ai.market" (listing wizard overview)

Update `getStepsForChannel()`:
```typescript
export function getStepsForChannel(channel: Channel): StepDef[] {
  return channel === "marketplace" ? MARKETPLACE_STEPS
    : channel === "aim-data" ? AIM_DATA_STEPS
    : DIRECT_STEPS;
}
```

### A8. Frontend — `frontend/src/contexts/CoPilotContext.tsx`

Update greeting selection (line 313) to handle `aim-data`. Ideally deduplicate by fetching from backend `/api/system/info` endpoint (which already returns channel info). If dedup is complex, add inline `aim-data` case:
```typescript
content: channel === "marketplace" ? "..." 
  : channel === "aim-data" ? "Hi! I'm your data management copilot..."
  : "..."
```

### A9. Frontend — `frontend/src/pages/DatasetDetail.tsx` (MP-R2-M1)

Update publish button logic (line 407-419) to treat `aim-data` as seller-oriented:
```typescript
<Button
  variant={channel === "marketplace" || channel === "aim-data" ? "default" : "ghost"}
  size="sm"
  onClick={() => setPublishModalOpen(true)}
  className={`gap-2${
    channel === "marketplace" || channel === "aim-data"
      ? " ring-2 ring-primary/30" : ""
  }`}
>
  <Upload className="w-4 h-4" />
  {channel === "marketplace" || channel === "aim-data"
    ? "Publish to ai.market" : "Publish"}
</Button>
```

### A10. Tests

| Test File | Changes |
|-----------|---------|
| `tests/test_channel_config.py` | Add `aim-data` parsing: valid value, env var, default fallback |
| `tests/test_channel_prompts.py` | Assert `aim_data` keys exist in CHANNEL_GREETINGS and CHANNEL_SYSTEM_CONTEXTS |
| `tests/test_channel_presentation_only.py` | Extend with `aim-data` channel — verify no feature gating |
| NEW: `tests/test_channel_sidebar_order.py` | Verify NAV_ORDER_AIM_DATA items and separator index |
| NEW: `tests/test_channel_onboarding.py` | Verify `getStepsForChannel("aim-data")` returns AIM_DATA_STEPS |
| NEW: `tests/test_channel_landing.py` | Verify `aim-data` landing redirects to `/datasets` |
| NEW: `tests/test_channel_dataset_detail.py` | Verify `aim-data` CTA shows "Publish to ai.market" with primary styling |

---

## Slice B: Upload UX Enhancement (8-10h)

Build on the existing raw file registration flow. No new models.

### B1. Dashboard Upload Drop Zone

Add a prominent drag-and-drop zone to the Dashboard page when channel is `aim-data`:
- Full-width card with dashed border, file icon, "Drop files here or click to browse"
- Accepts ANY file format (no extension filtering)
- On drop: calls `POST /api/raw/files` after saving to import directory
- Shows progress with allAI metadata extraction status (Slice C integration point)

### B2. Post-Upload Flow

After successful file registration:
1. Show file preview card (thumbnail/icon, filename, size, MIME type)
2. Primary CTA: "Create Listing" → opens raw listing creation form
3. Secondary CTA: "Vectorize for AI Queries" → routes to existing vectorization pipeline
4. Metadata panel: shows allAI-extracted metadata (populated async via Slice C)

### B3. Raw File List View

Enhance `/datasets` page for `aim-data` channel:
- Add "Raw Files" tab alongside "Datasets" tab
- Raw Files tab calls `listRawFiles()` from api.ts
- Each file card shows: filename, size, MIME type
- Listing status requires a joined query: add `GET /api/raw/files?include_listing_status=true` parameter that joins `raw_files` with `raw_listings` to return `listing_status` (draft/listed/none) per file. Alternatively, frontend fetches `listRawListings()` separately and joins client-side by `raw_file_id`.
- Click → detail view with metadata editor and publish CTA

### B4. Tests

- Upload drop zone renders when channel is `aim-data`
- File registration API call on drop
- Post-upload CTA routing
- Raw files tab visibility per channel

---

## Slice C: allAI Metadata Extraction (12-14h)

Extend `RawFileService` with async metadata extraction.

### C1. Metadata Extraction Interface

```python
# app/services/raw_file_metadata.py (NEW)

class MetadataExtractor:
    """Extract structured metadata from raw files using allAI."""

    async def extract(self, raw_file: RawFile) -> dict:
        """Dispatch to format-specific extractor, fall back to generic."""
        mime = raw_file.mime_type or ""
        if mime.startswith("image/"):
            return await self._extract_image(raw_file)
        elif mime == "application/pdf":
            return await self._extract_pdf(raw_file)
        elif mime.startswith("audio/"):
            return await self._extract_audio(raw_file)
        else:
            return await self._extract_generic(raw_file)
```

### C2. Format-Specific Extractors

**Image:** PIL/Pillow for dimensions, format, EXIF. allAI (Gemini) for description.
**PDF:** pypdf for page count, title, author. allAI for topic extraction.
**Audio:** mutagen for duration, format, sample rate. allAI for transcription summary (if <5min).
**Generic:** File size, MIME type. allAI for description from filename + any extractable text.

### C3. Integration with Existing Metadata Endpoint

The backend already has `POST /api/raw/files/{file_id}/metadata` which calls `RawFileService.generate_metadata()`. Current implementation is on-demand (user-triggered). Changes:

1. **Keep on-demand as primary path** — frontend calls `POST /api/raw/files/{file_id}/metadata` after upload completes
2. **Add optional auto-trigger** — after `register_file()` (which is sync), enqueue a background task via APScheduler to call `generate_metadata()` if channel is `aim-data` (presentation hint passed via header)
3. **Add `metadata` JSON column** to `RawFile` model — Alembic migration required
4. **Update `RawFileResponse` schema** — add `metadata: Optional[dict]` field
5. **Update `MetadataResponse`** — ensure it returns the full extracted metadata structure

Note: `register_file()` stays synchronous. Metadata extraction is a separate step, either on-demand or background-triggered.

### C4. allAI Prompt Templates

Use existing allAI infrastructure (Gemini via `google-genai` SDK). Prompts:
- Image: "Describe this image for a data marketplace listing. Include: subject, content type, potential use cases."
- PDF: "Summarize this document for a data marketplace listing. Include: topic, key content, potential use cases."
- Audio: "Summarize this audio content for a data marketplace listing."
- Generic: "Based on the filename '{filename}' and MIME type '{mime}', suggest a title, description, and tags for a data marketplace listing."

### C5. Tests

- Metadata extraction dispatch on file registration
- Format detection routing (image, PDF, audio, generic)
- Metadata stored in raw_file.metadata field
- allAI prompt construction (mock LLM calls)
- Migration creates metadata column

---

## Slice D: Dataset Detail Enhancements (8-10h)

### D1. Raw File Detail View

Create `/raw-files/:id` (frontend) → `GET /api/raw/files/{id}` (backend) detail page (or extend DatasetDetail):
- File preview: image thumbnail, PDF first page (pdf.js), audio player, generic file icon
- Metadata editor: form populated with allAI-extracted metadata, all fields editable
- Listing status: badge showing draft/listed/none
- Actions: "Create Listing", "Vectorize", "Download", "Delete"

### D2. Listing Readiness Indicator

Show checklist on raw file detail:
- ✅ File registered
- ✅/❌ Metadata complete (title, description, tags)
- ✅/❌ Price set (if paid listing)
- ✅/❌ Connected to ai.market (trust channel active)

### D3. Vectorize CTA

"Vectorize for AI Queries" button:
- Explains what vectorization enables (semantic search, RAG, ai_queryable fulfillment)
- Routes to existing dataset creation flow with raw file as source
- Post-vectorization: listing can be upgraded from direct_download to ai_queryable

### D4. Tests

- Raw file detail page renders correctly
- Metadata editor saves changes
- Listing readiness checklist logic
- Vectorize CTA routing

---

## Slice E: Deployment Reconciliation (6-8h)

### E1. docker-compose.aim-data.yml Refactor

Replace:
```yaml
# OLD
aim-data:
  image: ghcr.io/aidotmarket/aim-data:${AIM_DATA_VERSION:-latest}
  environment:
    - AIM_DATA_VERSION=...
    - AIM_DATA_MODE=...
    - AIM_DATA_MARKETPLACE_ENABLED=...
```

With:
```yaml
# NEW
vectoraiz:
  image: ghcr.io/aidotmarket/vectoraiz:${VECTORAIZ_VERSION:-latest}
  environment:
    - VECTORAIZ_CHANNEL=aim-data
    - VECTORAIZ_VERSION=${VECTORAIZ_VERSION:-latest}
    - VECTORAIZ_MODE=${VECTORAIZ_MODE:-standalone}
    - VECTORAIZ_SECRET_KEY=${VECTORAIZ_SECRET_KEY:-}
```

Preserve volume mounts, port mapping, and Qdrant/Postgres service definitions.

### E2. Installer Updates

Update `installers/` scripts to use `VECTORAIZ_CHANNEL=aim-data` instead of separate AIM Data image references. Update `install.sh` at repo root if it has AIM Data specific paths.

### E3. Remove Separate AIM Data CI

No `ghcr.io/aidotmarket/aim-data` build workflow needed. AIM Data deploys the standard vectoraiz image. Remove or update any GHA workflows referencing the separate image.

### E4. Smoke Tests

- `docker-compose.aim-data.yml` starts successfully
- `GET /api/system/info` returns `channel: "aim-data"`
- Sidebar order matches NAV_ORDER_AIM_DATA
- allAI greeting matches aim-data prompt

### E5. Tests

- docker-compose validation (image, env vars, services)
- Channel health endpoint returns correct value
- End-to-end smoke: start → onboard → upload → create listing

---

## Build Order & Dependencies

```
Slice A (channel config) ──┬── Slice B (upload UX) ── Slice C (allAI metadata) ── Slice D (detail enhancements)
                           └── Slice E (deployment)
```

Slice A is the foundation. B requires A (channel context + frontend API client). C requires B (upload flow feeds metadata extraction). D requires C (detail view shows extracted metadata). E is parallel after A (deployment config only).

## Mandate Coverage

| Mandate | Resolution | Slice |
|---------|-----------|-------|
| MP-R2-M1: DatasetDetail.tsx | §A9 — aim-data treated as seller-oriented for CTA | A |
| AG-M1: docker-compose image | §E1 — replace ghcr.io/aim-data with ghcr.io/vectoraiz | E |
| AG-M2: feature gating env vars | §E1 — remove AIM_DATA_MODE/MARKETPLACE_ENABLED | E |

## Estimate Summary

| Slice | Estimate | Builder |
|-------|----------|---------|
| A | 10-14h | MP (split: backend 4h + frontend 6-10h) |
| B | 8-10h | MP or CC |
| C | 12-14h | MP or CC |
| D | 8-10h | MP or CC |
| E | 6-8h | MP |
| **Total** | **44-56h** | |
