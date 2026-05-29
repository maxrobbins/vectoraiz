# BQ-AIM-DATA-REMOVE-VECTORAIZ-FEATURES-S726

**Pillar:** AIM-Channel (vectorAIz shared codebase, AIM Data customer build)
**Type:** operational / customer-facing — product-definition (removals)
**Session:** 726 · **Branch:** chore/aim-data-remove-vectoraiz-features · base origin/main

## Intent
Remove vectorAIz-only surfaces from the AIM DATA build. These features do not
exist in AIM Data's product definition. Gate every removal on the aim-data
brand/channel (`getRuntimeBrandName()==="aim-data"` frontend,
`CHANNEL==ChannelType.aim_data` backend) so the vectorAIz build is UNCHANGED.
Where a whole section/page is vectorAIz-only, hide it for aim-data rather than
deleting shared code that vectorAIz still uses.

## Items (Max S726 list) → traced locations
1. **allAI Credits** (Settings) — `frontend/src/pages/SettingsPage.tsx:437`
   "Section: allAI Credits — moved to /billing". Remove for aim-data.
2. **API Keys management** (Settings) — `SettingsPage.tsx:531` "Local API Keys
   Management". Remove the management UI for aim-data. DO NOT touch backend
   internal X-API-Key AUTH (that's how aim-data authenticates to ai.market).
3. **Processing settings** — `SettingsPage.tsx:668` "Processing Settings".
   vectorAIz local-processing. Remove for aim-data.
4. **External Connectivity** — `SettingsPage.tsx:848` "External Connectivity" +
   backend `app/routers/connectivity_mgmt.py`. Hide section for aim-data; leave
   router (shared) but it's unreachable from aim-data UI.
5. **Shared Search Portal** — `SettingsPage.tsx:981` `<PortalSettingsSection/>`
   (component def ~1156-1557). Backend portal routers ALREADY removed (S725).
   Remove the residual settings section + component for aim-data.
6. **Billing tab** — sidebar `frontend/src/components/layout/Sidebar.tsx:40`
   `/billing`. No billing on AIM Data. Remove nav item + route for aim-data.
7. **Data Types** — sidebar `Sidebar.tsx:44` `/data-types`. vectorAIz remnant.
   Remove nav item + route for aim-data.
8. **Earnings** — sidebar `Sidebar.tsx:43` `/earnings` (feature:marketplace).
   Verify: AIM Data sellers earn via ai.market, NOT via this local earnings
   surface. If vectorAIz-only, remove for aim-data; if ambiguous, LEAVE and
   note it — do not guess.
9. **Search reframing** — sidebar `/search`; backend `app/routers/search.py`
   (semantic/vector/embedding). Search copy currently "semantic search across
   your datasets". AIM Data has NO semantic search. Reframe the aim-data search
   UI to a plain filename/metadata FILTER over the user's existing files (for
   users with many files). Change user-facing copy; for aim-data, the search
   page should call a simple list/filter, not the vector search endpoint. Do
   NOT delete the semantic endpoint (vectorAIz uses it).

## Out of scope
allAI S3-assist (#8 separate, pairs with S3 connector). VECTORAIZ_*→AIM_DATA_*
env rename (separate). Standalone-mode removal (already shipped this session).

## Acceptance
- aim-data build: Settings shows none of {allAI credits, API keys, processing,
  external connectivity, shared-search portal}. Sidebar shows no {Billing,
  Data Types}. Search is a filename/metadata filter with reworded copy.
- vectorAIz build: ALL of the above unchanged and present.
- `cd frontend && npm run build` clean. Backend `py_compile` + `import app.main`
  clean. State explicitly how vectorAIz parity was preserved per item.
- Report each item's file:line changes; flag #8 Earnings decision explicitly.
