# Changelog

All notable changes to vectorAIz are documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **Repo split (2026-05-27).** AIM Data has been forked out to a separate repository at [github.com/aidotmarket/aim-data](https://github.com/aidotmarket/aim-data). vectorAIz now hosts only the customer-hosted standalone product. Shared core platform code (allAI, RAG, indexing, search, copilot, billing, attestation, S3 STS connectors) continues to live here and is maintained as upstream. AIM Data syncs from this repo weekly.

### Removed
- AIM Data-only files pruned from this repo: buyer portal surface (routers, services, models, schemas, middleware, frontend pages), raw file listing surface (publish modal, detail pages, services), marketplace publish service, aim-data brand assets and release workflow, aim-data-specific specs and tests.
- DEAD candidates pruned: specs/BQ-VZ-LARGE-FILES-SPEC-v1.1.md.bak, req_temp.txt.

### Tagged
- `pre-split` tag marks the state of this repository immediately before the AIM Data fork. Use this tag to access the pre-split monorepo state if needed.

### Notes
- Pre-split commit history is preserved in this repo's git log (351 commits back to 2026-02-19; consolidation point at 596fdda on 2026-02-25).
- For AIM Data work going forward, see [github.com/aidotmarket/aim-data](https://github.com/aidotmarket/aim-data).
- Bug fixes to shared core code should land here first; AIM Data picks them up via weekly sync.

---

*Prior to 2026-05-27 this repository was a monorepo serving both vectorAIz and AIM Data. Release history before the split is in the git log, not separately catalogued.*
