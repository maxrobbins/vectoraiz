# MP R4 Review — BQ-AIM-DATA-S3-STS-FULFILLMENT-S711 Gate 1

Reviewed branch SHA: `33eb12237a8a1cdbb9ba2e2aa5cf2f27aeebcffd`
Compared against fork-point/base: `cd8bb5dc4cc322635a96f78eb8a09ffbd7242e20`
R3 fold baseline: `e41086d`
vectoraiz predecessor checked: `cd8bb5dc`
ai-market-backend predecessor checked: `ef2e6d07`

Verdict: **APPROVE**

## Scope Verification

### Group 1 — R3 fold of AG R2 findings

Verified. The R3 fold remains intact:

- HIGH-1-R2 is addressed in §3.1 / §3.2 via onboarding-aware nullable `role_arn` and `external_id`, immutable configured rows, and the dedicated `POST /api/v1/s3-connections/{id}/finalize` transition. §2.3 Step 5 now calls `/finalize`, and C1(b) plus C4(g) carry the acceptance criteria.
- MEDIUM-2-R2 is addressed in §2.1 with a per-object size cap and disk-pressure pre-check; §3.1 adds `extraction_status`; C5(f)/(g) and §6 R9 carry implementation/testable requirements.
- LOW-3-R2 is addressed in §3.4 and §5 by defining and testing all four `session_purpose` families: `order-{order_id}`, `agent-qa-{listing_id}`, `scan-{scan_job_id}`, and `probe-{connection_id}`. §2.1 and §2.3 narratives use the scan/probe purposes, and C2(d) carries the broker acceptance criteria.

### Group 2 — R3.5 Block A path normalization

Verified. Grep returned zero hits in `specs/BQ-AIM-DATA-S3-STS-FULFILLMENT-S711-GATE1.md` for the forbidden substrings:

`vectoraiz-frontend`, `vectoraiz/app/`, `vectoraiz/tests/`, `src/pages/`, `src/components/`, `tests/services/`, `tests/routers/`, and `app/models/listing.py`.

§2.4 cites `components/PresignedUrlDownloader.tsx`. C8 is headed `Frontends (vectoraiz-monorepo frontend + ai-market-frontend)`, with the vectorAIz frontend subdir deferred to Gate 2 chunk-dispatch verification. C9 integration test paths use `tests/integration/` without a `vectoraiz/` prefix.

### Group 3 — R3.5 Block B raw_metadata files[] convention

Verified. §2.2, §3.3, §3.4, C6(b), §8, and §9 consistently apply Max's locked S712 17:18 UTC decision:

- `Listing.raw_metadata` reuses the existing `files[]` carrier convention from `app/schemas/raw_delivery.py`.
- Per-file `files[].path` carries `s3://{bucket}/{key}`.
- A top-level `s3_connection` metadata object carries `connection_id`, `vectoraiz_instance_url`, `bucket`, and `region`.
- The purchase flow reads `s3_connection.*` for the seller vectorAIz gatekeeper target and parses object keys from `files[].path`.
- The raw-delivery validator extension rejects S3 paths when `s3_connection` is null.
- `role_arn` and `external_id` remain excluded from all platform-side raw metadata, order snapshots, token payloads, logs, and DB tables.

## Citation Audit

Citation anchors were checked against the requested predecessor SHAs:

- vectoraiz-monorepo `cd8bb5dc`: sampled anchors still match, including `app/models/database_connection.py:18`, `app/routers/database.py:133-223`, `app/services/db_connector.py:108`, and `app/services/processing_service.py:294`.
- ai-market-backend `ef2e6d07`: special-audit anchors match. `app/schemas/raw_delivery.py` exists and defines `RawMetadataFile` at line 6 plus `RawMetadata` at line 14, with the existing `files` validator at lines 17-22. `app/models/marketplace.py:37` is `class FulfillmentType(str, enum.Enum)`. `app/api/v1/endpoints/orders.py:214` calls `order_service.issue_download_token`; line 619 is `async def refresh_access`. `app/services/order_service.py:147` is `async def create_order`; line 599 calls `create_delivery_token`. `app/core/security.py:188` is `def create_delivery_token`.

## Findings

No R4 findings. All three AG R2 findings remain correctly addressed, and R3.5 Block A plus Block B landed cleanly without introducing a new substantive issue.
