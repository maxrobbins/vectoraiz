# BQ-AIM-DATA-S3-STS-FULFILLMENT-S711 — Gate 1 Spec v1

**Branch:** `spec/bq-aim-data-s3-sts-fulfillment-s711-gate1`
**Pillar:** AIM-Channel (primary) + ai.market + allAI
**Priority:** P0
**Predecessor:** BQ-AIM-DATA-S3-SOURCE-CONNECTOR-S684 (PARKED; surviving elements per stash `infra:worker-artifact-stash:S711.W:r3-t4-c-sts-surviving-element-extraction`)
**Authored by:** Mars Worker S711.W round 4 — 2026-05-26
**Predecessor reads:** vectoraiz-monorepo `cd8bb5dc` + ai-market-backend `ef2e6d07` on origin/main

---

## §1 Problem statement

The ai.market non-custodial-data-marketplace promise is that seller data never transits ai.market servers. Today AIM-Data covers AI-queryable listings (upload → DuckDB extraction → vector index) and raw-file listings (upload → local storage with tunneled gatekeeper delivery). Neither path serves the case where a seller's data already lives in their own S3 bucket and they don't want to upload it — Sergey's case, and the general data-owner case for sellers with bulk catalogued data on cloud object storage.

Two distinct delivery requirements need to be served:

1. **Buyer fulfillment.** At purchase time the buyer receives a short-lived URL that streams bytes directly from the seller's S3 bucket. ai.market servers never see the bytes.
2. **Agent QA access.** At listing-creation time, and on buyer questions later, ai-market's allAI agents need to read sample data from the seller's bucket for quality scoring, metadata enrichment, and answer-first response generation. Same non-custodial constraint: ai.market processes bytes only in-memory at scoring time; nothing is persisted.

Both requirements are solved by the same AWS primitive: **STS AssumeRole + presigned URLs**, but signing ownership is locked to the AIM Data customer's vectorAIz container. The seller creates IAM roles in their AWS account with two-policy structure (trust policy + permissions policy). vectorAIz AssumeRole-s into the seller's roles on demand and exposes a seller-side `/sign-url` gatekeeper for both (a) buyer-facing presigned URLs at purchase time and (b) read-only agent QA access. ai.market platform NEVER holds `role_arn` or `external_id`, never assumes the seller role, and never signs S3 URLs. Seller IAM access keys are NEVER created — only roles exist. CloudTrail in the seller's AWS account audits every vectorAIz AssumeRole event and S3 access.

The implementation spans nine chunks across four surfaces (vectorAIz, ai-market-backend, ai-market-frontend, allAI). The largest single chunk is a new allAI conversational onboarding agent that walks sellers through IAM role setup with diagnostic feedback on the four most common failure modes: ExternalId mismatch, MaxSessionDuration too low, insufficient bucket permissions, role-trust-policy drift.

## §2 Architecture overview + per-pillar surface

References cite source-file:line on origin/main as of HEAD `cd8bb5dc` (vectoraiz-monorepo) and `ef2e6d07` (ai-market-backend).

### §2.1 AIM-Channel — vectorAIz (seller's machine)

Components added:

- `app/models/s3_connection.py` — `S3Connection` SQLModel storing seller bucket pointer + role ARN + ExternalId. No secret material at rest. Mirrors connection-record pattern at `app/models/database_connection.py:18` `class DatabaseConnection` but replaces `password_encrypted: str = Field(sa_column=Column(Text))` with onboarding-aware `role_arn: Optional[str]` + `external_id: Optional[str]` constraints per §3.1.
- `app/models/s3_scan_job.py` — `S3ScanJob` tracks paginated bucket scan progress (continuation_token + status). Lighter than the predecessor `S3IngestJob` (no per-file download status).
- `app/models/s3_object_metadata.py` — `S3ObjectMetadata` (one row per S3 object). `dataset_id` FK lives on the per-object row, not on the scan job, per M7 cardinality mandate carried forward from BQ-S684 R3 fold.
- `app/services/sts_credential_broker.py` — STS AssumeRole + boto3 client cache. Mirrors `app/services/llm_providers/bedrock.py:40-75` (ai-market-backend) pattern verbatim: `sts_client = boto3.client("sts", ...)` → `assumed_role = sts_client.assume_role(RoleArn=..., RoleSessionName=...)` → `boto3.client("s3", aws_session_token=credentials["SessionToken"], ...)`. Per-seller RoleSessionName per locked-decision format.
- `app/services/s3_connector.py` — Reshaped from predecessor design (`app/services/db_connector.py:108` `class DatabaseConnector` 695 LoC pattern). Keeps paginated `list_objects_v2` + continuation_token persistence + boto3 client cache + single `threading.Lock` + `asyncio.to_thread` bridge (M8 mandate carries forward). Byte-download paths removed.
- `app/routers/s3.py` — REST router mirroring `app/routers/database.py:133-223` CRUD shape (`@router.post("/connections")` create + GET list + GET/{id} detail + DELETE + POST /test). Adds `POST /{id}/test-assume-role` (probes STS path without bucket access), `POST /{id}/scan` (kicks off async scan), `POST /{id}/sign-url` (gatekeeper endpoint — see §2.2 for invocation).

Data flow at scan time: vectorAIz cron or seller-triggered `POST /scan` → S3Connector calls STSCredentialBroker.get_or_create_client(connection_id) → STSCredentialBroker AssumeRole-s into seller's role with ExternalId from `S3Connection` → paginated `list_objects_v2` walks bucket → per object, `S3ObjectMetadata` row inserted → per batch, scan orchestrator invokes `processing_service.create_dataset` (`app/services/processing_service.py:294`) whose signature is `def create_dataset(self, original_filename: str, file_type: str) -> DatasetRecord` and which expects **local file** input at `self.upload_dir / safe_filename`.

Because C-STS does NOT download bytes for storage, the C5 scan orchestrator uses an **ephemeral-download adapter**: for each object that scan elects to extract metadata from, generate presigned URL using `session_purpose = "scan-{scan_job_id}"`, fetch object bytes into `self.upload_dir / safe_filename`, call `create_dataset`, persist `dataset_id` to the `S3ObjectMetadata` row, then immediately delete the local copy. Bytes touch the seller's local AIM-Data installation for at most one extraction cycle. Bytes NEVER leave the seller's machine; the buyer-fulfillment path is a separate pure pass-through.

Size cap + skip-and-flag: the C5 ephemeral-download adapter enforces a per-object size cap (default 2 GiB for v1). Objects exceeding the cap are recorded in `S3ObjectMetadata` with `extraction_status = "SKIPPED_OVERSIZED"` and `metadata_extracted_at = NULL`; no download is attempted. Before each eligible ephemeral download, the adapter checks free disk space at `upload_dir` and fail-fast skips objects with `extraction_status = "SKIPPED_DISK_PRESSURE"` when available space is below a configurable threshold (default 5 GiB). Buyer fulfillment for skipped objects is unaffected because buyer fulfillment uses presigned URLs directly against the seller's S3 bucket, not the extraction path. v2 may introduce streaming extraction via chunked partial reads with S3 Range requests for oversized objects.

### §2.2 ai.market — ai-market-backend (platform)

Components added/extended:

- `app/services/order_service.py:147` `async def create_order` — branched: when `listing_snapshot["fulfillment_type"] == FulfillmentType.SELLER_S3_PRESIGNED_URL.value`, the order-create path resolves the listing's marketplace metadata and calls the seller vectorAIz `/sign-url` gatekeeper with `connection_id`, `object_key`, and `session_purpose = "order-{order_id}"`. The returned S3 presigned URL is carried in the existing JWT delivery-token response under `delivery_config`.
- `app/api/v1/endpoints/orders.py:619` `@router.post("/{order_id}/refresh") async def refresh_access` — already wired in current code for vectorAIz fulfillment refresh; extended to call the seller vectorAIz `/sign-url` gatekeeper again for SELLER_S3_PRESIGNED_URL when buyer's URL has expired.

Schema delta:

- `app/models/marketplace.py:37` `class FulfillmentType(str, enum.Enum)` extended with `SELLER_S3_PRESIGNED_URL = "seller_s3_presigned_url"`.
- `app/schemas/listing.py:31` matching extension.
- `Listing.raw_metadata` JSON extension for S3 fulfillment listings REUSES the existing `files[]` carrier convention (per `app/schemas/raw_delivery.py`). Per-file `path` carries `s3://{bucket}/{key}` URI; `checksum_sha256` carries the S3 object ETag (or computed SHA256 if available at scan time). One new top-level optional field `s3_connection` of shape `{connection_id, vectoraiz_instance_url, bucket, region}` carries the seller's vectorAIz connection metadata; concrete field paths are `s3_connection.connection_id`, `s3_connection.vectoraiz_instance_url`, `s3_connection.bucket`, and `s3_connection.region`. `role_arn` and `external_id` are NEVER present on the platform side; they remain in the seller's vectorAIz `S3Connection`. Validator rule: if ANY `files[].path` starts with `s3://`, then `s3_connection` MUST be non-null.
- Alembic migration `ALTER TYPE fulfillment_type_enum ADD VALUE IF NOT EXISTS 'seller_s3_presigned_url'`. Note `create_type=False` at `app/models/marketplace.py:122` — the enum is managed externally by migration; PostgreSQL ALTER TYPE is non-transactional so migration runs alone in its own transaction.

Data flow at purchase: buyer completes Stripe checkout → `order_service.create_order(buyer_id, seller_id, listing_id, ..., listing_snapshot)` → branch on `fulfillment_type == SELLER_S3_PRESIGNED_URL` → resolve target S3 object: read `Listing.raw_metadata.s3_connection.connection_id`, `Listing.raw_metadata.s3_connection.vectoraiz_instance_url`, `Listing.raw_metadata.s3_connection.bucket`, and `Listing.raw_metadata.s3_connection.region` for the gatekeeper target; iterate `Listing.raw_metadata.files[]` and for each entry parse `path` (expected `s3://{bucket}/{key}`) to extract `object_key` → call seller vectorAIz `POST /api/v1/s3-connections/{connection_id}/sign-url` with `{object_key, expires_in_seconds: 900, session_purpose: "order-{order_id}"}` → vectorAIz STSCredentialBroker assumes the seller role internally and returns `{presigned_url, expires_at}` → ai-market returns the URL via the existing JWT delivery-token chain: `app/api/v1/endpoints/orders.py:214` calls `order_service.issue_download_token`, `app/services/order_service.py:599` calls `create_delivery_token`, and `app/core/security.py:188` defines `create_delivery_token`. The S3 URL is carried in the existing delivery-token response `delivery_config`; do not introduce a new S3-specific token type and do not conflate this JWT delivery-token path with the hash-based `FulfillmentDownloadToken` / BQ-D1 system. Buyer presents JWT to ai-market refresh endpoint when URL expires; refresh path calls the same seller vectorAIz `/sign-url` gatekeeper to mint a new URL.

Dual-path architecture: buyer fulfillment uses the seller-side fulfillment role with `session_purpose = "order-{order_id}"`; agent QA uses a separate read-only seller-side role exposed through the same vectorAIz `/sign-url` gatekeeper with `session_purpose = "agent-qa-{listing_id}"`. ai.market allAI agents receive only short-lived presigned URLs for read-only sample access; they never receive `role_arn`, `external_id`, or STS credentials.

Non-custodial proof chain: seller IAM user credentials are NEVER created. Seller roles trust only the seller's vectorAIz signing identity / local credential context with the seller-specific ExternalId held in vectorAIz. STS-derived credentials live in memory inside the seller's vectorAIz container, not in ai-market-backend. The presigned URL contains a non-reversible signature derived from the temporary credentials. Buyer receives only the URL. Seller's CloudTrail logs every AssumeRole event + every S3 GetObject with `userIdentity.arn: arn:aws:sts::SELLER:assumed-role/aim-data-fulfillment/aim-{seller_id}-{order_id}` for buyer fulfillment and `aim-{seller_id}-agent-qa-{listing_id}` for agent QA.

### §2.3 allAI — onboarding agent

New agent at `app/allai/agents/aim_data_onboarding/agent.py` subclassing `app/allai/base_agent.py:90` `BaseAgent`. Mirrors the existing 8-agent registry pattern (`matchmaker.py:55`, `marketing_ops.py:65`, `crm_steward.py:157`, etc., per `r3-t5-allai-onboarding-flow-predecessor-read`). Co-located tools:

- `iam_policy_templates.py` — generates trust policy JSON + permissions policy JSON with `{bucket_name}`, `{external_id}`, `{vectoraiz_signing_principal}` templated per seller. Trust policy enforces four required conditions: single Principal (the seller-side vectorAIz signing principal / local credential context), `sts:AssumeRole` action only, `StringEquals` ExternalId condition (confused-deputy mitigation per AWS docs), no wildcards.
- `sts_probe.py` — invokes the seller vectorAIz `test-assume-role` path to validate seller-provided role ARN + ExternalId match the trust policy. `role_arn` and `external_id` are posted to vectorAIz only and are never stored by ai.market.
- `diagnostics.py` — classifies AssumeRole error responses. AccessDenied → ExternalId mismatch OR wrong Principal OR insufficient permissions. ValidationError → MaxSessionDuration too low OR malformed role ARN. Each classification ships with a remediation snippet the seller can paste into the IAM console.

Walkthrough conversation flow: (1) Generate per-seller ExternalId following the locked format `aim-data-seller-{uuid}-{32hex}`. Persist to a pre-row `S3Connection` with `status = "onboarding"` in the seller's vectorAIz instance; `external_id` is stored on the pre-row and `role_arn` remains NULL until seller paste. (2) Generate trust + permissions policy JSON with seller's bucket name. Display via allAI chat with copy-to-clipboard buttons. (3) Wait for seller to paste role ARN into the vectorAIz-backed onboarding form. (4) Invoke `sts_probe` through vectorAIz against the pasted ARN using `session_purpose = "probe-{connection_id}"`; `role_arn` and `external_id` are posted to vectorAIz only for validation and are never stored by ai.market. (5) On success: call `POST /api/v1/s3-connections/{id}/finalize` with the validated `role_arn` + `external_id` so vectorAIz atomically persists credentials and transitions `status = "onboarding"` to `status = "configured"` after test-assume-role validation, then publish marketplace listing metadata to `Listing.raw_metadata` and transition to bucket browser. (6) On AccessDenied: `diagnostics.classify_assume_role_error` produces structured remediation (e.g., "Your role's trust policy is missing the ExternalId condition. Add this StringEquals block: ..."). Surface to seller. (7) Idempotent flow with checkpoint state — seller can walk away and resume from the pre-row.

### §2.4 ai-market-frontend — buyer download UX

Component added:

- `components/PresignedUrlDownloader.tsx` — when order has `fulfillment_type == SELLER_S3_PRESIGNED_URL`, render an `<a download href={presigned_url}>` element. The buyer's browser navigates directly to S3; no CORS configuration is required on the seller's bucket. On 403/expired URL: invoke the `app/api/v1/endpoints/orders.py:619` refresh endpoint, receive a fresh URL, retry the download.

## §3 Schema deltas + API contracts

### §3.1 vectorAIz schema (three new tables)

`S3Connection` (`app/models/s3_connection.py`): `id` (str primary key, 36 chars), `name` (str 255), `bucket` (str 255), `region` (str 64), `role_arn` (Optional[str], length <= 512, NULL allowed ONLY when `status = "onboarding"`), `external_id` (Optional[str], length <= 128, NULL allowed ONLY when `status = "onboarding"`), `prefix` (optional str 512), `status` (str 32, default "configured"), `error_message` (optional Text), `last_scanned_at` (optional datetime), `continuation_token` (optional Text), `created_at`, `updated_at`. No secret columns. Constraint: rows with `status = "configured"` MUST have non-NULL `role_arn` AND `external_id`; rows with `status = "onboarding"` MAY have NULL `role_arn` AND/OR `external_id`. Express this as a database CHECK constraint where supported by the target DB, with a matching app-layer validator for SQLite/test parity.

`S3ScanJob` (`app/models/s3_scan_job.py`): `id`, `connection_id` (FK), `status` (pending | running | completed | failed), `started_at`, `completed_at` (optional), `continuation_token` (optional), `error_message` (optional), `objects_enumerated` (int default 0).

`S3ObjectMetadata` (`app/models/s3_object_metadata.py`): `id`, `connection_id` (FK), `scan_job_id` (FK), `object_key` (str 1024), `size_bytes` (int), `content_type` (str 128), `last_modified` (datetime), `etag` (str 128), `dataset_id` (optional FK to `datasets.dataset_id` — per M7 cardinality on the per-object row), `metadata_extracted_at` (optional datetime), `extraction_status` (enum: EXTRACTED | SKIPPED_OVERSIZED | SKIPPED_DISK_PRESSURE | FAILED), `extraction_skip_reason` (optional Text).

Alembic migration `alembic/versions/<date>_001_s3_sts_connector_schema.py`: CREATE TABLE for all three; indexes on `(connection_id, object_key)`, `(connection_id, scan_job_id)`; FK cascades on connection delete.

### §3.2 vectorAIz REST API (new endpoints)

`POST /api/v1/s3-connections` — create. Body `{name, bucket, region, role_arn?, external_id?, prefix?, status?}`. Returns 201 with full record. `status = "onboarding"` permits NULL `role_arn` and/or `external_id`; `status = "configured"` requires both.
`GET /api/v1/s3-connections` — list seller's connections.
`GET /api/v1/s3-connections/{id}` — detail.
`PUT /api/v1/s3-connections/{id}` — update name/prefix only. Immutable: bucket, region, role_arn, external_id. For `status = "configured"` rows, `role_arn` and `external_id` remain immutable through PUT.
`DELETE /api/v1/s3-connections/{id}` — soft-delete (status → archived).
`POST /api/v1/s3-connections/{id}/test-assume-role` — probe path. Returns success or structured error.
`POST /api/v1/s3-connections/{id}/finalize` — onboarding finalization path. Body `{role_arn, external_id}`. Valid only when the current row has `status = "onboarding"`; vectorAIz runs test-assume-role validation before persisting, then atomically stores `role_arn` + `external_id` and transitions `status` from `onboarding` to `configured`. Rejects with 409 if the row is already `configured`.
`POST /api/v1/s3-connections/{id}/scan` — kick off async scan job. Returns `scan_job_id`.
`POST /api/v1/s3-connections/{id}/sign-url` — gatekeeper endpoint. Internal-only JWT-authenticated. Body `{object_key, expires_in_seconds?, session_purpose}` where `session_purpose` is `order-{order_id}` for buyer fulfillment or `agent-qa-{listing_id}` for agent QA. Returns `{presigned_url, expires_at}`.

### §3.3 ai-market-backend schema delta

- `app/models/marketplace.py:37` enum extension: add `SELLER_S3_PRESIGNED_URL = "seller_s3_presigned_url"`.
- `app/schemas/listing.py:31` mirror extension.
- `Listing.raw_metadata` for SELLER_S3_PRESIGNED_URL listings REUSES the existing `files[]` carrier convention from `app/schemas/raw_delivery.py` (`RawMetadataFile` + `RawMetadata`). Per-file `path` is `s3://{bucket}/{key}` URI scheme. New top-level optional field `s3_connection: Optional[S3ConnectionMetadata]` where `S3ConnectionMetadata = {connection_id: str, vectoraiz_instance_url: str, bucket: str, region: str}`; concrete field paths include `s3_connection.connection_id`, `s3_connection.vectoraiz_instance_url`, `s3_connection.bucket`, and `s3_connection.region`. Validator extension at `app/schemas/raw_delivery.py`: reject if any `files[].path` starts with `s3://` AND `s3_connection` is None. `role_arn` and `external_id` MUST NOT appear in `raw_metadata`, in order snapshots, in token payloads, in logs, or in platform DB tables.
- Alembic migration: `ALTER TYPE fulfillment_type_enum ADD VALUE IF NOT EXISTS 'seller_s3_presigned_url';` Run standalone (non-transactional).

### §3.4 ai-market-backend service signatures

No ai-market-backend STS broker is added for S3 fulfillment. Under Max's locked ownership decision, STS AssumeRole and S3 signing live in vectorAIz only.

vectorAIz `STSCredentialBroker` (`app/services/sts_credential_broker.py` NEW): `async def get_or_create_s3_client(connection_id: str, session_purpose: str) -> boto3.client`. Cache key `(role_arn, region, session_purpose)` after loading `role_arn`, `external_id`, and `region` from the local `S3Connection`. Refresh 2-min before STS session expiry. Single `threading.Lock` + `asyncio.to_thread` bridge. `session_purpose` values: buyer fulfillment `order-{order_id}`; agent QA `agent-qa-{listing_id}`; bucket scan `scan-{scan_job_id}`; onboarding probe `probe-{connection_id}`. Trade-off: +1 STS AssumeRole call per distinct order, QA listing, scan job, or onboarding probe. STS rate limit ~10/sec/account is acceptable for v1; if seller-side volume exceeds this, v2 can introduce a coarser audit/session strategy.

`order_service.create_order` (`app/services/order_service.py:147` BRANCH): on `fulfillment_type == SELLER_S3_PRESIGNED_URL`, read `Listing.raw_metadata.s3_connection` (a non-null `S3ConnectionMetadata`) to resolve gatekeeper target via `s3_connection.connection_id`, `s3_connection.vectoraiz_instance_url`, `s3_connection.bucket`, and `s3_connection.region`; iterate `Listing.raw_metadata.files[]` and for each entry parse the `path` (expected `s3://{bucket}/{key}`) to extract `object_key`; call seller vectorAIz `/sign-url` with `{object_key, expires_in_seconds: 900, session_purpose: "order-{order_id}"}` for each file; collect returned `{presigned_url, expires_at}` entries into the existing JWT delivery-token response `delivery_config`. Predecessor chain: `app/api/v1/endpoints/orders.py:214` `order_service.issue_download_token` → `app/services/order_service.py:599` `create_delivery_token` call → `app/core/security.py:188` `create_delivery_token` definition.

`refresh_access` (`app/api/v1/endpoints/orders.py:619` EXTENSION): on expired URL for SELLER_S3_PRESIGNED_URL order, re-sign by calling seller vectorAIz `/sign-url` with the listing's `connection_id` and the same `session_purpose = "order-{order_id}"`. Existing JWT validation + downloads-remaining checks unchanged. JWT `jti` replay prevention applies to minting new URLs, not revoking URLs already issued by S3.

## §4 Per-chunk acceptance criteria

### C1 — Schema + models + migration (vectorAIz)
Scope: three SQLModel tables per §3.1 + Alembic migration. Files: `app/models/s3_connection.py`, `app/models/s3_scan_job.py`, `app/models/s3_object_metadata.py`, `alembic/versions/<date>_001_s3_sts_connector_schema.py`. Deps: none. Parallel: no (foundation). LoC: ~150. ACs: (a) `alembic upgrade head` creates the three tables idempotently. (b) `S3Connection` constraints allow NULL `role_arn` AND `external_id` ONLY when `status = "onboarding"` and reject NULL when `status = "configured"`. (c) `S3ObjectMetadata.dataset_id` lives on the per-object row (not on `S3ScanJob`) per M7. (d) FK cascades: deleting an `S3Connection` cascades to its scan jobs + object metadata.

### C2 — STS credential broker (vectorAIz)
Scope: `app/services/sts_credential_broker.py` mirroring `app/services/llm_providers/bedrock.py:40-75` (ai-market-backend) verbatim. Deps: C1. Parallel: no. LoC: ~200. ACs: (a) `get_or_create_s3_client(connection_id, session_purpose)` returns a fresh boto3 S3 client after loading local `role_arn`, `external_id`, and `region` from `S3Connection`. (b) Cache keyed by `(role_arn, region, session_purpose)`. Refresh 2-min before STS session expiry. (c) Single `threading.Lock` + `asyncio.to_thread` bridge for cache safety (M8 carried). (d) RoleSessionName per locked-decision format `aim-{seller_id}-{order_id_or_purpose}` where buyer fulfillment uses `session_purpose = "order-{order_id}"`, agent QA uses `session_purpose = "agent-qa-{listing_id}"`, bucket scan uses `session_purpose = "scan-{scan_job_id}"`, and onboarding probe uses `session_purpose = "probe-{connection_id}"`. (e) AssumeRole failure raises classified exception subclasses (ExternalIdMismatch, TrustPolicyPrincipalWrong, BucketPermissionsInsufficient, RoleMaxSessionTooLow, MalformedRoleArn).

### C3 — S3 connector backend service (vectorAIz)
Scope: `app/services/s3_connector.py` reshaped from predecessor `app/services/db_connector.py:108`. Deps: C2. Parallel: no. LoC: ~200. ACs: (a) `scan_bucket(connection_id)` paginated `list_objects_v2` with continuation_token persistence on `S3Connection.continuation_token`. (b) Resume from last good token after process restart. (c) `generate_presigned_url(connection_id, object_key, expires_in=900)` wraps STSCredentialBroker. (d) `test_assume_role(connection_id)` probes STS only — does not touch bucket. (e) `list_objects_preview(connection_id, prefix?, max_results=20)` for bucket browser UI.

### C4 — Management API + gatekeeper endpoint (vectorAIz)
Scope: `app/routers/s3.py` per §3.2. Files: `app/routers/s3.py`, `app/main.py` router registration. Deps: C3. Parallel: YES with C5. LoC: ~200. ACs: (a) CRUD endpoints mirror `app/routers/database.py:133-223` shape. (b) `POST /sign-url` requires internal JWT auth — not exposed to public buyers. (c) `POST /sign-url` accepts and validates `session_purpose` values `order-{order_id}` and `agent-qa-{listing_id}` and passes them to the vectorAIz STS broker cache key. (d) `POST /test-assume-role` returns 200 with diagnostic or 400 with classified error. (e) OpenAPI schema correctly types all request/response models. (f) Rate-limit `POST /sign-url` to 10/sec/connection. (g) `POST /finalize` accepts `{role_arn, external_id}` only for `status = "onboarding"` rows, validates with test-assume-role before persisting, atomically transitions to `status = "configured"`, and returns 409 for already-configured rows.

### C5 — Scan orchestrator (vectorAIz)
Scope: `app/services/s3_scan_orchestrator.py` (NEW). Walks bucket, populates `S3ObjectMetadata`, ephemerally downloads each object for `processing_service.create_dataset(original_filename, file_type)` (signature per `app/services/processing_service.py:294`) invocation. Deps: C3. Parallel: YES with C4. LoC: ~250. ACs: (a) End-to-end scan of 10-object moto bucket produces 10 `S3ObjectMetadata` rows + 10 `DatasetRecord` rows. (b) Ephemeral download cleanup: after each `create_dataset` returns, temp file deleted from `upload_dir`. (c) Continuation_token persisted; resume from last position. (d) Per-object error tolerated; failed objects logged but scan continues. (e) `S3ScanJob.status` transitions pending → running → completed/failed correctly. (f) Objects larger than the per-object size cap (default 2 GiB) are recorded with `extraction_status = "SKIPPED_OVERSIZED"` and `metadata_extracted_at = NULL`; no local download occurs for them. (g) Before each ephemeral download, available disk space at `upload_dir` is checked; downloads fail-fast with `extraction_status = "SKIPPED_DISK_PRESSURE"` when free space is below a configurable threshold (default 5 GiB).

### C6 — ai-market-backend purchase-flow integration + vectorAIz signing bridge
Scope: per §2.2 + §3.3 + §3.4. Files: `app/services/order_service.py:147` branch, `app/models/marketplace.py:37` enum, `app/schemas/listing.py:31` enum, `alembic/versions/<date>_alter_fulfillment_type_enum_seller_s3_sts.py`, `app/api/v1/endpoints/orders.py:619` refresh wire-up. Deps: C4 gatekeeper contract. Parallel: YES with C7. LoC: ~200. ACs: (a) `alembic upgrade head` adds enum value idempotently. (b) `Listing.raw_metadata` for SELLER_S3_PRESIGNED_URL listings REUSES the existing `files[]` carrier convention. Per-file `path` follows `s3://{bucket}/{key}` URI scheme. Top-level `s3_connection: {connection_id, vectoraiz_instance_url, bucket, region}` is required, with concrete field paths including `s3_connection.connection_id` (validator rejects null `s3_connection` when any `files[].path` starts with `s3://`). `role_arn` and `external_id` MUST NOT appear in raw_metadata or any platform-side state. (c) `order_service.create_order` for SELLER_S3_PRESIGNED_URL listing calls seller vectorAIz `/sign-url` with the listing's `connection_id`, `object_key`, and `session_purpose = "order-{order_id}"`; vectorAIz holds `role_arn`/`external_id` and assumes-role internally. (d) The returned S3 URL is carried via the existing JWT delivery-token response `delivery_config`, using the actual chain `orders.py:214` → `order_service.issue_download_token` → `order_service.py:599` → `create_delivery_token` → `app/core/security.py:188`; no new S3-specific token type and no hash-based `FulfillmentDownloadToken` conflation. (e) Refresh endpoint at `orders.py:619` re-signs through the same vectorAIz `/sign-url` gatekeeper on expired URL. (f) ai-market platform has no S3 STS broker and never stores `role_arn`, `external_id`, STS credentials, or seller IAM secrets. (g) Buyer JWT signature validation preserves existing `create_delivery_token` patterns including `jti` replay prevention for minting new URLs.

### C7 — allAI onboarding agent
Scope: per §2.3. Files: `app/allai/agents/aim_data_onboarding/agent.py`, `iam_policy_templates.py`, `sts_probe.py`, `diagnostics.py`, `__init__.py`. Deps: C1 (schema) + C4 (test-assume-role endpoint). Parallel: YES with C6. LoC: ~300 (largest). ACs: (a) Agent registered in `app/allai/agent_registry.py:25` `AgentRegistry` following existing pattern. (b) Agent manifest at `app/allai/agent_manifest.py:79` style. (c) `generate_trust_policy_json(seller_id, external_id, vectoraiz_signing_principal)` produces valid JSON enforcing the four required conditions. (d) `probe_role_arn(role_arn, external_id)` invokes seller vectorAIz `test-assume-role`; role data is stored only in vectorAIz and returns structured success or failure. (e) `classify_assume_role_error(error_response)` distinguishes 5+ distinct failure modes. (f) Walkthrough flow is idempotent and checkpointable via pre-row S3Connection with status=onboarding.

### C8 — Frontends (vectoraiz-monorepo frontend + ai-market-frontend)
Scope: per §2.4 + vectorAIz seller side. Files: vectoraiz-monorepo frontend — `S3ConnectionPage.tsx`, `S3BucketBrowser.tsx`, `S3OnboardingChat.tsx` (subdir under `frontend/` resolved at Gate 2 chunk-dispatch verification per Mars S712.W r2 audit); ai-market-frontend — `components/PresignedUrlDownloader.tsx` (Next.js app-router flat components/). Deps: C4 + C6 + C7. Parallel: no. LoC: ~200. ACs: (a) Seller creates S3 connection via guided allAI chat. (b) Buyer download uses `<a download href={url}>` pattern (no CORS required). (c) Refresh-on-expiry handled via `orders.py:619` refresh endpoint. (d) Bucket browser preview surfaces first 20 objects via `POST /list_objects_preview` proxy.

### C9 — Integration tests
Scope: end-to-end with moto STS+S3 fixtures. Files: `tests/integration/test_s3_sts_scan_end_to_end.py`, `tests/integration/test_s3_sts_sign_url.py`, `tests/integration/test_s3_sts_scan_pagination_resume_across_multiple_pages.py`, `ai-market-backend/tests/integration/test_s3_sts_purchase_flow.py`, `ai-market-backend/tests/integration/test_aim_data_onboarding_agent.py`, `ai-market-backend/tests/integration/test_aim_data_onboarding_resume_from_abandoned_pre_row_s3connection.py`. Deps: C5 + C6 + C7. Parallel: no. LoC: ~250. ACs: (a) Full scan-to-sign-to-buyer-fetch flow passes against moto. (b) Walkthrough agent unit-tested for all error classification paths. (c) STS session expiry simulated; refresh path verified. (d) ExternalId mismatch surfaces correct diagnostic. (e) Abandoned onboarding resumes from pre-row with ExternalId preserved. (f) Large-bucket scan resumes across 3+ pages without duplicate object metadata.

## §5 Test plan

**Unit tests:**

- `tests/unit/test_sts_credential_broker.py`: mocked boto3 client. Verify cache key tuple `(role_arn, region, session_purpose)`, 2-min refresh-buffer logic, RoleSessionName format compliance with locked decision, and distinct clients for all four purposes: `order-{order_id}`, `agent-qa-{listing_id}`, `scan-{scan_job_id}`, and `probe-{connection_id}`.
- `tests/unit/test_s3_connector.py`: mocked client, paginator behavior, continuation_token resume, error path on missing connection.
- `tests/unit/test_iam_policy_templates.py`: assert generated trust JSON has Principal=single ARN, Action=sts:AssumeRole only, Condition.StringEquals.sts:ExternalId required, no wildcards anywhere. Assert generated permissions JSON has s3:GetObject + s3:ListBucket scoped to specified bucket + bucket/*.
- `tests/unit/test_diagnostics.py`: 5+ distinct AssumeRole failure types map to 5+ distinct classified diagnostics with remediation snippets.
- `tests/unit/test_order_service_create_order_branch.py`: assert SELLER_S3_PRESIGNED_URL branch reads `Listing.raw_metadata`, calls seller vectorAIz `/sign-url` exactly once per order, never reads or stores `role_arn`/`external_id`, and returns the presigned URL in JWT delivery response `delivery_config`.

**Integration tests (moto fixtures):**

- `tests/integration/test_s3_sts_scan_end_to_end.py`: spin up moto S3 + STS, configure fake role with trust policy, end-to-end scan produces N `S3ObjectMetadata` rows. Verify ephemeral download cleanup.
- `tests/integration/test_s3_sts_sign_url.py`: assert sign-url endpoint returns URL with `X-Amz-Signature` query param. Assert URL is fetchable from moto and returns expected bytes.
- `tests/integration/test_s3_sts_purchase_flow.py`: simulate Stripe checkout → order create → JWT delivery response `delivery_config` contains presigned URL from vectorAIz `/sign-url` → mock fetch via moto returns object bytes. Verify CloudTrail-style event log emitted from vectorAIz signing.
- `tests/integration/test_aim_data_onboarding_agent.py`: walkthrough happy path, ExternalId mismatch path, MaxSessionDuration-too-low path, malformed-ARN path. Each path produces correct classified diagnostic.
- `tests/integration/test_aim_data_onboarding_resume_from_abandoned_pre_row_s3connection.py`: start onboarding (creates pre-row with status=onboarding + ExternalId), abandon (close client), resume from same seller_id, assert ExternalId preserved + walkthrough resumes from correct checkpoint step.
- `tests/integration/test_s3_sts_scan_pagination_resume_across_multiple_pages.py`: moto bucket with 2500 objects (forces 3+ `list_objects_v2` pages with default `max_keys=1000`), interrupt scan mid-second-page (kill process), resume via `S3ScanJob.continuation_token`, assert all 2500 `S3ObjectMetadata` rows present + no duplicates.

**End-to-end scenarios:**

- Onboarding happy path: seller initiates chat → role-ARN paste → probe success → S3Connection persisted → bucket scan kicked off → 10 objects enumerated → listing published with SELLER_S3_PRESIGNED_URL fulfillment_type.
- Error remediation: seller pastes role-ARN with missing ExternalId → diagnostic surfaced with remediation snippet → seller corrects → probe success.
- URL refresh on expiry: buyer order delivered → wait 16 min (URL expired) → buyer hits refresh endpoint → fresh URL returned with new X-Amz-Signature.

## §6 Risk analysis

**R1 — STS session expiry mid-request (MEDIUM).** STS session might expire between AssumeRole and presigned URL signing inside vectorAIz. **Mitigation:** Cache with 2-min refresh buffer per §6 of `infra:worker-artifact-stash:S711.W:r3-t3-aws-sts-presigned-url-semantics-reference`, keyed by `(role_arn, region, session_purpose)` to preserve per-order/per-QA auditability. Trade-off: +1 STS AssumeRole call per distinct order or QA listing; STS rate limit ~10/sec/account is acceptable for v1. **Monitoring:** Log STS refresh frequency; alert on >5/sec/connection (indicates abuse or bug). This cache lives in vectorAIz, not the ai-market platform.

**R2 — ExternalId mismatch (HIGH-PROBABILITY, LOW-IMPACT).** Most common onboarding failure. AccessDenied response from AssumeRole does NOT specify ExternalId as the cause (AWS security behavior). **Mitigation:** Walkthrough always passes ExternalId; diagnostic offers ExternalId verification as the primary failure mode. Display seller's ExternalId prominently with copy-to-clipboard.

**R3 — Role-trust-policy drift (LOW-PROBABILITY, HIGH-IMPACT).** Seller edits their trust policy after onboarding to break the trust relationship. **Mitigation:** Periodic background `test-assume-role` job (daily). On failure, mark `S3Connection.status = degraded` and surface to seller dashboard. Suspend listings linked to degraded connections; resume on next successful probe.

**R4 — Presigned URL replay (BOUNDED-EXPOSURE).** Once buyer receives the presigned URL, S3 honors it until its own 15-min expiry regardless of ai-market-side state. ai-market JWT invalidation, order revocation, and download counters cannot revoke an already-issued S3 URL within the 15-min window. **Accepted bounded exposure** for v1. Mitigation if stronger revocation needed in v2: (a) reduce ExpiresIn to 60s and force per-chunk re-signing for large downloads, OR (b) introduce stateful gatekeeper proxy that re-validates per request. JWT replay prevention via `jti` STILL applies — but only prevents NEW URLs from being minted after revocation, not invalidates URLs already in flight.

**R5 — allAI walkthrough abandoned mid-flow (MEDIUM-PROBABILITY).** Seller starts onboarding, walks away, returns later. **Mitigation:** Idempotent agent flow with explicit checkpoint state. ExternalId generated at flow start and persisted to a pre-row `S3Connection` (status=onboarding); resume reads pre-row.

**R6 — Large-bucket pagination resume (MEDIUM-PROBABILITY).** Scan of a 10M-object bucket may take hours; process restart should resume. **Mitigation:** `S3Connection.continuation_token` persisted on every page boundary. On scan resume, load token and continue.

**R7 — Confused-deputy attack (LOW-PROBABILITY, HIGH-IMPACT).** Seller A tries to register seller B's role ARN or connection metadata as their own. Without ExternalId and seller-side connection ownership checks, a signer could be tricked into accessing seller B's bucket for seller A's listings. **Mitigation:** ExternalId scheme is per-seller unique (`aim-data-seller-{uuid}-{32hex}`); trust policy enforces `StringEquals` ExternalId; vectorAIz binds each `S3Connection` to the owning seller; mismatched ExternalId fails AssumeRole.

**R8 — Cross-region latency for sign + download (LOW-IMPACT).** Bucket in eu-west-1, platform in us-east-1 — signing works but adds round-trip. **Mitigation:** Pre-cache `S3Connection.region` at onboarding; construct boto3 S3 client with correct `region_name` so URL host targets correct region endpoint.

**R9 — Local disk exhaustion during scan extraction (MEDIUM-PROBABILITY, MEDIUM-IMPACT).** Multi-GB objects in seller's bucket could exhaust the vectorAIz container's local disk during ephemeral download. **Mitigation:** per-object size cap (default 2 GiB) + disk-pressure pre-check before each download (default min-free 5 GiB); objects exceeding the cap or hitting disk pressure are skipped with explicit `extraction_status` and surfaced to the seller dashboard. Buyer fulfillment path is unaffected (presigned URL bypasses extraction). v2 enhancement: streaming extraction via S3 Range requests.

## §7 Dependencies + sibling BQs

**Predecessor:** BQ-AIM-DATA-S3-SOURCE-CONNECTOR-S684 — PARKED. Surviving elements per `infra:worker-artifact-stash:S711.W:r3-t4-c-sts-surviving-element-extraction`: `S3Connection` model shape, boto3 client cache + lock infrastructure (M8 carried), paginated `list_objects_v2` logic, frontend UI shape, `processing_service.create_dataset` integration target. MOOT under C-STS: M9 partial-download cleanup, M10 `local_temp_path` persistence.

**Sibling (already merged):** BQ-AIM-DATA-NON-VECTOR-LISTING-METADATA-RELAX-S684 — Gate 2 APPROVED_AND_MERGED via S691/S692/S693 parallel track (verified Mars S703.W round-2 verbatim source-read on ai-market-backend `52d5f1d4` + vectoraiz `a0afb37` + alembic `20260522_001_relax_nullable_privacy_quality_s691.py`). The PREVIEW_READY status precondition relax enables listing metadata generation for S3-sourced non-vector datasets.

**Sub-BQ (recommend filing separately):** seller vectorAIz signing-principal setup guidance. Ops/docs work — document how the seller's vectorAIz container obtains permission to AssumeRole into the seller-owned fulfillment and agent-QA roles without exposing `role_arn` or `external_id` to ai.market. Should ship before C2/C4 production enablement but has separate ops checklist.

**Cross-pillar dependency:** ai-market-frontend C8 buyer-flow chunk requires C6 backend contract finalized (FulfillmentType enum value + refresh endpoint shape).

## §8 Open questions resolved

Per `build:bq-aim-data-s3-sts-fulfillment-s711.body.decisions_locked_s711_12_50_utc` (Max-locked 2026-05-26 12:50 UTC, entity version 10):

- `session_duration_seconds = 3600`
- `presigned_url_expiry_seconds = 900`
- `external_id_format = "aim-data-seller-{uuid}-{32hex}"`
- `role_session_name_format = "aim-{seller_id}-{order_id_or_purpose}"`
- `required_role_max_session_duration_seconds = 3600`
- Signing ownership: AIM Data customer's vectorAIz container signs. ai.market platform never holds `role_arn` or `external_id`; buyer fulfillment and agent QA both flow through seller vectorAIz `/sign-url`.
- 9-chunk C-STS decomp APPROVED per `infra:worker-artifact-stash:S711.W:r3-t4-c-sts-surviving-element-extraction`
- allAI onboarding agent placement: PLATFORM SIDE (ai-market-backend allAI registry)
- Listing.raw_metadata convention for SELLER_S3 listings: REUSE `files[]` per-file carrier; add top-level `s3_connection` for connection metadata; per-file `path` = `s3://{bucket}/{key}` (Max-locked 2026-05-26 17:18 UTC; rationale: validator/frontend pattern parity with FILE_DOWNLOAD fulfillment; one carrier, one dispatch path)

No new open questions surfaced in this spec. Implementation chunks proceed against these locked decisions.

## §9 References

**vectoraiz-monorepo origin/main HEAD `cd8bb5dc`:**

- `app/config.py:80` — `class Settings(BaseSettings)`
- `app/config.py:140` — `AliasChoices("AIM_DATA_KEYSTORE_PASSPHRASE", "VECTORAIZ_KEYSTORE_PASSPHRASE")` post-PR #5 canonicalization
- `app/services/db_connector.py:108` — `class DatabaseConnector` predecessor pattern
- `app/services/db_credential_service.py:45,50` — `encrypt_password` / `decrypt_password` (Fernet pattern; REPLACED in C-STS)
- `app/models/database_connection.py:18` — `class DatabaseConnection` predecessor schema
- `app/routers/database.py:133-223` — CRUD route handler shape
- `app/services/processing_service.py:294` — `def create_dataset(self, original_filename: str, file_type: str) -> DatasetRecord` integration target

**ai-market-backend origin/main HEAD `ef2e6d07`:**

- `app/models/marketplace.py:37` — `class FulfillmentType(str, enum.Enum)`
- `app/models/marketplace.py:119-126` — `fulfillment_type` Column with `name="fulfillment_type_enum", create_type=False`
- `app/schemas/listing.py:31` — schema-side FulfillmentType
- `app/core/security.py:188` — `create_delivery_token` JWT delivery-token definition
- `app/models/fulfillment_download_token.py:39` — `class FulfillmentDownloadToken` (separate hash-based/BQ-D1 token system; not the S3 URL carrier)
- `app/schemas/raw_delivery.py` — `RawMetadataFile` + `RawMetadata` pydantic models; `files[]` validator (non-empty); existing FILE_DOWNLOAD carrier shape that SELLER_S3 listings reuse per Max decision S712 17:18 UTC
- `app/api/v1/endpoints/orders.py:214` — `order_service.issue_download_token`
- `app/api/v1/endpoints/orders.py:619` — `@router.post("/{order_id}/refresh")` `refresh_access`
- `app/services/order_service.py:147` — `async def create_order`
- `app/services/order_service.py:599` — calls `create_delivery_token`
- `app/services/llm_providers/bedrock.py:40-75` — STS AssumeRole + downstream S3 client construction pattern (mirror target)

**Mars stash references:**

- `infra:worker-artifact-stash:S711.W:r3-t1-gha-tag-trigger-diagnostic`
- `infra:worker-artifact-stash:S711.W:r3-t2-ai-market-backend-purchase-flow-predecessor-read` (v=2 with STS supplement)
- `infra:worker-artifact-stash:S711.W:r3-t3-aws-presigned-url-semantics-reference`
- `infra:worker-artifact-stash:S711.W:r3-t3-aws-sts-presigned-url-semantics-reference`
- `infra:worker-artifact-stash:S711.W:r3-t4-c-sts-surviving-element-extraction`
- `infra:worker-artifact-stash:S711.W:r3-t5-allai-onboarding-flow-predecessor-read`

**AWS documentation:**

- STS AssumeRole API: https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
- External ID best practices: https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html
- Confused Deputy Problem: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
- S3 generate_presigned_url: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_url.html
- CloudTrail S3 Data Events: https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-cloudtrail-events.html
