# S711 chunk — STS-native no-copy listing (closes the dataset_id gap)

## Problem (verified in current main @45365d4)
Buyer delivery `FulfillmentService._find_s3_object(dataset)` resolves an S3-backed
dataset via `S3ObjectMetadata.dataset_id == dataset.id` (fulfillment_service.py:521-540).
Nothing in the codebase constructs an `S3ObjectMetadata` row (only the class def matches
`S3ObjectMetadata(`), and nothing sets `dataset_id`. So S3 delivery never resolves for a
real order. C2 STS broker (#16) + C3 presigned delivery (#18) are merged and waiting on
this link.

## Goal — NO byte copy
Let a seller (1) scan their connected bucket into `s3_object_metadata`, and (2) register a
scanned object as a sellable `DatasetRecord`, setting `S3ObjectMetadata.dataset_id`. Data
stays in the seller's bucket; buyer pulls via presigned URL (C3).
v1 contract: one `DatasetRecord` <-> one S3 object (matches C3 `_find_s3_object(...).first()`
+ single presign). Multi-object datasets = follow-on.

## Build (vectoraiz-monorepo, app/)

### 1. New `app/services/s3_scan_service.py`
`scan_connection(connection_id) -> S3ScanJob`:
- Load S3Connection; create S3ScanJob(status="running").
- `creds = STSBroker().assume_role(connection, purpose="scan")`; build an s3 client from
  creds (access_key_id / secret_access_key / session_token / region).
- Paginate `list_objects_v2(Bucket=connection.bucket, Prefix=connection.prefix or "")` using
  the continuation token. Per object UPSERT `S3ObjectMetadata` keyed on
  (connection_id, object_key): object_key, size_bytes, etag, last_modified, content_type
  (best-effort from key extension; no per-object head_object in v1 — cost). On re-scan
  PRESERVE existing row id + dataset_id. Update scan_job.objects_enumerated +
  continuation_token as you go.
- Success: scan_job.status="completed", completed_at set, connection.last_scanned_at set.
- Error: scan_job.status="failed", error_message = STSAssumeError seller-actionable message;
  never leak raw AWS internals (consistent with S728 MED hardening / raise-from-None).

READINESS NOTE (verify before coding): `STSBroker.assume_role` gates on
`connection.status == "configured"` (sts_broker.py:57). `/verify` sets status="verified"
(s3_connections.py:203-250). Confirm the exact status a usable connection carries, and if
the broker gate would reject it, broaden the broker readiness check to accept BOTH
"configured" and "verified" (with a test). Do not guess — read the enum in source.

### 2. Endpoints in `app/routers/s3_connections.py`
- `POST /{connection_id}/scan` -> run scan (sync, v1 small buckets), return scan-job summary.
- `GET  /{connection_id}/scan/{scan_job_id}` -> scan-job status/progress.
- `GET  /{connection_id}/objects` -> list scanned S3ObjectMetadata for the connection
  (paginated; optional dataset_linked filter).
- `POST /{connection_id}/objects/{object_id}/register` -> body: dataset metadata (or an
  existing dataset_id / listing_id). Create-or-resolve a `DatasetRecord`
  (original_filename = basename(object_key), file_type from extension,
  file_size_bytes = size_bytes, status="s3_linked", storage_filename = object_key), then set
  that `S3ObjectMetadata.dataset_id = dataset.id`. Idempotent: re-register to the same
  dataset is a no-op; to a different dataset reassigns. Return dataset + object.

### 3. Schemas
Request/response pydantic models for the above; mirror existing `S3*Response` style.

### 4. Tests — `tests/test_s3_scan_service.py` + router tests (mock boto3 / STSBroker; moto ok)
- scan persists one row per object; objects_enumerated correct.
- re-scan idempotent (no dupes) + preserves dataset_id.
- register sets dataset_id + creates a well-formed DatasetRecord.
- end-to-end: DatasetRecord (listing_id set) linked to a scanned object ->
  `FulfillmentService._find_dataset(listing_id)` then `_find_s3_object(dataset)` returns
  (connection, metadata).
- broker readiness accepts post-verify status.
- error path: STSAssumeError -> scan_job failed; no AWS internals leaked.

## Constraints
- NO byte copy anywhere. Local-file delivery path UNCHANGED (additions only), per C3.
- Reuse STSBroker — do NOT duplicate the verify endpoint inline assume_role.
- Least privilege: scan uses the assumed-role creds only; no long-lived secrets persisted.
- S3ObjectMetadata + S3ScanJob tables already exist (C1) — no schema change expected unless
  the readiness fix requires one (it should not).

## Out of scope (follow-on chunks)
- Multi-object datasets + multi-URL delivery.
- allAI IAM-console onboarding walkthrough.
- Scheduled / auto re-scan cadence.
