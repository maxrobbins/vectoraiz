# BQ-AIM-DATA-S3-STS-FULFILLMENT-S711 — Gate 2 Chunk 3: Buyer presigned-URL delivery

Branch base: origin/main @ 446f462 (C2 STS broker merged). Diff against this base; cite the base SHA.
Decision (Max-approved S728): S3 delivery is folded into the EXISTING FulfillmentService as an
alternate delivery branch — NOT a separate service. One delivery contract for ai.market, no drift.

## Ground truth (read before coding; do not infer)
- `app/services/fulfillment_service.py` — FulfillmentService._handle_deliver(message): parses
  params (order_id, listing_id, request_id), generates transfer_id, writes a FulfillmentLog,
  calls `_find_dataset(listing_id) -> (DatasetRecord, file_path)`, then streams local-file chunks
  (metadata action → _stream_chunks → complete action). This is the LOCAL-FILE path; leave it
  intact and behaviorally unchanged.
- `app/services/sts_broker.py` (merged C2) — STSBroker.assume_role(connection, purpose) ->
  AssumedCredentials(access_key_id, secret_access_key, session_token, expiration, region).
  Raises STSConnectionNotReady / STSAssumeError.
- `app/models/s3_object_metadata.py` — S3ObjectMetadata: connection_id (FK s3_connection),
  object_key (≤1024), size_bytes, content_type, etag, dataset_id (FK dataset_records, nullable).
  This is the dataset→S3 link.
- `app/models/s3_connection.py` — S3Connection: id, bucket, region, prefix, role_arn,
  external_id, status.
- `app/models/fulfillment.py` — FulfillmentLog (transfer_id, order_id, listing_id, request_id,
  status, file_size_bytes, ...). Reuse as-is; no schema change.
- Locked default (entity decisions_locked_s711.sts_v1_defaults): presigned URL expiry = 900s.

## Scope (this chunk only)
Add an S3-backed delivery branch inside FulfillmentService so a buyer order for an S3-backed
dataset receives a short-lived presigned GET URL instead of a streamed file. NOTHING else.

## Acceptance criteria
- AC1: New private method `_find_s3_object(dataset: DatasetRecord) -> Optional[(S3Connection, S3ObjectMetadata)]`
  — resolves the S3ObjectMetadata row whose dataset_id == dataset.id and loads its S3Connection.
  Returns None when the dataset is not S3-backed (→ existing local-file path runs unchanged).
- AC2: In `_handle_deliver`, AFTER `_find_dataset` resolves a dataset, branch: if `_find_s3_object`
  returns a pair → S3 presigned-URL path (AC3–AC5). Else → existing local-file path, untouched.
  Dataset-not-found behavior is unchanged.
- AC3: New `app/services/s3_presign.py`: `generate_presigned_get(creds: AssumedCredentials,
  bucket: str, key: str, expires_in: int = 900) -> str`. Builds a boto3 s3 client from the
  AssumedCredentials (access key, secret, session token, region) and returns a presigned GET URL
  for {bucket}/{key}. expires_in default 900s (locked). No long-lived creds used.
- AC4: S3 delivery flow in FulfillmentService: assume role via STSBroker(purpose=transfer_id or
  f"order-{order_id}"), generate the presigned URL (900s) for the object, send a NEW Trust Channel
  action `vai.fulfillment.url` with parameters {url, expires_in: 900, object_key, content_type,
  size_bytes, sha256/etag if available, transfer_id, order_id, listing_id}. Then send the existing
  `vai.fulfillment.complete` action with status=fulfilled (no chunk_count for URL mode; include
  delivery_mode="presigned_url"). Update FulfillmentLog status received→uploading→completed; set
  file_size_bytes from S3ObjectMetadata.size_bytes.
- AC5: Errors map onto the existing _send_error + _update_log pattern (do NOT invent a new error
  channel): STSConnectionNotReady → error_code "S3_CONNECTION_NOT_READY"; STSAssumeError →
  "S3_ASSUME_FAILED" (include AWS error code in log error_message, not in buyer-facing text);
  presign/client failure → "S3_PRESIGN_FAILED". NEVER log secret_access_key/session_token/the
  signed URL query string.
- AC6: The presigned URL is generated with the SHORT-LIVED assumed credentials only; the platform's
  own long-lived credentials are never used to sign buyer URLs. The seller's connection model is
  not mutated.
- AC7: Tests `tests/services/test_s3_presign.py` + additions to fulfillment tests:
  (a) generate_presigned_get returns a URL containing the bucket+key and an expiry (use botocore
  Stubber/moto or a stubbed client); (b) _handle_deliver takes the S3 branch when dataset is
  S3-backed and sends vai.fulfillment.url then complete (mock STSBroker + TrustChannelClient,
  assert no chunk streaming); (c) _handle_deliver still takes the local-file path (unchanged) when
  dataset is NOT S3-backed; (d) STSConnectionNotReady → _send_error S3_CONNECTION_NOT_READY, log
  failed. All pass; existing fulfillment tests stay green.

## Out of scope (do NOT build)
Router/HTTP endpoints; the IAM-console onboarding walkthrough (allAI); agent-QA credential use of
the broker; bucket scanning; frontend; presigned PUT/upload. Multipart/range delivery for >5GB
objects is a later concern — v1 issues a single presigned GET.

## Constraints
- Touch: app/services/fulfillment_service.py (add branch + _find_s3_object), app/services/s3_presign.py
  (new), tests as above. Do NOT modify sts_broker.py, s3_connection.py, migration 020, or the
  local-file streaming methods (_stream_chunks/_compute_sha256/_resolve_file_path).
- Reuse the merged STSBroker; do not re-implement role assumption.
- boto3 already declared (C2). Diff against base origin/main 446f462; cite it.
