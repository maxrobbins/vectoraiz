# BQ-AIM-DATA-S3-STS-FULFILLMENT-S711 — Gate 2 Chunk 2: STS AssumeRole Broker

Branch base: origin/main @ e1ae36e. Diff against this base (cite the base SHA).
Parent: Gate 1 APPROVED + merged (PR #6). Chunk decomp approved by Max 2026-05-26 12:50 UTC.
Predecessor C1 MERGED (PR #7): schema + models + migration 020 + s3_connections router.

## Scope (this chunk only)
Implement the cross-account STS AssumeRole broker: a self-contained service that, given a
configured seller S3 connection, assumes the seller's IAM role and returns short-lived
credentials. This is the foundation both later chunks consume (presigned-URL fulfillment;
agent QA credential injection). Build NOTHING from those later chunks here.

## Ground truth (read these; do not infer)
- `app/models/s3_connection.py` — S3Connection: `role_arn` (str≤512, nullable), `external_id`
  (str≤128, nullable), `region` (str≤64), `bucket`, `status` ('onboarding'|'configured'),
  `id` (str pk). CHECK ck_s3_connection_configured_creds_required: configured ⇒ role_arn AND
  external_id non-null. The broker only reads this model; it must not mutate it.
- Max-locked STS v1 defaults (entity decisions_locked_s711_12_50_utc.sts_v1_defaults):
  - RoleSessionName format: `aim-{seller_id}-{order_id_or_purpose}` (readable in seller CloudTrail)
  - ExternalId: taken from `connection.external_id` (per-connection, embedded in seller trust policy)
  - DurationSeconds: 3600 (role MaxSessionDuration is 3600)
  - (presigned URL expiry 900s is a LATER chunk; not used here)
- Single platform IAM identity assumes ALL seller roles (Q7 rec). Broker uses the platform's
  ambient AWS credentials (boto3 default chain / platform-configured creds). No per-seller
  platform role.

## Acceptance criteria
- AC1: New module `app/services/sts_broker.py`. Public surface: `class STSBroker` with
  `assume_role(connection: S3Connection, purpose: str) -> AssumedCredentials`. `AssumedCredentials`
  is a typed dataclass/pydantic model: `access_key_id`, `secret_access_key`, `session_token`,
  `expiration` (tz-aware UTC datetime), `region`.
- AC2: assume_role calls sts:AssumeRole via boto3 with RoleArn=connection.role_arn,
  RoleSessionName=sanitized(`aim-{seller_id}-{purpose}`), ExternalId=connection.external_id,
  DurationSeconds=3600. `seller_id` derives from connection.id.
- AC3: RoleSessionName sanitization — STS allows charset [\w+=,.@-], length 2–64. Sanitize and
  truncate deterministically; collisions across distinct purposes must remain distinct after
  truncation (hash-suffix if truncation would collide).
- AC4: In-memory TTL cache keyed by (connection.id, role_session_name) — NOT connection.id alone
  (resolves Gate 1 R1 MED-4: per-order RoleSessionName must not share a cache slot). Reuse a
  cached credential only if `expiration` is more than a 300s safety margin away; otherwise
  re-assume. Cache is process-local; thread-safe.
- AC5: Reject assume_role when connection.status != 'configured' OR role_arn/external_id is None —
  raise `STSConnectionNotReady`. Do not call STS in that case.
- AC6: Error handling — botocore ClientError (AccessDenied, role not assumable, ExpiredToken)
  maps to a typed `STSAssumeError` carrying a seller-actionable message and the underlying AWS
  error code. The broker does NOT mutate connection.status/error_message (caller's concern).
- AC7: Structured logging on each real AssumeRole call: connection.id, role_session_name, purpose,
  outcome. NEVER log secret_access_key or session_token.
- AC8: Tests `tests/services/test_sts_broker.py` using botocore Stubber (or moto): success path;
  cache hit (no second STS call); cache expiry triggers refresh; two distinct purposes do not
  collide in cache; AccessDenied → STSAssumeError; not-configured → STSConnectionNotReady;
  RoleSessionName sanitization + truncation. All tests pass.

## Out of scope (later chunks — do not implement)
Presigned-URL generation; agent-runtime credential injection; router endpoints; bucket scan
wiring; frontend; allAI walkthrough.

## Constraints
- boto3/botocore only for AWS; reuse the dependency C1 already introduced (verify in requirements).
- Touch only: app/services/sts_broker.py (new), app/services/__init__.py (if needed),
  tests/services/test_sts_broker.py (new). Do not edit s3_connection.py or migration 020.
- Diff against branch base origin/main e1ae36e; cite the base SHA in the build summary.
