# BQ-AIM-DATA-S3-SOURCE-CONNECTOR-S684 Gate 2 Chunk 1 Spec

## Status

- Build Queue: `BQ-AIM-DATA-S3-SOURCE-CONNECTOR-S684`
- Gate: 2
- Chunk: 1 of N - vectoraiz-monorepo schema + credential reuse + boto3 dependency
- Fold round: R1 folded
- Outcome signal: ready for MP R2 review
- Baseline stash: `infra:worker-artifact-stash:S689.W:T2:gate2-chunk1-s3-spec-draft`
- Fold stash: `infra:worker-artifact-stash:S691.W:T2:s3-chunk1-r1-fold`
- R1 reviewers: AG task `33666f53`, MP task `03bef326`
- R1 mandates folded: 6 total, 2 HIGH + 3 MED + 1 LOW
- Current materialization base: vectoraiz `origin/main` at `f5ffd71`; R1 cite-validity from `b6387f30` remains intact because the intervening S693 release-plumbing commit did not touch `alembic/versions/`, `app/models/`, `app/services/`, or `requirements.txt`.

## Scope

Gate 2 chunk 1 adds the vectoraiz-monorepo schema and credential primitives needed for the S3 source connector:

- SQLModel definitions for S3 connections, ingest jobs, and ingest file entries.
- Alembic migration for the three new S3 tables, their indexes, and FK constraints.
- S3 credential service that reuses the existing Fernet key provider.
- `boto3>=1.34,<2.0` dependency in `requirements.txt`.
- Focused tests for the model, migration, dependency, and credential-service behavior.

This chunk ships independently of the relax backend chunk 1 merge. It has no runtime call into `ai-market-backend`. Chunks that integrate `processing_service.create_dataset` with marketplace push remain blocked behind the relax backend chunk 1 merge.

## File Deltas

### `requirements.txt`

Add:

```text
boto3>=1.34,<2.0
```

`botocore` is expected as a transitive dependency. `boto3` was verified absent from `requirements.txt` at the R1 baseline.

### `alembic/versions/2026MMDD_XXX_s3_source_connector_schema_s684.py`

Create a new migration after the current script-chain head:

```python
down_revision = "018_raw_files_metadata"
```

Upgrade creates:

```python
op.create_table(
    "s3_connections",
    sa.Column("id", sa.String(36), primary_key=True),
    sa.Column("connection_name", sa.String(100), nullable=False, unique=True),
    sa.Column("bucket_name", sa.String(255), nullable=False),
    sa.Column("region_name", sa.String(50), nullable=True),
    sa.Column("access_key_encrypted", sa.Text(), nullable=False),
    sa.Column("secret_key_encrypted", sa.Text(), nullable=False),
    sa.Column("role_arn", sa.String(2048), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    sa.Column("is_active", sa.Boolean(), server_default=sa.true(), nullable=False),
)

op.create_table(
    "s3_ingest_jobs",
    sa.Column("id", sa.String(36), primary_key=True),
    sa.Column(
        "s3_connection_id",
        sa.String(36),
        sa.ForeignKey("s3_connections.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
    sa.Column("continuation_token", sa.Text(), nullable=True),
    sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
)

op.create_table(
    "s3_ingest_file_entries",
    sa.Column("id", sa.String(36), primary_key=True),
    sa.Column(
        "ingest_job_id",
        sa.String(36),
        sa.ForeignKey("s3_ingest_jobs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    sa.Column("object_key", sa.String(1024), nullable=False),
    sa.Column("object_size_bytes", sa.BigInteger(), nullable=False),
    sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
    sa.Column(
        "dataset_id",
        sa.String(36),
        sa.ForeignKey("dataset_records.id", ondelete="SET NULL"),
        nullable=True,
    ),
    sa.Column("local_temp_path", sa.String(1024), nullable=True),
    sa.Column("error_detail", sa.Text(), nullable=True),
)

op.create_index("idx_s3_ingest_jobs_connection_id", "s3_ingest_jobs", ["s3_connection_id"])
op.create_index("idx_s3_ingest_file_entries_job_id", "s3_ingest_file_entries", ["ingest_job_id"])
op.create_index("idx_s3_ingest_file_entries_dataset_id", "s3_ingest_file_entries", ["dataset_id"])
```

Downgrade drops indexes and tables in reverse order:

1. `idx_s3_ingest_file_entries_dataset_id`
2. `idx_s3_ingest_file_entries_job_id`
3. `idx_s3_ingest_jobs_connection_id`
4. `s3_ingest_file_entries`
5. `s3_ingest_jobs`
6. `s3_connections`

### `app/models/s3_connection.py`

Add `S3Connection` SQLModel mirroring the `DatabaseConnection` UUID String(36) primary-key pattern:

```python
id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True, max_length=36)
```

Fields:

- `id`
- `connection_name` unique string
- `bucket_name`
- `region_name: Optional[str]`
- `access_key_encrypted`
- `secret_key_encrypted`
- `role_arn: Optional[str] = None`
- `created_at`
- `updated_at`
- `is_active: bool = True`

No serial integer primary key is allowed.

### `app/models/s3_ingest_job.py`

Add `S3IngestJob` SQLModel with UUID String(36) primary key:

```python
id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True, max_length=36)
s3_connection_id: str = Field(foreign_key="s3_connections.id", max_length=36, ondelete="CASCADE")
```

Fields:

- `id`
- `s3_connection_id`
- `status` enum: `pending`, `in_progress`, `completed`, `failed`, `cancelled`
- `continuation_token: Optional[str] = None`
- `started_at`
- `completed_at`

Per Gate 1 R3 fold M7, there is no `dataset_id` FK at the job level.

### `app/models/s3_ingest_file_entry.py`

Add `S3IngestFileEntry` SQLModel with UUID String(36) primary key:

```python
id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True, max_length=36)
ingest_job_id: str = Field(foreign_key="s3_ingest_jobs.id", max_length=36, ondelete="CASCADE")
dataset_id: Optional[str] = Field(
    default=None,
    foreign_key="dataset_records.id",
    max_length=36,
    ondelete="SET NULL",
)
```

Fields:

- `id`
- `ingest_job_id`
- `object_key`
- `object_size_bytes`
- `status` enum
- `dataset_id: Optional[str]`, nullable until DatasetRecord creation after ingest
- `local_temp_path: Optional[str] = None`
- `error_detail: Optional[str] = None`

One row represents one S3 object. This preserves N-objects-to-N-datasets cardinality.

### `app/services/s3_credential_service.py`

Add a new service mirroring the existing database credential service shape while reusing the shared Fernet provider:

```python
from app.services.db_credential_service import _get_fernet


def encrypt_secret(plaintext: str) -> str:
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt_secret(ciphertext: str) -> str:
    return _get_fernet().decrypt(ciphertext.encode()).decode()
```

The service reuses `VECTORAIZ_SECRET_KEY` via `_get_fernet()` per the Q2 Fernet reuse decision. It does not call the database-password helper names directly because S3 secret semantics and field names differ.

## Acceptance Criteria

- AC-S3-CH1-1: s3_credential_service.py exposes encrypt_secret/decrypt_secret reusing _get_fernet() from db_credential_service per Q2; passes round-trip tests.
- AC-S3-CH1-2: app/models/s3_connection.py defines S3Connection SQLModel with id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True, max_length=36) — UUID String(36) PK mirroring DatabaseConnection at app/models/database_connection.py:23 (which uses `id: str = Field(primary_key=True, max_length=36)`); plus connection_name unique, bucket_name, region_name Optional, access_key_encrypted, secret_key_encrypted, role_arn Optional, created_at + updated_at + is_active. NO serial int PK.
- AC-S3-CH1-3: app/models/s3_ingest_job.py defines S3IngestJob SQLModel with id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True, max_length=36) — UUID String(36) PK consistent with codebase convention (FulfillmentLog/RawFile/RawListing/Notification all use UUID strings); s3_connection_id: str = Field(foreign_key='s3_connections.id', max_length=36, ondelete='CASCADE'); status enum, continuation_token Optional, started_at + completed_at; per R3 fold M7: NO dataset_id FK at job level.
- AC-S3-CH1-4: app/models/s3_ingest_file_entry.py defines S3IngestFileEntry SQLModel with id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True, max_length=36); ingest_job_id: str = Field(foreign_key='s3_ingest_jobs.id', max_length=36, ondelete='CASCADE'); object_key str, object_size_bytes int, status enum, dataset_id: Optional[str] = Field(default=None, foreign_key='dataset_records.id', max_length=36, ondelete='SET NULL') — CORRECTED from datasets(dataset_id) varchar(8) per convergent HIGH; local_temp_path Optional per R4 fold M10, error_detail Optional.
- AC-S3-CH1-5: Alembic migration alembic/versions/2026MMDD_XXX_s3_source_connector_schema_s684.py with down_revision='018_raw_files_metadata' (corrected to actual chain head, NOT determined-at-author-time DB-state-dependent guidance); all PKs sa.Column('id', sa.String(36), primary_key=True); FK references all sa.String(36); dataset_records foreign key target corrected from datasets(dataset_id) to dataset_records.id; indexes idx_s3_ingest_jobs_connection_id + idx_s3_ingest_file_entries_job_id + idx_s3_ingest_file_entries_dataset_id; downgrade drops indexes + tables in reverse order.
- AC-S3-CH1-6: requirements.txt adds boto3>=1.34,<2.0 (botocore transitive); pip install + import succeeds.
- AC-S3-CH1-7: Chunk 1 SHIPS INDEPENDENTLY of relax backend chunk 1 merge — no runtime call into ai-market-backend in chunk 1.
- AC-S3-CH1-8: Test plan covers credential failure: test_s3_credential_service_decrypt_failure_on_invalid_fernet_token simulates invalid Fernet token using existing decrypt pattern at app/services/db_credential_service.py:50; explicit deferral list for S3 error classes (chunk 2): NoSuchBucket / AccessDenied / NetworkError / NoSuchKey-on-resume.

## Tests Planned For Chunk 1

1. `test_s3_credential_service_encrypt_decrypt_round_trip`
2. `test_s3_credential_service_uses_shared_fernet_key`
3. `test_s3_credential_service_decrypt_failure_on_invalid_fernet_token` (new per MP_MED2)
4. `test_s3_connection_model_serialization`
5. `test_s3_ingest_job_model_serialization`
6. `test_s3_ingest_file_entry_model_serialization_with_optional_dataset_id`
7. `test_s3_ingest_file_entry_local_temp_path_optional_field_present_per_r4_m10`
8. `test_alembic_migration_creates_three_tables_with_correct_constraints`
9. `test_alembic_migration_downgrade_drops_all_tables_in_reverse_order`
10. `test_boto3_import_after_requirements_install`

## S3 Error Classes Deferred To Chunk 2

Chunk 1 covers credential encryption/decryption and invalid-token failure only. The following S3 connector runtime error classes are explicitly deferred to chunk 2:

- `NoSuchBucket`
- `AccessDenied`
- `NetworkError`
- `NoSuchKey-on-resume`

## Estimated LoC

Estimated implementation size is `~200-300 LoC`.

Breakdown:

- 3 SQLModel files: ~90 LoC, including UUID PKs and FK String(36) cascades.
- Alembic migration: ~75 LoC, including 3 tables, FKs, indexes, and downgrade.
- `s3_credential_service.py`: ~25 LoC.
- `requirements.txt`: 1 LoC.
- Tests: ~80-110 LoC for 10 tests, including the credential failure test.

## Merge Sequence

Chunk 1 can merge to vectoraiz-monorepo main as soon as it is reviewed clean. It has no dependency on ai-market-backend chunk 1.

Chunks 2 and 3 are also independent because they exercise the schema and service contracts only. Chunk 4, the integration with `processing_service.create_dataset` and `marketplace_push_service`, is ship-blocked behind relax backend chunk 1 merge per M4 R1 hard-blocker.

## Out Of Scope For Chunk 1

- `S3Connector` service: boto3 client cache, ListObjectsV2 paginator, resume logic - chunk 2.
- Management API CRUD endpoints - chunk 3.
- Integration into `processing_service.create_dataset` - chunk 4.
- Frontend UI (`S3ConnectionPage.tsx`) - chunk 5.
- `trust_level` field semantics for S3-sourced listings - deferred per relax open Q1.

## Chunk Decomposition Overview

- Chunk 1: vectoraiz schema + Alembic migration + `s3_credential_service` + boto3 requirements addition. No runtime call into `ai-market-backend`; ships independently of relax backend chunk 1.
- Chunk 2: `s3_connector.py` service with boto3 client cache, single `threading.Lock` per R3 fold M8, `asyncio.to_thread` bridge for async, credential lookup via `s3_credential_service`, `head_bucket` region auto-detect per Q3, ListObjectsV2 paginator with continuation-token persistence, and resume cleanup per R3 fold M9 using persisted `local_temp_path` per R4 fold M10.
- Chunk 3: Management API endpoints in `app/routers/s3_connection.py` mirroring `app/routers/database.py`; ConnectionCreate/Update/Response schemas; IAM least-privilege template documentation per Q1.
- Chunk 4: Integration with `processing_service.create_dataset`: `S3IngestFileEntry` to `DatasetRecord` wiring via processing service. Ship-blocked behind relax backend chunk 1 merge.
- Chunk 5: Frontend UI: `S3ConnectionPage.tsx` mirroring `DatabasePage.tsx`; ingest dashboard with job/file-entry status tables.

## Verification Evidence From R1 Fold

- `alembic/versions/001_initial_bq111_tables.py:22` confirms `dataset_records.id` is `sa.String(36)` primary key.
- `app/models/database_connection.py:23` confirms the UUID String(36) PK pattern: `id: str = Field(primary_key=True, max_length=36)`.
- Alembic chain head was resolved by the Mars S691.W:T4 walk as `018_raw_files_metadata`, descending from `017_sync_state`.
- `boto3` was verified absent from `requirements.txt` at the R1 baseline.
- `app/services/db_credential_service.py` exposes `_get_fernet()` and decrypt behavior used for the credential failure test pattern.

## R1 Mandate Delta Mapping

- CONVERGENT_HIGH_FK_ERROR: Applied in `alembic/versions/2026MMDD_XXX_s3_source_connector_schema_s684.py`, `app/models/s3_ingest_file_entry.py`, AC-S3-CH1-4, AC-S3-CH1-5, and "Verification Evidence From R1 Fold". FK target is now `dataset_records.id` with `String(36)` and `SET NULL`.
- AG_HIGH_UUID_PK_CONVENTION: Applied in `app/models/s3_connection.py`, the Alembic `s3_connections.id` column, AC-S3-CH1-2, AC-S3-CH1-5, and "Verification Evidence From R1 Fold". `s3_connections.id` is now UUID String(36), not serial int.
- AG_MED_UUID_PK_CASCADE: Applied in `app/models/s3_ingest_job.py`, `app/models/s3_ingest_file_entry.py`, the Alembic `s3_ingest_jobs` and `s3_ingest_file_entries` tables, AC-S3-CH1-3, AC-S3-CH1-4, and AC-S3-CH1-5. Child PKs and all relevant FKs are String(36).
- MP_MED1_ALEMBIC_HEAD_RESOLVED: Applied in `alembic/versions/2026MMDD_XXX_s3_source_connector_schema_s684.py`, AC-S3-CH1-5, and "Verification Evidence From R1 Fold". `down_revision` is now `018_raw_files_metadata`.
- MP_MED2_CREDENTIAL_FAILURE_TEST: Applied in AC-S3-CH1-8, "Tests Planned For Chunk 1", and "S3 Error Classes Deferred To Chunk 2". Test count is now 10 and S3 error classes are explicitly deferred to chunk 2.
- MP_LOW_LOC_ESTIMATE: Applied in "Estimated LoC". Estimate is now `~200-300 LoC` with the requested breakdown.
