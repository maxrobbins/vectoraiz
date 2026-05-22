# BQ-AIM-DATA-NON-VECTOR-LISTING-METADATA-RELAX-S684 Gate 2 Chunk 2 R1 Fold

**Canonical fold body:** `infra:worker-artifact-stash:S691.W:T3:vectoraiz-chunk2-r1-fold` v=1
**Author:** Mars S691.W (2026-05-22T17:44Z)
**Base ref:** vectoraiz origin/main b6387f30
**Predecessor baseline:** `infra:worker-artifact-stash:S688.W2:T3:gate2-chunk2-relax-vectoraiz-spec-draft` v=1

R2 reviewers: dereference the canonical stash via `state_request action=get key=infra:worker-artifact-stash:S691.W:T3:vectoraiz-chunk2-r1-fold` for the full structured body (mandates_folded, acceptance_criteria_r1_folded, verification_evidence_at_b6387f30, post_fold_tests_planned_11).

## R1 mandates folded (6 total: 0 HIGH + 4 MED + 2 LOW)

1. **MP_MED1_READY_CHECK_LIST**: endpoint count corrected 9 → 13 strict-others. datasets.py:1041 RELAXES; strict checks remain at lines 783/819/872/920/954/989/1016/1069/1104/1140/1182/1308/1354 (13 total).

2. **CONVERGENT_MED_SCHEMA_CONSTRAINTS** (MP MED2 + AG MED1): `app/models/listing_metadata_schemas.py:29` `privacy_score: float = Field(1.0, description="1.0 = no PII detected, 0.0 = high PII risk")` → `Optional[float] = Field(None, ge=0.0, le=10.0, description="0-10 scale, 10.0 = no PII detected, 0.0 = high PII risk; None = not scanned")` + Optional/List import.

3. **MP_LOW_PATH_CORRECTION**: spec path corrected from approximate to exact `app/models/listing_metadata_schemas.py:29`.

4. **AG_MED3_FALLBACK_FIX**: `app/services/listing_metadata_service.py:340` `return float(pii_data.get("privacy_score", 1.0))` → `return None`; return type `Optional[float]`; catch-block also returns None.

5. **AG_MED4_TEST_SAMPLE_REGRESSION**: `tests/test_marketplace_push.py:37` SAMPLE `"privacy_score": 0.9,` → `"privacy_score": 9.0,`; `:115` assertion strips stale `# 0.9 * 10` comment.

6. **AG_LOW2_RANGE_VALIDATION_TEST**: new `test_listing_metadata_validation_fails_for_out_of_range_privacy_score` asserts ValidationError on -0.1 and 11.0. Tests count 10 → 11.

## Memory #29 catch resolved

Scale ambiguity from S688.W2:T3 catch #1 (HIGH severity, 0-1 vs 0-10) RESOLVED: pii_service.py is authoritative 0-10 emission per AG R1 verification + S687.W2 T1 pii_service audit. Stale 0-1 references in current code (schema description + marketplace_push_service.py:179 comment + *10 multiplier at :180) are all corrected by this fold. The *10 multiplier was a real production bug — push of 10.0 became 100.0 violating ai.market ListingCreate ge=0/le=10.

## Acceptance criteria (verbatim summary)

- **AC-C2-1** `datasets.py:1041` precondition `record.status not in (ProcessingStatus.READY, ProcessingStatus.PREVIEW_READY)`; 13 other READY-check endpoints REMAIN strict.
- **AC-C2-3** `_compute_privacy_score` returns None when no PII scan exists AND emits log warning; scan-present-missing-field fallback also None.
- **AC-C2-4** `marketplace_push_service.py:180` REMOVES `* 10` multiplier + stale comment; pass-through privacy_score directly; emit payload.privacy_scan_status='scanned' when value, 'not_scanned' when None.
- **AC-C2-5** Scale locked at 0-10 throughout pipeline.
- **AC-C2-6** `listing_metadata_schemas.py:29` Optional[float] + ge/le + corrected description.
- **AC-C2-9** new validation test for out-of-range privacy_score.

## R3 builder dispatch readiness

On R2 dual-approve clean: this fold is ready for vectoraiz-monorepo builder dispatch (chunk 2 implementation) after backend chunk 1 (PR #80 BQ-AIM-DATA-NON-VECTOR-LISTING-METADATA-RELAX-S684 backend) merges and deploys to Railway. The vectoraiz client side MUST ship AFTER backend deploy per M4 R1 hard-blocker (Optional[float] schemas live on backend first).
