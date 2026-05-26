# MP R2 Review — BQ-AIM-DATA-S3-STS-FULFILLMENT-S711 Gate 1

Reviewed branch SHA: `378447cf5575756c6585572e47f3d472aaddf726`
Compared against fork-point/base: `0beb7c42e96a`
R1 verdict baseline: `fcaf0730b26341ed197fac6894622b83f08e81c9`
vectoraiz predecessor checked: `cd8bb5dc`
ai-market-backend predecessor checked: `ef2e6d07`

Verdict: **APPROVE**

## R1 Mandate Verification

### HIGH-1 — Marketplace metadata contract + signing ownership

Resolved. The spec now consistently locks signing ownership to the AIM Data customer's vectorAIz container. ai.market platform is explicitly barred from holding `role_arn` or `external_id`, assuming seller roles, signing S3 URLs, or storing STS credentials.

The marketplace carrier is now limited to `Listing.raw_metadata` fields `{connection_id, vectoraiz_instance_url, object_key, bucket, region}` at §2.2 / §3.3 / C6. C6 now depends coherently on C4 by calling seller vectorAIz `/sign-url` with `connection_id`, `object_key`, and `session_purpose = "order-{order_id}"`, while vectorAIz owns `role_arn`/`external_id` internally. Agent QA is documented as a separate read-only seller-side role through the same `/sign-url` gatekeeper with `session_purpose = "agent-qa-{listing_id}"`.

### HIGH-2 — Fulfillment token citation drift

Resolved. The nonexistent `issue_fulfillment_token` references were replaced with the actual JWT delivery-token chain: `app/api/v1/endpoints/orders.py:214` calls `order_service.issue_download_token`; `app/services/order_service.py:599` calls `create_delivery_token`; `app/core/security.py:188` defines `create_delivery_token`.

The S3 URL is specified as being carried in the existing JWT delivery-token response `delivery_config`. The spec explicitly avoids introducing a new S3-specific token type and distinguishes this path from the hash-based `FulfillmentDownloadToken` / BQ-D1 system.

### MEDIUM-3 — Presigned URL revocation language

Resolved. §6 R4 now labels the risk as `BOUNDED-EXPOSURE`, states that S3 honors an already issued presigned URL until its 15-minute expiry regardless of ai-market-side revocation state, and preserves the narrower `jti` replay-prevention claim only for minting new URLs.

### MEDIUM-4 — STS cache key

Resolved. The vectorAIz STS cache key is now `(role_arn, region, session_purpose)` in §3.4, C2 acceptance criteria, C4 `/sign-url` criteria, and §6 R1. The spec defines `session_purpose` values as `order-{order_id}` for buyer fulfillment and `agent-qa-{listing_id}` for QA, and documents the v1 trade-off of one STS AssumeRole call per distinct order or QA listing. The cache is now correctly scoped to vectorAIz under the vectorAIz-signs ownership model.

### LOW-5 — Missing integration tests

Resolved. §5 now adds both requested integration tests:

- `tests/integration/test_aim_data_onboarding_resume_from_abandoned_pre_row_s3connection.py`
- `tests/integration/test_s3_sts_scan_pagination_resume_across_multiple_pages.py`

## Citation Audit

New and changed file:line references were checked against the requested predecessor SHAs. The corrected ai-market-backend citations match `ef2e6d07`: `app/api/v1/endpoints/orders.py:214`, `app/services/order_service.py:599`, `app/core/security.py:188`, `app/models/fulfillment_download_token.py:39`, and `app/api/v1/endpoints/orders.py:619`. Existing sampled vectoraiz citations still match `cd8bb5dc`, including `app/models/database_connection.py:18`, `app/routers/database.py:133-223`, `app/services/db_connector.py:108`, and `app/services/processing_service.py:294`.

## Findings

No R2 findings. All five R1 mandates are addressed, and I did not find a new substantive issue introduced by the fold.
