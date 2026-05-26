# Gate 1 R2 Verdict — AG

**BQ:** BQ-AIM-DATA-S3-STS-FULFILLMENT-S711
**Branch:** spec/bq-aim-data-s3-sts-fulfillment-s711-gate1
**Spec SHA reviewed:** 378447cf5575756c6585572e47f3d472aaddf726
**Reviewer:** AG (Gemini 3.5 Flash on Vertex via google-genai SDK; NOT Anthropic)
**Round:** 2
**Mode:** open_response (inline spec body to sidestep R1 file-reading turn cap exhaustion)
**Dispatch task_id:** cab8608c
**Dispatched at:** 2026-05-26T14:38:00Z
**Completed at:** 2026-05-26T14:48:07Z
**Persistence:** Committed by Primary (Vulcan-S712) — AG sandbox denies file writes per Council post-S528 contract

---

## Verdict

**CHANGES_REQUESTED**

---

## Mandate Verification

- **HIGH-1**: **ADDRESSED**
  - Evidence: §1 and §2.2 explicitly lock signing ownership to the customer's vectorAIz container (ai.market platform never holds `role_arn` or `external_id`), define the `Listing.raw_metadata` JSON contract, resolve the C6-to-C4 dependency via internal-only JWT-authenticated `/sign-url` calls, and document the agent-QA path as a separate read-only seller-side role.
- **HIGH-2**: **ADDRESSED**
  - Evidence: §2.2 and §3.4 correctly trace the token issuance chain from `orders.py:214` through `order_service.issue_download_token` to `security.py:188` (`create_delivery_token`), specifying that the S3 URL is carried in the existing JWT `delivery_config` without conflating it with the hash-based `FulfillmentDownloadToken`.
- **MED-3**: **ADDRESSED**
  - Evidence: §6 R4 explicitly rewords the presigned URL replay risk as "BOUNDED-EXPOSURE" and acknowledges the 15-minute post-issuance window during which platform-side revocations cannot stop an in-flight S3 download.
- **MED-4**: **ADDRESSED**
  - Evidence: §3.4 and §5 extend the `STSCredentialBroker` cache key to `(role_arn, region, session_purpose)` and explicitly test for distinct clients based on order vs. agent-QA purposes.
- **LOW-5**: **ADDRESSED**
  - Evidence: §5 explicitly lists both `test_aim_data_onboarding_resume_from_abandoned_pre_row_s3connection` and `test_s3_sts_scan_pagination_resume_across_multiple_pages` in the integration test suite.

---

## NEW_FINDINGS (R2)

### FINDING-HIGH-1-R2 — Onboarding Flow Contradiction (HIGH)

There is an irreconcilable conflict between the database schema constraints and the onboarding walkthrough flow.

- §3.1 states that database constraints reject empty `role_arn` values
- §3.2 states that `role_arn` is immutable on `PUT` requests
- However, §2.3 describes a walkthrough flow where a pre-row `S3Connection` is persisted in Step 1 with `status=onboarding` BEFORE the seller pastes the `role_arn` in Step 3

Under the current spec, Step 1 will fail database validation. Even if a placeholder were used, Step 3 would fail because `role_arn` cannot be updated via `PUT`.

### FINDING-MEDIUM-2-R2 — Disk Space Exhaustion Risk on Ephemeral Scan Downloads (MEDIUM)

§2.1 describes a scan flow where vectorAIz downloads S3 objects to `upload_dir/safe_filename` to run `create_dataset` and then immediately deletes them. For large buckets containing multi-gigabyte files, downloading these files to the local container's disk can easily exhaust disk space or cause severe performance degradation and timeouts during the extraction cycle. The spec lacks any mitigation, size-limit checks, or streaming extraction strategies for these ephemeral downloads.

### FINDING-LOW-3-R2 — Undocumented session_purpose for Scan and Probe Operations (LOW)

§3.4 specifies that the `STSCredentialBroker` cache key is `(role_arn, region, session_purpose)` and defines purposes for buyers (`order-{order_id}`) and agent QA (`agent-qa-{listing_id}`). However:

- Background scan jobs (§2.1) also call the broker / assume-role but no `session_purpose` is defined for them
- Onboarding probes (§2.3) also assume-role but no `session_purpose` is defined for them

If they use dynamic or missing purposes, it could lead to cache misses or validation failures at the gatekeeper.

---

## ONE_PARAGRAPH_SUMMARY

The R2 spec is exceptionally well-structured and successfully folds in all five R1 mandates, demonstrating a rigorous adherence to Max's locked non-custodial signing-ownership architecture. The dual-path separation for buyer fulfillment and agent QA is cleanly mapped, and the JWT delivery-token citation drift has been perfectly resolved. However, the spec cannot be approved for implementation in its current state due to a critical logical contradiction in the onboarding flow (where database constraints and immutability rules prevent the creation and subsequent update of the onboarding pre-row) and an operational risk regarding local disk exhaustion during ephemeral scan downloads. Once these API and schema contradictions are resolved, the spec will be fully ready for Gate 1 approval.
