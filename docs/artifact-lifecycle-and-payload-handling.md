# Artifact Lifecycle, Integrity, and Payload Data Handling (M2 — Issue #22)

Source of truth: `docs/blueprint.md` §18, §20, §24, §27.
This document is the delivery artifact for Issue #22.

## 1. Payload data handling

- Run/step inputs and outputs stay inline UTF-8 JSON while each value is
  at most **256 KiB** after serialization. Larger results must travel as
  typed artifact references (`{"$artifact": "<id>"}`) instead of JSONB.
- One object is at most **100 MiB**; each environment reserves at most
  **1 GiB** of artifact bytes before upload. Reservations that would
  exceed either bound fail closed (`413` / `429`) before any row or URL
  is minted.
- Task secrets never leave customer workers. Artifact bytes pass through
  the control plane only for size/SHA verification and orphan collection;
  presigned URLs carry signature material and are never written to logs.
- Treat artifacts as untrusted: downloads force `attachment` disposition
  with `application/octet-stream` so bytes never execute in the dashboard
  origin, and workers receive single-object URLs only — never bucket-list
  credentials.

## 2. Lifecycle

```text
reserve → PENDING_UPLOAD → PUT bytes → finalize → READY → download
              ↓ (24h, unfinalized)            ↓ (24h, unreferenced)
            EXPIRED                          DELETED
```

1. **Reserve** (`POST /v1/artifacts`): binds a live attempt (epoch must
   match), checks caps/quota, inserts `PENDING_UPLOAD`, and mints a
   single-object presigned PUT valid for 5 minutes.
2. **Upload**: the worker PUTs exactly the reserved bytes to object
   storage. S3 upload is never inside a DB transaction.
3. **Finalize** (`POST /v1/artifacts/{id}/finalize`): the server stats the
   object and streams its SHA-256. Size or digest mismatch fails closed
   (`422`) and leaves the row pending for re-upload.
4. **Associate**: only a `READY` artifact bound to the completing attempt
   by current ownership may complete a step; the step output becomes the
   typed reference. Anything else fails the step deterministically
   without rerunning the producer.
5. **Consume**: mapped inputs carrying `$artifact` references are verified
   (`READY` + bytes present at the reserved size) at claim time. A
   missing or truncated object fails the consumer with
   `ARTIFACT_UNAVAILABLE`; the successful producer is never rerun.
6. **Download** (`GET /v1/artifacts/{id}`): returns a 5-minute signed GET
   for `READY` artifacts inside the caller's scope.
7. **Collect**: the reconciler sweep batches orphans — unfinalized uploads
   past 24h become `EXPIRED`; finalized artifacts past 24h that no run
   output references become `DELETED`. Referenced artifacts (including
   all active-run outputs) are retained, and object deletion is
   best-effort idempotent so restarts safely resume collection.

## 3. Failure matrix for artifacts

| Situation                                      | Behavior                                                  |
| ---------------------------------------------- | --------------------------------------------------------- |
| PUT succeeds, finalize/complete crashes (F-20) | Orphan collected after 24h grace, bytes deleted           |
| Size/SHA mismatch at finalize                  | `422`, row stays pending, producer may re-upload          |
| Stale epoch, foreign session, lapsed ownership | `409 NOT_OWNED`, no state change                          |
| Cross-environment / cross-tenant access (F-21) | `404` without leaking scope                               |
| Missing/corrupt bytes behind a READY row       | Consumer fails `ARTIFACT_UNAVAILABLE`; producer untouched |
| Download of pending/expired/deleted            | `409` / `404` respectively                                |
| Store unconfigured                             | `503 ARTIFACT_STORE_UNAVAILABLE` on every operation       |

## 4. Operations guide

- **Storage configuration** (`DEADBOLT_ARTIFACTS_S3_*`): endpoint, bucket,
  access/secret key, region, TLS flag. Distinct from the `DEADBOLT_STORAGE_S3_*`
  backup/WAL variables. The control plane ensures the bucket at boot and
  warns (does not crash) when the store is unreachable.
- **Quota incidents**: `429 STORAGE_QUOTA_EXCEEDED` means the 1 GiB
  environment reservation is full — collect orphans via the sweep, delete
  unreferenced data, or request a measured cap review. Never raise caps
  through API payloads.
- **Integrity incidents**: `ARTIFACT_UNAVAILABLE` on a consumer means
  bytes were lost behind a committed reference. Do not rerun the
  producer blindly: inspect the step output reference, check provider and
  storage-side logs, and resolve through reconciliation like any unknown
  outcome.
- **Restore caution**: a database restored older than an external upload
  follows the disaster reconciliation procedure before workers resume;
  see the worker recovery runbook.

## 5. Migration and rollout

- Additive migrations only (`00022_artifact_attempt_binding.sql` binds
  uploads to owning attempts; earlier `artifacts` schema stands).
  No backfill; binary rollback does not roll the database back.
- No topology changes: the same control-plane binary serves the new
  routes; MinIO (local) or external S3-compatible storage holds bytes.
