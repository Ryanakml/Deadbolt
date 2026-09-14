# M1 SDK Bundles Acceptance Evidence

**Milestone:** M1 - One end-to-end execution through the real contract
**Issue:** #9 - Build TypeScript tasks and linear workflows into immutable local bundles
**PR:** #57
**Date:** 2026-09-14

This report records the evidence boundary for the TypeScript SDK, linear workflow builder,
immutable local bundle manifest, Node runner contract, and Go-to-Node worker boundary.
It intentionally separates implemented functionality from hosted CI, staging deployment,
and real-path acceptance in accordance with Blueprint section 30.

## Status Summary

| Gate                   | Status           | Evidence                                                                                                                                                                                                                                                                                                                                                |
| ---------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Implemented            | PASS             | SDK task/workflow builders, immutable bundle manifest generation, OpenAPI client, runner `stepId` enforcement, Go worker `TaskInput.stepId`, digest fail-closed behavior, and sanitized completion errors are implemented in PR #57.                                                                                                                    |
| Automated tests passed | PASS             | Local `pnpm run test` passed with 208 SDK tests and 8 runner tests on the current branch. Node 24 parity checks passed for both package test launchers.                                                                                                                                                                                                 |
| Hosted CI passed       | PASS             | The PR description and conversation record the exact hosted CI run for the current head. Hosted Foundation contracts include lint, gofmt, typecheck, JavaScript tests, Go race tests, vet, contract/parity checks, audit, Docker build, Compose validation, clean-clone boot smoke, secret scan, and SP-03 native addon checks for `amd64` and `arm64`. |
| Deployed to staging    | NOT YET VERIFIED | No real staging deployment evidence for this PR head has been attached. The clean-clone Compose boot smoke is CI evidence only; it is not a substitute for the build-once staging pipeline.                                                                                                                                                             |
| Acceptance verified    | NOT YET VERIFIED | The real staging A -> B -> C execution, artifact provenance, worker recovery, negative bundle-integrity case, runtime incompatibility rejection, and API-observed error redaction remain gated by the M1 staging acceptance path.                                                                                                                       |

## Implemented Scope

- `defineTask`, `TaskDefinition`, and `TaskContext` with explicit recovery policy declarations.
- `defineWorkflow`, `input`, `output`, and `literal` for strict M1 linear DAG workflows.
- Local deployment manifest generation pinned to manifest version, protocol major, Node runtime
  major, target OS, target architecture, dependency lock digest, and executable bundle digest.
- Fail-closed digest behavior: deployment builds require real dependency lock material or an
  externally verified lock digest, and real executable bundle material or an externally verified
  artifact digest.
- SDK client behavior for deployment registration, idempotent run creation, status reads, and
  terminal result polling. HTTP `202 Accepted` is treated as persisted acceptance, not workflow
  completion.
- Runner and Go worker protocol alignment requiring `stepId` as a distinct identity from
  `operationId`; missing `stepId` is rejected at the runner boundary.
- Durable completion errors omit raw stack traces and redact sensitive-looking values; diagnostic
  stacks are routed through the redaction-aware logger.

## Automated Evidence

Run from the repository root unless noted:

```bash
pnpm --filter @runtime/sdk run build
pnpm --filter @runtime/runner run build
pnpm --filter @runtime/sdk run typecheck
pnpm --filter @runtime/runner run typecheck
pnpm --filter @runtime/sdk run test
pnpm --filter @runtime/runner run test
pnpm run test
pnpm exec prettier --check scripts/run-node-tests.mjs runner/node/package.json sdk/typescript/package.json runner/node/src/context.ts runner/node/src/protocol.ts runner/node/src/runner.ts runner/node/tests/fixtures/sample-task.js runner/node/tests/runner.test.js sdk/typescript/src/bundle.ts sdk/typescript/src/context.ts sdk/typescript/tests/bundle.test.js sdk/typescript/tests/context.test.js
git diff --check
```

Node 24 parity was checked from each package directory:

```bash
npx -y node@24.21.0 ../../scripts/run-node-tests.mjs tests
```

Hosted CI evidence is recorded in the PR description and conversation for the exact current head.
For the runtime-changing head `c2058b569132ab03fad98d08614d014bc076ff8d`, hosted run
`34818801141` passed:

- `contracts`: PASS
- `SP-03 native addon (amd64)`: PASS
- `SP-03 native addon (arm64)`: PASS

## Staging Evidence Required Before Production Promotion

The following checks are not claimed by this PR until the real staging path has run and produced
redacted evidence:

1. Build the customer TypeScript tasks once, register the immutable deployment through the real
   control-plane API, create an A -> B -> C run through the SDK with a real `Idempotency-Key`,
   and poll the run until `SUCCEEDED`.
2. Record redacted deployment, run, request, event, and worker attempt IDs proving that B consumed
   A's committed output and C consumed B's committed output.
3. Record artifact provenance for the exact staging deployment: Git SHA, bundle SHA-256,
   dependency-lock digest, target architecture, Node runtime major, protocol major, manifest
   version, and the canonical `/version` output from the running component.
4. Exercise a controlled worker/runner interruption and prove already committed successful work is
   not blindly re-executed. Retry attempts must receive new `attemptId` values while the logical
   invocation retains the same stable `operationId`.
5. Prove stale ownership or stale completion cannot overwrite the current owner/result.
6. Verify worker-side bundle integrity using the real artifact, including a negative mismatch case
   that refuses execution before the handler starts.
7. Verify architecture/runtime incompatibility is rejected before the child runner starts.
8. Confirm API-observed task failures do not expose stack traces, filesystem paths, secret values,
   authorization material, or unredacted implementation details.
9. Confirm manifest, bundle, result, and event payloads contain allowed secret names only, never
   secret values.

## Rollout, Rollback, and Compatibility Impact

- **Database schema and migrations:** no new database migration is introduced by PR #57.
- **Runtime protocol compatibility:** the Go-to-Node worker protocol now requires `stepId`. The
  supported runtime combination is a worker that sends `stepId` together with a runner that enforces
  it. A newer runner rejects older worker payloads that omit `stepId` with `INVALID_INPUT` before
  customer code starts.
- **Bundle compatibility:** deployment manifests produced by this SDK require real lock/bundle
  provenance. Older packagers that emit synthetic digests must not be treated as equivalent evidence.
- **Rollback behavior:** rollback should restore the previous known-good immutable artifact/image
  digest and matching worker/runner version. Binary rollback does not imply database rollback, and
  production promotion remains release-owner governed after staging acceptance.
