# M1 Execution Gate Acceptance — Issue #16

This report is the evidence index for the first real execution gate. It separates code/test evidence from staging acceptance; a passing unit test or clean image build is not treated as staging proof.

## Gate contract

The gate must use the public CLI/API to create one run, execute a real Node.js child process, commit each result before acknowledging completion, advance a linear `A → B → C` workflow, and expose the same committed state through the API, Inspector, and logs.

## Automated evidence

| Evidence | Status | Source |
| --- | --- | --- |
| SDK bundle and workflow contracts | PASS | `pnpm --filter @runtime/sdk run test` — 208 tests passed. |
| Node runner execution | PASS | `pnpm --filter @runtime/runner run test` — 8 tests passed. |
| CLI package tests | PASS | `go test ./internal/cli` in the Linux acceptance image. |
| CLI E2E assertions | IMPLEMENTED | `tests/integration/cli_e2e_test.go` checks deployment pinning, three successful steps, committed attempts, final output, and committed logs. |
| Local PostgreSQL-backed CLI E2E | PASS | `TestCLIEndToEndDeveloperJourney` completed init/build/deploy, two workers, activation, `A → B → C`, inspect, and logs. |
| Hosted staging execution | NOT YET VERIFIED | Must run this PR through the build-once staging workflow and record `/version`, image digest, run ID, event sequence, and redacted logs. |

## Reproducible local acceptance

```bash
docker build -f deploy/Dockerfile.m1-e2e -t deadbolt-m1-e2e deploy
docker run --rm --add-host=host.docker.internal:host-gateway \
  -v "$PWD:/workspace" -w /workspace \
  -e TEST_DATABASE_URL=postgres://deadbolt_system:<password>@host.docker.internal:5432/deadbolt?sslmode=disable \
  deadbolt-m1-e2e test ./tests/integration -run TestCLIEndToEndDeveloperJourney -count=1 -v
```

The test must show two worker agents, a `SUCCEEDED` run, three successful steps, committed task logs, and final output containing `accountId` and `deliveryId`. A `SKIP` is not acceptance.

## Staging evidence required

Record the exact deployed commit SHA, `/version` response, control-plane image digest, bundle and lock digests, deployment ID, worker IDs and advertised digest, run ID, attempt IDs, event sequence, and redacted inspect/log output. Also record negative tests for incompatible runtime, wrong tenant/pool, duplicate completion, and invalid output.

Do not record API keys, cookies, enrollment tokens, task secrets, authorization headers, or unredacted stack traces.
