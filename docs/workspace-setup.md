# Clean-clone setup — M0 issues #1 and #2

This workspace currently implements contract validation and conformance tests. It does not yet start the control plane, execute customer tasks, migrate PostgreSQL, or deploy staging. `docs/blueprint.md` remains authoritative.

## Exact baseline

| Tool       | Pin     | Why                                                                                  |
| ---------- | ------- | ------------------------------------------------------------------------------------ |
| Go         | 1.27.1  | Current stable release selected from go.dev on 2026-09-11; same exact compiler in CI |
| Node.js    | 24.21.0 | Supported Node 24 LTS; replaces the contributor's EOL Node 20 baseline               |
| pnpm       | 10.24.0 | Retained exact package-manager pin; frozen dependency lock                           |
| TypeScript | 5.9.3   | Strict NodeNext compilation, package lock records all transitive versions            |
| pgx        | 5.11.0  | Explicit PostgreSQL driver baseline in go.mod/go.sum; SQL implementation is issue #3 |
| sqlc       | 1.31.1  | Explicit-query generation; pinned in tools.lock.json                                 |
| goose      | 3.28.0  | Ordered SQL migration tooling; pinned in tools.lock.json                             |

Install Go and Node from their official distributions (or an existing version manager reading `.tool-versions`); verify published SHA-256 checksums. Git and outbound access to the package registries are required. With Node on PATH:

```sh
npm install --global pnpm@10.24.0
pnpm install --frozen-lockfile --ignore-scripts
pnpm check:config
pnpm test
pnpm typecheck
pnpm lint
go test -race ./...
go vet ./...
pnpm check:contracts
pnpm check:parity
go mod verify
pnpm audit
node scripts/install-tools.mjs gitleaks govulncheck
bin/gitleaks git --redact --no-banner --log-opts="--all"
bin/govulncheck ./...
```

Commands run from the repository root. The root test command builds the SDK first; it works without a preexisting `dist`. If the default Go workspace location is unavailable, set `GOPATH` to a writable directory. `bin/`, dependency caches, `dist/`, and secret files are ignored. Never paste private environment files into a report.

For the next SQL owner, install the exact selected tools without creating schema or migrations:

```sh
node scripts/install-tools.mjs sqlc goose
bin/sqlc version
bin/goose -version
```

SQL is pgx + explicit queries/sqlc, not an ORM. There are no migrations to execute in this PR. Issue #3 must add its empty/previous-schema validation before using the migration tooling against a database.

## Images and later local-stack prerequisites

`deploy/images.lock.json` contains registry-resolved immutable multi-architecture index digests for the Go builder, Node runner, PostgreSQL, NATS, and development-only object store. Both Linux amd64 and arm64 were checked in the registry index. The digest, not a floating major tag, fixes the selected content. No hosted artifact bucket is implied by the development image.

Docker with Compose v2 is a prerequisite for the later local-stack/deployment issue, not for these pure contract tests. This PR has no Dockerfile or Compose runtime to build/start. The local Docker daemon was unavailable during review; no container execution or image security result is inferred from registry resolution. The deploying issue must scan/build the exact pinned images, measure host capacity, and record real health/version evidence.

## Generated files and checks

`node scripts/generate-contracts.mjs` regenerates SDK schema data and Go/TS canonical enums from `contracts/`. `pnpm check:contracts` rejects stale generated files, validates OpenAPI without fetching remote references, and checks security/error/version declarations. Schema IDs under `schemas.runtime.invalid` are non-resolving identifiers using the reserved `.invalid` namespace; they are not selected staging domains.

Contract fixtures include malformed raw JSON; do not rewrite them through a normal parser or regenerate expected hashes from the implementation under test. `pnpm check:parity` executes the same expected fixtures in Go and TypeScript. Future capability fields remain versioned but unavailable in the MVP validator.

See [environment-baseline.md](environment-baseline.md) for unresolved private provisioning and [the SP-01 report](reports/SP-01-manifest-conformance.md) for measured scope and evidence boundaries.
