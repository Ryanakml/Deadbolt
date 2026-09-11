# SP-01 — executable contract conformance

Scope: issue #2, dependent on repository preparation in #1. Sources: blueprint §§6, 9–10, 12, 14, 20, 33. Requirements: REQ-EXEC-01, REQ-DUR-01. This report concerns contract boundaries; it does not prove database, authorization, worker-process or recovery invariants at runtime.

## Candidates and decision

| Candidate                                                     | Evaluation                                                                                                                | Decision                                                                                                                                                    |
| ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Go `encoding/json` alone                                      | `go run ./tests/candidates` checks HTML escaping, supplementary Unicode key ordering, duplicate names and lone surrogates | Rejected as the canonicalization/strict-parser boundary; four adversarial cases disagree with the required contract                                         |
| JavaScript `JSON.parse` + `JSON.stringify` alone              | `node scripts/check-candidates.mjs` checks key order, duplicates and lone surrogates                                      | Rejected without strict parsing and JCS; three adversarial cases disagree                                                                                   |
| `canonicalize` 5.0.0 (TypeScript/Node)                        | Shared expected raw-JSON/hash fixtures plus JavaScript-only rejection tests                                               | Selected behind a strict JSON guard; dependency alone permits `toJSON` and non-JSON values, which the guard rejects without invoking accessors              |
| `cyberphone/json-canonicalization` commit `19d51d7fe467` (Go) | Same raw-JSON/hash fixtures                                                                                               | Selected with a scalar-root adapter, strict Unicode-pair/depth guard and safe-integer check; the upstream parser alone is not the complete product contract |
| `jsonschema` 1.5.0 (TypeScript/Node)                          | Shared schema, graph and worker fixtures; interpreter code inspected for dynamic code evaluation                          | Selected only for the explicitly constrained common subset. It is not advertised as a general Draft 2020-12 engine                                          |
| `santhosh-tekuri/jsonschema/v6` 6.0.3 (Go)                    | Same fixtures, Draft 2020-12 selected explicitly                                                                          | Selected with the same subset meta-schema and domain validation                                                                                             |

The chosen libraries are version/checksum locked. The custom layer is limited to product restrictions, mapping/expressions and graph semantics; it does not execute workflow JavaScript. No `eval`, dynamically generated JavaScript validators, network schema loading, clock, randomness or customer task execution is used. Go/TS independently execute the shared fixtures. The `.invalid` schema IDs are identifiers; they are never fetched.

Native serializers remain useful after strict parsing for ordinary transport. They are not substituted for JCS. The Go JCS dependency is an older immutable commit rather than a newly invented serializer; future maintenance changes must rerun the corpus. This selection makes no unsupported claim about upstream maintenance guarantees.

## Executable acceptance evidence

The corpus is `contracts/fixtures/conformance.json`. It currently covers:

- Canonical request/result/deployment hashes; scalar roots, negative zero, exponent boundaries, safe integers, Unicode ordering and no normalization.
- Duplicate raw keys including escaped equivalents; invalid Unicode/UTF-8, nonfinite/unsafe values, trailing data, nesting 32/33 and schema size at/over 64 KiB.
- JSON Pointer root, escaping, array indexes, literal reserved keys, missing vs null, defaults, absent ancestors and inherited-property rejection.
- All allowed choice operators, strict types, invalid operators/arity, nested Boolean expressions, and no short-circuit hiding of malformed expressions. This is a conformance evaluator, not V1 choice-node execution.
- Payload schema validation, tagged `oneOf`, bounds, required/extra fields, forbidden schema `$ref`, format/custom/unsupported keywords.
- Linear graph acceptance, cycle/duplicate/dependency/ancestor/leaf checks, recovery defaults and idempotency-window minimum, malformed nodes, MVP capability rejection, deployment compatibility metadata.
- Worker request versions, request/session identity and outcome/digest requirements, including negative fixtures.

Exact commands from the repository root:

```sh
pnpm install --frozen-lockfile --ignore-scripts
pnpm test
pnpm typecheck
pnpm lint
go test -race ./...
go vet ./...
pnpm check:config
pnpm check:contracts
pnpm check:parity
node scripts/check-candidates.mjs
go test ./internal/contracts -run '^$' -fuzz FuzzRawJSON -fuzztime 10s
pnpm audit
go mod verify
node scripts/install-tools.mjs gitleaks govulncheck
bin/gitleaks git --redact --no-banner --log-opts="--all"
bin/govulncheck ./...
```

The PR records the final commit and actual local/hosted results after these checks execute. The previous contributor statement “100% compliance / issue #2 acceptance criteria fulfilled” was removed because eight tests did not establish those claims. A passing corpus is evidence for its cases, not proof that no defect can exist.

## Delivery boundaries

- **Implemented:** versioned contracts, Go/TS validation/mapping/hashing, fixtures, workspace preparation and CI definitions.
- **Automated tests / security scans:** report only the final executed results in PR evidence, including fixture counts and any failures.
- **Hosted CI:** only a completed run linked to the final head is evidence; workflow files and a CodeRabbit status are not evidence.
- **Deployed:** no. There is no application image, database migration or runtime deployment in issues #1–#2.
- **Acceptance:** executable contract acceptance is separate from live runtime acceptance. External staging selections remain unresolved as documented in `deploy/provisioning.json`; dependent deployment/readiness is blocked until supplied.

Future control fields do not enable V1 execution. Current/previous-minor compatibility, actual worker auth/fencing, transaction/outbox behavior, object storage and recovery tests belong to their implementing issues. No runtime invariant is claimed proven by a schema file.

## Local execution record — 2026-09-11

On macOS arm64 with Go 1.27.1, Node 24.21.0 and pnpm 10.24.0: frozen install, configuration/pin checks, formatting, typecheck, SDK build/tests (**172 passed**), OpenAPI/schema generation checks, **165 shared expected fixtures with Go/TS parity**, candidate comparisons, Go race tests, vet and module verification passed. The Go parser fuzz run completed **287,232 executions** in about 11 seconds without a failure. This is a bounded fuzz run, not exhaustive proof.

`pnpm audit` and `govulncheck ./...` reported no known vulnerabilities after updating YAML to 2.9.0 and `golang.org/x/text` to 0.39.0. The earlier YAML finding and unused-module x/text finding were fixed rather than waived. Gitleaks scanned a tracked-source snapshot with redacted output and found no leaks; commit-history scanning and hosted CI evidence are recorded in the PR after commit/push.

Registry manifests verified five pinned image indexes for Linux amd64/arm64. Container execution/image scanning was not performed: the local Docker daemon was unavailable, and no runtime Dockerfile/Compose deployment is implemented here. sqlc/goose installation/version execution is additionally checked by hosted CI; their version pins were verified against the published upstream releases locally. External provisioning and live staging acceptance remain unverified, as listed above.
