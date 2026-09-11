# Environment and deployment baseline — issue #1

This is a preparation contract, not evidence of provisioned infrastructure or deployment. Source: blueprint §§7, 22, 24, 26, 27, 32–34 and the updated issue #1 staging agreement. No domain, artifact provider/bucket, human OIDC provider, KMS, or backup destination has been selected by this change.

## Agreed selections and isolation

- Reuse the existing AWS EC2 host, Linux x86_64/amd64, with a separate `deadbolt-staging` Compose project. Worker support remains Linux amd64 **and** arm64; a deployment is pinned to one architecture.
- Reserve Deadbolt-only logical names: network `deadbolt-net`, volumes `deadbolt_pgdata` / `deadbolt_natsdata`, release root `/opt/deadbolt`, configuration root `/etc/deadbolt`. These are declared implementation inputs, not claims that paths/resources exist. Issue #5 checks ownership and collisions before creating them.
- Do not reuse FlowDesk PostgreSQL, Redis, MinIO, networks, volumes or application configuration. No dependency on `flowdesk-staging` is introduced.
- Existing Caddy owns TCP 80/443 and UDP 443. Do not start another public proxy. Issue #5 inspects the existing edge and chooses a non-conflicting upstream path without joining FlowDesk application networks. No upstream port, route, reload command or network attachment is assumed here.
- Keep **GitHub Actions → GHCR → automated SSH → EC2**. No ECR, SSM or GitHub-to-AWS OIDC substitution. Human-login OIDC remains separately required.
- Build once; preserve the image digest and commit SHA through promotion. Issue #5 owns serialization, configuration validation, backup readiness, compatible migration, `/readyz` health gate, traffic switch, real smoke checks, exact `/version` SHA/digest, and rollback to the previous compatible image/configuration. Tags alone are not immutable provenance.

## Supplied capacity snapshot — not a measurement by this PR

The user reported approximately 7.6 GiB RAM total / 5.3 GiB available; root disk 96 GB total / 32 GB available; Docker images 64.7 GB with 61.3 GB reported reclaimable. These figures are not reserved capacity. Issue #5 must measure current headroom, resource allocations and retention before deployment. Reported reclaimability neither guarantees usable disk nor authorizes pruning.

## Missing provisioning inputs and readiness

`deploy/provisioning.json` records expected names, purposes and explicit **unresolved** status. No values, host fingerprints or private identifiers are stored. `pnpm check:config` validates the declaration and toolchain; it deliberately does **not** certify staging readiness.

The release owner must supply or confirm the following through private configuration channels before dependent staging deployment/readiness can proceed:

| Area              | Required selection/access                                                                                                                    | Configuration names                                                                                                                                 |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| Public routing    | Approved domain, DNS/TLS access, reviewed existing-Caddy upstream/validation/reload procedure                                                | `DEADBOLT_STAGING_DOMAIN`, `DEADBOLT_EDGE_PROCEDURE`                                                                                                |
| Artifact storage  | External S3-compatible provider, Deadbolt bucket, endpoint, region if needed, upload/finalize/checksum/versioning/lifecycle access           | `DEADBOLT_ARTIFACT_ENDPOINT`, `DEADBOLT_ARTIFACT_BUCKET`, `DEADBOLT_ARTIFACT_REGION`; credential names depend on the provider and remain unselected |
| SSH               | Authorized deployment identity, separately trusted host key, approved host/user                                                              | `DEADBOLT_DEPLOY_HOST`, `DEADBOLT_DEPLOY_USER`, `DEADBOLT_DEPLOY_SSH_KEY`, `DEADBOLT_DEPLOY_KNOWN_HOSTS`                                            |
| Registry          | GitHub Actions package publication permission; authorized host pull identity                                                                 | Publication uses scoped `GITHUB_TOKEN`; host uses `DEADBOLT_GHCR_PULL_USER`, `DEADBOLT_GHCR_PULL_TOKEN` if authentication is needed                 |
| Human identity    | Managed standards-compliant OIDC, authorization-code + PKCE, MFA, issuer/subject/JWKS, approved BFF redirects and public CLI loopback client | `DEADBOLT_OIDC_ISSUER`, `DEADBOLT_OIDC_CLIENT_ID`, `DEADBOLT_OIDC_CLIENT_SECRET`, `DEADBOLT_OIDC_CLI_CLIENT_ID`                                     |
| Platform secrets  | Cloud secret manager, envelope encryption through KMS, workload access and rotation policy                                                   | `DEADBOLT_SECRET_MANAGER`, `DEADBOLT_KMS_KEY_ID`; provider-specific auth names remain pending                                                       |
| Off-host recovery | Encrypted daily base backups + continuous WAL archive, lag monitoring, 30-day retention, monthly restore drill and restore identity          | `DEADBOLT_BACKUP_DESTINATION`, `DEADBOLT_BACKUP_ACCESS`; actual provider auth names remain pending                                                  |
| Host capacity     | Measured resource limits, safe disk/headroom/retention and non-conflicting connectivity                                                      | `DEADBOLT_RESOURCE_BUDGET`                                                                                                                          |

Names for procedure/access decisions are documentation inputs, not implemented environment variables or existing GitHub settings. Runtime variables such as `DEADBOLT_DATABASE_URL` and `DEADBOLT_NATS_URL` will be stored privately for Deadbolt's own services; no connection values are committed. Do not derive a trusted host key from an unauthenticated scan. Missing external inputs block issue #5's deployment/readiness, while independent repository preparation may proceed as issue #1 explicitly permits.

## Local and hosted security boundaries

The local Compose implementation belongs to its owning issue. Its planned services bind to loopback, use `RUNTIME_MODE=local`, show the dev-auth banner, and send no cloud telemetry by default. The pinned local object-store image is development-only; it is not a hosted artifact provider selection. Hosted startup must reject dev auth/development keys. Customer task secrets remain local to allowlisted worker environments; platform secrets use the selected secret manager/KMS.

No deployment, database migration, secret provisioning, host inspection, Caddy change, capacity reservation, backup or restore was performed by this repository baseline.
