# Environment & Deployment Baseline (M0 Issue #1)

This document establishes the environment selections, deployment boundaries, and staging architecture baseline agreed in Blueprint §26 and Issue #1.

## 1. Staging Host Infrastructure

- **Platform:** AWS EC2 instance (`x86_64` architecture, Linux `amd64`).
- **Coexistence & Isolation:**
  - The staging host coexists with an existing project (`flowdesk-staging`).
  - **Deadbolt resources must be completely isolated:**
    - Dedicated Docker Compose project name: `deadbolt-staging`.
    - Dedicated Docker network: `deadbolt-net` (no bridge to FlowDesk networks).
    - Dedicated Docker volumes for PostgreSQL (`deadbolt_pgdata`) and NATS (`deadbolt_natsdata`).
    - Dedicated internal port allocations to avoid collision with existing services.
- **Reverse Proxy / Edge:**
  - The public edge is managed by **Caddy** (which owns host ports `80`, `443`, and UDP `443`).
  - Deadbolt public endpoints (API and Dashboard) will route via Caddy reverse proxy upstream configuration. No additional proxy binds directly to host ports 80/443.

## 2. CI/CD Deployment Pipeline Pattern

- **Build Once, Deploy Immutably:**
  - **GitHub Actions** builds the unified Linux `amd64` control-plane Docker image on merge to `main`.
  - Image published to **GitHub Packages Container Registry (GHCR)** tagged with immutable commit SHA and semver.
  - Automated SSH deployment triggers deployment on the EC2 host.
  - Rolling update with `/readyz` health-gate verification.

## 3. Configuration & Secret Contract

In compliance with Blueprint §24 and Issue #1 guidelines, credentials and secrets are **never** committed to version control. They are supplied via environment variables at runtime:

| Variable Name | Purpose | Managed Location |
|---|---|---|
| `DEADBOLT_STAGING_DOMAIN` | Public domain routing for Control Plane & Dashboard | Environment / Caddy config |
| `DEADBOLT_PORT` | Internal HTTP port for the Control Plane binary | `.env` on host (default: `8080`) |
| `DEADBOLT_DATABASE_URL` | PostgreSQL connection string | Secret store / host `.env` |
| `DEADBOLT_NATS_URL` | Internal NATS connection string | Secret store / host `.env` |
| `DEADBOLT_ARTIFACT_ENDPOINT` | External S3-compatible storage endpoint | Environment config |
| `DEADBOLT_ARTIFACT_BUCKET` | External S3 artifact bucket name | Environment config |
| `DEADBOLT_ARTIFACT_REGION` | External S3 storage region | Environment config |
| `DEADBOLT_OIDC_ISSUER` | OIDC identity provider URL | Environment config |
| `DEADBOLT_OIDC_CLIENT_ID` | OIDC Client ID | Environment config |
| `DEADBOLT_OIDC_CLIENT_SECRET` | OIDC Client Secret | Secret store |
| `DEADBOLT_KMS_KEY_ID` | Key identifier for envelope encryption | Cloud KMS / Secret store |

## 4. Local Development Baseline

- `RUNTIME_MODE=local`:
  - Dev-auth enabled only on loopback (`127.0.0.1` / `localhost`).
  - Embedded dev user with full privileges.
  - Local S3-compatible storage (MinIO) and local PostgreSQL/NATS via Compose.
  - Zero cloud telemetry or external calls by default.
