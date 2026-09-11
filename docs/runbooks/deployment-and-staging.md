# Staging Deployment & Operations Runbook

## 1. Overview & Architecture

Deadbolt runs on an EC2 `x86_64` host co-located with `flowdesk-staging`. The staging deployment pipeline follows the immutable pattern:
**GitHub Actions → GHCR (immutable digest `@sha256:...`) → Automated SSH → EC2 Host**.

### Guiding Principles & Invariants
1. **Strict Co-Tenant Isolation**: FlowDesk containers, images, volumes, and networks must **never** be modified, stopped, deleted, or shared. All Deadbolt operations are strictly scoped to the Compose project `deadbolt-staging` and directory `/opt/deadbolt`.
2. **Caddy Edge Gateway**: Caddy owns public ports TCP 80/443 and UDP 443. Deadbolt listens only on internal loopback `127.0.0.1:8088` and is reverse-proxied by Caddy.
3. **No Destructive Database Rollback**: A rollback restores the previous known-good Deadbolt container image and Caddy configuration. It **never** executes down migrations or drops database volumes.
4. **Separate S3 Storage**: Artifact storage and WAL backups use the dedicated external S3 bucket provisioned in Issue #1, never FlowDesk MinIO.

---

## 2. Environments & Compose Topologies

### Local Development (`deploy/compose/docker-compose.yml`)
- All service ports bind exclusively to loopback (`127.0.0.1`).
- Services: PostgreSQL 18, NATS 2.10, MinIO (local dev S3 emulation), Control Plane.
- Data persistence via named volumes: `deadbolt_postgres_data`, `deadbolt_nats_data`, `deadbolt_minio_data`.
- Launch command:
  ```bash
  docker compose -f deploy/compose/docker-compose.yml up -d
  ```

### Staging (`deploy/compose/docker-compose.staging.yml`)
- Compose project: `deadbolt-staging`.
- Isolated bridge network: `deadbolt_staging_net`.
- PostgreSQL and NATS have **zero** host port publication (internal network only).
- Control Plane binds strictly to loopback: `127.0.0.1:8088:8080`.
- Resource constraints enforced via `deploy.resources.limits`:
  - Control Plane: 512 MiB RAM, 1.0 CPU
  - PostgreSQL: 1024 MiB RAM, 1.0 CPU
  - NATS: 256 MiB RAM, 0.5 CPU

---

## 3. Pre-Deployment Headroom Verification

Before initiating deployment, verify resource headroom and co-tenant isolation:

```bash
# 1. Check available RAM (minimum 1024 MB required for candidate launch)
free -m

# 2. Check available root disk space (minimum 4096 MB required)
df -m /

# 3. Verify FlowDesk containers are healthy and undisturbed
docker ps --filter "name=flowdesk" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

If memory or disk falls below threshold, deployment is halted immediately without disrupting running workloads.

---

## 4. Deployment Workflow

Automated deployment is executed via `scripts/deploy-staging.sh`:

```bash
/opt/deadbolt/scripts/deploy-staging.sh <IMAGE_DIGEST> [MIGRATION_VERSION]
```

### Deployment Pipeline Stages
1. **Pre-flight Checks**:
   - Validates memory (≥1024 MiB) and disk (≥4096 MiB).
   - Validates FlowDesk co-tenant boundaries.
   - Validates required configuration (`DEADBOLT_DATABASE_URL`, `DEADBOLT_STORAGE_S3_BUCKET`).
2. **Schema Migration**:
   - Runs Goose forward migrations against PostgreSQL using an advisory lock.
   - Schema changes must be backward-compatible (additive only).
3. **Candidate Image Pull & Launch**:
   - Pulls exact immutable digest from GHCR (`ghcr.io/ryanakml/deadbolt/control-plane@sha256:...`).
   - Starts candidate container under Compose project `deadbolt-staging`.
4. **Deep Readiness Gating**:
   - Polls `http://127.0.0.1:8088/readyz` up to 60 seconds (12 attempts × 5s).
   - Checks:
     - PostgreSQL connectivity & ping.
     - Goose schema migration version matching expected release level.
     - Scheduler loop heartbeat freshness (<60s).
     - Graceful NATS degradation (if NATS is down, reports `"status":"ready"`, `"nats":"degraded"` with HTTP 200 OK per Blueprint §25.2).
     - Strict topology redaction (zero credentials, passwords, or hostnames in responses).
5. **Caddy Edge Reload**:
   - Validates Caddyfile syntax: `caddy validate --config /etc/caddy/Caddyfile`.
   - Reloads Caddy with zero downtime: `caddy reload --config /etc/caddy/Caddyfile`.
   - On reload failure, immediately restores previous Caddy configuration.
6. **Post-Deployment Smoke Verification**:
   - Queries `http://127.0.0.1:8088/version` to confirm:
     - Exact semantic version.
     - Exact Git commit SHA.
     - Build timestamp.
     - Immutable image digest `@sha256:...`.
     - `runtime_mode = "hosted"`.
7. **Release Metadata Recording**:
   - Updates `/opt/deadbolt/releases/current` symlink.
   - Archives previous release reference in `/opt/deadbolt/releases/previous`.

---

## 5. Rollback Procedure

If candidate health checks fail or post-deployment smoke tests detect an anomaly, automated rollback triggers:

```bash
# Automated rollback execution (also triggered by deploy-staging.sh on error)
/opt/deadbolt/scripts/rollback-staging.sh
```

### Rollback Contract
- **Restores Previous Binary**: Re-launches the image digest referenced by `/opt/deadbolt/releases/previous`.
- **Restores Edge Route**: Restores previous Caddyfile snippet if route changes were made.
- **NEVER Rolls Back Database**: Backward compatibility ensures the previous binary runs safely against the newly migrated schema. Down migrations are **strictly prohibited** during automated rollback.
- **Preserves Persistent Volumes**: `deadbolt_postgres_data` is untouched.

---

## 6. Retention Policy

Docker image cleanup is executed safely via `scripts/retention.sh`:

```bash
# Preview retention cleanup (dry run)
/opt/deadbolt/scripts/retention.sh --dry-run

# Apply retention policy
/opt/deadbolt/scripts/retention.sh
```

### Retention Rules
- **Protected Images**:
  - All images tagged or labeled `flowdesk*` (never inspected or deleted).
  - Images currently running on the host.
  - The currently active Deadbolt image (`/opt/deadbolt/releases/current`).
  - The previous Deadbolt image retained for the rollback window (`/opt/deadbolt/releases/previous`).
- **Eligible for Cleanup**:
  - Older Deadbolt images exceeding the retention count (`RETENTION_COUNT=3`).
- **Strict Prohibitions**:
  - `docker system prune` is **strictly forbidden**.
  - `docker image prune -a` is **strictly forbidden**.
  - Volume pruning (`docker volume prune`) is **strictly forbidden**.

---

## 7. Single-Control-Host Limitation & Production Promotion Policy

### Single-Control-Host Limitation
Staging operates on a single shared EC2 host. Consequently:
- **Blast Radius**: A host kernel panic, OOM killer event, or full disk condition impacts both Deadbolt and FlowDesk. Strict memory/disk budgets mitigate this risk.
- **High Availability**: Staging has no active-active redundancy. Maintenance windows require coordinated downtime.
- **Failover**: Recovery relies on cold instance reconstruction from EBS snapshots and S3 WAL backups.

### Production Promotion Policy
Promotion from Staging to Production requires:
1. **Cumulative §30 Gates Met**:
   - Contract and type checks passed (`pnpm check:contracts`, `pnpm check:parity`).
   - Lint and unit test suites green (`pnpm lint`, `pnpm test`, `go test -race ./...`).
   - Zero vulnerabilities (`govulncheck`) and zero secret leaks (`gitleaks`).
2. **Provenance Verification**:
   - Only images built and signed via GitHub Actions with immutable GHCR digests `@sha256:...` may be promoted.
   - Staged candidate must have run cleanly in Staging for a minimum 24-hour bake period.
3. **Approval Gate**:
   - Production promotion requires formal sign-off by the release owner.
   - Production infrastructure will not share hosts with other tenants (dedicated multi-AZ deployment).
