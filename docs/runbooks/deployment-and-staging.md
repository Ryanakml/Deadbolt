# Staging Deployment & Operations Runbook

## 1. Overview & Architecture

Deadbolt runs on an EC2 `x86_64` host co-located with `flowdesk-staging`. The staging deployment pipeline follows the immutable pattern:
**GitHub Actions → GHCR (immutable digest `@sha256:...`) → Automated SSH → EC2 Host**.

### Guiding Principles & Invariants

1. **Strict Co-Tenant Isolation**: FlowDesk containers, images, volumes, and networks must **never** be modified, stopped, deleted, or shared. All Deadbolt operations are strictly scoped to the Compose project `deadbolt-staging` and directory `/opt/deadbolt`.
2. **Caddy Edge Gateway & Dual Slot Upstream**: Caddy owns public ports TCP 80/443 and UDP 443. Deadbolt listens only on internal loopback ports (`127.0.0.1:8088` for Slot Blue, `127.0.0.1:8089` for Slot Green) and is reverse-proxied by Caddy.
3. **Pre-Traffic Readiness Gating**: Candidates start in the inactive slot, pass deep `/readyz` and `/version` checks while the active instance continues serving live staging traffic, and traffic is switched only after readiness is proven.
4. **Least-Privilege Credential Separation**: Migrations run via candidate transient container using DDL-capable `MIGRATOR_DATABASE_URL` with session-level advisory locking. The runtime control plane receives `DATABASE_URL` (restricted DML role with zero DDL privileges).
5. **No Destructive Database Rollback**: A rollback restores the previous known-good Deadbolt container image and Caddy configuration. It **never** executes down migrations or drops database volumes.
6. **Separate S3 Storage & WAL Readiness**: Artifact storage and WAL backups use the dedicated external S3 bucket provisioned in Issue #1, never FlowDesk MinIO.

---

## 2. Environments & Compose Topologies

### Local Development (`deploy/compose/docker-compose.yml`)

- All service ports bind exclusively to loopback (`127.0.0.1`).
- Supports profiles:
  - `core`: PostgreSQL 18, NATS 2.10, MinIO (local dev S3 emulation), Control Plane.
  - `telemetry`: OpenTelemetry Collector, Prometheus.
  - `fault`: Toxiproxy chaos injection proxy.
- Container-local execution uses `DEADBOLT_CONTAINER_LOCAL: "true"` to permit `0.0.0.0` container binding while restricting dev auth to loopback/private container callers.
- Data persistence via named volumes: `deadbolt_postgres_data`, `deadbolt_nats_data`, `deadbolt_minio_data`.
- Launch command:
  ```bash
  docker compose -f deploy/compose/docker-compose.yml --profile core up -d
  ```

### Staging (`deploy/compose/docker-compose.staging.yml`)

- Compose project: `deadbolt-staging`.
- Isolated bridge network: `deadbolt_staging_net`.
- PostgreSQL and NATS have **zero** host port publication (internal network only).
- Dual blue/green control-plane service slots:
  - `control-plane-blue`: binds to `127.0.0.1:8088:8080` (profile `slot-blue`).
  - `control-plane-green`: binds to `127.0.0.1:8089:8080` (profile `slot-green`).
- Resource constraints enforced via `deploy.resources.limits`:
  - Control Plane: 512 MiB RAM, 1.0 CPU
  - PostgreSQL: 1024 MiB RAM, 1.0 CPU
  - NATS: 256 MiB RAM, 0.5 CPU

---

## 3. Pre-Deployment Headroom & Backup Verification

Before initiating deployment, verify resource headroom, co-tenant isolation, and backup readiness:

```bash
# 1. Check available RAM (minimum 1024 MB required for candidate launch)
free -m

# 2. Check available root disk space (minimum 4096 MB required)
df -m /

# 3. Verify FlowDesk containers are healthy and undisturbed
docker ps --filter "name=flowdesk" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# 4. Verify external S3 backup readiness and archive lag
/opt/deadbolt/scripts/check-backup-readiness.sh
```

If memory, disk, or backup readiness falls below threshold, deployment is halted immediately without disrupting running workloads.

---

## 4. Deployment Workflow

Automated deployment is executed via `scripts/deploy-staging.sh`:

```bash
/opt/deadbolt/scripts/deploy-staging.sh <IMAGE_DIGEST> [COMMIT_SHA]
```

### Deployment Pipeline Stages

1. **Host Configuration Loading**:
   - Sources private configuration from `/etc/deadbolt/staging.env` (or `/opt/deadbolt/config/staging.env`).
   - Validates file permissions (rejects world-readable files, `chmod 600` enforced).
   - Validates required configuration keys and ensures `MIGRATOR_DATABASE_URL != DATABASE_URL`.
2. **Backup & Headroom Pre-flight Checks**:
   - Validates memory (≥1024 MiB) and disk (≥4096 MiB).
   - Executes `scripts/check-backup-readiness.sh`.
3. **Candidate Image Pull**:
   - Pulls exact immutable digest from GHCR (`ghcr.io/ryanakml/deadbolt/control-plane@sha256:...`).
4. **Deterministic Forward Migration Execution**:
   - Runs forward schema migrations using the candidate container runner with advisory locking:
     ```bash
     docker run --rm --network deadbolt_staging_net \
       -e MIGRATOR_DATABASE_URL="$MIGRATOR_DATABASE_URL" \
       "$CANDIDATE_DIGEST" --migrate
     ```
   - Schema changes must be backward-compatible (additive only).
5. **Candidate Launch into Inactive Slot**:
   - Identifies active slot from `/opt/deadbolt/releases/active_slot` (e.g. `blue` on port 8088).
   - Starts candidate in alternate slot (e.g. `green` on port 8089). Active instance continues serving traffic uninterrupted.
6. **Pre-Traffic Readiness Gating**:
   - Polls `http://127.0.0.1:<CANDIDATE_PORT>/readyz` up to 60 seconds.
   - Requires:
     - PostgreSQL connectivity & ping.
     - Verified and current Goose schema migration version (`goose_db_version`).
     - Active scheduler loop heartbeat (<60s).
     - Separate reporting of NATS connectivity/degradation (does not fail readiness per Blueprint §25.2).
     - Strict topology redaction (zero credentials, passwords, or hostnames in responses).
7. **Provenance Verification**:
   - Queries `http://127.0.0.1:<CANDIDATE_PORT>/version` and verifies both:
     - Exact Git commit SHA.
     - Exact immutable image digest (`@sha256:...`).
8. **Atomic Edge Traffic Switch**:
   - Updates Caddy upstream: `export DEADBOLT_UPSTREAM_PORT=<CANDIDATE_PORT>`.
   - Reloads Caddy with zero downtime: `./scripts/reload-caddy.sh`.
   - On reload failure, immediately restores previous Caddy configuration.
9. **Staged Edge Smoke Verification**:
   - Smokes `https://${DEADBOLT_STAGING_DOMAIN}/version` through Caddy.
10. **Decommission Old Slot & Update State**:
    - Stops previous slot container.
    - Updates `/opt/deadbolt/releases/active_slot` and release pointers (`current`, `previous`).

---

## 5. Rollback Procedure

If candidate health checks fail or post-deployment smoke tests detect an anomaly, automated rollback triggers:

```bash
/opt/deadbolt/scripts/rollback-staging.sh
```

### Rollback Contract

- **Restores Previous Binary**: Re-launches the image digest referenced by `/opt/deadbolt/releases/previous` in the alternate slot.
- **Gates Before Switching**: Validates `/readyz` on the restored instance before updating Caddy.
- **Restores Edge Route**: Points Caddy edge to the restored slot port.
- **NEVER Rolls Back Database**: Backward compatibility ensures the previous binary runs safely against the newly migrated schema. Down migrations are **strictly prohibited** during automated rollback.
- **Preserves Persistent Volumes**: `deadbolt_staging_postgres_data` is untouched.

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
