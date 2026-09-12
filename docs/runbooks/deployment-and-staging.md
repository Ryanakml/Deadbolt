# Staging Deployment & Operations Runbook

## 1. Overview & Architecture

Deadbolt runs on an EC2 `x86_64` host co-located with `flowdesk-staging`. The staging deployment pipeline follows the immutable pattern:
**GitHub Actions → GHCR (immutable digest `@sha256:...`) → Automated SSH → EC2 Host**.

### Guiding Principles & Invariants

1. **Strict Co-Tenant Isolation**: FlowDesk containers, images, volumes, and networks must **never** be modified, stopped, deleted, or shared. All Deadbolt operations are strictly scoped to the Compose project `deadbolt-staging` and directory `/opt/deadbolt`. Deadbolt never joins or reuses FlowDesk application networks (`flowdesk-staging_application`, `flowdesk-staging_edge`).
2. **Containerized Caddy Edge Gateway & Dedicated Edge Network**:
   - The existing FlowDesk Caddy container (`flowdesk-staging-caddy-1`, image `caddy:2.10.0-alpine`) remains the sole owner of host public ports TCP 80/443 and UDP 443. No second reverse proxy is ever created.
   - A dedicated external Docker network `deadbolt-edge` bridges FlowDesk Caddy and Deadbolt control plane slots.
   - Staging Compose attaches **only** `control-plane-blue` (alias: `deadbolt-control-plane-blue`) and `control-plane-green` (alias: `deadbolt-control-plane-green`) to `deadbolt-edge`.
   - PostgreSQL and NATS remain strictly private on internal network `deadbolt_staging_net` and are **never** attached to `deadbolt-edge` or published to host ports.
   - Caddy proxies to Docker DNS aliases (`deadbolt-control-plane-blue:8080` or `deadbolt-control-plane-green:8080`), never host loopback `127.0.0.1`.
   - The host directory `/opt/deadbolt/caddy` is mounted read-only into Caddy at `/etc/caddy/deadbolt:ro`, and the active Caddyfile imports `/etc/caddy/deadbolt/*.caddyfile`.
   - Host loopback ports (`127.0.0.1:8088` for Blue, `127.0.0.1:8089` for Green) remain active strictly for host-side pre-traffic readiness (`/readyz`) and image provenance (`/version`) gating.
3. **Pre-Traffic Readiness Gating**: Candidates start in the inactive slot, pass deep `/readyz` and `/version` checks on host loopback while the active instance continues serving live staging traffic, and edge traffic is switched only after readiness is proven.
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
- Internal bridge network: `deadbolt_staging_net`.
- Dedicated external edge network: `deadbolt-edge` (external: true).
- PostgreSQL and NATS have **zero** host port publication and join only `deadbolt_staging_net`.
- Dual blue/green control-plane service slots:
  - `control-plane-blue`: joins `deadbolt_staging_net` and `deadbolt-edge` (network alias: `deadbolt-control-plane-blue`); binds to `127.0.0.1:8088:8080` (profile `slot-blue`).
  - `control-plane-green`: joins `deadbolt_staging_net` and `deadbolt-edge` (network alias: `deadbolt-control-plane-green`); binds to `127.0.0.1:8089:8080` (profile `slot-green`).
- Resource constraints enforced via `deploy.resources.limits`:
  - Control Plane: 512 MiB RAM, 1.0 CPU
  - PostgreSQL: 1024 MiB RAM, 1.0 CPU
  - NATS: 256 MiB RAM, 0.5 CPU

---

## 3. Pre-Deployment Headroom & Backup Verification

Before initiating normal deployment, verify resource headroom, co-tenant isolation, and backup readiness:

```bash
# 1. Check available RAM (minimum 1024 MB required for candidate launch)
free -m

# 2. Check available root disk space (minimum 4096 MB required)
df -m /

# 3. Verify FlowDesk containers are healthy and undisturbed
docker ps --filter "name=flowdesk" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# 4. Verify external S3 backup readiness, exact WAL archive probe, and SSE encryption
/opt/deadbolt/scripts/check-backup-readiness.sh
```

If memory, disk, or backup readiness falls below threshold, deployment is halted immediately without disrupting running workloads.

---

## 4. Fresh Cluster Bootstrap vs. Candidate Promotion

### Clean Host Cluster Bootstrap (`scripts/bootstrap-staging-cluster.sh`)

On a fresh host where the Deadbolt PostgreSQL cluster has not yet been initialized:

1. Data services must be started first with dedicated database roles (`deadbolt_admin`, `deadbolt_migrator`, `deadbolt_runtime`, `deadbolt_system`).
2. An initial physical base backup and WAL segment switch must be performed via `scripts/bootstrap-initial-backup.sh`.
3. Fail-closed backup readiness (`scripts/check-backup-readiness.sh`) must verify base backup existence, an active exact WAL archive probe, and ServerSideEncryption policy.
4. Only after backup recoverability is proven can forward schema migrations and candidate promotion proceed.

Run standalone bootstrap:

```bash
/opt/deadbolt/scripts/bootstrap-staging-cluster.sh
```

Or pass `--bootstrap` directly to the automated deployment script:

```bash
/opt/deadbolt/scripts/deploy-staging.sh --bootstrap <IMAGE_DIGEST> [COMMIT_SHA]
```

### Staging Database Secret Model & Zero-Default Policy

### Deploy-user Secret-File Permission Contract

GitHub Actions connects as the non-root deployment account. Before the first deployment, provision the secret path with a dedicated group and add **only** that account to it:

```bash
sudo groupadd --force deadbolt-deploy
sudo usermod -aG deadbolt-deploy <automated-deploy-user>
sudo install -d -o root -g deadbolt-deploy -m 750 /etc/deadbolt
sudo install -o root -g deadbolt-deploy -m 640 /path/to/approved/staging.env /etc/deadbolt/staging.env
```

The directory must remain `root:deadbolt-deploy` mode `750`; `staging.env` must remain `root:deadbolt-deploy` mode `640`. The scripts reject world access and group-writable files, and enforce this exact ownership/mode contract for `/etc/deadbolt/staging.env`. Do not use `644`, and do not grant the deploy account a root shell merely to read configuration.

All database credentials in `deploy/compose/docker-compose.staging.yml` are strictly required with **zero repository-known fallback defaults**:

- `DEADBOLT_DB_ADMIN_PASSWORD`: Superuser password used solely during cluster init and base backups.
- `DEADBOLT_MIGRATOR_PASSWORD`: DDL-capable password for schema migrations.
- `DEADBOLT_RUNTIME_PASSWORD`: Restricted DML password for runtime control plane.
- `DEADBOLT_SYSTEM_PASSWORD`: Restricted function-caller password for scheduler tenancy sweeps.
- Connection URLs (`DATABASE_URL`, `MIGRATOR_DATABASE_URL`, `SYSTEM_DATABASE_URL`) are validated for strict password consistency against the above role passwords prior to starting PostgreSQL or running migrations.

---

## 5. Deployment Workflow

Automated deployment is executed via `scripts/deploy-staging.sh`:

```bash
/opt/deadbolt/scripts/deploy-staging.sh <IMAGE_DIGEST> [COMMIT_SHA]
```

### Deployment Pipeline Stages

1. **Configuration Loading & Security Checks**:
   - Sources configuration from `/etc/deadbolt/staging.env`.
   - Enforces the deploy-user permission contract above (`root:deadbolt-deploy` directory `750`; config `640`) and rejects world access or group writes.
   - Validates password consistency between connection URLs and secret variables.
   - Enforces `MIGRATOR_DATABASE_URL != DATABASE_URL` and `SYSTEM_DATABASE_URL != DATABASE_URL`.
     1b. **Fresh-Host First Deploy Detection**:
   - Checks for the durable bootstrap marker `/opt/deadbolt/releases/bootstrap_complete`.
   - If missing (uninitialized host), automatically activates `--bootstrap` mode and delegates to `scripts/bootstrap-staging-cluster.sh`.
   - Initializes PostgreSQL and NATS on `deadbolt_staging_net`, takes the first verified base backup, verifies backup readiness, installs the 14-day retention backup cron, and records the durable bootstrap marker only on complete success.
2. **Backup & Headroom Pre-flight Checks**:
   - Validates memory (≥1024 MiB) and disk (≥4096 MiB).
   - Verifies FlowDesk co-tenant containers are running and untouched.
   - In normal promotion mode, executes `scripts/check-backup-readiness.sh`.
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
7. **Provenance & Image Identity Verification**:
   - Queries `http://127.0.0.1:<CANDIDATE_PORT>/version` and verifies both:
     - Exact Git commit SHA.
     - Exact immutable image digest (`@sha256:...`).
   - Independently inspects running container image ID and RepoDigests via `docker inspect`.
8. **Atomic Edge Traffic Switch (Containerized Caddy)**:
   - Updates managed snippet `/opt/deadbolt/caddy/Deadbolt.caddyfile` via `./scripts/reload-caddy.sh <CANDIDATE_PORT>`.
   - Upstream points to Docker DNS alias (`deadbolt-control-plane-blue:8080` or `green:8080`), never loopback.
   - Validates merged Caddy configuration inside `flowdesk-staging-caddy-1` via `docker exec`.
   - Reloads Caddy with zero downtime inside the container.
   - Fails closed if Caddy container, mount (`/etc/caddy/deadbolt`), or import directive is missing.
9. **Staged Edge Smoke Verification & Rollback Protection**:
   - Smokes `https://${DEADBOLT_STAGING_DOMAIN}/version` through Caddy.
   - If edge smoke fails:
     - Restores previous Deadbolt snippet.
     - Reloads Caddy container to point back to previous slot.
     - Verifies route restoration before stopping the candidate slot container; if restoration fails, preserves both slots and exits failed for operator recovery.
     - Never touches FlowDesk containers or routes.
10. **Decommission Old Slot & Update State**:
    - Stops previous slot container.
    - Updates `/opt/deadbolt/releases/active_slot` and release pointers (`current`, `previous`).
    - Confirms `/opt/deadbolt/releases/bootstrap_complete` is recorded.

---

## 5. Rollback Procedure

If candidate health checks fail or post-deployment smoke tests detect an anomaly, automated rollback triggers:

```bash
/opt/deadbolt/scripts/rollback-staging.sh
```

### Rollback Contract

- **Restores Previous Binary**: Re-launches the image digest referenced by `/opt/deadbolt/releases/previous` in the alternate slot.
- **Image Provenance Recovery**: Automatically recovers `DEADBOLT_POSTGRES_IMAGE` from `/opt/deadbolt/releases/postgres_image` or the running container, enabling standalone rollback execution even when host environment or `staging.env` omits the digest.
- **Gates Before Switching**: Validates `/readyz` on the restored instance host port before updating Caddy.
- **Restores Edge Route**: Points Caddy edge to the restored slot Docker DNS alias via containerized reload.
- **NEVER Rolls Back Database**: Backward compatibility ensures the previous binary runs safely against the newly migrated schema. Down migrations are **strictly prohibited** during automated rollback.
- **Preserves Persistent Volumes**: `deadbolt_staging_postgres_data` is untouched.
- **FlowDesk Isolation**: FlowDesk containers, networks, volumes, and routes remain completely untouched.

---

## 6. Retention Policy

Docker image cleanup is executed safely via `scripts/retention.sh`:

```bash
# Preview retention cleanup (dry run)
cd /opt/deadbolt && DRY_RUN=true ./scripts/retention.sh

# Apply retention policy
/opt/deadbolt/scripts/retention.sh
```

### Retention Rules

- **Protected Images**:
  - Image IDs resolved from immutable current and previous Deadbolt release digests.
  - Image IDs used by every running `deadbolt-staging` container.
  - FlowDesk images, containers, volumes, and networks are never targeted.
- **Eligible for Cleanup**:
  - Unprotected local image IDs whose `RepoDigest` is an immutable `ghcr.io/ryanakml/deadbolt/control-plane@sha256:...` reference.
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
