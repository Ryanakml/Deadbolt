# Backup & Disaster Recovery Runbook

## 1. Overview & Recovery Objectives

Deadbolt maintains an independent database persistence layer on PostgreSQL 18. In accordance with architectural invariants, backups are continuously archived to the dedicated external S3 bucket provisioned in Issue #1, with zero reliance on or cross-contamination with FlowDesk MinIO.

### Key Metrics

- **RPO (Recovery Point Objective)**: < 5 minutes (via continuous WAL archiving).
- **RTO (Recovery Time Objective)**: < 60 minutes for cold restoration on a replacement instance.
- **Rollback Invariant**: Application binary rollback **never** implies database rollback; schema migrations are additive and forward-compatible.

---

## 2. Backup Architecture

### 2.1 Continuous WAL Archiving

PostgreSQL 18 is configured to ship closed WAL segments directly to the external S3 bucket under the prefix `s3://${DEADBOLT_STORAGE_S3_BUCKET}/postgres/wal/`.

- **Archive Command Configuration (`deploy/postgres/postgresql.conf`)**:
  ```ini
  wal_level = replica
  archive_mode = on
  archive_command = '/usr/local/bin/archive-wal.sh %p %f'
  archive_timeout = 300
  ```
- **Archiver Implementation**: Uses `deploy/postgres/archive-wal.sh` running in the hardened PostgreSQL image (`deploy/Dockerfile.postgres`). Uploads segments via `aws s3 cp` with retry backoff and fallback local spooling (`/var/lib/postgresql/wal_archive_spool`).

### 2.2 Nightly Base Backups & Retention

Nightly physical base backups are created using `scripts/take-base-backup.sh`, scheduled via cron (`deploy/cron/deadbolt-backup.cron` & `scripts/setup-backup-cron.sh`):

- Destination: `s3://${DEADBOLT_STORAGE_S3_BUCKET}/postgres/basebackups/base_<TIMESTAMP>.tar.gz`
- Bounded Retention: Strict 14-backup retention policy (`RETENTION_COUNT=14`). Backups older than the 14 most recent backups are automatically purged from S3 and local storage.
- Immediate WAL checkpointing and sync via `pg_backup_stop(wait_for_archive => true)`.

### 2.3 Volume Independence

- Database files reside in the dedicated Docker named volume `deadbolt_staging_postgres_data`.
- Default staging database: `deadbolt_staging`.
- Administrative user: `deadbolt_admin`.
- Application user: `deadbolt_app`.
- Migration user: `deadbolt_migrator`.
- This volume is never touched, pruned, or shared across projects or with FlowDesk.

### 2.4 Fresh-Host Cluster Bootstrap & Baseline Backups

On an uninitialized staging host (where `/opt/deadbolt/releases/bootstrap_complete` does not exist), `scripts/deploy-staging.sh` automatically engages `--bootstrap` mode.
The cluster initialization workflow in `scripts/bootstrap-staging-cluster.sh` establishes the baseline:

1. Validates host configuration and credentials.
2. Starts PostgreSQL and NATS on `deadbolt_staging_net`.
3. Waits for PostgreSQL healthcheck (verifying schema/role initialization).
4. Creates the initial physical base backup and triggers an immediate WAL switch via `scripts/bootstrap-initial-backup.sh`.
5. Verifies backup and WAL readiness via `scripts/check-backup-readiness.sh`.
6. Configures nightly base backup cron with 14-day retention via `scripts/setup-backup-cron.sh`.
7. Records the durable bootstrap marker `/opt/deadbolt/releases/bootstrap_complete` only upon 100% successful completion.
   Subsequent promotions require this marker and verify backup readiness before running candidate migrations.

---

## 3. Pre-Flight Backup Readiness Validation

Before every candidate deployment, the deployment script executes a backup readiness check:

```bash
# 1. Verify S3 bucket access and credentials
aws s3 ls "s3://${DEADBOLT_STORAGE_S3_BUCKET}/postgres/wal/" --summarize

# 2. Check that the most recent WAL archive is less than 15 minutes old
LATEST_WAL=$(aws s3 ls "s3://${DEADBOLT_STORAGE_S3_BUCKET}/postgres/wal/" | sort | tail -n 1)
echo "Latest archived WAL: ${LATEST_WAL}"

# 3. Check base backup existence within the last 24 hours
LATEST_BASE=$(aws s3 ls "s3://${DEADBOLT_STORAGE_S3_BUCKET}/postgres/basebackups/" | sort | tail -n 1)
echo "Latest base backup: ${LATEST_BASE}"
```

If the S3 backup target is unreachable or the latest backup is stale, deployment halts to prevent running migrations without recoverable state.

---

## 4. Disaster Recovery & Restore Drill Procedure

Deadbolt provides an executable disaster recovery script: `scripts/restore-staging-db.sh`.
In adherence to architectural invariants, the tool provides two distinct, explicitly guarded operational modes:

1. **Safe Isolated Recovery Drill (Default)**:
   - Evaluates recoverability without stopping live services or altering persistent staging state.
   - Spins up an isolated recovery container (`deadbolt-recovery-drill-postgres`) attached to `--network none` and an ephemeral volume.
   - When S3 is configured, WAL archives are prefetched by the host to an ephemeral cache directory and mounted read-only (`/wal_archive:ro`), allowing PostgreSQL to replay via `cp /wal_archive/%f %p` under strict `--network none` isolation without AWS credentials inside the container.
   - Replays WAL archives up to the specified target time (or latest), verifies database promotion, runs smoke queries, and cleans up.
   - Live staging services (`deadbolt-staging-postgres`) and data volume (`deadbolt_staging_postgres_data`) remain 100% untouched.

2. **Destructive Disaster Recovery (Live Cluster)**:
   - For real disaster recovery when active staging persistence must be replaced.
   - Requires explicit `--destructive-staging-restore` flag and `FORCE_RESTORE=true` environment variable.

### Automated Execution

#### A. Running Safe Isolated Recovery Drill (Default)

```bash
# Run isolated drill against latest base backup + WAL archive
./scripts/restore-staging-db.sh

# Or drill recovery to a specific point in time:
DEADBOLT_RECOVERY_TARGET_TIME="2026-09-12 04:00:00 UTC" ./scripts/restore-staging-db.sh --drill
```

#### B. Executing Live Disaster Recovery (Destructive)

```bash
# Explicit destructive restoration into active staging volume
FORCE_RESTORE=true ./scripts/restore-staging-db.sh --destructive-staging-restore
```

### Manual Step-by-Step Restoration (PITR via AWS CLI)

#### Step 1: Provision Clean Recovery Workspace

Fetch the latest physical base backup from S3:

```bash
TMP_DIR=$(mktemp -d)
LATEST_BASE=$(aws s3 ls "s3://${DEADBOLT_STORAGE_S3_BUCKET}/postgres/basebackups/" | grep -E '\.tar\.gz$' | tail -n 1 | awk '{print $4}')
aws s3 cp "s3://${DEADBOLT_STORAGE_S3_BUCKET}/postgres/basebackups/${LATEST_BASE}" "${TMP_DIR}/base.tar.gz"
mkdir -p "${TMP_DIR}/recovered_data"
tar -xzf "${TMP_DIR}/base.tar.gz" -C "${TMP_DIR}/recovered_data"
chmod 700 "${TMP_DIR}/recovered_data"
```

#### Step 2: Configure Recovery Signal & WAL Replay for PostgreSQL 18

Create `recovery.signal` and configure `restore_command` in `postgresql.auto.conf`:

```bash
touch "${TMP_DIR}/recovered_data/recovery.signal"
cat <<EOF >> "${TMP_DIR}/recovered_data/postgresql.auto.conf"
restore_command = 'aws s3 cp s3://${DEADBOLT_STORAGE_S3_BUCKET}/postgres/wal/%f %p'
recovery_target_action = 'promote'
recovery_target_time = '2026-09-12 04:00:00 UTC'
EOF
```

#### Step 3: Swap Staging Data Volume (Destructive Recovery Only)

```bash
docker compose -f deploy/compose/docker-compose.staging.yml stop postgres
docker run --rm --entrypoint sh -v deadbolt_staging_postgres_data:/dest -v "${TMP_DIR}/recovered_data":/src "${DEADBOLT_POSTGRES_IMAGE}" -c "rm -rf /dest/* && cp -a /src/* /dest/ && chown -R 999:999 /dest"
docker compose -f deploy/compose/docker-compose.staging.yml up -d postgres
```

#### Step 4: Monitor Recovery Logs

Monitor PostgreSQL container logs during replay:

```bash
docker compose -f deploy/compose/docker-compose.staging.yml logs -f postgres
```

Verify that PostgreSQL logs indicate:

1. `starting archive recovery`
2. `restored log file ... from archive`
3. `recovery stopping at ..., reached recovery target time`
4. `archive recovery complete; database system is ready to accept connections`

#### Step 5: Smoke Check Restored Data

Run verification queries against `deadbolt_staging`:

```bash
docker exec -i deadbolt-staging-postgres psql -U deadbolt_admin -d deadbolt_staging -c \
  "SELECT version_id, is_applied FROM goose_db_version ORDER BY id DESC LIMIT 5;"
```

#### Step 6: Cleanup Temporary Files

```bash
rm -rf "$TMP_DIR"
```

---

## 5. Failure Modes & Mitigations

| Failure Mode                     | Impact                                              | Immediate Mitigation                                              | Recovery Procedure                                                                         |
| -------------------------------- | --------------------------------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| **Failed Candidate Healthcheck** | Candidate won't accept traffic                      | Zero impact to live traffic; Caddy edge remains untouched         | Automated rollback to previous image digest via `scripts/rollback-staging.sh`              |
| **Corrupted Host Filesystem**    | Host becomes unresponsive                           | Staging outage (shared with FlowDesk)                             | Rebuild EC2 instance from base AMI, re-attach or restore EBS snapshot, replay WALs from S3 |
| **Database Container Crash**     | Control plane reports 503 on `/readyz`              | Edge returns 502/503                                              | Docker restart policy restarts PostgreSQL; investigate OOM or disk exhaustion              |
| **NATS Outage**                  | Internal event dispatch delayed                     | Zero API downtime; `/readyz` returns 200 with `"nats":"degraded"` | Background outbox pattern queues tasks in PostgreSQL until NATS recovers                   |
| **S3 Outage**                    | WAL archiving deferred; artifact store inaccessible | Backups queue in `pg_wal`; task dispatch blocked                  | Monitor disk for WAL accumulation; alerts fire if queue depth > 100 segments               |

---

## 6. Single-Control-Host Constraints

- Staging currently operates on a single EC2 host without active cross-AZ failover.
- In the event of catastrophic AWS AZ failure, RTO is dependent on provisioning a new instance in an alternate AZ and executing the PITR procedure outlined in Section 4.
- Production architecture will utilize managed multi-AZ PostgreSQL (Aurora or RDS) and decoupled ECS/EKS task runner services.
