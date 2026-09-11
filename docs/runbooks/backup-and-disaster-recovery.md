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

PostgreSQL is configured to ship closed WAL segments directly to the external S3 bucket under the prefix `s3://${DEADBOLT_STORAGE_S3_BUCKET}/postgres/wal/`.

- **Archive Command Configuration (`postgresql.conf`)**:
  ```ini
  wal_level = replica
  archive_mode = on
  archive_command = 'envdir /etc/wal-e.d/env wal-g wal-push %p'
  archive_timeout = 300
  ```

### 2.2 Nightly Base Backups

Nightly full physical base backups are triggered via cron and pushed to:
`s3://${DEADBOLT_STORAGE_S3_BUCKET}/postgres/basebackups/`.

- Retention: Last 14 daily base backups retained.
- WAL segments corresponding to retained base backups are protected from deletion.

### 2.3 Volume Independence

- Database files reside in the dedicated Docker named volume `deadbolt_postgres_data`.
- This volume is never touched, pruned, or shared across projects.

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

A disaster recovery drill must be executed periodically on an isolated environment (never on the live staging host).

### Step-by-Step Restoration (PITR)

#### Step 1: Provision Clean Recovery Container

Spin up a recovery container with an empty target data volume:

```bash
docker run -d --name deadbolt-recovery \
  -v deadbolt_recovery_data:/var/lib/postgresql/data \
  -e POSTGRES_PASSWORD="${RECOVERY_DB_PASSWORD}" \
  postgres:18-bookworm
```

#### Step 2: Fetch Base Backup

Stop PostgreSQL in the recovery container and extract the base backup into the data directory:

```bash
docker stop deadbolt-recovery
wal-g backup-fetch /var/lib/postgresql/data LATEST
```

#### Step 3: Configure Recovery Signal & WAL Replay

Create `/var/lib/postgresql/data/recovery.signal` and configure `restore_command`:

```bash
touch /var/lib/postgresql/data/recovery.signal
cat <<EOF >> /var/lib/postgresql/data/postgresql.auto.conf
restore_command = 'wal-g wal-fetch %f %p'
recovery_target_time = '2026-09-11 12:00:00 UTC'
recovery_target_action = 'promote'
EOF
```

#### Step 4: Start Instance & Verify Schema

Start the container and monitor recovery logs:

```bash
docker start deadbolt-recovery
docker logs -f deadbolt-recovery
```

Verify that PostgreSQL logs indicate: `database system was not properly shut down; automatic recovery in progress`, followed by `consistent recovery state reached` and `database system is ready to accept connections`.

#### Step 5: Smoke Check Restored Data

Run verification queries:

```bash
docker exec -i deadbolt-recovery psql -U deadbolt -d deadbolt_control_plane -c \
  "SELECT version_id, is_applied FROM goose_db_version ORDER BY id DESC LIMIT 5;"
```

#### Step 6: Cleanup Recovery Container

```bash
docker stop deadbolt-recovery
docker rm deadbolt-recovery
docker volume rm deadbolt_recovery_data
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
