// Package deployment owns immutable manifest registration and workflow-channel activation.
package deployment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/jackc/pgx/v5"
)

var (
	ErrNotFound  = errors.New("NOT_FOUND")
	ErrConflict  = errors.New("REVISION_CONFLICT")
	ErrImmutable = errors.New("IMMUTABLE_CONTENT_CONFLICT")
	ErrPreflight = errors.New("WORKER_PREFLIGHT_FAILED")
)

type Deployment struct {
	ID           string    `json:"id"`
	ManifestHash string    `json:"manifestHash"`
	BundleDigest string    `json:"bundleDigest"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"createdAt"`
}
type Workflow struct {
	Name               string  `json:"name"`
	ActiveDeploymentID *string `json:"activeDeploymentId"`
	Revision           int64   `json:"revision"`
	Warning            string  `json:"warning,omitempty"`
}
type Service struct {
	pool     *storage.Pool
	commands *tenant.Service
}

// ReconcileAvailability refreshes the cached lifecycle state from the current
// authenticated worker inventory. Worker enrollment/heartbeat paths call this
// after changing a session or advertised bundle; an ACTIVE channel remains
// pinned even when its compatible worker set drops to zero.
func (s *Service) ReconcileAvailability(ctx context.Context, orgID, envID string) error {
	return s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		return reconcileAvailabilityTx(ctx, tx, orgID, envID)
	})
}
func reconcileAvailabilityTx(ctx context.Context, tx storage.Tx, orgID, envID string) error {
	_, err := tx.Exec(ctx, `UPDATE deployments d SET status = CASE
WHEN EXISTS (SELECT 1 FROM workflow_channels c WHERE c.environment_id=d.environment_id AND c.active_deployment_id=d.id) THEN 'ACTIVE'
WHEN EXISTS (SELECT 1 FROM worker_sessions ws JOIN workers w ON w.id=ws.worker_id AND w.organization_id=ws.organization_id JOIN worker_deployments wd ON wd.session_id=ws.id AND wd.organization_id=ws.organization_id WHERE ws.organization_id=$1 AND ws.environment_id=d.environment_id AND ws.revoked_at IS NULL AND ws.expires_at>clock_timestamp() AND w.status='ACTIVE' AND wd.bundle_digest=d.bundle_digest) THEN 'AVAILABLE'
ELSE 'REGISTERED' END WHERE d.organization_id=$1 AND d.environment_id=$2`, orgID, envID)
	if err != nil {
		return err
	}
	// Reset the one-shot warning after compatibility returns, then emit one
	// durable observable event when an active pointer has lost its last worker.
	if _, err = tx.Exec(ctx, `UPDATE deployments d SET compatibility_warning_at=NULL WHERE d.organization_id=$1 AND d.environment_id=$2 AND d.status='ACTIVE' AND d.compatibility_warning_at IS NOT NULL AND EXISTS (SELECT 1 FROM worker_sessions ws JOIN workers w ON w.id=ws.worker_id AND w.organization_id=ws.organization_id JOIN worker_deployments wd ON wd.session_id=ws.id AND wd.organization_id=ws.organization_id WHERE ws.organization_id=$1 AND ws.environment_id=d.environment_id AND ws.revoked_at IS NULL AND ws.expires_at>clock_timestamp() AND w.status='ACTIVE' AND wd.bundle_digest=d.bundle_digest)`, orgID, envID); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `UPDATE deployments d SET compatibility_warning_at=clock_timestamp() WHERE d.organization_id=$1 AND d.environment_id=$2 AND d.status='ACTIVE' AND d.compatibility_warning_at IS NULL AND NOT EXISTS (SELECT 1 FROM worker_sessions ws JOIN workers w ON w.id=ws.worker_id AND w.organization_id=ws.organization_id JOIN worker_deployments wd ON wd.session_id=ws.id AND wd.organization_id=ws.organization_id WHERE ws.organization_id=$1 AND ws.environment_id=d.environment_id AND ws.revoked_at IS NULL AND ws.expires_at>clock_timestamp() AND w.status='ACTIVE' AND wd.bundle_digest=d.bundle_digest) RETURNING id::text`, orgID, envID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var lostIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		lostIDs = append(lostIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range lostIDs {
		if _, err := tx.Exec(ctx, `INSERT INTO outbox_events (organization_id,subject,payload) VALUES ($1,'deployment.compatibility_lost',jsonb_build_object('deploymentId',$2::text,'environmentId',$3::text))`, orgID, id, envID); err != nil {
			return err
		}
	}
	return nil
}

func NewService(pool *storage.Pool, commands *tenant.Service) *Service {
	return &Service{pool: pool, commands: commands}
}

func str(v any, key string) string {
	if m, ok := v.(map[string]any); ok {
		if s, ok := m[key].(string); ok {
			return s
		}
	}
	return ""
}
func arr(v any, key string) []any {
	if m, ok := v.(map[string]any); ok {
		if a, ok := m[key].([]any); ok {
			return a
		}
	}
	return nil
}
func obj(v any, key string) map[string]any {
	if m, ok := v.(map[string]any); ok {
		if x, ok := m[key].(map[string]any); ok {
			return x
		}
	}
	return map[string]any{}
}
func integer(v any, key string, d int) int {
	if m, ok := v.(map[string]any); ok {
		if n, ok := m[key].(float64); ok {
			return int(n)
		}
	}
	return d
}

// Register validates before writing, canonically hashes the exact manifest, and
// atomically persists its definitions. Replays return the original row.
func (s *Service) Register(ctx context.Context, orgID, envID string, raw []byte, audit *tenant.AuditContext) (*Deployment, int, error) {
	v, err := contracts.ParseJSON(raw)
	if err != nil {
		return nil, 0, err
	}
	if err := contracts.ValidateDeployment(v); err != nil {
		return nil, 0, err
	}
	canonical, hash, err := contracts.Digest(raw)
	if err != nil {
		return nil, 0, err
	}
	bundle := str(v, "bundleDigest")
	var out Deployment
	created := false
	if audit == nil {
		return nil, 0, tenant.ErrAuditRequired
	}
	// The environment must belong to the organization: a UUID alone never
	// authorizes cross-organization binding, including for stale client IDs.
	if _, err := s.commands.GetEnvironment(ctx, orgID, envID); err != nil {
		if errors.Is(err, tenant.ErrNotFound) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}
	responseCode := 201
	_, recordedCode, err := s.commands.WithCommandTxDynamic(ctx, orgID, "", func(ctx context.Context, tx storage.Tx) error {
		// A bundle digest identifies executable immutable bytes. It may not be
		// rebound to a changed manifest in the same environment.
		var existingHash string
		err := tx.QueryRow(ctx, `SELECT manifest_hash FROM deployments WHERE environment_id=$1 AND bundle_digest=$2`, envID, bundle).Scan(&existingHash)
		if err == nil && existingHash != hash {
			return ErrImmutable
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		err = tx.QueryRow(ctx, `INSERT INTO deployments (organization_id,environment_id,manifest_hash,bundle_digest,manifest,protocol_version,runtime_version)
			VALUES ($1,$2,$3,$4,$5::jsonb,1,'node:24') ON CONFLICT DO NOTHING
			RETURNING id::text,manifest_hash,bundle_digest,status,created_at`, orgID, envID, hash, bundle, canonical).Scan(&out.ID, &out.ManifestHash, &out.BundleDigest, &out.Status, &out.CreatedAt)
		if err == nil {
			created = true
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if !created {
			responseCode = 200
			var existing Deployment
			if err := tx.QueryRow(ctx, `SELECT id::text,manifest_hash,bundle_digest,status,created_at FROM deployments WHERE environment_id=$1 AND bundle_digest=$2`, envID, bundle).Scan(&existing.ID, &existing.ManifestHash, &existing.BundleDigest, &existing.Status, &existing.CreatedAt); err != nil {
				return err
			}
			if existing.ManifestHash != hash {
				return ErrImmutable
			}
			out = existing
			return nil
		}
		for _, task := range arr(v, "tasks") {
			retry := obj(task, "retry")
			var window any
			if n, ok := task.(map[string]any)["idempotencyWindowMs"].(float64); ok {
				window = int64(n)
			}
			_, err := tx.Exec(ctx, `INSERT INTO task_definitions (organization_id,deployment_id,name,entrypoint,input_schema,output_schema,recovery_policy,timeout_ms,max_attempts,initial_delay_ms,max_delay_ms,idempotency_window_ms) VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9,$10,$11,$12)`, orgID, out.ID, str(task, "name"), str(task, "entrypoint"), json.RawMessage(mustJSON(obj(task, "inputSchema"))), json.RawMessage(mustJSON(obj(task, "outputSchema"))), str(task, "recovery"), integer(task, "timeoutMs", 300000), integer(retry, "maxAttempts", 3), integer(retry, "initialDelayMs", 1000), integer(retry, "maxDelayMs", 30000), window)
			if err != nil {
				return err
			}
		}
		for _, wf := range arr(v, "workflows") {
			_, err := tx.Exec(ctx, `INSERT INTO workflow_definitions (organization_id,deployment_id,name,input_schema,output_schema,nodes,output_mapping) VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb)`, orgID, out.ID, str(wf, "name"), json.RawMessage(mustJSON(obj(wf, "inputSchema"))), json.RawMessage(mustJSON(obj(wf, "outputSchema"))), json.RawMessage(mustJSON(arr(wf, "nodes"))), json.RawMessage(mustJSON(obj(wf, "output"))))
			if err != nil {
				return err
			}
		}
		var workers int
		if err := tx.QueryRow(ctx, `SELECT count(DISTINCT w.id) FROM worker_sessions ws JOIN workers w ON w.id=ws.worker_id AND w.organization_id=ws.organization_id JOIN worker_deployments wd ON wd.session_id=ws.id AND wd.organization_id=ws.organization_id WHERE ws.organization_id=$1 AND ws.environment_id=$2 AND ws.revoked_at IS NULL AND ws.expires_at>clock_timestamp() AND w.status='ACTIVE' AND wd.bundle_digest=$3`, orgID, envID, bundle).Scan(&workers); err != nil {
			return err
		}
		if workers > 0 {
			if _, err := tx.Exec(ctx, `UPDATE deployments SET status='AVAILABLE' WHERE id=$1 AND status='REGISTERED'`, out.ID); err != nil {
				return err
			}
			out.Status = "AVAILABLE"
		}
		if err := reconcileAvailabilityTx(ctx, tx, orgID, envID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT status FROM deployments WHERE id=$1`, out.ID).Scan(&out.Status); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO outbox_events (organization_id,subject,payload) VALUES ($1,'deployment.registered',jsonb_build_object('deploymentId',$2::text,'manifestHash',$3::text))`, orgID, out.ID, hash); err != nil {
			return err
		}
		meta, _ := json.Marshal(map[string]any{"role": audit.Role, "capabilities": audit.Capabilities, "environment_id": envID})
		_, err = tx.Exec(ctx, `INSERT INTO audit_events (organization_id,actor_id,action,target_type,target_id,correlation_id,reason,metadata) VALUES ($1,$2,'deployment.register','deployment',$3,$4,$5,$6::jsonb)`, orgID, audit.ActorID, out.ID, audit.CorrelationID, audit.Reason, meta)
		return err
	}, func() int { return responseCode }, func() any { return &out }, func(raw json.RawMessage) error { return json.Unmarshal(raw, &out) })
	return &out, recordedCode, err
}
func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

func (s *Service) Activate(ctx context.Context, orgID, envID, name, deploymentID string, expected int64, allowSingle bool, audit *tenant.AuditContext) (*Workflow, error) {
	var out Workflow
	if audit == nil {
		return nil, tenant.ErrAuditRequired
	}
	_, err := s.commands.WithCommandTx(ctx, orgID, "", 200, func(ctx context.Context, tx storage.Tx) error {
		var envName string
		if err := tx.QueryRow(ctx, `SELECT name FROM environments WHERE id=$1 FOR UPDATE`, envID).Scan(&envName); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		var bundle string
		if err := tx.QueryRow(ctx, `SELECT d.bundle_digest FROM deployments d JOIN workflow_definitions w ON w.deployment_id=d.id AND w.organization_id=d.organization_id WHERE d.id=$1 AND d.environment_id=$2 AND d.organization_id=$3 AND w.name=$4`, deploymentID, envID, orgID, name).Scan(&bundle); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		var workers int
		if err := tx.QueryRow(ctx, `SELECT count(DISTINCT w.id) FROM worker_sessions ws JOIN workers w ON w.id=ws.worker_id AND w.organization_id=ws.organization_id JOIN worker_deployments wd ON wd.session_id=ws.id AND wd.organization_id=ws.organization_id WHERE ws.organization_id=$1 AND ws.environment_id=$2 AND ws.revoked_at IS NULL AND ws.expires_at>clock_timestamp() AND w.status='ACTIVE' AND wd.bundle_digest=$3`, orgID, envID, bundle).Scan(&workers); err != nil {
			return err
		}
		need := 2
		if allowSingle && strings.EqualFold(envName, "production") {
			return ErrPreflight
		}
		if allowSingle {
			need = 1
		}
		if workers < need {
			return fmt.Errorf("%w: %d compatible workers, need %d", ErrPreflight, workers, need)
		}
		var current int64
		var previous *string
		err := tx.QueryRow(ctx, `SELECT active_deployment_id::text,revision FROM workflow_channels WHERE environment_id=$1 AND workflow_name=$2 FOR UPDATE`, envID, name).Scan(&previous, &current)
		if errors.Is(err, pgx.ErrNoRows) {
			if expected != 0 {
				return ErrConflict
			}
			current = 0
			_, err = tx.Exec(ctx, `INSERT INTO workflow_channels (environment_id,organization_id,workflow_name,active_deployment_id,revision) VALUES ($1,$2,$3,$4,1)`, envID, orgID, name, deploymentID)
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			if current != expected {
				return ErrConflict
			}
			_, err = tx.Exec(ctx, `UPDATE workflow_channels SET active_deployment_id=$1,revision=revision+1,updated_at=clock_timestamp() WHERE environment_id=$2 AND workflow_name=$3`, deploymentID, envID, name)
			if err != nil {
				return err
			}
		}
		if err := reconcileAvailabilityTx(ctx, tx, orgID, envID); err != nil {
			return err
		}
		out = Workflow{Name: name, ActiveDeploymentID: &deploymentID, Revision: current + 1}
		if workers == 1 {
			out.Warning = "single compatible worker; no failover"
		}
		if _, err = tx.Exec(ctx, `INSERT INTO outbox_events (organization_id,subject,payload) VALUES ($1,'deployment.activated',jsonb_build_object('deploymentId',$2::text,'workflow',$3::text,'revision',$4::bigint))`, orgID, deploymentID, name, out.Revision); err != nil {
			return err
		}
		meta, _ := json.Marshal(map[string]any{"role": audit.Role, "capabilities": audit.Capabilities, "environment_id": envID, "revision": out.Revision})
		_, err = tx.Exec(ctx, `INSERT INTO audit_events (organization_id,actor_id,action,target_type,target_id,correlation_id,reason,metadata) VALUES ($1,$2,'deployment.activate','deployment',$3,$4,$5,$6::jsonb)`, orgID, audit.ActorID, deploymentID, audit.CorrelationID, audit.Reason, meta)
		return err
	}, func() any { return &out }, func(raw json.RawMessage) error { return json.Unmarshal(raw, &out) })
	return &out, err
}
