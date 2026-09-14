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
type Service struct{ pool *storage.Pool }

func NewService(pool *storage.Pool) *Service { return &Service{pool: pool} }

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
func (s *Service) Register(ctx context.Context, orgID, envID string, raw []byte) (*Deployment, bool, error) {
	v, err := contracts.ParseJSON(raw)
	if err != nil {
		return nil, false, err
	}
	if err := contracts.ValidateDeployment(v); err != nil {
		return nil, false, err
	}
	canonical, hash, err := contracts.Digest(raw)
	if err != nil {
		return nil, false, err
	}
	bundle := str(v, "bundleDigest")
	var out Deployment
	created := false
	err = s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
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
			VALUES ($1,$2,$3,$4,$5::jsonb,1,'node:24') ON CONFLICT (environment_id,manifest_hash) DO NOTHING
			RETURNING id::text,manifest_hash,bundle_digest,status,created_at`, orgID, envID, hash, bundle, canonical).Scan(&out.ID, &out.ManifestHash, &out.BundleDigest, &out.Status, &out.CreatedAt)
		if err == nil {
			created = true
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if !created {
			if err := tx.QueryRow(ctx, `SELECT id::text,manifest_hash,bundle_digest,status,created_at FROM deployments WHERE environment_id=$1 AND manifest_hash=$2`, envID, hash).Scan(&out.ID, &out.ManifestHash, &out.BundleDigest, &out.Status, &out.CreatedAt); err != nil {
				return err
			}
			return nil
		}
		for _, task := range arr(v, "tasks") {
			retry := obj(task, "retry")
			_, err := tx.Exec(ctx, `INSERT INTO task_definitions (organization_id,deployment_id,name,entrypoint,input_schema,output_schema,recovery_policy,timeout_ms,max_attempts,initial_delay_ms,max_delay_ms) VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9,$10,$11)`, orgID, out.ID, str(task, "name"), str(task, "entrypoint"), json.RawMessage(mustJSON(obj(task, "inputSchema"))), json.RawMessage(mustJSON(obj(task, "outputSchema"))), str(task, "recovery"), integer(task, "timeoutMs", 300000), integer(retry, "maxAttempts", 3), integer(retry, "initialDelayMs", 1000), integer(retry, "maxDelayMs", 30000))
			if err != nil {
				return err
			}
		}
		for _, wf := range arr(v, "workflows") {
			_, err := tx.Exec(ctx, `INSERT INTO workflow_definitions (organization_id,deployment_id,name,input_schema,output_schema,nodes) VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6::jsonb)`, orgID, out.ID, str(wf, "name"), json.RawMessage(mustJSON(obj(wf, "inputSchema"))), json.RawMessage(mustJSON(obj(wf, "outputSchema"))), json.RawMessage(mustJSON(arr(wf, "nodes"))))
			if err != nil {
				return err
			}
		}
		var workers int
		if err := tx.QueryRow(ctx, `SELECT count(DISTINCT ws.id) FROM worker_sessions ws JOIN workers w ON w.id=ws.worker_id AND w.organization_id=ws.organization_id JOIN worker_deployments wd ON wd.session_id=ws.id AND wd.organization_id=ws.organization_id WHERE ws.organization_id=$1 AND ws.environment_id=$2 AND ws.revoked_at IS NULL AND ws.expires_at>clock_timestamp() AND w.status='ACTIVE' AND wd.bundle_digest=$3`, orgID, envID, bundle).Scan(&workers); err != nil {
			return err
		}
		if workers > 0 {
			if _, err := tx.Exec(ctx, `UPDATE deployments SET status='AVAILABLE' WHERE id=$1 AND status='REGISTERED'`, out.ID); err != nil {
				return err
			}
			out.Status = "AVAILABLE"
		}
		_, err = tx.Exec(ctx, `INSERT INTO outbox_events (organization_id,subject,payload) VALUES ($1,'deployment.registered',jsonb_build_object('deploymentId',$2,'manifestHash',$3))`, orgID, out.ID, hash)
		return err
	})
	return &out, created, err
}
func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

func (s *Service) Activate(ctx context.Context, orgID, envID, name, deploymentID string, expected int64, allowSingle bool) (*Workflow, error) {
	var out Workflow
	err := s.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
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
		if err := tx.QueryRow(ctx, `SELECT count(DISTINCT ws.id) FROM worker_sessions ws JOIN workers w ON w.id=ws.worker_id AND w.organization_id=ws.organization_id JOIN worker_deployments wd ON wd.session_id=ws.id AND wd.organization_id=ws.organization_id WHERE ws.organization_id=$1 AND ws.environment_id=$2 AND ws.revoked_at IS NULL AND ws.expires_at>clock_timestamp() AND w.status='ACTIVE' AND wd.bundle_digest=$3`, orgID, envID, bundle).Scan(&workers); err != nil {
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
		if _, err = tx.Exec(ctx, `UPDATE deployments SET status='ACTIVE' WHERE id=$1`, deploymentID); err != nil {
			return err
		}
		if previous != nil && *previous != deploymentID {
			_, err = tx.Exec(ctx, `UPDATE deployments SET status=CASE WHEN EXISTS (SELECT 1 FROM workflow_channels WHERE active_deployment_id=$1) THEN 'ACTIVE' ELSE 'AVAILABLE' END WHERE id=$1`, *previous)
			if err != nil {
				return err
			}
		}
		out = Workflow{Name: name, ActiveDeploymentID: &deploymentID, Revision: current + 1}
		if workers == 1 {
			out.Warning = "single compatible worker; no failover"
		}
		_, err = tx.Exec(ctx, `INSERT INTO outbox_events (organization_id,subject,payload) VALUES ($1,'deployment.activated',jsonb_build_object('deploymentId',$2,'workflow',$3,'revision',$4))`, orgID, deploymentID, name, out.Revision)
		return err
	})
	return &out, err
}
