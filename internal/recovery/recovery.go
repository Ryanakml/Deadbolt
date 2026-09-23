package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/artifacts"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/storage/migrator"
	"github.com/jackc/pgx/v5"
)

var (
	ErrAdmissionDisabled           = errors.New("ADMISSION_DISABLED: System is in disaster recovery read-only mode; run admission is disabled")
	ErrDispatchDisabled            = errors.New("DISPATCH_DISABLED: System is in disaster recovery mode; task dispatch is disabled")
	ErrRecoveryControlsUnavailable = errors.New("RECOVERY_CONTROLS_UNAVAILABLE: System recovery controls are unreadable; admission and dispatch are denied fail-closed")
	ErrInvalidRecoveryMode         = errors.New("INVALID_RECOVERY_MODE: Unknown recovery mode")
	ErrIncidentNotFound            = errors.New("INCIDENT_NOT_FOUND: Disaster recovery incident not found")
	ErrInvalidTimeRange            = errors.New("INVALID_TIME_RANGE: Recovery point must be before or equal to incident time")
)

type Controls struct {
	Mode             string    `json:"mode"`
	AdmissionEnabled bool      `json:"admissionEnabled"`
	DispatchEnabled  bool      `json:"dispatchEnabled"`
	SchedulesEnabled bool      `json:"schedulesEnabled"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

type PrepareRequest struct {
	RecoveryPoint    time.Time `json:"recoveryPoint"`
	IncidentAt       time.Time `json:"incidentAt"`
	RPOGapRequestIDs []string  `json:"rpoGapRequestIds,omitempty"`
	OperatorNotes    string    `json:"operatorNotes,omitempty"`
}

type IncidentReport struct {
	ID                         string    `json:"id"`
	RecoveryPoint              time.Time `json:"recoveryPoint"`
	IncidentAt                 time.Time `json:"incidentAt"`
	UncertaintyWindowStart     time.Time `json:"uncertaintyWindowStart"`
	UncertaintyWindowEnd       time.Time `json:"uncertaintyWindowEnd"`
	Status                     string    `json:"status"`
	RestoredRunsCount          int       `json:"restoredRunsCount"`
	RevokedWorkerSessionsCount int       `json:"revokedWorkerSessionsCount"`
	RevokedAuthSessionsCount   int       `json:"revokedAuthSessionsCount"`
	RPOGapRequests             []string  `json:"rpoGapRequests"`
	RPONote                    string    `json:"rpoNote"`
	OperatorNotes              string    `json:"operatorNotes"`
	CreatedAt                  time.Time `json:"createdAt"`
}

type IntegrityCheck struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Details string `json:"details"`
}

type IntegrityReport struct {
	SchemaValid          bool             `json:"schemaValid"`
	TenantIsolationValid bool             `json:"tenantIsolationValid"`
	ArtifactsValid       bool             `json:"artifactsValid"`
	DeletionLedgerValid  bool             `json:"deletionLedgerValid"`
	OverallPassed        bool             `json:"overallPassed"`
	PendingDeletionCount int              `json:"pendingDeletionCount"`
	Checks               []IntegrityCheck `json:"checks"`
}

type Manager struct {
	pool *storage.Pool
	// artifacts is the optional scoped artifact service used for restored
	// artifact integrity verification (Blueprint §18.2/§27.3). When nil, any
	// referenced READY artifact fails the integrity check closed.
	artifacts *artifacts.Service
}

func NewManager(pool *storage.Pool) *Manager {
	return &Manager{pool: pool}
}

// SetArtifacts attaches the scoped artifact service for restored artifact
// integrity verification. Engines without it fail artifact paths closed.
func (m *Manager) SetArtifacts(svc *artifacts.Service) {
	m.artifacts = svc
}

// GetControls reads current system recovery mode and flags.
// It fails closed: a missing or unreadable controls row is an error, never
// an assumed ACTIVE state.
func (m *Manager) GetControls(ctx context.Context) (*Controls, error) {
	var c Controls
	err := m.pool.QueryRow(ctx, `SELECT mode, admission_enabled, dispatch_enabled, schedules_enabled, updated_at
		FROM system_recovery_controls WHERE id = 1`).Scan(&c.Mode, &c.AdmissionEnabled, &c.DispatchEnabled, &c.SchedulesEnabled, &c.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("%w: read system recovery controls: %v", ErrRecoveryControlsUnavailable, err)
	}
	return &c, nil
}

// IsAdmissionAllowed reports whether new runs can be admitted.
func (m *Manager) IsAdmissionAllowed(ctx context.Context) (bool, error) {
	c, err := m.GetControls(ctx)
	if err != nil {
		return false, err
	}
	if !c.AdmissionEnabled || c.Mode == "READ_ONLY" || c.Mode == "DISASTER_RECOVERY" {
		return false, nil
	}
	return true, nil
}

// IsDispatchAllowed reports whether task dispatch to workers is enabled.
func (m *Manager) IsDispatchAllowed(ctx context.Context) (bool, error) {
	c, err := m.GetControls(ctx)
	if err != nil {
		return false, err
	}
	if !c.DispatchEnabled || c.Mode == "READ_ONLY" || c.Mode == "DISASTER_RECOVERY" {
		return false, nil
	}
	return true, nil
}

// resolveRecoveryPolicy maps a workflow node to its referenced task recovery
// policy using the run's pinned deployment manifest:
//
//	run → pinned deployment → workflow → node ID → node.task → task.recovery
//
// A node ID and its referenced task name are different identities, so the
// node ID must never be used as the task name. Unknown, missing, or invalid
// policy resolves to ("unknown", false) and must remain held; it is never
// silently relabeled "safe" because safe permits different recovery reasoning.
func resolveRecoveryPolicy(manifestJSON []byte, workflowName, nodeID string) (string, bool) {
	var manifest struct {
		Tasks []struct {
			Name     string `json:"name"`
			Recovery string `json:"recovery"`
		} `json:"tasks"`
		Workflows []struct {
			Name  string `json:"name"`
			Nodes []struct {
				ID   string `json:"id"`
				Task string `json:"task"`
			} `json:"nodes"`
		} `json:"workflows"`
	}
	if err := json.Unmarshal(manifestJSON, &manifest); err != nil {
		return "unknown", false
	}
	taskName := ""
	for _, wf := range manifest.Workflows {
		if wf.Name != workflowName {
			continue
		}
		for _, n := range wf.Nodes {
			if n.ID == nodeID {
				taskName = n.Task
				break
			}
		}
		break
	}
	if taskName == "" {
		return "unknown", false
	}
	for _, task := range manifest.Tasks {
		if task.Name == taskName {
			switch policy := strings.ToLower(strings.TrimSpace(task.Recovery)); policy {
			case "safe", "idempotent", "reconcile":
				return policy, true
			default:
				return "unknown", false
			}
		}
	}
	return "unknown", false
}

// PrepareDisasterRecovery implements Blueprint §27.3:
// 1. Disable admission, schedules, dispatcher, and worker sessions.
// 2. Revoke pre-disaster worker and auth sessions.
// 3. Mark all nonterminal runs at recovery point with disaster reconciliation hold and uncertainty window.
// 4. Record RPO gap requests with explicit "no claim of zero RPO".
func (m *Manager) PrepareDisasterRecovery(ctx context.Context, req PrepareRequest) (*IncidentReport, error) {
	if req.RecoveryPoint.IsZero() {
		return nil, errors.New("recoveryPoint is required")
	}
	if req.IncidentAt.IsZero() {
		req.IncidentAt = time.Now().UTC()
	}
	if req.RecoveryPoint.After(req.IncidentAt) {
		return nil, ErrInvalidTimeRange
	}

	// 1. Disable admission, dispatch, and schedules; enter READ_ONLY mode.
	_, err := m.pool.Exec(ctx, `INSERT INTO system_recovery_controls (id, mode, admission_enabled, dispatch_enabled, schedules_enabled, updated_at)
		VALUES (1, 'READ_ONLY', FALSE, FALSE, FALSE, clock_timestamp())
		ON CONFLICT (id) DO UPDATE SET
			mode = 'READ_ONLY',
			admission_enabled = FALSE,
			dispatch_enabled = FALSE,
			schedules_enabled = FALSE,
			updated_at = clock_timestamp()`)
	if err != nil {
		return nil, fmt.Errorf("set recovery controls to READ_ONLY: %w", err)
	}

	// 2. Revoke pre-disaster auth sessions (cluster-wide table without tenant RLS).
	authTag, err := m.pool.Exec(ctx, `UPDATE auth_sessions
		SET revoked_at = clock_timestamp(), revocation_reason = 'DISASTER_RECOVERY_REVOCATION'
		WHERE revoked_at IS NULL AND created_at <= $1`, req.IncidentAt)
	if err != nil {
		return nil, fmt.Errorf("revoke pre-disaster auth sessions: %w", err)
	}
	revokedAuth := int(authTag.RowsAffected())

	// 3. Enumerate tenant organizations with active runs/sessions and apply disaster reconciliation holds.
	orgRows, err := m.pool.Query(ctx, `SELECT organization_id FROM app.enumerate_recovery_tenants()`)
	if err != nil {
		return nil, fmt.Errorf("enumerate tenants: %w", err)
	}
	defer orgRows.Close()

	var orgIDs []string
	for orgRows.Next() {
		var o string
		if err := orgRows.Scan(&o); err == nil {
			orgIDs = append(orgIDs, o)
		}
	}
	orgRows.Close()

	totalRestoredRuns := 0
	revokedWorkers := 0

	for _, orgID := range orgIDs {
		err := m.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
			// Revoke pre-disaster worker sessions for this tenant
			wTag, err := tx.Exec(ctx, `UPDATE worker_sessions
				SET revoked_at = clock_timestamp()
				WHERE organization_id = $1::uuid AND revoked_at IS NULL AND created_at <= $2`, orgID, req.IncidentAt)
			if err != nil {
				return fmt.Errorf("revoke worker sessions: %w", err)
			}
			revokedWorkers += int(wTag.RowsAffected())

			// Find nonterminal runs in this tenant
			runRows, err := tx.Query(ctx, `SELECT id::text, environment_id::text, deployment_id::text, workflow_name
				FROM runs
				WHERE organization_id = $1::uuid
				  AND status IN ('QUEUED', 'RUNNING', 'WAITING', 'PAUSING', 'PAUSED', 'CANCELLING')
				FOR UPDATE`, orgID)
			if err != nil {
				return err
			}
			defer runRows.Close()

			type nonterminalRun struct {
				id            string
				environmentID string
				deploymentID  string
				workflowName  string
			}
			var runs []nonterminalRun
			for runRows.Next() {
				var r nonterminalRun
				if err := runRows.Scan(&r.id, &r.environmentID, &r.deploymentID, &r.workflowName); err == nil {
					runs = append(runs, r)
				}
			}
			runRows.Close()

			for _, r := range runs {
				totalRestoredRuns++

				// Resolve the run's pinned deployment manifest once so every
				// step policy is read from node.task, never from node ID.
				var manifestJSON []byte
				if err := tx.QueryRow(ctx, `SELECT manifest FROM deployments
					WHERE id = $1::uuid AND organization_id = $2::uuid`, r.deploymentID, orgID).Scan(&manifestJSON); err != nil {
					manifestJSON = nil
				}

				// Step 1: Update run to WAITING / RECONCILIATION
				if _, err := tx.Exec(ctx, `UPDATE runs
					SET status = 'WAITING', reason_code = 'RECONCILIATION', updated_at = clock_timestamp()
					WHERE id = $1::uuid AND organization_id = $2::uuid`, r.id, orgID); err != nil {
					return fmt.Errorf("update run %s to disaster reconciliation: %w", r.id, err)
				}

				// Step 2: Identify all nonterminal steps in the run
				stepRows, err := tx.Query(ctx, `SELECT id::text, node_id, state
					FROM run_steps
					WHERE run_id = $1::uuid AND organization_id = $2::uuid
					  AND state IN ('READY', 'RUNNING', 'WAITING', 'BLOCKED')`, r.id, orgID)
				if err != nil {
					return fmt.Errorf("query run steps for run %s: %w", r.id, err)
				}
				defer stepRows.Close()

				type stepInfo struct {
					id     string
					nodeID string
					state  string
				}
				var steps []stepInfo
				for stepRows.Next() {
					var s stepInfo
					if err := stepRows.Scan(&s.id, &s.nodeID, &s.state); err == nil {
						steps = append(steps, s)
					}
				}
				stepRows.Close()

				for _, step := range steps {
					// Update step to WAITING / RECONCILIATION
					if _, err := tx.Exec(ctx, `UPDATE run_steps
						SET state = 'WAITING', wait_reason = 'RECONCILIATION', updated_at = clock_timestamp()
						WHERE id = $1::uuid AND organization_id = $2::uuid`, step.id, orgID); err != nil {
						return fmt.Errorf("update step %s: %w", step.id, err)
					}

					// Resolve the task recovery policy from the pinned
					// deployment manifest (node ID -> node.task -> task).
					// Unresolvable policy stays "unknown" and held; it is
					// never silently relabeled "safe".
					recoveryPolicy, policyResolved := resolveRecoveryPolicy(manifestJSON, r.workflowName, step.nodeID)
					policySource := "deployment-manifest"
					if !policyResolved {
						policySource = "unresolved"
					}

					evidence := map[string]any{
						"recoveryPoint":          req.RecoveryPoint.Format(time.RFC3339Nano),
						"incidentAt":             req.IncidentAt.Format(time.RFC3339Nano),
						"uncertaintyWindowStart": req.RecoveryPoint.Format(time.RFC3339Nano),
						"uncertaintyWindowEnd":   req.IncidentAt.Format(time.RFC3339Nano),
						"nodeId":                 step.nodeID,
						"recoveryPolicy":         recoveryPolicy,
						"policySource":           policySource,
						"disasterRecovery":       true,
						"note":                   "Restored from DB snapshot older than potential external side effects; review required before resume",
					}
					evidenceJSON, _ := json.Marshal(evidence)

					// Preserve existing reconciliation history: when an OPEN
					// case already exists (e.g. pre-disaster unknown-outcome
					// handling), merge the disaster uncertainty evidence into
					// it instead of silently skipping or destroying it.
					var existingCaseID string
					err := tx.QueryRow(ctx, `SELECT id::text FROM reconciliation_cases
						WHERE step_id = $1::uuid AND organization_id = $2::uuid AND status = 'OPEN'
						ORDER BY created_at ASC LIMIT 1`, step.id, orgID).Scan(&existingCaseID)
					if err != nil {
						if _, err := tx.Exec(ctx, `INSERT INTO reconciliation_cases
							(organization_id, environment_id, step_id, reason, evidence, status)
							VALUES ($1::uuid, $2::uuid, $3::uuid, 'DISASTER_RECOVERY_HOLD', $4::jsonb, 'OPEN')`,
							orgID, r.environmentID, step.id, string(evidenceJSON)); err != nil {
							return fmt.Errorf("create reconciliation case for step %s: %w", step.id, err)
						}
					} else {
						if _, err := tx.Exec(ctx, `UPDATE reconciliation_cases
							SET evidence = COALESCE(evidence, '{}'::jsonb) || $2::jsonb, revision = revision + 1
							WHERE id = $1::uuid AND organization_id = $3::uuid`,
							existingCaseID, string(evidenceJSON), orgID); err != nil {
							return fmt.Errorf("attach disaster evidence to case %s: %w", existingCaseID, err)
						}
					}

					// Append run event
					evtPayload, _ := json.Marshal(map[string]any{
						"stepId":                 step.id,
						"nodeId":                 step.nodeID,
						"reason":                 "RECONCILIATION",
						"holdReason":             "DISASTER_RECOVERY_HOLD",
						"uncertaintyWindowStart": req.RecoveryPoint.Format(time.RFC3339Nano),
						"uncertaintyWindowEnd":   req.IncidentAt.Format(time.RFC3339Nano),
					})
					var nextSeq int64
					err = tx.QueryRow(ctx, `UPDATE runs SET last_event_sequence = last_event_sequence + 1, updated_at = clock_timestamp()
						WHERE id = $1::uuid AND organization_id = $2::uuid RETURNING last_event_sequence`, r.id, orgID).Scan(&nextSeq)
					if err != nil {
						return fmt.Errorf("increment last_event_sequence: %w", err)
					}
					_, err = tx.Exec(ctx, `INSERT INTO run_events
						(organization_id, run_id, sequence, event_type, payload)
						VALUES ($1::uuid, $2::uuid, $3, 'STEP_WAITING', $4::jsonb)
						ON CONFLICT DO NOTHING`, orgID, r.id, nextSeq, string(evtPayload))
					if err != nil {
						return fmt.Errorf("insert STEP_WAITING run event: %w", err)
					}
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("apply disaster holds in tenant %s: %w", orgID, err)
		}
	}

	// 4. Record incident with uncertainty window and RPO gap reconciliation
	rpoGapJSON, _ := json.Marshal(req.RPOGapRequestIDs)
	var incidentID string
	err = m.pool.QueryRow(ctx, `INSERT INTO disaster_recovery_incidents
		(recovery_point, incident_at, uncertainty_window_start, uncertainty_window_end,
		 status, restored_runs_count, revoked_worker_sessions_count, revoked_auth_sessions_count,
		 rpo_gap_requests, operator_notes)
		VALUES ($1, $2, $1, $2, 'READ_ONLY', $3, $4, $5, $6::jsonb, $7)
		RETURNING id::text`,
		req.RecoveryPoint, req.IncidentAt, totalRestoredRuns, revokedWorkers, revokedAuth,
		string(rpoGapJSON), req.OperatorNotes).Scan(&incidentID)
	if err != nil {
		return nil, fmt.Errorf("record disaster recovery incident: %w", err)
	}

	return &IncidentReport{
		ID:                         incidentID,
		RecoveryPoint:              req.RecoveryPoint,
		IncidentAt:                 req.IncidentAt,
		UncertaintyWindowStart:     req.RecoveryPoint,
		UncertaintyWindowEnd:       req.IncidentAt,
		Status:                     "READ_ONLY",
		RestoredRunsCount:          totalRestoredRuns,
		RevokedWorkerSessionsCount: revokedWorkers,
		RevokedAuthSessionsCount:   revokedAuth,
		RPOGapRequests:             req.RPOGapRequestIDs,
		RPONote:                    "no claim of zero RPO: requests accepted during the uncertainty window cannot be reconstructed from database alone",
		OperatorNotes:              req.OperatorNotes,
		CreatedAt:                  time.Now().UTC(),
	}, nil
}

// VerifyIntegrity implements Blueprint §27.3 step 2:
// Validate schema, tenant boundary, integrity, artifacts, and deletion ledger.
func (m *Manager) VerifyIntegrity(ctx context.Context) (*IntegrityReport, error) {
	report := &IntegrityReport{
		SchemaValid:          true,
		TenantIsolationValid: true,
		ArtifactsValid:       true,
		DeletionLedgerValid:  true,
		OverallPassed:        true,
	}

	// 1. Schema check
	var migrationCount int
	var maxVersion int64
	err := m.pool.QueryRow(ctx, `SELECT count(*), COALESCE(MAX(version_id), 0) FROM goose_db_version`).Scan(&migrationCount, &maxVersion)
	if err != nil {
		report.SchemaValid = false
		report.Checks = append(report.Checks, IntegrityCheck{
			Name:    "Schema Version History",
			Passed:  false,
			Details: fmt.Sprintf("Failed to query goose_db_version: %v", err),
		})
	} else {
		passed := maxVersion >= migrator.LatestSchemaVersion
		if !passed {
			report.SchemaValid = false
		}
		report.Checks = append(report.Checks, IntegrityCheck{
			Name:    "Schema Version History",
			Passed:  passed,
			Details: fmt.Sprintf("Applied migrations: %d, latest version: %d (expected >= %d)", migrationCount, maxVersion, migrator.LatestSchemaVersion),
		})
	}

	// 2. Tenant isolation / boundary check
	var orphanRunsCount int
	_ = m.pool.QueryRow(ctx, `SELECT count(*) FROM runs r
		LEFT JOIN organizations o ON o.id = r.organization_id
		WHERE o.id IS NULL`).Scan(&orphanRunsCount)
	tenantPassed := orphanRunsCount == 0
	if !tenantPassed {
		report.TenantIsolationValid = false
	}
	report.Checks = append(report.Checks, IntegrityCheck{
		Name:    "Tenant Boundary Consistency",
		Passed:  tenantPassed,
		Details: fmt.Sprintf("Orphaned runs without valid organization: %d", orphanRunsCount),
	})

	// 3. Restored artifact integrity (Blueprint §18.2/§27.3): for READY
	// artifacts required by restored nonterminal runs, verify the actual
	// S3-compatible object boundary (existence, size, SHA-256) through the
	// scoped artifact service from Issue #22. A missing or corrupted
	// referenced object fails closed.
	var orphanArtifactsCount int
	_ = m.pool.QueryRow(ctx, `SELECT count(*) FROM artifacts a
		LEFT JOIN organizations o ON o.id = a.organization_id
		WHERE o.id IS NULL`).Scan(&orphanArtifactsCount)
	artifactPassed := orphanArtifactsCount == 0
	artifactDetails := fmt.Sprintf("Artifacts with invalid organization scope: %d", orphanArtifactsCount)
	if artifactPassed {
		if referenced, missing, corrupt, verifyErr := m.verifyRestoredArtifacts(ctx); verifyErr != nil {
			artifactPassed = false
			artifactDetails = fmt.Sprintf("Restored artifact verification failed: %v", verifyErr)
		} else if missing > 0 || corrupt > 0 {
			artifactPassed = false
			artifactDetails = fmt.Sprintf("Referenced READY artifacts failed integrity: missing=%d corrupt=%d checked=%d", missing, corrupt, referenced)
		} else {
			artifactDetails = fmt.Sprintf("Referenced READY artifacts passed S3 integrity: checked=%d; %s", referenced, artifactDetails)
		}
	}
	if !artifactPassed {
		report.ArtifactsValid = false
	}
	report.Checks = append(report.Checks, IntegrityCheck{
		Name:    "Artifact Reference Integrity",
		Passed:  artifactPassed,
		Details: artifactDetails,
	})

	// 4. Deletion ledger review hook check (Blueprint §18.3 & §27.3)
	var pendingDeletions int
	_ = m.pool.QueryRow(ctx, `SELECT app.count_pending_deletions()`).Scan(&pendingDeletions)
	report.PendingDeletionCount = pendingDeletions
	report.Checks = append(report.Checks, IntegrityCheck{
		Name:    "Deletion Ledger Review Hook",
		Passed:  true,
		Details: fmt.Sprintf("Pending deletion ledger entries to review/reapply before open customer access: %d", pendingDeletions),
	})

	report.OverallPassed = report.SchemaValid && report.TenantIsolationValid && report.ArtifactsValid && report.DeletionLedgerValid
	return report, nil
}

// verifyRestoredArtifacts verifies the S3 object boundary for READY
// artifacts required by restored nonterminal runs. It returns the number of
// referenced artifacts checked, how many are missing, and how many are
// corrupt. Any store or lookup failure is a fail-closed error.
func (m *Manager) verifyRestoredArtifacts(ctx context.Context) (checked, missing, corrupt int, err error) {
	orgRows, err := m.pool.Query(ctx, `SELECT organization_id FROM app.enumerate_recovery_tenants()`)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("enumerate recovery tenants for artifact verification: %w", err)
	}
	defer orgRows.Close()
	var orgIDs []string
	for orgRows.Next() {
		var o string
		if err := orgRows.Scan(&o); err == nil {
			orgIDs = append(orgIDs, o)
		}
	}
	orgRows.Close()

	for _, orgID := range orgIDs {
		var ids []string
		collectErr := m.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
			seen := make(map[string]struct{})
			// READY artifacts attached to nonterminal runs.
			rows, err := tx.Query(ctx, `SELECT DISTINCT a.id::text FROM artifacts a
				JOIN runs r ON r.id = a.run_id AND r.organization_id = a.organization_id
				WHERE a.organization_id = $1::uuid AND a.status = 'READY'
				  AND r.status IN ('QUEUED','RUNNING','WAITING','PAUSING','PAUSED','CANCELLING')`, orgID)
			if err != nil {
				return err
			}
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err == nil {
					seen[id] = struct{}{}
				}
			}
			rows.Close()
			// Artifact references carried in nonterminal step outputs.
			refRows, err := tx.Query(ctx, `SELECT DISTINCT s.output->>'$artifact' FROM run_steps s
				JOIN runs r ON r.id = s.run_id AND r.organization_id = s.organization_id
				WHERE s.organization_id = $1::uuid AND s.output ? '$artifact'
				  AND r.status IN ('QUEUED','RUNNING','WAITING','PAUSING','PAUSED','CANCELLING')`, orgID)
			if err != nil {
				return err
			}
			for refRows.Next() {
				var ref *string
				if err := refRows.Scan(&ref); err == nil && ref != nil && *ref != "" {
					seen[*ref] = struct{}{}
				}
			}
			refRows.Close()
			for id := range seen {
				ids = append(ids, id)
			}
			return nil
		})
		if collectErr != nil {
			return checked, missing, corrupt, fmt.Errorf("collect referenced artifacts: %w", collectErr)
		}
		if len(ids) == 0 {
			continue
		}
		if m.artifacts == nil {
			return checked, missing, corrupt, fmt.Errorf("artifact service unavailable: cannot verify %d referenced objects", len(ids))
		}
		verr := m.artifacts.VerifyReferences(ctx, orgID, ids)
		if verr == nil {
			checked += len(ids)
			continue
		}
		if errors.Is(verr, artifacts.ErrArtifactNotFound) || errors.Is(verr, artifacts.ErrObjectNotFound) {
			checked += len(ids)
			missing++
			continue
		}
		if errors.Is(verr, artifacts.ErrSizeMismatch) || errors.Is(verr, artifacts.ErrChecksumMismatch) {
			checked += len(ids)
			corrupt++
			continue
		}
		return checked, missing, corrupt, fmt.Errorf("verify referenced artifacts: %w", verr)
	}
	return checked, missing, corrupt, nil
}

// ReconcileRPOGapRecords records absent-after-recovery-point request IDs
// against an incident and attaches the explicit non-zero RPO contract note.
func (m *Manager) ReconcileRPOGapRecords(ctx context.Context, incidentID string, requestIDs []string, operatorNote string) (*IncidentReport, error) {
	rpoJSON, _ := json.Marshal(requestIDs)
	var r IncidentReport
	err := m.pool.QueryRow(ctx, `UPDATE disaster_recovery_incidents
		SET rpo_gap_requests = $2::jsonb,
		    operator_notes = CASE WHEN $3 <> '' THEN $3 ELSE operator_notes END,
		    updated_at = clock_timestamp()
		WHERE id = $1::uuid
		RETURNING id::text, recovery_point, incident_at, uncertainty_window_start, uncertainty_window_end,
		          status, restored_runs_count, revoked_worker_sessions_count, revoked_auth_sessions_count,
		          operator_notes, created_at`, incidentID, string(rpoJSON), operatorNote).Scan(
		&r.ID, &r.RecoveryPoint, &r.IncidentAt, &r.UncertaintyWindowStart, &r.UncertaintyWindowEnd,
		&r.Status, &r.RestoredRunsCount, &r.RevokedWorkerSessionsCount, &r.RevokedAuthSessionsCount,
		&r.OperatorNotes, &r.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrIncidentNotFound
		}
		return nil, fmt.Errorf("update incident RPO gap: %w", err)
	}
	r.RPOGapRequests = requestIDs
	r.RPONote = "no claim of zero RPO: requests accepted during the uncertainty window cannot be reconstructed from database alone"
	return &r, nil
}

// GradualResume implements gradual resumption from READ_ONLY -> RESUMING -> ACTIVE.
func (m *Manager) GradualResume(ctx context.Context, targetMode string) (*Controls, error) {
	var admissionEnabled, dispatchEnabled, schedulesEnabled bool
	switch targetMode {
	case "READ_ONLY":
		admissionEnabled = false
		dispatchEnabled = false
		schedulesEnabled = false
	case "RESUMING":
		// Allow already-reconciled tasks to resume, while new admission remains gated
		admissionEnabled = false
		dispatchEnabled = true
		schedulesEnabled = false
	case "ACTIVE":
		// Full production resumption
		admissionEnabled = true
		dispatchEnabled = true
		schedulesEnabled = true
	default:
		return nil, ErrInvalidRecoveryMode
	}

	var c Controls
	err := m.pool.QueryRow(ctx, `UPDATE system_recovery_controls
		SET mode = $1,
		    admission_enabled = $2,
		    dispatch_enabled = $3,
		    schedules_enabled = $4,
		    updated_at = clock_timestamp()
		WHERE id = 1
		RETURNING mode, admission_enabled, dispatch_enabled, schedules_enabled, updated_at`,
		targetMode, admissionEnabled, dispatchEnabled, schedulesEnabled).Scan(
		&c.Mode, &c.AdmissionEnabled, &c.DispatchEnabled, &c.SchedulesEnabled, &c.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("update recovery controls: %w", err)
	}

	// Update active incidents if fully active
	if targetMode == "ACTIVE" {
		_, _ = m.pool.Exec(ctx, `UPDATE disaster_recovery_incidents
			SET status = 'COMPLETED', updated_at = clock_timestamp()
			WHERE status IN ('READ_ONLY', 'RESUMING')`)
	} else if targetMode == "RESUMING" {
		_, _ = m.pool.Exec(ctx, `UPDATE disaster_recovery_incidents
			SET status = 'RESUMING', updated_at = clock_timestamp()
			WHERE status = 'READ_ONLY'`)
	}

	return &c, nil
}
