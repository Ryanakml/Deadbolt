package integration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

// TestCreateRunPreCommitFailureRollsBack proves F-01A: failure before commit
// leaves no run, no half-committed idempotency record, and retry with the
// same key creates exactly one logical run.
func TestCreateRunPreCommitFailureRollsBack(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	schema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"val": map[string]any{"type": "string"}},
		"required":             []any{"val"},
		"additionalProperties": false,
	}
	tasks := []map[string]any{
		{"name": "task-simple", "entrypoint": "tasks/simple.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema},
	}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "f01-flow", "inputSchema": schema, "outputSchema": schema,
		"nodes": []map[string]any{{
			"id": "step-1", "type": "task", "task": "task-simple", "after": []any{},
			"input": map[string]any{"val": map[string]any{"$ref": "run.input", "pointer": "/val"}},
		}},
		"output": map[string]any{"val": map[string]any{"$ref": "step.output", "stepId": "step-1", "pointer": "/val"}},
	}}
	manifest := createLifecycleManifest("1111111111111111111111111111111111111111111111111111111111111111", tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "f01-flow", manifest)

	svc := execution.NewService(tc.pool, tc.service)
	audit := &tenant.AuditContext{CorrelationID: "f01-precommit", Reason: "test"}
	injected := errors.New("injected precommit failure")
	svc.SetBeforeCreateCommitHookForTest(func() error { return injected })

	_, _, err := svc.CreateRun(context.Background(), orgID, envID, "f01-flow", "f01-precommit-key", nil, map[string]any{"val": "hello"}, audit)
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected precommit failure, got %v", err)
	}

	var runs, steps, idemp, outbox, auditCount int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM runs WHERE organization_id=$1::uuid AND idempotency_key=$2),
			(SELECT count(*) FROM run_steps rs JOIN runs r ON r.id=rs.run_id WHERE r.organization_id=$1::uuid AND r.idempotency_key=$2),
			(SELECT count(*) FROM idempotency_records WHERE organization_id=$1::uuid AND environment_id=$3::uuid AND operation_type='CREATE_RUN'),
			(SELECT count(*) FROM outbox_events WHERE organization_id=$1::uuid),
			(SELECT count(*) FROM audit_events WHERE organization_id=$1::uuid AND action='run.create')`, orgID, "f01-precommit-key", envID).Scan(&runs, &steps, &idemp, &outbox, &auditCount)
	}); err != nil {
		t.Fatal(err)
	}
	// Outbox/audit counts are global to the org fixture; the key-scoped
	// assertions are runs/steps/idempotency. Outbox must not contain a hint
	// for a run that was never committed.
	if runs != 0 || steps != 0 || idemp != 0 {
		t.Fatalf("precommit failure leaked state: runs=%d steps=%d idemp=%d outbox=%d audit=%d", runs, steps, idemp, outbox, auditCount)
	}

	svc.SetBeforeCreateCommitHookForTest(nil)
	run, isReplay, err := svc.CreateRun(context.Background(), orgID, envID, "f01-flow", "f01-precommit-key", nil, map[string]any{"val": "hello"}, audit)
	if err != nil || isReplay || run == nil || run.ID == "" {
		t.Fatalf("retry after precommit failure rejected: run=%+v replay=%v err=%v", run, isReplay, err)
	}
}

// TestCreateRunPostCommitFailureReplaysSameID proves F-01B: commit + lost
// response still yields exactly one logical run; retry same key+payload
// returns the SAME run ID and creates no second run.
func TestCreateRunPostCommitFailureReplaysSameID(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	schema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"val": map[string]any{"type": "string"}},
		"required":             []any{"val"},
		"additionalProperties": false,
	}
	tasks := []map[string]any{
		{"name": "task-simple", "entrypoint": "tasks/simple.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema},
	}
	workflows := []map[string]any{{
		"manifestVersion": 1, "name": "f01b-flow", "inputSchema": schema, "outputSchema": schema,
		"nodes": []map[string]any{{
			"id": "step-1", "type": "task", "task": "task-simple", "after": []any{},
			"input": map[string]any{"val": map[string]any{"$ref": "run.input", "pointer": "/val"}},
		}},
		"output": map[string]any{"val": map[string]any{"$ref": "step.output", "stepId": "step-1", "pointer": "/val"}},
	}}
	manifest := createLifecycleManifest("2222222222222222222222222222222222222222222222222222222222222222", tasks, workflows)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "f01b-flow", manifest)

	svc := execution.NewService(tc.pool, tc.service)
	audit := &tenant.AuditContext{CorrelationID: "f01-postcommit", Reason: "test"}
	injected := errors.New("injected postcommit response failure")
	svc.SetAfterCreateCommitHookForTest(func() error { return injected })

	_, _, err := svc.CreateRun(context.Background(), orgID, envID, "f01b-flow", "f01-postcommit-key", nil, map[string]any{"val": "hello"}, audit)
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected postcommit failure, got %v", err)
	}

	var committedRunID string
	var runs, idemp int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT id::text FROM runs WHERE organization_id=$1::uuid AND idempotency_key=$2`, orgID, "f01-postcommit-key").Scan(&committedRunID); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM runs WHERE organization_id=$1::uuid AND idempotency_key=$2),
			(SELECT count(*) FROM idempotency_records WHERE organization_id=$1::uuid AND environment_id=$3::uuid AND operation_type='CREATE_RUN' AND response_identity=$4::uuid)`, orgID, "f01-postcommit-key", envID, committedRunID).Scan(&runs, &idemp)
	}); err != nil {
		t.Fatal(err)
	}
	if committedRunID == "" || runs != 1 || idemp != 1 {
		t.Fatalf("postcommit failure did not commit exactly once: run=%q runs=%d idemp=%d", committedRunID, runs, idemp)
	}

	svc.SetAfterCreateCommitHookForTest(nil)
	retry, isReplay, err := svc.CreateRun(context.Background(), orgID, envID, "f01b-flow", "f01-postcommit-key", nil, map[string]any{"val": "hello"}, audit)
	if err != nil || !isReplay || retry == nil || retry.ID != committedRunID {
		t.Fatalf("retry did not return same run ID: retry=%+v replay=%v err=%v want=%s", retry, isReplay, err, committedRunID)
	}

	var totalRuns int
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM runs WHERE organization_id=$1::uuid AND idempotency_key=$2`, orgID, "f01-postcommit-key").Scan(&totalRuns)
	}); err != nil {
		t.Fatal(err)
	}
	if totalRuns != 1 {
		t.Fatalf("retry created duplicate run: count=%d", totalRuns)
	}
}
