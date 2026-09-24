package integration_test

// Issue #27 Choice & Merge execution acceptance suite (Blueprint §6, §10, §16).
//
// Proves:
// 1. Declarative choice evaluation selects branch matching expression, skipping unselected branches with BRANCH_NOT_SELECTED.
// 2. Merge waits for selected branch terminal only, produces schema-valid {branch, value}, and never hangs on skipped unselected branches.
// 3. Downstream tasks receive schema-valid tagged {branch, value} output.
// 4. Default branch fallback when no explicit conditions match.
// 5. Mismatched operand types strictly fail-fast with INVALID_EXPRESSION.
// 6. Scheduler replay / reconciliation preserves the single selected branch without reopening terminal work.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

func choiceMergeIntSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"x": map[string]any{"type": "integer"}},
		"required":             []any{"x"},
		"additionalProperties": false,
	}
}

func choiceMergeTaggedSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"branch": map[string]any{"type": "string"},
			"value":  choiceMergeIntSchema(),
		},
		"required":             []any{"branch", "value"},
		"additionalProperties": false,
	}
}

func seedChoiceMergeDeployment(t *testing.T, tc *tenantTestContext, orgID, envID, digest, workflowName string, tasks []map[string]any, nodes []map[string]any, output any) string {
	t.Helper()
	manifest := map[string]any{
		"targetOS": "linux", "targetArchitecture": "amd64", "secretNames": []string{},
		"tasks": tasks,
		"workflows": []map[string]any{{
			"manifestVersion": 1, "name": workflowName,
			"inputSchema": choiceMergeIntSchema(), "outputSchema": choiceMergeTaggedSchema(),
			"nodes": nodes, "output": output,
		}},
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var deploymentID string
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO deployments
			(organization_id,environment_id,manifest_hash,bundle_digest,manifest,protocol_version,runtime_version)
			VALUES ($1,$2,$3,$4,$5::jsonb,1,'1.0') RETURNING id::text`,
			orgID, envID, "manifest-"+digest, digest, string(raw)).Scan(&deploymentID)
	})
	if err != nil {
		t.Fatalf("seed deployment: %v", err)
	}
	return deploymentID
}

// TestChoiceMerge_TrueBranchTaken verifies that:
// 1. A choice expression x > 5 selects branch opt_a and immediately marks opt_b as SKIPPED (wait_reason: BRANCH_NOT_SELECTED).
// 2. The task in opt_a becomes READY and is executed.
// 3. The merge node joins the branches, waiting only for opt_a, and produces {branch: "opt_a", value: ...}.
// 4. Downstream task receives the tagged output and the run completes SUCCEEDED.
func TestChoiceMerge_TrueBranchTaken(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundleDigest := "4444444444444444444444444444444444444444444444444444444444444444"
	tasks := []map[string]any{
		parallelTask("task-a", "safe", 3),
		parallelTask("task-b", "safe", 3),
		{
			"name":         "task-downstream",
			"entrypoint":   "tasks/task-downstream.js",
			"timeoutMs":    30000,
			"recovery":     "safe",
			"inputSchema":  choiceMergeTaggedSchema(),
			"outputSchema": choiceMergeTaggedSchema(),
			"retry":        map[string]any{"maxAttempts": 3, "initialDelayMs": 100, "maxDelayMs": 1000},
		},
	}

	nodes := []map[string]any{
		{
			"id":   "decide",
			"type": "choice",
			"choice": map[string]any{
				"branches": []map[string]any{
					{
						"name": "opt_a",
						"condition": map[string]any{
							"op": "gt",
							"args": []any{
								map[string]any{"$ref": "run.input", "pointer": "/x"},
								map[string]any{"literal": 5},
							},
						},
					},
					{
						"name": "opt_b",
					},
				},
				"default": "opt_b",
			},
		},
		{
			"id":    "step_a",
			"type":  "task",
			"task":  "task-a",
			"after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}},
		},
		{
			"id":    "step_b",
			"type":  "task",
			"task":  "task-b",
			"after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}},
		},
		{
			"id":    "join",
			"type":  "merge",
			"after": []any{"step_a", "step_b"},
			"merge": map[string]any{
				"choice": "decide",
				"branches": []map[string]any{
					{
						"branch":   "opt_a",
						"terminal": "step_a",
						"value": map[string]any{
							"x": map[string]any{"$ref": "step.output", "stepId": "step_a", "pointer": "/x"},
						},
					},
					{
						"branch":   "opt_b",
						"terminal": "step_b",
						"value": map[string]any{
							"x": map[string]any{"$ref": "step.output", "stepId": "step_b", "pointer": "/x"},
						},
					},
				},
				"outputSchema": choiceMergeTaggedSchema(),
			},
		},
		{
			"id":    "final_task",
			"type":  "task",
			"task":  "task-downstream",
			"after": []any{"join"},
			"input": map[string]any{
				"branch": map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/branch"},
				"value":  map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/value"},
			},
		},
	}

	outputRef := map[string]any{
		"branch": map[string]any{"$ref": "step.output", "stepId": "final_task", "pointer": "/branch"},
		"value":  map[string]any{"$ref": "step.output", "stepId": "final_task", "pointer": "/value"},
	}

	deploymentID := seedChoiceMergeDeployment(t, tc, orgID, envID, bundleDigest, "choice-flow", tasks, nodes, outputRef)
	workerSession, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cm-worker-1")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, workerSession.SessionID, orgID, bundleDigest)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	execSvc := execution.NewService(tc.pool, tc.service)
	engine := execution.NewWorkerEngine(tc.pool)

	// Create run with x = 10 -> branch opt_a should be chosen
	runDTO, _, err := execSvc.CreateRun(context.Background(), orgID, envID, "choice-flow", "idemp-cm-1", &deploymentID, map[string]any{"x": float64(10)}, &tenant.AuditContext{
		ActorType: tenant.IdentityTypeMachine,
	})
	if err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	runID := runDTO.ID

	// Check step states right after creation:
	type stepInfo struct {
		state      string
		waitReason *string
		output     []byte
	}
	getSteps := func() map[string]stepInfo {
		stepMap := map[string]stepInfo{}
		err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
			rows, err := tx.Query(ctx, `SELECT node_id, state, wait_reason, output FROM run_steps WHERE run_id=$1::uuid`, runID)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var nid, st string
				var wr *string
				var out []byte
				if err := rows.Scan(&nid, &st, &wr, &out); err != nil {
					return err
				}
				stepMap[nid] = stepInfo{state: st, waitReason: wr, output: out}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("getSteps: %v", err)
		}
		return stepMap
	}

	sm := getSteps()
	if sm["decide"].state != "SUCCEEDED" {
		t.Fatalf("expected decide state SUCCEEDED, got %s", sm["decide"].state)
	}
	var decideOut map[string]any
	_ = json.Unmarshal(sm["decide"].output, &decideOut)
	if decideOut["selected"] != "opt_a" {
		t.Fatalf("expected decide output selected=opt_a, got %v", decideOut)
	}

	if sm["step_b"].state != "SKIPPED" {
		t.Fatalf("expected step_b state SKIPPED, got %s", sm["step_b"].state)
	}
	if sm["step_b"].waitReason == nil || *sm["step_b"].waitReason != "BRANCH_NOT_SELECTED" {
		t.Fatalf("expected step_b waitReason BRANCH_NOT_SELECTED, got %v", sm["step_b"].waitReason)
	}

	if sm["step_a"].state != "READY" {
		t.Fatalf("expected step_a state READY, got %s", sm["step_a"].state)
	}
	if sm["join"].state != "BLOCKED" {
		t.Fatalf("expected join state BLOCKED, got %s", sm["join"].state)
	}

	// Worker claims step_a
	assignments := parallelClaim(t, engine, workerSession, orgID, envID, 1, "claim-step-a")
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(assignments))
	}
	a := assignments[0]
	parallelStart(t, engine, workerSession, orgID, envID, a)
	parallelCompleteSuccess(t, engine, workerSession, orgID, envID, a, map[string]any{"x": float64(100)})

	// Now: step_a is SUCCEEDED, join evaluated and SUCCEEDED with tagged output, final_task is READY!
	sm = getSteps()
	if sm["step_a"].state != "SUCCEEDED" {
		t.Fatalf("expected step_a SUCCEEDED, got %s", sm["step_a"].state)
	}
	if sm["join"].state != "SUCCEEDED" {
		t.Fatalf("expected join SUCCEEDED, got %s", sm["join"].state)
	}
	var joinOut map[string]any
	_ = json.Unmarshal(sm["join"].output, &joinOut)
	if joinOut["branch"] != "opt_a" {
		t.Fatalf("expected join output branch=opt_a, got %v", joinOut)
	}
	valObj, ok := joinOut["value"].(map[string]any)
	if !ok || valObj["x"] != float64(100) {
		t.Fatalf("expected join output value.x=100, got %v", joinOut["value"])
	}

	if sm["final_task"].state != "READY" {
		t.Fatalf("expected final_task READY, got %s", sm["final_task"].state)
	}

	// Worker claims and completes final_task
	assignments = parallelClaim(t, engine, workerSession, orgID, envID, 1, "claim-final")
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment for final_task, got %d", len(assignments))
	}
	a = assignments[0]
	parallelStart(t, engine, workerSession, orgID, envID, a)
	parallelCompleteSuccess(t, engine, workerSession, orgID, envID, a, map[string]any{"branch": "opt_a", "value": map[string]any{"x": float64(100)}})

	// Verify run terminal state: SUCCEEDED
	var runStatus string
	var runOutputJSON []byte
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, output FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &runOutputJSON)
	})
	if err != nil {
		t.Fatalf("query run final state: %v", err)
	}
	if runStatus != "SUCCEEDED" {
		t.Fatalf("expected run status SUCCEEDED, got %s", runStatus)
	}

	// Verify STEP_SKIPPED event was appended with reason BRANCH_NOT_SELECTED
	var skippedCount int
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM run_events
			WHERE run_id=$1::uuid AND event_type='STEP_SKIPPED'
			  AND payload->>'reason'='BRANCH_NOT_SELECTED'`, runID).Scan(&skippedCount)
	})
	if err != nil {
		t.Fatalf("count skipped events: %v", err)
	}
	if skippedCount != 1 {
		t.Fatalf("expected 1 STEP_SKIPPED with BRANCH_NOT_SELECTED, got %d", skippedCount)
	}
}

// TestChoiceMerge_DefaultBranchFallback verifies that when x <= 5, the default branch opt_b is taken.
func TestChoiceMerge_DefaultBranchFallback(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundleDigest := "5555555555555555555555555555555555555555555555555555555555555555"
	tasks := []map[string]any{
		parallelTask("task-a", "safe", 3),
		parallelTask("task-b", "safe", 3),
	}

	nodes := []map[string]any{
		{
			"id":   "decide",
			"type": "choice",
			"choice": map[string]any{
				"branches": []map[string]any{
					{
						"name": "opt_a",
						"condition": map[string]any{
							"op": "gt",
							"args": []any{
								map[string]any{"$ref": "run.input", "pointer": "/x"},
								map[string]any{"literal": 5},
							},
						},
					},
					{
						"name": "opt_b",
					},
				},
				"default": "opt_b",
			},
		},
		{
			"id":    "step_a",
			"type":  "task",
			"task":  "task-a",
			"after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}},
		},
		{
			"id":    "step_b",
			"type":  "task",
			"task":  "task-b",
			"after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}},
		},
		{
			"id":    "join",
			"type":  "merge",
			"after": []any{"step_a", "step_b"},
			"merge": map[string]any{
				"choice": "decide",
				"branches": []map[string]any{
					{
						"branch":   "opt_a",
						"terminal": "step_a",
						"value": map[string]any{
							"x": map[string]any{"$ref": "step.output", "stepId": "step_a", "pointer": "/x"},
						},
					},
					{
						"branch":   "opt_b",
						"terminal": "step_b",
						"value": map[string]any{
							"x": map[string]any{"$ref": "step.output", "stepId": "step_b", "pointer": "/x"},
						},
					},
				},
				"outputSchema": choiceMergeTaggedSchema(),
			},
		},
	}

	outputRef := map[string]any{
		"branch": map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/branch"},
		"value":  map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/value"},
	}

	deploymentID := seedChoiceMergeDeployment(t, tc, orgID, envID, bundleDigest, "fallback-flow", tasks, nodes, outputRef)
	workerSession, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cm-worker-2")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, workerSession.SessionID, orgID, bundleDigest)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	execSvc := execution.NewService(tc.pool, tc.service)
	engine := execution.NewWorkerEngine(tc.pool)

	// Create run with x = 2 -> opt_a condition false -> opt_b taken
	runDTO, _, err := execSvc.CreateRun(context.Background(), orgID, envID, "fallback-flow", "idemp-fallback-1", &deploymentID, map[string]any{"x": float64(2)}, &tenant.AuditContext{
		ActorType: tenant.IdentityTypeMachine,
	})
	if err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	runID := runDTO.ID

	// Check step_a is SKIPPED with BRANCH_NOT_SELECTED and step_b is READY
	var stateA, waitReasonA, stateB string
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT state, COALESCE(wait_reason,'') FROM run_steps WHERE run_id=$1::uuid AND node_id='step_a'`, runID).Scan(&stateA, &waitReasonA); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE run_id=$1::uuid AND node_id='step_b'`, runID).Scan(&stateB)
	})
	if err != nil {
		t.Fatalf("query steps: %v", err)
	}

	if stateA != "SKIPPED" || waitReasonA != "BRANCH_NOT_SELECTED" {
		t.Fatalf("expected step_a SKIPPED with BRANCH_NOT_SELECTED, got %s / %s", stateA, waitReasonA)
	}
	if stateB != "READY" {
		t.Fatalf("expected step_b READY, got %s", stateB)
	}

	// Worker claims and executes step_b
	assignments := parallelClaim(t, engine, workerSession, orgID, envID, 1, "claim-step-b")
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(assignments))
	}
	a := assignments[0]
	parallelStart(t, engine, workerSession, orgID, envID, a)
	parallelCompleteSuccess(t, engine, workerSession, orgID, envID, a, map[string]any{"x": float64(200)})

	// Run should now be terminal SUCCEEDED with branch opt_b
	var runStatus string
	var rawOut []byte
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, output FROM runs WHERE id=$1::uuid`, runID).Scan(&runStatus, &rawOut)
	})
	if err != nil {
		t.Fatalf("query run: %v", err)
	}
	if runStatus != "SUCCEEDED" {
		t.Fatalf("expected run SUCCEEDED, got %s", runStatus)
	}
	var outMap map[string]any
	_ = json.Unmarshal(rawOut, &outMap)
	if outMap["branch"] != "opt_b" {
		t.Fatalf("expected output branch=opt_b, got %v", outMap)
	}
}

// TestChoiceMerge_InvalidExpressionFailFast proves that an operand type mismatch
// immediately fails the run with INVALID_EXPRESSION.
func TestChoiceMerge_InvalidExpressionFailFast(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundleDigest := "6666666666666666666666666666666666666666666666666666666666666666"
	tasks := []map[string]any{
		parallelTask("task-a", "safe", 3),
	}

	// Condition comparing int to string with gt -> INVALID_EXPRESSION
	nodes := []map[string]any{
		{
			"id":   "decide",
			"type": "choice",
			"choice": map[string]any{
				"branches": []map[string]any{
					{
						"name": "opt_a",
						"condition": map[string]any{
							"op": "gt",
							"args": []any{
								map[string]any{"$ref": "run.input", "pointer": "/x"},
								map[string]any{"literal": "not-a-number"}, // mismatched operand type!
							},
						},
					},
				},
			},
		},
		{
			"id":    "step_a",
			"type":  "task",
			"task":  "task-a",
			"after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}},
		},
		{
			"id":    "join",
			"type":  "merge",
			"after": []any{"step_a"},
			"merge": map[string]any{
				"choice": "decide",
				"branches": []map[string]any{
					{
						"branch":   "opt_a",
						"terminal": "step_a",
						"value": map[string]any{
							"x": map[string]any{"$ref": "step.output", "stepId": "step_a", "pointer": "/x"},
						},
					},
				},
				"outputSchema": choiceMergeTaggedSchema(),
			},
		},
	}

	outputRef := map[string]any{
		"branch": map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/branch"},
		"value":  map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/value"},
	}

	deploymentID := seedChoiceMergeDeployment(t, tc, orgID, envID, bundleDigest, "failfast-flow", tasks, nodes, outputRef)
	execSvc := execution.NewService(tc.pool, tc.service)

	runDTO, _, err := execSvc.CreateRun(context.Background(), orgID, envID, "failfast-flow", "idemp-failfast-1", &deploymentID, map[string]any{"x": float64(10)}, &tenant.AuditContext{
		ActorType: tenant.IdentityTypeMachine,
	})
	if err != nil {
		t.Fatalf("CreateRun unexpected error: %v", err)
	}

	// Run should have been marked FAILED directly during initial DAG evaluation
	var runStatus, reasonCode string
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code,'') FROM runs WHERE id=$1::uuid`, runDTO.ID).Scan(&runStatus, &reasonCode)
	})
	if err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if runStatus != "FAILED" || reasonCode != "INVALID_EXPRESSION" {
		t.Fatalf("expected run FAILED with INVALID_EXPRESSION, got %s / %s", runStatus, reasonCode)
	}

	// Verify RUN_FAILED event was appended
	var eventCount int
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM run_events
			WHERE run_id=$1::uuid AND event_type='RUN_FAILED'
			  AND payload->>'reason'='INVALID_EXPRESSION'`, runDTO.ID).Scan(&eventCount)
	})
	if err != nil {
		t.Fatalf("count RUN_FAILED events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("expected 1 RUN_FAILED event, got %d", eventCount)
	}
}

// TestChoiceMerge_ReconcilerReplayInvariance verifies that ReconcileReadyWork
// preserves the single selected branch and never re-opens terminal work.
func TestChoiceMerge_ReconcilerReplayInvariance(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundleDigest := "7777777777777777777777777777777777777777777777777777777777777777"
	tasks := []map[string]any{
		parallelTask("task-a", "safe", 3),
		parallelTask("task-b", "safe", 3),
	}

	nodes := []map[string]any{
		{
			"id":   "decide",
			"type": "choice",
			"choice": map[string]any{
				"branches": []map[string]any{
					{
						"name": "opt_a",
						"condition": map[string]any{
							"op": "gt",
							"args": []any{
								map[string]any{"$ref": "run.input", "pointer": "/x"},
								map[string]any{"literal": 5},
							},
						},
					},
					{
						"name": "opt_b",
					},
				},
				"default": "opt_b",
			},
		},
		{
			"id":    "step_a",
			"type":  "task",
			"task":  "task-a",
			"after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}},
		},
		{
			"id":    "step_b",
			"type":  "task",
			"task":  "task-b",
			"after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}},
		},
		{
			"id":    "join",
			"type":  "merge",
			"after": []any{"step_a", "step_b"},
			"merge": map[string]any{
				"choice": "decide",
				"branches": []map[string]any{
					{
						"branch":   "opt_a",
						"terminal": "step_a",
						"value": map[string]any{
							"x": map[string]any{"$ref": "step.output", "stepId": "step_a", "pointer": "/x"},
						},
					},
					{
						"branch":   "opt_b",
						"terminal": "step_b",
						"value": map[string]any{
							"x": map[string]any{"$ref": "step.output", "stepId": "step_b", "pointer": "/x"},
						},
					},
				},
				"outputSchema": choiceMergeTaggedSchema(),
			},
		},
	}

	outputRef := map[string]any{
		"branch": map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/branch"},
		"value":  map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/value"},
	}

	deploymentID := seedChoiceMergeDeployment(t, tc, orgID, envID, bundleDigest, "replay-flow", tasks, nodes, outputRef)
	workerSession, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cm-worker-3")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, workerSession.SessionID, orgID, bundleDigest)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	execSvc := execution.NewService(tc.pool, tc.service)
	engine := execution.NewWorkerEngine(tc.pool)

	runDTO, _, err := execSvc.CreateRun(context.Background(), orgID, envID, "replay-flow", "idemp-replay-1", &deploymentID, map[string]any{"x": float64(10)}, &tenant.AuditContext{
		ActorType: tenant.IdentityTypeMachine,
	})
	if err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	runID := runDTO.ID

	// Complete step_a
	assignments := parallelClaim(t, engine, workerSession, orgID, envID, 1, "claim-replay-a")
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(assignments))
	}
	a := assignments[0]
	parallelStart(t, engine, workerSession, orgID, envID, a)
	parallelCompleteSuccess(t, engine, workerSession, orgID, envID, a, map[string]any{"x": float64(42)})

	// Verify run is SUCCEEDED
	var statusBefore string
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM runs WHERE id=$1::uuid`, runID).Scan(&statusBefore)
	})
	if err != nil {
		t.Fatalf("query status: %v", err)
	}
	if statusBefore != "SUCCEEDED" {
		t.Fatalf("expected run SUCCEEDED, got %s", statusBefore)
	}

	// Run ReconcileReadyWork on the organization:
	// It must NOT reopen any terminal work or change step states
	repaired, err := engine.ReconcileReadyWork(context.Background(), orgID)
	if err != nil {
		t.Fatalf("ReconcileReadyWork failed: %v", err)
	}
	if repaired != 0 {
		t.Fatalf("expected 0 repaired transitions on settled run, got %d", repaired)
	}

	// Verify all step states remain unchanged:
	// step_a: SUCCEEDED
	// step_b: SKIPPED
	// decide: SUCCEEDED
	// join: SUCCEEDED
	var countUnchanged int
	err = tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM run_steps WHERE run_id=$1::uuid AND state IN ('SUCCEEDED', 'SKIPPED')`, runID).Scan(&countUnchanged)
	})
	if err != nil {
		t.Fatalf("query step count: %v", err)
	}
	if countUnchanged != 4 {
		t.Fatalf("expected all 4 steps to remain SUCCEEDED or SKIPPED, got %d", countUnchanged)
	}
}

// TestChoiceMerge_DefaultFirstReversedOrder proves fallback-only semantics:
// the conditionless default listed first must not win immediately. With
// branches [standard (default, conditionless) first, high-value (x>5)
// second], input x=10 must select high-value (declaration order applies
// solely to actual conditions), and input x=2 must fall back to standard.
func TestChoiceMerge_DefaultFirstReversedOrder(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundleDigest := "8888888888888888888888888888888888888888888888888888888888888888"
	tasks := []map[string]any{
		parallelTask("task-a", "safe", 3),
		parallelTask("task-b", "safe", 3),
	}
	// Reversed: conditionless default first.
	nodes := []map[string]any{
		{
			"id": "decide", "type": "choice",
			"choice": map[string]any{
				"branches": []map[string]any{
					{"name": "standard"},
					{"name": "high-value", "condition": map[string]any{
						"op": "gt", "args": []any{
							map[string]any{"$ref": "run.input", "pointer": "/x"},
							map[string]any{"literal": 5},
						},
					}},
				},
				"default": "standard",
			},
		},
		{"id": "manual-review", "type": "task", "task": "task-a", "after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "auto-approve", "type": "task", "task": "task-b", "after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "join", "type": "merge", "after": []any{"manual-review", "auto-approve"},
			"merge": map[string]any{
				"choice": "decide",
				"branches": []map[string]any{
					{"branch": "high-value", "terminal": "manual-review",
						"value": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "manual-review", "pointer": "/x"}}},
					{"branch": "standard", "terminal": "auto-approve",
						"value": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "auto-approve", "pointer": "/x"}}},
				},
				"outputSchema": choiceMergeTaggedSchema(),
			}},
	}
	outputRef := map[string]any{
		"branch": map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/branch"},
		"value":  map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/value"},
	}
	deploymentID := seedChoiceMergeDeployment(t, tc, orgID, envID, bundleDigest, "reversed-flow", tasks, nodes, outputRef)
	execSvc := execution.NewService(tc.pool, tc.service)

	// High input must select high-value despite standard listed first.
	runHigh, _, err := execSvc.CreateRun(context.Background(), orgID, envID, "reversed-flow", "idemp-rev-high", &deploymentID, map[string]any{"x": float64(10)}, &tenant.AuditContext{
		ActorType: tenant.IdentityTypeMachine,
	})
	if err != nil {
		t.Fatalf("CreateRun high failed: %v", err)
	}
	var selectedHigh string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		var out []byte
		if err := tx.QueryRow(ctx, `SELECT output FROM run_steps WHERE run_id=$1::uuid AND node_id='decide'`, runHigh.ID).Scan(&out); err != nil {
			return err
		}
		var decideOut map[string]any
		if err := json.Unmarshal(out, &decideOut); err != nil {
			return err
		}
		selectedHigh, _ = decideOut["selected"].(string)
		return nil
	}); err != nil {
		t.Fatalf("decode decide high: %v", err)
	}
	if selectedHigh != "high-value" {
		t.Fatalf("reversed order: expected high-value for x=10, got %s", selectedHigh)
	}

	// Low input must fall back to standard.
	runLow, _, err := execSvc.CreateRun(context.Background(), orgID, envID, "reversed-flow", "idemp-rev-low", &deploymentID, map[string]any{"x": float64(2)}, &tenant.AuditContext{
		ActorType: tenant.IdentityTypeMachine,
	})
	if err != nil {
		t.Fatalf("CreateRun low failed: %v", err)
	}
	var selectedLow string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		var out []byte
		if err := tx.QueryRow(ctx, `SELECT output FROM run_steps WHERE run_id=$1::uuid AND node_id='decide'`, runLow.ID).Scan(&out); err != nil {
			return err
		}
		var decideOut map[string]any
		if err := json.Unmarshal(out, &decideOut); err != nil {
			return err
		}
		selectedLow, _ = decideOut["selected"].(string)
		return nil
	}); err != nil {
		t.Fatalf("decode decide low: %v", err)
	}
	if selectedLow != "standard" {
		t.Fatalf("reversed order fallback: expected standard for x=2, got %s", selectedLow)
	}
}

// TestChoiceMerge_MergeIgnoresUnselectedInternalDep proves Blueprint §16.2/F-16:
// even when merge.after contains an extra unselected-branch internal node
// (seeded directly to bypass structural validation), the merge still executes
// from the selected terminal and never becomes SKIPPED because of the
// unselected branch.
func TestChoiceMerge_MergeIgnoresUnselectedInternalDep(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundleDigest := "9999999999999999999999999999999999999999999999999999999999999999"
	tasks := []map[string]any{
		parallelTask("task-a", "safe", 3),
		parallelTask("task-b", "safe", 3),
	}
	nodes := []map[string]any{
		{"id": "decide", "type": "choice", "choice": map[string]any{
			"branches": []map[string]any{
				{"name": "opt_a", "condition": map[string]any{
					"op": "gt", "args": []any{
						map[string]any{"$ref": "run.input", "pointer": "/x"},
						map[string]any{"literal": 5},
					}}},
				{"name": "opt_b"},
			},
			"default": "opt_b",
		}},
		{"id": "step_a", "type": "task", "task": "task-a", "after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "low_step1", "type": "task", "task": "task-b", "after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "step_b", "type": "task", "task": "task-b", "after": []any{"low_step1"},
			"input": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "low_step1", "pointer": "/x"}}},
		{"id": "join", "type": "merge", "after": []any{"step_a", "step_b", "low_step1"},
			"merge": map[string]any{
				"choice": "decide",
				"branches": []map[string]any{
					{"branch": "opt_a", "terminal": "step_a",
						"value": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "step_a", "pointer": "/x"}}},
					{"branch": "opt_b", "terminal": "step_b",
						"value": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "step_b", "pointer": "/x"}}},
				},
				"outputSchema": choiceMergeTaggedSchema(),
			}},
	}
	outputRef := map[string]any{
		"branch": map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/branch"},
		"value":  map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/value"},
	}
	deploymentID := seedChoiceMergeDeployment(t, tc, orgID, envID, bundleDigest, "extra-dep-flow", tasks, nodes, outputRef)
	workerSession, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "cm-worker-extra")
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1::uuid,$2::uuid,$3)`, workerSession.SessionID, orgID, bundleDigest)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	execSvc := execution.NewService(tc.pool, tc.service)
	engine := execution.NewWorkerEngine(tc.pool)
	runDTO, _, err := execSvc.CreateRun(context.Background(), orgID, envID, "extra-dep-flow", "idemp-extra-1", &deploymentID, map[string]any{"x": float64(10)}, &tenant.AuditContext{
		ActorType: tenant.IdentityTypeMachine,
	})
	if err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}
	// Selected branch terminal succeeds.
	assignments := parallelClaim(t, engine, workerSession, orgID, envID, 1, "claim-extra-a")
	if len(assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(assignments))
	}
	a := assignments[0]
	parallelStart(t, engine, workerSession, orgID, envID, a)
	parallelCompleteSuccess(t, engine, workerSession, orgID, envID, a, map[string]any{"x": float64(77)})
	// Merge must execute from selected terminal, never SKIPPED via unselected branch.
	var joinState string
	var joinOut []byte
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT state, output FROM run_steps WHERE run_id=$1::uuid AND node_id='join'`, runDTO.ID).Scan(&joinState, &joinOut)
	}); err != nil {
		t.Fatalf("query join: %v", err)
	}
	if joinState != "SUCCEEDED" {
		t.Fatalf("expected join SUCCEEDED despite extra unselected dep, got %s", joinState)
	}
	var joinMap map[string]any
	_ = json.Unmarshal(joinOut, &joinMap)
	if joinMap["branch"] != "opt_a" {
		t.Fatalf("expected join branch opt_a, got %v", joinMap)
	}
}

// TestChoiceMerge_NoMatchNoDefaultFails proves that with no matching condition
// and no declared default, execution fails fast with INVALID_EXPRESSION.
func TestChoiceMerge_NoMatchNoDefaultFails(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundleDigest := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tasks := []map[string]any{parallelTask("task-a", "safe", 3), parallelTask("task-b", "safe", 3)}
	nodes := []map[string]any{
		{"id": "decide", "type": "choice", "choice": map[string]any{
			"branches": []map[string]any{
				{"name": "opt_a", "condition": map[string]any{
					"op": "gt", "args": []any{
						map[string]any{"$ref": "run.input", "pointer": "/x"},
						map[string]any{"literal": 100},
					}}},
				{"name": "opt_b", "condition": map[string]any{
					"op": "lt", "args": []any{
						map[string]any{"$ref": "run.input", "pointer": "/x"},
						map[string]any{"literal": 0},
					}}},
			},
		}},
		{"id": "step_a", "type": "task", "task": "task-a", "after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "step_b", "type": "task", "task": "task-b", "after": []any{"decide"},
			"input": map[string]any{"x": map[string]any{"$ref": "run.input", "pointer": "/x"}}},
		{"id": "join", "type": "merge", "after": []any{"step_a", "step_b"},
			"merge": map[string]any{
				"choice": "decide",
				"branches": []map[string]any{
					{"branch": "opt_a", "terminal": "step_a",
						"value": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "step_a", "pointer": "/x"}}},
					{"branch": "opt_b", "terminal": "step_b",
						"value": map[string]any{"x": map[string]any{"$ref": "step.output", "stepId": "step_b", "pointer": "/x"}}},
				},
				"outputSchema": choiceMergeTaggedSchema(),
			}},
	}
	outputRef := map[string]any{
		"branch": map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/branch"},
		"value":  map[string]any{"$ref": "step.output", "stepId": "join", "pointer": "/value"},
	}
	deploymentID := seedChoiceMergeDeployment(t, tc, orgID, envID, bundleDigest, "no-match-flow", tasks, nodes, outputRef)
	execSvc := execution.NewService(tc.pool, tc.service)
	runDTO, _, err := execSvc.CreateRun(context.Background(), orgID, envID, "no-match-flow", "idemp-nomatch-1", &deploymentID, map[string]any{"x": float64(10)}, &tenant.AuditContext{
		ActorType: tenant.IdentityTypeMachine,
	})
	if err != nil {
		t.Fatalf("CreateRun unexpected error: %v", err)
	}
	var status, reason string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, COALESCE(reason_code,'') FROM runs WHERE id=$1::uuid`, runDTO.ID).Scan(&status, &reason)
	}); err != nil {
		t.Fatalf("query run: %v", err)
	}
	if status != "FAILED" || reason != "INVALID_EXPRESSION" {
		t.Fatalf("expected FAILED/INVALID_EXPRESSION for no match no default, got %s/%s", status, reason)
	}
}
