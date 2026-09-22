package integration_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/storage"
)

func TestCreateRunTokenBucketRefillAndAdmission429(t *testing.T) {
	tc, server, orgID, envID, adminKey := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	bundle := "2222222222222222222222222222222222222222222222222222222222222222"
	schema := map[string]any{
		"type":                 "object",
		"properties":           map[string]any{"val": map[string]any{"type": "string"}},
		"required":             []any{"val"},
		"additionalProperties": false,
	}
	manifest := createLifecycleManifest(bundle,
		[]map[string]any{{"name": "rate-task", "entrypoint": "tasks/rate.js", "timeoutMs": 30000, "recovery": "idempotent", "idempotencyWindowMs": 305000, "inputSchema": schema, "outputSchema": schema}},
		[]map[string]any{{"manifestVersion": 1, "name": "rate-flow", "inputSchema": schema, "outputSchema": schema, "nodes": []map[string]any{{"id": "step-1", "type": "task", "task": "rate-task", "after": []any{}, "input": map[string]any{"val": map[string]any{"$ref": "run.input", "pointer": "/val"}}}}, "output": map[string]any{"val": map[string]any{"$ref": "step.output", "stepId": "step-1", "pointer": "/val"}}}},
	)
	registerAndActivateTestWorkflow(t, tc, server, adminKey, orgID, envID, "rate-flow", manifest)

	create := func(key string) int {
		body := bytes.NewReader([]byte(`{"environment":"staging","input":{"val":"ok"}}`))
		req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/workflows/rate-flow/runs", body)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
		req.Header.Set("X-Organization-ID", orgID)
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE environment_admissions
			SET create_rate_tokens=0, create_rate_updated_at=clock_timestamp()
			WHERE environment_id=$1::uuid AND organization_id=$2::uuid`, envID, orgID)
		return err
	}); err != nil {
		t.Fatalf("prepare empty bucket: %v", err)
	}
	if status := create("rate-empty"); status != http.StatusTooManyRequests {
		t.Fatalf("empty bucket status=%d, want 429", status)
	}

	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE environment_admissions
			SET create_rate_tokens=0, create_rate_updated_at=clock_timestamp()-INTERVAL '1 second'
			WHERE environment_id=$1::uuid AND organization_id=$2::uuid`, envID, orgID)
		return err
	}); err != nil {
		t.Fatalf("prepare token refill: %v", err)
	}
	if status := create("rate-refilled"); status != http.StatusAccepted {
		t.Fatalf("refilled request status=%d, want 202", status)
	}
}
