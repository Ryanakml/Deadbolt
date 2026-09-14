package integration_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/deployment"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

func deploymentManifest(t *testing.T, bundle string) []byte {
	t.Helper()
	schema := map[string]any{"type": "object", "properties": map[string]any{"done": map[string]any{"type": "boolean"}}, "required": []any{"done"}}
	m := map[string]any{"manifestVersion": 1, "sdkVersion": "test", "protocolMajor": 1, "nodeRuntimeMajor": 24, "targetOS": "linux", "targetArchitecture": "amd64", "dependencyLockDigest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bundleDigest": bundle, "secretNames": []any{}, "tasks": []any{map[string]any{"name": "send", "inputSchema": map[string]any{}, "outputSchema": schema, "recovery": "idempotent", "idempotencyWindowMs": 305000, "entrypoint": "./send.js"}}, "workflows": []any{map[string]any{"manifestVersion": 1, "name": "wf", "inputSchema": map[string]any{}, "outputSchema": schema, "nodes": []any{map[string]any{"id": "send", "type": "task", "task": "send"}}, "output": map[string]any{"done": map[string]any{"$ref": "step.output", "stepId": "send", "pointer": "/done"}}}}}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDeploymentRegistrationIsImmutableAndPersistsCompleteDefinitions(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	ctx := context.Background()
	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "deployment fixture")
	if err != nil {
		t.Fatal(err)
	}
	project, err := tc.service.CreateProject(ctx, org.ID, "project")
	if err != nil {
		t.Fatal(err)
	}
	env, err := tc.service.CreateEnvironment(ctx, org.ID, project.ID, tenant.EnvStaging, 1)
	if err != nil {
		t.Fatal(err)
	}
	svc := deployment.NewService(tc.pool, tc.service)
	audit := &tenant.AuditContext{ActorID: &owner, ActorType: tenant.IdentityTypeHuman, Role: tenant.RoleOwner, Capabilities: tenant.RoleCapabilities(tenant.RoleOwner), CorrelationID: "fixture"}
	bundle := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	first, created, err := svc.Register(ctx, org.ID, env.ID, deploymentManifest(t, bundle), audit)
	if err != nil || !created {
		t.Fatalf("first registration: created=%v err=%v", created, err)
	}
	second, created, err := svc.Register(ctx, org.ID, env.ID, deploymentManifest(t, bundle), audit)
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("idempotent replay: created=%v first=%s second=%s err=%v", created, first.ID, second.ID, err)
	}
	altered := deploymentManifest(t, bundle)
	var v map[string]any
	_ = json.Unmarshal(altered, &v)
	v["sdkVersion"] = "altered"
	altered, _ = json.Marshal(v)
	if _, _, err := svc.Register(ctx, org.ID, env.ID, altered, audit); err != deployment.ErrImmutable {
		t.Fatalf("expected immutable conflict, got %v", err)
	}
	// An existing deployment accepted under a fresh idempotency key records and
	// replays 200, not the create-path 201.
	cmdKey := "existing-manifest-replay"
	cmdCtx := tenant.ContextWithCommand(ctx, tenant.Command{Scope: "org:" + org.ID + ":user:" + owner, Key: cmdKey, Operation: "POST /deployments?environment=" + env.ID, Fingerprint: tenant.RequestFingerprint("POST", "/deployments?environment="+env.ID, deploymentManifest(t, bundle))})
	if _, created, err := svc.Register(cmdCtx, org.ID, env.ID, deploymentManifest(t, bundle), audit); err != nil || created {
		t.Fatalf("existing command registration: created=%v err=%v", created, err)
	}
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		var code int
		if err := tx.QueryRow(ctx, `SELECT response_code FROM tenant_commands WHERE idempotency_key=$1`, cmdKey).Scan(&code); err != nil {
			return err
		}
		if code != 200 {
			t.Fatalf("expected recorded 200, got %d", code)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		var window int64
		var mapping []byte
		if err := tx.QueryRow(ctx, `SELECT t.idempotency_window_ms,w.output_mapping FROM task_definitions t JOIN workflow_definitions w ON w.deployment_id=t.deployment_id WHERE t.deployment_id=$1`, first.ID).Scan(&window, &mapping); err != nil {
			return err
		}
		if window != 305000 || string(mapping) == "{}" {
			t.Fatalf("definition fields not persisted: window=%d mapping=%s", window, mapping)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestProductionActivationRequiresDistinctCompatibleWorkers(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	ctx := context.Background()
	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "worker fixture")
	if err != nil {
		t.Fatal(err)
	}
	project, err := tc.service.CreateProject(ctx, org.ID, "project")
	if err != nil {
		t.Fatal(err)
	}
	env, err := tc.service.CreateEnvironment(ctx, org.ID, project.ID, tenant.EnvProduction, 1)
	if err != nil {
		t.Fatal(err)
	}
	svc := deployment.NewService(tc.pool, tc.service)
	audit := &tenant.AuditContext{ActorID: &owner, ActorType: tenant.IdentityTypeHuman, Role: tenant.RoleOwner, Capabilities: tenant.RoleCapabilities(tenant.RoleOwner), CorrelationID: "fixture"}
	bundle := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	d, _, err := svc.Register(ctx, org.ID, env.ID, deploymentManifest(t, bundle), audit)
	if err != nil {
		t.Fatal(err)
	}
	seed := func(worker string, sessions int) {
		err := tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO workers (id,organization_id,environment_id,public_key) VALUES ($1,$2,$3,'pk')`, worker, org.ID, env.ID)
			if err != nil {
				return err
			}
			for i := 0; i < sessions; i++ {
				sid, _ := tenant.NewUUID()
				if _, err := tx.Exec(ctx, `INSERT INTO worker_sessions (id,organization_id,worker_id,environment_id,session_token_hash,expires_at) VALUES ($1,$2,$3,$4,'h',clock_timestamp()+interval '1 hour')`, sid, org.ID, worker, env.ID); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1,$2,$3)`, sid, org.ID, bundle); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	w1, _ := tenant.NewUUID()
	seed(w1, 2)
	if _, err := svc.Activate(ctx, org.ID, env.ID, "wf", d.ID, 0, false, audit); err == nil {
		t.Fatal("two sessions from one worker must not satisfy production preflight")
	}
	w2, _ := tenant.NewUUID()
	seed(w2, 1)
	if _, err := svc.Activate(ctx, org.ID, env.ID, "wf", d.ID, 0, false, audit); err != nil {
		t.Fatalf("two distinct workers should activate: %v", err)
	}
}
