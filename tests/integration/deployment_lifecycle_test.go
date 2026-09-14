package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/deployment"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/jackc/pgx/v5/pgconn"
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
	first, code, err := svc.Register(ctx, org.ID, env.ID, deploymentManifest(t, bundle), audit)
	if err != nil || code != 201 {
		t.Fatalf("first registration: code=%d err=%v", code, err)
	}
	second, code, err := svc.Register(ctx, org.ID, env.ID, deploymentManifest(t, bundle), audit)
	if err != nil || code != 200 || second.ID != first.ID {
		t.Fatalf("idempotent replay: code=%d first=%s second=%s err=%v", code, first.ID, second.ID, err)
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
	if _, code, err := svc.Register(cmdCtx, org.ID, env.ID, deploymentManifest(t, bundle), audit); err != nil || code != 200 {
		t.Fatalf("existing command registration: code=%d err=%v", code, err)
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

// TestDeploymentAvailabilityLossWarningAndOutbox proves that an active deployment
// preserves its ACTIVE pointer when all workers are lost, sets compatibility_warning_at,
// emits exactly one deployment.compatibility_lost outbox event, and resets/re-emits cleanly
// across worker recovery cycles.
func TestDeploymentAvailabilityLossWarningAndOutbox(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	ctx := context.Background()
	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "availability-fixture")
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
	audit := &tenant.AuditContext{ActorID: &owner, ActorType: tenant.IdentityTypeHuman, Role: tenant.RoleOwner, Capabilities: tenant.RoleCapabilities(tenant.RoleOwner), CorrelationID: "avail-fixture"}
	bundle := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	d, code, err := svc.Register(ctx, org.ID, env.ID, deploymentManifest(t, bundle), audit)
	if err != nil || code != 201 {
		t.Fatalf("register: code=%d err=%v", code, err)
	}

	// 1. Initial status is REGISTERED
	if d.Status != "REGISTERED" {
		t.Fatalf("expected REGISTERED, got %s", d.Status)
	}

	// 2. Add worker with compatible session -> ReconcileAvailability sets AVAILABLE
	workerID, _ := tenant.NewUUID()
	sessionID, _ := tenant.NewUUID()
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO workers (id,organization_id,environment_id,public_key,status) VALUES ($1,$2,$3,'pk','ACTIVE')`, workerID, org.ID, env.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO worker_sessions (id,organization_id,worker_id,environment_id,session_token_hash,expires_at) VALUES ($1,$2,$3,$4,'h',clock_timestamp()+interval '1 hour')`, sessionID, org.ID, workerID, env.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1,$2,$3)`, sessionID, org.ID, bundle); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ReconcileAvailability(ctx, org.ID, env.ID); err != nil {
		t.Fatal(err)
	}
	var status string
	var warningAt *time.Time
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, compatibility_warning_at FROM deployments WHERE id=$1`, d.ID).Scan(&status, &warningAt)
	})
	if err != nil {
		t.Fatal(err)
	}
	if status != "AVAILABLE" {
		t.Fatalf("expected AVAILABLE after worker arrival, got %s", status)
	}
	if warningAt != nil {
		t.Fatalf("expected compatibility_warning_at to be nil, got %v", warningAt)
	}

	// 3. Activate workflow channel with allowSingleWorker=true in staging -> status becomes ACTIVE
	wf, err := svc.Activate(ctx, org.ID, env.ID, "wf", d.ID, 0, true, audit)
	if err != nil {
		t.Fatalf("activate failed: %v", err)
	}
	if wf.Warning != "single compatible worker; no failover" {
		t.Fatalf("expected single worker warning, got %q", wf.Warning)
	}
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, compatibility_warning_at FROM deployments WHERE id=$1`, d.ID).Scan(&status, &warningAt)
	})
	if err != nil {
		t.Fatal(err)
	}
	if status != "ACTIVE" {
		t.Fatalf("expected ACTIVE after activation, got %s", status)
	}
	if warningAt != nil {
		t.Fatalf("expected compatibility_warning_at to be nil after activation, got %v", warningAt)
	}

	// 4. Revoke worker session -> call ReconcileAvailability
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE worker_sessions SET revoked_at=clock_timestamp() WHERE id=$1`, sessionID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ReconcileAvailability(ctx, org.ID, env.ID); err != nil {
		t.Fatal(err)
	}

	// Assert ACTIVE remains, compatibility_warning_at is set, and one outbox event emitted
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, compatibility_warning_at FROM deployments WHERE id=$1`, d.ID).Scan(&status, &warningAt)
	})
	if err != nil {
		t.Fatal(err)
	}
	if status != "ACTIVE" {
		t.Fatalf("expected status to remain ACTIVE when workers disappear, got %s", status)
	}
	if warningAt == nil {
		t.Fatal("expected compatibility_warning_at to be set")
	}

	countOutbox := func() int {
		var cnt int
		err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
			return tx.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE organization_id=$1 AND subject='deployment.compatibility_lost'`, org.ID).Scan(&cnt)
		})
		if err != nil {
			t.Fatal(err)
		}
		return cnt
	}
	if cnt := countOutbox(); cnt != 1 {
		t.Fatalf("expected exactly 1 compatibility_lost outbox event, got %d", cnt)
	}

	// Calling ReconcileAvailability again while still disconnected should be idempotent (no duplicate outbox event)
	if err := svc.ReconcileAvailability(ctx, org.ID, env.ID); err != nil {
		t.Fatal(err)
	}
	if cnt := countOutbox(); cnt != 1 {
		t.Fatalf("expected still exactly 1 compatibility_lost outbox event, got %d", cnt)
	}

	// 5. Restore compatible worker session -> call ReconcileAvailability
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE worker_sessions SET revoked_at=NULL WHERE id=$1`, sessionID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ReconcileAvailability(ctx, org.ID, env.ID); err != nil {
		t.Fatal(err)
	}

	// Assert compatibility_warning_at is reset to NULL
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, compatibility_warning_at FROM deployments WHERE id=$1`, d.ID).Scan(&status, &warningAt)
	})
	if err != nil {
		t.Fatal(err)
	}
	if status != "ACTIVE" {
		t.Fatalf("expected ACTIVE, got %s", status)
	}
	if warningAt != nil {
		t.Fatalf("expected compatibility_warning_at to be reset to nil, got %v", warningAt)
	}

	// 6. Worker lost again -> warning set again, second outbox event emitted
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE worker_sessions SET revoked_at=clock_timestamp() WHERE id=$1`, sessionID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.ReconcileAvailability(ctx, org.ID, env.ID); err != nil {
		t.Fatal(err)
	}
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT status, compatibility_warning_at FROM deployments WHERE id=$1`, d.ID).Scan(&status, &warningAt)
	})
	if err != nil {
		t.Fatal(err)
	}
	if warningAt == nil {
		t.Fatal("expected compatibility_warning_at to be re-set")
	}
	if cnt := countOutbox(); cnt != 2 {
		t.Fatalf("expected 2 compatibility_lost outbox events after cycle, got %d", cnt)
	}
}

// TestConcurrentSameBundleDifferentManifestRegistrationConflict proves that two
// concurrent registrations with different manifests but the same bundle digest do not
// abort the transaction or fail with 500, but deterministically return ErrImmutable.
func TestConcurrentSameBundleDifferentManifestRegistrationConflict(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	ctx := context.Background()
	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "race-fixture")
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
	audit := &tenant.AuditContext{ActorID: &owner, ActorType: tenant.IdentityTypeHuman, Role: tenant.RoleOwner, Capabilities: tenant.RoleCapabilities(tenant.RoleOwner), CorrelationID: "race-fixture"}
	bundle := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

	m1 := deploymentManifest(t, bundle)
	var v map[string]any
	_ = json.Unmarshal(m1, &v)
	v["sdkVersion"] = "variant-b"
	m2, _ := json.Marshal(v)

	const concurrency = 8
	errs := make(chan error, concurrency)
	codes := make(chan int, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		manifest := m1
		if i%2 == 1 {
			manifest = m2
		}
		go func(body []byte) {
			defer wg.Done()
			_, code, err := svc.Register(ctx, org.ID, env.ID, body, audit)
			errs <- err
			codes <- code
		}(manifest)
	}
	wg.Wait()
	close(errs)
	close(codes)

	conflictCount := 0
	successCount := 0
	for err := range errs {
		if err == nil {
			successCount++
		} else if errors.Is(err, deployment.ErrImmutable) {
			conflictCount++
		} else {
			t.Fatalf("unexpected error during concurrent registration: %v", err)
		}
	}
	if successCount == 0 {
		t.Fatal("expected at least one successful registration")
	}
	if conflictCount == 0 {
		t.Fatal("expected conflicting registrations to return ErrImmutable")
	}
}

// TestConcurrentSameKeyRegistrationReplay proves that concurrent identical requests
// with the same fresh Idempotency-Key both receive HTTP 201 Created (replaying the
// recorded 201 response code rather than falling through to 200).
func TestConcurrentSameKeyRegistrationReplay(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	ctx := context.Background()
	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "replay-race-fixture")
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

	prodMux := controlplane.BuildMux(tc.authCfg, tc.runtimePool, nil, nil)
	server := httptest.NewServer(prodMux)
	defer server.Close()

	key := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapDeploymentsRegister})
	bundle := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	manifest := deploymentManifest(t, bundle)

	idempKey := "concurrent-fresh-idemp-key"
	const concurrency = 4
	var wg sync.WaitGroup
	statusCodes := make(chan int, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/deployments?environment=%s", server.URL, env.ID), bytes.NewReader(manifest))
			req.Header.Set("Authorization", "Bearer "+key.PlaintextKey)
			req.Header.Set("X-Organization-ID", org.ID)
			req.Header.Set("Idempotency-Key", idempKey)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("request failed: %v", err)
				return
			}
			defer resp.Body.Close()
			statusCodes <- resp.StatusCode
		}()
	}
	wg.Wait()
	close(statusCodes)

	for code := range statusCodes {
		if code != http.StatusCreated {
			t.Fatalf("expected every concurrent same-key request to receive 201, got %d", code)
		}
	}
}

// TestDeploymentHTTPAcceptanceMatrix exercises the full HTTP route-level and DB-backed
// acceptance requirements for deployment registration, activation, idempotency replay/conflict,
// capability/role + cross-environment behavior, revision conflict, single-worker warnings,
// production override rejection, and deletion protection.
func TestDeploymentHTTPAcceptanceMatrix(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	ctx := context.Background()
	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "acceptance-matrix-fixture")
	if err != nil {
		t.Fatal(err)
	}
	project, err := tc.service.CreateProject(ctx, org.ID, "matrix-proj")
	if err != nil {
		t.Fatal(err)
	}
	stagingEnv, err := tc.service.CreateEnvironment(ctx, org.ID, project.ID, tenant.EnvStaging, 1)
	if err != nil {
		t.Fatal(err)
	}
	prodEnv, err := tc.service.CreateEnvironment(ctx, org.ID, project.ID, tenant.EnvProduction, 2)
	if err != nil {
		t.Fatal(err)
	}

	prodMux := controlplane.BuildMux(tc.authCfg, tc.runtimePool, nil, nil)
	server := httptest.NewServer(prodMux)
	defer server.Close()

	// Keys
	regKey := bootstrapTestKey(t, tc.service, org.ID, stagingEnv.ID, []string{tenant.CapDeploymentsRegister})
	actStagingOnlyKey := bootstrapTestKey(t, tc.service, org.ID, stagingEnv.ID, []string{tenant.CapDeploymentsActivateStaging})
	actProdOnlyKey := bootstrapTestKey(t, tc.service, org.ID, prodEnv.ID, []string{tenant.CapDeploymentsActivateProd})
	actStagingOnProdKey := bootstrapTestKey(t, tc.service, org.ID, prodEnv.ID, []string{tenant.CapDeploymentsActivateStaging})

	bundle := "1111111111111111111111111111111111111111111111111111111111111111"
	manifest := deploymentManifest(t, bundle)

	// 1. Cross-environment negative: Machine key for stagingEnv attempting to register in prodEnv
	crossReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/deployments?environment=%s", server.URL, prodEnv.ID), bytes.NewReader(manifest))
	crossReq.Header.Set("Authorization", "Bearer "+regKey.PlaintextKey)
	crossReq.Header.Set("X-Organization-ID", org.ID)
	crossReq.Header.Set("Idempotency-Key", "key-cross-env")
	crossReq.Header.Set("Content-Type", "application/json")
	respCross, err := http.DefaultClient.Do(crossReq)
	if err != nil {
		t.Fatal(err)
	}
	respCross.Body.Close()
	if respCross.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 ENVIRONMENT_MISMATCH, got %d", respCross.StatusCode)
	}

	// 2. Capability negative: Machine key missing deployments:register
	noCapReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/deployments?environment=%s", server.URL, stagingEnv.ID), bytes.NewReader(manifest))
	noCapReq.Header.Set("Authorization", "Bearer "+actStagingOnlyKey.PlaintextKey)
	noCapReq.Header.Set("X-Organization-ID", org.ID)
	noCapReq.Header.Set("Idempotency-Key", "key-no-cap")
	noCapReq.Header.Set("Content-Type", "application/json")
	respNoCap, err := http.DefaultClient.Do(noCapReq)
	if err != nil {
		t.Fatal(err)
	}
	respNoCap.Body.Close()
	if respNoCap.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 FORBIDDEN for missing capability, got %d", respNoCap.StatusCode)
	}

	// 3. Contract validation negative: unsupported protocol major version -> 422
	badProtocol := deploymentManifest(t, bundle)
	var badV map[string]any
	_ = json.Unmarshal(badProtocol, &badV)
	badV["protocolMajor"] = 99
	badProtocol, _ = json.Marshal(badV)

	badReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/deployments?environment=%s", server.URL, stagingEnv.ID), bytes.NewReader(badProtocol))
	badReq.Header.Set("Authorization", "Bearer "+regKey.PlaintextKey)
	badReq.Header.Set("X-Organization-ID", org.ID)
	badReq.Header.Set("Idempotency-Key", "key-bad-protocol")
	badReq.Header.Set("Content-Type", "application/json")
	respBad, err := http.DefaultClient.Do(badReq)
	if err != nil {
		t.Fatal(err)
	}
	var badErrBody map[string]any
	_ = json.NewDecoder(respBad.Body).Decode(&badErrBody)
	respBad.Body.Close()
	if respBad.StatusCode != http.StatusUnprocessableEntity || badErrBody["code"] != "INVALID_MANIFEST" {
		t.Fatalf("expected 422 INVALID_MANIFEST, got %d (%v)", respBad.StatusCode, badErrBody)
	}

	// 4. Successful registration: 201 Created
	regReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/deployments?environment=%s", server.URL, stagingEnv.ID), bytes.NewReader(manifest))
	regReq.Header.Set("Authorization", "Bearer "+regKey.PlaintextKey)
	regReq.Header.Set("X-Organization-ID", org.ID)
	regReq.Header.Set("Idempotency-Key", "key-fresh-create")
	regReq.Header.Set("Content-Type", "application/json")
	respReg, err := http.DefaultClient.Do(regReq)
	if err != nil {
		t.Fatal(err)
	}
	var regBody deployment.Deployment
	_ = json.NewDecoder(respReg.Body).Decode(&regBody)
	respReg.Body.Close()
	if respReg.StatusCode != http.StatusCreated || regBody.ID == "" || regBody.Status != "REGISTERED" {
		t.Fatalf("expected 201 REGISTERED, got %d (%+v)", respReg.StatusCode, regBody)
	}

	// 5. Idempotent replay with same key -> 201 Created, same deployment ID
	replayReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/deployments?environment=%s", server.URL, stagingEnv.ID), bytes.NewReader(manifest))
	replayReq.Header.Set("Authorization", "Bearer "+regKey.PlaintextKey)
	replayReq.Header.Set("X-Organization-ID", org.ID)
	replayReq.Header.Set("Idempotency-Key", "key-fresh-create")
	replayReq.Header.Set("Content-Type", "application/json")
	respReplay, err := http.DefaultClient.Do(replayReq)
	if err != nil {
		t.Fatal(err)
	}
	var replayBody deployment.Deployment
	_ = json.NewDecoder(respReplay.Body).Decode(&replayBody)
	respReplay.Body.Close()
	if respReplay.StatusCode != http.StatusCreated || replayBody.ID != regBody.ID {
		t.Fatalf("expected 201 replay with same ID, got %d (%+v)", respReplay.StatusCode, replayBody)
	}

	// 6. Idempotency conflict: same key with modified body
	conflictReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/deployments?environment=%s", server.URL, stagingEnv.ID), bytes.NewReader([]byte(`{"invalid":"json"}`)))
	conflictReq.Header.Set("Authorization", "Bearer "+regKey.PlaintextKey)
	conflictReq.Header.Set("X-Organization-ID", org.ID)
	conflictReq.Header.Set("Idempotency-Key", "key-fresh-create")
	conflictReq.Header.Set("Content-Type", "application/json")
	respConflict, err := http.DefaultClient.Do(conflictReq)
	if err != nil {
		t.Fatal(err)
	}
	respConflict.Body.Close()
	if respConflict.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 IDEMPOTENCY_CONFLICT, got %d", respConflict.StatusCode)
	}

	// 7. Register existing manifest under fresh key -> 200 OK
	secondKeyReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/deployments?environment=%s", server.URL, stagingEnv.ID), bytes.NewReader(manifest))
	secondKeyReq.Header.Set("Authorization", "Bearer "+regKey.PlaintextKey)
	secondKeyReq.Header.Set("X-Organization-ID", org.ID)
	secondKeyReq.Header.Set("Idempotency-Key", "key-second-existing")
	secondKeyReq.Header.Set("Content-Type", "application/json")
	respSecondKey, err := http.DefaultClient.Do(secondKeyReq)
	if err != nil {
		t.Fatal(err)
	}
	var secondKeyBody deployment.Deployment
	_ = json.NewDecoder(respSecondKey.Body).Decode(&secondKeyBody)
	respSecondKey.Body.Close()
	if respSecondKey.StatusCode != http.StatusOK || secondKeyBody.ID != regBody.ID {
		t.Fatalf("expected 200 for existing manifest, got %d", respSecondKey.StatusCode)
	}

	// 8. Replay second key -> 200 OK
	replaySecondReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/deployments?environment=%s", server.URL, stagingEnv.ID), bytes.NewReader(manifest))
	replaySecondReq.Header.Set("Authorization", "Bearer "+regKey.PlaintextKey)
	replaySecondReq.Header.Set("X-Organization-ID", org.ID)
	replaySecondReq.Header.Set("Idempotency-Key", "key-second-existing")
	replaySecondReq.Header.Set("Content-Type", "application/json")
	respReplaySecond, err := http.DefaultClient.Do(replaySecondReq)
	if err != nil {
		t.Fatal(err)
	}
	respReplaySecond.Body.Close()
	if respReplaySecond.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 replay for existing manifest, got %d", respReplaySecond.StatusCode)
	}

	// 9. Altered manifest reusing same bundle digest -> 409 IMMUTABLE_CONTENT_CONFLICT
	altered := deploymentManifest(t, bundle)
	var altV map[string]any
	_ = json.Unmarshal(altered, &altV)
	altV["sdkVersion"] = "altered-http"
	altered, _ = json.Marshal(altV)

	altReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/deployments?environment=%s", server.URL, stagingEnv.ID), bytes.NewReader(altered))
	altReq.Header.Set("Authorization", "Bearer "+regKey.PlaintextKey)
	altReq.Header.Set("X-Organization-ID", org.ID)
	altReq.Header.Set("Idempotency-Key", "key-alt-bundle")
	altReq.Header.Set("Content-Type", "application/json")
	respAlt, err := http.DefaultClient.Do(altReq)
	if err != nil {
		t.Fatal(err)
	}
	var altErrBody map[string]any
	_ = json.NewDecoder(respAlt.Body).Decode(&altErrBody)
	respAlt.Body.Close()
	if respAlt.StatusCode != http.StatusConflict || altErrBody["code"] != "IMMUTABLE_CONTENT_CONFLICT" {
		t.Fatalf("expected 409 IMMUTABLE_CONTENT_CONFLICT, got %d (%v)", respAlt.StatusCode, altErrBody)
	}

	// --- Workflow Activation Tests ---

	// 10. Activation missing expectedRevision -> 400
	actMissingRevReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/workflows/wf/activate?environment=%s", server.URL, stagingEnv.ID), bytes.NewReader([]byte(fmt.Sprintf(`{"deploymentId":%q}`, regBody.ID))))
	actMissingRevReq.Header.Set("Authorization", "Bearer "+actStagingOnlyKey.PlaintextKey)
	actMissingRevReq.Header.Set("X-Organization-ID", org.ID)
	actMissingRevReq.Header.Set("Idempotency-Key", "act-missing-rev")
	actMissingRevReq.Header.Set("Content-Type", "application/json")
	respMissingRev, err := http.DefaultClient.Do(actMissingRevReq)
	if err != nil {
		t.Fatal(err)
	}
	respMissingRev.Body.Close()
	if respMissingRev.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing expectedRevision, got %d", respMissingRev.StatusCode)
	}

	// 11. Staging activation with 0 workers -> 409 WORKER_PREFLIGHT_FAILED
	actPreflightReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/workflows/wf/activate?environment=%s", server.URL, stagingEnv.ID), bytes.NewReader([]byte(fmt.Sprintf(`{"deploymentId":%q,"expectedRevision":0}`, regBody.ID))))
	actPreflightReq.Header.Set("Authorization", "Bearer "+actStagingOnlyKey.PlaintextKey)
	actPreflightReq.Header.Set("X-Organization-ID", org.ID)
	actPreflightReq.Header.Set("Idempotency-Key", "act-preflight-0")
	actPreflightReq.Header.Set("Content-Type", "application/json")
	respActPreflight, err := http.DefaultClient.Do(actPreflightReq)
	if err != nil {
		t.Fatal(err)
	}
	respActPreflight.Body.Close()
	if respActPreflight.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 WORKER_PREFLIGHT_FAILED for 0 workers, got %d", respActPreflight.StatusCode)
	}

	// Seed 1 compatible worker in staging
	wStagingID, _ := tenant.NewUUID()
	sStagingID, _ := tenant.NewUUID()
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO workers (id,organization_id,environment_id,public_key,status) VALUES ($1,$2,$3,'pk','ACTIVE')`, wStagingID, org.ID, stagingEnv.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO worker_sessions (id,organization_id,worker_id,environment_id,session_token_hash,expires_at) VALUES ($1,$2,$3,$4,'h',clock_timestamp()+interval '1 hour')`, sStagingID, org.ID, wStagingID, stagingEnv.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1,$2,$3)`, sStagingID, org.ID, bundle); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// 12. Staging activation without allowSingleWorker fails preflight
	actNoSingleReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/workflows/wf/activate?environment=%s", server.URL, stagingEnv.ID), bytes.NewReader([]byte(fmt.Sprintf(`{"deploymentId":%q,"expectedRevision":0,"allowSingleWorker":false}`, regBody.ID))))
	actNoSingleReq.Header.Set("Authorization", "Bearer "+actStagingOnlyKey.PlaintextKey)
	actNoSingleReq.Header.Set("X-Organization-ID", org.ID)
	actNoSingleReq.Header.Set("Idempotency-Key", "act-no-single")
	actNoSingleReq.Header.Set("Content-Type", "application/json")
	respNoSingle, err := http.DefaultClient.Do(actNoSingleReq)
	if err != nil {
		t.Fatal(err)
	}
	respNoSingle.Body.Close()
	if respNoSingle.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 when allowSingleWorker is false with 1 worker, got %d", respNoSingle.StatusCode)
	}

	// 13. Staging activation with allowSingleWorker: true succeeds with warning
	actSingleReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/workflows/wf/activate?environment=%s", server.URL, stagingEnv.ID), bytes.NewReader([]byte(fmt.Sprintf(`{"deploymentId":%q,"expectedRevision":0,"allowSingleWorker":true}`, regBody.ID))))
	actSingleReq.Header.Set("Authorization", "Bearer "+actStagingOnlyKey.PlaintextKey)
	actSingleReq.Header.Set("X-Organization-ID", org.ID)
	actSingleReq.Header.Set("Idempotency-Key", "act-single-ok")
	actSingleReq.Header.Set("Content-Type", "application/json")
	respSingle, err := http.DefaultClient.Do(actSingleReq)
	if err != nil {
		t.Fatal(err)
	}
	var actSingleBody deployment.Workflow
	_ = json.NewDecoder(respSingle.Body).Decode(&actSingleBody)
	respSingle.Body.Close()
	if respSingle.StatusCode != http.StatusOK || actSingleBody.Warning != "single compatible worker; no failover" || actSingleBody.Revision != 1 {
		t.Fatalf("expected 200 with single worker warning, got %d (%+v)", respSingle.StatusCode, actSingleBody)
	}

	// --- Production Environment Activation Tests ---

	// Register deployment in production
	regProdKey := bootstrapTestKey(t, tc.service, org.ID, prodEnv.ID, []string{tenant.CapDeploymentsRegister})
	regProdReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/deployments?environment=%s", server.URL, prodEnv.ID), bytes.NewReader(manifest))
	regProdReq.Header.Set("Authorization", "Bearer "+regProdKey.PlaintextKey)
	regProdReq.Header.Set("X-Organization-ID", org.ID)
	regProdReq.Header.Set("Idempotency-Key", "key-prod-create")
	regProdReq.Header.Set("Content-Type", "application/json")
	respRegProd, err := http.DefaultClient.Do(regProdReq)
	if err != nil {
		t.Fatal(err)
	}
	var prodDep deployment.Deployment
	_ = json.NewDecoder(respRegProd.Body).Decode(&prodDep)
	respRegProd.Body.Close()
	if respRegProd.StatusCode != http.StatusCreated {
		t.Fatalf("failed to register deployment in prod: %d", respRegProd.StatusCode)
	}

	// Seed 1 worker in production
	wProd1, _ := tenant.NewUUID()
	sProd1, _ := tenant.NewUUID()
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO workers (id,organization_id,environment_id,public_key,status) VALUES ($1,$2,$3,'pk','ACTIVE')`, wProd1, org.ID, prodEnv.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO worker_sessions (id,organization_id,worker_id,environment_id,session_token_hash,expires_at) VALUES ($1,$2,$3,$4,'h',clock_timestamp()+interval '1 hour')`, sProd1, org.ID, wProd1, prodEnv.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1,$2,$3)`, sProd1, org.ID, bundle); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// 14. Production activation override rejection: allowSingleWorker=true in production is rejected
	prodOverrideReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/workflows/wf/activate?environment=%s", server.URL, prodEnv.ID), bytes.NewReader([]byte(fmt.Sprintf(`{"deploymentId":%q,"expectedRevision":0,"allowSingleWorker":true}`, prodDep.ID))))
	prodOverrideReq.Header.Set("Authorization", "Bearer "+actProdOnlyKey.PlaintextKey)
	prodOverrideReq.Header.Set("X-Organization-ID", org.ID)
	prodOverrideReq.Header.Set("Idempotency-Key", "act-prod-override")
	prodOverrideReq.Header.Set("Content-Type", "application/json")
	respProdOverride, err := http.DefaultClient.Do(prodOverrideReq)
	if err != nil {
		t.Fatal(err)
	}
	respProdOverride.Body.Close()
	if respProdOverride.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for production single worker override rejection, got %d", respProdOverride.StatusCode)
	}

	// Seed second distinct worker in production
	wProd2, _ := tenant.NewUUID()
	sProd2, _ := tenant.NewUUID()
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO workers (id,organization_id,environment_id,public_key,status) VALUES ($1,$2,$3,'pk','ACTIVE')`, wProd2, org.ID, prodEnv.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO worker_sessions (id,organization_id,worker_id,environment_id,session_token_hash,expires_at) VALUES ($1,$2,$3,$4,'h',clock_timestamp()+interval '1 hour')`, sProd2, org.ID, wProd2, prodEnv.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id,organization_id,bundle_digest) VALUES ($1,$2,$3)`, sProd2, org.ID, bundle); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// 15. Key with ONLY deployments:activate:staging rejected in production
	stagingKeyProdReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/workflows/wf/activate?environment=%s", server.URL, prodEnv.ID), bytes.NewReader([]byte(fmt.Sprintf(`{"deploymentId":%q,"expectedRevision":0}`, prodDep.ID))))
	stagingKeyProdReq.Header.Set("Authorization", "Bearer "+actStagingOnProdKey.PlaintextKey)
	stagingKeyProdReq.Header.Set("X-Organization-ID", org.ID)
	stagingKeyProdReq.Header.Set("Idempotency-Key", "act-staging-on-prod")
	stagingKeyProdReq.Header.Set("Content-Type", "application/json")
	respStagingKeyProd, err := http.DefaultClient.Do(stagingKeyProdReq)
	if err != nil {
		t.Fatal(err)
	}
	respStagingKeyProd.Body.Close()
	if respStagingKeyProd.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 FORBIDDEN for staging key activating in prod, got %d", respStagingKeyProd.StatusCode)
	}

	// 16. Key with ONLY deployments:activate:production succeeds in production
	// (Proves prod-only capability path is not gated by staging capability)
	prodOnlyReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/workflows/wf/activate?environment=%s", server.URL, prodEnv.ID), bytes.NewReader([]byte(fmt.Sprintf(`{"deploymentId":%q,"expectedRevision":0}`, prodDep.ID))))
	prodOnlyReq.Header.Set("Authorization", "Bearer "+actProdOnlyKey.PlaintextKey)
	prodOnlyReq.Header.Set("X-Organization-ID", org.ID)
	prodOnlyReq.Header.Set("Idempotency-Key", "act-prod-only")
	prodOnlyReq.Header.Set("Content-Type", "application/json")
	respProdOnly, err := http.DefaultClient.Do(prodOnlyReq)
	if err != nil {
		t.Fatal(err)
	}
	var prodWfBody deployment.Workflow
	_ = json.NewDecoder(respProdOnly.Body).Decode(&prodWfBody)
	respProdOnly.Body.Close()
	if respProdOnly.StatusCode != http.StatusOK || prodWfBody.Revision != 1 {
		t.Fatalf("expected 200 for prod-only capability activation, got %d (%+v)", respProdOnly.StatusCode, prodWfBody)
	}

	// 17. Revision conflict on subsequent activation with stale expectedRevision
	revConflictReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/workflows/wf/activate?environment=%s", server.URL, prodEnv.ID), bytes.NewReader([]byte(fmt.Sprintf(`{"deploymentId":%q,"expectedRevision":0}`, prodDep.ID))))
	revConflictReq.Header.Set("Authorization", "Bearer "+actProdOnlyKey.PlaintextKey)
	revConflictReq.Header.Set("X-Organization-ID", org.ID)
	revConflictReq.Header.Set("Idempotency-Key", "act-rev-conflict")
	revConflictReq.Header.Set("Content-Type", "application/json")
	respRevConflict, err := http.DefaultClient.Do(revConflictReq)
	if err != nil {
		t.Fatal(err)
	}
	respRevConflict.Body.Close()
	if respRevConflict.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 REVISION_CONFLICT, got %d", respRevConflict.StatusCode)
	}

	// 18. Subsequent activation with correct revision (1) succeeds -> revision 2
	revNextReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/workflows/wf/activate?environment=%s", server.URL, prodEnv.ID), bytes.NewReader([]byte(fmt.Sprintf(`{"deploymentId":%q,"expectedRevision":1}`, prodDep.ID))))
	revNextReq.Header.Set("Authorization", "Bearer "+actProdOnlyKey.PlaintextKey)
	revNextReq.Header.Set("X-Organization-ID", org.ID)
	revNextReq.Header.Set("Idempotency-Key", "act-rev-next")
	revNextReq.Header.Set("Content-Type", "application/json")
	respRevNext, err := http.DefaultClient.Do(revNextReq)
	if err != nil {
		t.Fatal(err)
	}
	var revNextBody deployment.Workflow
	_ = json.NewDecoder(respRevNext.Body).Decode(&revNextBody)
	respRevNext.Body.Close()
	if respRevNext.StatusCode != http.StatusOK || revNextBody.Revision != 2 {
		t.Fatalf("expected 200 with revision 2, got %d (%+v)", respRevNext.StatusCode, revNextBody)
	}

	// 19. Referenced deployment deletion protection while runs remain active (Issue #10 invariant):
	// Seed an active run pinned to prodDep.
	runID, _ := tenant.NewUUID()
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO runs (id,organization_id,environment_id,deployment_id,workflow_name,status) VALUES ($1,$2,$3,$4,'wf','RUNNING')`, runID, org.ID, prodEnv.ID, prodDep.ID)
		return err
	})
	if err != nil {
		t.Fatalf("failed to seed active run: %v", err)
	}

	// Remove the workflow channel reference so prodDep is no longer referenced by workflow_channels.
	// This isolates the runs -> deployments ON DELETE RESTRICT constraint from any channel FK effect.
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `DELETE FROM workflow_channels WHERE environment_id = $1 AND workflow_name = $2`, prodEnv.ID, "wf")
		return err
	})
	if err != nil {
		t.Fatalf("failed to remove active channel reference: %v", err)
	}

	// Attempting to delete prodDep must now be rejected specifically by the runs foreign key constraint.
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, `DELETE FROM deployments WHERE id = $1`, prodDep.ID)
		return err
	})
	if err == nil {
		t.Fatal("expected deletion of deployment referenced by active run to fail with foreign key restriction")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" || !strings.Contains(pgErr.ConstraintName, "runs") {
		t.Fatalf("expected foreign key violation on runs table (23503, constraint containing 'runs'), got error: %v", err)
	}

	// Once the referencing run is removed, deletion of the unreferenced deployment succeeds.
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM runs WHERE id = $1`, runID); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `DELETE FROM deployments WHERE id = $1`, prodDep.ID)
		return err
	})
	if err != nil {
		t.Fatalf("expected deployment deletion to succeed once run reference is removed: %v", err)
	}
}
