package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/storage/migrator"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/jackc/pgx/v5/pgxpool"
)

type tenantTestContext struct {
	pool         *storage.Pool
	service      *tenant.Service
	handler      *tenant.HTTPHandler
	sessionStore *auth.SessionStore
	authCfg      auth.Config
	runtimePool  *pgxpool.Pool
	cleanup      func()
}

func setupTenantContext(t *testing.T) *tenantTestContext {
	t.Helper()
	db, runtimePool, _ := setupTestDB(t)

	pool := storage.NewPool(runtimePool)
	service := tenant.NewService(pool)

	authCfg := auth.Config{
		RuntimeMode:            auth.ModeHosted,
		CookieSecure:           true,
		SessionIdleTimeout:     12 * time.Hour,
		SessionAbsoluteTimeout: 7 * 24 * time.Hour,
		AllowedOrigins:         []string{"http://localhost:3000", "http://localhost:8080"},
	}
	sessionStore := auth.NewSessionStore(runtimePool)
	handler := tenant.NewHTTPHandler(service, runtimePool, sessionStore, authCfg)

	cleanup := func() {
		db.Close()
		runtimePool.Close()
	}
	return &tenantTestContext{
		pool:         pool,
		service:      service,
		handler:      handler,
		sessionStore: sessionStore,
		authCfg:      authCfg,
		runtimePool:  runtimePool,
		cleanup:      cleanup,
	}
}

// setupTenantSuite initializes DB and builds tenant Service and HTTPHandler for integration testing.
func setupTenantSuite(t *testing.T) (*tenant.Service, *tenant.HTTPHandler, func()) {
	tc := setupTenantContext(t)
	return tc.service, tc.handler, tc.cleanup
}

func bootstrapTestKey(t *testing.T, service *tenant.Service, orgID, envID string, caps []string) *tenant.GeneratedKey {
	t.Helper()
	key, err := service.BootstrapAPIKey(context.Background(), orgID, envID, caps, 90, &tenant.AuditContext{
		Reason: "test_fixture_setup",
	})
	if err != nil {
		t.Fatalf("bootstrapTestKey failed: %v", err)
	}
	return key
}

// 1. TestOrganizationBootstrapAndOwnerCreation
func TestOrganizationBootstrapAndOwnerCreation(t *testing.T) {
	service, _, cleanup := setupTenantSuite(t)
	defer cleanup()

	ctx := context.Background()
	userAID, err := tenant.NewUUID()
	if err != nil {
		t.Fatalf("failed to generate user id: %v", err)
	}

	// Bootstrap Organization
	org, err := service.CreateOrganization(ctx, userAID, "Acme Robotics")
	if err != nil {
		t.Fatalf("CreateOrganization failed: %v", err)
	}
	if org.ID == "" || org.Name != "Acme Robotics" {
		t.Fatalf("unexpected organization data: %+v", org)
	}

	// Verify creator is automatically bound as Owner
	members, err := service.ListMembers(ctx, org.ID)
	if err != nil {
		t.Fatalf("ListMembers failed: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("expected exactly 1 member, got %d", len(members))
	}
	if members[0].UserID != userAID || members[0].Role != tenant.RoleOwner || members[0].Status != tenant.StatusActive {
		t.Fatalf("unexpected member binding: %+v", members[0])
	}
}

// 2. TestProjectAndEnvironmentProvisioning
func TestProjectAndEnvironmentProvisioning(t *testing.T) {
	service, _, cleanup := setupTenantSuite(t)
	defer cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, err := service.CreateOrganization(ctx, ownerID, "Platform Engineering")
	if err != nil {
		t.Fatalf("CreateOrganization failed: %v", err)
	}

	// Create project
	proj, err := service.CreateProject(ctx, org.ID, "Payment Pipeline")
	if err != nil {
		t.Fatalf("CreateProject failed: %v", err)
	}
	if proj.ID == "" || proj.Name != "Payment Pipeline" {
		t.Fatalf("unexpected project: %+v", proj)
	}

	// Create environments: development, staging, production
	devEnv, err := service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvDevelopment, 5)
	if err != nil {
		t.Fatalf("CreateEnvironment dev failed: %v", err)
	}
	if devEnv.MaxConcurrency != 5 {
		t.Fatalf("expected dev concurrency 5, got %d", devEnv.MaxConcurrency)
	}

	stagingEnv, err := service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvStaging, 15)
	if err != nil {
		t.Fatalf("CreateEnvironment staging failed: %v", err)
	}
	if stagingEnv.MaxConcurrency != 15 {
		t.Fatalf("expected staging concurrency 15, got %d", stagingEnv.MaxConcurrency)
	}

	prodEnv, err := service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 50)
	if err != nil {
		t.Fatalf("CreateEnvironment prod failed: %v", err)
	}
	if prodEnv.MaxConcurrency != 50 {
		t.Fatalf("expected prod concurrency 50, got %d", prodEnv.MaxConcurrency)
	}

	// List environments and verify count
	envs, err := service.ListEnvironments(ctx, org.ID, proj.ID)
	if err != nil {
		t.Fatalf("ListEnvironments failed: %v", err)
	}
	if len(envs) != 3 {
		t.Fatalf("expected 3 environments, got %d", len(envs))
	}

	// Reject invalid environment name
	_, err = service.CreateEnvironment(ctx, org.ID, proj.ID, "qa_environment", 10)
	if err == nil || !strings.Contains(err.Error(), "INVALID_ENVIRONMENT") {
		t.Fatalf("expected INVALID_ENVIRONMENT error for qa_environment, got: %v", err)
	}
}

// 3. TestAPIKeyGenerationAndEntropy
func TestAPIKeyGenerationAndEntropy(t *testing.T) {
	service, _, cleanup := setupTenantSuite(t)
	defer cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, err := service.CreateOrganization(ctx, ownerID, "Key Security Corp")
	if err != nil {
		t.Fatalf("CreateOrganization failed: %v", err)
	}

	proj, err := service.CreateProject(ctx, org.ID, "Inference Gateway")
	if err != nil {
		t.Fatalf("CreateProject failed: %v", err)
	}

	prodEnv, err := service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 20)
	if err != nil {
		t.Fatalf("CreateEnvironment failed: %v", err)
	}

	// Generate key with 256-bit entropy and valid machine capabilities
	caps := []string{tenant.CapRunsCreate, tenant.CapRunsRead, tenant.CapPayloadRead}
	creatorCaps := tenant.RoleCapabilities(tenant.RoleOwner)
	audit := &tenant.AuditContext{
		ActorID: &ownerID,
		Reason:  "test_keygen",
	}
	genKey, err := service.CreateAPIKey(ctx, org.ID, prodEnv.ID, caps, 90, creatorCaps, audit)
	if err != nil {
		t.Fatalf("CreateAPIKey failed: %v", err)
	}

	// 1. Prefix format check: db_production_<8hex>
	if !strings.HasPrefix(genKey.Prefix, "db_production_") {
		t.Fatalf("expected prefix starting with db_production_, got %s", genKey.Prefix)
	}
	prefixParts := strings.Split(genKey.Prefix, "_")
	if len(prefixParts) != 3 || len(prefixParts[2]) != 8 {
		t.Fatalf("invalid prefix structure: %s", genKey.Prefix)
	}

	// 2. Plaintext key format check: <prefix>_<secret_b64>
	if !strings.HasPrefix(genKey.PlaintextKey, genKey.Prefix+"_") {
		t.Fatalf("plaintext key does not start with prefix: %s", genKey.PlaintextKey)
	}
	secretB64 := strings.TrimPrefix(genKey.PlaintextKey, genKey.Prefix+"_")
	secretRaw, err := base64.RawURLEncoding.DecodeString(secretB64)
	if err != nil {
		t.Fatalf("failed to base64url decode secret part: %v", err)
	}
	// 3. Cryptographic entropy >= 256 bits (32 bytes)
	if len(secretRaw) != 32 {
		t.Fatalf("expected 32 bytes (256 bits) of entropy, got %d bytes", len(secretRaw))
	}

	// 4. Verify DB storage: secret is strictly hashed (SHA-256 hex string), not plaintext
	var storedHash string
	_, err = service.GetOrganization(ctx, org.ID) // verify org exists
	if err != nil {
		t.Fatalf("GetOrganization failed: %v", err)
	}

	keysList, err := service.ListAPIKeys(ctx, org.ID, prodEnv.ID)
	if err != nil {
		t.Fatalf("ListAPIKeys failed: %v", err)
	}
	if len(keysList) != 1 {
		t.Fatalf("expected 1 api key in summary, got %d", len(keysList))
	}
	// Summary should contain prefix, not secret or hash
	if keysList[0].Prefix != genKey.Prefix {
		t.Fatalf("summary prefix mismatch: %s vs %s", keysList[0].Prefix, genKey.Prefix)
	}

	// Direct lookup of hash using authenticate function
	authKey, err := service.AuthenticateAPIKey(ctx, genKey.PlaintextKey)
	if err != nil {
		t.Fatalf("AuthenticateAPIKey failed: %v", err)
	}
	storedHash = authKey.HashedSecret
	if len(storedHash) != 64 {
		t.Fatalf("expected 64-char SHA-256 hex string in DB, got length %d: %s", len(storedHash), storedHash)
	}
	if strings.Contains(storedHash, secretB64) {
		t.Fatalf("SECURITY VIOLATION: plaintext secret found inside stored hash!")
	}
	if storedHash == genKey.PlaintextKey {
		t.Fatalf("SECURITY VIOLATION: plaintext key stored directly in database!")
	}
	decodedHex, err := hex.DecodeString(storedHash)
	if err != nil || len(decodedHex) != 32 {
		t.Fatalf("invalid hex-encoded SHA-256 stored hash: %v", err)
	}
}

// 4. TestAPIKeyAuthenticationAndScoping
func TestAPIKeyAuthenticationAndScoping(t *testing.T) {
	service, _, cleanup := setupTenantSuite(t)
	defer cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := service.CreateOrganization(ctx, ownerID, "Scoping Corp")
	proj, _ := service.CreateProject(ctx, org.ID, "Core Project")
	stagingEnv, _ := service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvStaging, 10)

	caps := []string{tenant.CapRunsCreate, tenant.CapRunsRead}
	genKey := bootstrapTestKey(t, service, org.ID, stagingEnv.ID, caps)

	// Successful authentication
	authKey, err := service.AuthenticateAPIKey(ctx, genKey.PlaintextKey)
	if err != nil {
		t.Fatalf("AuthenticateAPIKey failed: %v", err)
	}
	if authKey.OrganizationID != org.ID {
		t.Fatalf("organization ID mismatch: %s vs %s", authKey.OrganizationID, org.ID)
	}
	if authKey.EnvironmentID != stagingEnv.ID {
		t.Fatalf("environment ID mismatch: %s vs %s", authKey.EnvironmentID, stagingEnv.ID)
	}
	if authKey.LastUsedAt == nil {
		t.Fatalf("expected last_used_at to be updated upon authentication")
	}

	// Tampered secret rejection
	tamperedKey := genKey.Prefix + "_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	_, err = service.AuthenticateAPIKey(ctx, tamperedKey)
	if err == nil || !errors.Is(err, tenant.ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized for tampered key, got: %v", err)
	}

	// Unknown prefix rejection
	unknownKey := "db_staging_ffffffff_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	_, err = service.AuthenticateAPIKey(ctx, unknownKey)
	if err == nil || !errors.Is(err, tenant.ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized for unknown prefix, got: %v", err)
	}
}

// 5. TestExpiredAndRevokedAPIKeys
func TestExpiredAndRevokedAPIKeys(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Lifecycle Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Lifecycle Project")
	env, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvDevelopment, 10)

	// 1. Test Revoked Key
	keyToRevoke := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapRunsRead})
	audit := &tenant.AuditContext{ActorID: &ownerID, Reason: "test_revoke"}
	if err := tc.service.RevokeAPIKey(ctx, org.ID, keyToRevoke.ID, "", audit); err != nil {
		t.Fatalf("RevokeAPIKey failed: %v", err)
	}
	_, err := tc.service.AuthenticateAPIKey(ctx, keyToRevoke.PlaintextKey)
	if err == nil || !errors.Is(err, tenant.ErrKeyRevoked) {
		t.Fatalf("expected ErrKeyRevoked, got: %v", err)
	}

	// 2. Test Expired Key persisted in DB
	expiredKey := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapRunsRead})

	// Persist expired expires_at timestamp directly into the database within tenant RLS context
	pastTime := time.Now().UTC().Add(-2 * time.Hour)
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE api_keys SET expires_at = $1 WHERE id = $2", pastTime, expiredKey.ID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("no rows affected when updating expires_at")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to update expires_at in DB: %v", err)
	}

	_, err = tc.service.AuthenticateAPIKey(ctx, expiredKey.PlaintextKey)
	if err == nil || !errors.Is(err, tenant.ErrKeyExpired) {
		t.Fatalf("expected ErrKeyExpired from AuthenticateAPIKey on expired DB row, got: %v", err)
	}
}

// 6. TestCrossTenantDenial
func TestCrossTenantDenial(t *testing.T) {
	service, handler, cleanup := setupTenantSuite(t)
	defer cleanup()

	ctx := context.Background()
	userA, _ := tenant.NewUUID()
	userB, _ := tenant.NewUUID()

	// Org A
	orgA, err := service.CreateOrganization(ctx, userA, "Organization Alpha")
	if err != nil {
		t.Fatalf("CreateOrganization A failed: %v", err)
	}
	projA, err := service.CreateProject(ctx, orgA.ID, "Alpha Project")
	if err != nil {
		t.Fatalf("CreateProject A failed: %v", err)
	}
	envA, err := service.CreateEnvironment(ctx, orgA.ID, projA.ID, tenant.EnvProduction, 10)
	if err != nil {
		t.Fatalf("CreateEnvironment A failed: %v", err)
	}
	keyA := bootstrapTestKey(t, service, orgA.ID, envA.ID, []string{tenant.CapRunsRead, tenant.CapAdminKey})

	// Org B
	orgB, err := service.CreateOrganization(ctx, userB, "Organization Beta")
	if err != nil {
		t.Fatalf("CreateOrganization B failed: %v", err)
	}
	projB, err := service.CreateProject(ctx, orgB.ID, "Beta Project")
	if err != nil {
		t.Fatalf("CreateProject B failed: %v", err)
	}

	// Service-level cross-tenant query: Querying Org B's project with Org A's tenant context
	_, err = service.GetProject(ctx, orgA.ID, projB.ID)
	if err == nil || !errors.Is(err, tenant.ErrNotFound) {
		t.Fatalf("RLS ISOLATION VIOLATION: Org A was able to read Org B project! (err: %v)", err)
	}

	// HTTP-level cross-tenant query: Key A attempting to query Org B via X-Organization-ID
	server := httptest.NewServer(handler.Routes())
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL+"/api/v1/projects", nil)
	req.Header.Set("Authorization", "Bearer "+keyA.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgB.ID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("SECURITY VIOLATION: expected 403 Forbidden for cross-tenant API key, got %d", resp.StatusCode)
	}
}

// 7. TestRBACPermissionMatrix
func TestRBACPermissionMatrix(t *testing.T) {
	// Verify Role Capabilities according to Blueprint §24.2
	// 1. Viewer
	if !tenant.CanRolePerform(tenant.RoleViewer, tenant.CapRunRead) {
		t.Fatalf("Viewer should have CapRunRead")
	}
	if tenant.CanRolePerform(tenant.RoleViewer, tenant.CapPayloadRead) {
		t.Fatalf("Viewer must NOT have CapPayloadRead by default")
	}
	if tenant.CanRolePerform(tenant.RoleViewer, tenant.CapRunCreate) {
		t.Fatalf("Viewer must NOT have CapRunCreate")
	}
	if tenant.CanRolePerform(tenant.RoleViewer, tenant.CapDeployRegister) {
		t.Fatalf("Viewer must NOT have CapDeployRegister")
	}
	if tenant.CanRolePerform(tenant.RoleViewer, tenant.CapAdminMember) {
		t.Fatalf("Viewer must NOT have CapAdminMember")
	}

	// 2. Developer
	if !tenant.CanRolePerform(tenant.RoleDeveloper, tenant.CapPayloadRead) {
		t.Fatalf("Developer should have CapPayloadRead")
	}
	if !tenant.CanRolePerform(tenant.RoleDeveloper, tenant.CapRunCreate) {
		t.Fatalf("Developer should have CapRunCreate")
	}
	if !tenant.CanRolePerform(tenant.RoleDeveloper, tenant.CapRunControl) {
		t.Fatalf("Developer should have CapRunControl")
	}
	if !tenant.CanRolePerform(tenant.RoleDeveloper, tenant.CapDeployActivateStaging) {
		t.Fatalf("Developer should have CapDeployActivateStaging")
	}
	if tenant.CanRolePerform(tenant.RoleDeveloper, tenant.CapDeployActivateProd) {
		t.Fatalf("Developer must NOT have CapDeployActivateProd")
	}
	if tenant.CanRolePerform(tenant.RoleDeveloper, tenant.CapApprovalDecide) {
		t.Fatalf("Developer must NOT have CapApprovalDecide")
	}
	if tenant.CanRolePerform(tenant.RoleDeveloper, tenant.CapReconcileResolve) {
		t.Fatalf("Developer must NOT have CapReconcileResolve")
	}

	// 3. Operator
	if !tenant.CanRolePerform(tenant.RoleOperator, tenant.CapDeployActivateProd) {
		t.Fatalf("Operator should have CapDeployActivateProd")
	}
	if !tenant.CanRolePerform(tenant.RoleOperator, tenant.CapApprovalDecide) {
		t.Fatalf("Operator should have CapApprovalDecide")
	}
	if !tenant.CanRolePerform(tenant.RoleOperator, tenant.CapReconcileResolve) {
		t.Fatalf("Operator should have CapReconcileResolve")
	}
	if !tenant.CanRolePerform(tenant.RoleOperator, tenant.CapWorkerDrain) {
		t.Fatalf("Operator should have CapWorkerDrain")
	}
	if tenant.CanRolePerform(tenant.RoleOperator, tenant.CapAdminMember) {
		t.Fatalf("Operator must NOT have CapAdminMember")
	}

	// 4. Admin
	if !tenant.CanRolePerform(tenant.RoleAdmin, tenant.CapAdminMember) {
		t.Fatalf("Admin should have CapAdminMember")
	}
	if !tenant.CanRolePerform(tenant.RoleAdmin, tenant.CapAdminKey) {
		t.Fatalf("Admin should have CapAdminKey")
	}
	if !tenant.CanRolePerform(tenant.RoleAdmin, tenant.CapAdminProject) {
		t.Fatalf("Admin should have CapAdminProject")
	}
	if tenant.CanRolePerform(tenant.RoleAdmin, tenant.CapOrgDelete) {
		t.Fatalf("Admin must NOT have CapOrgDelete")
	}

	// 5. Owner
	if !tenant.CanRolePerform(tenant.RoleOwner, tenant.CapOrgDelete) {
		t.Fatalf("Owner should have CapOrgDelete")
	}

	// HTTP payload preview protection check: Viewer without payload:read capability
	tc := setupTenantContext(t)
	defer tc.cleanup()

	server := httptest.NewServer(tc.handler.Routes())
	defer server.Close()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Payload Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "TestProj")
	env, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)

	// 1. Machine key with only runs:read attempting to read payload preview -> 403
	viewerKey := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapRunsRead})

	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/environments/%s/payload-preview", server.URL, env.ID), nil)
	req.Header.Set("Authorization", "Bearer "+viewerKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", org.ID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("MACHINE VIEWER SECURITY VIOLATION: expected 403 Forbidden for Viewer reading payload, got %d", resp.StatusCode)
	}

	// 2. Human session with RoleViewer attempting to read payload preview -> 403
	viewerUserID, _ := tenant.NewUUID()
	if _, err := tc.runtimePool.Exec(ctx, "INSERT INTO users (id, email, name) VALUES ($1, $2, $3)", viewerUserID, "viewer@corp.test", "Corp Viewer"); err != nil {
		t.Fatalf("failed to insert viewer user: %v", err)
	}
	if _, err := tc.service.AddMember(ctx, org.ID, viewerUserID, tenant.RoleViewer); err != nil {
		t.Fatalf("AddMember RoleViewer failed: %v", err)
	}

	sess, sessionToken, _, err := tc.sessionStore.CreateSession(
		ctx,
		viewerUserID,
		&org.ID,
		"127.0.0.1",
		"TestAgent",
		tc.authCfg.SessionIdleTimeout,
		tc.authCfg.SessionAbsoluteTimeout,
	)
	if err != nil || sess == nil {
		t.Fatalf("CreateSession for viewer failed: %v", err)
	}

	cookie := &http.Cookie{
		Name:  tc.authCfg.SessionCookieName(),
		Value: sessionToken,
	}

	humanReq, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/environments/%s/payload-preview", server.URL, env.ID), nil)
	humanReq.AddCookie(cookie)
	humanReq.Header.Set("Origin", "http://localhost:3000")

	humanResp, err := http.DefaultClient.Do(humanReq)
	if err != nil {
		t.Fatalf("human session request failed: %v", err)
	}
	defer humanResp.Body.Close()

	if humanResp.StatusCode != http.StatusForbidden {
		t.Fatalf("HUMAN VIEWER SECURITY VIOLATION: expected 403 Forbidden for RoleViewer session reading payload, got %d", humanResp.StatusCode)
	}
}

// 8. TestLastOwnerDefense
func TestLastOwnerDefense(t *testing.T) {
	service, _, cleanup := setupTenantSuite(t)
	defer cleanup()

	ctx := context.Background()
	ownerA, _ := tenant.NewUUID()
	ownerB, _ := tenant.NewUUID()

	org, err := service.CreateOrganization(ctx, ownerA, "Sole Owner Corp")
	if err != nil {
		t.Fatalf("CreateOrganization failed: %v", err)
	}

	// 1. Attempt to demote sole active Owner
	err = service.UpdateMemberRole(ctx, org.ID, ownerA, tenant.RoleAdmin)
	if err == nil || !errors.Is(err, tenant.ErrLastOwnerDemotion) {
		t.Fatalf("expected ErrLastOwnerDemotion when demoting sole Owner, got: %v", err)
	}

	// 2. Attempt to remove sole active Owner
	err = service.RemoveMember(ctx, org.ID, ownerA)
	if err == nil || !errors.Is(err, tenant.ErrLastOwnerRemoval) {
		t.Fatalf("expected ErrLastOwnerRemoval when removing sole Owner, got: %v", err)
	}

	// 3. Add second Owner
	_, err = service.AddMember(ctx, org.ID, ownerB, tenant.RoleOwner)
	if err != nil {
		t.Fatalf("AddMember Owner B failed: %v", err)
	}

	// 4. Now demoting Owner A succeeds because Owner B remains
	err = service.UpdateMemberRole(ctx, org.ID, ownerA, tenant.RoleAdmin)
	if err != nil {
		t.Fatalf("expected demoting Owner A to succeed with second Owner present, got: %v", err)
	}

	// 5. Removing Owner B now fails because Owner B is the last active Owner
	err = service.RemoveMember(ctx, org.ID, ownerB)
	if err == nil || !errors.Is(err, tenant.ErrLastOwnerRemoval) {
		t.Fatalf("expected ErrLastOwnerRemoval when removing Owner B, got: %v", err)
	}
}

// 9. TestMachineKeyApprovalRestriction
func TestMachineKeyApprovalRestriction(t *testing.T) {
	service, _, cleanup := setupTenantSuite(t)
	defer cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := service.CreateOrganization(ctx, ownerID, "Human Decision Corp")
	proj, _ := service.CreateProject(ctx, org.ID, "Decision Proj")
	env, _ := service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)

	// Attempt to create machine key with approvals:decide
	creatorCaps := tenant.RoleCapabilities(tenant.RoleOwner)
	audit := &tenant.AuditContext{ActorID: &ownerID, Reason: "test_machine_cap"}
	_, err := service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapRunsCreate, tenant.CapApprovalsDecide}, 90, creatorCaps, audit)
	if err == nil || !errors.Is(err, tenant.ErrMachineKeyRestricted) {
		t.Fatalf("expected ErrMachineKeyRestricted for approvals:decide, got: %v", err)
	}

	// Attempt to create machine key with runs:reconcile
	_, err = service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapRunsCreate, tenant.CapRunsReconcile}, 90, creatorCaps, audit)
	if err == nil || !errors.Is(err, tenant.ErrMachineKeyRestricted) {
		t.Fatalf("expected ErrMachineKeyRestricted for runs:reconcile, got: %v", err)
	}
}

// 10. TestHTTPTenantEndpoints
func TestHTTPTenantEndpoints(t *testing.T) {
	service, handler, cleanup := setupTenantSuite(t)
	defer cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := service.CreateOrganization(ctx, ownerID, "HTTP Test Corp")
	proj, _ := service.CreateProject(ctx, org.ID, "HTTP Project")
	env, _ := service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvDevelopment, 10)

	apiKey := bootstrapTestKey(t, service, org.ID, env.ID, []string{tenant.CapAdminKey, tenant.CapRunsRead})

	server := httptest.NewServer(handler.Routes())
	defer server.Close()

	// 1. List API Keys via HTTP
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/environments/%s/api-keys", server.URL, env.ID), nil)
	req.Header.Set("Authorization", "Bearer "+apiKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", org.ID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}

	var listResp struct {
		APIKeys []tenant.APIKeySummary `json:"api_keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(listResp.APIKeys) != 1 {
		t.Fatalf("expected 1 api key, got %d", len(listResp.APIKeys))
	}

	// 2. Revoke API Key via HTTP
	reqRevoke, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/api/v1/api-keys/%s", server.URL, apiKey.ID), nil)
	reqRevoke.Header.Set("Authorization", "Bearer "+apiKey.PlaintextKey)
	reqRevoke.Header.Set("X-Organization-ID", org.ID)
	reqRevoke.Header.Set("Idempotency-Key", "idemp-http-revoke-1")

	respRevoke, err := http.DefaultClient.Do(reqRevoke)
	if err != nil {
		t.Fatalf("http request failed: %v", err)
	}
	defer respRevoke.Body.Close()

	if respRevoke.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 No Content for key revocation, got %d", respRevoke.StatusCode)
	}
}

// 11. TestErrorEnvelopeFormat (Blueprint §20.1 & §25.1)
func TestErrorEnvelopeFormat(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	server := httptest.NewServer(tc.handler.Routes())
	defer server.Close()

	// 1. Test 401 Unauthenticated ErrorEnvelope
	req, _ := http.NewRequest("GET", server.URL+"/api/v1/organizations", nil)
	req.Header.Set("X-Request-ID", "custom-trace-id-12345")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	var env tenant.ErrorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("failed to decode ErrorEnvelope: %v", err)
	}
	if env.Code != "UNAUTHENTICATED" {
		t.Fatalf("expected code UNAUTHENTICATED, got %q", env.Code)
	}
	if env.RequestID != "custom-trace-id-12345" {
		t.Fatalf("expected requestId custom-trace-id-12345, got %q", env.RequestID)
	}
	if env.Details == nil {
		t.Fatalf("expected details map, got nil")
	}
	if env.Retryable != false {
		t.Fatalf("expected retryable false for 401, got %v", env.Retryable)
	}

	// 2. Test response header contains X-Request-ID
	if resp.Header.Get("X-Request-ID") != "custom-trace-id-12345" {
		t.Fatalf("expected response header X-Request-ID custom-trace-id-12345, got %q", resp.Header.Get("X-Request-ID"))
	}
}

// 12. TestEnvironmentMismatchRejection (Blueprint §20.1 & §24.3)
func TestEnvironmentMismatchRejection(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Mismatch Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Mismatch Project")
	stagingEnv, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvStaging, 10)
	prodEnv, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)

	// API key bound strictly to staging
	key := bootstrapTestKey(t, tc.service, org.ID, stagingEnv.ID, []string{tenant.CapRunsRead, tenant.CapPayloadRead})

	server := httptest.NewServer(tc.handler.Routes())
	defer server.Close()

	// Scenario A: Mismatched URL path envId
	reqA, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/environments/%s/payload-preview", server.URL, prodEnv.ID), nil)
	reqA.Header.Set("Authorization", "Bearer "+key.PlaintextKey)
	reqA.Header.Set("X-Organization-ID", org.ID)
	respA, err := http.DefaultClient.Do(reqA)
	if err != nil {
		t.Fatalf("reqA failed: %v", err)
	}
	defer respA.Body.Close()
	if respA.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for mismatched path envId, got %d", respA.StatusCode)
	}
	var envA tenant.ErrorEnvelope
	_ = json.NewDecoder(respA.Body).Decode(&envA)
	if envA.Code != "ENVIRONMENT_MISMATCH" {
		t.Fatalf("expected ENVIRONMENT_MISMATCH, got %q", envA.Code)
	}

	// Scenario B: Mismatched query parameter ?environment=production
	reqB, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/environments/%s/payload-preview?environment=production", server.URL, stagingEnv.ID), nil)
	reqB.Header.Set("Authorization", "Bearer "+key.PlaintextKey)
	reqB.Header.Set("X-Organization-ID", org.ID)
	respB, err := http.DefaultClient.Do(reqB)
	if err != nil {
		t.Fatalf("reqB failed: %v", err)
	}
	defer respB.Body.Close()
	if respB.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for mismatched query param, got %d", respB.StatusCode)
	}

	// Scenario C: Mismatched header X-Environment: production
	reqC, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/environments/%s/payload-preview", server.URL, stagingEnv.ID), nil)
	reqC.Header.Set("Authorization", "Bearer "+key.PlaintextKey)
	reqC.Header.Set("X-Organization-ID", org.ID)
	reqC.Header.Set("X-Environment", "production")
	respC, err := http.DefaultClient.Do(reqC)
	if err != nil {
		t.Fatalf("reqC failed: %v", err)
	}
	defer respC.Body.Close()
	if respC.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for mismatched header, got %d", respC.StatusCode)
	}

	// Scenario D: Valid matching environment staging -> 200 OK
	reqD, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/environments/%s/payload-preview?environment=staging", server.URL, stagingEnv.ID), nil)
	reqD.Header.Set("Authorization", "Bearer "+key.PlaintextKey)
	reqD.Header.Set("X-Organization-ID", org.ID)
	reqD.Header.Set("X-Environment", "staging")
	respD, err := http.DefaultClient.Do(reqD)
	if err != nil {
		t.Fatalf("reqD failed: %v", err)
	}
	defer respD.Body.Close()
	if respD.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for valid matching environment, got %d", respD.StatusCode)
	}
}

// 13. TestCSRFAndOriginEnforcementOnMutations (Blueprint §24.1)
func TestCSRFAndOriginEnforcementOnMutations(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	if _, err := tc.runtimePool.Exec(ctx, "INSERT INTO users (id, email, name) VALUES ($1, $2, $3)", ownerID, "owner@bff.test", "BFF Owner"); err != nil {
		t.Fatalf("failed to insert test user: %v", err)
	}
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "BFF Security Corp")

	// Create valid human session
	sess, sessionToken, csrfToken, err := tc.sessionStore.CreateSession(
		ctx,
		ownerID,
		&org.ID,
		"127.0.0.1",
		"TestAgent",
		tc.authCfg.SessionIdleTimeout,
		tc.authCfg.SessionAbsoluteTimeout,
	)
	if err != nil {
		t.Fatalf("CreateSession failed: %v", err)
	}
	if sess == nil {
		t.Fatalf("expected non-nil session")
	}

	server := httptest.NewServer(tc.handler.Routes())
	defer server.Close()

	cookie := &http.Cookie{
		Name:  tc.authCfg.SessionCookieName(),
		Value: sessionToken,
	}

	// 1. Mutating POST without Origin -> rejected with 403 ORIGIN_FORBIDDEN
	req1, _ := http.NewRequest("POST", server.URL+"/api/v1/projects", strings.NewReader(`{"name":"Proj1"}`))
	req1.AddCookie(cookie)
	req1.Header.Set("Content-Type", "application/json")
	req1.Header.Set("X-CSRF-Token", csrfToken)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("req1 failed: %v", err)
	}
	defer resp1.Body.Close()
	if resp1.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for missing Origin, got %d", resp1.StatusCode)
	}

	// 2. Mutating POST with untrusted Origin -> rejected with 403 ORIGIN_FORBIDDEN
	req2, _ := http.NewRequest("POST", server.URL+"/api/v1/projects", strings.NewReader(`{"name":"Proj1"}`))
	req2.AddCookie(cookie)
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Origin", "https://attacker.evil.com")
	req2.Header.Set("X-CSRF-Token", csrfToken)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("req2 failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for untrusted Origin, got %d", resp2.StatusCode)
	}

	// 3. Mutating POST with valid Origin but missing X-CSRF-Token -> rejected with 403 CSRF_VALIDATION_FAILED
	req3, _ := http.NewRequest("POST", server.URL+"/api/v1/projects", strings.NewReader(`{"name":"Proj1"}`))
	req3.AddCookie(cookie)
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("Origin", "http://localhost:3000")
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("req3 failed: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for missing CSRF token, got %d", resp3.StatusCode)
	}

	// 4. Mutating POST with valid Origin and valid X-CSRF-Token -> 201 Created
	req4, _ := http.NewRequest("POST", server.URL+"/api/v1/projects", strings.NewReader(`{"name":"Valid Project"}`))
	req4.AddCookie(cookie)
	req4.Header.Set("Content-Type", "application/json")
	req4.Header.Set("Origin", "http://localhost:3000")
	req4.Header.Set("X-CSRF-Token", csrfToken)
	req4.Header.Set("Idempotency-Key", "idemp-csrf-valid-1")
	resp4, err := http.DefaultClient.Do(req4)
	if err != nil {
		t.Fatalf("req4 failed: %v", err)
	}
	defer resp4.Body.Close()
	if resp4.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created with valid CSRF & Origin, got %d", resp4.StatusCode)
	}

	// 5. Safe GET method does NOT require CSRF token -> 200 OK
	req5, _ := http.NewRequest("GET", server.URL+"/api/v1/projects", nil)
	req5.AddCookie(cookie)
	resp5, err := http.DefaultClient.Do(req5)
	if err != nil {
		t.Fatalf("req5 failed: %v", err)
	}
	defer resp5.Body.Close()
	if resp5.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for GET without CSRF token, got %d", resp5.StatusCode)
	}
}

// 14. TestAPIKeyRotation (Blueprint §24.4)
func TestAPIKeyRotation(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Rotation Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Rotation Project")
	env, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)

	caps := []string{tenant.CapRunsCreate, tenant.CapRunsRead}
	originalKey := bootstrapTestKey(t, tc.service, org.ID, env.ID, caps)

	// Create an admin key with CapAdminKey and the target key's capabilities to allow rotation
	adminKey := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapAdminKey, tenant.CapRunsCreate, tenant.CapRunsRead})

	// Verify original key authenticates
	auth1, err := tc.service.AuthenticateAPIKey(ctx, originalKey.PlaintextKey)
	if err != nil || auth1 == nil {
		t.Fatalf("original key authentication failed: %v", err)
	}

	// Rotate key via HTTP endpoint
	server := httptest.NewServer(tc.handler.Routes())
	defer server.Close()

	// 1. Unprivileged key without admin:key must be rejected with 403 Forbidden
	unauthReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/api-keys/%s/rotate", server.URL, originalKey.ID), strings.NewReader(`{"expiry_days": 60}`))
	unauthReq.Header.Set("Authorization", "Bearer "+originalKey.PlaintextKey)
	unauthReq.Header.Set("X-Organization-ID", org.ID)
	unauthReq.Header.Set("Content-Type", "application/json")
	unauthReq.Header.Set("Idempotency-Key", "idemp-rotate-unauth-1")
	unauthResp, err := http.DefaultClient.Do(unauthReq)
	if err != nil {
		t.Fatalf("unauth request failed: %v", err)
	}
	defer unauthResp.Body.Close()
	if unauthResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for unprivileged key rotation, got %d", unauthResp.StatusCode)
	}

	// 2. Admin key with admin:key and subsuming capabilities successfully rotates key
	rotateReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/api-keys/%s/rotate", server.URL, originalKey.ID), strings.NewReader(`{"expiry_days": 60}`))
	rotateReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	rotateReq.Header.Set("X-Organization-ID", org.ID)
	rotateReq.Header.Set("Content-Type", "application/json")
	rotateReq.Header.Set("Idempotency-Key", "idemp-rotate-admin-1")

	rotateResp, err := http.DefaultClient.Do(rotateReq)
	if err != nil {
		t.Fatalf("rotate request failed: %v", err)
	}
	defer rotateResp.Body.Close()

	if rotateResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for key rotation, got %d", rotateResp.StatusCode)
	}

	var rotatedKey tenant.GeneratedKey
	if err := json.NewDecoder(rotateResp.Body).Decode(&rotatedKey); err != nil {
		t.Fatalf("failed to decode rotated key: %v", err)
	}

	if rotatedKey.ID == originalKey.ID {
		t.Fatalf("rotated key must have new ID")
	}
	if rotatedKey.PlaintextKey == originalKey.PlaintextKey {
		t.Fatalf("rotated key must have new plaintext key")
	}
	if rotatedKey.EnvironmentID != env.ID {
		t.Fatalf("rotated key must retain environment binding")
	}
	if rotatedKey.EnvironmentName != tenant.EnvProduction {
		t.Fatalf("expected environment_name production, got %q", rotatedKey.EnvironmentName)
	}

	// Old key must now be revoked
	_, err = tc.service.AuthenticateAPIKey(ctx, originalKey.PlaintextKey)
	if err == nil || !errors.Is(err, tenant.ErrKeyRevoked) {
		t.Fatalf("expected old key to be revoked, got: %v", err)
	}

	// New key must authenticate successfully
	authNew, err := tc.service.AuthenticateAPIKey(ctx, rotatedKey.PlaintextKey)
	if err != nil || authNew == nil {
		t.Fatalf("new key authentication failed: %v", err)
	}
	if authNew.EnvironmentName != tenant.EnvProduction {
		t.Fatalf("expected authenticated key environment_name production, got %q", authNew.EnvironmentName)
	}

	// Rotating an already revoked key must fail
	_, err = tc.service.RotateAPIKey(ctx, org.ID, originalKey.ID, 30, []string{tenant.CapAdminKey, tenant.CapRunsCreate, tenant.CapRunsRead}, "", &tenant.AuditContext{Reason: "test_revoked_rotate"})
	if err == nil || !errors.Is(err, tenant.ErrKeyRevoked) {
		t.Fatalf("expected ErrKeyRevoked when rotating revoked key, got: %v", err)
	}
}

// 15. TestMemberStatusAndLastOwnerSuspension (Blueprint §24.2 & §24.3)
func TestMemberStatusAndLastOwnerSuspension(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	devID, _ := tenant.NewUUID()

	org, err := tc.service.CreateOrganization(ctx, ownerID, "Status Corp")
	if err != nil {
		t.Fatalf("CreateOrganization failed: %v", err)
	}

	// Add Developer member
	devMember, err := tc.service.AddMember(ctx, org.ID, devID, tenant.RoleDeveloper)
	if err != nil {
		t.Fatalf("AddMember failed: %v", err)
	}
	if devMember.Status != tenant.StatusActive {
		t.Fatalf("expected active status")
	}

	// 1. Suspend Developer -> succeeds
	err = tc.service.UpdateMemberStatus(ctx, org.ID, devID, tenant.StatusSuspended)
	if err != nil {
		t.Fatalf("UpdateMemberStatus suspend failed: %v", err)
	}

	// Verify Developer is suspended
	m, err := tc.service.GetMember(ctx, org.ID, devID)
	if err != nil || m.Status != tenant.StatusSuspended {
		t.Fatalf("expected member to be SUSPENDED, got %+v", m)
	}

	// 2. Attempt to suspend the sole active Owner -> rejected with ErrLastOwnerSuspension
	err = tc.service.UpdateMemberStatus(ctx, org.ID, ownerID, tenant.StatusSuspended)
	if err == nil || !errors.Is(err, tenant.ErrLastOwnerSuspension) {
		t.Fatalf("expected ErrLastOwnerSuspension when suspending sole Owner, got: %v", err)
	}

	// 3. Add second Owner -> now Owner 1 can be suspended
	owner2ID, _ := tenant.NewUUID()
	_, err = tc.service.AddMember(ctx, org.ID, owner2ID, tenant.RoleOwner)
	if err != nil {
		t.Fatalf("AddMember owner2 failed: %v", err)
	}

	err = tc.service.UpdateMemberStatus(ctx, org.ID, ownerID, tenant.StatusSuspended)
	if err != nil {
		t.Fatalf("expected suspending Owner 1 to succeed with second active Owner present, got: %v", err)
	}

	// 4. Attempting to suspend Owner 2 now fails because Owner 2 is the last remaining active Owner
	err = tc.service.UpdateMemberStatus(ctx, org.ID, owner2ID, tenant.StatusSuspended)
	if err == nil || !errors.Is(err, tenant.ErrLastOwnerSuspension) {
		t.Fatalf("expected ErrLastOwnerSuspension when suspending last remaining Owner 2, got: %v", err)
	}
}

// 16. TestLastOwnerDefenseConcurrentRace (Blueprint §24.2 & §24.3)
func TestLastOwnerDefenseConcurrentRace(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	owner0, _ := tenant.NewUUID()

	org, err := tc.service.CreateOrganization(ctx, owner0, "Concurrent Corp")
	if err != nil {
		t.Fatalf("CreateOrganization failed: %v", err)
	}

	const numOwners = 10
	ownerIDs := make([]string, numOwners)
	ownerIDs[0] = owner0

	for i := 1; i < numOwners; i++ {
		id, _ := tenant.NewUUID()
		ownerIDs[i] = id
		if _, err := tc.service.AddMember(ctx, org.ID, id, tenant.RoleOwner); err != nil {
			t.Fatalf("AddMember owner %d failed: %v", i, err)
		}
	}

	// 10 concurrent goroutines racing to remove each owner simultaneously.
	// Row-level locking on organizations table must serialize updates such that
	// exactly 9 removals succeed and exactly 1 fails with ErrLastOwnerRemoval.
	var wg sync.WaitGroup
	errs := make([]error, numOwners)
	startGate := make(chan struct{})

	for i := 0; i < numOwners; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-startGate
			errs[idx] = tc.service.RemoveMember(ctx, org.ID, ownerIDs[idx])
		}(i)
	}

	close(startGate)
	wg.Wait()

	successCount := 0
	lastOwnerErrCount := 0
	for i, err := range errs {
		if err == nil {
			successCount++
		} else if errors.Is(err, tenant.ErrLastOwnerRemoval) {
			lastOwnerErrCount++
		} else {
			t.Errorf("unexpected error for owner %d: %v", i, err)
		}
	}

	if successCount != numOwners-1 {
		t.Fatalf("expected exactly %d successful removals, got %d", numOwners-1, successCount)
	}
	if lastOwnerErrCount != 1 {
		t.Fatalf("expected exactly 1 ErrLastOwnerRemoval rejection, got %d", lastOwnerErrCount)
	}

	// Verify organization in database retains exactly 1 active Owner
	members, err := tc.service.ListMembers(ctx, org.ID)
	if err != nil {
		t.Fatalf("ListMembers failed: %v", err)
	}
	activeOwners := 0
	for _, m := range members {
		if m.Role == tenant.RoleOwner && m.Status == tenant.StatusActive {
			activeOwners++
		}
	}
	if activeOwners != 1 {
		t.Fatalf("expected exactly 1 active owner after concurrent removal race, got %d", activeOwners)
	}
}

// 17. TestAPIKeyLastUsedAtThrottling (Blueprint §24.4)
func TestAPIKeyLastUsedAtThrottling(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Throttle Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Throttle Proj")
	env, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)

	key := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapRunsRead})

	// 1st authentication -> updates last_used_at in DB
	auth1, err := tc.service.AuthenticateAPIKey(ctx, key.PlaintextKey)
	if err != nil {
		t.Fatalf("first authenticate failed: %v", err)
	}
	if auth1.LastUsedAt == nil {
		t.Fatalf("expected last_used_at to be populated")
	}

	var dbLastUsed1 time.Time
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, "SELECT last_used_at FROM api_keys WHERE id = $1", key.ID).Scan(&dbLastUsed1)
	})
	if err != nil {
		t.Fatalf("failed to query last_used_at after 1st auth: %v", err)
	}

	// 2nd authentication immediately after -> throttled (60s window)
	auth2, err := tc.service.AuthenticateAPIKey(ctx, key.PlaintextKey)
	if err != nil {
		t.Fatalf("second authenticate failed: %v", err)
	}
	if auth2 == nil {
		t.Fatalf("expected non-nil key")
	}

	var dbLastUsed2 time.Time
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, "SELECT last_used_at FROM api_keys WHERE id = $1", key.ID).Scan(&dbLastUsed2)
	})
	if err != nil {
		t.Fatalf("failed to query last_used_at after 2nd auth: %v", err)
	}

	// Assert timestamp in PostgreSQL did NOT advance
	if !dbLastUsed1.Equal(dbLastUsed2) {
		t.Fatalf("throttling violation: last_used_at advanced from %v to %v despite 60s throttle window", dbLastUsed1, dbLastUsed2)
	}
}

// 18. TestDualRouteMounting (OpenAPI /v1 and Gateway /api/v1)
func TestDualRouteMounting(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Route Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Route Proj")
	env, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvStaging, 10)
	key := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapRunsRead, tenant.CapOrgRead})

	server := httptest.NewServer(tc.handler.Routes())
	defer server.Close()

	// Call via /api/v1/projects
	req1, _ := http.NewRequest("GET", server.URL+"/api/v1/projects", nil)
	req1.Header.Set("Authorization", "Bearer "+key.PlaintextKey)
	req1.Header.Set("X-Organization-ID", org.ID)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("req1 failed: %v", err)
	}
	defer resp1.Body.Close()
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for /api/v1/projects, got %d", resp1.StatusCode)
	}

	// Call via /v1/projects
	req2, _ := http.NewRequest("GET", server.URL+"/v1/projects", nil)
	req2.Header.Set("Authorization", "Bearer "+key.PlaintextKey)
	req2.Header.Set("X-Organization-ID", org.ID)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("req2 failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for /v1/projects, got %d", resp2.StatusCode)
	}
}

// 19. TestCapabilityElevationForbidden
func TestCapabilityElevationForbidden(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Elevation Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Elevation Proj")
	env, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)

	// 1. Direct Service Call: Creator with [runs:read] cannot create key with [runs:read, runs:create]
	creatorCaps := []string{tenant.CapRunsRead}
	reqCaps := []string{tenant.CapRunsRead, tenant.CapRunsCreate}
	audit := &tenant.AuditContext{ActorID: &ownerID, Reason: "elevation_test"}
	_, err := tc.service.CreateAPIKey(ctx, org.ID, env.ID, reqCaps, 90, creatorCaps, audit)
	if err == nil || !errors.Is(err, tenant.ErrCapabilityElevation) {
		t.Fatalf("expected ErrCapabilityElevation, got: %v", err)
	}

	// 2. HTTP Endpoint Call: Caller machine key has [admin:key, runs:read], attempts to create key with [runs:create]
	callerKey := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapAdminKey, tenant.CapRunsRead})

	server := httptest.NewServer(tc.handler.Routes())
	defer server.Close()

	payload := fmt.Sprintf(`{"capabilities":["%s","%s"],"expiry_days":90}`, tenant.CapRunsRead, tenant.CapRunsCreate)
	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/environments/%s/api-keys", server.URL, env.ID), strings.NewReader(payload))
	req.Header.Set("Authorization", "Bearer "+callerKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", org.ID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "idemp-elevate-test-1")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for capability elevation, got %d", resp.StatusCode)
	}
	var envErr tenant.ErrorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&envErr); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if envErr.Code != "CAPABILITY_ELEVATION_FORBIDDEN" {
		t.Fatalf("expected code CAPABILITY_ELEVATION_FORBIDDEN, got %q", envErr.Code)
	}
}

// 20. TestEnvironmentMismatchOnKeyLifecycle
func TestEnvironmentMismatchOnKeyLifecycle(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Scope Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Scope Proj")
	stagingEnv, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvStaging, 10)
	prodEnv, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)

	// Machine admin key scoped strictly to stagingEnv
	stagingAdminKey := bootstrapTestKey(t, tc.service, org.ID, stagingEnv.ID, []string{tenant.CapAdminKey, tenant.CapRunsRead})

	// Production key owned by prodEnv
	prodKey := bootstrapTestKey(t, tc.service, org.ID, prodEnv.ID, []string{tenant.CapRunsRead})

	server := httptest.NewServer(tc.handler.Routes())
	defer server.Close()

	// 1. Attempt to rotate prod key using staging admin key -> 403 ENVIRONMENT_MISMATCH
	rotateReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/api-keys/%s/rotate", server.URL, prodKey.ID), strings.NewReader(`{}`))
	rotateReq.Header.Set("Authorization", "Bearer "+stagingAdminKey.PlaintextKey)
	rotateReq.Header.Set("X-Organization-ID", org.ID)
	rotateReq.Header.Set("Content-Type", "application/json")
	rotateReq.Header.Set("Idempotency-Key", "idemp-mismatch-rot-1")

	rotateResp, err := http.DefaultClient.Do(rotateReq)
	if err != nil {
		t.Fatalf("rotate request failed: %v", err)
	}
	defer rotateResp.Body.Close()

	if rotateResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden on cross-env rotate, got %d", rotateResp.StatusCode)
	}
	var rotErr tenant.ErrorEnvelope
	_ = json.NewDecoder(rotateResp.Body).Decode(&rotErr)
	if rotErr.Code != "ENVIRONMENT_MISMATCH" {
		t.Fatalf("expected ENVIRONMENT_MISMATCH, got %q", rotErr.Code)
	}

	// 2. Attempt to revoke prod key using staging admin key -> 403 ENVIRONMENT_MISMATCH
	revokeReq, _ := http.NewRequest("DELETE", fmt.Sprintf("%s/api/v1/api-keys/%s", server.URL, prodKey.ID), nil)
	revokeReq.Header.Set("Authorization", "Bearer "+stagingAdminKey.PlaintextKey)
	revokeReq.Header.Set("X-Organization-ID", org.ID)
	revokeReq.Header.Set("Idempotency-Key", "idemp-mismatch-rev-1")

	revokeResp, err := http.DefaultClient.Do(revokeReq)
	if err != nil {
		t.Fatalf("revoke request failed: %v", err)
	}
	defer revokeResp.Body.Close()

	if revokeResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden on cross-env revoke, got %d", revokeResp.StatusCode)
	}
	var revErr tenant.ErrorEnvelope
	_ = json.NewDecoder(revokeResp.Body).Decode(&revErr)
	if revErr.Code != "ENVIRONMENT_MISMATCH" {
		t.Fatalf("expected ENVIRONMENT_MISMATCH, got %q", revErr.Code)
	}
}

// 21. TestAuditedKeyLifecycleEvents
func TestAuditedKeyLifecycleEvents(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Audit Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Audit Proj")
	env, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)

	auditCtx := &tenant.AuditContext{
		ActorID:       &ownerID,
		CorrelationID: "test-corr-12345",
		Reason:        "unit-test-lifecycle",
	}

	// 1. Create API key with audit
	key1, err := tc.service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapRunsRead}, 90, tenant.RoleCapabilities(tenant.RoleOwner), auditCtx)
	if err != nil {
		t.Fatalf("CreateAPIKey failed: %v", err)
	}

	// 2. Rotate API key with audit
	key2, err := tc.service.RotateAPIKey(ctx, org.ID, key1.ID, 30, tenant.RoleCapabilities(tenant.RoleOwner), "", auditCtx)
	if err != nil {
		t.Fatalf("RotateAPIKey failed: %v", err)
	}

	// 3. Revoke API key with audit
	err = tc.service.RevokeAPIKey(ctx, org.ID, key2.ID, "", auditCtx)
	if err != nil {
		t.Fatalf("RevokeAPIKey failed: %v", err)
	}

	// Query audit_events table within tenant RLS context
	type eventRow struct {
		action        string
		targetType    string
		targetID      string
		actorID       string
		correlationID string
		metaText      string
	}
	var events []eventRow
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		query := `
			SELECT action, target_type, target_id, actor_id, correlation_id, metadata::text
			FROM audit_events
			WHERE organization_id = $1 AND target_type = 'api_key'
			ORDER BY created_at ASC
		`
		rows, err := tx.Query(ctx, query, org.ID)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var e eventRow
			if err := rows.Scan(&e.action, &e.targetType, &e.targetID, &e.actorID, &e.correlationID, &e.metaText); err != nil {
				return err
			}
			events = append(events, e)
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatalf("failed to query audit_events: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected exactly 3 audit events, got %d", len(events))
	}

	expectedActions := []string{"api_key.create", "api_key.rotate", "api_key.revoke"}
	for i, exp := range expectedActions {
		if events[i].action != exp {
			t.Errorf("event %d: expected action %s, got %s", i, exp, events[i].action)
		}
		if events[i].actorID != ownerID {
			t.Errorf("event %d: expected actor %s, got %s", i, ownerID, events[i].actorID)
		}
		if events[i].correlationID != "test-corr-12345" {
			t.Errorf("event %d: expected correlationId test-corr-12345, got %s", i, events[i].correlationID)
		}
		// Strict check: metadata MUST NOT contain plaintext secret or full secret key
		if strings.Contains(events[i].metaText, key1.PlaintextKey) || strings.Contains(events[i].metaText, key2.PlaintextKey) {
			t.Fatalf("CRITICAL SECURITY VIOLATION: plaintext API key found in audit_events metadata: %s", events[i].metaText)
		}
	}
}

// 22. TestAPIKeyRotationPrivilegeEscalation
func TestAPIKeyRotationPrivilegeEscalation(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Escalate Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Escalate Proj")
	env, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)

	// Target key has [runs:create, runs:read]
	targetKey := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapRunsCreate, tenant.CapRunsRead})

	// Caller key has [admin:key, runs:read] - LACKS runs:create!
	callerKey := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapAdminKey, tenant.CapRunsRead})

	server := httptest.NewServer(tc.handler.Routes())
	defer server.Close()

	// 1. Caller with subset of capabilities attempts to rotate -> rejected with 403 CAPABILITY_ELEVATION_FORBIDDEN
	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/api-keys/%s/rotate", server.URL, targetKey.ID), strings.NewReader(`{"expiry_days":60}`))
	req.Header.Set("Authorization", "Bearer "+callerKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", org.ID)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "idemp-rot-elevate-1")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for capability elevation on rotation, got %d", resp.StatusCode)
	}
	var errEnv tenant.ErrorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&errEnv); err != nil {
		t.Fatalf("failed to decode error envelope: %v", err)
	}
	if errEnv.Code != "CAPABILITY_ELEVATION_FORBIDDEN" {
		t.Fatalf("expected code CAPABILITY_ELEVATION_FORBIDDEN, got %q", errEnv.Code)
	}

	// 2. Caller with subsuming capabilities sends malformed JSON -> rejected with 400 MALFORMED_JSON
	fullAdminKey := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapAdminKey, tenant.CapRunsCreate, tenant.CapRunsRead})

	malformedReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/api-keys/%s/rotate", server.URL, targetKey.ID), strings.NewReader(`{"unknown_field": 123}`))
	malformedReq.Header.Set("Authorization", "Bearer "+fullAdminKey.PlaintextKey)
	malformedReq.Header.Set("X-Organization-ID", org.ID)
	malformedReq.Header.Set("Content-Type", "application/json")
	malformedReq.Header.Set("Idempotency-Key", "idemp-rot-malformed-1")

	malformedResp, err := http.DefaultClient.Do(malformedReq)
	if err != nil {
		t.Fatalf("malformed request failed: %v", err)
	}
	defer malformedResp.Body.Close()

	if malformedResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for unknown field in rotation body, got %d", malformedResp.StatusCode)
	}
	var malformedEnv tenant.ErrorEnvelope
	if err := json.NewDecoder(malformedResp.Body).Decode(&malformedEnv); err != nil {
		t.Fatalf("failed to decode malformed error envelope: %v", err)
	}
	if malformedEnv.Code != "MALFORMED_JSON" {
		t.Fatalf("expected code MALFORMED_JSON, got %q", malformedEnv.Code)
	}

	// 3. Caller with subsuming capabilities succeeds
	validReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/api-keys/%s/rotate", server.URL, targetKey.ID), strings.NewReader(`{"expiry_days":30}`))
	validReq.Header.Set("Authorization", "Bearer "+fullAdminKey.PlaintextKey)
	validReq.Header.Set("X-Organization-ID", org.ID)
	validReq.Header.Set("Content-Type", "application/json")
	validReq.Header.Set("Idempotency-Key", "idemp-rot-valid-1")

	validResp, err := http.DefaultClient.Do(validReq)
	if err != nil {
		t.Fatalf("valid rotate request failed: %v", err)
	}
	defer validResp.Body.Close()

	if validResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for rotation with subsuming capabilities, got %d", validResp.StatusCode)
	}
}

// 23. TestAuditEventsTableImmutability
func TestAuditEventsTableImmutability(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Immutability Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Immutability Proj")
	env, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)

	// Create an API key with audit to generate an audit_events record
	auditCtx := &tenant.AuditContext{
		ActorID:       &ownerID,
		CorrelationID: "audit-immut-1",
		Reason:        "test-immutability",
	}
	_, err := tc.service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapRunsRead}, 90, tenant.RoleCapabilities(tenant.RoleOwner), auditCtx)
	if err != nil {
		t.Fatalf("CreateAPIKey failed: %v", err)
	}

	// Attempt mutating audit_events table directly via runtimePool (connected as deadbolt_runtime)
	// 1. UPDATE must fail
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, "UPDATE audit_events SET action = 'tampered' WHERE organization_id = $1", org.ID)
		return err
	})
	if err == nil {
		t.Fatalf("SECURITY VIOLATION: UPDATE on audit_events should fail!")
	}
	t.Logf("UPDATE audit_events correctly rejected: %v", err)

	// 2. DELETE must fail
	err = tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, err := tx.Exec(ctx, "DELETE FROM audit_events WHERE organization_id = $1", org.ID)
		return err
	})
	if err == nil {
		t.Fatalf("SECURITY VIOLATION: DELETE on audit_events should fail!")
	}
	t.Logf("DELETE audit_events correctly rejected: %v", err)
}

// 24. TestAuthoritativeEnvironmentScoping
func TestAuthoritativeEnvironmentScoping(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Scope Enforcement Corp")
	projA, _ := tc.service.CreateProject(ctx, org.ID, "Project Alpha")
	projB, _ := tc.service.CreateProject(ctx, org.ID, "Project Beta")
	envA, _ := tc.service.CreateEnvironment(ctx, org.ID, projA.ID, tenant.EnvProduction, 10)
	_, _ = tc.service.CreateEnvironment(ctx, org.ID, projB.ID, tenant.EnvProduction, 10)

	// Machine key bound strictly to envA (under projA)
	keyA := bootstrapTestKey(t, tc.service, org.ID, envA.ID, []string{tenant.CapAdminProject, tenant.CapAdminMember, tenant.CapOrgRead})

	server := httptest.NewServer(tc.handler.Routes())
	defer server.Close()

	// 1. GET /projects by machine key returns ONLY projA, NOT projB
	reqProj, _ := http.NewRequest("GET", server.URL+"/api/v1/projects", nil)
	reqProj.Header.Set("Authorization", "Bearer "+keyA.PlaintextKey)
	reqProj.Header.Set("X-Organization-ID", org.ID)
	respProj, err := http.DefaultClient.Do(reqProj)
	if err != nil {
		t.Fatalf("GET /projects failed: %v", err)
	}
	defer respProj.Body.Close()
	if respProj.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for GET /projects, got %d", respProj.StatusCode)
	}
	var projResp struct {
		Projects []tenant.Project `json:"projects"`
	}
	if err := json.NewDecoder(respProj.Body).Decode(&projResp); err != nil {
		t.Fatalf("failed to decode project list: %v", err)
	}
	projList := projResp.Projects
	if len(projList) != 1 || projList[0].ID != projA.ID {
		t.Fatalf("expected machine key to only see scoped projA, got: %+v", projList)
	}

	// 2. POST /projects by machine key is rejected with 403 MACHINE_KEY_UNAUTHORIZED
	createProjReq, _ := http.NewRequest("POST", server.URL+"/api/v1/projects", strings.NewReader(`{"name":"Project Gamma"}`))
	createProjReq.Header.Set("Authorization", "Bearer "+keyA.PlaintextKey)
	createProjReq.Header.Set("X-Organization-ID", org.ID)
	createProjReq.Header.Set("Content-Type", "application/json")
	createProjReq.Header.Set("Idempotency-Key", "idemp-scope-proj-1")
	respCreateProj, err := http.DefaultClient.Do(createProjReq)
	if err != nil {
		t.Fatalf("POST /projects failed: %v", err)
	}
	defer respCreateProj.Body.Close()
	if respCreateProj.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for machine key creating project, got %d", respCreateProj.StatusCode)
	}

	// 3. POST /projects/{id}/environments by machine key is rejected with 403 MACHINE_KEY_UNAUTHORIZED
	createEnvReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/projects/%s/environments", server.URL, projA.ID), strings.NewReader(`{"name":"staging"}`))
	createEnvReq.Header.Set("Authorization", "Bearer "+keyA.PlaintextKey)
	createEnvReq.Header.Set("X-Organization-ID", org.ID)
	createEnvReq.Header.Set("Content-Type", "application/json")
	createEnvReq.Header.Set("Idempotency-Key", "idemp-scope-env-1")
	respCreateEnv, err := http.DefaultClient.Do(createEnvReq)
	if err != nil {
		t.Fatalf("POST /environments failed: %v", err)
	}
	defer respCreateEnv.Body.Close()
	if respCreateEnv.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for machine key creating environment, got %d", respCreateEnv.StatusCode)
	}

	// 4. GET /projects/{projB.ID}/environments by machine key bound to projA is rejected with 403 ENVIRONMENT_MISMATCH
	getForeignEnvReq, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/projects/%s/environments", server.URL, projB.ID), nil)
	getForeignEnvReq.Header.Set("Authorization", "Bearer "+keyA.PlaintextKey)
	getForeignEnvReq.Header.Set("X-Organization-ID", org.ID)
	respForeignEnv, err := http.DefaultClient.Do(getForeignEnvReq)
	if err != nil {
		t.Fatalf("GET foreign envs failed: %v", err)
	}
	defer respForeignEnv.Body.Close()
	if respForeignEnv.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for machine key accessing foreign project environments, got %d", respForeignEnv.StatusCode)
	}

	// 5. Member management endpoints reject machine keys with 403 MACHINE_KEY_UNAUTHORIZED
	listMembersReq, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/organizations/%s/members", server.URL, org.ID), nil)
	listMembersReq.Header.Set("Authorization", "Bearer "+keyA.PlaintextKey)
	listMembersReq.Header.Set("X-Organization-ID", org.ID)
	respListMembers, err := http.DefaultClient.Do(listMembersReq)
	if err != nil {
		t.Fatalf("GET members failed: %v", err)
	}
	defer respListMembers.Body.Close()
	if respListMembers.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for machine key listing members, got %d", respListMembers.StatusCode)
	}
}

// 25. TestControlPlaneProductionMuxWiring
func TestControlPlaneProductionMuxWiring(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	// Assert schema migration version matches expected
	if migrator.LatestSchemaVersion != 6 {
		t.Fatalf("expected migrator.LatestSchemaVersion to be 6, got %d", migrator.LatestSchemaVersion)
	}

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Mux Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Mux Proj")
	env, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)
	key := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapRunsRead, tenant.CapOrgRead})

	// Create production mux and wire routes identically to cmd/control-plane/main.go
	prodMux := http.NewServeMux()
	tc.handler.RegisterRoutes(prodMux)

	server := httptest.NewServer(prodMux)
	defer server.Close()

	// Assert dual prefix routes: /api/v1/projects and /v1/projects
	for _, prefix := range []string{"/api/v1", "/v1"} {
		req, _ := http.NewRequest("GET", server.URL+prefix+"/projects", nil)
		req.Header.Set("Authorization", "Bearer "+key.PlaintextKey)
		req.Header.Set("X-Organization-ID", org.ID)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("prefix %s failed: %v", prefix, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK on %s/projects, got %d", prefix, resp.StatusCode)
		}
	}
}

// 26. TestCapabilityVocabularyParity
func TestCapabilityVocabularyParity(t *testing.T) {
	// 1. Verify all defined capabilities are in canonical plural format
	allCaps := tenant.AllCapabilities
	if len(allCaps) == 0 {
		t.Fatalf("AllCapabilities must not be empty")
	}

	for _, cap := range allCaps {
		if strings.HasPrefix(cap, "run:") {
			t.Errorf("found singular capability %q, must use canonical plural 'runs:'", cap)
		}
		if strings.HasPrefix(cap, "approval:") {
			t.Errorf("found singular capability %q, must use canonical plural 'approvals:'", cap)
		}
		if strings.HasPrefix(cap, "deployment:") {
			t.Errorf("found singular capability %q, must use canonical plural 'deployments:'", cap)
		}
		if strings.HasPrefix(cap, "workflow:") {
			t.Errorf("found singular capability %q, must use canonical plural 'workflows:'", cap)
		}
		if strings.HasPrefix(cap, "worker:") {
			t.Errorf("found singular capability %q, must use canonical plural 'workers:'", cap)
		}
		if strings.HasPrefix(cap, "schedule:") {
			t.Errorf("found singular capability %q, must use canonical plural 'schedules:'", cap)
		}
		if strings.HasPrefix(cap, "webhook:") {
			t.Errorf("found singular capability %q, must use canonical plural 'webhooks:'", cap)
		}
		if strings.HasPrefix(cap, "artifact:") {
			t.Errorf("found singular capability %q, must use canonical plural 'artifacts:'", cap)
		}
	}

	// 2. Verify all role mappings only contain valid canonical capabilities
	for _, role := range []string{tenant.RoleViewer, tenant.RoleDeveloper, tenant.RoleOperator, tenant.RoleAdmin, tenant.RoleOwner} {
		roleCaps := tenant.RoleCapabilities(role)
		for _, rc := range roleCaps {
			if !tenant.IsValidCapability(rc) {
				t.Errorf("role %s contains invalid capability %q", role, rc)
			}
		}
	}
}

// 27. TestIdempotencyKeyEnforcement
func TestIdempotencyKeyEnforcement(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := tc.service.CreateOrganization(ctx, ownerID, "Idempotency Corp")
	proj, _ := tc.service.CreateProject(ctx, org.ID, "Idempotency Proj")
	env, _ := tc.service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)
	adminKey := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapAdminKey, tenant.CapRunsRead})

	server := httptest.NewServer(tc.handler.Routes())
	defer server.Close()

	// 1. Missing Idempotency-Key header on mutation -> 400 MISSING_IDEMPOTENCY_KEY
	reqMissing, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/environments/%s/api-keys", server.URL, env.ID), strings.NewReader(`{"capabilities":["runs:read"]}`))
	reqMissing.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	reqMissing.Header.Set("X-Organization-ID", org.ID)
	reqMissing.Header.Set("Content-Type", "application/json")

	respMissing, err := http.DefaultClient.Do(reqMissing)
	if err != nil {
		t.Fatalf("missing idemp request failed: %v", err)
	}
	defer respMissing.Body.Close()

	if respMissing.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for missing Idempotency-Key, got %d", respMissing.StatusCode)
	}
	var errEnv tenant.ErrorEnvelope
	if err := json.NewDecoder(respMissing.Body).Decode(&errEnv); err != nil {
		t.Fatalf("failed to decode error envelope: %v", err)
	}
	if errEnv.Code != "MISSING_IDEMPOTENCY_KEY" {
		t.Fatalf("expected code MISSING_IDEMPOTENCY_KEY, got %q", errEnv.Code)
	}

	// 2. Valid initial mutation with Idempotency-Key succeeds
	idempKey := "idemp-unique-" + time.Now().Format("20060102150405.000000")
	reqFirst, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/environments/%s/api-keys", server.URL, env.ID), strings.NewReader(`{"capabilities":["runs:read"]}`))
	reqFirst.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	reqFirst.Header.Set("X-Organization-ID", org.ID)
	reqFirst.Header.Set("Content-Type", "application/json")
	reqFirst.Header.Set("Idempotency-Key", idempKey)

	respFirst, err := http.DefaultClient.Do(reqFirst)
	if err != nil {
		t.Fatalf("first mutation failed: %v", err)
	}
	defer respFirst.Body.Close()

	if respFirst.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created on first mutation, got %d", respFirst.StatusCode)
	}

	// 3. Replay with identical Idempotency-Key -> 409 IDEMPOTENCY_CONFLICT
	reqReplay, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/environments/%s/api-keys", server.URL, env.ID), strings.NewReader(`{"capabilities":["runs:read"]}`))
	reqReplay.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	reqReplay.Header.Set("X-Organization-ID", org.ID)
	reqReplay.Header.Set("Content-Type", "application/json")
	reqReplay.Header.Set("Idempotency-Key", idempKey)

	respReplay, err := http.DefaultClient.Do(reqReplay)
	if err != nil {
		t.Fatalf("replay mutation failed: %v", err)
	}
	defer respReplay.Body.Close()

	if respReplay.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on replayed Idempotency-Key, got %d", respReplay.StatusCode)
	}
	var conflictEnv tenant.ErrorEnvelope
	if err := json.NewDecoder(respReplay.Body).Decode(&conflictEnv); err != nil {
		t.Fatalf("failed to decode conflict envelope: %v", err)
	}
	if conflictEnv.Code != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("expected code IDEMPOTENCY_CONFLICT, got %q", conflictEnv.Code)
	}
}
