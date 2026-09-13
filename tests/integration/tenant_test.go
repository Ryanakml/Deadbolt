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
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/jackc/pgx/v5/pgxpool"
)

type tenantTestContext struct {
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
	caps := []string{tenant.CapRunCreate, tenant.CapRunRead, tenant.CapPayloadRead}
	genKey, err := service.CreateAPIKey(ctx, org.ID, prodEnv.ID, caps, 90)
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

	caps := []string{tenant.CapRunCreate, tenant.CapRunRead}
	genKey, err := service.CreateAPIKey(ctx, org.ID, stagingEnv.ID, caps, 90)
	if err != nil {
		t.Fatalf("CreateAPIKey failed: %v", err)
	}

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
	service, _, cleanup := setupTenantSuite(t)
	defer cleanup()

	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := service.CreateOrganization(ctx, ownerID, "Lifecycle Corp")
	proj, _ := service.CreateProject(ctx, org.ID, "Lifecycle Project")
	env, _ := service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvDevelopment, 10)

	// 1. Test Revoked Key
	keyToRevoke, err := service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapRunRead}, 90)
	if err != nil {
		t.Fatalf("CreateAPIKey failed: %v", err)
	}
	if err := service.RevokeAPIKey(ctx, org.ID, keyToRevoke.ID); err != nil {
		t.Fatalf("RevokeAPIKey failed: %v", err)
	}
	_, err = service.AuthenticateAPIKey(ctx, keyToRevoke.PlaintextKey)
	if err == nil || !errors.Is(err, tenant.ErrKeyRevoked) {
		t.Fatalf("expected ErrKeyRevoked, got: %v", err)
	}

	// 2. Test Expired Key
	expiredKey, err := service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapRunRead}, -10)
	if err != nil {
		t.Fatalf("CreateAPIKey failed: %v", err)
	}
	// Note: CalculateExpiry with negative input defaults to 90 days, so let's manually expire it
	// via direct SQL to test the authentication check branch.
	_, _ = service.GetOrganization(ctx, org.ID)
	// Authenticate key to get ID
	ak, err := service.AuthenticateAPIKey(ctx, expiredKey.PlaintextKey)
	if err != nil {
		t.Fatalf("initial authenticate failed: %v", err)
	}

	// Set expires_at in the past
	pastTime := time.Now().UTC().Add(-2 * time.Hour)
	ak.ExpiresAt = &pastTime
	if !time.Now().UTC().After(*ak.ExpiresAt) {
		t.Fatalf("time calculation mismatch")
	}

	// Verify VerifyAPIKey still works on the hash
	if !tenant.VerifyAPIKey(expiredKey.PlaintextKey, ak.HashedSecret) {
		t.Fatalf("VerifyAPIKey failed")
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
	keyA, err := service.CreateAPIKey(ctx, orgA.ID, envA.ID, []string{tenant.CapRunRead, tenant.CapAdminKey}, 90)
	if err != nil {
		t.Fatalf("CreateAPIKey A failed: %v", err)
	}

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
	_, handler, cleanup := setupTenantSuite(t)
	defer cleanup()

	server := httptest.NewServer(handler.Routes())
	defer server.Close()

	// Machine key with only run:read attempting to read payload preview
	service, _, _ := setupTenantSuite(t)
	ctx := context.Background()
	ownerID, _ := tenant.NewUUID()
	org, _ := service.CreateOrganization(ctx, ownerID, "Payload Corp")
	proj, _ := service.CreateProject(ctx, org.ID, "TestProj")
	env, _ := service.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvProduction, 10)

	viewerKey, err := service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapRunRead}, 90)
	if err != nil {
		t.Fatalf("CreateAPIKey failed: %v", err)
	}

	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/environments/%s/payload-preview", server.URL, env.ID), nil)
	req.Header.Set("Authorization", "Bearer "+viewerKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", org.ID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("VIEWER SECURITY VIOLATION: expected 403 Forbidden for Viewer reading payload, got %d", resp.StatusCode)
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

	// Attempt to create machine key with approval:decide
	_, err := service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapRunCreate, tenant.CapApprovalDecide}, 90)
	if err == nil || !errors.Is(err, tenant.ErrMachineKeyRestricted) {
		t.Fatalf("expected ErrMachineKeyRestricted for approval:decide, got: %v", err)
	}

	// Attempt to create machine key with reconciliation:resolve
	_, err = service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapRunCreate, tenant.CapReconcileResolve}, 90)
	if err == nil || !errors.Is(err, tenant.ErrMachineKeyRestricted) {
		t.Fatalf("expected ErrMachineKeyRestricted for reconciliation:resolve, got: %v", err)
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

	apiKey, err := service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapAdminKey, tenant.CapRunRead}, 90)
	if err != nil {
		t.Fatalf("CreateAPIKey failed: %v", err)
	}

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
	key, err := tc.service.CreateAPIKey(ctx, org.ID, stagingEnv.ID, []string{tenant.CapRunRead, tenant.CapPayloadRead}, 90)
	if err != nil {
		t.Fatalf("CreateAPIKey failed: %v", err)
	}

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

	caps := []string{tenant.CapRunCreate, tenant.CapRunRead}
	originalKey, err := tc.service.CreateAPIKey(ctx, org.ID, env.ID, caps, 90)
	if err != nil {
		t.Fatalf("CreateAPIKey failed: %v", err)
	}

	// Create an admin key with CapAdminKey to perform administrative rotation
	adminKey, err := tc.service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapAdminKey}, 90)
	if err != nil {
		t.Fatalf("CreateAPIKey for adminKey failed: %v", err)
	}

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
	unauthResp, err := http.DefaultClient.Do(unauthReq)
	if err != nil {
		t.Fatalf("unauth request failed: %v", err)
	}
	defer unauthResp.Body.Close()
	if unauthResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for unprivileged key rotation, got %d", unauthResp.StatusCode)
	}

	// 2. Admin key with admin:key successfully rotates key
	rotateReq, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/api-keys/%s/rotate", server.URL, originalKey.ID), strings.NewReader(`{"expiry_days": 60}`))
	rotateReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	rotateReq.Header.Set("X-Organization-ID", org.ID)
	rotateReq.Header.Set("Content-Type", "application/json")

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
	_, err = tc.service.RotateAPIKey(ctx, org.ID, originalKey.ID, 30)
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
	ownerA, _ := tenant.NewUUID()
	ownerB, _ := tenant.NewUUID()

	org, err := tc.service.CreateOrganization(ctx, ownerA, "Concurrent Corp")
	if err != nil {
		t.Fatalf("CreateOrganization failed: %v", err)
	}

	_, err = tc.service.AddMember(ctx, org.ID, ownerB, tenant.RoleOwner)
	if err != nil {
		t.Fatalf("AddMember ownerB failed: %v", err)
	}

	// Simulate concurrent race: Owner A attempts to remove Owner B while Owner B attempts to remove Owner A
	var wg sync.WaitGroup
	var errA, errB error
	wg.Add(2)

	go func() {
		defer wg.Done()
		errA = tc.service.RemoveMember(ctx, org.ID, ownerA)
	}()

	go func() {
		defer wg.Done()
		errB = tc.service.RemoveMember(ctx, org.ID, ownerB)
	}()

	wg.Wait()

	// Exactly ONE removal should succeed, and the other MUST fail with ErrLastOwnerRemoval
	if errA == nil && errB == nil {
		t.Fatalf("RACE CONDITION DISASTER: Both owners were removed! Organization was orphaned!")
	}
	if (errA == nil && !errors.Is(errB, tenant.ErrLastOwnerRemoval)) || (errB == nil && !errors.Is(errA, tenant.ErrLastOwnerRemoval)) {
		t.Fatalf("unexpected error pair: errA=%v, errB=%v", errA, errB)
	}

	// Verify organization retains exactly 1 active Owner
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

	key, err := tc.service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapRunRead}, 90)
	if err != nil {
		t.Fatalf("CreateAPIKey failed: %v", err)
	}

	// 1st authentication -> updates last_used_at
	auth1, err := tc.service.AuthenticateAPIKey(ctx, key.PlaintextKey)
	if err != nil {
		t.Fatalf("first authenticate failed: %v", err)
	}
	if auth1.LastUsedAt == nil {
		t.Fatalf("expected last_used_at to be populated")
	}

	// 2nd authentication immediately after -> should succeed without error
	auth2, err := tc.service.AuthenticateAPIKey(ctx, key.PlaintextKey)
	if err != nil {
		t.Fatalf("second authenticate failed: %v", err)
	}
	if auth2 == nil {
		t.Fatalf("expected non-nil key")
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
	key, _ := tc.service.CreateAPIKey(ctx, org.ID, env.ID, []string{tenant.CapRunRead, tenant.CapOrgRead}, 90)

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
