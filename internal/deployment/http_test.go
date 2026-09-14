package deployment

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/tenant"
)

func TestRegisterRequiresEnvironmentAndAuthentication(t *testing.T) {
	h := NewHTTPHandler(nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/deployments", nil)
	rec := httptest.NewRecorder()
	h.Register(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthenticated registration to be denied, got %d", rec.Code)
	}
}

func TestMachineKeyAuthorizationIsEnvironmentScoped(t *testing.T) {
	h := NewHTTPHandler(nil, nil)
	caller := &tenant.CallerIdentity{Type: tenant.IdentityTypeMachine, OrganizationID: "org", EnvironmentID: "env-a", Capabilities: []string{tenant.CapDeploymentsRegister}}
	if !h.allowed(httptest.NewRequest(http.MethodPost, "/", nil), caller, "env-a", tenant.CapDeploymentsRegister) {
		t.Fatal("expected scoped key with capability to be permitted")
	}
	if h.allowed(httptest.NewRequest(http.MethodPost, "/", nil), caller, "env-b", tenant.CapDeploymentsRegister) {
		t.Fatal("machine key must not cross environment scope")
	}
}
