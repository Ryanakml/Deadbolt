package deployment

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
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

func deploymentRequest(method, target string, body []byte, caps []string) *http.Request {
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	caller := &tenant.CallerIdentity{Type: tenant.IdentityTypeMachine, OrganizationID: "org", EnvironmentID: "env", Capabilities: caps}
	return req.WithContext(tenant.ContextWithCaller(context.Background(), caller))
}

func TestActivateRejectsMissingRevisionAndUnknownFields(t *testing.T) {
	h := NewHTTPHandler(nil, nil)
	for _, body := range [][]byte{[]byte(`{"deploymentId":"d"}`), []byte(`{"deploymentId":"d","expectedRevision":0,"extra":true}`)} {
		rec := httptest.NewRecorder()
		h.Activate(rec, deploymentRequest(http.MethodPost, "/v1/workflows/w/activate?environment=env", body, []string{tenant.CapDeploymentsActivateStaging}))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected malformed activation rejected, got %d", rec.Code)
		}
	}
}

func TestRegisterReturns413ForOversizedManifest(t *testing.T) {
	h := NewHTTPHandler(nil, nil)
	rec := httptest.NewRecorder()
	h.Register(rec, deploymentRequest(http.MethodPost, "/v1/deployments?environment=env", bytes.Repeat([]byte("x"), (2<<20)+1), []string{tenant.CapDeploymentsRegister}))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
}

func TestUnexpectedFailureStaysSanitized(t *testing.T) {
	rec := httptest.NewRecorder()
	writeServiceErr(rec, httptest.NewRequest(http.MethodPost, "/v1/deployments", nil), errors.New("boom: password=hunter2 password=deadbolt_runtime://db:5432/x"))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
	var got map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if got["code"] != "INTERNAL_ERROR" {
		t.Fatalf("expected sanitized INTERNAL_ERROR code, got %#v", got)
	}
	if body := rec.Body.String(); strings.Contains(body, "boom") || strings.Contains(body, "hunter2") {
		t.Fatalf("sanitized envelope leaked internals: %s", body)
	}
}

func TestContractErrorPreservesCanonicalCode(t *testing.T) {
	rec := httptest.NewRecorder()
	writeServiceErr(rec, httptest.NewRequest(http.MethodPost, "/", nil), &contracts.Error{Code: "UNSUPPORTED_CAPABILITY"})
	var got map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil || got["code"] != "UNSUPPORTED_CAPABILITY" {
		t.Fatalf("canonical validator code was lost: %#v, %v", got, err)
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
