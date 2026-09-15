package worker_test

import (
	"errors"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func TestAgentURLSecurityEnforcement(t *testing.T) {
	// Remote HTTP must be rejected
	cfgRemoteHTTP := worker.AgentConfig{
		ControlPlaneURL: "http://api.deadbolt.cloud",
	}
	_, err := worker.NewAgent(cfgRemoteHTTP)
	if !errors.Is(err, worker.ErrHTTPSRequired) {
		t.Errorf("expected ErrHTTPSRequired for remote HTTP, got: %v", err)
	}

	// Remote HTTPS must be allowed
	cfgRemoteHTTPS := worker.AgentConfig{
		ControlPlaneURL: "https://api.deadbolt.cloud",
	}
	agent, err := worker.NewAgent(cfgRemoteHTTPS)
	if err != nil {
		t.Errorf("expected success for remote HTTPS, got: %v", err)
	}
	if agent == nil {
		t.Errorf("expected non-nil agent")
	}

	// Loopback HTTP must be allowed
	loopbackURLs := []string{
		"http://localhost:8080",
		"http://127.0.0.1:8080",
		"http://127.0.0.1",
	}
	for _, u := range loopbackURLs {
		cfg := worker.AgentConfig{ControlPlaneURL: u}
		agent, err := worker.NewAgent(cfg)
		if err != nil {
			t.Errorf("expected success for loopback url %s, got: %v", u, err)
		}
		if agent == nil {
			t.Errorf("expected non-nil agent for %s", u)
		}
	}
}
