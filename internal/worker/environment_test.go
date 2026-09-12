package worker_test

import (
	"strings"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func TestSanitizeEnvironment(t *testing.T) {
	parentEnv := []string{
		"PATH=/usr/bin:/bin",
		"SYSTEMROOT=C:\\Windows",
		"DEADBOLT_AGENT_TOKEN=secret_agent_token_12345",
		"DEADBOLT_SESSION_KEY=secret_session_key_abcde",
		"DATABASE_URL=postgres://user:pass@localhost/db",
		"SOME_SECRET_KEY=confidential",
		"NODE_ENV=production",
		"USER_NAME=worker_runner",
	}

	declaredTaskEnv := map[string]string{
		"CUSTOM_CONFIG": "enabled",
		"MAX_WORKERS":   "4",
	}

	allowlist := []string{"NODE_ENV"}

	sanitized := worker.SanitizeEnvironment(parentEnv, declaredTaskEnv, allowlist)

	lookup := make(map[string]string)
	for _, entry := range sanitized {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) == 2 {
			lookup[parts[0]] = parts[1]
		}
	}

	// 1. Verify sensitive tokens stripped
	sensitiveKeys := []string{
		"DEADBOLT_AGENT_TOKEN",
		"DEADBOLT_SESSION_KEY",
		"DATABASE_URL",
		"SOME_SECRET_KEY",
	}
	for _, k := range sensitiveKeys {
		if _, exists := lookup[k]; exists {
			t.Errorf("expected sensitive key %q to be stripped from child env, but was present", k)
		}
	}

	// 2. Verify allowed system paths kept
	if _, exists := lookup["PATH"]; !exists {
		t.Errorf("expected PATH to be preserved in child env")
	}

	// 3. Verify task declared variables injected
	if val, exists := lookup["CUSTOM_CONFIG"]; !exists || val != "enabled" {
		t.Errorf("expected CUSTOM_CONFIG=enabled, got %v", val)
	}
	if val, exists := lookup["MAX_WORKERS"]; !exists || val != "4" {
		t.Errorf("expected MAX_WORKERS=4, got %v", val)
	}
}
