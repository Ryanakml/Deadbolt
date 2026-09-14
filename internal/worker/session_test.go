package worker_test

import (
	"strings"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func TestSessionTokenGeneration(t *testing.T) {
	rawToken, tokenHash, err := worker.GenerateSessionToken()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.HasPrefix(rawToken, "dbs_") {
		t.Errorf("expected session token prefix dbs_, got: %s", rawToken)
	}

	if len(tokenHash) != 64 {
		t.Errorf("expected 64-char sha256 hex digest, got length %d", len(tokenHash))
	}

	expectedHash := worker.HashToken(rawToken)
	if tokenHash != expectedHash {
		t.Errorf("session token hash mismatch")
	}
}
