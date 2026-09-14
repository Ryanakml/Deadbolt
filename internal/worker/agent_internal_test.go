package worker

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestResolveTaskSecretsFailsClosedWhenRequiredSecretIsMissing(t *testing.T) {
	const name = "DEADBOLT_TEST_REQUIRED_SECRET_MUST_NOT_EXIST"
	t.Setenv(name, "temporary")
	// t.Setenv registers restoration; remove the temporary value to exercise
	// the actual missing-secret path used before process creation.
	if err := os.Unsetenv(name); err != nil {
		t.Fatal(err)
	}
	values, err := resolveTaskSecrets([]string{name})
	if err == nil || values != nil || !strings.Contains(err.Error(), name) {
		t.Fatalf("missing required secret did not fail closed: values=%v err=%v", values, err)
	}
}

func TestCapturedProcessOutputIsStrictlyBounded(t *testing.T) {
	var output cappedBuffer
	payload := bytes.Repeat([]byte("x"), maxCapturedOutput+4096)
	written, err := output.Write(payload)
	if err != nil || written != len(payload) {
		t.Fatalf("writer contract failed: written=%d err=%v", written, err)
	}
	if output.Len() != maxCapturedOutput || !output.truncated {
		t.Fatalf("output cap failed: len=%d truncated=%v", output.Len(), output.truncated)
	}
	if written, err := output.Write([]byte("ignored")); err != nil || written != len("ignored") || output.Len() != maxCapturedOutput {
		t.Fatalf("writes after cap changed buffer: written=%d len=%d err=%v", written, output.Len(), err)
	}
}
