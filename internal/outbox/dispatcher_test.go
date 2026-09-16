package outbox_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/outbox"
)

func TestBuildSanitizedHint_NoSecretsOrOutputs(t *testing.T) {
	// Raw payload containing customer secrets and outputs that MUST NOT be forwarded into NATS
	rawPayload := map[string]any{
		"runId":       "98765432-1111-2222-3333-444455556666",
		"eventType":   "TASK_COMPLETED",
		"sequence":    float64(4),
		"secretToken": "super-sensitive-api-token-should-never-leak",
		"output": map[string]any{
			"accountId": "acc-12345",
			"password":  "do-not-leak",
		},
		"environmentVariables": []string{"STRIPE_KEY=sk_live_123"},
	}
	encoded, err := json.Marshal(rawPayload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	record := outbox.OutboxEventRecord{
		ID:             "00000000-0000-0000-0000-000000000001",
		OrganizationID: "11111111-2222-3333-4444-555566667777",
		EventID:        "22222222-3333-4444-5555-666677778888",
		Subject:        "execution.state_changed",
		Payload:        encoded,
		CreatedAt:      time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC),
	}

	hint, err := outbox.BuildSanitizedHint(record)
	if err != nil {
		t.Fatalf("BuildSanitizedHint failed: %v", err)
	}

	if hint.EventID != record.EventID {
		t.Errorf("expected eventId %s, got %s", record.EventID, hint.EventID)
	}
	if hint.OrganizationID != record.OrganizationID {
		t.Errorf("expected orgId %s, got %s", record.OrganizationID, hint.OrganizationID)
	}
	if hint.RunID != "98765432-1111-2222-3333-444455556666" {
		t.Errorf("expected runId %s, got %s", "98765432-1111-2222-3333-444455556666", hint.RunID)
	}
	if hint.EventType != "TASK_COMPLETED" {
		t.Errorf("expected eventType TASK_COMPLETED, got %s", hint.EventType)
	}
	if hint.Sequence != 4 {
		t.Errorf("expected sequence 4, got %d", hint.Sequence)
	}

	hintJSON, err := json.Marshal(hint)
	if err != nil {
		t.Fatalf("marshal hint failed: %v", err)
	}
	jsonStr := string(hintJSON)

	// Verify absence of all sensitive strings
	forbiddenSubstrings := []string{
		"super-sensitive-api-token",
		"do-not-leak",
		"STRIPE_KEY",
		"password",
		"secretToken",
	}
	for _, sub := range forbiddenSubstrings {
		if strings.Contains(jsonStr, sub) {
			t.Errorf("SECURITY LEAK: sanitized hint contains %q: %s", sub, jsonStr)
		}
	}
}

func TestBuildSanitizedHint_RejectsPoisonPayloads(t *testing.T) {
	base := outbox.OutboxEventRecord{OrganizationID: "org", EventID: "event", Subject: "execution.state_changed", PayloadVersion: 1, CreatedAt: time.Now()}
	for name, payload := range map[string][]byte{
		"malformed JSON":      []byte(`{"runId":`),
		"missing routing IDs": []byte(`{"eventType":"TASK_STARTED"}`),
		"wrong field types":   []byte(`{"runId":42,"eventType":"TASK_STARTED"}`),
		"fractional sequence": []byte(`{"runId":"run","eventType":"TASK_STARTED","sequence":1.5}`),
	} {
		t.Run(name, func(t *testing.T) {
			record := base
			record.Payload = payload
			if _, err := outbox.BuildSanitizedHint(record); err == nil {
				t.Fatal("expected payload validation error")
			}
		})
	}

	unsupported := base
	unsupported.PayloadVersion = 2
	unsupported.Payload = []byte(`{"runId":"run","eventType":"TASK_STARTED"}`)
	if _, err := outbox.BuildSanitizedHint(unsupported); err == nil {
		t.Fatal("expected unsupported payload version error")
	}
}

func TestCalculateBackoff_ExponentialAndBounded(t *testing.T) {
	d := outbox.NewDispatcher(nil, nil, outbox.DispatcherConfig{
		BaseBackoff:   100 * time.Millisecond,
		MaxRetryDelay: 5 * time.Second,
	}, nil, nil)

	// Attempt 1: ~100ms
	b1 := d.CalculateBackoff(1)
	if b1 < 100*time.Millisecond || b1 > 120*time.Millisecond {
		t.Errorf("attempt 1 out of bounds: %v", b1)
	}

	// Attempt 2: ~200ms
	b2 := d.CalculateBackoff(2)
	if b2 < 200*time.Millisecond || b2 > 240*time.Millisecond {
		t.Errorf("attempt 2 out of bounds: %v", b2)
	}

	// High attempt number should cap at MaxRetryDelay (5s + jitter)
	bHigh := d.CalculateBackoff(20)
	if bHigh > 6*time.Second {
		t.Errorf("attempt 20 exceeded cap: %v", bHigh)
	}
}

func TestMetrics_FormatPrometheus(t *testing.T) {
	m := outbox.NewMetrics(nil)
	m.IncPublished()
	m.IncPublished()
	m.IncFailures()

	text := m.FormatPrometheus()
	if !strings.Contains(text, "deadbolt_outbox_published_total 2") {
		t.Errorf("expected deadbolt_outbox_published_total 2, got:\n%s", text)
	}
	if !strings.Contains(text, "deadbolt_outbox_publish_failures_total 1") {
		t.Errorf("expected deadbolt_outbox_publish_failures_total 1, got:\n%s", text)
	}
	if !strings.Contains(text, "deadbolt_outbox_age_seconds 0") {
		t.Errorf("expected deadbolt_outbox_age_seconds 0, got:\n%s", text)
	}
	if !strings.Contains(text, "deadbolt_scheduler_loop_lag_seconds") {
		t.Errorf("expected deadbolt_scheduler_loop_lag_seconds, got:\n%s", text)
	}
}
