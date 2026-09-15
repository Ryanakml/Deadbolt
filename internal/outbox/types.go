package outbox

import (
	"context"
	"encoding/json"
	"time"
)

// StreamName is the internal NATS JetStream stream name for control-plane wake-up hints.
const StreamName = "RUNTIME_WAKEUP"

// SubjectPrefix is the internal versioned subject prefix for wake-up hints (Blueprint §19.1).
const SubjectPrefix = "runtime.v1.wakeup."

// DefaultShard is the default subject shard.
const DefaultShard = "default"

// DefaultBatchSize is the standard bounded batch size for outbox sweeps.
const DefaultBatchSize = 100

// DefaultMaxRetryDelay is the maximum backoff duration for failed outbox events.
const DefaultMaxRetryDelay = 60 * time.Second

// OutboxEventRecord represents a row from the outbox_events table.
type OutboxEventRecord struct {
	ID             string          `json:"id"`
	OrganizationID string          `json:"organizationId"`
	EventID        string          `json:"eventId"`
	Subject        string          `json:"subject"`
	Payload        json.RawMessage `json:"payload"`
	PayloadVersion int             `json:"payloadVersion"`
	Attempts       int             `json:"attempts"`
	NextAt         time.Time       `json:"nextAt"`
	PublishedAt    *time.Time      `json:"publishedAt,omitempty"`
	CreatedAt      time.Time       `json:"createdAt"`
}

// WakeupHintDTO represents the sanitized wake-up message published to NATS JetStream (Blueprint §19.1).
// Requirement: "Messages contain IDs/routing hints without secrets/outputs and trigger DB scans only."
type WakeupHintDTO struct {
	EventID        string `json:"eventId"`
	OrganizationID string `json:"organizationId"`
	RunID          string `json:"runId,omitempty"`
	Subject        string `json:"subject"`
	EventType      string `json:"eventType,omitempty"`
	Sequence       int64  `json:"sequence,omitempty"`
	Timestamp      string `json:"timestamp"`
}

// WakeupHandler defines the callback triggered when a wake-up hint is delivered by JetStream.
// Requirement: Receiving a hint triggers DB scans only; it does NOT grant execution ownership.
type WakeupHandler interface {
	HandleWakeup(ctx context.Context, hint WakeupHintDTO) error
}

// WakeupHandlerFunc allows using a regular function as a WakeupHandler.
type WakeupHandlerFunc func(ctx context.Context, hint WakeupHintDTO) error

// HandleWakeup calls the underlying function.
func (f WakeupHandlerFunc) HandleWakeup(ctx context.Context, hint WakeupHintDTO) error {
	return f(ctx, hint)
}
