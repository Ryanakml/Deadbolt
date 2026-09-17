package worker

import (
	"context"
	"time"
)

// TaskInput matches the runner input protocol.
type TaskInput struct {
	AttemptID   string            `json:"attemptId"`
	OperationID string            `json:"operationId"`
	StepID      string            `json:"stepId"`
	TaskName    string            `json:"taskName"`
	Entrypoint  string            `json:"entrypoint,omitempty"`
	Input       any               `json:"input"`
	TimeoutMs   int64             `json:"timeoutMs,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
	Bundle      *BundleSpec       `json:"bundle,omitempty"`
}

// BundleSpec is the manifest-pinned artifact identity the supervisor verifies
// before it permits customer code to start.
type BundleSpec struct {
	Path       string `json:"path"`
	SHA256     string `json:"sha256"`
	TargetArch string `json:"targetArch"`
	TargetOS   string `json:"targetOS,omitempty"`   // assigned execution target OS; empty when the assignment predates it
	Entrypoint string `json:"entrypoint,omitempty"` // relative path inside a verified tar bundle
}

// TaskError represents structured failure details.
type TaskError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	Details   any    `json:"details,omitempty"`
}

// TaskMetrics records attempt execution duration.
type TaskMetrics struct {
	DurationMs int64 `json:"durationMs"`
}

// TaskCompletion represents the authoritative result payload from FD 3 / result channel.
type TaskCompletion struct {
	AttemptID string      `json:"attemptId"`
	Status    string      `json:"status"` // "SUCCEEDED" or "FAILED"
	Output    any         `json:"output,omitempty"`
	Error     *TaskError  `json:"error,omitempty"`
	Metrics   TaskMetrics `json:"metrics"`
}

// StopResult records the outcome of a graceful/forced termination sequence.
type StopResult struct {
	AttemptID     string
	PID           int
	Stopped       bool
	GraceExceeded bool
	Duration      time.Duration
}

// StartAckFunc represents the callback to verify Start ACK with control plane.
type StartAckFunc func(ctx context.Context, attemptID string, epoch int64) error

type StartDecision string

const (
	StartAccepted  StartDecision = "ACCEPTED"
	StartRejected  StartDecision = "REJECTED"
	StartAmbiguous StartDecision = "AMBIGUOUS"
)

// StartDecisionFunc models the gateway result. An ambiguous transport result
// is retried with the same attempt identity; reject is authoritative.
type StartDecisionFunc func(ctx context.Context, attemptID string, epoch int64) (StartDecision, error)

// StartAuthorization is an immutable, typed handoff of the authenticated
// control-plane Start decision to the local supervisor. It prevents a second
// logical Start transition while keeping the supervisor's launch gate intact.
type StartAuthorization struct {
	AttemptID string
	Epoch     int64
	Decision  StartDecision
}

type LeaseRenewal struct {
	ExpiresAt time.Time
	RTT       time.Duration
}

type RenewLeaseFunc func(ctx context.Context, attemptID string, epoch int64) (LeaseRenewal, error)
