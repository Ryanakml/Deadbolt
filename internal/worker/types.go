package worker

import (
	"context"
	"time"
)

// TaskInput matches the runner input protocol.
type TaskInput struct {
	AttemptID   string            `json:"attemptId"`
	OperationID string            `json:"operationId"`
	TaskName    string            `json:"taskName"`
	Entrypoint  string            `json:"entrypoint,omitempty"`
	Input       any               `json:"input"`
	TimeoutMs   int64             `json:"timeoutMs,omitempty"`
	Env         map[string]string `json:"env,omitempty"`
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
