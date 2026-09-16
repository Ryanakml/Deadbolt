package execution

import (
	"github.com/Ryanakml/Deadbolt/internal/contracts"
)

type CreateRunRequestDTO struct {
	Environment  string  `json:"environment"`
	Input        any     `json:"input"`
	DeploymentID *string `json:"deploymentId,omitempty"`
}

type RunDTO struct {
	ID             string              `json:"id"`
	OrganizationID string              `json:"organizationId,omitempty"`
	ProjectID      string              `json:"projectId,omitempty"`
	EnvironmentID  string              `json:"environmentId,omitempty"`
	WorkflowName   string              `json:"workflowName"`
	DeploymentID   string              `json:"deploymentId"`
	Status         contracts.RunStatus `json:"status"`
	ReasonCode     *string             `json:"reasonCode,omitempty"`
	Revision       int64               `json:"revision"`
	CreatedAt      string              `json:"createdAt"`
	DeadlineAt     *string             `json:"deadlineAt,omitempty"`
}

type RunStepDTO struct {
	ID           string               `json:"id"`
	NodeID       string               `json:"nodeId"`
	Status       contracts.StepStatus `json:"status"`
	CurrentEpoch int64                `json:"currentEpoch"`
	Attempts     []AttemptSummaryDTO  `json:"attempts"`
}

type AttemptSummaryDTO struct {
	ID              string  `json:"id"`
	AttemptNumber   int     `json:"attemptNumber"`
	Status          string  `json:"status"`
	WorkerSessionID *string `json:"workerSessionId,omitempty"`
	OwnershipEpoch  int64   `json:"ownershipEpoch,omitempty"`
	StartedAt       *string `json:"startedAt,omitempty"`
	CompletedAt     *string `json:"completedAt,omitempty"`
	Error           any     `json:"error,omitempty"`
}

type RunSnapshotDTO struct {
	RunDTO
	LastEventSequence int64        `json:"lastEventSequence"`
	Steps             []RunStepDTO `json:"steps"`
	Output            any          `json:"output,omitempty"`
	Error             any          `json:"error,omitempty"`
}

type RunListResponseDTO struct {
	Items      []RunDTO `json:"items"`
	NextCursor *string  `json:"nextCursor"`
}

type WorkerDTO struct {
	ID                string   `json:"id"`
	EnvironmentID     string   `json:"environmentId"`
	Pool              string   `json:"pool"`
	Status            string   `json:"status"`
	DeploymentDigests []string `json:"deploymentDigests"`
}

type WorkerListResponseDTO struct {
	Items      []WorkerDTO `json:"items"`
	NextCursor *string     `json:"nextCursor"`
}

type RunEventDTO struct {
	ID            string  `json:"id"`
	RunID         string  `json:"runId"`
	Sequence      int64   `json:"sequence"`
	SchemaVersion int     `json:"schemaVersion"`
	Type          string  `json:"type"`
	RequestID     *string `json:"requestId,omitempty"`
	Payload       any     `json:"payload"`
	CommittedAt   string  `json:"committedAt"`
}

type RunEventsResponseDTO struct {
	Events     []RunEventDTO `json:"events"`
	HasMore    bool          `json:"hasMore"`
	NextCursor *int64        `json:"nextCursor,omitempty"`
}

type TaskLogRecordDTO struct {
	ID        string `json:"id"`
	RunID     string `json:"runId"`
	StepID    string `json:"stepId"`
	AttemptID string `json:"attemptId"`
	Sequence  int64  `json:"sequence"`
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
}

type RunLogsResponseDTO struct {
	Items      []TaskLogRecordDTO `json:"items"`
	NextCursor *string            `json:"nextCursor,omitempty"`
	Expired    bool               `json:"expired"`
	Message    *string            `json:"message,omitempty"`
}
