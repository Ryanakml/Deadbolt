package execution

import (
	"time"

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
	Attempts     []AttemptSummaryDTO  `json:"attempts,omitempty"`
}

type AttemptSummaryDTO struct {
	AttemptNumber int        `json:"attemptNumber"`
	Status        string     `json:"status"`
	StartedAt     *time.Time `json:"startedAt,omitempty"`
	CompletedAt   *time.Time `json:"completedAt,omitempty"`
}

type RunSnapshotDTO struct {
	RunDTO
	LastEventSequence int64        `json:"lastEventSequence"`
	Steps             []RunStepDTO `json:"steps"`
	Output            any          `json:"output,omitempty"`
	Error             any          `json:"error,omitempty"`
}
