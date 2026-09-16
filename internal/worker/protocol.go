package worker

import (
	"encoding/json"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
)

// ProtocolVersion is the canonical protocol version for /worker/v1/...
const ProtocolVersion = 1

type TraceContextDTO struct {
	Traceparent string `json:"traceparent"`
	Tracestate  string `json:"tracestate,omitempty"`
}

type ChallengeRequestDTO struct {
	ProtocolVersion int    `json:"protocolVersion"`
	RequestID       string `json:"requestId"`
	WorkerID        string `json:"workerId,omitempty"`
	PublicKey       string `json:"publicKey,omitempty"`
}

type ChallengeResponseDTO struct {
	ProtocolVersion int    `json:"protocolVersion"`
	RequestID       string `json:"requestId"`
	Nonce           string `json:"nonce"`
	ExpiresAt       string `json:"expiresAt"`
}

type EnrollRequestDTO struct {
	ProtocolVersion int    `json:"protocolVersion"`
	RequestID       string `json:"requestId"`
	EnrollmentToken string `json:"enrollmentToken"`
	PublicKey       string `json:"publicKey"`
	Nonce           string `json:"nonce"`
	Signature       string `json:"signature"`
}

type SessionRequestDTO struct {
	ProtocolVersion int    `json:"protocolVersion"`
	RequestID       string `json:"requestId"`
	WorkerID        string `json:"workerId"`
	Nonce           string `json:"nonce"`
	Signature       string `json:"signature"`
}

type SessionResponseDTO struct {
	ProtocolVersion int    `json:"protocolVersion"`
	RequestID       string `json:"requestId"`
	WorkerID        string `json:"workerId"`
	EnvironmentID   string `json:"environmentId"`
	SessionID       string `json:"sessionId"`
	SessionToken    string `json:"sessionToken"`
	ExpiresAt       string `json:"expiresAt"`
}

type PollRequestDTO struct {
	ProtocolVersion   int      `json:"protocolVersion"`
	RequestID         string   `json:"requestId"`
	WorkerID          string   `json:"workerId"`
	SessionID         string   `json:"sessionId"`
	AvailableSlots    int      `json:"availableSlots"`
	DeploymentDigests []string `json:"deploymentDigests"`
	Pool              string   `json:"pool"`
}

type AssignmentDTO struct {
	RunID                string          `json:"runId"`
	StepID               string          `json:"stepId"`
	AttemptID            string          `json:"attemptId"`
	OwnershipEpoch       int64           `json:"ownershipEpoch"`
	TaskEntrypoint       string          `json:"taskEntrypoint"`
	Input                any             `json:"input"`
	DeploymentDigest     string          `json:"deploymentDigest"`
	BundleDigest         string          `json:"bundleDigest"`
	OperationID          string          `json:"operationId"`
	LeaseTTLMs           int64           `json:"leaseTtlMs"`
	LeaseExpiresAt       string          `json:"leaseExpiresAt"`
	ClaimStartDeadlineAt string          `json:"claimStartDeadlineAt"`
	AttemptTimeoutMs     int64           `json:"attemptTimeoutMs"`
	RunDeadlineAt        string          `json:"runDeadlineAt"`
	TraceContext         TraceContextDTO `json:"traceContext"`
	TargetArchitecture   string          `json:"targetArchitecture,omitempty"`
	SecretNames          []string        `json:"secretNames,omitempty"`
}

type PollResponseDTO struct {
	ProtocolVersion int             `json:"protocolVersion"`
	RequestID       string          `json:"requestId"`
	Assignments     []AssignmentDTO `json:"assignments"`
}

type StartRequestDTO struct {
	ProtocolVersion int    `json:"protocolVersion"`
	RequestID       string `json:"requestId"`
	WorkerID        string `json:"workerId"`
	SessionID       string `json:"sessionId"`
	AttemptID       string `json:"attemptId"`
	OwnershipEpoch  int64  `json:"ownershipEpoch"`
}

type StartResponseDTO struct {
	ProtocolVersion   int    `json:"protocolVersion"`
	RequestID         string `json:"requestId"`
	AttemptID         string `json:"attemptId"`
	OwnershipEpoch    int64  `json:"ownershipEpoch"`
	Accepted          bool   `json:"accepted"`
	AttemptDeadlineAt string `json:"attemptDeadlineAt"`
	LeaseTTLMs        int64  `json:"leaseTtlMs"`
}

type HeartbeatAttemptDTO struct {
	AttemptID      string `json:"attemptId"`
	OwnershipEpoch int64  `json:"ownershipEpoch"`
	Progress       any    `json:"progress,omitempty"`
}

type HeartbeatRequestDTO struct {
	ProtocolVersion int                   `json:"protocolVersion"`
	RequestID       string                `json:"requestId"`
	WorkerID        string                `json:"workerId"`
	SessionID       string                `json:"sessionId"`
	Attempts        []HeartbeatAttemptDTO `json:"attempts"`
}

type LeaseRenewalDTO struct {
	AttemptID      string `json:"attemptId"`
	OwnershipEpoch int64  `json:"ownershipEpoch"`
	LeaseTTLMs     int64  `json:"leaseTtlMs"`
	LeaseExpiresAt string `json:"leaseExpiresAt"`
}

type StopCommandDTO struct {
	AttemptID      string `json:"attemptId"`
	OwnershipEpoch int64  `json:"ownershipEpoch"`
	Reason         string `json:"reason"`
	GraceTimeoutMs int64  `json:"graceTimeoutMs"`
}

type HeartbeatResponseDTO struct {
	ProtocolVersion int               `json:"protocolVersion"`
	RequestID       string            `json:"requestId"`
	Renewals        []LeaseRenewalDTO `json:"renewals"`
	Stops           []StopCommandDTO  `json:"stops"`
}

type TaskErrorDTO struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	Details      any    `json:"details,omitempty"`
	Retryable    bool   `json:"retryable"`
	EffectStatus string `json:"effectStatus,omitempty"`
	RetryAfter   string `json:"retryAfter,omitempty"`
}

type CompleteRequestDTO struct {
	ProtocolVersion int           `json:"protocolVersion"`
	RequestID       string        `json:"requestId"`
	WorkerID        string        `json:"workerId"`
	SessionID       string        `json:"sessionId"`
	AttemptID       string        `json:"attemptId"`
	OwnershipEpoch  int64         `json:"ownershipEpoch"`
	Outcome         string        `json:"outcome"`
	Output          any           `json:"output,omitempty"`
	ArtifactID      string        `json:"artifactId,omitempty"`
	Error           *TaskErrorDTO `json:"error,omitempty"`
	ResultDigest    string        `json:"resultDigest"`
}

type CompleteResponseDTO struct {
	ProtocolVersion int    `json:"protocolVersion"`
	RequestID       string `json:"requestId"`
	AttemptID       string `json:"attemptId"`
	OwnershipEpoch  int64  `json:"ownershipEpoch"`
	Accepted        bool   `json:"accepted"`
	ResultDigest    string `json:"resultDigest"`
}

// CanonicalCompletionDigest is the server-verifiable identity of a completion.
// It deliberately includes outcome and the structured error envelope, not just
// an arbitrary worker-provided output string.
func CanonicalCompletionDigest(req *CompleteRequestDTO) (string, error) {
	var errorEnvelope any
	if req.Error != nil {
		b, err := json.Marshal(req.Error)
		if err != nil {
			return "", err
		}
		if err := json.Unmarshal(b, &errorEnvelope); err != nil {
			return "", err
		}
	}
	canonical, err := contracts.CanonicalizeGeneric(map[string]any{
		"outcome":    req.Outcome,
		"output":     req.Output,
		"artifactId": req.ArtifactID,
		"error":      errorEnvelope,
	})
	if err != nil {
		return "", err
	}
	return contracts.SHA256Hex(canonical), nil
}

type StopAckRequestDTO struct {
	ProtocolVersion int    `json:"protocolVersion"`
	RequestID       string `json:"requestId"`
	WorkerID        string `json:"workerId"`
	SessionID       string `json:"sessionId"`
	AttemptID       string `json:"attemptId"`
	OwnershipEpoch  int64  `json:"ownershipEpoch"`
	ProcessStopped  bool   `json:"processStopped"`
}

type AckResponseDTO struct {
	ProtocolVersion int    `json:"protocolVersion"`
	RequestID       string `json:"requestId"`
	Accepted        bool   `json:"accepted"`
	DroppedCount    int    `json:"droppedCount,omitempty"`
	BudgetExhausted bool   `json:"budgetExhausted,omitempty"`
}

type LogRecordDTO struct {
	Sequence  int64  `json:"sequence"`
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
}

type LogBatchRequestDTO struct {
	ProtocolVersion int            `json:"protocolVersion"`
	RequestID       string         `json:"requestId"`
	WorkerID        string         `json:"workerId"`
	SessionID       string         `json:"sessionId"`
	AttemptID       string         `json:"attemptId"`
	Records         []LogRecordDTO `json:"records"`
}

type ErrorEnvelopeDTO struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId"`
	Details   any    `json:"details,omitempty"`
	Retryable bool   `json:"retryable"`
}
