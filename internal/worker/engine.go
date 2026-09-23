package worker

import (
	"context"
	"errors"

	"github.com/Ryanakml/Deadbolt/internal/storage"
)

// ErrExecutionEngineUnavailable means the gateway has no authoritative engine
// wired in. It intentionally carries no claim, result, or state semantics.
var ErrExecutionEngineUnavailable = errors.New("EXECUTION_ENGINE_UNAVAILABLE: authoritative execution engine is not installed")

// ErrRecoveryControlsUnavailable means the disaster recovery controls row is
// missing or unreadable, so dispatch is denied fail-closed. It carries no
// tenant data and no database internals.
var ErrRecoveryControlsUnavailable = errors.New("RECOVERY_CONTROLS_UNAVAILABLE: System recovery controls are unavailable; dispatch is denied")

// ExecutionEngine owns authoritative claim and completion transitions. The
// worker transport only carries the engine's assignment/result decisions.
type ExecutionEngine interface {
	Claim(context.Context, *WorkerSessionContext, *PollRequestDTO) (*PollResponseDTO, error)
	Start(context.Context, *WorkerSessionContext, *StartRequestDTO) (*StartResponseDTO, error)
	Heartbeat(context.Context, *WorkerSessionContext, *HeartbeatRequestDTO) (*HeartbeatResponseDTO, error)
	Complete(context.Context, *WorkerSessionContext, *CompleteRequestDTO) (*CompleteResponseDTO, error)
	StopAck(context.Context, *WorkerSessionContext, *StopAckRequestDTO) (*AckResponseDTO, error)
}

// SessionFencer allows fencing worker sessions and releasing claimed/running
// tasks on reconnect or revocation within a tenant transaction.
type SessionFencer interface {
	FenceWorkerSessions(ctx context.Context, tx storage.Tx, organizationID, workerID, reason string) error
}
