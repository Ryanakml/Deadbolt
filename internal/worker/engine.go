package worker

import (
	"context"
	"errors"
)

// ErrExecutionEngineUnavailable means the gateway has no authoritative engine
// wired in. It intentionally carries no claim, result, or state semantics.
var ErrExecutionEngineUnavailable = errors.New("EXECUTION_ENGINE_UNAVAILABLE: authoritative execution engine is not installed")

// ExecutionEngine owns authoritative claim and completion transitions. The
// worker transport only carries the engine's assignment/result decisions.
type ExecutionEngine interface {
	Claim(context.Context, *WorkerSessionContext, *PollRequestDTO) (*PollResponseDTO, error)
	Start(context.Context, *WorkerSessionContext, *StartRequestDTO) (*StartResponseDTO, error)
	Heartbeat(context.Context, *WorkerSessionContext, *HeartbeatRequestDTO) (*HeartbeatResponseDTO, error)
	Complete(context.Context, *WorkerSessionContext, *CompleteRequestDTO) (*CompleteResponseDTO, error)
	StopAck(context.Context, *WorkerSessionContext, *StopAckRequestDTO) (*AckResponseDTO, error)
}
