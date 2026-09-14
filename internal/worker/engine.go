package worker

import (
	"context"

	"github.com/Ryanakml/Deadbolt/internal/storage"
)

// ExecutionEngine owns authoritative claim and completion transitions. The
// worker transport only carries the engine's assignment/result decisions.
type ExecutionEngine interface {
	Claim(context.Context, *WorkerSessionContext, *PollRequestDTO) (*PollResponseDTO, error)
	Start(context.Context, *WorkerSessionContext, *StartRequestDTO) (*StartResponseDTO, error)
	Heartbeat(context.Context, *WorkerSessionContext, *HeartbeatRequestDTO) (*HeartbeatResponseDTO, error)
	Complete(context.Context, *WorkerSessionContext, *CompleteRequestDTO) (*CompleteResponseDTO, error)
	StopAck(context.Context, *WorkerSessionContext, *StopAckRequestDTO) (*AckResponseDTO, error)
	FenceWorkerSessions(context.Context, storage.Tx, string, string, string) error
}
