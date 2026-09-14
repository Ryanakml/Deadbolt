package worker

import "context"

// ExecutionEngine owns authoritative claim and completion transitions. The
// worker transport only carries the engine's assignment/result decisions.
type ExecutionEngine interface {
	Claim(context.Context, *WorkerSessionContext, *PollRequestDTO) (*PollResponseDTO, error)
	Complete(context.Context, *WorkerSessionContext, *CompleteRequestDTO) (*CompleteResponseDTO, error)
}

type sqlExecutionEngine struct{ service *Service }

func (e sqlExecutionEngine) Claim(ctx context.Context, session *WorkerSessionContext, req *PollRequestDTO) (*PollResponseDTO, error) {
	return e.service.claimAssignmentsSQL(ctx, session, req)
}

func (e sqlExecutionEngine) Complete(ctx context.Context, session *WorkerSessionContext, req *CompleteRequestDTO) (*CompleteResponseDTO, error) {
	return e.service.completeAttemptSQL(ctx, session, req)
}
