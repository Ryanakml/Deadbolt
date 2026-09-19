package integration_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// TestWrongPoolPollRejectsWithoutClaim proves F-21's missing wrong-pool case
// at the real worker poll boundary: a default-pool session polling with a
// different pool is rejected per the canonical 401 contract, creates no
// ownership/lease, while the correct pool can claim the same step.
func TestWrongPoolPollRejectsWithoutClaim(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()
	session, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "wrong-pool")
	const digest = "bundle-wrong-pool"
	deploymentID := seedExecutionDeployment(t, tc, orgID, envID, digest)
	_, stepID := seedExecutionRun(t, tc, orgID, envID, deploymentID, "node-a")

	// Advertise compatibility via correct pool first (zero slots keeps it fast
	// and still persists worker_deployments).
	var advert worker.PollResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "wrong-pool-advertise", WorkerID: session.WorkerID,
		SessionID: session.SessionID, AvailableSlots: 0, DeploymentDigests: []string{digest}, Pool: "default",
	}, &advert); status != http.StatusOK {
		t.Fatalf("advertise status=%d", status)
	}

	// Wrong pool must be rejected and must not reach the claim path.
	var rejected worker.PollResponseDTO
	if status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: "wrong-pool-attempt", WorkerID: session.WorkerID,
		SessionID: session.SessionID, AvailableSlots: 1, DeploymentDigests: []string{digest}, Pool: "gpu",
	}, &rejected); status != http.StatusUnauthorized {
		t.Fatalf("wrong-pool poll status=%d, want 401", status)
	}
	var attempts, leases int
	var stepState string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM task_attempts WHERE step_id=$1::uuid),
			(SELECT count(*) FROM task_leases WHERE step_id=$1::uuid),
			(SELECT state FROM run_steps WHERE id=$1::uuid)`, stepID).Scan(&attempts, &leases, &stepState)
	}); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || leases != 0 || stepState != "READY" {
		t.Fatalf("wrong-pool poll created ownership: attempts=%d leases=%d state=%s", attempts, leases, stepState)
	}

	// Correct pool can claim the same step.
	assignment := claimExecution(t, server, session, digest, "wrong-pool-correct")
	if assignment.StepID != stepID {
		t.Fatalf("correct-pool claim did not own step: got %s want %s", assignment.StepID, stepID)
	}
}
