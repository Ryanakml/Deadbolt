package integration_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/deployment"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// workerGatewayObserver deliberately has no execution semantics. It only proves
// that authenticated/scoped gateway requests reach the Issue #12 adapter.
type workerGatewayObserver struct{ claims atomic.Int32 }

func (e *workerGatewayObserver) Claim(_ context.Context, _ *worker.WorkerSessionContext, r *worker.PollRequestDTO) (*worker.PollResponseDTO, error) {
	e.claims.Add(1)
	return &worker.PollResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: r.RequestID, Assignments: []worker.AssignmentDTO{{AttemptID: "adapter-observation"}}}, nil
}
func (e *workerGatewayObserver) Start(_ context.Context, _ *worker.WorkerSessionContext, r *worker.StartRequestDTO) (*worker.StartResponseDTO, error) {
	return &worker.StartResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: r.RequestID}, nil
}
func (e *workerGatewayObserver) Heartbeat(_ context.Context, _ *worker.WorkerSessionContext, r *worker.HeartbeatRequestDTO) (*worker.HeartbeatResponseDTO, error) {
	return &worker.HeartbeatResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: r.RequestID}, nil
}
func (e *workerGatewayObserver) Complete(_ context.Context, _ *worker.WorkerSessionContext, r *worker.CompleteRequestDTO) (*worker.CompleteResponseDTO, error) {
	return &worker.CompleteResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: r.RequestID}, nil
}
func (e *workerGatewayObserver) StopAck(_ context.Context, _ *worker.WorkerSessionContext, r *worker.StopAckRequestDTO) (*worker.AckResponseDTO, error) {
	return &worker.AckResponseDTO{ProtocolVersion: worker.ProtocolVersion, RequestID: r.RequestID}, nil
}

func workerGatewayServer(t *testing.T, tc *tenantTestContext, engine worker.ExecutionEngine) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	wh := worker.NewHTTPHandler(worker.NewService(tc.pool, deployment.NewService(tc.pool, tc.service), engine), tc.service)
	wh.RegisterRoutes(mux)
	mux.Handle("POST /api/v1/environments/{envId}/worker-enrollments", tc.handler.WithRequestID(tc.handler.RequireAuth(tc.handler.RequireOrgScope(tenant.CapDeploymentsWrite, wh.HandleCreateEnrollmentToken))))
	return httptest.NewServer(mux)
}

func workerPost(t *testing.T, url, path string, in, out any, token string) int {
	t.Helper()
	body, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, url+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil {
		_ = json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}

func TestWorkerGatewayAuthAndScopeBoundaries(t *testing.T) {
	tc := setupTenantContext(t)
	defer tc.cleanup()
	ctx := context.Background()
	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "worker-auth")
	if err != nil {
		t.Fatal(err)
	}
	project, err := tc.service.CreateProject(ctx, org.ID, "p")
	if err != nil {
		t.Fatal(err)
	}
	env, err := tc.service.CreateEnvironment(ctx, org.ID, project.ID, tenant.EnvStaging, 2)
	if err != nil {
		t.Fatal(err)
	}
	observer := &workerGatewayObserver{}
	server := workerGatewayServer(t, tc, observer)
	defer server.Close()
	admin := bootstrapTestKey(t, tc.service, org.ID, env.ID, []string{tenant.CapAdminKey, tenant.CapDeploymentsWrite})
	var enrollment worker.EnrollmentTokenInfo
	enrollmentBody, _ := json.Marshal(map[string]string{"pool": "p1"})
	enrollmentReq, _ := http.NewRequest(http.MethodPost, server.URL+"/api/v1/environments/"+env.ID+"/worker-enrollments", bytes.NewReader(enrollmentBody))
	enrollmentReq.Header.Set("Content-Type", "application/json")
	enrollmentReq.Header.Set("Authorization", "Bearer "+admin.PlaintextKey)
	enrollmentReq.Header.Set("X-Organization-ID", org.ID)
	enrollmentReq.Header.Set("Idempotency-Key", "worker-enrollment")
	enrollmentResp, err := http.DefaultClient.Do(enrollmentReq)
	if err != nil {
		t.Fatal(err)
	}
	var enrollmentError worker.ErrorEnvelopeDTO
	if enrollmentResp.StatusCode == http.StatusCreated {
		_ = json.NewDecoder(enrollmentResp.Body).Decode(&enrollment)
	} else {
		_ = json.NewDecoder(enrollmentResp.Body).Decode(&enrollmentError)
	}
	status := enrollmentResp.StatusCode
	enrollmentResp.Body.Close()
	if status != http.StatusCreated {
		t.Fatalf("enrollment token status=%d code=%s message=%s", status, enrollmentError.Code, enrollmentError.Message)
	}
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	var challenge worker.ChallengeResponseDTO
	if status := workerPost(t, server.URL, "/worker/v1/challenge", worker.ChallengeRequestDTO{ProtocolVersion: 1, RequestID: "c1", PublicKey: worker.EncodePublicKey(pub)}, &challenge, ""); status != http.StatusOK {
		t.Fatalf("challenge status=%d", status)
	}
	enrollReq := worker.EnrollRequestDTO{ProtocolVersion: 1, RequestID: "e1", EnrollmentToken: enrollment.Token, PublicKey: worker.EncodePublicKey(pub), Nonce: challenge.Nonce, Signature: worker.SignChallenge(priv, challenge.Nonce)}
	var session worker.SessionResponseDTO
	if status := workerPost(t, server.URL, "/worker/v1/enroll", enrollReq, &session, ""); status != http.StatusOK {
		t.Fatalf("enroll status=%d", status)
	}
	// Same nonce is single use; a fresh nonce with same consumed token proves token replay rejection.
	if status := workerPost(t, server.URL, "/worker/v1/enroll", enrollReq, nil, ""); status != http.StatusBadRequest {
		t.Fatalf("nonce replay status=%d", status)
	}
	var fresh worker.ChallengeResponseDTO
	_ = workerPost(t, server.URL, "/worker/v1/challenge", worker.ChallengeRequestDTO{ProtocolVersion: 1, RequestID: "c2", PublicKey: worker.EncodePublicKey(pub)}, &fresh, "")
	enrollReq.Nonce, enrollReq.Signature = fresh.Nonce, worker.SignChallenge(priv, fresh.Nonce)
	if status := workerPost(t, server.URL, "/worker/v1/enroll", enrollReq, nil, ""); status != http.StatusUnauthorized {
		t.Fatalf("token replay status=%d", status)
	}
	poll := worker.PollRequestDTO{ProtocolVersion: 1, RequestID: "p1", WorkerID: session.WorkerID, SessionID: session.SessionID, Pool: "p1", AvailableSlots: 1}
	if status := workerPost(t, server.URL, "/worker/v1/poll", poll, nil, session.SessionToken); status != http.StatusOK || observer.claims.Load() != 1 {
		t.Fatalf("scoped poll status=%d calls=%d", status, observer.claims.Load())
	}
	unavailable := workerGatewayServer(t, tc, nil)
	if status := workerPost(t, unavailable.URL, "/worker/v1/poll", poll, nil, session.SessionToken); status != http.StatusServiceUnavailable {
		t.Fatalf("nil engine status=%d", status)
	}
	unavailable.Close()
	poll.Pool = "wrong"
	if status := workerPost(t, server.URL, "/worker/v1/poll", poll, nil, session.SessionToken); status != http.StatusUnauthorized || observer.claims.Load() != 1 {
		t.Fatalf("pool mismatch delegated: status=%d calls=%d", status, observer.claims.Load())
	}
	poll.Pool = "p1"
	poll.SessionID = "wrong"
	if status := workerPost(t, server.URL, "/worker/v1/poll", poll, nil, session.SessionToken); status != http.StatusUnauthorized || observer.claims.Load() != 1 {
		t.Fatalf("identity mismatch delegated")
	}
	poll.SessionID = session.SessionID
	if err := tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, e := tx.Exec(ctx, "UPDATE workers SET status='DRAINING' WHERE id=$1::uuid", session.WorkerID)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	if status := workerPost(t, server.URL, "/worker/v1/poll", poll, nil, session.SessionToken); status != http.StatusOK || observer.claims.Load() != 1 {
		t.Fatalf("draining delegated")
	}
	if err := tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, e := tx.Exec(ctx, "UPDATE workers SET status='REVOKED' WHERE id=$1::uuid", session.WorkerID)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	if status := workerPost(t, server.URL, "/worker/v1/poll", poll, nil, session.SessionToken); status != http.StatusForbidden {
		t.Fatalf("revoked worker status=%d", status)
	}
	if err := tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, e := tx.Exec(ctx, "UPDATE workers SET status='ACTIVE' WHERE id=$1::uuid", session.WorkerID)
		if e != nil {
			return e
		}
		_, e = tx.Exec(ctx, "UPDATE worker_sessions SET revoked_at=clock_timestamp() WHERE id=$1::uuid", session.SessionID)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	if status := workerPost(t, server.URL, "/worker/v1/poll", poll, nil, session.SessionToken); status != http.StatusUnauthorized {
		t.Fatalf("revoked session status=%d", status)
	}
	if err := tc.pool.WithTenantTx(ctx, org.ID, func(ctx context.Context, tx storage.Tx) error {
		_, e := tx.Exec(ctx, "UPDATE worker_sessions SET revoked_at=NULL, expires_at=clock_timestamp()-INTERVAL '1 second' WHERE id=$1::uuid", session.SessionID)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	if status := workerPost(t, server.URL, "/worker/v1/poll", poll, nil, session.SessionToken); status != http.StatusUnauthorized {
		t.Fatalf("expired session status=%d", status)
	}
}
