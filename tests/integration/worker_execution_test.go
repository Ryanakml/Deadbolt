package integration_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// setupWorkerIntegrationTest initializes a test tenant, environment, and HTTP server.
func setupWorkerIntegrationTest(t *testing.T) (*tenantTestContext, *httptest.Server, string, string, string) {
	t.Helper()
	tc := setupTenantContext(t)
	ctx := context.Background()

	owner, _ := tenant.NewUUID()
	org, err := tc.service.CreateOrganization(ctx, owner, "Worker Integration Org")
	if err != nil {
		t.Fatalf("create org failed: %v", err)
	}

	project, err := tc.service.CreateProject(ctx, org.ID, "Worker Project")
	if err != nil {
		t.Fatalf("create project failed: %v", err)
	}

	env, err := tc.service.CreateEnvironment(ctx, org.ID, project.ID, tenant.EnvStaging, 2)
	if err != nil {
		t.Fatalf("create env failed: %v", err)
	}

	prodMux := controlplane.BuildMux(tc.authCfg, tc.runtimePool, nil, nil)
	server := httptest.NewServer(prodMux)

	return tc, server, org.ID, env.ID, owner
}

// TestWorkerEnrollmentLifecycle verifies single-use token enrollment, challenge nonces,
// Ed25519 signature validation, and replay rejection according to Blueprint §12.
func TestWorkerEnrollmentLifecycle(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	adminKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{tenant.CapDeploymentsWrite, tenant.CapWorkersDrain, tenant.CapAdminKey})

	// 1. Admin generates enrollment token
	enrollReqBody, _ := json.Marshal(map[string]any{
		"poolName": "default",
	})
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/environments/%s/worker-enrollments", server.URL, envID), bytes.NewReader(enrollReqBody))
	req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Idempotency-Key", "idemp-enroll-token-lifecycle-1")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to create enrollment token: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for enrollment token, got %d", resp.StatusCode)
	}

	var enrollTokenInfo worker.EnrollmentTokenInfo
	if err := json.NewDecoder(resp.Body).Decode(&enrollTokenInfo); err != nil {
		t.Fatalf("failed to decode enrollment token response: %v", err)
	}
	if enrollTokenInfo.Token == "" {
		t.Fatalf("empty enrollment token returned")
	}

	// 2. Worker requests challenge nonce
	pubKey, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("failed to generate ed25519 keys: %v", err)
	}
	pubHex := hex.EncodeToString(pubKey)

	chalReqBody, _ := json.Marshal(worker.ChallengeRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "test-chal-1",
		PublicKey:       pubHex,
	})
	cResp, err := http.Post(server.URL+"/worker/v1/challenge", "application/json", bytes.NewReader(chalReqBody))
	if err != nil {
		t.Fatalf("failed to request challenge nonce: %v", err)
	}
	defer cResp.Body.Close()

	if cResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for challenge nonce, got %d", cResp.StatusCode)
	}
	var chalResp worker.ChallengeResponseDTO
	if err := json.NewDecoder(cResp.Body).Decode(&chalResp); err != nil {
		t.Fatalf("failed to decode challenge response: %v", err)
	}
	if chalResp.Nonce == "" {
		t.Fatalf("empty challenge nonce returned")
	}

	// 3. Tampered signature must fail with 401
	tamperedBody, _ := json.Marshal(worker.EnrollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "test-enroll-tampered",
		EnrollmentToken: enrollTokenInfo.Token,
		Nonce:           chalResp.Nonce,
		Signature:       "baddeadbeefbaddeadbeefbaddeadbeefbaddeadbeefbaddeadbeefbaddeadbeefbaddeadbeefbaddeadbeefbaddeadbeefbaddeadbeefbaddeadbeefbaddeadbeef",
		PublicKey:       pubHex,
	})
	tResp, err := http.Post(server.URL+"/worker/v1/enroll", "application/json", bytes.NewReader(tamperedBody))
	if err != nil {
		t.Fatalf("failed to send tampered enrollment: %v", err)
	}
	defer tResp.Body.Close()
	if tResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for tampered signature, got %d", tResp.StatusCode)
	}

	// 4. Legitimate worker requests a fresh challenge nonce and enrolls
	chalReqBodyValid, _ := json.Marshal(worker.ChallengeRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "test-chal-valid",
		PublicKey:       pubHex,
	})
	cRespValid, err := http.Post(server.URL+"/worker/v1/challenge", "application/json", bytes.NewReader(chalReqBodyValid))
	if err != nil {
		t.Fatalf("failed to request fresh challenge nonce: %v", err)
	}
	var chalRespValid worker.ChallengeResponseDTO
	if err := json.NewDecoder(cRespValid.Body).Decode(&chalRespValid); err != nil {
		t.Fatalf("failed to decode fresh challenge response: %v", err)
	}
	cRespValid.Body.Close()

	validSig := worker.SignChallenge(privKey, chalRespValid.Nonce)

	validEnrollBody, _ := json.Marshal(worker.EnrollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "test-enroll-valid",
		EnrollmentToken: enrollTokenInfo.Token,
		Nonce:           chalRespValid.Nonce,
		Signature:       validSig,
		PublicKey:       pubHex,
	})
	eResp, err := http.Post(server.URL+"/worker/v1/enroll", "application/json", bytes.NewReader(validEnrollBody))
	if err != nil {
		t.Fatalf("failed to submit worker enrollment: %v", err)
	}
	defer eResp.Body.Close()

	if eResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for worker enrollment, got %d", eResp.StatusCode)
	}
	var enrollSuccess worker.SessionResponseDTO
	if err := json.NewDecoder(eResp.Body).Decode(&enrollSuccess); err != nil {
		t.Fatalf("failed to decode enroll response: %v", err)
	}
	if enrollSuccess.WorkerID == "" || enrollSuccess.SessionToken == "" {
		t.Fatalf("missing workerId or sessionToken in enroll response: %+v", enrollSuccess)
	}

	// 5. Replay attack: Reusing single-use enrollment token must fail with 401
	chalReqBody2, _ := json.Marshal(worker.ChallengeRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "test-chal-2",
		PublicKey:       pubHex,
	})
	cResp2, _ := http.Post(server.URL+"/worker/v1/challenge", "application/json", bytes.NewReader(chalReqBody2))
	var chalResp2 worker.ChallengeResponseDTO
	_ = json.NewDecoder(cResp2.Body).Decode(&chalResp2)
	cResp2.Body.Close()

	sig2 := worker.SignChallenge(privKey, chalResp2.Nonce)
	replayBody, _ := json.Marshal(worker.EnrollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "test-enroll-replay",
		EnrollmentToken: enrollTokenInfo.Token,
		Nonce:           chalResp2.Nonce,
		Signature:       sig2,
		PublicKey:       pubHex,
	})
	replayResp, err := http.Post(server.URL+"/worker/v1/enroll", "application/json", bytes.NewReader(replayBody))
	if err != nil {
		t.Fatalf("failed to post replay enrollment: %v", err)
	}
	defer replayResp.Body.Close()
	if replayResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized on enrollment token replay, got %d", replayResp.StatusCode)
	}
}

// TestWorkerSessionReauthenticationAndFencing verifies challenge-based session re-authentication
// and that reconnecting revokes old sessions/leases.
func TestWorkerSessionReauthenticationAndFencing(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	adminKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{tenant.CapDeploymentsWrite, tenant.CapWorkersDrain, tenant.CapAdminKey})

	// Generate enrollment token and enroll worker
	enrollReqBody, _ := json.Marshal(map[string]any{"poolName": "default"})
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/environments/%s/worker-enrollments", server.URL, envID), bytes.NewReader(enrollReqBody))
	req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Idempotency-Key", "idemp-enroll-token-reauth-1")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to create enrollment token: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for enrollment token, got %d", resp.StatusCode)
	}
	var enrollTokenInfo worker.EnrollmentTokenInfo
	_ = json.NewDecoder(resp.Body).Decode(&enrollTokenInfo)

	pubKey, privKey, _ := ed25519.GenerateKey(nil)
	pubHex := hex.EncodeToString(pubKey)

	chalReqBody, _ := json.Marshal(worker.ChallengeRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "chal-reauth-1",
		PublicKey:       pubHex,
	})
	cResp, err := http.Post(server.URL+"/worker/v1/challenge", "application/json", bytes.NewReader(chalReqBody))
	if err != nil {
		t.Fatalf("failed to get challenge: %v", err)
	}
	var chalResp worker.ChallengeResponseDTO
	_ = json.NewDecoder(cResp.Body).Decode(&chalResp)
	cResp.Body.Close()

	sigHex := worker.SignChallenge(privKey, chalResp.Nonce)
	enrollBody, _ := json.Marshal(worker.EnrollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "enroll-reauth-1",
		EnrollmentToken: enrollTokenInfo.Token,
		Nonce:           chalResp.Nonce,
		Signature:       sigHex,
		PublicKey:       pubHex,
	})
	eResp, err := http.Post(server.URL+"/worker/v1/enroll", "application/json", bytes.NewReader(enrollBody))
	if err != nil {
		t.Fatalf("failed to enroll: %v", err)
	}
	defer eResp.Body.Close()
	if eResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for enroll, got %d", eResp.StatusCode)
	}
	var enrollSuccess worker.SessionResponseDTO
	_ = json.NewDecoder(eResp.Body).Decode(&enrollSuccess)

	workerID := enrollSuccess.WorkerID
	oldSessionToken := enrollSuccess.SessionToken

	// 1. Worker re-authenticates via /worker/v1/session
	chalReqBody2, _ := json.Marshal(worker.ChallengeRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "chal-reauth-2",
		WorkerID:        workerID,
	})
	cResp3, _ := http.Post(server.URL+"/worker/v1/challenge", "application/json", bytes.NewReader(chalReqBody2))
	var chalResp3 worker.ChallengeResponseDTO
	_ = json.NewDecoder(cResp3.Body).Decode(&chalResp3)
	cResp3.Body.Close()

	sig3 := worker.SignChallenge(privKey, chalResp3.Nonce)
	sessReqBody, _ := json.Marshal(worker.SessionRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "sess-req-1",
		WorkerID:        workerID,
		Nonce:           chalResp3.Nonce,
		Signature:       sig3,
	})
	sResp, err := http.Post(server.URL+"/worker/v1/session", "application/json", bytes.NewReader(sessReqBody))
	if err != nil {
		t.Fatalf("failed to re-authenticate session: %v", err)
	}
	defer sResp.Body.Close()
	if sResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for session re-authentication, got %d", sResp.StatusCode)
	}
	var newSession worker.SessionResponseDTO
	if err := json.NewDecoder(sResp.Body).Decode(&newSession); err != nil {
		t.Fatalf("failed to decode new session response: %v", err)
	}
	if newSession.SessionToken == "" || newSession.SessionToken == oldSessionToken {
		t.Fatalf("expected a new distinct session token, got: %s", newSession.SessionToken)
	}

	// 2. Old session token must now be rejected with 401 Unauthorized
	pollReqBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "poll-old",
		WorkerID:        workerID,
		SessionID:       enrollSuccess.SessionID,
		AvailableSlots:  2,
		Pool:            "default",
	})
	pollReq, _ := http.NewRequest(http.MethodPost, server.URL+"/worker/v1/poll", bytes.NewReader(pollReqBody))
	pollReq.Header.Set("Authorization", "Bearer "+oldSessionToken)
	pollReq.Header.Set("Content-Type", "application/json")
	pResp, err := http.DefaultClient.Do(pollReq)
	if err != nil {
		t.Fatalf("failed to execute poll with old session: %v", err)
	}
	defer pResp.Body.Close()
	if pResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for revoked old session, got %d", pResp.StatusCode)
	}

	// 3. New session token must be accepted
	pollReqBody2, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "poll-new",
		WorkerID:        workerID,
		SessionID:       newSession.SessionID,
		AvailableSlots:  0,
		Pool:            "default",
	})
	pollReq2, _ := http.NewRequest(http.MethodPost, server.URL+"/worker/v1/poll", bytes.NewReader(pollReqBody2))
	pollReq2.Header.Set("Authorization", "Bearer "+newSession.SessionToken)
	pollReq2.Header.Set("Content-Type", "application/json")
	pResp2, err := http.DefaultClient.Do(pollReq2)
	if err != nil {
		t.Fatalf("failed to execute poll with new session: %v", err)
	}
	defer pResp2.Body.Close()
	if pResp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for new session poll, got %d", pResp2.StatusCode)
	}
}

// TestWorkerExecutionStartDeadlineAndIdempotency verifies Blueprint §13.1:
// - Start within 5s moves attempt to RUNNING.
// - Start retry idempotently returns original deadline without extending it.
func TestWorkerExecutionStartDeadlineAndIdempotency(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	ctx := context.Background()
	adminKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{tenant.CapDeploymentsWrite, tenant.CapWorkersDrain, tenant.CapAdminKey})

	// Enroll worker
	enrollReqBody, _ := json.Marshal(map[string]any{"poolName": "default"})
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/environments/%s/worker-enrollments", server.URL, envID), bytes.NewReader(enrollReqBody))
	req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Idempotency-Key", "idemp-enroll-token-start-1")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to create enrollment token: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for enrollment token, got %d", resp.StatusCode)
	}
	var enrollTokenInfo worker.EnrollmentTokenInfo
	_ = json.NewDecoder(resp.Body).Decode(&enrollTokenInfo)

	pubKey, privKey, _ := ed25519.GenerateKey(nil)
	pubHex := hex.EncodeToString(pubKey)

	chalReqBody, _ := json.Marshal(worker.ChallengeRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "chal-start-1",
		PublicKey:       pubHex,
	})
	cResp, err := http.Post(server.URL+"/worker/v1/challenge", "application/json", bytes.NewReader(chalReqBody))
	if err != nil {
		t.Fatalf("failed to get challenge: %v", err)
	}
	var chalResp worker.ChallengeResponseDTO
	_ = json.NewDecoder(cResp.Body).Decode(&chalResp)
	cResp.Body.Close()

	sigHex := worker.SignChallenge(privKey, chalResp.Nonce)
	enrollBody, _ := json.Marshal(worker.EnrollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "enroll-start-1",
		EnrollmentToken: enrollTokenInfo.Token,
		Nonce:           chalResp.Nonce,
		Signature:       sigHex,
		PublicKey:       pubHex,
	})
	eResp, err := http.Post(server.URL+"/worker/v1/enroll", "application/json", bytes.NewReader(enrollBody))
	if err != nil {
		t.Fatalf("failed to enroll: %v", err)
	}
	defer eResp.Body.Close()
	if eResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for enroll, got %d", eResp.StatusCode)
	}
	var enrollSuccess worker.SessionResponseDTO
	_ = json.NewDecoder(eResp.Body).Decode(&enrollSuccess)

	workerID := enrollSuccess.WorkerID
	sessionID := enrollSuccess.SessionID
	sessionToken := enrollSuccess.SessionToken

	// Seed database with a deployment, run, and eligible run_step
	var deploymentID, runID, stepID string
	err = tc.pool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx storage.Tx) error {
		err := tx.QueryRow(ctx, `
			INSERT INTO deployments (organization_id, environment_id, manifest_hash, bundle_digest, manifest, protocol_version, runtime_version)
			VALUES ($1, $2, 'm_hash_1', 'b_digest_1', '{}', 1, '1.0')
			RETURNING id
		`, orgID, envID).Scan(&deploymentID)
		if err != nil {
			return err
		}

		err = tx.QueryRow(ctx, `
			INSERT INTO runs (organization_id, environment_id, deployment_id, workflow_name, status)
			VALUES ($1, $2, $3, 'workflow-test', 'RUNNING')
			RETURNING id
		`, orgID, envID, deploymentID).Scan(&runID)
		if err != nil {
			return err
		}

		err = tx.QueryRow(ctx, `
			INSERT INTO run_steps (organization_id, environment_id, run_id, node_id, state, eligible_at)
			VALUES ($1, $2, $3, 'node-1', 'READY', clock_timestamp())
			RETURNING id
		`, orgID, envID, runID).Scan(&stepID)
		return err
	})
	if err != nil {
		t.Fatalf("failed to seed test execution data: %v", err)
	}

	// 1. Worker polls and claims task
	pollReqBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "poll-claim-1",
		WorkerID:          workerID,
		SessionID:         sessionID,
		AvailableSlots:    2,
		DeploymentDigests: []string{"b_digest_1"},
		Pool:              "default",
	})
	pollReq, _ := http.NewRequest(http.MethodPost, server.URL+"/worker/v1/poll", bytes.NewReader(pollReqBody))
	pollReq.Header.Set("Authorization", "Bearer "+sessionToken)
	pollReq.Header.Set("Content-Type", "application/json")
	pResp, err := http.DefaultClient.Do(pollReq)
	if err != nil {
		t.Fatalf("poll failed: %v", err)
	}
	defer pResp.Body.Close()
	if pResp.StatusCode != http.StatusOK {
		var errEnv map[string]any
		_ = json.NewDecoder(pResp.Body).Decode(&errEnv)
		t.Fatalf("expected 200 OK for poll, got %d: %+v", pResp.StatusCode, errEnv)
	}

	var pollResp worker.PollResponseDTO
	if err := json.NewDecoder(pResp.Body).Decode(&pollResp); err != nil {
		t.Fatalf("failed to decode poll response: %v", err)
	}
	if len(pollResp.Assignments) == 0 {
		t.Fatalf("expected 1 task claimed, got 0")
	}

	claimed := pollResp.Assignments[0]
	if claimed.AttemptID == "" || claimed.ClaimStartDeadlineAt == "" {
		t.Fatalf("invalid claimed task: %+v", claimed)
	}

	// 2. Start attempt within 5s deadline
	startReqBody, _ := json.Marshal(worker.StartRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "start-1",
		WorkerID:        workerID,
		SessionID:       sessionID,
		AttemptID:       claimed.AttemptID,
		OwnershipEpoch:  claimed.OwnershipEpoch,
	})
	startReq, _ := http.NewRequest(http.MethodPost, server.URL+"/worker/v1/start", bytes.NewReader(startReqBody))
	startReq.Header.Set("Authorization", "Bearer "+sessionToken)
	startReq.Header.Set("Content-Type", "application/json")
	sResp, err := http.DefaultClient.Do(startReq)
	if err != nil {
		t.Fatalf("start request failed: %v", err)
	}
	defer sResp.Body.Close()

	if sResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for start, got %d", sResp.StatusCode)
	}
	var startResp worker.StartResponseDTO
	if err := json.NewDecoder(sResp.Body).Decode(&startResp); err != nil {
		t.Fatalf("failed to decode start response: %v", err)
	}
	if !startResp.Accepted {
		t.Fatalf("expected accepted=true")
	}
	firstDeadline := startResp.AttemptDeadlineAt

	// 3. Idempotent Start retry on RUNNING attempt must return 200 OK and same deadline without extension
	startReqRetry, _ := http.NewRequest(http.MethodPost, server.URL+"/worker/v1/start", bytes.NewReader(startReqBody))
	startReqRetry.Header.Set("Authorization", "Bearer "+sessionToken)
	startReqRetry.Header.Set("Content-Type", "application/json")
	rResp, err := http.DefaultClient.Do(startReqRetry)
	if err != nil {
		t.Fatalf("start retry failed: %v", err)
	}
	defer rResp.Body.Close()

	if rResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on idempotent start retry, got %d", rResp.StatusCode)
	}
	var retryResp worker.StartResponseDTO
	_ = json.NewDecoder(rResp.Body).Decode(&retryResp)
	if !retryResp.Accepted || retryResp.AttemptDeadlineAt != firstDeadline {
		t.Fatalf("expected same deadline on retry, original: %s, retry: %s", firstDeadline, retryResp.AttemptDeadlineAt)
	}

	// 4. Heartbeat extends lease
	hbReqBody, _ := json.Marshal(worker.HeartbeatRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "hb-1",
		WorkerID:        workerID,
		SessionID:       sessionID,
		Attempts: []worker.HeartbeatAttemptDTO{
			{
				AttemptID:      claimed.AttemptID,
				OwnershipEpoch: claimed.OwnershipEpoch,
			},
		},
	})
	hbReq, _ := http.NewRequest(http.MethodPost, server.URL+"/worker/v1/heartbeat", bytes.NewReader(hbReqBody))
	hbReq.Header.Set("Authorization", "Bearer "+sessionToken)
	hbReq.Header.Set("Content-Type", "application/json")
	hResp, err := http.DefaultClient.Do(hbReq)
	if err != nil {
		t.Fatalf("heartbeat request failed: %v", err)
	}
	defer hResp.Body.Close()

	if hResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for heartbeat, got %d", hResp.StatusCode)
	}
	var hbResp worker.HeartbeatResponseDTO
	_ = json.NewDecoder(hResp.Body).Decode(&hbResp)
	if len(hbResp.Stops) > 0 {
		t.Fatalf("unexpected stop request on heartbeat")
	}

	// 5. Complete attempt with valid session
	completion := worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "comp-1",
		WorkerID:        workerID,
		SessionID:       sessionID,
		AttemptID:       claimed.AttemptID,
		OwnershipEpoch:  claimed.OwnershipEpoch,
		Outcome:         "SUCCEEDED",
	}
	completion.ResultDigest, _ = worker.CanonicalCompletionDigest(&completion)
	compReqBody, _ := json.Marshal(completion)
	compReq, _ := http.NewRequest(http.MethodPost, server.URL+"/worker/v1/complete", bytes.NewReader(compReqBody))
	compReq.Header.Set("Authorization", "Bearer "+sessionToken)
	compReq.Header.Set("Content-Type", "application/json")
	compResp, err := http.DefaultClient.Do(compReq)
	if err != nil {
		t.Fatalf("complete request failed: %v", err)
	}
	defer compResp.Body.Close()

	if compResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for complete, got %d", compResp.StatusCode)
	}

	// 6. An identical Complete retry is idempotently acknowledged even though
	// the lease was released by the first commit.
	compReq2, _ := http.NewRequest(http.MethodPost, server.URL+"/worker/v1/complete", bytes.NewReader(compReqBody))
	compReq2.Header.Set("Authorization", "Bearer "+sessionToken)
	compReq2.Header.Set("Content-Type", "application/json")
	compResp2, err := http.DefaultClient.Do(compReq2)
	if err != nil {
		t.Fatalf("stale complete failed: %v", err)
	}
	defer compResp2.Body.Close()
	if compResp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for identical Complete retry, got %d", compResp2.StatusCode)
	}

	// 7. A different digest for the same terminal attempt is a conflict.
	conflictingBody, _ := json.Marshal(worker.CompleteRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "comp-conflict",
		WorkerID:        workerID,
		SessionID:       sessionID,
		AttemptID:       claimed.AttemptID,
		OwnershipEpoch:  claimed.OwnershipEpoch,
		Outcome:         "SUCCEEDED",
		ResultDigest:    "sha256:different-outcome-digest",
	})
	conflictingReq, _ := http.NewRequest(http.MethodPost, server.URL+"/worker/v1/complete", bytes.NewReader(conflictingBody))
	conflictingReq.Header.Set("Authorization", "Bearer "+sessionToken)
	conflictingReq.Header.Set("Content-Type", "application/json")
	conflictingResp, err := http.DefaultClient.Do(conflictingReq)
	if err != nil {
		t.Fatalf("conflicting complete failed: %v", err)
	}
	defer conflictingResp.Body.Close()
	if conflictingResp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for different Complete digest, got %d", conflictingResp.StatusCode)
	}
}

// TestWorkerRevocationAndDraining verifies Blueprint §12.2 admin revocation and draining.
func TestWorkerRevocationAndDraining(t *testing.T) {
	tc, server, orgID, envID, _ := setupWorkerIntegrationTest(t)
	defer tc.cleanup()
	defer server.Close()

	adminKey := bootstrapTestKey(t, tc.service, orgID, envID, []string{tenant.CapDeploymentsWrite, tenant.CapWorkersDrain, tenant.CapAdminKey})

	// Enroll worker
	enrollReqBody, _ := json.Marshal(map[string]any{"poolName": "default"})
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/environments/%s/worker-enrollments", server.URL, envID), bytes.NewReader(enrollReqBody))
	req.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	req.Header.Set("X-Organization-ID", orgID)
	req.Header.Set("Idempotency-Key", "idemp-enroll-token-drain-1")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to create enrollment token: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created for enrollment token, got %d", resp.StatusCode)
	}
	var enrollTokenInfo worker.EnrollmentTokenInfo
	_ = json.NewDecoder(resp.Body).Decode(&enrollTokenInfo)

	pubKey, privKey, _ := ed25519.GenerateKey(nil)
	pubHex := hex.EncodeToString(pubKey)

	chalReqBody, _ := json.Marshal(worker.ChallengeRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "chal-drain-1",
		PublicKey:       pubHex,
	})
	cResp, err := http.Post(server.URL+"/worker/v1/challenge", "application/json", bytes.NewReader(chalReqBody))
	if err != nil {
		t.Fatalf("failed to get challenge: %v", err)
	}
	var chalResp worker.ChallengeResponseDTO
	_ = json.NewDecoder(cResp.Body).Decode(&chalResp)
	cResp.Body.Close()

	sigHex := worker.SignChallenge(privKey, chalResp.Nonce)
	enrollBody, _ := json.Marshal(worker.EnrollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "enroll-drain-1",
		EnrollmentToken: enrollTokenInfo.Token,
		Nonce:           chalResp.Nonce,
		Signature:       sigHex,
		PublicKey:       pubHex,
	})
	eResp, err := http.Post(server.URL+"/worker/v1/enroll", "application/json", bytes.NewReader(enrollBody))
	if err != nil {
		t.Fatalf("failed to enroll: %v", err)
	}
	defer eResp.Body.Close()
	if eResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for enroll, got %d", eResp.StatusCode)
	}
	var enrollSuccess worker.SessionResponseDTO
	_ = json.NewDecoder(eResp.Body).Decode(&enrollSuccess)

	workerID := enrollSuccess.WorkerID
	sessionID := enrollSuccess.SessionID
	sessionToken := enrollSuccess.SessionToken

	// 1. Drain worker
	drainReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/workers/%s/drain", server.URL, workerID), nil)
	drainReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	drainReq.Header.Set("X-Organization-ID", orgID)
	drainReq.Header.Set("Idempotency-Key", "idemp-drain-worker-1")
	dResp, err := http.DefaultClient.Do(drainReq)
	if err != nil {
		t.Fatalf("failed to drain worker: %v", err)
	}
	defer dResp.Body.Close()
	if dResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for drain worker, got %d", dResp.StatusCode)
	}

	// 2. Revoke worker
	revokeReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/api/v1/workers/%s/revoke", server.URL, workerID), nil)
	revokeReq.Header.Set("Authorization", "Bearer "+adminKey.PlaintextKey)
	revokeReq.Header.Set("X-Organization-ID", orgID)
	revokeReq.Header.Set("Idempotency-Key", "idemp-revoke-worker-1")
	rResp, err := http.DefaultClient.Do(revokeReq)
	if err != nil {
		t.Fatalf("failed to revoke worker: %v", err)
	}
	defer rResp.Body.Close()
	if rResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for revoke worker, got %d", rResp.StatusCode)
	}

	// 3. Worker subsequent poll must fail with 403 Forbidden (WORKER_REVOKED)
	pollReqBody, _ := json.Marshal(worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion,
		RequestID:       "poll-after-revoke",
		WorkerID:        workerID,
		SessionID:       sessionID,
		AvailableSlots:  2,
		Pool:            "default",
	})
	pollReq, _ := http.NewRequest(http.MethodPost, server.URL+"/worker/v1/poll", bytes.NewReader(pollReqBody))
	pollReq.Header.Set("Authorization", "Bearer "+sessionToken)
	pollReq.Header.Set("Content-Type", "application/json")
	pResp, err := http.DefaultClient.Do(pollReq)
	if err != nil {
		t.Fatalf("poll after revoke failed: %v", err)
	}
	defer pResp.Body.Close()
	if pResp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden on revoked worker poll, got %d", pResp.StatusCode)
	}
}
