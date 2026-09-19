package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

// TestWorkerAdvertisementReplacesCurrentSet proves the F-11 stale-advertisement
// fix at the worker poll boundary. A poll's DeploymentDigests is the session's
// CURRENT advertised set, not an append-only history.
func TestWorkerAdvertisementReplacesCurrentSet(t *testing.T) {
	tc, server, orgID, envID, _ := setupRunLifecycleTest(t)
	defer tc.cleanup()
	defer server.Close()

	sessionA, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "advertise-replace-a")
	sessionB, _ := enrollExecutionWorker(t, tc, server, orgID, envID, "advertise-replace-b")

	v1 := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	v2 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	assertAdvertised := func(session *testWorkerSession, want ...string) {
		t.Helper()
		got := listAdvertisedDigests(t, tc, orgID, session.SessionID)
		sort.Strings(got)
		sortedWant := append([]string(nil), want...)
		sort.Strings(sortedWant)
		if len(got) != len(sortedWant) {
			t.Fatalf("session %s advertised %v, want %v", session.SessionID, got, sortedWant)
		}
		for i := range got {
			if got[i] != sortedWant[i] {
				t.Fatalf("session %s advertised %v, want %v", session.SessionID, got, sortedWant)
			}
		}
	}

	// Session A advertises V1; persisted compatibility must contain V1.
	pollAdvertisement(t, server, sessionA, []string{v1}, "advertise-a-v1")
	assertAdvertised(sessionA, v1)

	// Duplicate/current advertisements remain idempotent (no duplicate rows, no error).
	pollAdvertisement(t, server, sessionA, []string{v1, v1}, "advertise-a-v1-dup")
	assertAdvertised(sessionA, v1)

	// Another session's advertisements are independent.
	pollAdvertisement(t, server, sessionB, []string{v1}, "advertise-b-v1")
	assertAdvertised(sessionB, v1)

	// Same session next advertises only V2: V1 must be gone, V2 present.
	pollAdvertisement(t, server, sessionA, []string{v2}, "advertise-a-v2")
	assertAdvertised(sessionA, v2)
	// Other session is not affected by A's replacement.
	assertAdvertised(sessionB, v1)

	// Empty advertisement clears current deployment compatibility.
	pollAdvertisement(t, server, sessionA, []string{}, "advertise-a-empty")
	assertAdvertised(sessionA)
	assertAdvertised(sessionB, v1)

	// Nil advertisement behaves like empty (clears).
	pollAdvertisement(t, server, sessionB, nil, "advertise-b-nil")
	assertAdvertised(sessionB)
}

func pollAdvertisement(t *testing.T, server *httptest.Server, session *testWorkerSession, digests []string, requestID string) {
	t.Helper()
	var out worker.PollResponseDTO
	status := postWorkerJSON(t, server, "/worker/v1/poll", session.SessionToken, worker.PollRequestDTO{
		ProtocolVersion: worker.ProtocolVersion, RequestID: requestID, WorkerID: session.WorkerID,
		SessionID: session.SessionID, AvailableSlots: 0, DeploymentDigests: digests, Pool: "default",
	}, &out)
	if status != http.StatusOK {
		t.Fatalf("poll %s: status %d", requestID, status)
	}
	if len(out.Assignments) != 0 {
		t.Fatalf("poll %s with zero slots returned assignments: %+v", requestID, out.Assignments)
	}
}

func listAdvertisedDigests(t *testing.T, tc *tenantTestContext, orgID, sessionID string) []string {
	t.Helper()
	var digests []string
	if err := tc.pool.WithTenantTx(context.Background(), orgID, func(ctx context.Context, tx storage.Tx) error {
		rows, err := tx.Query(ctx, `SELECT bundle_digest FROM worker_deployments WHERE session_id=$1::uuid AND organization_id=$2::uuid`, sessionID, orgID)
		if err != nil {
			return err
		}
		defer rows.Close()
		digests = nil
		for rows.Next() {
			var digest string
			if err := rows.Scan(&digest); err != nil {
				return err
			}
			digests = append(digests, digest)
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	return digests
}
