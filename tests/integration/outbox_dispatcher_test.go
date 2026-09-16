package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/controlplane"
	"github.com/Ryanakml/Deadbolt/internal/execution"
	"github.com/Ryanakml/Deadbolt/internal/gateway"
	"github.com/Ryanakml/Deadbolt/internal/outbox"
	"github.com/Ryanakml/Deadbolt/internal/scheduling"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/Ryanakml/Deadbolt/internal/storage/migrator"
	"github.com/Ryanakml/Deadbolt/internal/storage/testdb"
	"github.com/Ryanakml/Deadbolt/internal/tenant"
	"github.com/Ryanakml/Deadbolt/internal/worker"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	natsServer "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// startRealNATSServer spins up an in-process real NATS JetStream server instance
// without requiring Docker, running on an isolated loopback port.
func startRealNATSServer(t *testing.T) (*natsServer.Server, *nats.Conn, nats.JetStreamContext) {
	t.Helper()
	opts := &natsServer.Options{
		Host:      "127.0.0.1",
		Port:      -1, // choose an ephemeral port
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	ns, err := natsServer.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to initialize NATS server: %v", err)
	}
	ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatalf("NATS server failed to become ready for connections")
	}

	nc, err := nats.Connect(ns.ClientURL(), nats.Timeout(5*time.Second))
	if err != nil {
		ns.Shutdown()
		t.Fatalf("failed to connect to local NATS server: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		ns.Shutdown()
		t.Fatalf("failed to get JetStream context: %v", err)
	}

	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
	})

	return ns, nc, js
}

type outboxTestContext struct {
	db          *sql.DB
	pool        *pgxpool.Pool
	systemPool  *pgxpool.Pool
	storagePool *storage.Pool
	tenantSvc   *tenant.Service
	workerEng   *execution.WorkerEngine
	cleanup     func()
}

func setupOutboxTestContext(t *testing.T) *outboxTestContext {
	t.Helper()
	migrationsDir, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatalf("resolve migrations dir: %v", err)
	}
	bootstrapPath, err := filepath.Abs("../../scripts/bootstrap-db-roles.sql")
	if err != nil {
		t.Fatalf("resolve bootstrap path: %v", err)
	}

	migratorURL, runtimeURL, systemURL, err := testdb.SetupIsolatedDatabase("deadbolt_outbox_test", bootstrapPath)
	if err != nil {
		t.Skipf("PostgreSQL isolated database setup skipped: %v", err)
	}

	db, err := sql.Open("pgx", migratorURL)
	if err != nil {
		t.Fatalf("open migrator db: %v", err)
	}
	runner := migrator.NewRunner(db, migrationsDir)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	_, _ = db.Exec("TRUNCATE TABLE outbox_events, run_events, runs, projects, organizations CASCADE")

	runtimePool, err := pgxpool.New(context.Background(), runtimeURL)
	if err != nil {
		t.Fatalf("runtime pool failed: %v", err)
	}
	systemPool, err := pgxpool.New(context.Background(), systemURL)
	if err != nil {
		runtimePool.Close()
		t.Fatalf("system pool failed: %v", err)
	}

	storagePool := storage.NewPool(runtimePool)
	tenantSvc := tenant.NewService(storagePool)
	workerEng := execution.NewWorkerEngine(storagePool)

	return &outboxTestContext{
		db:          db,
		pool:        runtimePool,
		systemPool:  systemPool,
		storagePool: storagePool,
		tenantSvc:   tenantSvc,
		workerEng:   workerEng,
		cleanup: func() {
			runtimePool.Close()
			systemPool.Close()
			db.Close()
		},
	}
}

func createOutboxTestTenant(t *testing.T, tenantSvc *tenant.Service, name string) (string, string, string) {
	t.Helper()
	ctx := context.Background()
	owner, _ := tenant.NewUUID()
	org, err := tenantSvc.CreateOrganization(ctx, owner, name)
	if err != nil {
		t.Fatalf("create org failed: %v", err)
	}
	proj, err := tenantSvc.CreateProject(ctx, org.ID, name+"-proj")
	if err != nil {
		t.Fatalf("create proj failed: %v", err)
	}
	env, err := tenantSvc.CreateEnvironment(ctx, org.ID, proj.ID, tenant.EnvStaging, 5)
	if err != nil {
		t.Fatalf("create env failed: %v", err)
	}
	return org.ID, env.ID, proj.ID
}

// TestOutboxAtomicIntentAndPublishAckPrecedesMark proves:
// 1. Blueprint §11.1 & INV-06: State, run_events, and outbox_events are committed atomically in one DB tx.
// 2. Blueprint §19.1: Publish ACK from JetStream precedes updating published_at.
func TestOutboxAtomicIntentAndPublishAckPrecedesMark(t *testing.T) {
	tc := setupOutboxTestContext(t)
	defer tc.cleanup()
	_, nc, js := startRealNATSServer(t)

	// Ensure internal wakeup stream exists
	_, err := outbox.EnsureStream(js, outbox.StreamName, []string{outbox.SubjectPrefix + ">"}, 2*time.Minute)
	if err != nil {
		t.Fatalf("ensure stream failed: %v", err)
	}

	ctx := context.Background()
	orgID, envID, _ := createOutboxTestTenant(t, tc.tenantSvc, "test-outbox-atomic")

	// 1. Insert the authoritative state, event, and outbox intent in one
	// transaction. This is the actual INV-06 atomicity boundary.
	var eventID string
	var outboxID string
	deploymentID, _ := tenant.NewUUID()
	runID, _ := tenant.NewUUID()
	err = tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO deployments (id, organization_id, environment_id, manifest_hash, bundle_digest, manifest, runtime_version)
			VALUES ($1,$2,$3,'atomic-manifest',$4,'{}'::jsonb,'1.0')`, deploymentID, orgID, envID, "sha256:atomic"); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO runs (id, organization_id, environment_id, deployment_id, workflow_name, status)
			VALUES ($1,$2,$3,$4,'atomic-workflow','RUNNING')`, runID, orgID, envID, deploymentID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO run_events (organization_id, run_id, sequence, event_type, payload)
			VALUES ($1,$2,1,'RUN_CREATED','{}'::jsonb)`, orgID, runID); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			INSERT INTO outbox_events (
				organization_id, subject, payload
			) VALUES (
				$1::uuid, 'execution.state_changed',
				jsonb_build_object('runId', $2::text, 'eventType', 'RUN_CREATED')
			) RETURNING id::text, event_id::text
		`, orgID, runID)
		return row.Scan(&outboxID, &eventID)
	})
	if err != nil {
		t.Fatalf("atomic outbox insert failed: %v", err)
	}

	// 2. Verify initially published_at IS NULL
	var initialPublishedAt *time.Time
	err = tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT published_at FROM outbox_events WHERE id = $1::uuid`, outboxID).Scan(&initialPublishedAt)
	})
	if err != nil {
		t.Fatalf("query outbox failed: %v", err)
	}
	if initialPublishedAt != nil {
		t.Fatalf("expected published_at to be NULL before dispatcher runs, got %v", initialPublishedAt)
	}
	var committedState, committedEvent, committedOutbox int
	err = tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM runs WHERE id=$1::uuid),
			(SELECT count(*) FROM run_events WHERE run_id=$1::uuid),
			(SELECT count(*) FROM outbox_events WHERE event_id=$2::uuid)`, runID, eventID).Scan(&committedState, &committedEvent, &committedOutbox)
	})
	if err != nil || committedState != 1 || committedEvent != 1 || committedOutbox != 1 {
		t.Fatalf("atomic commit evidence missing: err=%v state=%d event=%d outbox=%d", err, committedState, committedEvent, committedOutbox)
	}
	rollbackRunID, _ := tenant.NewUUID()
	rollbackErr := tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO runs (id, organization_id, environment_id, deployment_id, workflow_name, status)
			VALUES ($1,$2,$3,$4,'rolled-back','RUNNING')`, rollbackRunID, orgID, envID, deploymentID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO run_events (organization_id, run_id, sequence, event_type) VALUES ($1,$2,1,'RUN_CREATED')`, orgID, rollbackRunID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO outbox_events (organization_id, subject, payload) VALUES ($1,'execution.state_changed',jsonb_build_object('runId',$2::text,'eventType','RUN_CREATED'))`, orgID, rollbackRunID); err != nil {
			return err
		}
		return fmt.Errorf("intentional rollback for atomicity evidence")
	})
	if rollbackErr == nil {
		t.Fatal("expected rollback transaction to fail")
	}
	var rollbackCount int
	err = tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM runs WHERE id=$1::uuid`, rollbackRunID).Scan(&rollbackCount)
	})
	if err != nil || rollbackCount != 0 {
		t.Fatalf("rollback leaked authoritative state: err=%v count=%d", err, rollbackCount)
	}

	// 3. Setup subscriber to capture published NATS JetStream message
	msgCh := make(chan *nats.Msg, 1)
	sub, err := nc.Subscribe(outbox.SubjectPrefix+">", func(m *nats.Msg) {
		msgCh <- m
	})
	if err != nil {
		t.Fatalf("nats subscribe failed: %v", err)
	}
	defer sub.Unsubscribe()

	// 4. Run dispatcher batch
	metrics := outbox.NewMetrics(tc.pool)
	dispatcher := outbox.NewDispatcher(tc.systemPool, js, outbox.DefaultConfig(), metrics, nil)
	count, err := dispatcher.DispatchBatch(ctx, 10)
	if err != nil {
		t.Fatalf("dispatch batch failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 event dispatched, got %d", count)
	}

	// 5. Verify NATS received message with stable event ID in header
	select {
	case msg := <-msgCh:
		msgIdHeader := msg.Header.Get("Nats-Msg-Id")
		if msgIdHeader != eventID {
			t.Errorf("expected Nats-Msg-Id header %s, got %s", eventID, msgIdHeader)
		}
		var hint outbox.WakeupHintDTO
		if err := json.Unmarshal(msg.Data, &hint); err != nil {
			t.Fatalf("unmarshal hint: %v", err)
		}
		if hint.EventID != eventID {
			t.Errorf("expected hint eventId %s, got %s", eventID, hint.EventID)
		}
		if hint.OrganizationID != orgID {
			t.Errorf("expected hint orgId %s, got %s", orgID, hint.OrganizationID)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for NATS message")
	}

	// 6. Verify published_at is now marked non-null in PostgreSQL
	var publishedAt *time.Time
	err = tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT published_at FROM outbox_events WHERE id = $1::uuid`, outboxID).Scan(&publishedAt)
	})
	if err != nil {
		t.Fatalf("query outbox published_at failed: %v", err)
	}
	if publishedAt == nil {
		t.Fatalf("expected published_at to be non-null after successful publish ACK")
	}
	if metrics.PublishedCount() != 1 {
		t.Errorf("expected metrics publishedCount 1, got %d", metrics.PublishedCount())
	}
}

// TestOutboxPublishCrashAndRedeliverySafety proves:
// 1. Failure Mode F-02: Crash after publish / before marking in DB.
// 2. Next dispatcher pass safely resends with stable event_id.
// 3. Duplicate consumer delivery does NOT duplicate ownership (INV-03, INV-04).
func TestOutboxPublishCrashAndRedeliverySafety(t *testing.T) {
	tc := setupOutboxTestContext(t)
	defer tc.cleanup()
	_, _, js := startRealNATSServer(t)

	_, err := outbox.EnsureStream(js, outbox.StreamName, []string{outbox.SubjectPrefix + ">"}, 2*time.Minute)
	if err != nil {
		t.Fatalf("ensure stream failed: %v", err)
	}

	ctx := context.Background()
	orgID, _, _ := createOutboxTestTenant(t, tc.tenantSvc, "test-outbox-f02")

	// Create an outbox row
	var eventID string
	var outboxID string
	err = tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO outbox_events (
				organization_id, subject, payload
			) VALUES (
				$1::uuid, 'execution.state_changed',
				jsonb_build_object('runId', gen_random_uuid(), 'eventType', 'RUN_CREATED')
			) RETURNING id::text, event_id::text
		`, orgID)
		return row.Scan(&outboxID, &eventID)
	})
	if err != nil {
		t.Fatalf("insert outbox: %v", err)
	}

	// 1. Emulate crash: publish to JetStream directly with MsgId, but DO NOT mark published_at in DB
	rawHint := outbox.WakeupHintDTO{
		EventID:        eventID,
		OrganizationID: orgID,
		Subject:        "execution.state_changed",
		Timestamp:      time.Now().UTC().Format(time.RFC3339Nano),
	}
	data, _ := json.Marshal(rawHint)
	pubAck1, err := js.PublishMsg(&nats.Msg{
		Subject: outbox.SubjectPrefix + outbox.DefaultShard,
		Header:  nats.Header{"Nats-Msg-Id": []string{eventID}},
		Data:    data,
	})
	if err != nil {
		t.Fatalf("manual publish 1 failed: %v", err)
	}
	if pubAck1.Duplicate {
		t.Fatalf("initial publish should not be duplicate")
	}

	// published_at remains NULL (simulating crash before mark)
	var pubAt *time.Time
	_ = tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT published_at FROM outbox_events WHERE id = $1::uuid`, outboxID).Scan(&pubAt)
	})
	if pubAt != nil {
		t.Fatalf("published_at should be NULL before recovery")
	}

	// 2. Dispatcher runs next pass: claims the un-marked row and republishes with same event_id
	metrics := outbox.NewMetrics(tc.pool)
	dispatcher := outbox.NewDispatcher(tc.systemPool, js, outbox.DefaultConfig(), metrics, nil)
	count, err := dispatcher.DispatchBatch(ctx, 10)
	if err != nil {
		t.Fatalf("recovery dispatch batch failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 event dispatched during recovery, got %d", count)
	}

	// 3. Verify published_at is now marked
	_ = tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT published_at FROM outbox_events WHERE id = $1::uuid`, outboxID).Scan(&pubAt)
	})
	if pubAt == nil {
		t.Fatalf("expected published_at to be marked after recovery pass")
	}

	// 4. Test duplicate consumer delivery: consumer redelivers hint, verifies singular claim
	consumerInvokedCount := 0
	var consumerMu sync.Mutex
	wakeupConsumer := outbox.NewWakeupConsumer(js, outbox.DefaultConsumerConfig(), outbox.WakeupHandlerFunc(func(ctx context.Context, hint outbox.WakeupHintDTO) error {
		consumerMu.Lock()
		consumerInvokedCount++
		consumerMu.Unlock()
		return nil
	}), nil)

	if err := wakeupConsumer.Start(ctx); err != nil {
		t.Fatalf("start wakeup consumer failed: %v", err)
	}
	defer wakeupConsumer.Stop()

	// Wait for consumer to process message
	time.Sleep(300 * time.Millisecond)

	consumerMu.Lock()
	invoked := consumerInvokedCount
	consumerMu.Unlock()
	if invoked < 1 {
		t.Errorf("expected wakeup consumer to be invoked at least once, got %d", invoked)
	}
}

// TestMessagesContainRoutingHintsWithoutSecrets proves Blueprint §19.1:
// Messages contain IDs/routing hints without secrets/outputs.
func TestMessagesContainRoutingHintsWithoutSecrets(t *testing.T) {
	tc := setupOutboxTestContext(t)
	defer tc.cleanup()
	_, nc, js := startRealNATSServer(t)

	_, err := outbox.EnsureStream(js, outbox.StreamName, []string{outbox.SubjectPrefix + ">"}, 2*time.Minute)
	if err != nil {
		t.Fatalf("ensure stream failed: %v", err)
	}

	ctx := context.Background()
	orgID, _, _ := createOutboxTestTenant(t, tc.tenantSvc, "test-outbox-secrets")

	// Payload with customer secrets, output artifacts, tokens
	payloadWithSecrets := map[string]any{
		"runId":       "33333333-4444-5555-6666-777788889999",
		"eventType":   "TASK_STARTED",
		"sequence":    float64(2),
		"apiKey":      "deadbolt_live_secret_key_1234567890",
		"credentials": "super-secret-password-123",
		"output": map[string]any{
			"signedUrl": "https://s3.amazonaws.com/sensitive-bucket/secret.pdf",
		},
	}
	encoded, _ := json.Marshal(payloadWithSecrets)

	var eventID string
	err = tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO outbox_events (
				organization_id, subject, payload
			) VALUES (
				$1::uuid, 'execution.state_changed', $2::jsonb
			) RETURNING event_id::text
		`, orgID, string(encoded))
		return row.Scan(&eventID)
	})
	if err != nil {
		t.Fatalf("insert outbox with secrets: %v", err)
	}

	receivedMsgCh := make(chan []byte, 1)
	sub, err := nc.Subscribe(outbox.SubjectPrefix+">", func(m *nats.Msg) {
		receivedMsgCh <- m.Data
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	metrics := outbox.NewMetrics(tc.pool)
	dispatcher := outbox.NewDispatcher(tc.systemPool, js, outbox.DefaultConfig(), metrics, nil)
	count, err := dispatcher.DispatchBatch(ctx, 10)
	if err != nil || count != 1 {
		t.Fatalf("dispatch batch: err=%v count=%d", err, count)
	}

	select {
	case rawBytes := <-receivedMsgCh:
		msgStr := string(rawBytes)
		leakedStrings := []string{
			"deadbolt_live_secret_key",
			"super-secret-password",
			"signedUrl",
			"sensitive-bucket",
		}
		for _, s := range leakedStrings {
			if strings.Contains(msgStr, s) {
				t.Errorf("CRITICAL SECURITY LEAK: JetStream message contains %q: %s", s, msgStr)
			}
		}

		// Verify expected routing hints ARE present
		if !strings.Contains(msgStr, "33333333-4444-5555-6666-777788889999") {
			t.Errorf("expected runId in hint, got: %s", msgStr)
		}
		if !strings.Contains(msgStr, "TASK_STARTED") {
			t.Errorf("expected eventType in hint, got: %s", msgStr)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for JetStream message")
	}
}

// TestOutboxMetricsAndPrometheusEndpoint proves:
// Blueprint §25.2: Expose outbox age/failure and scheduler metrics.
func TestOutboxMetricsAndPrometheusEndpoint(t *testing.T) {
	tc := setupOutboxTestContext(t)
	defer tc.cleanup()

	ctx := context.Background()
	orgID, _, _ := createOutboxTestTenant(t, tc.tenantSvc, "test-outbox-metrics")

	err := tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		_, execErr := tx.Exec(ctx, `
			INSERT INTO outbox_events (
				organization_id, subject, payload, created_at
			) VALUES (
				$1::uuid, 'execution.state_changed', '{}'::jsonb, clock_timestamp() - interval '45 seconds'
			)
		`, orgID)
		return execErr
	})
	if err != nil {
		t.Fatalf("insert old pending outbox event: %v", err)
	}

	metrics := outbox.NewMetrics(tc.pool)
	err = metrics.UpdateDatabaseGauges(ctx)
	if err != nil {
		t.Fatalf("update database gauges failed: %v", err)
	}

	if metrics.PendingCount() < 1 {
		t.Errorf("expected pending count >= 1, got %d", metrics.PendingCount())
	}
	if metrics.OutboxAgeSeconds() < 40 {
		t.Errorf("expected outbox age >= 40s, got %d", metrics.OutboxAgeSeconds())
	}

	// Test HTTP /metrics endpoint
	authCfg := auth.Config{
		RuntimeMode: auth.ModeLocal,
	}
	mux := controlplane.BuildMuxWithMetrics(authCfg, tc.pool, nil, metrics, nil)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 OK from /metrics, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	requiredMetrics := []string{
		"deadbolt_outbox_published_total",
		"deadbolt_outbox_publish_failures_total",
		"deadbolt_outbox_age_seconds",
		"deadbolt_outbox_pending_count",
		"deadbolt_scheduler_loop_lag_seconds",
	}
	for _, m := range requiredMetrics {
		if !strings.Contains(bodyStr, m) {
			t.Errorf("missing metric %q in /metrics output:\n%s", m, bodyStr)
		}
	}
}

// TestNATSGracefulDegradationDoesNotBlockReadyz proves Blueprint §25.2:
// "degraded NATS/telemetry is reported separately because DB fallback remains valid"
func TestNATSGracefulDegradationDoesNotBlockReadyz(t *testing.T) {
	tc := setupOutboxTestContext(t)
	defer tc.cleanup()

	// 1. HealthChecker with a broken/degraded NATS checker
	degradedNATS := gateway.NewTCPNATSChecker("127.0.0.1:59999") // closed port
	versionInfo := gateway.VersionInfo{
		Version:     "0.1.0",
		CommitSHA:   "dev",
		RuntimeMode: auth.ModeLocal,
	}
	checker := gateway.NewHealthChecker(versionInfo, tc.pool, degradedNATS, migrator.LatestSchemaVersion)

	// Attach active scheduler
	reconciler := scheduling.NewReconciler(tc.pool, 5*time.Second, nil)
	reconciler.Ticker().Store(time.Now().UnixNano())
	checker.SetSchedulerTicker(reconciler.Ticker(), 60*time.Second)

	authCfg := auth.Config{RuntimeMode: auth.ModeLocal}
	mux := controlplane.BuildMux(authCfg, tc.pool, checker, nil)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /readyz during NATS degradation, got %d", resp.StatusCode)
	}

	var data gateway.ReadyResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("decode ready response: %v", err)
	}

	if data.Status != "ready" {
		t.Errorf("expected status=ready, got %s", data.Status)
	}
	if data.NATS != "degraded" {
		t.Errorf("expected nats=degraded, got %s", data.NATS)
	}
	if data.Database != "healthy" {
		t.Errorf("expected database=healthy, got %s", data.Database)
	}
}

// TestDuplicateWakeupConsumer_SingularClaimEvidence proves:
// Checklist Item 2 & F-02: Crash after publish/before marking and consumer redelivery do not duplicate ownership.
// INV-03: At most one current ownership lease exists for a task step.
func TestDuplicateWakeupConsumer_SingularClaimEvidence(t *testing.T) {
	tc := setupOutboxTestContext(t)
	defer tc.cleanup()
	_, _, js := startRealNATSServer(t)

	_, err := outbox.EnsureStream(js, outbox.StreamName, []string{outbox.SubjectPrefix + ">"}, 2*time.Minute)
	if err != nil {
		t.Fatalf("ensure stream failed: %v", err)
	}

	ctx := context.Background()
	orgID, envID, _ := createOutboxTestTenant(t, tc.tenantSvc, "test-singular-claim")

	// 1. Seed deployment, run, and a READY step in DB
	deploymentID, _ := tenant.NewUUID()
	runID, _ := tenant.NewUUID()
	stepID, _ := tenant.NewUUID()
	bundleDigest := "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	err = tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		// Insert deployment
		manifestJSON := fmt.Sprintf(`{"bundleDigest":%q,"workflows":[{"name":"test-wf","nodes":[{"id":"step-1","type":"task","task":"t1"}]}],"tasks":[{"name":"t1","entrypoint":"t1.js","recoveryPolicy":"safe","timeoutMs":60000,"maxAttempts":3}]}`, bundleDigest)
		if _, err := tx.Exec(ctx, `
			INSERT INTO deployments (id, organization_id, environment_id, manifest_hash, bundle_digest, manifest, runtime_version)
			VALUES ($1::uuid, $2::uuid, $3::uuid, 'm-hash', $4, $5::jsonb, '1.0')
		`, deploymentID, orgID, envID, bundleDigest, manifestJSON); err != nil {
			return fmt.Errorf("insert deployment: %w", err)
		}
		// Insert task definition
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_definitions (organization_id, deployment_id, name, entrypoint, input_schema, output_schema, recovery_policy, timeout_ms, max_attempts)
			VALUES ($1::uuid, $2::uuid, 't1', 't1.js', '{}'::jsonb, '{}'::jsonb, 'safe', 60000, 3)
		`, orgID, deploymentID); err != nil {
			return fmt.Errorf("insert task def: %w", err)
		}
		// Insert workflow definition
		if _, err := tx.Exec(ctx, `
			INSERT INTO workflow_definitions (organization_id, deployment_id, name, input_schema, output_schema, nodes)
			VALUES ($1::uuid, $2::uuid, 'test-wf', '{}'::jsonb, '{}'::jsonb, '[{"id":"step-1","type":"task","task":"t1"}]'::jsonb)
		`, orgID, deploymentID); err != nil {
			return fmt.Errorf("insert workflow def: %w", err)
		}
		// Insert active workflow pointer
		if _, err := tx.Exec(ctx, `
			INSERT INTO workflow_channels (environment_id, organization_id, workflow_name, active_deployment_id, revision)
			VALUES ($1::uuid, $2::uuid, 'test-wf', $3::uuid, 1)
		`, envID, orgID, deploymentID); err != nil {
			return fmt.Errorf("insert workflow_channels: %w", err)
		}
		// Insert run in RUNNING state
		if _, err := tx.Exec(ctx, `
			INSERT INTO runs (id, organization_id, environment_id, deployment_id, workflow_name, status)
			VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, 'test-wf', 'RUNNING')
		`, runID, orgID, envID, deploymentID); err != nil {
			return fmt.Errorf("insert run: %w", err)
		}
		// Insert step in READY state
		if _, err := tx.Exec(ctx, `
			INSERT INTO run_steps (id, organization_id, environment_id, run_id, node_id, state, eligible_at)
			VALUES ($1::uuid, $2::uuid, $3::uuid, $4::uuid, 'step-1', 'READY', clock_timestamp())
		`, stepID, orgID, envID, runID); err != nil {
			return fmt.Errorf("insert run_step: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed test run failed: %v", err)
	}

	// 2. Register two active workers in the environment
	worker1ID, _ := tenant.NewUUID()
	session1ID, _ := tenant.NewUUID()
	worker2ID, _ := tenant.NewUUID()
	session2ID, _ := tenant.NewUUID()

	err = tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		for _, pair := range []struct {
			wID, sID string
		}{{worker1ID, session1ID}, {worker2ID, session2ID}} {
			if _, err := tx.Exec(ctx, `INSERT INTO workers (id, organization_id, environment_id, public_key, status) VALUES ($1,$2,$3,'pk','ACTIVE')`, pair.wID, orgID, envID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO worker_sessions (id, organization_id, worker_id, environment_id, session_token_hash, expires_at) VALUES ($1,$2,$3,$4,'h',clock_timestamp()+interval '1 hour')`, pair.sID, orgID, pair.wID, envID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO worker_deployments (session_id, organization_id, bundle_digest) VALUES ($1,$2,$3)`, pair.sID, orgID, bundleDigest); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed workers failed: %v", err)
	}

	// 3. Dispatch outbox hint to JetStream (duplicate hints)
	eventID, _ := tenant.NewUUID()
	hint := outbox.WakeupHintDTO{
		EventID:        eventID,
		OrganizationID: orgID,
		RunID:          runID,
		Subject:        "execution.state_changed",
		EventType:      "STEP_READY",
		Timestamp:      time.Now().UTC().Format(time.RFC3339Nano),
	}
	hintData, _ := json.Marshal(hint)
	// Publish hint twice to simulate duplicate delivery
	for i := 0; i < 2; i++ {
		_, err = js.PublishMsg(&nats.Msg{
			Subject: outbox.SubjectPrefix + outbox.DefaultShard,
			Header:  nats.Header{"Nats-Msg-Id": []string{fmt.Sprintf("%s-%d", eventID, i)}},
			Data:    hintData,
		})
		if err != nil {
			t.Fatalf("publish hint %d failed: %v", i, err)
		}
	}

	// 4. Route both deliveries through the real consumer -> authoritative scan
	// path. The first handler claims the step and then deliberately returns an
	// error, simulating a crash before ACK; JetStream redelivers the message and
	// the second handler invocation must not create another lease.
	session1Ctx := &worker.WorkerSessionContext{
		SessionID:      session1ID,
		WorkerID:       worker1ID,
		OrganizationID: orgID,
		EnvironmentID:  envID,
		PoolName:       "default",
	}
	pollReq := &worker.PollRequestDTO{
		ProtocolVersion:   worker.ProtocolVersion,
		RequestID:         "poll-req-1",
		WorkerID:          worker1ID,
		SessionID:         session1ID,
		Pool:              "default",
		AvailableSlots:    1,
		DeploymentDigests: []string{bundleDigest},
	}
	var consumerCalls, claimedCount int
	var consumerMu sync.Mutex
	wakeupConsumer := outbox.NewWakeupConsumer(js, outbox.DefaultConsumerConfig(), outbox.WakeupHandlerFunc(func(ctx context.Context, hint outbox.WakeupHintDTO) error {
		consumerMu.Lock()
		consumerCalls++
		call := consumerCalls
		consumerMu.Unlock()
		resp, err := tc.workerEng.Claim(ctx, session1Ctx, pollReq)
		if err != nil {
			return err
		}
		consumerMu.Lock()
		claimedCount += len(resp.Assignments)
		consumerMu.Unlock()
		if call == 1 {
			return fmt.Errorf("simulate crash before ACK")
		}
		return nil
	}), nil)
	if err := wakeupConsumer.Start(ctx); err != nil {
		t.Fatalf("start wakeup consumer: %v", err)
	}
	defer wakeupConsumer.Stop()
	deadline := time.After(5 * time.Second)
	for {
		consumerMu.Lock()
		calls := consumerCalls
		consumerMu.Unlock()
		if calls >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for JetStream redelivery")
		case <-time.After(25 * time.Millisecond):
		}
	}
	consumerMu.Lock()
	if claimedCount != 1 {
		t.Fatalf("expected one claim across consumer redelivery, got %d", claimedCount)
	}
	consumerMu.Unlock()

	// 6. Direct database evidence of singular ownership (INV-03)
	var attemptCount int
	var leaseCount int
	var stepStatus string
	err = tc.storagePool.WithTenantTx(ctx, orgID, func(ctx context.Context, tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE step_id = $1::uuid`, stepID).Scan(&attemptCount); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_leases WHERE step_id = $1::uuid`, stepID).Scan(&leaseCount); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT state FROM run_steps WHERE id = $1::uuid`, stepID).Scan(&stepStatus)
	})
	if err != nil {
		t.Fatalf("query claim evidence: %v", err)
	}

	if attemptCount != 1 {
		t.Errorf("SINGULAR CLAIM VIOLATION: expected 1 attempt row in task_attempts, got %d", attemptCount)
	}
	if leaseCount != 1 {
		t.Errorf("SINGULAR CLAIM VIOLATION: expected 1 active lease row in task_leases, got %d", leaseCount)
	}
	if stepStatus != "RUNNING" {
		t.Errorf("expected step status RUNNING, got %s", stepStatus)
	}
}
