package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Ryanakml/Deadbolt/internal/artifacts"
	"github.com/Ryanakml/Deadbolt/internal/auth"
	"github.com/Ryanakml/Deadbolt/internal/recovery"
	"github.com/Ryanakml/Deadbolt/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

// runRecoveryPrepare executes authoritative disaster recovery preparation
// (Blueprint §27.3) with direct database authority. This is the operator path
// used by scripts/restore-staging-db.sh and human platform operators; it runs
// under the same host-operator trust as --migrate and never accepts tenant
// credentials, so one organization can never freeze the platform (INV-01).
func runRecoveryPrepare(logger *log.Logger, recoveryPoint, incidentAt time.Time, rpoGapIDs []string, operatorNotes string) error {
	mgr, _, cleanup, err := openRecoveryManager(logger)
	if err != nil {
		return err
	}
	defer cleanup()

	report, err := mgr.PrepareDisasterRecovery(context.Background(), recovery.PrepareRequest{
		RecoveryPoint:    recoveryPoint,
		IncidentAt:       incidentAt,
		RPOGapRequestIDs: rpoGapIDs,
		OperatorNotes:    operatorNotes,
	})
	if err != nil {
		return fmt.Errorf("disaster recovery preparation failed: %w", err)
	}
	enc, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(enc))
	logger.Printf("Disaster recovery prepared: incident=%s restoredRuns=%d revokedWorkers=%d revokedAuth=%d status=%s",
		report.ID, report.RestoredRunsCount, report.RevokedWorkerSessionsCount, report.RevokedAuthSessionsCount, report.Status)
	return nil
}

// runRecoveryVerify executes authoritative recovery integrity verification
// (Blueprint §27.3 step 2) and fails closed when any check fails.
func runRecoveryVerify(logger *log.Logger) error {
	mgr, pool, cleanup, err := openRecoveryManager(logger)
	if err != nil {
		return err
	}
	defer cleanup()

	if store, ok := artifacts.StoreFromEnv(); ok && store != nil {
		mgr.SetArtifacts(artifacts.NewService(pool, store))
	}
	report, err := mgr.VerifyIntegrity(context.Background())
	if err != nil {
		return fmt.Errorf("recovery integrity verification failed: %w", err)
	}
	enc, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(enc))
	if !report.OverallPassed {
		return errors.New("recovery integrity verification failed: one or more checks did not pass")
	}
	logger.Printf("Recovery integrity verified: schema=%v tenant=%v artifacts=%v deletions=%v",
		report.SchemaValid, report.TenantIsolationValid, report.ArtifactsValid, report.DeletionLedgerValid)
	return nil
}

// runRecoveryRPOGap records absent-after-recovery-point request evidence.
func runRecoveryRPOGap(logger *log.Logger, incidentID string, rpoGapIDs []string, operatorNote string) error {
	if strings.TrimSpace(incidentID) == "" {
		return errors.New("--incident-id is required for RPO-gap reconciliation")
	}
	mgr, _, cleanup, err := openRecoveryManager(logger)
	if err != nil {
		return err
	}
	defer cleanup()

	report, err := mgr.ReconcileRPOGapRecords(context.Background(), incidentID, rpoGapIDs, operatorNote)
	if err != nil {
		return fmt.Errorf("RPO-gap reconciliation failed: %w", err)
	}
	enc, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(enc))
	return nil
}

// runRecoveryResume transitions recovery controls (READ_ONLY -> RESUMING -> ACTIVE).
func runRecoveryResume(logger *log.Logger, mode string) error {
	mgr, _, cleanup, err := openRecoveryManager(logger)
	if err != nil {
		return err
	}
	defer cleanup()

	controls, err := mgr.GradualResume(context.Background(), mode)
	if err != nil {
		return fmt.Errorf("recovery resume to %q failed: %w", mode, err)
	}
	enc, _ := json.MarshalIndent(controls, "", "  ")
	fmt.Println(string(enc))
	return nil
}

// openRecoveryManager connects to PostgreSQL with operator (migrator-grade)
// credentials and returns the recovery manager. Host-operator trust only.
func openRecoveryManager(logger *log.Logger) (*recovery.Manager, *storage.Pool, func(), error) {
	dbURL := os.Getenv("MIGRATOR_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("DEADBOLT_MIGRATOR_DATABASE_URL")
	}
	if dbURL == "" && strings.ToLower(strings.TrimSpace(os.Getenv("RUNTIME_MODE"))) == auth.ModeLocal {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		return nil, nil, nil, errors.New("MIGRATOR_DATABASE_URL is required for recovery operations (operator database authority)")
	}
	poolConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("invalid recovery database URL: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to initialize recovery database pool: %w", err)
	}
	cleanup := func() { pool.Close() }
	logger.Printf("Recovery operator database connection initialized.")
	return recovery.NewManager(storage.NewPool(pool)), storage.NewPool(pool), cleanup, nil
}

// parseRecoveryPoint parses a required RFC3339 recovery point timestamp.
func parseRecoveryPoint(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, errors.New("--recovery-point is required (RFC3339, e.g. 2026-09-22T14:00:00Z)")
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid --recovery-point %q: must be RFC3339: %w", raw, err)
	}
	return parsed.UTC(), nil
}

// parseIncidentAt parses an optional RFC3339 incident timestamp (defaults to now).
func parseIncidentAt(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Now().UTC(), nil
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid --incident-at %q: must be RFC3339: %w", raw, err)
	}
	return parsed.UTC(), nil
}

// splitCSV splits comma-separated request IDs, dropping blanks.
func splitCSV(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
