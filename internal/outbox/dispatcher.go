package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
)

// JetStreamPublisherSlot lets the dispatcher keep running while NATS is down.
// The recovery loop swaps in a live JetStream client after reconnecting.
type JetStreamPublisherSlot struct {
	mu sync.RWMutex
	js JetStreamClient
}

func NewJetStreamPublisherSlot() *JetStreamPublisherSlot { return &JetStreamPublisherSlot{} }

func (s *JetStreamPublisherSlot) Set(js JetStreamClient) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.js = js
	s.mu.Unlock()
}

func (s *JetStreamPublisherSlot) PublishMsg(msg *nats.Msg, opts ...nats.PubOpt) (*nats.PubAck, error) {
	if s == nil {
		return nil, errors.New("NATS JetStream publisher slot is nil")
	}
	s.mu.RLock()
	js := s.js
	s.mu.RUnlock()
	if js == nil {
		return nil, errors.New("NATS JetStream is unavailable")
	}
	return js.PublishMsg(msg, opts...)
}

// JetStreamClient represents the minimum interface needed to publish wake-up hints to NATS JetStream.
type JetStreamClient interface {
	PublishMsg(m *nats.Msg, opts ...nats.PubOpt) (*nats.PubAck, error)
}

// DispatcherConfig configures the transactional outbox dispatcher.
type DispatcherConfig struct {
	BatchSize     int
	PollInterval  time.Duration
	Shard         string
	BaseBackoff   time.Duration
	MaxRetryDelay time.Duration
	MaxAttempts   int
}

// DefaultConfig returns safe, blueprint-compliant defaults for the outbox dispatcher.
func DefaultConfig() DispatcherConfig {
	return DispatcherConfig{
		BatchSize:     DefaultBatchSize,
		PollInterval:  200 * time.Millisecond,
		Shard:         DefaultShard,
		BaseBackoff:   500 * time.Millisecond,
		MaxRetryDelay: DefaultMaxRetryDelay,
		MaxAttempts:   DefaultMaxAttempts,
	}
}

// Dispatcher coordinates reading outbox intents from PostgreSQL and publishing wake-up hints to NATS JetStream.
type Dispatcher struct {
	pool       *pgxpool.Pool
	js         JetStreamClient
	cfg        DispatcherConfig
	metrics    *Metrics
	logger     *log.Logger
	mu         sync.Mutex
	running    bool
	stopSignal chan struct{}
}

// NewDispatcher creates an outbox Dispatcher.
func NewDispatcher(pool *pgxpool.Pool, js JetStreamClient, cfg DispatcherConfig, metrics *Metrics, logger *log.Logger) *Dispatcher {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 200 * time.Millisecond
	}
	if cfg.Shard == "" {
		cfg.Shard = DefaultShard
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 500 * time.Millisecond
	}
	if cfg.MaxRetryDelay <= 0 {
		cfg.MaxRetryDelay = DefaultMaxRetryDelay
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	if logger == nil {
		logger = log.Default()
	}

	return &Dispatcher{
		pool:       pool,
		js:         js,
		cfg:        cfg,
		metrics:    metrics,
		logger:     logger,
		stopSignal: make(chan struct{}),
	}
}

// CalculateBackoff determines retry delay using exponential backoff with jitter, bounded by MaxRetryDelay.
func (d *Dispatcher) CalculateBackoff(attempts int) time.Duration {
	if attempts <= 0 {
		attempts = 1
	}
	// 2^(attempts-1) * baseBackoff
	mult := math.Pow(2, float64(attempts-1))
	delay := time.Duration(mult * float64(d.cfg.BaseBackoff))
	if delay > d.cfg.MaxRetryDelay || delay < 0 {
		delay = d.cfg.MaxRetryDelay
	}
	// Add 10% jitter
	jitter := time.Duration(rand.Float64() * 0.1 * float64(delay))
	if delay > d.cfg.MaxRetryDelay-jitter {
		return d.cfg.MaxRetryDelay
	}
	return delay + jitter
}

// BuildSanitizedHint constructs a WakeupHintDTO strictly omitting secrets or outputs (Blueprint §19.1).
func BuildSanitizedHint(record OutboxEventRecord) (WakeupHintDTO, error) {
	if record.PayloadVersion != 1 {
		return WakeupHintDTO{}, fmt.Errorf("unsupported outbox payload version %d", record.PayloadVersion)
	}
	if record.EventID == "" || record.OrganizationID == "" || record.Subject == "" {
		return WakeupHintDTO{}, errors.New("outbox routing identifiers are required")
	}
	hint := WakeupHintDTO{
		EventID:        record.EventID,
		OrganizationID: record.OrganizationID,
		Subject:        record.Subject,
		Timestamp:      record.CreatedAt.UTC().Format(time.RFC3339Nano),
	}

	if len(record.Payload) > 0 {
		var rawMap map[string]json.RawMessage
		if err := json.Unmarshal(record.Payload, &rawMap); err != nil {
			return WakeupHintDTO{}, fmt.Errorf("invalid outbox payload JSON: %w", err)
		}
		if raw, ok := rawMap["runId"]; ok {
			if err := json.Unmarshal(raw, &hint.RunID); err != nil || hint.RunID == "" {
				return WakeupHintDTO{}, errors.New("outbox runId must be a non-empty string")
			}
		}
		if raw, ok := rawMap["eventType"]; ok {
			if err := json.Unmarshal(raw, &hint.EventType); err != nil || hint.EventType == "" {
				return WakeupHintDTO{}, errors.New("outbox eventType must be a non-empty string")
			}
		}
		if raw, ok := rawMap["sequence"]; ok {
			var number json.Number
			if err := json.Unmarshal(raw, &number); err != nil {
				return WakeupHintDTO{}, errors.New("outbox sequence must be an integer")
			}
			seq, err := strconv.ParseInt(string(number), 10, 64)
			if err != nil || seq < 0 {
				return WakeupHintDTO{}, errors.New("outbox sequence must be a non-negative integer")
			}
			hint.Sequence = seq
		}
		if record.Subject == "execution.state_changed" && (hint.RunID == "" || hint.EventType == "") {
			return WakeupHintDTO{}, errors.New("execution.state_changed requires runId and eventType")
		}
	}
	return hint, nil
}

// DispatchBatch processes up to batchSize pending outbox events.
// Requirement: "Publish ACK precedes published_at; Stable event IDs make duplicate delivery harmless."
func (d *Dispatcher) DispatchBatch(ctx context.Context, batchSize int) (int, error) {
	if d.pool == nil {
		return 0, fmt.Errorf("outbox dispatcher pool is nil")
	}
	if batchSize <= 0 {
		batchSize = d.cfg.BatchSize
	}

	// 1. Begin transaction to select batch with FOR UPDATE SKIP LOCKED (Blueprint §11.2)
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin outbox claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	query := `
		SELECT
			id, organization_id, event_id, subject, payload,
			payload_version, attempts, next_at, created_at
		FROM app.claim_outbox_batch($1)
	`
	rows, err := tx.Query(ctx, query, batchSize)
	if err != nil {
		return 0, fmt.Errorf("query pending outbox: %w", err)
	}

	var records []OutboxEventRecord
	for rows.Next() {
		var rec OutboxEventRecord
		if err := rows.Scan(
			&rec.ID, &rec.OrganizationID, &rec.EventID, &rec.Subject,
			&rec.Payload, &rec.PayloadVersion, &rec.Attempts, &rec.NextAt, &rec.CreatedAt,
		); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan outbox record: %w", err)
		}
		records = append(records, rec)
	}
	rows.Close()

	if len(records) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		if d.metrics != nil {
			d.metrics.RecordSweep()
		}
		return 0, nil
	}
	// claim_outbox_batch advances next_at and attempts in this transaction. Commit
	// before contacting NATS: the blueprint explicitly forbids holding a database
	// transaction open while waiting on an external provider. The due-time claim
	// is the bounded, crash-safe reservation; an abandoned claim becomes eligible
	// again after the claim lease.
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit outbox claims: %w", err)
	}

	publishedCount := 0
	targetSubject := fmt.Sprintf("%s%s", SubjectPrefix, d.cfg.Shard)

	// 2. Publish each claimed outbox record to NATS JetStream
	for _, rec := range records {
		hint, err := BuildSanitizedHint(rec)
		if err != nil {
			d.handleValidationFailure(ctx, rec, err)
			continue
		}
		data, err := json.Marshal(hint)
		if err != nil {
			d.logger.Printf("[OUTBOX] Error marshaling hint for event %s: %v", rec.EventID, err)
			continue
		}

		msg := &nats.Msg{
			Subject: targetSubject,
			Data:    data,
			Header: nats.Header{
				// Stable event_id in Nats-Msg-Id enables broker-level deduplication window
				"Nats-Msg-Id": []string{rec.EventID},
			},
		}

		// WAIT for broker publish ACK before marking published_at
		var pubAck *nats.PubAck
		if d.js != nil {
			pubAck, err = d.js.PublishMsg(msg)
		} else {
			err = errors.New("NATS JetStream client is nil")
		}

		if err != nil {
			// Publish failed: calculate backoff and release the claim. Attempts was
			// incremented by claim_outbox_batch exactly once.
			d.handlePublishFailure(ctx, rec, err)
			d.logger.Printf("[OUTBOX] Publish failed for event %s (attempt %d): %v", rec.EventID, rec.Attempts, err)
		} else {
			// Publish ACK received: Mark published_at atomically in PostgreSQL
			updateSuccess := `SELECT app.mark_outbox_published($1::uuid)`
			if _, execErr := d.pool.Exec(ctx, updateSuccess, rec.ID); execErr != nil {
				d.logger.Printf("[OUTBOX] Failed to mark published_at for event %s: %v", rec.EventID, execErr)
				// If DB mark fails, the committed claim expires and the event is
				// retried safely on a later sweep.
				return publishedCount, fmt.Errorf("mark published_at for event %s: %w", rec.EventID, execErr)
			}
			if d.metrics != nil {
				d.metrics.IncPublished()
			}
			publishedCount++
			if pubAck != nil && pubAck.Duplicate {
				d.logger.Printf("[OUTBOX] Event %s recognized as duplicate by broker stream %s (seq %d)", rec.EventID, pubAck.Stream, pubAck.Sequence)
			}
		}
	}

	if d.metrics != nil {
		d.metrics.RecordSweep()
	}
	return publishedCount, nil
}

func (d *Dispatcher) handleValidationFailure(ctx context.Context, rec OutboxEventRecord, cause error) {
	if d.metrics != nil {
		d.metrics.IncFailures()
	}
	if _, err := d.pool.Exec(ctx, `SELECT app.dead_letter_outbox_event($1::uuid, $2)`, rec.ID, cause.Error()); err != nil {
		d.logger.Printf("[OUTBOX] Failed to dead-letter invalid event %s: %v", rec.EventID, err)
	}
	d.logger.Printf("[OUTBOX] Dead-lettered invalid event %s: %v", rec.EventID, cause)
}

func (d *Dispatcher) handlePublishFailure(ctx context.Context, rec OutboxEventRecord, cause error) {
	if d.metrics != nil {
		d.metrics.IncFailures()
	}
	// Broker/network failures are transient. Do not apply MaxAttempts here:
	// doing so would permanently lose events during a prolonged NATS outage.
	backoff := d.CalculateBackoff(rec.Attempts)
	if _, err := d.pool.Exec(ctx, `SELECT app.retry_outbox_event($1::uuid, $2::interval)`, rec.ID, fmt.Sprintf("%d milliseconds", backoff.Milliseconds())); err != nil {
		d.logger.Printf("[OUTBOX] Failed to schedule retry for event %s: %v", rec.EventID, err)
	}
	if _, err := d.pool.Exec(ctx, `SELECT app.record_outbox_failure($1::uuid, $2)`, rec.ID, cause.Error()); err != nil {
		d.logger.Printf("[OUTBOX] Failed to record error for event %s: %v", rec.EventID, err)
	}
}

// Run executes the continuous background dispatch loop until ctx cancellation.
func (d *Dispatcher) Run(ctx context.Context) error {
	d.mu.Lock()
	if d.running {
		d.mu.Unlock()
		return fmt.Errorf("outbox dispatcher is already running")
	}
	d.running = true
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		d.running = false
		d.mu.Unlock()
	}()

	d.logger.Printf("[OUTBOX] Dispatcher started (poll interval: %v, shard: %s)", d.cfg.PollInterval, d.cfg.Shard)
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	// Periodic metrics gauge update ticker (every 5 seconds)
	metricsTicker := time.NewTicker(5 * time.Second)
	defer metricsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Printf("[OUTBOX] Dispatcher stopping: context cancelled")
			return ctx.Err()
		case <-d.stopSignal:
			d.logger.Printf("[OUTBOX] Dispatcher stopping: stop signaled")
			return nil
		case <-metricsTicker.C:
			if d.metrics != nil {
				_ = d.metrics.UpdateDatabaseGauges(ctx)
			}
		case <-ticker.C:
			// Process available batch
			batchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			count, err := d.DispatchBatch(batchCtx, d.cfg.BatchSize)
			cancel()
			if err != nil && !errors.Is(err, context.Canceled) {
				d.logger.Printf("[OUTBOX] Dispatcher batch error: %v", err)
			} else if count > 0 {
				d.logger.Printf("[OUTBOX] Dispatched %d outbox wake-up hint(s)", count)
			}
		}
	}
}

// Stop gracefully signals the background dispatcher loop to terminate.
func (d *Dispatcher) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.running {
		close(d.stopSignal)
	}
}
