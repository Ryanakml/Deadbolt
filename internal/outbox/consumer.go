package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// ConsumerConfig configures the NATS JetStream wake-up hint consumer.
type ConsumerConfig struct {
	StreamName   string
	ConsumerName string
	FilterPrefix string
	AckWait      time.Duration
	MaxDeliver   int
}

// DefaultConsumerConfig returns standard settings for the wake-up consumer.
func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		StreamName:   StreamName,
		ConsumerName: "runtime-controlplane-wakeup",
		FilterPrefix: SubjectPrefix + ">",
		AckWait:      10 * time.Second,
		MaxDeliver:   5,
	}
}

// EnsureStream ensures that the internal JetStream stream exists with the specified deduplication window.
func EnsureStream(js nats.JetStreamContext, streamName string, subjects []string, dupWindow time.Duration) (*nats.StreamInfo, error) {
	if js == nil {
		return nil, fmt.Errorf("jetstream context is nil")
	}
	if streamName == "" {
		streamName = StreamName
	}
	if len(subjects) == 0 {
		subjects = []string{SubjectPrefix + ">"}
	}
	if dupWindow <= 0 {
		dupWindow = 2 * time.Minute
	}

	info, err := js.StreamInfo(streamName)
	if err == nil {
		return info, nil
	}

	cfg := &nats.StreamConfig{
		Name:        streamName,
		Subjects:    subjects,
		Storage:     nats.FileStorage,
		Duplicates:  dupWindow,
		Retention:   nats.WorkQueuePolicy,
		Discard:     nats.DiscardOld,
		MaxAge:      24 * time.Hour,
		Description: "Deadbolt internal control-plane wake-up hints",
	}

	info, err = js.AddStream(cfg)
	if err != nil {
		// In memory fallback for test environments without disk write access
		cfg.Storage = nats.MemoryStorage
		info, err = js.AddStream(cfg)
		if err != nil {
			return nil, fmt.Errorf("create stream %s: %w", streamName, err)
		}
	}
	return info, nil
}

// WakeupConsumer listens to NATS JetStream wake-up hints and triggers authoritative database scans.
type WakeupConsumer struct {
	js         nats.JetStreamContext
	cfg        ConsumerConfig
	handler    WakeupHandler
	logger     *log.Logger
	sub        *nats.Subscription
	mu         sync.Mutex
	running    bool
	stopSignal chan struct{}
}

// NewWakeupConsumer creates a new WakeupConsumer.
func NewWakeupConsumer(js nats.JetStreamContext, cfg ConsumerConfig, handler WakeupHandler, logger *log.Logger) *WakeupConsumer {
	if cfg.StreamName == "" {
		cfg.StreamName = StreamName
	}
	if cfg.ConsumerName == "" {
		cfg.ConsumerName = "runtime-controlplane-wakeup"
	}
	if cfg.FilterPrefix == "" {
		cfg.FilterPrefix = SubjectPrefix + ">"
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = 10 * time.Second
	}
	if cfg.MaxDeliver <= 0 {
		cfg.MaxDeliver = 5
	}
	if logger == nil {
		logger = log.Default()
	}

	return &WakeupConsumer{
		js:         js,
		cfg:        cfg,
		handler:    handler,
		logger:     logger,
		stopSignal: make(chan struct{}),
	}
}

// Start begins consuming wake-up hints from NATS JetStream.
// Requirement: "A control-plane consumer ACKs after the hint is used to trigger a DB scan; if it crashes before scan/ACK, redelivery or polling still discovers the work."
func (c *WakeupConsumer) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("wakeup consumer is already running")
	}
	if c.js == nil {
		c.mu.Unlock()
		return fmt.Errorf("jetstream context is nil")
	}

	sub, err := c.js.QueueSubscribe(
		c.cfg.FilterPrefix,
		c.cfg.ConsumerName,
		func(msg *nats.Msg) {
			c.processMessage(msg)
		},
		nats.Durable(c.cfg.ConsumerName),
		nats.AckExplicit(),
		nats.AckWait(c.cfg.AckWait),
		nats.MaxDeliver(c.cfg.MaxDeliver),
	)
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("subscribe to wakeup stream: %w", err)
	}

	c.sub = sub
	c.running = true
	c.mu.Unlock()

	c.logger.Printf("[OUTBOX-CONSUMER] Started consuming wake-up hints on %s (consumer: %s)", c.cfg.FilterPrefix, c.cfg.ConsumerName)
	return nil
}

// processMessage decodes the sanitized hint, triggers the DB scan, and ACKs the message upon completion.
func (c *WakeupConsumer) processMessage(msg *nats.Msg) {
	var hint WakeupHintDTO
	if err := json.Unmarshal(msg.Data, &hint); err != nil {
		c.logger.Printf("[OUTBOX-CONSUMER] Malformed wake-up hint received: %v", err)
		// Terminate unparseable message to avoid poisonous retry loops
		_ = msg.Term()
		return
	}

	c.logger.Printf("[OUTBOX-CONSUMER] Received wake-up hint: event=%s subject=%s run=%s", hint.EventID, hint.Subject, hint.RunID)

	if c.handler != nil {
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.AckWait)
		defer cancel()

		// Trigger DB scan only (Blueprint §19.1: does NOT grant execution ownership)
		if err := c.handler.HandleWakeup(ctx, hint); err != nil {
			c.logger.Printf("[OUTBOX-CONSUMER] Wakeup handler error for event %s: %v (nacking for redelivery)", hint.EventID, err)
			_ = msg.Nak()
			return
		}
	}

	// ACK message after DB scan completed successfully
	if err := msg.Ack(); err != nil {
		c.logger.Printf("[OUTBOX-CONSUMER] Failed to ACK message for event %s: %v", hint.EventID, err)
	}
}

// Stop gracefully unsubscribes from JetStream and shuts down the consumer.
func (c *WakeupConsumer) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return nil
	}
	c.running = false
	if c.sub != nil {
		_ = c.sub.Unsubscribe()
	}
	c.logger.Printf("[OUTBOX-CONSUMER] Wakeup consumer stopped")
	return nil
}
