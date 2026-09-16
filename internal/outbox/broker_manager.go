package outbox

import (
	"context"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

// BrokerManager owns the replaceable NATS connection used by the dispatcher
// and its wake-up consumer. It deliberately retries initial connection,
// stream/consumer setup, and post-connect outages through the same path.
type BrokerManager struct {
	url           string
	publisher     *JetStreamPublisherSlot
	handler       WakeupHandler
	logger        *log.Logger
	retryInterval time.Duration
	consumerCfg   ConsumerConfig

	nc       *nats.Conn
	consumer *WakeupConsumer
}

func NewBrokerManager(url string, publisher *JetStreamPublisherSlot, handler WakeupHandler, logger *log.Logger, retryInterval time.Duration) *BrokerManager {
	if logger == nil {
		logger = log.Default()
	}
	if retryInterval <= 0 {
		retryInterval = 2 * time.Second
	}
	return &BrokerManager{
		url:           url,
		publisher:     publisher,
		handler:       handler,
		logger:        logger,
		retryInterval: retryInterval,
		consumerCfg:   DefaultConsumerConfig(),
	}
}

func (m *BrokerManager) Run(ctx context.Context) error {
	if m == nil {
		return nil
	}
	ticker := time.NewTicker(m.retryInterval)
	defer ticker.Stop()
	defer m.disconnect()

	m.connect(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if m.nc == nil || !m.nc.IsConnected() {
				m.disconnect()
				m.connect(ctx)
			}
		}
	}
}

func (m *BrokerManager) connect(ctx context.Context) {
	if m.url == "" || m.publisher == nil {
		return
	}
	nc, err := nats.Connect(m.url, nats.Timeout(2*time.Second), nats.MaxReconnects(0))
	if err != nil {
		m.publisher.Set(nil)
		m.logger.Printf("[OUTBOX] NATS unavailable; retrying connection: %v", err)
		return
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		m.publisher.Set(nil)
		m.logger.Printf("[OUTBOX] JetStream unavailable; retrying connection: %v", err)
		return
	}
	if _, err := EnsureStream(js, StreamName, []string{SubjectPrefix + ">"}, 2*time.Minute); err != nil {
		nc.Close()
		m.publisher.Set(nil)
		m.logger.Printf("[OUTBOX] NATS stream contract unavailable; retrying: %v", err)
		return
	}
	consumer := NewWakeupConsumer(js, m.consumerCfg, m.handler, m.logger)
	if err := consumer.Start(ctx); err != nil {
		nc.Close()
		m.publisher.Set(nil)
		m.logger.Printf("[OUTBOX] Wake-up consumer unavailable; retrying: %v", err)
		return
	}
	m.nc, m.consumer = nc, consumer
	m.publisher.Set(js)
	m.logger.Printf("[OUTBOX] NATS JetStream connected and wake-up consumer ready")
}

func (m *BrokerManager) disconnect() {
	if m == nil {
		return
	}
	if m.consumer != nil {
		_ = m.consumer.Stop()
		m.consumer = nil
	}
	if m.nc != nil {
		m.nc.Close()
		m.nc = nil
	}
	if m.publisher != nil {
		m.publisher.Set(nil)
	}
}
