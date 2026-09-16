package outbox

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	natsServer "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func startTestJetStream(t *testing.T, port int, storeDir string) *natsServer.Server {
	t.Helper()
	server, err := natsServer.NewServer(&natsServer.Options{
		Host: "127.0.0.1", Port: port, JetStream: true, StoreDir: storeDir, NoLog: true, NoSigs: true,
	})
	if err != nil {
		t.Fatalf("create NATS server: %v", err)
	}
	server.Start()
	if !server.ReadyForConnections(5 * time.Second) {
		server.Shutdown()
		t.Fatal("NATS server did not become ready")
	}
	return server
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for broker manager state")
		case <-ticker.C:
		}
	}
}

func TestBrokerManagerRecoversUnavailableStartupAndReconnects(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve NATS port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	storeDir := t.TempDir()
	url := "nats://127.0.0.1:" + fmt.Sprint(port)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publisher := NewJetStreamPublisherSlot()
	received := make(chan string, 4)
	manager := NewBrokerManager(url, publisher, WakeupHandlerFunc(func(ctx context.Context, hint WakeupHintDTO) error {
		received <- hint.EventID
		return nil
	}), log.Default(), 25*time.Millisecond)
	managerDone := make(chan error, 1)
	go func() { managerDone <- manager.Run(ctx) }()

	// The manager starts before the broker and must keep retrying.
	server := startTestJetStream(t, port, storeDir)
	defer server.Shutdown()

	data := []byte(`{"eventId":"event-1","organizationId":"org-1","runId":"run-1","subject":"execution.state_changed","eventType":"STEP_READY","timestamp":"2026-09-16T00:00:00Z"}`)
	waitFor(t, 5*time.Second, func() bool {
		_, err := publisher.PublishMsg(&nats.Msg{Subject: SubjectPrefix + DefaultShard, Data: data, Header: nats.Header{"Nats-Msg-Id": []string{"event-1"}}})
		return err == nil
	})
	select {
	case got := <-received:
		if got != "event-1" {
			t.Fatalf("received event %q, want event-1", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for startup recovery delivery")
	}

	server.Shutdown()
	waitFor(t, 5*time.Second, func() bool {
		_, err := publisher.PublishMsg(&nats.Msg{Subject: SubjectPrefix + DefaultShard, Data: data})
		return err != nil
	})

	server = startTestJetStream(t, port, storeDir)
	defer server.Shutdown()
	waitFor(t, 5*time.Second, func() bool {
		_, err := publisher.PublishMsg(&nats.Msg{Subject: SubjectPrefix + DefaultShard, Data: data, Header: nats.Header{"Nats-Msg-Id": []string{"event-2"}}})
		return err == nil
	})
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reconnect delivery")
	}

	cancel()
	select {
	case err := <-managerDone:
		if err != context.Canceled {
			t.Fatalf("manager returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not stop after context cancellation")
	}
	if _, err := publisher.PublishMsg(&nats.Msg{Subject: SubjectPrefix + DefaultShard, Data: data}); err == nil {
		t.Fatal("publisher slot remained live after manager cancellation")
	}
}
