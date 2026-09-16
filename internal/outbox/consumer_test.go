package outbox

import (
	"testing"
	"time"

	natsServer "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestSameStreamContractRequiresExactDurableContract(t *testing.T) {
	wantSubjects := []string{SubjectPrefix + ">"}
	base := nats.StreamConfig{
		Name:       StreamName,
		Subjects:   append([]string(nil), wantSubjects...),
		Storage:    nats.FileStorage,
		Retention:  nats.WorkQueuePolicy,
		Discard:    nats.DiscardOld,
		MaxAge:     24 * time.Hour,
		Duplicates: 2 * time.Minute,
	}

	cases := []struct {
		name   string
		mutate func(*nats.StreamConfig)
		valid  bool
	}{
		{name: "matching contract", valid: true},
		{name: "wrong stream name", mutate: func(c *nats.StreamConfig) { c.Name = "OTHER_STREAM" }},
		{name: "memory storage", mutate: func(c *nats.StreamConfig) { c.Storage = nats.MemoryStorage }},
		{name: "wrong retention", mutate: func(c *nats.StreamConfig) { c.Retention = nats.LimitsPolicy }},
		{name: "discard policy", mutate: func(c *nats.StreamConfig) { c.Discard = nats.DiscardNew }},
		{name: "wrong max age", mutate: func(c *nats.StreamConfig) { c.MaxAge = time.Hour }},
		{name: "wrong duplicate window", mutate: func(c *nats.StreamConfig) { c.Duplicates = time.Hour }},
		{name: "extra subject", mutate: func(c *nats.StreamConfig) { c.Subjects = append(c.Subjects, "deadbolt.wakeup.other") }},
		{name: "missing subject", mutate: func(c *nats.StreamConfig) { c.Subjects = nil }},
		{name: "duplicate subject", mutate: func(c *nats.StreamConfig) { c.Subjects = append(c.Subjects, c.Subjects[0]) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.Subjects = append([]string(nil), base.Subjects...)
			if tc.mutate != nil {
				tc.mutate(&cfg)
			}
			if got := sameStreamContract(cfg, StreamName, wantSubjects, 2*time.Minute); got != tc.valid {
				t.Fatalf("sameStreamContract() = %v, want %v", got, tc.valid)
			}
		})
	}
}

func TestEnsureStreamReturnsDurabilityErrorInsteadOfMemoryFallback(t *testing.T) {
	serverOpts := &natsServer.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true}
	server, err := natsServer.NewServer(serverOpts)
	if err != nil {
		t.Fatalf("create NATS server: %v", err)
	}
	server.Start()
	t.Cleanup(server.Shutdown)
	if !server.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server did not become ready")
	}
	nc, err := nats.Connect(server.ClientURL())
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("create JetStream context: %v", err)
	}
	if _, err := EnsureStream(js, "INVALID_STREAM", []string{"bad subject with spaces"}, time.Minute); err == nil {
		t.Fatal("expected durable stream creation error")
	}
}
