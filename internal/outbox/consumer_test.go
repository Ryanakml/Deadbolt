package outbox

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestSameStreamContractRequiresExactDurableContract(t *testing.T) {
	wantSubjects := []string{"deadbolt.wakeup.>"}
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
		name  string
		mutate func(*nats.StreamConfig)
		valid bool
	}{
		{name: "matching contract", valid: true},
		{name: "discard policy", mutate: func(c *nats.StreamConfig) { c.Discard = nats.DiscardNew }},
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
