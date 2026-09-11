package contracts_test

import (
	"github.com/Ryanakml/Deadbolt/internal/contracts"
	"testing"
)

func TestRawUTF8(t *testing.T) {
	for _, raw := range [][]byte{{'"', 0xc0, 0xaf, '"'}, {'"', 0xed, 0xa0, 0x80, '"'}} {
		if _, err := contracts.ParseJSON(raw); err == nil {
			t.Fatal("invalid UTF-8 accepted")
		}
	}
}
func FuzzRawJSON(f *testing.F) {
	for _, raw := range []string{`{}`, `{"a":1,"a":2}`, `"\ud800"`, `[1,null,true]`, `1.0`, `{} {}`} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		canonical, hash, err := contracts.Digest([]byte(raw))
		if err != nil {
			return
		}
		canonical2, hash2, err := contracts.Digest(canonical)
		if err != nil || string(canonical) != string(canonical2) || hash != hash2 {
			t.Fatal("canonicalization is not idempotent")
		}
	})
}
