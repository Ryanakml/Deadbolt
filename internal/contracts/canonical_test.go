package contracts_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/contracts"
)

type GoldenVector struct {
	ID                string `json:"id"`
	Input             any    `json:"input"`
	ExpectedCanonical string `json:"expectedCanonical"`
	ExpectedSha256    string `json:"expectedSha256"`
}

func TestRFC8785GoldenVectors(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "contracts", "fixtures", "canonical-json", "jcs-vectors.json")
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("Failed to read fixture file: %v", err)
	}

	var vectors []GoldenVector
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatalf("Failed to parse fixture JSON: %v", err)
	}

	for _, v := range vectors {
		t.Run(v.ID, func(t *testing.T) {
			rawInput, err := json.Marshal(v.Input)
			if err != nil {
				t.Fatalf("Failed to marshal input: %v", err)
			}

			canonical, hash, err := contracts.Digest(rawInput)
			if err != nil {
				t.Fatalf("Failed to digest: %v", err)
			}

			if string(canonical) != v.ExpectedCanonical {
				t.Errorf("Canonical mismatch.\nExpected: %s\nGot:      %s", v.ExpectedCanonical, string(canonical))
			}

			if hash != v.ExpectedSha256 {
				t.Errorf("SHA-256 hash mismatch.\nExpected: %s\nGot:      %s", v.ExpectedSha256, hash)
			}
		})
	}
}
