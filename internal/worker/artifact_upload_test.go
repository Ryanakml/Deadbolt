package worker_test

import (
	"testing"

	"github.com/Ryanakml/Deadbolt/internal/artifacts"
	"github.com/Ryanakml/Deadbolt/internal/worker"
)

func TestArtifactLimitsMirrorServerContract(t *testing.T) {
	if worker.MaxArtifactUploadBytes != artifacts.MaxObjectBytes {
		t.Fatalf("worker max %d != server max %d", worker.MaxArtifactUploadBytes, artifacts.MaxObjectBytes)
	}
	if worker.InlineResultLimitBytes != artifacts.InlineJSONLimitBytes {
		t.Fatalf("worker inline %d != server inline %d", worker.InlineResultLimitBytes, artifacts.InlineJSONLimitBytes)
	}
}
