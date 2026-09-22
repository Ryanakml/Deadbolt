package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Scoped artifact upload from the worker agent (Blueprint §18.2).
//
// Task runners are network-isolated; the agent publishes result bytes on
// their behalf through the scoped artifact APIs using the worker session:
// reserve (pre-upload admission) → PUT bytes to the single-object presigned
// URL → finalize (server verifies size/SHA) → Complete with the artifact ID.
// Large results never enter inline JSONB.

const (
	// InlineResultLimitBytes mirrors the inline payload ceiling; larger
	// successful results spill to artifact references instead of JSONB.
	InlineResultLimitBytes = 256 << 10
	// MaxArtifactUploadBytes mirrors the per-object maximum enforced
	// server-side (artifacts.MaxObjectBytes). Equality is asserted in tests.
	MaxArtifactUploadBytes = 100 << 20
	// ArtifactUploadMarkerKey is the platform-reserved marker a task returns
	// to request an intentional upload of bytes that fit inline JSON.
	// Canonical shape; the TypeScript SDK builds the same marker.
	ArtifactUploadMarkerKey = "$artifactUpload"
)

// ArtifactPublishError carries a contract-coded, retry-classified upload
// failure into the completion error envelope.
type ArtifactPublishError struct {
	Code      string
	Retryable bool
	Err       error
}

func (e *ArtifactPublishError) Error() string {
	if e.Err != nil {
		return e.Code + ": " + e.Err.Error()
	}
	return e.Code
}

// extractArtifactBytes decides whether a successful runner output must
// travel as an artifact: explicit upload markers first, then oversize
// canonical JSON. It returns the raw bytes and a content type.
func extractArtifactBytes(output any) (data []byte, contentType string, ok bool) {
	if m, isMap := output.(map[string]any); isMap && len(m) == 1 {
		if marker, isMarker := m[ArtifactUploadMarkerKey].(map[string]any); isMarker {
			encoded, _ := marker["data"].(string)
			ct, _ := marker["contentType"].(string)
			if encoded == "" {
				return nil, "", false
			}
			raw, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				raw, err = base64.URLEncoding.DecodeString(strings.TrimSpace(encoded))
				if err != nil {
					return nil, "", false
				}
			}
			if ct == "" {
				ct = "application/octet-stream"
			}
			return raw, ct, true
		}
	}
	canonical, err := json.Marshal(output)
	if err != nil {
		return nil, "", false
	}
	if len(canonical) <= InlineResultLimitBytes {
		return nil, "", false
	}
	return canonical, "application/json", true
}

// publishArtifact uploads result bytes through reserve → PUT → finalize and
// returns the finalized artifact ID. PublishArtifactFn may be replaced in
// tests; nil preserves the control-plane implementation.
func (a *Agent) publishArtifact(ctx context.Context, assignment AssignmentDTO, data []byte, contentType string) (string, error) {
	if a.PublishArtifactFn != nil {
		return a.PublishArtifactFn(ctx, assignment, data, contentType)
	}
	return a.publishArtifactViaControlPlane(ctx, assignment, data, contentType)
}

func (a *Agent) publishArtifactViaControlPlane(ctx context.Context, assignment AssignmentDTO, data []byte, contentType string) (string, error) {
	if len(data) > MaxArtifactUploadBytes {
		return "", &ArtifactPublishError{Code: "ARTIFACT_TOO_LARGE", Retryable: false}
	}
	digest := sha256.Sum256(data)
	sha := hex.EncodeToString(digest[:])
	// Stable command identities: retries of the same logical publication reuse
	// the same Idempotency-Key so the control plane replays instead of
	// minting a second row or charging quota twice. Never time/random based.
	createKey := fmt.Sprintf("artifact-create:%s:%s:%d", assignment.AttemptID, sha, len(data))
	createBody, _ := json.Marshal(map[string]any{
		"runId": assignment.RunID, "attemptId": assignment.AttemptID,
		"ownershipEpoch": assignment.OwnershipEpoch,
		"sizeBytes":      len(data), "sha256": sha,
	})
	var created struct {
		ID        string `json:"id"`
		UploadURL string `json:"uploadUrl"`
	}
	if err := a.postArtifactJSON(ctx, "/v1/artifacts", createBody, &created, createKey); err != nil {
		return "", &ArtifactPublishError{Code: "UPLOAD_FAILED", Retryable: true, Err: err}
	}
	if created.ID == "" || created.UploadURL == "" {
		return "", &ArtifactPublishError{Code: "UPLOAD_FAILED", Retryable: true}
	}
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, created.UploadURL, bytes.NewReader(data))
	if err != nil {
		return "", &ArtifactPublishError{Code: "UPLOAD_FAILED", Retryable: true, Err: err}
	}
	putReq.Header.Set("Content-Type", contentType)
	putReq.ContentLength = int64(len(data))
	putResp, err := a.client.Do(putReq)
	if err != nil {
		return "", &ArtifactPublishError{Code: "UPLOAD_FAILED", Retryable: true, Err: err}
	}
	_, _ = io.Copy(io.Discard, putResp.Body)
	putResp.Body.Close()
	if putResp.StatusCode < 200 || putResp.StatusCode >= 300 {
		return "", &ArtifactPublishError{Code: "UPLOAD_FAILED", Retryable: true,
			Err: fmt.Errorf("presigned PUT status %d", putResp.StatusCode)}
	}
	finalizeBody, _ := json.Marshal(map[string]any{
		"attemptId": assignment.AttemptID, "ownershipEpoch": assignment.OwnershipEpoch, "sha256": sha,
	})
	var finalized struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	finalizeKey := fmt.Sprintf("artifact-finalize:%s:%s", created.ID, assignment.AttemptID)
	if err := a.postArtifactJSON(ctx, "/v1/artifacts/"+created.ID+"/finalize", finalizeBody, &finalized, finalizeKey); err != nil {
		return "", &ArtifactPublishError{Code: "UPLOAD_FAILED", Retryable: true, Err: err}
	}
	if finalized.Status != "READY" {
		return "", &ArtifactPublishError{Code: "UPLOAD_FAILED", Retryable: true,
			Err: fmt.Errorf("finalize status %q", finalized.Status)}
	}
	return finalized.ID, nil
}

// maybePublishArtifact routes a successful runner output through the scoped
// artifact flow when it carries an upload marker or exceeds the inline
// ceiling. It reports whether the output was consumed as an artifact.
func (a *Agent) maybePublishArtifact(ctx context.Context, assignment AssignmentDTO, output any) (string, bool, error) {
	data, contentType, ok := extractArtifactBytes(output)
	if !ok {
		return "", false, nil
	}
	if len(data) > MaxArtifactUploadBytes {
		return "", true, &ArtifactPublishError{Code: "ARTIFACT_TOO_LARGE", Retryable: false}
	}
	id, err := a.publishArtifact(ctx, assignment, data, contentType)
	if err != nil {
		return "", true, err
	}
	return id, true, nil
}

func (a *Agent) postArtifactJSON(ctx context.Context, path string, body []byte, resBody any, idempotencyKey ...string) error {
	u := a.baseURL.ResolveReference(&url.URL{Path: path})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if len(idempotencyKey) > 0 && strings.TrimSpace(idempotencyKey[0]) != "" {
		req.Header.Set("Idempotency-Key", strings.TrimSpace(idempotencyKey[0]))
	}
	if a.sessionTok != "" {
		req.Header.Set("Authorization", "Bearer "+a.sessionTok)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("artifact API status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}
	if resBody != nil {
		if err := json.Unmarshal(respBytes, resBody); err != nil {
			return err
		}
	}
	return nil
}
