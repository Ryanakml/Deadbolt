package httpstaging

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"time"
)

// Fixture is a small, safe, controlled HTTP target for M2 network/timeout
// proof (Issue #25). It performs no financial transaction and has no
// third-party dependency: a loopback httptest server with bounded delay.
//
// Endpoints:
//
//	GET /health            -> 200 {"ok":true} immediately
//	GET /echo?msg=...      -> 200 {"echo":...}
//	GET /slow?delayMs=...  -> sleeps up to maxDelayMs then 200 {"sleptMs":N}
//
// The request log lives in memory (and optionally a file) separately from the
// runtime DB, so timeout/retry evidence never depends on committed run state.
type Fixture struct {
	mu         sync.Mutex
	server     *httptest.Server
	requests   []RequestRecord
	maxDelayMs int
}

// RequestRecord is one observed inbound request.
type RequestRecord struct {
	Path       string    `json:"path"`
	ReceivedAt time.Time `json:"receivedAt"`
}

// NewFixture starts the fixture with a bounded slow-path delay.
func NewFixture(maxDelayMs int) *Fixture {
	if maxDelayMs <= 0 {
		maxDelayMs = 5000
	}
	f := &Fixture{maxDelayMs: maxDelayMs}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", f.handleHealth)
	mux.HandleFunc("GET /echo", f.handleEcho)
	mux.HandleFunc("GET /slow", f.handleSlow)
	f.server = httptest.NewServer(mux)
	return f
}

// URL returns the fixture base URL.
func (f *Fixture) URL() string {
	return f.server.URL
}

// Close stops the fixture server.
func (f *Fixture) Close() {
	if f.server != nil {
		f.server.Close()
	}
}

// RequestCount returns the number of observed requests.
func (f *Fixture) RequestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func (f *Fixture) record(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, RequestRecord{Path: path, ReceivedAt: time.Now().UTC()})
}

func (f *Fixture) handleHealth(w http.ResponseWriter, r *http.Request) {
	f.record("/health")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
}

func (f *Fixture) handleEcho(w http.ResponseWriter, r *http.Request) {
	f.record("/echo")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"echo": r.URL.Query().Get("msg")})
}

func (f *Fixture) handleSlow(w http.ResponseWriter, r *http.Request) {
	f.record("/slow")
	delayMs, _ := strconv.Atoi(r.URL.Query().Get("delayMs"))
	if delayMs < 0 {
		delayMs = 0
	}
	if delayMs > f.maxDelayMs {
		delayMs = f.maxDelayMs
	}
	select {
	case <-r.Context().Done():
		return
	case <-time.After(time.Duration(delayMs) * time.Millisecond):
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"sleptMs": delayMs})
}

// StagingConfig resolves the hosted staging HTTP target for the real safe
// HTTP integration (Issue #25). Locally the env var is unset and the hosted
// test skips as PENDING_HOSTED_STAGING; on hosted staging the operator sets
// DEADBOLT_HTTP_STAGING_URL to the controlled fixture URL before running the
// gate. No financial or third-party URL is accepted here: the host must be a
// controlled fixture or the staging control plane itself.
func StagingConfig() (url string, ok bool) {
	url = os.Getenv("DEADBOLT_HTTP_STAGING_URL")
	if url == "" {
		return "", false
	}
	return url, true
}
