package externaleffect

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Record represents one committed external side effect recorded in the
// independent external ledger. This ledger lives entirely outside PostgreSQL
// (Blueprint §27.3 & §28).
type Record struct {
	IdempotencyKey string    `json:"idempotencyKey"`
	Action         string    `json:"action"`
	Payload        any       `json:"payload"`
	ExecutedAt     time.Time `json:"executedAt"`
	Duplicate      bool      `json:"duplicate"`
}

// Fixture is a disposable external side-effect server that persists its
// dedup ledger to an independent file store outside of PostgreSQL.
// It survives database snapshot restores, simulating external reality that
// has moved forward while the database travelled backward in time.
type Fixture struct {
	mu         sync.Mutex
	ledgerPath string
	records    map[string]Record
	server     *httptest.Server
}

// NewFixture creates and starts a new external effect fixture backed by an
// independent file on disk.
func NewFixture(dir string) (*Fixture, error) {
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "deadbolt-external-effect-*")
		if err != nil {
			return nil, err
		}
	}
	ledgerPath := filepath.Join(dir, "external_dedup_ledger.json")

	f := &Fixture{
		ledgerPath: ledgerPath,
		records:    make(map[string]Record),
	}

	// Load existing ledger if present (survives DB restore)
	_ = f.loadLedger()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/effects/execute", f.handleExecute)
	mux.HandleFunc("GET /v1/effects/ledger", f.handleGetLedger)
	mux.HandleFunc("GET /v1/effects/{key}", f.handleGetByKey)

	f.server = httptest.NewServer(mux)
	return f, nil
}

func (f *Fixture) URL() string {
	return f.server.URL
}

func (f *Fixture) Close() {
	if f.server != nil {
		f.server.Close()
	}
}

func (f *Fixture) loadLedger() error {
	data, err := os.ReadFile(f.ledgerPath)
	if err != nil {
		return err
	}
	var list []Record
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	for _, rec := range list {
		f.records[rec.IdempotencyKey] = rec
	}
	return nil
}

func (f *Fixture) saveLedger() error {
	var list []Record
	for _, rec := range f.records {
		list = append(list, rec)
	}
	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(f.ledgerPath, data, 0644)
}

func (f *Fixture) handleExecute(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var req struct {
		IdempotencyKey string `json:"idempotencyKey"`
		Action         string `json:"action"`
		Payload        any    `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.IdempotencyKey == "" {
		http.Error(w, "idempotencyKey is required", http.StatusBadRequest)
		return
	}

	// Check if already executed in external reality
	if existing, found := f.records[req.IdempotencyKey]; found {
		resp := existing
		resp.Duplicate = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// First execution: record to independent external ledger
	rec := Record{
		IdempotencyKey: req.IdempotencyKey,
		Action:         req.Action,
		Payload:        req.Payload,
		ExecutedAt:     time.Now().UTC(),
		Duplicate:      false,
	}
	f.records[req.IdempotencyKey] = rec
	_ = f.saveLedger()

	// Simulate response loss if header requested (Blueprint §28)
	if r.Header.Get("X-Simulate-Loss") == "true" {
		// External effect committed to ledger, but client sees transport failure!
		hj, ok := w.(http.Hijacker)
		if ok {
			conn, _, err := hj.Hijack()
			if err == nil {
				_ = conn.Close()
				return
			}
		}
		http.Error(w, "SIMULATED_TRANSPORT_FAILURE", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rec)
}

func (f *Fixture) handleGetLedger(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var list []Record
	for _, rec := range f.records {
		list = append(list, rec)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(list)
}

func (f *Fixture) handleGetByKey(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := r.PathValue("key")
	if rec, found := f.records[key]; found {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rec)
		return
	}
	http.Error(w, "not found", http.StatusNotFound)
}

// GetRecord returns record directly from memory/ledger.
func (f *Fixture) GetRecord(key string) (Record, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, found := f.records[key]
	return rec, found
}

// Get is an alias for GetRecord.
func (f *Fixture) Get(key string) (Record, bool) {
	return f.GetRecord(key)
}

// TotalRecords returns the number of committed external side effects.
func (f *Fixture) TotalRecords() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

// Count returns the number of committed external side effects.
func (f *Fixture) Count() int {
	return f.TotalRecords()
}

// DuplicateCount returns the number of duplicate executions recorded.
func (f *Fixture) DuplicateCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, rec := range f.records {
		if rec.Duplicate {
			count++
		}
	}
	return count
}

// ExecuteEffect performs an effect directly in Go and records it in the disk-backed ledger.
func (f *Fixture) ExecuteEffect(idempotencyKey, action string, payload any) (Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if existing, found := f.records[idempotencyKey]; found {
		dup := existing
		dup.Duplicate = true
		return dup, nil
	}

	rec := Record{
		IdempotencyKey: idempotencyKey,
		Action:         action,
		Payload:        payload,
		ExecutedAt:     time.Now().UTC(),
		Duplicate:      false,
	}
	f.records[idempotencyKey] = rec
	if err := f.saveLedger(); err != nil {
		return rec, err
	}
	return rec, nil
}
