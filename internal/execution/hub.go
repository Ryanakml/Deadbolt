package execution

import (
	"sync"
)

// EventHub manages in-process notifications for run execution events.
// It allows SSE streams to awaken immediately upon event commit while
// preserving PostgreSQL as the authoritative store.
type EventHub struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan struct{}]struct{}
}

func NewEventHub() *EventHub {
	return &EventHub{
		subscribers: make(map[string]map[chan struct{}]struct{}),
	}
}

// Publish signals all active listeners for a specific run ID that new events
// have been committed to the database.
func (h *EventHub) Publish(runID string) {
	if h == nil {
		return
	}
	h.mu.RLock()
	subs, ok := h.subscribers[runID]
	if !ok || len(subs) == 0 {
		h.mu.RUnlock()
		return
	}
	// Copy channel references under read lock
	chans := make([]chan struct{}, 0, len(subs))
	for ch := range subs {
		chans = append(chans, ch)
	}
	h.mu.RUnlock()

	for _, ch := range chans {
		select {
		case ch <- struct{}{}:
		default:
			// Non-blocking if subscriber hasn't drained previous tick
		}
	}
}

// Subscribe returns a notification channel for runID and an unsubscribe cleanup function.
func (h *EventHub) Subscribe(runID string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	if h == nil {
		return ch, func() {}
	}
	h.mu.Lock()
	if _, ok := h.subscribers[runID]; !ok {
		h.subscribers[runID] = make(map[chan struct{}]struct{})
	}
	h.subscribers[runID][ch] = struct{}{}
	h.mu.Unlock()

	cleanup := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if subs, ok := h.subscribers[runID]; ok {
			delete(subs, ch)
			if len(subs) == 0 {
				delete(h.subscribers, runID)
			}
		}
	}

	return ch, cleanup
}
