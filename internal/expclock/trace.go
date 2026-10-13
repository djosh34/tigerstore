// Package expclock records bounded, opt-in events for the throwaway batch experiment.
package expclock

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Record struct {
	NS   int64  `json:"ns"`
	Kind string `json:"k"`
	A    uint64 `json:"a,omitempty"`
	B    uint64 `json:"b,omitempty"`
	C    uint64 `json:"c,omitempty"`
	D    uint64 `json:"d,omitempty"`
}

var enabled atomic.Bool
var mu sync.Mutex
var records []Record
var dropped uint64

const capacity = 2000000

func Enabled() bool { return enabled.Load() }

func Event(kind string, a, b, c, d uint64) {
	if !enabled.Load() {
		return
	}
	ns := time.Now().UnixNano()
	mu.Lock()
	if enabled.Load() {
		if len(records) < cap(records) {
			records = append(records, Record{ns, kind, a, b, c, d})
		} else {
			dropped++
		}
	}
	mu.Unlock()
}

// Start and Stop run only outside the measurement interval.
func Start(w http.ResponseWriter, _ *http.Request) {
	mu.Lock()
	records = make([]Record, 0, capacity)
	dropped = 0
	enabled.Store(true)
	mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}
func Stop(w http.ResponseWriter, _ *http.Request) {
	mu.Lock()
	enabled.Store(false)
	data := records
	lost := dropped
	records = nil
	mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(struct {
		Dropped uint64   `json:"dropped"`
		Events  []Record `json:"events"`
	}{lost, data}); err != nil {
		return
	}
}
