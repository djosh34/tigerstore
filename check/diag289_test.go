package check

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// Populated only by scripts/diag289-instrument.py in the Actions diagnosis.
var diag289Reads = map[int]int{}
var diag289Superseded = map[int]int{}

// The exact -sim.out history from the failing race-sim job of #289.
func TestDiag289History(t *testing.T) {
	if os.Getenv("DIAG289") == "" {
		t.Skip("diagnosis-only replay")
	}
	f, err := os.Open("testdata/diag289-seed7004.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	var h history.History
	if err := json.NewDecoder(z).Decode(&h); err != nil {
		t.Fatal(err)
	}
	limit := time.Minute
	if v := os.Getenv("DIAG289_TIMEOUT"); v != "" {
		limit, err = time.ParseDuration(v)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range bothModes {
		if v := os.Getenv("DIAG289_MODE"); v != "" && v != mode.String() {
			continue
		}
		t.Run(mode.String(), func(t *testing.T) {
			start := time.Now()
			err := Witnessed(&h, mode, limit)
			t.Logf("mode=%s ops=%d limit=%s elapsed=%s verdict=%v", mode, len(h.Ops), limit, time.Since(start), err)
			t.Logf("readsFit calls by op=%v; calls on already-compacted trial=%v", diag289Reads, diag289Superseded)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A rejected trial must not trigger a read-placement replay. This is an
// algorithmic (not wall-clock) red/green check for #289.
func TestDiag289RejectedOptional(t *testing.T) {
	h := grantReadCompact(t, false)
	w := newWitness(h, time.Now().Add(time.Second))
	if err := w.fromLog(); err != nil {
		t.Fatal(err)
	}
	var compact, read int
	for i, op := range h.Ops {
		if op.Req.Compact != nil {
			compact = i
		}
		if op.Req.Range != nil {
			read = i
		}
	}
	r := &revisionOrder{w: w, ready: map[int]bool{}}
	o := optional{op: compact, reads: []int{read}}
	before := w.steps
	if r.takes(w.live.clone(), o) {
		t.Fatal("already-applied compaction was taken twice")
	}
	if w.steps != before {
		t.Fatalf("rejected compaction replayed read placement: %d extra steps", w.steps-before)
	}
}

// #284's reduced legal order and its stale variant, with exact verdicts.
func TestDiag289Reduced284(t *testing.T) {
	for _, early := range []bool{false, true} {
		for _, mode := range bothModes {
			err := Witnessed(grantReadCompact(t, early), mode, time.Second)
			t.Logf("stale=%v mode=%s verdict=%v", early, mode, err)
			if (err != nil) != early {
				t.Errorf("stale=%v mode=%s: %v", early, mode, err)
			}
		}
	}
}
