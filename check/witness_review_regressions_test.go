package check

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

func TestWitnessDeterministicBudget(t *testing.T) {
	b := newBuilder(t, "k")
	for range 6 {
		b.put(0, "k", "v")
		b.do(0, history.Request{Range: &history.RangeRequest{End: history.AllKeys, CountOnly: true}})
	}
	h := b.done()
	var firstSteps int64
	var firstTimeout bool
	for i := range 80 {
		var used int64
		err := Witnessed(h, FromRevisions, time.Minute, MaxSteps(27), StepsUsed(&used))
		timeout := errors.Is(err, ErrWitnessTimeout)
		if err != nil && !timeout {
			t.Fatal(err)
		}
		if i == 0 {
			firstSteps, firstTimeout = used, timeout
		} else if used != firstSteps || timeout != firstTimeout {
			t.Fatalf("map iteration changed a budget verdict: first (%d, timeout=%v), run %d (%d, timeout=%v)", firstSteps, firstTimeout, i, used, timeout)
		}
	}
	if firstSteps != 27 || firstTimeout {
		t.Fatalf("six sequential put/count pairs: got %d steps, timeout %v; want 27 and pass", firstSteps, firstTimeout)
	}
}

func TestWitnessSavedStepsDeterministic(t *testing.T) {
	h := budgetHistory(t, "seed-5058843.json")
	var first int64
	for i := range 50 {
		var used int64
		if err := Witnessed(h, FromRevisions, time.Minute, StepsUsed(&used)); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = used
		} else if used != first {
			t.Fatalf("seed-5058843 used %d then %d steps", first, used)
		}
	}
	if first != 1794 {
		t.Fatalf("seed-5058843 used %d steps, want 1794", first)
	}
}

func TestWitnessLargeScalarStopsBeforeEncodingWholeValue(t *testing.T) {
	v := &history.Response{Range: &history.RangeResponse{KVs: []history.KeyValue{{Key: "k", Value: strings.Repeat("<", 1<<20)}}}}
	w := diagnosticWitness()
	w.maxSteps = 1
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got := w.jsonOf(v)
	runtime.ReadMemStats(&after)
	if got != "" || !w.stopped || w.steps != 2 {
		t.Fatalf("large scalar escaped cancellation: %d bytes, stopped %v, steps %d", len(got), w.stopped, w.steps)
	}
	// The old scalar encoder allocated/escaped the entire >6 MiB token
	// before its first accounting Write. A one-step budget must stop in
	// the first bounded chunk instead, not after that allocation.
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
		t.Fatalf("encoded the whole scalar before its budget stop: %d allocated bytes", allocated)
	}
}

func TestWitnessRollbackStops(t *testing.T) {
	r := dueShape(0)
	keys := make([]string, 100_000)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%06d", i)
	}
	s := newState(keys, nil)
	for i, key := range keys {
		s.kvs[i] = history.KeyValue{Key: key, Version: 1}
	}
	_, undo := r.try(s, history.Request{Delete: &history.DeleteRequest{End: history.AllKeys}})
	for i := range s.kvs {
		s.kvs[i] = history.KeyValue{}
	}
	r.w.steps, r.w.workTicks, r.workLeft, r.budgetLeft = 0, 0, 0, 0
	polls := 0
	r.w.stop = func() bool { polls++; return true }
	undo()
	restored := 0
	for _, kv := range s.kvs {
		if kv.Version != 0 {
			restored++
		}
	}
	if !r.w.stopped || polls != 1 || r.w.workTicks != 1024 || r.w.steps == 0 || restored == 0 || restored >= len(keys) {
		t.Fatalf("undo bypassed cancellation: stopped %v, polls %d, ticks %d, steps %d, restored %d", r.w.stopped, polls, r.w.workTicks, r.w.steps, restored)
	}
}
