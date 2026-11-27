package check

import (
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestWitnessResolvedBudgetDeterministic(t *testing.T) {
	b := newBuilder(t, "k")
	var puts []int
	for i := range 3 {
		puts = append(puts, b.put(0, "k", strconv.Itoa(i)))
		b.get(0, "k")
	}
	for range 5 {
		b.get(0, "k")
	}
	h := b.done()
	for _, i := range puts {
		h.Ops[i].Resp, h.Ops[i].Err = nil, errUnavailable
	}
	for i := range 200 {
		var used int64
		err := Witnessed(h, FromRevisions, time.Minute, MaxSteps(25), StepsUsed(&used))
		if err != nil || used != 25 {
			t.Fatalf("resolved writers changed budget verdict on run %d: %v, %d steps; want pass at 25", i, err, used)
		}
	}
}

func TestWitnessResolvedDuplicatesOrderIndependent(t *testing.T) {
	b := newBuilder(t, "k")
	var puts, reads []int
	for i := range 3 {
		puts = append(puts, b.put(0, "k", strconv.Itoa(i)))
		reads = append(reads, b.get(0, "k"))
	}
	h := b.done()
	for _, i := range puts {
		h.Ops[i].Resp, h.Ops[i].Err = nil, errUnavailable
	}
	// Three different revisions appear to name the same ambiguous put.
	// None may survive merely because the map visited it last.
	for _, i := range reads {
		h.Ops[i].Resp.Range.KVs[0].Value = "0"
	}
	var first int64
	for run := range 50 {
		w := newWitness(h, time.Now().Add(time.Minute))
		r := newRevisionOrder(w)
		if err := r.index(); err != nil {
			t.Fatal(err)
		}
		if len(r.resolved) != 0 || len(r.reserved) != 0 {
			t.Fatalf("map traversal retained an arbitrary duplicate: resolved %v, reserved %v", r.resolved, r.reserved)
		}
		if run == 0 {
			first = w.steps
		} else if w.steps != first {
			t.Fatalf("duplicate filtering charged %d then %d steps", first, w.steps)
		}
	}
}

func TestWitnessDiagnosticFinalAssemblyStops(t *testing.T) {
	for _, limit := range []string{"step", "wall"} {
		t.Run(limit, func(t *testing.T) {
			w := diagnosticWitness()
			d := &witnessJSONWriter{w: w}
			if _, err := d.Write(make([]byte, 8<<20)); err != nil {
				t.Fatal(err)
			}
			before := w.steps
			polls := 0
			if limit == "step" {
				w.maxSteps = before
			} else {
				w.workTicks, w.search.workLeft = 0, 0
				w.stop = func() bool { polls++; return true }
			}
			if got := d.b.String(); got != "" || !w.stopped {
				t.Fatalf("final copy bypassed %s limit: %d bytes, stopped %v", limit, len(got), w.stopped)
			}
			if limit == "step" && w.steps != before+1 || limit == "wall" && (polls != 1 || w.workTicks != 1024) {
				t.Fatalf("final copy did not charge/poll: steps %d -> %d, polls %d, ticks %d", before, w.steps, polls, w.workTicks)
			}
			steps := w.steps
			if d.b.String() != "" || w.steps != steps {
				t.Fatal("assembly continued after cancellation")
			}
		})
	}
}

func TestWitnessDiagnosticGrowthCopiesBounded(t *testing.T) {
	w := diagnosticWitness()
	d := &witnessJSONWriter{w: w}
	part := make([]byte, 4096)
	var before, after runtime.MemStats
	var largest uint64
	for range (32 << 20) / len(part) {
		runtime.ReadMemStats(&before)
		if _, err := d.Write(part); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		largest = max(largest, after.TotalAlloc-before.TotalAlloc)
	}
	// Relocating strings.Builder's accumulated prefix used to allocate
	// and copy tens of MiB in a single 4 KiB Write, with no hook inside.
	// Growing the production storage must allocate bounded chunks only.
	if largest > 256<<10 {
		t.Fatalf("a 4 KiB Write relocated the accumulated prefix: largest allocation %d bytes", largest)
	}
	t.Logf("largest allocation while appending 4 KiB to 32 MiB output: %d bytes", largest)
	if d.b.Len() != 32<<20 {
		t.Fatalf("lost output during growth: %d bytes", d.b.Len())
	}
	// Assembly into the final string must itself poll. Merely returning
	// the old builder's contiguous prefix used no polls at all.
	w.workTicks, w.search.workLeft = 0, 0
	polls := 0
	w.stop = func() bool { polls++; return true }
	if got := d.b.String(); got != "" || !w.stopped || polls != 1 || w.workTicks != 1024 {
		t.Fatalf("final assembly bypassed stop: %d returned bytes, stopped %v, polls %d, ticks %d", len(got), w.stopped, polls, w.workTicks)
	}
}
