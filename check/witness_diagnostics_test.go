package check

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

func diagnosticWitness() *witness {
	w := newWitness(&history.History{}, time.Now().Add(time.Hour))
	w.search = newRevisionOrder(w)
	return w
}

func TestWitnessDiagnosticJSONExact(t *testing.T) {
	for _, v := range []any{
		nil,
		&history.Response{Revision: 4, Range: &history.RangeResponse{KVs: []history.KeyValue{{Key: "<&>\u2028", Value: "\xff\n\"\\", Version: 2}}}},
		&history.Response{Txn: &history.TxnResponse{Results: []history.TxnOpResponse{{Range: &history.RangeResponse{}}, {Delete: &history.DeleteResponse{Deleted: 1}}}}},
		&history.Request{Put: &history.PutRequest{Key: "key", Value: strings.Repeat("value", 1000)}},
	} {
		w := diagnosticWitness()
		got, want := w.jsonOf(v), jsonOf(v)
		if got != want {
			t.Fatalf("streaming changed diagnostic JSON: got %q, want %q", got, want)
		}
		wantSteps := int64((len(want) + diagnosticBytesPerStep - 1) / diagnosticBytesPerStep)
		if w.steps != wantSteps {
			t.Fatalf("%d bytes used %d steps, want %d", len(want), w.steps, wantSteps)
		}
	}
}

func TestWitnessDiagnosticJSONSharesCarry(t *testing.T) {
	w := diagnosticWitness()
	var bytes int
	for range 100 {
		got := w.jsonOf("x")
		bytes += len(got)
		want := int64((bytes + diagnosticBytesPerStep - 1) / diagnosticBytesPerStep)
		if got != `"x"` || w.steps != want {
			t.Fatalf("small messages rounded independently: %d bytes, %d steps, want %d", bytes, w.steps, want)
		}
	}
	// A bulk writer must poll inside a single large Write, not only
	// between encodings or after copying the entire input. A cancelled
	// prefix remains private and is discarded by jsonOf.
	w.stop = func() bool { return true }
	d := &witnessJSONWriter{w: w}
	if _, err := d.Write(make([]byte, 8<<20)); !errors.Is(err, ErrWitnessTimeout) || d.b.Len() == 0 || d.b.Len() >= 8<<20 || w.workTicks != 1024 {
		t.Fatalf("bulk diagnostic delayed cancellation: %v, %d bytes, %d ticks", err, d.b.Len(), w.workTicks)
	}
}

func TestWitnessDiagnosticJSONStops(t *testing.T) {
	response := &history.Response{Range: &history.RangeResponse{KVs: make([]history.KeyValue, 100_000)}}
	for i := range response.Range.KVs {
		response.Range.KVs[i] = history.KeyValue{Key: "k", Value: strings.Repeat("v", 100), Version: 1}
	}
	for _, limit := range []string{"step", "wall"} {
		t.Run(limit, func(t *testing.T) {
			w := diagnosticWitness()
			polls := 0
			if limit == "step" {
				w.maxSteps = 10
			} else {
				w.stop = func() bool { polls++; return true }
			}
			if got := w.jsonOf(response); got != "" || !w.stopped {
				t.Fatalf("cancelled diagnostic escaped: %d bytes, stopped %v", len(got), w.stopped)
			}
			if limit == "step" && w.steps != 11 {
				t.Fatalf("diagnostic bypassed budget: %d steps", w.steps)
			}
			if limit == "wall" && (polls != 1 || w.workTicks != 1024 || w.steps > 1024*64) {
				t.Fatalf("diagnostic delayed polling: %d polls, %d ticks, %d steps", polls, w.workTicks, w.steps)
			}
			steps := w.steps
			if w.jsonOf(response) != "" || w.steps != steps {
				t.Fatal("diagnostic continued after cancellation")
			}
			if err := w.unanswered(0, 0, 0, 0, 0); !errors.Is(err, ErrWitnessTimeout) {
				t.Fatalf("cancelled diagnostic invented a failure: %v", err)
			}
		})
	}
}
