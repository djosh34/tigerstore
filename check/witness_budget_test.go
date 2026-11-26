package check

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// budgetHistory and the two synthetic builders stay source-compatible with
// base 37e51817, so both trees measure exactly the same inputs.
func budgetHistory(t testing.TB, name string) *history.History {
	t.Helper()
	var h *history.History
	switch name {
	case "grants-81000":
		h = sameRevisionGrants(81_000)
	case "gaps-256-expiries-4096":
		h = dueGapHistory(t, 4096, 256)
	default:
		f, err := os.Open("testdata/" + name)
		if err != nil {
			t.Fatal(err)
		}
		decoder := json.NewDecoder(f)
		if strings.HasSuffix(name, ".gz") {
			z, err := gzip.NewReader(f)
			if err != nil {
				t.Fatal(err)
			}
			decoder = json.NewDecoder(z)
		}
		h = &history.History{}
		err = decoder.Decode(h)
		if closeErr := f.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	h.Log = nil
	for i := range h.Ops {
		h.Ops[i].Effect = nil
	}
	return h
}

// BenchmarkWitnessBudgetHistories measures the step conversion without
// either tree's budget. Copy this file and the synthetic builders to base.
func BenchmarkWitnessBudgetHistories(b *testing.B) {
	for _, name := range []string{
		"planted-B01a-1.json", "seed-5058843.json", "seed-5068434.json",
		"seed-59113215.json", "seed-77148.json", "seed-888737.json",
		"witness-425.json.gz", "witness-control-80-201.json.gz", "witness-control-320-1529.json.gz",
		"grants-81000", "gaps-256-expiries-4096",
	} {
		b.Run(name, func(b *testing.B) {
			h := budgetHistory(b, name)
			var steps int64
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				w := newWitness(h, time.Now().Add(time.Hour))
				if err := w.fromRevisions(); errors.Is(err, ErrWitnessTimeout) {
					b.Fatal(err)
				}
				steps = w.steps
			}
			b.ReportMetric(float64(steps), "steps/op")
		})
	}
}
