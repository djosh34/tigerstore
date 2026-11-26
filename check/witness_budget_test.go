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

// BenchmarkWitnessBudgetHistories measures the step conversion from base
// 37e51817 to #872 without either budget. Keep this benchmark and the two
// synthetic builders source-compatible with that base so both trees run
// exactly the same histories and allocation measurement.
func BenchmarkWitnessBudgetHistories(b *testing.B) {
	for _, name := range []string{
		"planted-B01a-1.json", "seed-5058843.json", "seed-5068434.json",
		"seed-59113215.json", "seed-77148.json", "seed-888737.json",
		"witness-425.json.gz", "witness-control-80-201.json.gz", "witness-control-320-1529.json.gz",
		"grants-81000", "gaps-256-expiries-4096",
	} {
		b.Run(name, func(b *testing.B) {
			var h *history.History
			switch name {
			case "grants-81000":
				h = sameRevisionGrants(81_000)
			case "gaps-256-expiries-4096":
				h = dueGapHistory(b, 4096, 256)
			default:
				f, err := os.Open("testdata/" + name)
				if err != nil {
					b.Fatal(err)
				}
				decoder := json.NewDecoder(f)
				if strings.HasSuffix(name, ".gz") {
					z, err := gzip.NewReader(f)
					if err != nil {
						b.Fatal(err)
					}
					decoder = json.NewDecoder(z)
				}
				h = &history.History{}
				err = decoder.Decode(h)
				if closeErr := f.Close(); closeErr != nil {
					b.Fatal(closeErr)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			h.Log = nil
			for i := range h.Ops {
				h.Ops[i].Effect = nil
			}
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
