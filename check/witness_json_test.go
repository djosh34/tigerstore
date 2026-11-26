package check

import (
	"errors"
	"strings"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

func TestWitnessJSONScalarExact(t *testing.T) {
	var all []byte
	for i := range 256 {
		all = append(all, byte(i))
	}
	input := strings.Repeat(string(all)+"\u2028\u2029\ufffd😀<&>\"\\", 40)
	w := diagnosticWitness()
	for n := 0; n <= len(input); n += 127 {
		v := &history.Response{Range: &history.RangeResponse{KVs: []history.KeyValue{{Key: input[:n], Value: input[:n], Lease: -1, CreateRevision: -2}}}}
		if got, want := w.jsonOf(v), jsonOf(v); got != want {
			t.Fatalf("escaped scalar length %d differs from v1 JSON", n)
		}
	}
}

func TestWitnessJSONScalarWallStop(t *testing.T) {
	input := strings.Repeat("<", 1<<20)
	want := jsonOf(input)
	w := diagnosticWitness()
	d := &witnessJSONWriter{w: w}
	observed := 0
	w.stop = func() bool {
		observed = d.b.Len()
		return true
	}
	if err := d.quoted(input); !errors.Is(err, ErrWitnessTimeout) {
		t.Fatalf("large scalar missed wall hook: %v", err)
	}
	if observed == 0 || observed >= len(want) || observed > 4<<20 || d.b.String() != want[:observed] {
		t.Fatalf("escaped/copied whole scalar before polling: %d of %d bytes", observed, len(want))
	}
	if w.jsonOf(input) != "" {
		t.Fatal("partial scalar escaped after cancellation")
	}
}

func TestWitnessJSONSavedHistoriesExact(t *testing.T) {
	for _, name := range []string{
		"planted-B01a-1.json", "seed-5058843.json", "seed-5068434.json",
		"seed-59113215.json", "seed-77148.json", "seed-888737.json",
		"witness-425.json.gz", "witness-control-80-201.json.gz", "witness-control-320-1529.json.gz",
		"grants-81000", "gaps-256-expiries-4096",
	} {
		t.Run(name, func(t *testing.T) {
			h := budgetHistory(t, name)
			w := diagnosticWitness()
			for i, op := range h.Ops {
				for _, v := range []any{op.Req, op.Resp} {
					if got, want := w.jsonOf(v), jsonOf(v); got != want {
						t.Fatalf("op %d diagnostic differs from v1 JSON", i)
					}
				}
			}
		})
	}
}
