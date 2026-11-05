package check

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// This is a checker regression, not a new simulator scenario. Changing
// the simulator's environment makes the old seed produce a different history.
// Preserve the actual failing input so the search change can be tested alone.
func TestWitness425(t *testing.T) {
	f, err := os.Open("testdata/witness-425.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck // read only
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close() //nolint:errcheck // read only
	var h history.History
	if err := json.NewDecoder(z).Decode(&h); err != nil {
		t.Fatal(err)
	}
	if err := h.Validate(); err != nil {
		t.Fatal(err)
	}
	// The revision witness must find a client-valid order without using
	// the committed log or its assignment of ambiguous writes to revisions.
	h.Log = nil
	for i := range h.Ops {
		h.Ops[i].Effect = nil
	}
	steps := int64(25000)
	if s := os.Getenv("DIAG_STEPS"); s != "" {
		steps, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	w := newWitness(&h, time.Now().Add(10*time.Minute))
	w.maxSteps = steps
	start := time.Now()
	err = w.fromRevisions()
	t.Logf("steps=%d backs=%d elapsed=%s error=%v", w.steps, w.search.backs, time.Since(start), err)
	if err != nil {
		t.Log(w.search.progress())
		for _, f := range w.search.stack {
			t.Logf("gap=%d tried=%d candidates=%v", f.rev, f.next, f.cands)
		}
		t.Fatal(err)
	}
	for j, e := range w.events {
		if rev := w.states[j+1].rev; 110 <= rev && rev <= 126 {
			t.Logf("revision=%d op=%d", rev, e.op)
		}
	}
	if err := Witnessed(&h, FromRevisions, time.Minute, MaxSteps(steps)); err != nil {
		t.Fatal(err)
	}
}

// The nearest count is irrelevant to key b. A later count already makes
// the prefix impossible, without trying any intervening gap permutations.
func TestReachableBeyondNearestCount(t *testing.T) {
	for _, tc := range []struct {
		name      string
		present   bool
		wantCount int64
		put, del  int64
		want      bool
	}{
		{name: "lost key", wantCount: 1},
		{name: "cannot delete surplus", present: true},
		{name: "creation before observation", wantCount: 1, put: 4, want: true},
		{name: "creation too late", wantCount: 1, put: 6},
		{name: "deletion before observation", present: true, del: 4, want: true},
		{name: "deletion too late", present: true, del: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newState([]string{"a", "b"})
			s.rev = 2
			s.kvs[0] = history.KeyValue{Key: "a", Version: 1}
			if tc.present {
				s.kvs[1] = history.KeyValue{Key: "b", Version: 1}
			}
			r := &revisionOrder{
				w:         &witness{h: &history.History{}},
				countRevs: []int64{3, 5},
				counts: map[int64][]count{
					3: {{rev: 3, lo: 0, hi: 1, n: 1}},
					5: {{rev: 5, lo: 1, hi: 2, n: tc.wantCount}},
				},
				putRevs: make([][]int64, 2), delRevs: make([][]int64, 2),
				putters: make([][]int, 2), deleters: make([][]int, 2),
			}
			if tc.put != 0 {
				r.putRevs[1] = []int64{tc.put}
			}
			if tc.del != 0 {
				r.delRevs[1] = []int64{tc.del}
			}
			if got := r.reachable(s, -1); got != tc.want {
				t.Fatalf("reachable=%v, want %v", got, tc.want)
			}
		})
	}
}
