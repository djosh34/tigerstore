package check

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

// TestWitnessHardHistories checks histories on which the witness from the
// revisions reached its step limit, with no log and no effects to help
// it: the search must find an order within the steps given.
//   - witness-425: #425's seed, saved from main at bd1ade31, where it
//     timed out after 50,000,000 steps; the seed no longer plays out that
//     way (#482). It needs about 21,500 steps.
//   - witness-control-80-201: the unplanted control seed 201 of #485's
//     sweep with 80 s of faults, replayed on exp/485-sim-ceiling at
//     0a63d065, which timed out after 50,000,000 steps; it needs fewer
//     than 10,000 (#425, #499).
func TestWitnessHardHistories(t *testing.T) {
	for _, c := range []struct {
		file  string
		steps int64
	}{
		{"witness-425.json.gz", 25_000},
		{"witness-control-80-201.json.gz", 10_000},
	} {
		t.Run(c.file, func(t *testing.T) {
			f, err := os.Open("testdata/" + c.file)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close() //nolint:errcheck // read only
			z, err := gzip.NewReader(f)
			if err != nil {
				t.Fatal(err)
			}
			var h history.History
			if err := json.NewDecoder(z).Decode(&h); err != nil {
				t.Fatal(err)
			}
			h.Log = nil
			for i := range h.Ops {
				h.Ops[i].Effect = nil
			}
			if err := Witnessed(&h, FromRevisions, 10*time.Minute, MaxSteps(c.steps)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestReachableSeesCountsBeyondTheNearest has a count of key a at revision
// 3 and one of key b at revision 5, and requires reachable to answer by the
// count of b, which the nearest count says nothing about.
func TestReachableSeesCountsBeyondTheNearest(t *testing.T) {
	for _, c := range []struct {
		name          string
		present       bool
		count         int64
		putAt, delAt  int64
		wantReachable bool
	}{
		{name: "b is gone and no write creates it", count: 1},
		{name: "b is there and no write deletes it", present: true},
		{name: "a put creates b in time", count: 1, putAt: 4, wantReachable: true},
		{name: "a put creates b too late", count: 1, putAt: 6},
		{name: "a delete removes b in time", present: true, delAt: 4, wantReachable: true},
		{name: "a delete removes b too late", present: true, delAt: 6},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newState([]string{"a", "b"})
			s.rev = 2
			s.kvs[0] = history.KeyValue{Key: "a", Version: 1}
			if c.present {
				s.kvs[1] = history.KeyValue{Key: "b", Version: 1}
			}
			r := &revisionOrder{
				w:         &witness{h: &history.History{}},
				countRevs: []int64{3, 5},
				counts: revisionLists[count]{sparse: map[int64][]count{
					3: {{rev: 3, lo: 0, hi: 1, n: 1}},
					5: {{rev: 5, lo: 1, hi: 2, n: c.count}},
				}},
				putRevs: make([][]int64, 2), delRevs: make([][]int64, 2),
				putters: make([][]int, 2), deleters: make([][]int, 2),
			}
			if c.putAt != 0 {
				r.putRevs[1] = []int64{c.putAt}
			}
			if c.delAt != 0 {
				r.delRevs[1] = []int64{c.delAt}
			}
			if got := r.reachable(s, -1); got != c.wantReachable {
				t.Fatalf("reachable = %v, want %v", got, c.wantReachable)
			}
		})
	}
}
