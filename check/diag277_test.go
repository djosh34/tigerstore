package check

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

// Saved-history reductions, not simulator replays. Requests, responses and
// timestamps are unchanged. Effects and watches are omitted.
// 8143538: run 39170367649, sim-campaign-8; original ops
// 54,55,56,63,66,67,70,71,72,75,76,81,82,84, log [1:13].
// 8200093 and 8200334: run 39185668816, sim-campaign-0; keep mutations
// through the last successful historical read below the ambiguous compaction,
// their log prefix, the read, and earlier compacted rejections.
func TestDiag277SavedHistory(t *testing.T) {
	for _, seed := range []int{8143538, 8200093, 8200334} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			data, err := os.ReadFile(fmt.Sprintf("testdata/diag277-seed-%d.json", seed))
			if err != nil {
				t.Fatal(err)
			}
			var h history.History
			if err := json.Unmarshal(data, &h); err != nil {
				t.Fatal(err)
			}
			t.Logf("%d saved operations", len(h.Ops))
			t.Run("porcupine", func(t *testing.T) {
				mustPass(t, "Linearizable", Linearizable(&h, timeout))
			})
			for _, mode := range bothModes {
				t.Run(mode.String(), func(t *testing.T) {
					mustPass(t, "Witnessed "+mode.String(), Witnessed(&h, mode, timeout))
				})
			}
		})
	}
}

// A smaller optional compaction must sometimes precede the protected read too:
// compact(1) rejects at compaction 1, while range(1) still succeeds there.
func TestDiag277RejectionPredecessor(t *testing.T) {
	b := newBuilder(t, "a")
	b.put(0, "a", "1")
	b.put(0, "a", "2")
	low := b.do(1, history.Request{Compact: &history.CompactRequest{Revision: 1}})
	b.do(2, history.Request{Compact: &history.CompactRequest{Revision: 1}})
	b.do(3, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 1}})
	b.get(4, "a")
	high := b.do(5, history.Request{Compact: &history.CompactRequest{Revision: 2}})
	h := b.done()
	h.Ops[high].Call = 1
	for _, i := range []int{low, high} {
		h.Ops[i].Resp, h.Ops[i].Err = nil, errUnavailable
	}
	mustPass(t, "Linearizable", Linearizable(h, timeout))
	for _, mode := range bothModes {
		mustPass(t, "Witnessed "+mode.String(), Witnessed(h, mode, timeout))
	}
}

// A pre-compaction state at the right revision is not enough: it must be
// after the grant that returned before the successful read was called.
func TestDiag277ReadPredecessor(t *testing.T) {
	b := newBuilder(t, "a")
	b.put(0, "a", "1")
	b.put(0, "a", "2")
	b.do(0, history.Request{LeaseGrant: &history.LeaseGrantRequest{ID: 42, TTL: 10}})
	read := b.do(1, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
	compact := b.do(2, history.Request{Compact: &history.CompactRequest{Revision: 3}})
	gone := b.do(3, history.Request{Range: &history.RangeRequest{Key: "a", Revision: 2}})
	b.do(4, history.Request{Range: &history.RangeRequest{Key: "a"}})
	h := b.done()
	h.Ops[compact].Call = 1
	h.Ops[compact].Resp, h.Ops[compact].Err = nil, errUnavailable
	for _, mode := range bothModes {
		mustPass(t, "Witnessed "+mode.String(), Witnessed(h, mode, timeout))
	}
	mustPass(t, "Linearizable", Linearizable(h, timeout))

	// A compacted rejection that completes before the successful read is
	// called makes this impossible. Do not fix it by skipping the rejection,
	// weakening response checks, or ignoring real time.
	h.Ops[gone].Call, h.Ops[gone].Return = h.Ops[read].Call-3, h.Ops[read].Call-1
	mustFail(t, "Linearizable", Linearizable(h, timeout), "")
	for _, mode := range bothModes {
		mustFail(t, "Witnessed "+mode.String(), Witnessed(h, mode, timeout), "")
	}
}
