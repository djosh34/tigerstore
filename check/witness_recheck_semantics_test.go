package check

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/djosh34/tigerstore/history"
)

func TestTxnBeforeReadsOverlayFirstWrites(t *testing.T) {
	for _, events := range []bool{false, true} {
		t.Run(strconv.FormatBool(events), func(t *testing.T) {
			st := &history.Start{Rev: 2}
			for _, key := range []string{"a", "b", "c", "e"} {
				st.KVs = append(st.KVs, history.KeyValue{Key: key, Value: "old", CreateRevision: 2, ModRevision: 2, Version: 1})
			}
			s := newState([]string{"a", "b", "c", "d", "e"}, st)
			s.compacted = -1
			s.past = versions{}
			s.past.add(startEvents(st))
			txn := &history.TxnRequest{Success: []history.TxnOp{
				{Put: &history.PutRequest{Key: "a", Value: "first"}},
				{Delete: &history.DeleteRequest{Key: "b", End: "d"}},
				{Put: &history.PutRequest{Key: "a", Value: "last"}},
				{Put: &history.PutRequest{Key: "d", Value: "created"}},
				{Range: &history.RangeRequest{Key: "a", End: history.AllKeys, Revision: 2}},
				{Range: &history.RangeRequest{Key: "a", End: history.AllKeys}},
				{Range: &history.RangeRequest{Key: "b", End: "d", Revision: 2, Limit: 1, KeysOnly: true}},
				{Range: &history.RangeRequest{Key: "a", End: history.AllKeys, Revision: 1}},
				{Range: &history.RangeRequest{Key: "c", Revision: 2, CountOnly: true}},
			}}
			o := s.stepTxn(txn, events)
			if o.err != nil {
				t.Fatal(o.err)
			}
			got := o.resp.Txn.Results
			if !slices.Equal(got[4].Range.KVs, st.KVs) || got[4].Range.Count != 4 {
				t.Fatalf("start-revision read lost original/removed keys: %+v", got[4].Range)
			}
			live := got[5].Range
			if live.Count != 3 || len(live.KVs) != 3 || live.KVs[0].Key != "a" || live.KVs[0].Value != "last" || live.KVs[1].Key != "d" || live.KVs[2].Key != "e" || live.KVs[2].Value != "old" {
				t.Fatalf("current read lost preceding writes: %+v", live)
			}
			page := got[6].Range
			if page.Count != 2 || !page.More || len(page.KVs) != 1 || page.KVs[0].Key != "b" || page.KVs[0].Value != "" {
				t.Fatalf("before overlay lost range options: %+v", page)
			}
			if got[7].Range.Count != 0 || got[8].Range.Count != 1 || len(got[8].Range.KVs) != 0 {
				t.Fatal("past/count-only reads used the wrong versions")
			}
			if events && (len(o.events) != 5 || o.events[0].PrevKV == nil || o.events[3].PrevKV == nil || *o.events[0].PrevKV != st.KVs[0] || *o.events[3].PrevKV != st.KVs[0] || o.events[4].PrevKV != nil) {
				t.Fatalf("copy-on-write changed event previous values: %+v", o.events)
			}
		})
	}
}

func TestCovererMergeMatchesSortedUnion(t *testing.T) {
	for _, n := range []int{0, 16, 2048, 2049, 10_000} {
		for _, sources := range []int{1, 8} {
			t.Run(fmt.Sprintf("%d/%d", n, sources), func(t *testing.T) {
				rng := rand.New(rand.NewPCG(857, uint64(n)))
				r := dueShape(0)
				r.deleters = [][]int{nil}
				r.room = &room{leasesOf: make([][]int64, 1), revokesOf: map[int64][]int{}}
				var want []int
				for i := range sources {
					ops := make([]int, n)
					for j := range ops {
						ops[j] = rng.IntN(max(n, 1)) - n/2
					}
					slices.Sort(ops)
					id := int64(i + 1)
					r.room.leasesOf[0] = append(r.room.leasesOf[0], id)
					r.room.revokesOf[id] = ops
					want = append(want, ops...)
				}
				slices.Sort(want)
				want = slices.Compact(want)
				s := newState([]string{"k"}, nil)
				// Abandon an iterator, then repeat it: consuming the heap
				// must not consume or reorder the retained source lists.
				for range r.coverers(s, []int{0, 1}) {
					break
				}
				for range 2 {
					got := slices.Collect(r.coverers(s, []int{0, 1}))
					if !slices.Equal(got, want) {
						t.Fatalf("union mismatch: %d got, %d want", len(got), len(want))
					}
				}
			})
		}
	}
}
