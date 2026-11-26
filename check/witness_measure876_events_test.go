package check

import (
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
)

func TestMeasure876EventCounterexamples(t *testing.T) {
	cases := []struct {
		name    string
		req     history.Request
		present bool
		want    bool
	}{
		{"write-then-undo", history.Request{Txn: &history.TxnRequest{Success: []history.TxnOp{{Put: &history.PutRequest{Key: "b", Value: "v"}}, {Delete: &history.DeleteRequest{Key: "a", End: history.AllKeys}}}}}, false, false},
		{"zero-count-cannot-cover-put", history.Request{Put: &history.PutRequest{Key: "b", Value: "v"}}, false, false},
		{"unchanged-delete", history.Request{Delete: &history.DeleteRequest{Key: "a"}}, true, true},
		{"read-only-failed-cas", history.Request{Txn: &history.TxnRequest{Compare: []history.Compare{{Key: "b", Target: history.TargetMod, Result: history.Equal, Number: 2}}, Success: []history.TxnOp{{Put: &history.PutRequest{Key: "b", Value: "v"}}}, Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: "b"}}}}}, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := &history.Start{Rev: 3}
			if tc.present {
				st.KVs = []history.KeyValue{{Key: "a", Value: "v", CreateRevision: 2, ModRevision: 2, Version: 1}}
			}
			h := &history.History{Start: st, Ops: []history.Op{{Req: tc.req, Err: errUnavailable}}}
			r := newRevisionOrder(newWitness(h, time.Now().Add(time.Minute)))
			r.top = 4
			r.covered["a"] = []span{{2, 4}}
			// This count must not justify rejecting an absent-key put.
			r.counts[4] = []count{{lo: 0, hi: len(r.w.keys), n: 0}}
			c := measure876EventCertificate(r, 4, []int{0})
			if c.Certified != tc.want {
				t.Fatalf("got %+v, want certified %v", c, tc.want)
			}
		})
	}
}
