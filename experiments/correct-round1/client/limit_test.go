package main

import (
	"github.com/djosh34/tigerstore/history"
	"testing"
)

func TestFitNearLimit(t *testing.T) {
	for _, extra := range []int{-64, -8, -7, 0, 1, 10} {
		for _, txn := range []bool{false, true} {
			p := &history.PutRequest{Key: "/correct-round1/k0", Value: "", PrevKV: true}
			q := history.Request{Put: p}
			if txn {
				q = history.Request{Txn: &history.TxnRequest{Compare: []history.Compare{{Key: p.Key, Target: history.TargetMod, Result: history.Equal, Number: 42}}, Success: []history.TxnOp{{Put: p}}, Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: p.Key}}}}}
			}
			fitLimit(&q, "seed-client-sequence:", extra)
			if got := framedSize(q); got != capBytes+extra {
				t.Fatalf("txn=%v extra=%d size=%d", txn, extra, got)
			}
			if len(p.Value) < capBytes-256 || len(p.Value) >= capBytes {
				t.Fatalf("unexpected admitted value size %d", len(p.Value))
			}
		}
	}
}
