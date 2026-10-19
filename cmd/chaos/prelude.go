package main

import (
	"fmt"
	"github.com/djosh34/tigerstore/history"
)

// A short targeted fault can finish before the random clients happen to
// delete two keys together. Exercise the unchanged coverage floor first,
// on keys the random clients never touch. Watches cover these calls too.
func (t *traffic) coveragePrelude() error {
	key := prefix + "coverage-cas"
	put := func(k, v string) history.Request {
		return history.Request{Put: &history.PutRequest{Key: k, Value: v, PrevKV: true}}
	}
	var calls []history.Request
	calls = append(calls, put(key, "first"))
	for _, version := range []int64{1, -1} {
		calls = append(calls, history.Request{Txn: &history.TxnRequest{
			Compare: []history.Compare{{Key: key, Target: history.TargetVersion, Result: history.Equal, Number: version}},
			Success: []history.TxnOp{{Put: &history.PutRequest{Key: key, Value: "second", PrevKV: true}}},
			Failure: []history.TxnOp{{Range: &history.RangeRequest{Key: key}}},
		}})
	}
	del := prefix + "coverage-delete/"
	calls = append(calls, put(del+"a", "a"), put(del+"b", "b"), history.Request{Delete: &history.DeleteRequest{Key: del, End: prefix + "coverage-delete0", PrevKV: true}})
	for _, q := range calls {
		op := t.r.call(t.cs[0], 90, q, t.cfg.requestTimeout)
		if op.Resp == nil {
			return fmt.Errorf("coverage prelude: %+v", op.Err)
		}
	}
	return nil
}
