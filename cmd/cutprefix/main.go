// Command cutprefix (exp branch only, #499) writes the prefix of a
// history up to its first n calls: the ops called before the call of the
// (n+1)th, a write still pending then made ambiguous and a read or a
// failed op pending then left out, and the watch responses before that
// time with their events up to the highest revision a kept acked op
// answered. Every write such an event reports was called before the cut,
// so the prefix of a true history is true.
package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"

	"github.com/djosh34/tigerstore/history"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: cutprefix in.json.gz n out.json.gz")
		os.Exit(2)
	}
	n, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	z, err := gzip.NewReader(f)
	if err != nil {
		panic(err)
	}
	var h history.History
	if err := json.NewDecoder(z).Decode(&h); err != nil {
		panic(err)
	}
	calls := make([]int64, len(h.Ops))
	for i, op := range h.Ops {
		calls[i] = op.Call
	}
	slices.Sort(calls)
	cut := calls[n]
	out := history.History{Target: h.Target}
	top := int64(0)
	for _, op := range h.Ops {
		if op.Call >= cut {
			continue
		}
		if op.Return >= cut {
			if !op.Req.Mutates() {
				continue
			}
			op.Resp, op.Err, op.Effect = nil, &history.Error{Code: "Unavailable", Message: "pending at the cut", Ambiguous: true}, nil
			op.Return = cut
		}
		if op.Err != nil && !op.Err.Ambiguous && op.Return >= cut {
			continue
		}
		if op.Resp != nil {
			top = max(top, op.Resp.Revision)
		}
		out.Ops = append(out.Ops, op)
	}
	for _, w := range h.Watches {
		var rs []history.WatchResponse
		for _, r := range w.Responses {
			if r.Time >= cut {
				break
			}
			had := len(r.Events) > 0
			r.Events = slices.DeleteFunc(slices.Clone(r.Events), func(e history.Event) bool { return e.Revision() > top })
			if had && len(r.Events) == 0 || r.Revision > top {
				break
			}
			rs = append(rs, r)
		}
		w.Responses = rs
		out.Watches = append(out.Watches, w)
	}
	of, err := os.Create(os.Args[3])
	if err != nil {
		panic(err)
	}
	zw := gzip.NewWriter(of)
	if err := json.NewEncoder(zw).Encode(&out); err != nil {
		panic(err)
	}
	if err := zw.Close(); err != nil {
		panic(err)
	}
	fmt.Printf("prefix of %d calls up to %d: %d ops, %d watches, top revision %d\n", n, cut, len(out.Ops), len(out.Watches), top)
}
