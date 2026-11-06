// Command scanprobe (exp only, never merged) times linear passes over a
// history's ops, per op, to tell the witness's growth with the history's
// length from the machine's: each pass does the same work per op.
package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/djosh34/tigerstore/history"
)

func main() {
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
	n := len(h.Ops)
	var sink int64
	pass := func(name string, fn func()) {
		start := time.Now()
		fn()
		fmt.Printf("PROBE %s %.4f\n", name, float64(time.Since(start).Nanoseconds())/float64(n))
	}
	pass("fields", func() {
		for i := range h.Ops {
			sink += h.Ops[i].Return - h.Ops[i].Call
		}
	})
	pass("resp", func() {
		for i := range h.Ops {
			if r := h.Ops[i].Resp; r != nil {
				sink += r.Revision
			}
		}
	})
	pass("req", func() {
		for i := range h.Ops {
			if p := h.Ops[i].Req.Put; p != nil {
				sink += int64(len(p.Key)) + p.Lease
			}
			if t := h.Ops[i].Req.Txn; t != nil {
				sink += int64(len(t.Success))
			}
		}
	})
	byCall := make([]int, n)
	for i := range byCall {
		byCall[i] = i
	}
	pass("sortcall", func() {
		slices.SortFunc(byCall, func(a, b int) int {
			if h.Ops[a].Call < h.Ops[b].Call {
				return -1
			}
			if h.Ops[a].Call > h.Ops[b].Call {
				return 1
			}
			return a - b
		})
	})
	pass("bycallresp", func() {
		for _, i := range byCall {
			if r := h.Ops[i].Resp; r != nil {
				sink += r.Revision
			}
		}
	})
	col := make([]int64, n)
	pass("column", func() {
		for i := range h.Ops {
			col[i] = h.Ops[i].Call
		}
		for _, i := range byCall {
			sink += col[i]
		}
	})
	m := map[int64]int{}
	pass("map", func() {
		for i := range h.Ops {
			if r := h.Ops[i].Resp; r != nil {
				m[r.Revision] = i
			}
		}
		for i := range h.Ops {
			if r := h.Ops[i].Resp; r != nil {
				sink += int64(m[r.Revision])
			}
		}
	})
	fmt.Fprintln(os.Stderr, sink)
}
