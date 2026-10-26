package check

import (
	"github.com/djosh34/tigerstore/history"
	"testing"
)

func TestFloor448Projection(t *testing.T) {
	makeHistory := func() *history.History {
		p := history.Request{Put: &history.PutRequest{Key: "k", Value: "unique"}}
		return &history.History{Log: []history.Request{p}, Target: 2, Ops: []history.Op{
			{Client: 1, Call: 1, Return: 2, Req: p, Resp: &history.Response{Revision: 2, Put: &history.PutResponse{}}},
			{Client: 2, Call: 3, Return: 4, Req: history.Request{Range: &history.RangeRequest{Key: "k"}}, Resp: &history.Response{Revision: 2, Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{{Key: "k", Value: "unique", CreateRevision: 2, ModRevision: 2, Version: 1}}}}},
		}}
	}
	for _, tc := range []struct {
		name string
		bad  bool
		edit func(*history.History)
	}{
		{"valid", false, func(*history.History) {}},
		{"bad data", true, func(h *history.History) { h.Ops[1].Resp.Range.KVs[0].Value = "wrong" }},
		{"bad version", true, func(h *history.History) { h.Ops[1].Resp.Range.KVs[0].Version = 2 }},
		{"stale read", true, func(h *history.History) { h.Ops[1].Resp.Revision = 1; h.Ops[1].Resp.Range = &history.RangeResponse{} }},
		{"ambiguous applied", false, func(h *history.History) { h.Ops[0].Resp = nil; h.Ops[0].Err = &history.Error{Ambiguous: true} }},
		{"ambiguous before call", true, func(h *history.History) {
			h.Ops[0].Resp = nil
			h.Ops[0].Err = &history.Error{Ambiguous: true}
			h.Ops[0].Call = 5
			h.Ops[0].Return = 6
		}},
		{"duplicate commit", true, func(h *history.History) { h.Log = append(h.Log, h.Log[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := makeHistory()
			tc.edit(h)
			err := Floor448(h)
			if (err != nil) != tc.bad {
				t.Fatalf("error=%v bad=%v", err, tc.bad)
			}
		})
	}
}
