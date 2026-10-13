package main

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

func TestWithoutLostWrites(t *testing.T) {
	put := func(v string) history.Request {
		return history.Request{Put: &history.PutRequest{Key: "k", Value: v}}
	}
	del := history.Request{Delete: &history.DeleteRequest{Key: "k"}}
	amb := &history.Error{Code: "Unknown", Ambiguous: true}
	h := &history.History{
		Ops: []history.Op{
			{Client: 0, Req: put("lost"), Err: amb},
			{Client: 0, Req: put("kept"), Err: amb},
			{Client: 1, Req: del, Resp: &history.Response{}},
			{Client: 1, Req: del, Err: amb},
			{Client: 2, Req: history.Request{Range: &history.RangeRequest{Key: "k"}}, Err: amb},
		},
		Log: []history.Request{put("kept"), del},
	}
	got, pruned := withoutLostWrites(h)
	// The lost put and the delete whose only log copy belongs to the
	// successful call go; the committed put and the read stay.
	if pruned != 2 || len(got.Ops) != 3 || got.Ops[0].Req.Put.Value != "kept" || got.Ops[1].Resp == nil || got.Ops[2].Req.Range == nil {
		t.Fatalf("pruned %d, kept %+v", pruned, got.Ops)
	}
	h.Log = append(h.Log, del)
	if _, pruned := withoutLostWrites(h); pruned != 1 {
		t.Fatalf("with a second delete in the log, pruned %d, want 1", pruned)
	}
	if len(h.Ops) != 5 {
		t.Fatal("the input history changed")
	}
}
