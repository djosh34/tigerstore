package main

import (
	"reflect"
	"testing"

	"github.com/djosh34/tigerstore/check"
	"github.com/djosh34/tigerstore/history"
)

func TestProofRevisionOffset(t *testing.T) {
	const base = 123
	kv := history.KeyValue{Key: proofPrefix + "k", Value: "v", CreateRevision: base + 2, ModRevision: base + 2, Version: 1, Lease: 900}
	h := history.History{
		Target: base + 2,
		Ops: []history.Op{{
			Client: 1, Call: 1, Return: 2,
			Req:  history.Request{Range: &history.RangeRequest{Key: kv.Key}},
			Resp: &history.Response{Revision: base + 2, Range: &history.RangeResponse{Count: 1, KVs: []history.KeyValue{kv}}},
		}},
		Watches: []history.Watch{{Req: history.WatchRequest{StartRevision: base + 2}, Responses: []history.WatchResponse{{Revision: base + 2, Events: []history.Event{{Type: history.EventPut, KV: kv, PrevKV: &kv}}}}}},
		Log:     []history.Request{{Txn: &history.TxnRequest{Compare: []history.Compare{{Target: history.TargetMod, Number: base + 2}, {Target: history.TargetVersion, Number: 9}, {Target: history.TargetMod, Number: 0}}}}, {Compact: &history.CompactRequest{Revision: base + 2}}},
	}
	if err := shiftRevisions(reflect.ValueOf(&h), base); err != nil {
		t.Fatal(err)
	}
	got := h.Ops[0].Resp.Range.KVs[0]
	if got.CreateRevision != 2 || got.ModRevision != 2 || got.Lease != 900 || got.Version != 1 || h.Target != 2 || h.Ops[0].Resp.Revision != 2 || h.Ops[0].Req.Range.Revision != 0 {
		t.Fatalf("bad revision shift: %+v %+v", h, got)
	}
	cmps := h.Log[0].Txn.Compare
	if cmps[0].Number != 2 || cmps[1].Number != 9 || cmps[2].Number != 0 || h.Log[1].Compact.Revision != 2 || h.Watches[0].Responses[0].Events[0].PrevKV.ModRevision != 2 {
		t.Fatal("request or watch shift changed non-revision fields")
	}
	bad := history.KeyValue{ModRevision: base}
	if shiftRevisions(reflect.ValueOf(&bad), base) == nil {
		t.Fatal("accepted metadata from the excluded preload")
	}
}

// A missing watch event must still fail after the offset. Offset handling
// cannot turn a partial watch into a complete one.
func TestProofWatchCompleteness(t *testing.T) {
	put := history.Request{Put: &history.PutRequest{Key: proofPrefix + "k", Value: "v"}}
	h := history.History{
		Target:  102,
		Log:     []history.Request{put},
		Ops:     []history.Op{{Client: 1, Call: 1, Return: 2, Req: put, Resp: &history.Response{Revision: 102, Put: &history.PutResponse{}}}},
		Watches: []history.Watch{{Req: history.WatchRequest{Key: proofPrefix, End: "/proof0", StartRevision: 102}, Responses: []history.WatchResponse{{Time: 3, Revision: 102, Progress: true}}}},
	}
	if err := shiftRevisions(reflect.ValueOf(&h), 100); err != nil {
		t.Fatal(err)
	}
	if err := check.Watches(&h); err == nil {
		t.Fatal("missing event passed the watch checker")
	}
	h.Watches[0].Responses = []history.WatchResponse{{Time: 3, Revision: 2, Events: []history.Event{{Type: history.EventPut, KV: history.KeyValue{Key: proofPrefix + "k", Value: "v", Version: 1, CreateRevision: 2, ModRevision: 2}}}}}
	if err := check.Watches(&h); err != nil {
		t.Fatal(err)
	}
}
