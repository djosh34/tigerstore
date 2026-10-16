package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
	"go.etcd.io/etcd/api/v3/mvccpb"
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
	got, pruned, err := withoutLostWrites(h)
	if err != nil {
		t.Fatal(err)
	}
	// The lost put and the delete whose only log copy belongs to the
	// successful call go; the committed put and the read stay.
	if pruned != 2 || len(got.Ops) != 3 || got.Ops[0].Req.Put.Value != "kept" || got.Ops[1].Resp == nil || got.Ops[2].Req.Range == nil {
		t.Fatalf("pruned %d, kept %+v", pruned, got.Ops)
	}
	h.Log = append(h.Log, del)
	if _, pruned, err := withoutLostWrites(h); err != nil || pruned != 1 {
		t.Fatalf("with a second delete in the log, pruned %d, want 1", pruned)
	}
	if len(h.Ops) != 5 {
		t.Fatal("the input history changed")
	}
}

// TestFaultsOverlapWrites checks that a fault no write overlaps fails the
// run, and that reads alone do not count.
func TestFaultsOverlapWrites(t *testing.T) {
	put := history.Op{Call: int64(2 * time.Second), Return: int64(2100 * time.Millisecond), Req: history.Request{Put: &history.PutRequest{}}}
	read := history.Op{Call: int64(5 * time.Second), Return: int64(5100 * time.Millisecond), Req: history.Request{Range: &history.RangeRequest{}}}
	h := &history.History{Ops: []history.Op{put, read}}
	covered := record{Step: 1, Installed: true, Requested: time.Second, HealedAt: 3 * time.Second}
	if err := faultsOverlapWrites([]record{covered}, h); err != nil {
		t.Fatalf("a fault with a write inside: %v", err)
	}
	onlyRead := record{Step: 2, Installed: true, Requested: 4 * time.Second, HealedAt: 6 * time.Second}
	if err := faultsOverlapWrites([]record{covered, onlyRead}, h); err == nil {
		t.Fatal("a fault that only a read overlaps passed")
	}
	if err := faultsOverlapWrites([]record{{Step: 3, Requested: 4 * time.Second}}, h); err != nil {
		t.Fatalf("a fault that was not installed: %v", err)
	}
}

// TestFailedStart runs with a binary that does not exist: the run must
// report the failed start, not crash while stopping nodes that never ran.
func TestFailedStart(t *testing.T) {
	err := run(config{Seed: 1, Tigerstore: filepath.Join(t.TempDir(), "missing"), Faults: allFaults}, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "start: ") || strings.Contains(err.Error(), "stop: ") {
		t.Fatalf("got %v, want only the failed start", err)
	}
}

// A watch that drops one of the two delete events of one revision
// differs from one that got both.
func TestSameEvents(t *testing.T) {
	ref := []watchEvent{{2, "a", mvccpb.PUT}, {3, "a", mvccpb.DELETE}, {3, "b", mvccpb.DELETE}, {4, "a", mvccpb.PUT}, {5, "c", mvccpb.PUT}}
	if err := sameEvents(ref[:4], ref, 4); err != nil {
		t.Errorf("same events up to 4: %v", err)
	}
	if err := sameEvents([]watchEvent{ref[0], ref[1], ref[3]}, ref, 4); err == nil {
		t.Error("a dropped delete at a revision with two passed")
	}
	if err := sameEvents(ref[:2], ref, 2); err == nil {
		t.Error("an event no other watch got passed")
	}
}
