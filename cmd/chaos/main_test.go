package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/history"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// TestFaultsOverlapWrites checks that a fault no write overlaps is marked
// so, and that reads alone do not count.
func TestFaultsOverlapWrites(t *testing.T) {
	put := history.Op{Call: int64(2 * time.Second), Return: int64(2100 * time.Millisecond), Req: history.Request{Put: &history.PutRequest{}}}
	read := history.Op{Call: int64(5 * time.Second), Return: int64(5100 * time.Millisecond), Req: history.Request{Range: &history.RangeRequest{}}}
	h := &history.History{Ops: []history.Op{put, read}}
	records := []record{
		{Step: 1, Installed: true, Requested: time.Second, HealedAt: 3 * time.Second},
		{Step: 2, Installed: true, Requested: 4 * time.Second, HealedAt: 6 * time.Second},
		{Step: 3, Requested: 4 * time.Second},
	}
	faultsOverlapWrites(records, h)
	if !records[0].Overlapped || records[1].Overlapped || records[2].Overlapped {
		t.Fatalf("overlaps: a fault with a write inside %v, one only a read overlaps %v, one not installed %v",
			records[0].Overlapped, records[1].Overlapped, records[2].Overlapped)
	}
}

// TestTally counts, over the runs saved under a directory, the runs in
// which each fault took effect, and fails a fault that took effect in
// fewer than three in four of them.
func TestTally(t *testing.T) {
	dir := t.TempDir()
	took := record{Planned: step{Fault: netDrop}, Installed: true, Effect: true, Overlapped: true}
	missed := record{Planned: step{Fault: netDrop}, Installed: true, Effect: true}
	kill := record{Planned: step{Fault: killLeader}, Installed: true, Effect: true, Overlapped: true}
	for i, run := range [][]record{{took, kill}, {took, kill}, {took, kill}, {missed, kill}} {
		sub := filepath.Join(dir, fmt.Sprintf("seed-%d", i))
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := save(filepath.Join(sub, "faults.json"), run); err != nil {
			t.Fatal(err)
		}
	}
	nop := func(string, ...any) {}
	if ok, err := tally([]string{dir}, nop); err != nil || !ok {
		t.Fatalf("3 of 4 runs: ok %v, %v", ok, err)
	}
	if err := save(filepath.Join(dir, "seed-1", "faults.json"), []record{missed, kill}); err != nil {
		t.Fatal(err)
	}
	if ok, err := tally([]string{dir}, nop); err != nil || ok {
		t.Fatalf("2 of 4 runs: ok %v, %v", ok, err)
	}
}

// TestFailedStart runs with a binary that does not exist: the run must
// report the failed start, not crash while stopping nodes that never ran.
func TestFailedStart(t *testing.T) {
	err := run(config{Seed: 1, Tigerstore: filepath.Join(t.TempDir(), "missing"), Faults: allFaults}, t.TempDir())
	var steps []string
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, e := range joined.Unwrap() {
			if se, ok := errors.AsType[*stepError](e); ok {
				steps = append(steps, se.step)
			}
		}
	}
	if !slices.Equal(steps, []string{"start"}) || !errors.Is(err, fs.ErrNotExist) {
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
