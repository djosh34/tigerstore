package main

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRound3CompleteArchiveWinsCommitTie(t *testing.T) {
	var entries []*raftpb.Entry
	for i := uint64(1); i <= 3; i++ {
		entries = append(entries, &raftpb.Entry{Index: new(i), Term: new(uint64(1)), Type: raftpb.EntryNormal.Enum()})
	}
	got, err := committedPrefix([]recovered{
		{commit: 3, first: 3, entries: entries[2:], dir: "replacement"},
		{commit: 3, first: 1, entries: entries, dir: "survivor"},
	})
	if err != nil || len(got) != 3 {
		t.Fatalf("got %d entries, %v; want the full committed prefix", len(got), err)
	}
}

func TestRound3WatchRetriesLearner(t *testing.T) {
	for _, err := range []error{rpctypes.ErrGRPCNotSupportedForLearner, rpctypes.Error(rpctypes.ErrGRPCNotSupportedForLearner)} {
		if !retryWatch(err) {
			t.Fatalf("learner refusal must resume after promotion: %v", err)
		}
	}
	if retryWatch(rpctypes.ErrCompacted) {
		t.Fatal("compaction must not be hidden as a retry")
	}
}

func TestRound3CompactionFence(t *testing.T) {
	q := history.Request{Compact: &history.CompactRequest{Revision: 2}}
	if err := campaignRequest(q); err != nil {
		t.Fatal(err)
	}
	log := []history.Request{{Put: &history.PutRequest{Key: "k", Value: "v"}}, q, {Put: &history.PutRequest{Key: "k", Value: "w"}}}
	if rev, err := finalRevision(log); err != nil || rev != 3 {
		t.Fatalf("revision %d, error %v; want 3", rev, err)
	}
}

func TestRound3ReplacementLast(t *testing.T) {
	for seed := range uint64(100) {
		steps := plan(seed, []string{learnerLeader, rollingLoss, tripleLoss})
		if len(steps) != 3 || steps[2].Fault != learnerLeader {
			t.Fatalf("seed %d: replacement must follow all proxy cuts: %+v", seed, steps)
		}
	}
}
