package main

import (
	"testing"

	"github.com/djosh34/tigerstore/history"
)

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
