package core

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/wal"
)

func TestSnapshotSendError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		stop bool
	}{
		{"success", nil, false},
		{"connection lost", io.ErrClosedPipe, false},
		{"snapshot no longer available", errNoSnapshot, false},
		{"corrupt record", &wal.RecordError{Entry: 8, Segment: 1, Reason: "does not verify"}, true},
		{"wrapped corruption", fmt.Errorf("snapshot: %w", wal.ErrCorrupt), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newMemCluster(t, func(*Config) {})
			// The serve can finish after the sender loses leadership.
			// Local corruption must stop a follower too.
			for _, n := range c.nodes[:2] {
				n.SnapshotStarted(3)
				n.SnapshotSent(3, tc.err)
				_, err := n.Process()
				if tc.stop {
					if !errors.Is(err, tc.err) {
						t.Fatalf("node %d: %v, want the serve error %v", n.cfg.ID, err, tc.err)
					}
				} else if err != nil {
					t.Fatalf("node %d stopped on a retryable serve result: %v", n.cfg.ID, err)
				}
			}
		})
	}
}

// TestDroppedSnapshotSentAgain checks the leader's rule for a snapshot a
// follower dropped: a heartbeat answer an election timeout after the
// snapshot went out, with no fetch started, gives up on it at once; one
// earlier, or after the fetch started, does not.
func TestDroppedSnapshotSentAgain(t *testing.T) {
	c := newMemCluster(t, func(*Config) {})
	n := c.nodes[0]
	resp := &raftpb.Message{Type: new(raftpb.MessageType_MsgHeartbeatResp), From: new(uint64(3)), To: new(n.cfg.ID), Term: new(n.term)}
	for _, tc := range []struct {
		name    string
		since   int64
		started bool
		dropped bool
	}{
		{"just sent", 1, false, false},
		{"an election timeout ago", int64(n.cfg.ElectionTicks), false, true},
		{"fetch under way", int64(n.cfg.ElectionTicks), true, false},
	} {
		n.snapWait[3] = n.now - tc.since
		if tc.started {
			n.SnapshotStarted(3)
		}
		if err := n.Step(resp); err != nil {
			t.Fatal(err)
		}
		if _, waiting := n.snapWait[3]; waiting == tc.dropped {
			t.Errorf("%s: still waiting %v, want %v", tc.name, waiting, !tc.dropped)
		}
		delete(n.snapWait, 3)
	}
}
