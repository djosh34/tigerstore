package core

import (
	"errors"
	"fmt"
	"io"
	"testing"

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
