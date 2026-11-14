package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestNoTurnAfterStop stops the committer from inside a core turn while
// a message waits in its inbox, as the simulator does when it holds a
// failed core (#296). The core must take no turn after the stop.
func TestNoTurnAfterStop(t *testing.T) {
	for i := range 32 {
		ctx, cancel := context.WithCancel(context.Background())
		turns := 0
		var n *Node
		cfg := Config{Core: core.DefaultConfig(1, []uint64{1, 2, 3}), Tick: time.Hour}
		cfg.Turn = func(*core.Node, core.Effects, error) {
			turns++
			if turns == 1 {
				cancel()
				n.Unreachable(2)
			}
		}
		n, err := newOnDisk(cfg, "", &concurrentDisk{d: simfs.New(), cuts: map[string]*simfs.Disk{}})
		if err != nil {
			t.Fatal(err)
		}
		n.Unreachable(2)
		err = n.commit(ctx, noPeers{})
		cancel()
		if err := n.log.close(); err != nil {
			t.Fatal(err)
		}
		if err != nil {
			t.Fatal(err)
		}
		if turns != 1 {
			t.Fatalf("run %d: the core took %d turns after the stop", i, turns-1)
		}
	}
}

// TestNoReportAfterStop reports a group after the stop, with the
// cancellation an earlier report returned as its error, as syncGroups
// passes it on. The committer must not get it: it would stop with it as a
// disk failure (#297).
func TestNoReportAfterStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n := &Node{log: &diskLog{}, persisted: make(chan persistDone, 1)}
	for i := range 32 {
		if err := n.report(ctx, written{}, ctx.Err()); !errors.Is(err, context.Canceled) {
			t.Fatalf("run %d: report returned %v", i, err)
		}
		select {
		case done := <-n.persisted:
			t.Fatalf("run %d: the committer got %v after the stop", i, done.err)
		default:
		}
	}
}
