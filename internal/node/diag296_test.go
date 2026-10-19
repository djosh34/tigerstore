package node

import (
	"context"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// The simulator's failed Turn waits for cancellation before returning.
// There can still be a message queued when it returns. Cancellation must
// stop the committer before that message starts another core turn.
func TestCommitStopsAfterCancelledTurn(t *testing.T) {
	extra := 0
	for range 32 {
		extra += cancelledTurn(t)
	}
	if extra != 0 {
		t.Fatalf("the committer ran %d extra core turns after Turn cancelled its context", extra)
	}
}

func cancelledTurn(t *testing.T) int {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	var err error
	n, err = newOnDisk(cfg, "", simfs.New())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := n.log.close(); err != nil {
			t.Error(err)
		}
	}()
	n.Unreachable(2)
	if err := n.commit(ctx, noPeers{}); err != nil {
		t.Fatal(err)
	}
	return turns - 1
}
