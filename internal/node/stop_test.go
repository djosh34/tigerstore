package node

import (
	"context"
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
