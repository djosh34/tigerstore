package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
)

// TestDiag297CanceledReport forces the first report to meet a full
// persisted channel after cancellation. A second group is already in
// flight, as it can be when the TLS watch test stops its cluster.
func TestDiag297CanceledReport(t *testing.T) {
	n, err := newOnDisk(Config{Core: core.DefaultConfig(1, []uint64{1, 2, 3}), Tick: time.Hour}, "", &concurrentDisk{d: simfs.New(), cuts: map[string]*simfs.Disk{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := n.log.close(); err != nil {
			t.Error(err)
		}
	})
	for attempt := range 64 {
		ctx, cancel := context.WithCancel(context.Background())
		// Occupy the sole report slot. The first report must take ctx.Done.
		n.persisted <- persistDone{}
		cancel()
		groups := make(chan written)
		synced := make(chan syncedGroup, 1)
		stopped := make(chan struct{})
		go func() {
			n.syncGroups(groups, synced, func(g written, err error) error {
				return n.reportPersist(ctx, g, err)
			})
			close(stopped)
		}()
		groups <- written{}
		first := <-synced
		if !errors.Is(first.err, context.Canceled) {
			t.Fatalf("first canceled report returned %v", first.err)
		}
		<-n.persisted
		// The saved cancellation is now passed to the next group's report.
		groups <- written{}
		<-synced
		close(groups)
		<-stopped
		select {
		case d := <-n.persisted:
			if d.err == nil {
				t.Fatal("second report lost the pipeline's saved cancellation")
			}
			t.Logf("attempt %d: successful groups produced persistDone.err=%v", attempt, d.err)
			n.persisted <- d
			// Both select cases are ready. Retry the same stopped committer
			// until it chooses the queued report rather than ctx.Done.
			for range 64 {
				if err := n.commit(ctx, noPeers{}); err != nil {
					t.Fatalf("commit returned the report's cancellation at stop: %v", err)
				}
			}
			// A successful report must not remain queued between attempts.
			select {
			case <-n.persisted:
			default:
			}
		default:
		}
	}
}

// A stop fix must not hide a real disk error on a running node.
func TestDiag297DiskFailureStillReturned(t *testing.T) {
	n := &Node{cfg: Config{Tick: time.Hour}, persisted: make(chan persistDone, 1)}
	n.persisted <- persistDone{err: errSync}
	if err := n.commit(context.Background(), noPeers{}); !errors.Is(err, errSync) {
		t.Fatalf("commit returned %v, want %v", err, errSync)
	}
}
