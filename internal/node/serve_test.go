package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/wal"
)

// gate is a writer that blocks its first write until released.
type gate struct {
	once    sync.Once
	wrote   chan struct{}
	release chan struct{}
	buf     bytes.Buffer
}

func (g *gate) Write(p []byte) (int, error) {
	g.once.Do(func() {
		close(g.wrote)
		<-g.release
	})
	return g.buf.Write(p)
}

// TestServeKeepsItsFiles serves a snapshot from a node's checkpoint and,
// while the stream is under way, runs a root job that was queued before
// the fetch began, whose removal would take the checkpoint and the old
// segments the stream reads. The removal must wait, and the stream must
// end well.
func TestServeKeepsItsFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{DataDir: dir, Tick: 10 * time.Millisecond, Logf: t.Logf, SegmentBytes: 64 << 10}
	cfg.Core = core.DefaultConfig(1, []uint64{1})
	cfg.Core.WindowBytes, cfg.Core.WindowMaxBytes = 4<<10, 8<<10
	cfg.Core.CheckpointBytes, cfg.Core.CheckpointRatio = 64<<10, 1
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, noPeers{}) }()
	// Long keys make a checkpoint larger than the stream's write buffer,
	// so the stream writes while it still reads the checkpoint.
	// Rounds of writes, each compacted away, let the oldest segments go.
	key := func(i int) []byte { return fmt.Appendf(nil, "%01000d", i) }
	// do retries op until it succeeds or the node stops.
	do := func(op proto.Message) error {
		for {
			c, done := context.WithTimeout(ctx, 5*time.Second)
			_, err := n.Do(c, op)
			done()
			if err == nil || errors.Is(err, rpctypes.ErrGRPCStopped) {
				return err
			}
		}
	}
	for round := range 4 {
		var wg sync.WaitGroup
		for w := range 8 {
			wg.Go(func() {
				for i := w; i < 800; i += 8 {
					if do(&pb.PutRequest{Key: key(i), Value: fmt.Appendf(nil, "v%d", round)}) != nil {
						return
					}
				}
			})
		}
		wg.Wait()
		if do(&pb.CompactionRequest{Revision: int64(800 * (round + 1))}) != nil {
			t.Fatalf("the node stopped: %v", <-ran)
		}
	}
	// Let the last checkpoint's root become durable.
	time.Sleep(500 * time.Millisecond)
	cancel()
	if err := <-ran; err != nil {
		t.Fatal(err)
	}

	n, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ckpt := n.log.ckpt
	st, err := os.Stat(filepath.Join(dir, "wal", wal.CheckpointName(ckpt.Index)))
	if err != nil {
		t.Fatalf("setup: checkpoint at %d: %v", ckpt.Index, err)
	}
	if st.Size() < 512<<10 {
		t.Fatalf("setup: the checkpoint at %d has only %d bytes", ckpt.Index, st.Size())
	}
	if n.log.w.Has(1) {
		t.Fatal("setup: entry 1 is still on disk")
	}
	go n.persist(t.Context(), noPeers{})
	g := &gate{wrote: make(chan struct{}), release: make(chan struct{})}
	served := make(chan error, 1)
	go func() { served <- n.Serve(2, 1, n.core.Applied(), g) }()
	<-g.wrote
	// A root job the core made before the fetch began: it removes every
	// segment it can and the checkpoint the stream reads.
	last := n.core.Applied()
	n.persistQ <- []core.PersistJob{{Root: &wal.Root{Index: last, Term: ckpt.Term}, RemoveBelow: last + 1}}
	if done := <-n.persisted; done.err != nil {
		t.Fatal(done.err)
	}
	close(g.release)
	if err := <-served; err != nil {
		t.Fatalf("the stream failed after the root job ran: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "wal", wal.CheckpointName(ckpt.Index))); err != nil {
		t.Fatalf("the checkpoint the stream read is gone: %v", err)
	}
}
