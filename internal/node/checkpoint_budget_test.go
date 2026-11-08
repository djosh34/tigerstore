package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/protobuf/proto"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// pausedDisk is a disk whose checkpoint files take no write while held.
type pausedDisk struct {
	*concurrentDisk
	held atomic.Bool
}

func (d *pausedDisk) Create(name string) (wal.File, error) {
	f, err := d.concurrentDisk.Create(name)
	if err != nil || !strings.HasPrefix(name, "ckpt-") {
		return f, err
	}
	return &pausedFile{File: f, d: d}, nil
}

type pausedFile struct {
	wal.File
	d *pausedDisk
}

func (f *pausedFile) Write(p []byte) (int, error) {
	for f.d.held.Load() {
		time.Sleep(time.Millisecond)
	}
	return f.File.Write(p)
}

// TestCheckpointHeldByItsWriter cuts a checkpoint of 10,000 leases, a key
// with 3,000 versions and a key of 4 MiB while the writer of checkpoint
// files is held. The node hands out at most the parts the writer's queue
// and the committer hold, about 65 of CheckpointChunk, however large the
// header or a key: it copies the next only once the writer took the last
// (#434, #435). Once the writer goes on, the checkpoint is written.
func TestCheckpointHeldByItsWriter(t *testing.T) {
	d := &pausedDisk{concurrentDisk: &concurrentDisk{d: simfs.New(), cuts: map[string]*simfs.Disk{}}}
	cfg := Config{Tick: 10 * time.Millisecond, Logf: t.Logf}
	cfg.Core = core.DefaultConfig(1, []uint64{1})
	cfg.Core.MaxRequestBytes = 8 << 20
	var handed, last atomic.Int64
	cfg.Turn = func(_ *core.Node, eff core.Effects, _ error) {
		for _, c := range eff.Checkpoint {
			if d.held.Load() {
				handed.Add(int64(len(c.Data)))
			}
			if c.Done {
				last.Store(int64(c.Index))
			}
		}
	}
	n, err := newOnDisk(cfg, "", d)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, noPeers{}) }()
	defer func() {
		cancel()
		if err := <-ran; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	// 10,000 leases and 3,000 versions of one key, from 32 clients.
	ops := make(chan proto.Message, 64)
	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for op := range ops {
				if _, err := do(n, op); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	for i := range 10_000 {
		ops <- &pb.LeaseGrantRequest{ID: int64(i + 1), TTL: 3600}
		if i < 3000 {
			ops <- &pb.PutRequest{Key: []byte("hot"), Value: fmt.Appendf(nil, "%d", i)}
		}
	}
	close(ops)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if _, err := do(n, &pb.PutRequest{Key: bytes.Repeat([]byte("k"), 4<<20), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	d.held.Store(true)
	r, err := do(n, &pb.RangeRequest{Key: []byte("hot")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := do(n, &pb.CompactionRequest{Revision: r.(*pb.RangeResponse).Header.Revision}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	d.held.Store(false)
	limit := int64(65 * (cfg.Core.CheckpointChunk + 1<<10))
	t.Logf("the node handed out %d bytes of checkpoint while its writer was held, bound %d", handed.Load(), limit)
	if handed.Load() > limit {
		t.Fatalf("the node handed out %d bytes of checkpoint while its writer was held, more than %d", handed.Load(), limit)
	}
	for deadline := time.Now().Add(20 * time.Second); last.Load() == 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the checkpoint was never finished")
		}
	}
}
