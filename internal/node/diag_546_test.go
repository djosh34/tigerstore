package node

// Scratch diagnosis for #546. This branch is not for merging.
import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

type diag546Disk struct {
	FS
	checkpointHold  chan struct{}
	packHold        chan struct{}
	packDelay       time.Duration
	checkpointDelay time.Duration
	logDelay        time.Duration
	logSyncs        atomic.Int64
	armed           atomic.Bool
}

func (d *diag546Disk) Create(name string) (wal.File, error) {
	f, err := d.FS.Create(name)
	if err != nil {
		return f, err
	}
	return &diag546File{File: f, d: d, name: name}, nil
}
func (d *diag546Disk) Open(name string) (wal.File, error) {
	f, err := d.FS.Open(name)
	if err != nil {
		return f, err
	}
	return &diag546File{File: f, d: d, name: name}, nil
}

type diag546File struct {
	wal.File
	d    *diag546Disk
	name string
}

func (f *diag546File) Sync() error {
	if f.d.armed.Load() && strings.HasPrefix(f.name, "pack-") {
		if f.d.packHold != nil {
			<-f.d.packHold
		}
		time.Sleep(f.d.packDelay)
	}
	if f.d.armed.Load() && strings.HasPrefix(f.name, "ckpt-") {
		if f.d.checkpointHold != nil {
			<-f.d.checkpointHold
		}
		time.Sleep(f.d.checkpointDelay)
	}
	if f.d.armed.Load() && strings.HasSuffix(f.name, ".wal") && f.d.logDelay > 0 {
		n := f.d.logSyncs.Add(1)
		if n == 1 {
			time.Sleep(368 * time.Millisecond)
		} else if n <= 6 {
			time.Sleep(f.d.logDelay)
		}
	}
	return f.File.Sync()
}

func diag546Snapshot(t *testing.T, n *Node, dir string, round int, err error) {
	t.Helper()
	var sizes [5]int64
	var counts [5]int
	ents, e := os.ReadDir(dir)
	if e != nil {
		t.Fatal(e)
	}
	for _, ent := range ents {
		info, e := ent.Info()
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		k := 4
		switch {
		case strings.HasSuffix(ent.Name(), ".wal"):
			k = 0
		case strings.HasPrefix(ent.Name(), "ckpt-"):
			k = 1
		case strings.HasPrefix(ent.Name(), "pack-"):
			k = 2
		case strings.HasPrefix(ent.Name(), "spare-"):
			k = 3
		}
		counts[k]++
		sizes[k] += info.Size()
	}
	n.log.mu.Lock()
	roots := append([]wal.Root(nil), n.log.ckpts...)
	var eligible []wal.CleanFile
	if len(roots) > 0 {
		eligible = n.log.w.Cleanable(roots[0].Index)
	}
	n.log.mu.Unlock()
	var rootIDs []uint64
	for _, r := range roots {
		rootIDs = append(rootIDs, r.Index)
	}
	t.Logf("round=%d err=%v files(seg,ckpt,pack,spare,other)=%v bytes=%v measured=%d cleaned=%d roots=%v eligible=%v", round, err, counts, sizes, n.log.used.Load(), n.log.cleaned.Load(), rootIDs, eligible)
}

// A focused alternative to racing fresh keys against the disk budget.
func TestDiag546FocusedSharedValues(t *testing.T) {
	dir := t.TempDir()
	r := runClean(t, dir, 512<<10)
	alarmsAllowed(t, r.n)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	big := []byte(fmt.Sprintf("%032768d", 1))
	if _, err := r.n.Do(ctx, &pb.PutRequest{Key: []byte("big"), Value: big}); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if _, err := r.n.Do(ctx, &pb.PutRequest{Key: []byte("big"), IgnoreValue: true}); err != nil {
			t.Fatal(err)
		}
	}
	// Revision 1 predates all puts. This drops none of the 101 versions,
	// but forces a durable checkpoint of them before Defragment runs.
	if _, err := r.n.Do(ctx, &pb.CompactionRequest{Revision: 1, Physical: true}); err != nil {
		t.Fatal(err)
	}
	r.n.log.mu.Lock()
	roots := r.n.log.ckpts
	index := roots[len(roots)-1].Index
	f, err := r.n.log.w.Checkpoint(index)
	r.n.log.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	refs := 0
	unique := map[mvcc.Ref]bool{}
	err = core.WalkCheckpoint(f, index, func(ref mvcc.Ref) error { refs++; unique[ref] = true; return nil })
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if refs != 101 || len(unique) != 1 {
		t.Fatalf("checkpoint has %d refs and %d unique refs", refs, len(unique))
	}
	// WAL.keep protects the newest two segments. Seal the second one
	// so the first (holding big) is eligible, without approaching quota.
	if _, err := r.n.Do(ctx, &pb.PutRequest{Key: []byte("padding"), Value: make([]byte, 40<<10)}); err != nil {
		t.Fatal(err)
	}
	if err := r.n.Defragment(ctx); err != nil {
		t.Fatal(err)
	}
	m, err := r.n.Metrics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("checkpoint=%d refs=%d unique=%d disk=%d cleaned=%d", index, refs, len(unique), m.DiskBytes, m.CleanedBytes)
	if m.CleanedBytes < int64(len(big)) || m.CleanedBytes > int64(len(big)+1024) {
		t.Fatalf("pack copied %d bytes, want one 32 KiB value", m.CleanedBytes)
	}
	resp, err := r.n.Do(ctx, &pb.RangeRequest{Key: []byte("big")})
	if err != nil {
		t.Fatal(err)
	}
	kv := resp.(*pb.RangeResponse).Kvs[0]
	if string(kv.Value) != string(big) || kv.Version != 101 {
		t.Fatal("shared value changed")
	}
}

func TestDiag546SharedValues(t *testing.T) {
	for _, mode := range []string{"normal", "original-startup", "original-slow-log", "paced-writes", "slow-log", "slow-pack", "held-pack", "slow-checkpoint", "held-checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "wal")
			fs, err := osfs.New(dir)
			if err != nil {
				t.Fatal(err)
			}
			d := &diag546Disk{FS: fs}
			var release func()
			switch mode {
			case "slow-pack":
				d.packDelay = 368 * time.Millisecond
			case "held-pack":
				d.packHold = make(chan struct{})
				release = sync.OnceFunc(func() { close(d.packHold) })
				defer release()
			case "slow-log", "original-slow-log":
				d.logDelay = 75 * time.Millisecond
			case "slow-checkpoint":
				d.checkpointDelay = 368 * time.Millisecond
			case "held-checkpoint":
				d.checkpointHold = make(chan struct{})
				release = sync.OnceFunc(func() { close(d.checkpointHold) })
				defer release()
			}
			cfg := Config{Core: core.DefaultConfig(1, []uint64{1}), Tick: 50 * time.Millisecond, SegmentBytes: 32 << 10, DiskBudget: 512 << 10, Logf: t.Logf}
			cfg.Core.WindowBytes, cfg.Core.WindowMaxBytes = 4<<10, 8<<10
			cfg.Core.UnappliedBytes = 1 << 20
			cfg.Core.CheckpointBytes, cfg.Core.CheckpointRatio = 16<<10, 2
			cfg.Turn = func(_ *core.Node, eff core.Effects, _ error) {
				for _, c := range eff.Checkpoint {
					if c.Done {
						t.Logf("checkpoint copied index=%d", c.Index)
					}
				}
			}
			n, err := NewOnFS(cfg, d)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			ran := make(chan error, 1)
			go func() { ran <- n.Run(ctx, noPeers{}) }()
			defer func() {
				if release != nil {
					release()
				}
				cancel()
				if err := <-ran; err != nil {
					t.Error(err)
				}
			}()
			if mode != "original-startup" && mode != "original-slow-log" {
				alarmsAllowed(t, n)
			}
			d.armed.Store(true)
			big := []byte(fmt.Sprintf("%032768d", 1))
			if _, err := n.Do(ctx, &pb.PutRequest{Key: []byte("big"), Value: big}); err != nil {
				t.Fatal(err)
			}
			for range 100 {
				if _, err := n.Do(ctx, &pb.PutRequest{Key: []byte("big"), IgnoreValue: true}); err != nil {
					t.Fatal(err)
				}
			}
			diag546Snapshot(t, n, dir, -1, nil)
			if m, err := n.Metrics(ctx); err == nil {
				t.Logf("before rounds: cluster version=%d", m.ClusterVersion)
			}
			refused, lastRound := -1, -1
			roundsStarted := time.Now()
			for round := 0; round < 100; round++ {
				lastRound = round
				_, err := putAll(n, round, keys(fmt.Sprintf("r%03d-", round), 10)...)
				diag546Snapshot(t, n, dir, round, err)
				if errors.Is(err, rpctypes.ErrGRPCNoSpace) {
					if os.Getenv("DIAG546_STRICT") == "1" {
						t.Fatalf("original no-retry assertion: round %d: %v", round, err)
					}
					refused = round
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				m, err := n.Metrics(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if m.CleanedBytes > 0 {
					t.Logf("first cleaned at round %d; cluster version=%d", round, m.ClusterVersion)
					break
				}
				if mode == "paced-writes" {
					time.Sleep(2 * time.Millisecond)
				}
			}
			t.Logf("round loop took %v; refused=%d", time.Since(roundsStarted), refused)
			if refused >= 0 {
				if release != nil {
					release()
				}
				for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
					m, err := n.Metrics(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if m.CleanedBytes > 0 {
						diag546Snapshot(t, n, dir, 1000, nil)
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("no cleaning after release with no more writes")
					}
				}
				time.Sleep(time.Second)
				diag546Snapshot(t, n, dir, 1001, nil)
				_, err = n.Do(ctx, &pb.PutRequest{Key: []byte("probe"), Value: []byte("v")})
				t.Logf("put after cleaning: %v", err)
				if !strings.Contains(mode, "checkpoint") && err != nil {
					t.Fatalf("temporary refusal did not recover: %v", err)
				}
			}
			if cleaned := n.log.cleaned.Load(); cleaned == 0 || cleaned > int64(len(big)+(lastRound+1)*12<<10) {
				t.Fatalf("cleaned=%d, outside the original shared-value bound after round %d", cleaned, lastRound)
			}
			resp, err := n.Do(ctx, &pb.RangeRequest{Key: []byte("big")})
			if err != nil {
				t.Fatal(err)
			}
			kv := resp.(*pb.RangeResponse).Kvs[0]
			if string(kv.Value) != string(big) || kv.Version != 101 {
				t.Fatalf("shared value changed: version=%d", kv.Version)
			}
		})
	}
}
