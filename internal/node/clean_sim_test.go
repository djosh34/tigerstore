package node

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// concurrentDisk serializes simfs operations, not whole WAL operations. The
// real persister, checkpoint writer, cleaner, readers and snapshot sender
// interleave at file calls. Yielding never sleeps while the caller holds a
// production mutex: synctest cannot advance time through mutex contention.
type concurrentDisk struct {
	mu             sync.Mutex
	d              *simfs.Disk
	seed, calls    uint64
	packs, removes int
	cuts           map[string]*simfs.Disk
}

func (d *concurrentDisk) delay() {
	d.mu.Lock()
	d.calls++
	n := (d.calls*6364136223846793005 + d.seed) >> 32
	d.mu.Unlock()
	for range 1 + n%3 {
		runtime.Gosched()
	}
}

func (d *concurrentDisk) List() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.d.List()
}

func (d *concurrentDisk) Usage() (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return int64(d.d.Bytes()), nil
}

func (d *concurrentDisk) Create(name string) (wal.File, error) {
	d.delay()
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := d.d.Create(name)
	if err != nil {
		return nil, err
	}
	return &concurrentFile{d: d, f: f, name: name}, nil
}

func (d *concurrentDisk) Open(name string) (wal.File, error) {
	d.delay()
	d.mu.Lock()
	defer d.mu.Unlock()
	f, err := d.d.Open(name)
	if err != nil {
		return nil, err
	}
	return &concurrentFile{d: d, f: f, name: name}, nil
}

func (d *concurrentDisk) Remove(name string) error {
	d.delay()
	d.mu.Lock()
	defer d.mu.Unlock()
	err := d.d.Remove(name)
	if err == nil && strings.HasSuffix(name, ".wal") {
		d.removes++
	}
	return err
}

func (d *concurrentDisk) Rename(a, b string) error {
	d.delay()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.d.Rename(a, b)
}

func (d *concurrentDisk) SyncDir() error {
	d.delay()
	d.mu.Lock()
	defer d.mu.Unlock()
	err := d.d.SyncDir()
	if err == nil && d.removes > 0 {
		d.cut("directory-removal")
	}
	return err
}

func (d *concurrentDisk) cut(stage string) {
	if d.cuts[stage] == nil {
		d.cuts[stage] = d.d.Durable()
	}
}

type concurrentFile struct {
	d    *concurrentDisk
	f    wal.File
	name string
}

func (f *concurrentFile) ReadAll() ([]byte, error) {
	f.d.delay()
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.ReadAll()
}

func (f *concurrentFile) ReadAt(p []byte, off int64) (int, error) {
	f.d.delay()
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.ReadAt(p, off)
}

func (f *concurrentFile) Write(p []byte) (int, error) {
	f.d.delay()
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.Write(p)
}

func (f *concurrentFile) WriteAt(p []byte, off int64) (int, error) {
	f.d.delay()
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.WriteAt(p, off)
}

func (f *concurrentFile) Truncate(n int64) error {
	f.d.delay()
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.Truncate(n)
}

func (f *concurrentFile) Sync() error {
	f.d.delay()
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	err := f.f.Sync()
	if err == nil && strings.HasPrefix(f.name, "pack-") {
		f.d.packs++
	}
	return err
}

func (f *concurrentFile) Close() error {
	f.d.mu.Lock()
	defer f.d.mu.Unlock()
	return f.f.Close()
}

type slowSnapshot struct{}

func (slowSnapshot) Write(p []byte) (int, error) {
	time.Sleep(30 * time.Millisecond)
	return io.Discard.Write(p)
}

// errLostValue is how the cleaner scenario fails when a value that the
// durable checkpoint or a client still needs is gone.
var errLostValue = errors.New("lost a value")

// This runs Node.Run, including cleaner.run's pacing and its real queues.
// It does not substitute the test's own packing protocol for the cleaner.
func TestConcurrentCleanerSimulation(t *testing.T) {
	for seed := uint64(1); seed <= 8; seed++ {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				if err := concurrentCleaning(t, seed); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func concurrentCleaning(t *testing.T, seed uint64) (err error) {
	d := &concurrentDisk{d: simfs.New(), seed: seed, cuts: map[string]*simfs.Disk{}}
	cfg := Config{
		Tick: 50 * time.Millisecond, SegmentBytes: 32 << 10, DiskBudget: 2 << 20,
		BeforeRead: func() { time.Sleep(2 * time.Millisecond) },
		cleanStage: func(stage string) {
			d.mu.Lock()
			d.cut(stage)
			d.mu.Unlock()
		},
	}
	cfg.Core = core.DefaultConfig(1, []uint64{1})
	cfg.Core.WindowBytes, cfg.Core.WindowMaxBytes = 4<<10, 8<<10
	cfg.Core.CheckpointBytes, cfg.Core.CheckpointRatio = 32<<10, 2
	cfg.Core.MemoryBytes = 128 << 10
	n, err := newOnDisk(cfg, "", d)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan error, 1)
	go func() { ran <- n.Run(ctx, noPeers{}) }()
	defer func() {
		cancel()
		err = errors.Join(err, lostValue(<-ran))
	}()
	// Cold values stay compressible; hot churn must still fill the disk.
	cold, hot := keys("c", 24), keys("noise-h", 24)
	want := map[string]int{}
	if _, err := putAll(n, 0, cold...); err != nil {
		return err
	}
	for _, k := range cold {
		want[k] = 0
	}
	stop := make(chan struct{})
	readDone, snapDone := make(chan error, 1), make(chan error, 1)
	var reads, snapshots int
	go func() {
		for {
			select {
			case <-stop:
				readDone <- nil
				return
			default:
			}
			r, err := do(n, &pb.RangeRequest{Key: []byte(cold[reads%len(cold)])})
			if err == nil {
				kvs := r.(*pb.RangeResponse).Kvs
				if len(kvs) != 1 || string(kvs[0].Value) != string(bigValue(cold[reads%len(cold)], 0)) {
					err = fmt.Errorf("%w: concurrent reader got a wrong cold value", errLostValue)
				}
			}
			if err != nil {
				readDone <- err
				return
			}
			reads++
			time.Sleep(7 * time.Millisecond)
		}
	}()
	go func() {
		for {
			select {
			case <-stop:
				snapDone <- nil
				return
			default:
			}
			n.log.mu.Lock()
			var root wal.Root
			if len(n.log.ckpts) > 0 {
				root = n.log.ckpts[len(n.log.ckpts)-1]
			}
			n.log.mu.Unlock()
			if root.Index > 0 {
				if err := n.Serve(2, 1, root.Index, slowSnapshot{}); err != nil {
					snapDone <- err
					return
				}
				snapshots++
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	// Join the helpers even on a failed assertion, before stopping Node.Run.
	defer func() {
		close(stop)
		err = errors.Join(err, lostValue(<-readDone), lostValue(<-snapDone))
		if err == nil && (reads == 0 || snapshots == 0) {
			err = fmt.Errorf("missing overlap: reads=%d snapshots=%d", reads, snapshots)
		}
		t.Logf("seed=%d reads=%d snapshots=%d", seed, reads, snapshots)
	}()
	for round := 1; round <= 80; round++ {
		rev, err := putAll(n, round, hot...)
		if err != nil {
			return err
		}
		for _, k := range hot {
			want[k] = round
		}
		if _, err := do(n, &pb.CompactionRequest{Revision: rev}); err != nil {
			return err
		}
		time.Sleep(25 * time.Millisecond)
	}
	// Wait for the durable commit hint and pending removals, without a new KV
	// revision. The crash copy is taken while the production driver is live.
	if _, err := do(n, &pb.TxnRequest{}); err != nil {
		return err
	}
	time.Sleep(time.Second)
	d.mu.Lock()
	crash := d.d.Durable()
	packs, removes := d.packs, d.removes
	cuts := maps.Clone(d.cuts)
	d.mu.Unlock()
	names, err := crash.List()
	if err != nil {
		return err
	}
	if slices.Contains(names, wal.SegmentName(1)) {
		return errors.New("original cold-value segment is still on the durable disk")
	}
	if packs == 0 || removes == 0 {
		return fmt.Errorf("no concurrent cleaning: packs=%d removes=%d", packs, removes)
	}
	t.Logf("seed=%d pack-syncs=%d segment-removes=%d crash-cuts=%d", seed, packs, removes, len(cuts))
	for _, stage := range []string{"pack-synced", "root-buffered", "root-synced", "removed"} {
		if cuts[stage] == nil {
			return fmt.Errorf("missing production cleaner crash boundary %s", stage)
		}
	}
	for stage, disk := range cuts {
		if err := checkCrashReferences(stage, disk); err != nil {
			return err
		}
	}
	if err := checkCrashReferences("final", crash.Durable()); err != nil {
		return err
	}
	// Recovery compares against the client's acknowledged values, never
	// another replica or a hash made by the store.
	rd := &concurrentDisk{d: crash, seed: seed, cuts: map[string]*simfs.Disk{}}
	cfg.cleanStage = nil
	restarted, err := newOnDisk(cfg, "", rd)
	if err != nil {
		return err
	}
	rctx, rcancel := context.WithCancel(context.Background())
	rdone := make(chan error, 1)
	go func() { rdone <- restarted.Run(rctx, noPeers{}) }()
	err = checkValues(restarted, want)
	rcancel()
	return lostValue(errors.Join(err, <-rdone))
}

// lostValue marks a read of a value the log no longer has as a lost value.
func lostValue(err error) error {
	if errors.Is(err, wal.ErrNotOnDisk) {
		return fmt.Errorf("%w: %w", errLostValue, err)
	}
	return err
}

func checkCrashReferences(stage string, disk *simfs.Disk) (err error) {
	w, rec, err := wal.Open(disk)
	if err != nil {
		return fmt.Errorf("%s recovery: %w", stage, err)
	}
	defer func() { err = errors.Join(err, w.Close()) }()
	if rec.Root.Index == 0 {
		return fmt.Errorf("%s has no durable checkpoint", stage)
	}
	f, err := w.Checkpoint(rec.Root.Index)
	if err != nil {
		return err
	}
	err = core.WalkCheckpoint(f, rec.Root.Index, func(r mvcc.Ref) error {
		b, err := w.Value(r.Index, r.Off, r.Len)
		if err == nil && crc32.Checksum(b, castagnoli) != r.Sum {
			err = errors.New("checkpoint value checksum mismatch")
		}
		return err
	})
	if err = errors.Join(err, f.Close()); err != nil {
		return fmt.Errorf("%s: %w of the durable checkpoint: %w", stage, errLostValue, err)
	}
	return nil
}
