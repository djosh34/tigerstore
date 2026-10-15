package node

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/djosh34/tigerstore/internal/core"
	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/wal"
)

// Cleaning keeps the log's files within Config.DiskBudget. A checkpoint
// names every value that recovery and a snapshot fetch from it still
// need, so the segments at or below the oldest checkpoint whose file the
// node keeps hold nothing else that is needed. The cleaner copies the
// values that checkpoint names out of such segments, and out of packs it
// made before, into a new pack, which keeps their Raft positions. It syncs
// the pack, has the disk goroutine install and sync a root that names it,
// and only then removes the files the pack replaced. The index never
// changes. Segments go only from the front, so the cleaner takes the
// oldest ones, and packs once at most half of their bytes are live.
//
// From half the budget on, the cleaner runs at most half the time. From
// three quarters on, it runs without pause, and writes wait before the
// node takes them, longer the fuller the disk. At the budget, the node
// answers writes with etcd's NOSPACE error. Without a budget, the cleaner
// cleans only for Defragment.

const (
	// cleanTick is how often the cleaner measures the disk.
	cleanTick = 20 * time.Millisecond
	// cleanLive bounds the live bytes that one pass copies into a pack.
	cleanLive = 64 << 20
	// maxDelay is how long a write waits at most while the disk is nearly
	// full.
	maxDelay = 10 * time.Millisecond
	// whereBatch is how many values the cleaner looks up per lock.
	whereBatch = 256
)

// packRoot asks the disk goroutine to install and sync a root that names
// the pack add, if any, and no longer the packs drop. It closes synced
// once the root is durable.
type packRoot struct {
	add    *wal.Pack
	drop   []uint64
	synced chan struct{}
}

// removal is what a synced root replaced: the oldest segments up to upto
// and the packs drop.
type removal struct {
	upto uint64
	drop []uint64
}

// cleaner is the state of one run's cleaner.
type cleaner struct {
	n *Node
	// idle is the checkpoint, the files and whether space was short in
	// the last pass that found nothing to clean; until they change,
	// neither does the answer.
	idle      uint64
	idleFiles []wal.CleanFile
	idleShort bool
	// pending waits for a snapshot fetch to end before it is removed.
	pending *removal
}

// clean measures the disk every cleanTick and cleans while it is past
// half the budget. A failed disk operation, or a value that does not
// match its checksum, stops the node.
func (n *Node) clean(ctx context.Context) {
	c := &cleaner{n: n}
	if err := c.run(ctx); err != nil && ctx.Err() == nil {
		select {
		case n.persisted <- persistDone{err: fmt.Errorf("clean: %w", err)}:
		case <-ctx.Done():
		}
	}
}

func (c *cleaner) run(ctx context.Context) error {
	budget := c.n.cfg.DiskBudget
	tick := time.NewTicker(cleanTick)
	defer tick.Stop()
	for {
		if budget > 0 {
			used, err := c.n.log.fs.Usage()
			if err != nil {
				return err
			}
			c.n.log.used.Store(used)
		}
		if err := c.remove(); err != nil {
			return err
		}
		if used := c.n.log.used.Load(); budget > 0 && used >= budget/2 && c.pending == nil {
			start := time.Now()
			did, err := c.pass(ctx, used >= budget*3/4)
			if err != nil {
				return err
			}
			if did {
				if used < budget*3/4 && !sleep(ctx, time.Since(start)) {
					return nil
				}
				continue
			}
		}
		select {
		case <-tick.C:
		case done := <-c.n.defragQ:
			err := c.defrag(ctx)
			done <- err
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// defrag is Defragment: passes that take the segments below the oldest
// checkpoint the node keeps, however much of them is live, until one
// finds nothing more or a removal waits for a snapshot fetch to end.
func (c *cleaner) defrag(ctx context.Context) error {
	for c.pending == nil {
		did, err := c.pass(ctx, true)
		if err != nil || !did {
			return err
		}
	}
	return nil
}

// Defragment is etcd's Maintenance.Defragment. etcd rewrites its database
// file without the free pages; tigerstore's log has no free pages, and the
// space it can give back is in the segments below the oldest checkpoint,
// which a cleaning pass copies the live values out of.
func (n *Node) Defragment(ctx context.Context) error {
	m := n.cfg.Metrics
	m.DefragInflight.Set(1)
	defer m.DefragInflight.Set(0)
	start := time.Now()
	err := n.defragment(ctx)
	if err == nil {
		m.Defrag.Observe(time.Since(start).Seconds())
	}
	return err
}

func (n *Node) defragment(ctx context.Context) error {
	done := make(chan error, 1)
	select {
	case n.defragQ <- done:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return rpctypes.ErrGRPCStopped
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return rpctypes.ErrGRPCStopped
	}
}

// pass cleans once: it picks the files to replace, copies the values they
// hold that the checkpoint names into a pack, has its root installed, and
// removes them. short means that space is short: the oldest segments go
// however much of them is live. It reports whether it changed anything.
func (c *cleaner) pass(ctx context.Context, short bool) (did bool, err error) {
	l := c.n.log
	l.mu.Lock()
	if l.serving > 0 || len(l.ckpts) == 0 {
		l.mu.Unlock()
		return false, nil
	}
	ckpt := l.ckpts[0].Index
	files := l.w.Cleanable(ckpt)
	if len(files) == 0 || (ckpt == c.idle && short == c.idleShort && slices.Equal(files, c.idleFiles)) {
		l.mu.Unlock()
		return false, nil
	}
	// A newer root may remove the checkpoint's file during the pass; the
	// files opened now stay readable.
	var f [2]io.ReadCloser
	for i := range f {
		if f[i], err = l.w.Checkpoint(ckpt); err != nil {
			break
		}
	}
	l.mu.Unlock()
	defer func() {
		for _, r := range f {
			if r != nil {
				err = errors.Join(err, r.Close())
			}
		}
	}()
	if err != nil {
		return false, err
	}
	live := map[wal.FileID]int64{}
	for _, f := range files {
		live[f.FileID] = 0
	}
	err = c.walk(f[0], ckpt, func(id wal.FileID, r mvcc.Ref) error {
		if v, ok := live[id]; ok {
			live[id] = v + int64(r.Len)
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	rm, victims := choose(files, live, short)
	if len(victims) == 0 {
		c.idle, c.idleFiles, c.idleShort = ckpt, files, short
		return false, nil
	}
	pack, err := c.copyLive(f[1], ckpt, victims)
	if err != nil {
		return false, err
	}
	pr := packRoot{add: pack, drop: rm.drop, synced: make(chan struct{})}
	select {
	case c.n.cleanQ <- pr:
	case <-ctx.Done():
		if pack != nil {
			return false, pack.Close()
		}
		return false, nil
	}
	select {
	case <-pr.synced:
	case <-ctx.Done():
		return false, nil
	}
	var size int64
	if pack != nil {
		size = pack.Size()
		c.n.log.cleaned.Add(size)
	}
	c.n.logf("cleaning below checkpoint %d: %d files into a pack of %d bytes", ckpt, len(victims), size)
	c.pending = &rm
	return true, c.remove()
}

// choose picks the files of one pass: the oldest segments, in order,
// while each is at most half live or space is short, then the packs that
// are at most half live, while their live bytes fit one pack. The first
// file always fits, however large, and so does a file with nothing live.
func choose(files []wal.CleanFile, live map[wal.FileID]int64, short bool) (removal, map[wal.FileID]bool) {
	var rm removal
	victims := map[wal.FileID]bool{}
	var copied int64
	segs := true
	for _, f := range files {
		v := live[f.FileID]
		mostlyDead := 2*v <= f.Size
		fits := len(victims) == 0 || v == 0 || copied+v <= cleanLive
		if !f.Pack {
			if !segs || (!mostlyDead && !short) || !fits {
				segs = false
				continue
			}
			rm.upto = f.N
		} else {
			if !mostlyDead || !fits {
				continue
			}
			rm.drop = append(rm.drop, f.N)
		}
		victims[f.FileID] = true
		copied += v
	}
	return rm, victims
}

// copyLive copies the values the checkpoint in f names from the files
// victims into a new pack and returns it, or nil if there are none.
func (c *cleaner) copyLive(f io.Reader, ckpt uint64, victims map[wal.FileID]bool) (*wal.Pack, error) {
	l := c.n.log
	var pw *wal.PackWriter
	err := c.walk(f, ckpt, func(id wal.FileID, r mvcc.Ref) error {
		if !victims[id] {
			return nil
		}
		b, err := l.Value(r.Index, r.Off, r.Len)
		if errors.Is(err, wal.ErrNotOnDisk) {
			// A newer root removed it since walk found it; that
			// checkpoint no longer names it.
			return nil
		}
		if err != nil {
			return err
		}
		if crc32.Checksum(b, castagnoli) != r.Sum {
			return fmt.Errorf("%w: entry %d, offset %d", mvcc.ErrBadValue, r.Index, r.Off)
		}
		if pw == nil {
			l.mu.Lock()
			pw, err = l.w.NewPack()
			l.mu.Unlock()
			if err != nil {
				return err
			}
		}
		return pw.Add(r.Index, r.Off, b)
	})
	if pw == nil {
		return nil, err
	}
	var p *wal.Pack
	if err == nil {
		p, err = pw.Finish()
	}
	if err != nil {
		return nil, errors.Join(err, pw.Abort())
	}
	return p, nil
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// walk calls fn once for every value the checkpoint in f names, with the
// file that holds it. It looks the values up in batches, one lock each.
func (c *cleaner) walk(f io.Reader, ckpt uint64, fn func(wal.FileID, mvcc.Ref) error) error {
	l := c.n.log
	var last mvcc.Ref
	batch := make([]mvcc.Ref, 0, whereBatch)
	ids := make([]wal.FileID, whereBatch)
	found := make([]bool, whereBatch)
	flush := func() error {
		l.mu.Lock()
		for i, r := range batch {
			ids[i], found[i] = l.w.Where(r.Index, r.Off, r.Len)
		}
		l.mu.Unlock()
		for i, r := range batch {
			// A value no file holds is in a segment that a newer root
			// removed; that checkpoint no longer names it.
			if found[i] {
				if err := fn(ids[i], r); err != nil {
					return err
				}
			}
		}
		batch = batch[:0]
		return nil
	}
	err := core.WalkCheckpoint(f, ckpt, func(r mvcc.Ref) error {
		// A version put with ignore_value names the value of the version
		// before it, so versions that share a value come one after another.
		if r == last {
			return nil
		}
		last = r
		if batch = append(batch, r); len(batch) < whereBatch {
			return nil
		}
		return flush()
	})
	if err != nil {
		return err
	}
	return flush()
}

// remove removes what the last synced root replaced, unless a snapshot
// fetch is under way, which may read it.
func (c *cleaner) remove() error {
	rm := c.pending
	if rm == nil {
		return nil
	}
	l := c.n.log
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.serving > 0 {
		return nil
	}
	c.pending = nil
	if err := l.w.RemoveCleaned(rm.upto, rm.drop); err != nil {
		return err
	}
	return l.unmap(rm.upto)
}

// sleep waits for d and reports whether ctx is still live.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// installPacks installs and syncs the root of a cleaning pass, on the
// disk goroutine.
func (n *Node) installPacks(pr packRoot) error {
	w := n.log.w
	n.log.mu.Lock()
	err := w.InstallPacks(pr.add, pr.drop)
	n.log.mu.Unlock()
	err = errors.Join(err, w.Sync())
	n.log.mu.Lock()
	err = errors.Join(err, n.log.synced())
	n.log.mu.Unlock()
	if err == nil {
		close(pr.synced)
	}
	return err
}

// delay is how long a write waits before the node takes it: nothing while
// the disk is within three quarters of its budget, then up to maxDelay as
// it fills up to the budget.
func (n *Node) delay() time.Duration {
	b := n.cfg.DiskBudget
	over := n.log.used.Load() - b*3/4
	if b <= 0 || over <= 0 {
		return 0
	}
	return time.Duration(float64(maxDelay) * min(1, float64(over)/float64(b/4)))
}

// throttle waits as long as delay says before a write.
func (n *Node) throttle(ctx context.Context) error {
	d := n.delay()
	if d == 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return rpctypes.ErrGRPCStopped
	}
}
