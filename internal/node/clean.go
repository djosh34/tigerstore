package node

import (
	"cmp"
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
	"github.com/djosh34/tigerstore/internal/planted"
	"github.com/djosh34/tigerstore/internal/wal"
)

// The cleaner does the log's housekeeping, so that the write path only
// appends, syncs and rotates. It removes what a durable root made
// obsolete: the segments below its mark and the older checkpoints. It
// cleans: a checkpoint names every value that recovery and a snapshot
// fetch from it still need, so the segments at or below the oldest
// checkpoint whose file the node keeps hold nothing else that is needed.
// The cleaner copies the values that checkpoint names out of such
// segments, and out of packs it made before, into a new pack, which keeps
// their Raft positions. It syncs the pack, has the disk goroutine write
// and sync a root that names it, and only then removes the files the pack
// replaced. The index never changes. Segments go only from the front, so
// the cleaner takes the oldest ones, and packs once at most half of their
// values' bytes are live. A failed removal is logged, counted and tried again; it
// never fails a write or stops the node.
//
// Out of disk is one rule from the measured total of the log's files: at
// Config.DiskBudget, the node raises etcd's NOSPACE alarm through the log
// (core.Node.SetDisk), which refuses writes on every member, and clears
// it once cleaning brought the files below the budget. Past the budget,
// the cleaner runs without pause, takes the oldest segments however much
// of them is live, and keeps no spare files: they hold nothing, and the
// spares a pass's removals made would hold the files above the budget
// (#181). Below it, it cleans what is mostly dead, at most half the time.
// Without a budget, the cleaner cleans only for Defragment. Every
// compaction cuts a checkpoint, so the space it frees comes back.

const (
	// cleanTick is how often the cleaner measures the disk.
	cleanTick = 20 * time.Millisecond
	// cleanLive bounds the live bytes that one pass copies into a pack.
	cleanLive = 64 << 20
	// whereBatch is how many values the cleaner looks up per lock.
	whereBatch = 256
	// removeRetry is how long the cleaner waits before it tries a failed
	// removal again.
	removeRetry = time.Second
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
	// pending is what the last pass's durable root replaced, until the
	// log drops it, which waits while a snapshot fetch or a value read is
	// under way.
	pending *removal
	// files are the files the log dropped, oldest first, which wait to be
	// removed, and checkpoints go below keep, which kept had last. retryAt
	// is when a failed removal is tried again.
	files      []string
	keep, kept uint64
	retryAt    time.Time
	// past is whether the files were past the budget at the last measure,
	// and sparesGone whether the spare files were removed since.
	past, sparesGone bool
}

// spares is how many removed segments the log keeps as spare files: none
// past the budget, and within one at most an eighth of it, as they count
// against it.
func (c *cleaner) spares() int {
	cfg := c.n.cfg
	switch {
	case c.past:
		return 0
	case cfg.DiskBudget > 0:
		return int(min(wal.MaxSpares, cfg.DiskBudget/8/cmp.Or(cfg.SegmentBytes, int64(wal.SegmentBytes))))
	}
	return wal.MaxSpares
}

// clean removes what durable roots made obsolete, and measures the disk
// every cleanTick and cleans within the budget, as the comment at the top
// says. A failed write, or a value that does not match its checksum,
// stops the node.
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
			c.past = used >= budget
		}
		c.remove()
		if budget > 0 && c.pending == nil {
			start := time.Now()
			did, err := c.pass(ctx, c.past)
			if errors.Is(err, errServing) {
				did, err = false, nil
			}
			if err != nil {
				return err
			}
			if did {
				if !c.past && !sleep(ctx, time.Since(start)) {
					return nil
				}
				continue
			}
		}
		select {
		case <-tick.C:
		case <-c.n.log.retry:
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
// finds nothing more or a removal waits for a snapshot fetch to end. A
// pass cannot run while a snapshot is served, so it waits for that to
// end and tries again, as etcd's defragmentation waits for a snapshot it
// sends.
func (c *cleaner) defrag(ctx context.Context) error {
	for c.pending == nil {
		did, err := c.pass(ctx, true)
		if errors.Is(err, errServing) {
			if !sleep(ctx, 10*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		if err != nil || !did {
			return err
		}
	}
	return nil
}

// errServing is a pass's answer while a snapshot is served.
var errServing = errors.New("node: a snapshot is being served")

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
// however much of them is live. It reports whether it changed anything,
// or errServing while a snapshot is served.
func (c *cleaner) pass(ctx context.Context, short bool) (did bool, err error) {
	l := c.n.log
	l.mu.Lock()
	if l.serving > 0 {
		l.mu.Unlock()
		return false, errServing
	}
	if len(l.ckpts) == 0 {
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
		if budget := c.n.cfg.DiskBudget; budget > 0 && l.used.Load() >= budget/2 {
			var total, values int64
			for _, f := range files {
				total += f.Size
				values += live[f.FileID]
			}
			c.n.logf("diag385 cleaner idle checkpoint %d: used %d budget %d, %d eligible files, size %d live %d, first file %+v live %d", ckpt, l.used.Load(), budget, len(files), total, values, files[0], live[files[0].FileID])
		}
		return false, nil
	}
	pack, err := c.copyLive(f[1], ckpt, victims)
	if err != nil {
		return false, err
	}
	c.n.cleaningStage("pack-synced")
	if planted.On("B29") {
		// Remove the old segments now, before a root names the pack, and
		// wait for readers rather than skip; sleeping between tries lets
		// a simulated clock move. The packs it replaces are still named,
		// so they stay until the normal removal.
		l := c.n.log
		for {
			l.mu.Lock()
			if l.reading.TryLock() {
				break
			}
			l.mu.Unlock()
			time.Sleep(time.Millisecond)
		}
		err := l.w.RemoveCleaned(rm.upto, nil)
		l.reading.Unlock()
		l.mu.Unlock()
		if err != nil {
			return false, err
		}
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
	c.remove()
	return true, nil
}

// choose picks the files of one pass: the longest run of the oldest
// segments that fits one pack and is at most half live in all, so that a
// live segment in front does not hold back the dead ones behind it, or
// every segment that fits while space is short; then the packs that are
// at most half live, while their live bytes fit one pack. The first file
// always fits, however large, and so does a file with nothing live.
func choose(files []wal.CleanFile, live map[wal.FileID]int64, short bool) (removal, map[wal.FileID]bool) {
	var rm removal
	victims := map[wal.FileID]bool{}
	var copied, size, segLive int64
	segs := 0
	for i, f := range files {
		v := live[f.FileID]
		if f.Pack || i > 0 && v > 0 && segLive+v > cleanLive {
			break
		}
		size, segLive = size+f.Size, segLive+v
		if short || 2*segLive <= size {
			segs, copied = i+1, segLive
		}
	}
	for _, f := range files[:segs] {
		victims[f.FileID] = true
		rm.upto = f.N
	}
	for _, f := range files[segs:] {
		v := live[f.FileID]
		// A pack's half is of its values' bytes: small values take less
		// than half of their records, and a pack of nothing but live
		// ones would be copied again on every pass (#362).
		if !f.Pack || 2*v > f.Values || len(victims) > 0 && v > 0 && copied+v > cleanLive {
			continue
		}
		rm.drop = append(rm.drop, f.N)
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
	skipped := false
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
		if planted.On("B30") && !skipped {
			skipped = true
			return nil
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

// remove drops from the log what durable roots made obsolete and what
// the last pass replaced, unless a snapshot fetch or a value read is under
// way, as either may read it. It then removes their files, outside the
// lock, and past the budget the spare files.
func (c *cleaner) remove() {
	l := c.n.log
	l.mu.Lock()
	if (l.removal != nil || c.pending != nil) && l.serving == 0 && l.reading.TryLock() {
		var errs []error
		if r := l.removal; r != nil {
			names, err := l.w.DropBelow(r.below)
			c.files, errs = append(c.files, names...), append(errs, err)
			c.keep = max(c.keep, r.root.Index)
			l.ckpts = slices.DeleteFunc(l.ckpts, func(ck wal.Root) bool { return ck.Index < r.root.Index })
			l.removal = nil
			l.held.Store(false)
		}
		if rm := c.pending; rm != nil {
			names, err := l.w.DropCleaned(rm.upto, rm.drop)
			c.files, errs = append(c.files, names...), append(errs, err)
			c.pending = nil
		}
		errs = append(errs, l.unmap(l.w.Oldest()-1))
		l.reading.Unlock()
		// A failed close or unmap leaves nothing to try again.
		if err := errors.Join(errs...); err != nil {
			c.n.logf("drop old files of the log: %v", err)
		}
	}
	l.mu.Unlock()
	if time.Now().Before(c.retryAt) {
		return
	}
	k, err := wal.RemoveFiles(l.fs, c.files, c.spares())
	if k > 0 {
		c.n.cleaningStage("removed")
	}
	c.files = c.files[k:]
	if err == nil && c.keep > c.kept {
		if err = l.w.RemoveCheckpoints(c.keep); err == nil {
			c.kept = c.keep
		}
	}
	if err == nil && c.past && !c.sparesGone {
		err = wal.RemoveSpares(l.fs, 0)
		c.sparesGone = err == nil
	}
	c.sparesGone = c.sparesGone && c.past
	if err != nil {
		c.failed(err)
	}
}

// failed logs and counts a failed removal, which the cleaner tries again
// after removeRetry.
func (c *cleaner) failed(err error) {
	c.n.log.removeFailures.Add(1)
	c.retryAt = time.Now().Add(removeRetry)
	c.n.logf("remove old files of the log: %v; trying again in %v", err, removeRetry)
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

func (n *Node) cleaningStage(stage string) {
	if n.cfg.cleanStage != nil {
		n.cfg.cleanStage(stage)
	}
}
