package sim

import (
	"errors"
	"runtime"
	"strconv"
	"strings"

	"github.com/djosh34/tigerstore/internal/simfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// disk is a process's handle on its node's simulated disk. Every call
// first yields a seeded number of times, so that the process's goroutines
// interleave at file calls, then takes the run's lock for the call. A call
// never waits otherwise: the node's goroutines may hold its locks. Once
// the process died, every call fails.
type disk struct {
	s *Sim
	p *proc
}

// errDead is what the disk calls of a process that crashed get.
var errDead = errors.New("sim: the process crashed")

func (d *disk) yield() {
	d.s.mu.Lock()
	k := 1 + d.s.rng.IntN(3)
	d.s.mu.Unlock()
	for range k {
		runtime.Gosched()
	}
}

// call runs f on the disk with the lock held, then what the run does
// right after the call: a due fault, a crash at a step, or a copy of
// every disk.
func (d *disk) call(op simfs.Op, name string, f func() error) error {
	d.yield()
	s := d.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if d.p.dead() {
		return errDead
	}
	n := d.p.n
	if s.diskFaultDue && op == simfs.OpWrite && isSegment(name) {
		// The guaranteed fault fires in this write of the log.
		s.diskFaultDue = false
		s.armDiskFault(n, 1+s.rng.IntN(2))
	}
	if err := f(); err != nil {
		return err
	}
	return s.afterCall(d.p, op, name)
}

func isSegment(name string) bool    { return strings.HasSuffix(name, ".wal") }
func isCheckpoint(name string) bool { return strings.HasPrefix(name, "ckpt-") }

// afterCall notes what a disk call of p made durable, and crashes p there
// once a crash is due. The lock is held.
func (s *Sim) afterCall(p *proc, op simfs.Op, name string) error {
	n := p.n
	if n.proc != p {
		// Recovery of a process that is starting.
		return nil
	}
	switch {
	case op == simfs.OpSync && isSegment(name) && n.ckptSynced:
		// The root of the checkpoint is in the log now, or soon.
		n.ckptSynced = false
		if s.cfg.Faults.Checkpoint && s.stats.ErasedAtRoot == 0 {
			s.stats.ErasedAtRoot++
			s.takeCheckpoint()
		}
	case op == simfs.OpRemove && isSegment(name):
		n.removedSegment = true
	case op == simfs.OpSyncDir:
		for _, f := range n.ckptDone {
			s.ckptWritten(n, f)
			n.ckptSynced = true
		}
		n.ckptDone = nil
		if n.removedSegment {
			n.removedSegment = false
			if s.cfg.Faults.Checkpoint && s.stats.ErasedAtRemoval == 0 {
				s.stats.ErasedAtRemoval++
				s.takeCheckpoint()
			}
		}
	}
	if s.recoveryDue && !s.healed && op == simfs.OpWrite && isSegment(name) {
		// Crash between a write of the log and its sync, so that the
		// restart finds a torn tail to repair, and its recovery crashes
		// too.
		s.recoveryDue = false
		s.tracef("node %d: crash before a sync of the log", n.id)
		s.stopNode(n)
		return errDead
	}
	step := op == simfs.OpSyncDir || op == simfs.OpRemove || (op == simfs.OpWrite || op == simfs.OpSync) && isCheckpoint(name)
	if step && s.stepCrashDue && !s.healed && s.rng.IntN(4) == 0 {
		s.stepCrashDue = false
		s.stats.StepCrashes++
		s.tracef("node %d: crash at a step, after %v of %s", n.id, op, name)
		s.stopNode(n)
		return errDead
	}
	return nil
}

func (d *disk) List() (names []string, err error) {
	err = d.call(simfs.OpRead, "", func() (err error) {
		names, err = d.p.n.disk.List()
		return err
	})
	return names, err
}

func (d *disk) Usage() (int64, error) {
	d.s.mu.Lock()
	defer d.s.mu.Unlock()
	return int64(d.p.n.disk.Bytes()), nil
}

func (d *disk) open(op simfs.Op, name string, open func(string) (wal.File, error)) (wal.File, error) {
	var f wal.File
	err := d.call(op, name, func() (err error) {
		f, err = open(name)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &file{d: d, f: f, name: name}, nil
}

func (d *disk) Create(name string) (wal.File, error) {
	return d.open(simfs.OpCreate, name, d.p.n.disk.Create)
}

func (d *disk) Open(name string) (wal.File, error) {
	return d.open(simfs.OpOpen, name, d.p.n.disk.Open)
}

func (d *disk) Remove(name string) error {
	return d.call(simfs.OpRemove, name, func() error { return d.p.n.disk.Remove(name) })
}

func (d *disk) Rename(a, b string) error {
	return d.call(simfs.OpRename, a, func() error { return d.p.n.disk.Rename(a, b) })
}

func (d *disk) SyncDir() error {
	return d.call(simfs.OpSyncDir, "", d.p.n.disk.SyncDir)
}

// file is an open file of a process. It knows whether everything written
// to it is synced, so that a checkpoint closed right after its last sync
// counts as complete.
type file struct {
	d             *disk
	f             wal.File
	name          string
	dirty, synced bool
}

func (f *file) ReadAll() (b []byte, err error) {
	err = f.d.call(simfs.OpRead, f.name, func() (err error) {
		b, err = f.f.ReadAll()
		return err
	})
	return b, err
}

func (f *file) ReadAt(p []byte, off int64) (n int, err error) {
	err = f.d.call(simfs.OpRead, f.name, func() (err error) {
		n, err = f.f.ReadAt(p, off)
		return err
	})
	return n, err
}

func (f *file) Write(p []byte) (n int, err error) {
	err = f.d.call(simfs.OpWrite, f.name, func() (err error) {
		f.dirty = true
		n, err = f.f.Write(p)
		return err
	})
	return n, err
}

func (f *file) WriteAt(p []byte, off int64) (n int, err error) {
	err = f.d.call(simfs.OpWrite, f.name, func() (err error) {
		f.dirty = true
		n, err = f.f.WriteAt(p, off)
		return err
	})
	return n, err
}

func (f *file) Truncate(size int64) error {
	return f.d.call(simfs.OpTruncate, f.name, func() error {
		f.dirty = true
		return f.f.Truncate(size)
	})
}

func (f *file) Sync() error {
	return f.d.call(simfs.OpSync, f.name, func() error {
		if err := f.f.Sync(); err != nil {
			return err
		}
		f.dirty, f.synced = false, true
		return nil
	})
}

func (f *file) Close() error {
	f.d.s.mu.Lock()
	defer f.d.s.mu.Unlock()
	if isCheckpoint(f.name) && f.synced && !f.dirty && !f.d.p.dead() {
		f.d.p.n.ckptDone = append(f.d.p.n.ckptDone, f.name)
	}
	return f.f.Close()
}

// ckptWritten checks that every node writes the same bytes for the
// checkpoint in file name, once it is durable. The lock is held.
func (s *Sim) ckptWritten(n *node, name string) {
	index, err := strconv.ParseUint(strings.TrimPrefix(name, "ckpt-"), 16, 64)
	if err != nil {
		s.fail("node %d wrote checkpoint file %q: %w", n.id, name, err)
		return
	}
	b := n.disk.DurableBytes(name)
	if b == nil {
		return
	}
	sum := hashBytes(b)
	if c, ok := s.ckpts[index]; ok && c.sum != sum {
		s.fail("node %d and node %d wrote different checkpoints at index %d", n.id, c.node, index)
		return
	}
	s.ckpts[index] = ckptRec{sum: sum, node: n.id}
	s.stats.Checkpoints++
}
