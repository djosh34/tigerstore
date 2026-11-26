package wl

import (
	"errors"
	"fmt"
	"io"
)

// Only the loop acquires/releases source leases. Worker reads use the
// immutable fd/interval, not rfds or a mutable store's offset lookup.
// At most two physical source extents belong to the one active piece.
type walFetchPin struct {
	segment int64
	fd      int
	epoch   uint64
	refs    uint32
}

type walSourceLease struct {
	wal   *wal
	epoch uint64
	slot  int
	fd    int
	off   int64
	size  int
	done  bool
}

// lendFetch clips at both durability and segment boundaries. A caller
// resolves/copies mutable stage bytes on-loop instead of lending them.
func (w *wal) lendFetch(off int64, size int) (*walSourceLease, error) {
	if w.closing.Load() {
		return nil, errWALClosed
	}
	end := w.durableEnd()
	if off < 0 || size <= 0 || off >= end {
		return nil, errAway
	}
	size = min(size, int(end-off), int(segSize-off%segSize))
	w.mu.Lock()
	defer w.mu.Unlock()
	segment := off / segSize
	i := segment - w.rbase
	if i < 0 || i >= int64(len(w.rfds)) {
		return nil, errLogCut
	}
	fd := w.rfds[i]
	refs := uint32(0)
	for _, p := range w.fetchPins {
		refs += p.refs
	}
	if refs >= 2 {
		return nil, errAway
	}
	slot := -1
	for j := range w.fetchPins {
		p := &w.fetchPins[j]
		if p.refs > 0 && p.segment == segment && p.fd == fd {
			slot = j
			break
		}
		if p.refs == 0 {
			slot = j
		}
	}
	if slot < 0 || w.fetchEpoch == ^uint64(0) {
		return nil, errAway
	}
	p := &w.fetchPins[slot]
	if p.refs == 0 {
		w.fetchEpoch++
		*p = walFetchPin{segment: segment, fd: fd, epoch: w.fetchEpoch}
	}
	p.refs++
	return &walSourceLease{wal: w, epoch: p.epoch, slot: slot, fd: fd, off: off, size: size}, nil
}

func (s *walSourceLease) read(p []byte, off int64, noWait bool) error {
	if s.done {
		return errLogCut
	}
	if off < s.off || off > s.off+int64(s.size) || int64(len(p)) > s.off+int64(s.size)-off {
		return fmt.Errorf("%w: read outside a leased log interval", ErrCorruptLog)
	}
	for len(p) > 0 {
		var n int
		var err error
		if noWait {
			n, err = sysPreadNoWait(s.fd, p, off%segSize)
			if notReadNow(n, len(p), err) {
				return errAway
			}
		} else {
			n, err = sysPread(s.fd, p, off%segSize)
		}
		if errors.Is(err, io.EOF) || err == nil && n == 0 {
			return fmt.Errorf("%w: a leased segment ends before %d", ErrCorruptLog, off+int64(len(p)))
		}
		if err != nil {
			return err
		}
		p, off = p[n:], off+int64(n)
	}
	return nil
}

// release runs after the matching I/O acknowledgement. Returned paths
// belong to the loop's normal removeAll path; a ring wakes its maker in
// cut. No client window or destination drain is needed to free a source.
func (s *walSourceLease) release() []string {
	if s.done {
		return nil
	}
	s.done = true
	w := s.wal
	s.wal = nil
	w.mu.Lock()
	p := &w.fetchPins[s.slot]
	matched := p.refs > 0 && p.epoch == s.epoch && p.fd == s.fd
	if matched {
		p.refs--
	}
	w.mu.Unlock()
	if !matched {
		return nil
	}
	return w.cut(w.fetchCut)
}

// copyPlannedStage never retries through read/readDurable. A durability
// advance after planning cannot accidentally turn an on-loop stage copy
// into disk I/O. The loop cannot overwrite the stage during this copy.
func (w *wal) copyPlannedStage(p []byte, off int64) bool {
	if w.stageLen <= 0 || off < max(0, w.off-w.stageLen) || off > w.off || int64(len(p)) > w.off-off {
		return false
	}
	for len(p) > 0 {
		pos := off & (w.stageLen - 1)
		n := copy(p, w.stage[pos:])
		p, off = p[n:], off+int64(n)
	}
	return true
}

// leasedCkpt owns only a checkpoint file, never its store's mappings.
// Temp-file/rename keeps this inode immutable. Close can retire it on
// the loop while a worker still reads; the last acknowledgement closes it.
type leasedCkpt struct {
	ckptFile
	refs    uint32
	retired bool
	closed  bool
}

type ckptSourceLease struct {
	file *leasedCkpt
	done bool
}

func (st *store) lendCkpt(gen int) (*ckptSourceLease, error) {
	if gen < 0 || gen >= len(st.ckpts) {
		return nil, errAway
	}
	f := st.ckpts[gen]
	if f == nil {
		return nil, errAway
	}
	p, ok := f.(*leasedCkpt)
	if !ok {
		p = &leasedCkpt{ckptFile: f}
		st.ckpts[gen] = p
	}
	if p.retired || p.closed {
		return nil, errAway
	}
	p.refs++
	return &ckptSourceLease{file: p}, nil
}

func (f *leasedCkpt) Close() error {
	f.retired = true
	if f.refs > 0 || f.closed {
		return nil
	}
	f.closed = true
	return f.ckptFile.Close()
}

func (s *ckptSourceLease) release() error {
	if s.done {
		return nil
	}
	s.done = true
	f := s.file
	s.file = nil
	f.refs--
	if f.refs == 0 && f.retired {
		return f.Close()
	}
	return nil
}
