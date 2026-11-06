//go:build sim

package simrun

import "github.com/jellevandenhooff/gosim"

// HostPages keeps pages of bytes in a file on the disk of the machine that
// runs the simulation, outside it (gosim.HostFile), each in a slot that
// stays taken while its owner is reachable: a simfs.Store. Its reads and
// writes change nothing in the run.
type HostPages struct{ f *gosim.HostFile }

// NewHostPages makes an empty HostPages, which the end of the run closes.
func NewHostPages() (*HostPages, error) {
	f, err := gosim.CreateHostFile()
	if err != nil {
		return nil, err
	}
	return &HostPages{f: f}, nil
}

// Put writes p to a free slot that stays taken while owner is reachable,
// and returns where.
func (h *HostPages) Put(owner any, p []byte) (int64, error) {
	off, err := h.f.Slot(owner, len(p))
	if err != nil {
		return 0, err
	}
	_, err = h.f.WriteAt(p, off)
	return off, err
}

// Get reads the slot at off into p.
func (h *HostPages) Get(off int64, p []byte) error {
	_, err := h.f.ReadAt(p, off)
	return err
}

// Close closes the file and gives its space back.
func (h *HostPages) Close() error { return h.f.Close() }
