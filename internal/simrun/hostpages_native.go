//go:build !sim

package simrun

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
)

// HostPages keeps pages of bytes in a file outside memory, each in a slot
// that stays taken while its owner is reachable: a simfs.Store. Under
// gosim the file is a host file of the run; natively it is a temporary
// file, removed as it is made.
type HostPages struct {
	f *os.File
	// mu guards the slots: a finalizer gives a slot back from the
	// collector's goroutine.
	mu   sync.Mutex
	size int
	end  int64
	free []int64
}

// errPageSize is the error of a page of another size than the first.
var errPageSize = errors.New("simrun: the pages of a HostPages all have one size")

// NewHostPages makes an empty HostPages, which lives as long as the
// process.
func NewHostPages() (*HostPages, error) {
	f, err := os.CreateTemp("", "simrun-pages-*")
	if err != nil {
		return nil, err
	}
	if err := os.Remove(f.Name()); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return &HostPages{f: f}, nil
}

// Put writes p to a free slot that stays taken while owner is reachable,
// and returns where. owner must point to the start of an object of its
// own with no finalizer, as runtime.SetFinalizer requires.
func (h *HostPages) Put(owner any, p []byte) (int64, error) {
	h.mu.Lock()
	if h.size == 0 {
		h.size = len(p)
	}
	if len(p) != h.size {
		h.mu.Unlock()
		return 0, fmt.Errorf("%w: %d bytes, not %d", errPageSize, len(p), h.size)
	}
	var off int64
	if n := len(h.free); n > 0 {
		off, h.free = h.free[n-1], h.free[:n-1]
	} else {
		off, h.end = h.end, h.end+int64(h.size)
	}
	h.mu.Unlock()
	runtime.SetFinalizer(owner, func(any) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.free = append(h.free, off)
	})
	_, err := h.f.WriteAt(p, off)
	return off, err
}

// Get reads the slot at off into p.
func (h *HostPages) Get(off int64, p []byte) error {
	_, err := h.f.ReadAt(p, off)
	return err
}

// Close closes the file and gives its space back.
func (h *HostPages) Close() error { return h.f.Close() }
