package wl

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

func sourceWAL(t *testing.T, files int) *wal {
	t.Helper()
	w := &wal{dir: t.TempDir(), off: int64(files) * segSize, stable: int64(files) * segSize}
	w.done.Store(w.off)
	for i := range files {
		name := segPath(w.dir, int64(i))
		if err := os.WriteFile(name, bytes.Repeat([]byte{byte('a' + i)}, 4096), 0o600); err != nil {
			t.Fatal(err)
		}
		fd, err := unix.Open(name, unix.O_RDONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		w.rfds = append(w.rfds, fd)
	}
	t.Cleanup(func() {
		for _, fd := range w.rfds {
			_ = unix.Close(fd)
		}
	})
	return w
}

func TestWALFetchLeaseDefersPhysicalRingReuse(t *testing.T) {
	w := sourceWAL(t, 2)
	w.ring, w.freedCh = &ringInfo{segs: 2}, make(chan struct{}, 1)
	lease, err := w.lendFetch(128, 32)
	if err != nil {
		t.Fatal(err)
	}
	if paths := w.cut(segSize); len(paths) != 0 || w.rbase != 0 || len(w.freed) != 0 {
		t.Fatal("a cut returned a leased physical generation to the ring")
	}
	if err := w.recycle(); err != nil || len(w.spares) != 0 {
		t.Fatalf("the maker recycled a leased physical file: %v", err)
	}
	var got [32]byte
	if err := lease.read(got[:], 128, true); err != nil && !errors.Is(err, errAway) {
		t.Fatal(err)
	}
	if err := lease.read(got[:], 128, false); err != nil || !bytes.Equal(got[:], bytes.Repeat([]byte("a"), len(got))) {
		t.Fatalf("the worker lost its source after cut/recycle: %v", err)
	}
	lease.release()
	if w.rbase != 1 || len(w.freed) != 1 {
		t.Fatal("I/O acknowledgement did not resume the pending cut")
	}
	newer, err := w.lendFetch(segSize+128, 32)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.recycle(); err != nil || len(w.spares) != 1 {
		t.Fatalf("the acknowledged extent did not return to the maker: %v", err)
	}
	if err := os.WriteFile(w.spares[0], []byte("a different generation"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := lease.read(got[:], 128, false); !errors.Is(err, errLogCut) || len(lease.release()) != 0 {
		t.Fatal("a stale lease read a reused generation or released it twice")
	}
	if w.fetchPins[newer.slot].refs != 1 {
		t.Fatal("the old acknowledgement cleared the reused source slot")
	}
	if err := newer.read(got[:], segSize+128, false); err != nil || !bytes.Equal(got[:], bytes.Repeat([]byte("b"), len(got))) {
		t.Fatalf("the new source generation was disturbed: %v", err)
	}
	newer.release()
}

func TestWALFetchSourcesSplitAndReleaseIndependently(t *testing.T) {
	w := sourceWAL(t, 3)
	a, err := w.lendFetch(segSize-8, 100)
	if err != nil || a.size != 8 {
		t.Fatalf("the first source crossed its physical extent: %v", err)
	}
	b, err := w.lendFetch(segSize+128, 32)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.lendFetch(2*segSize+128, 32); !errors.Is(err, errAway) {
		t.Fatal("the piece acquired a third physical source")
	}
	w.cut(3 * segSize)
	if len(b.release()) != 0 || w.rbase != 0 {
		t.Fatal("releasing the later source retired the earlier pinned file")
	}
	if paths := a.release(); len(paths) != 3 || w.rbase != 3 {
		t.Fatal("the matching final acknowledgement did not finish the pending cut")
	}
	if a.release() != nil || b.release() != nil {
		t.Fatal("a stale acknowledgement returned files twice")
	}
}

func TestPlannedStageCopyCannotFallBackToDisk(t *testing.T) {
	w := &wal{stage: make([]byte, 64), stageLen: 64, off: 8192}
	for i := range w.stage {
		w.stage[i] = byte(i)
	}
	// Planning selected the stage before this durability advance. There
	// are deliberately no disk descriptors for a fallback read to use.
	w.done.Store(w.off)
	var got [12]byte
	if !w.copyPlannedStage(got[:], 8180) || !bytes.Equal(got[:], w.stage[52:]) {
		t.Fatal("a durability advance changed the selected stage copy")
	}
	if w.copyPlannedStage(got[:], 8000) || w.copyPlannedStage(got[:], 8190) {
		t.Fatal("the stage copy accepted overwritten or unwritten source bytes")
	}
}

func TestCheckpointFetchLeaseOutlivesStoreAndRename(t *testing.T) {
	for _, gen := range []int{0, 1} {
		st, _ := newTestStore(t)
		t.Cleanup(func() { _ = st.close() })
		name := filepath.Join(t.TempDir(), "checkpoint")
		if err := os.WriteFile(name, []byte("old immutable inode"), 0o600); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		st.ckpts[gen] = f
		lease, err := st.lendCkpt(gen)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name+".tmp", []byte("replacement bytes"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(name+".tmp", name); err != nil {
			t.Fatal(err)
		}
		if err := st.close(); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len("old immutable inode"))
		if _, err := lease.file.ReadAt(got, 0); err != nil || string(got) != "old immutable inode" {
			t.Fatalf("closing the store lost a leased checkpoint inode: %q, %v", got, err)
		}
		if err := lease.release(); err != nil {
			t.Fatal(err)
		}
		if _, err := f.ReadAt(got, 0); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("the acknowledged old file is still open: %v", err)
		}
		if err := lease.release(); err != nil {
			t.Fatal(err)
		}
	}
}

type sourceNoWaitTrap struct {
	*os.File
	reads int
}

func (f *sourceNoWaitTrap) Fd() uintptr { return ^uintptr(0) }

func (f *sourceNoWaitTrap) ReadAt(p []byte, off int64) (int, error) {
	f.reads++
	return f.File.ReadAt(p, off)
}

func TestLeasedCheckpointStillAttemptsNoWait(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	trap := &sourceNoWaitTrap{File: f}
	wrapped := &leasedCkpt{ckptFile: trap}
	var p [8]byte
	if err := readFileNoWait(wrapped, p[:], 0); err == nil || trap.reads != 0 {
		t.Fatal("wrapping a leased file silently replaced NOWAIT with a blocking ReadAt")
	}
}

func TestFetchSourceControlFitsItsFixedWorkAllowance(t *testing.T) {
	bytes := 2 * (unsafe.Sizeof(walFetchPin{}) + unsafe.Sizeof(walSourceLease{}) + unsafe.Sizeof(leasedCkpt{}) + unsafe.Sizeof(ckptSourceLease{}))
	if bytes > 16<<10 {
		t.Fatalf("fixed source leases need %d bytes, past the shared control region", bytes)
	}
}
