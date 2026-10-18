package node

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// residentBytes is how much of the files under dir whose names match
// pattern the page cache holds.
func residentBytes(t *testing.T, dir, pattern string) int64 {
	t.Helper()
	var total int64
	// The node's cleaner may remove a file during the walk; a removed
	// file holds no pages.
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		if ok, err := filepath.Match(pattern, d.Name()); !ok || err != nil {
			return err
		}
		f, err := os.Open(path)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		defer f.Close() //nolint:errcheck // a file opened for reading
		st, err := f.Stat()
		if err != nil || st.Size() == 0 {
			return err
		}
		m, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			return err
		}
		ps := int64(os.Getpagesize())
		vec := make([]byte, (st.Size()+ps-1)/ps)
		if _, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&m[0])), uintptr(len(m)), uintptr(unsafe.Pointer(&vec[0]))); errno != 0 {
			err = errno
		}
		for _, v := range vec {
			if v&1 != 0 {
				total += ps
			}
		}
		return errors.Join(err, unix.Munmap(m))
	})
	if err != nil {
		t.Fatal(err)
	}
	return total
}

// TestPageCacheGivenBack is #226's case: a node writes 8 MiB and reads
// its values back from the segments it maps. Within a second or two the
// page cache must hold almost none of its files, which its cgroup would
// otherwise charge to it past the margin its budget leaves the kernel.
func TestPageCacheGivenBack(t *testing.T) {
	dir := t.TempDir()
	r := runClean(t, dir, 0, func(c *Config) { c.SegmentBytes = 1 << 20 })
	want := map[string]int{}
	for round := range 160 {
		if _, err := putAll(r.n, round, keys("k", 50)...); err != nil {
			t.Fatal(err)
		}
		for _, k := range keys("k", 50) {
			want[k] = round
		}
	}
	// Before any read: the checkpoints the node wrote are closed, and
	// only their drop on close gives their pages back.
	waitPageCache(t, dir, "after the writes")
	// The reads open and map the sealed segments.
	if err := checkValues(r.n, want); err != nil {
		t.Fatal(err)
	}
	waitPageCache(t, dir, "after the reads")
}

// waitPageCache waits until the page cache holds less than 1 MiB of the
// files in dir and none of its checkpoints.
func waitPageCache(t *testing.T, dir, when string) {
	t.Helper()
	var held, ckpts int64
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		held, ckpts = residentBytes(t, dir, "*"), residentBytes(t, dir, "ckpt-*")
		if held < 1<<20 && ckpts == 0 {
			return
		}
	}
	t.Fatalf("%s the page cache still holds %d KiB of the node's files, %d KiB of them checkpoints", when, held>>10, ckpts>>10)
}
