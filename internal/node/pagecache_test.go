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

// residentBytes is how much of the files under dir the page cache holds.
func residentBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		f, err := os.Open(path)
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
	if err := checkValues(r.n, want); err != nil {
		t.Fatal(err)
	}
	var held int64
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if held = residentBytes(t, dir); held < 1<<20 {
			return
		}
	}
	t.Fatalf("the page cache still holds %d KiB of the node's files", held>>10)
}
