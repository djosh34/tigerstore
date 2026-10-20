//go:build chaosenv

package env

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// TestLazyFSPartialBlockAppend reads durable bytes in the block of a new
// unsynced append, through a different descriptor after a cache clear.
// Reads in LazyFS do not populate its block cache, unlike writes.
func TestLazyFSPartialBlockAppend(t *testing.T) {
	d := newDisk(t, lazyfsBin(t))
	cfg, err := os.ReadFile(d.cfgfile)
	if err != nil {
		t.Fatal(err)
	}
	cfg = bytes.ReplaceAll(cfg, []byte("log_all_operations=false"), []byte("log_all_operations=true"))
	if err := os.WriteFile(d.cfgfile, cfg, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := d.Remount(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { t.Logf("LazyFS operations:\n%s", d.lazyLog()) })
	for _, reset := range []string{"clear", "remount"} {
		for _, size := range []int{3000, 100000} {
			for _, appendBytes := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/size=%d/append=%t", reset, size, appendBytes), func(t *testing.T) {
					path := filepath.Join(d.Data, "probe")
					want := make([]byte, size)
					for i := range want {
						want[i] = byte('a' + i%26)
					}
					if err := write(t, path, string(want), true); err != nil {
						t.Fatal(err)
					}
					if reset == "clear" {
						if _, err := d.PowerLoss(); err != nil {
							t.Fatal(err)
						}
					} else if err := d.Remount(); err != nil {
						t.Fatal(err)
					}
					r, err := os.Open(path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := r.Close(); err != nil {
							t.Error(err)
						}
					})
					block := size / 4096 * 4096
					read := func(stage string) {
						t.Helper()
						readsBefore := strings.Count(d.lazyLog(), "lfs_read(")
						// Force FUSE reads, rather than accepting the kernel's old pages.
						if err := unix.Fadvise(int(r.Fd()), 0, 0, unix.FADV_DONTNEED); err != nil {
							t.Fatal(err)
						}
						for _, off := range []int{block, size - 274, size - 4} {
							got := make([]byte, 4)
							n, err := r.ReadAt(got, int64(off))
							t.Logf("%s: offset=%d n=%d bytes=%q err=%v", stage, off, n, got[:n], err)
							if err != nil || !bytes.Equal(got, want[off:off+4]) {
								t.Errorf("%s: read at %d = %q, %v; want %q", stage, off, got[:n], err, want[off:off+4])
							}
						}
						readsAfter := strings.Count(d.lazyLog(), "lfs_read(")
						t.Logf("%s: LazyFS read callbacks=%d", stage, readsAfter-readsBefore)
						if readsAfter <= readsBefore {
							t.Fatal("reads did not reach LazyFS")
						}
					}
					read("before append")
					if appendBytes {
						w, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
						if err != nil {
							t.Fatal(err)
						}
						if n, err := w.WriteString("TAIL"); err != nil || n != 4 {
							t.Fatalf("append = %d, %v", n, err)
						}
						if err := w.Close(); err != nil {
							t.Fatal(err)
						}
					}
					read("after append or control")
					backing, err := os.ReadFile(filepath.Join(d.root, "probe"))
					if err != nil || !bytes.Equal(backing, want) {
						t.Fatalf("backing durable prefix changed: size=%d err=%v", len(backing), err)
					}
				})
			}
		}
	}
}
