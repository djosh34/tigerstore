//go:build linux

package osfs

import (
	"os"
	"syscall"
	"testing"
)

func TestPre47BlockingFsync(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "fsync")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := f.WriteString("durable bytes"); err != nil {
		t.Fatal(err)
	}
	if err := pre47BlockingFsync(f.Fd()); err != 0 {
		t.Fatal(err)
	}
	if err := pre47BlockingFsync(^uintptr(0)); err != syscall.EBADF {
		t.Fatalf("invalid fd error %v", err)
	}
}
