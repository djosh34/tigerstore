//go:build chaosenv

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/chaos/env"
)

// TestDiag288RepairWhileArchiving forces the archive's open/read/close
// interval to overlap repair. Only the archive's ordinary read descriptor
// pins the mount: no tigerstore process or other open file is needed.
func TestDiag288RepairWhileArchiving(t *testing.T) {
	d, err := env.NewDisk(t.TempDir(), fmt.Sprintf("diag288-%d", os.Getpid()), 64<<20, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	e := &environment{disks: [3]*env.Disk{d, {Data: t.TempDir()}, {Data: t.TempDir()}}}
	wal := filepath.Join(d.Data, "wal")
	if err := os.MkdirAll(wal, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(wal, "copy.wal"), []byte("archive me"), 0o600); err != nil {
		t.Fatal(err)
	}
	opened, release := make(chan struct{}), make(chan struct{})
	originalRead := readArchiveFile
	t.Cleanup(func() { readArchiveFile = originalRead })
	readArchiveFile = func(path string) ([]byte, error) {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		close(opened)
		<-release
		b, err := io.ReadAll(f)
		return b, errors.Join(err, f.Close())
	}
	archived := make(chan error, 1)
	dir := t.TempDir()
	go func() { archived <- e.archiveOnce(dir, map[string]time.Time{}) }()
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("archive did not open its reader")
	}
	fds, _ := filepath.Glob("/proc/self/fd/*")
	holders := 0
	for _, fd := range fds {
		if target, err := os.Readlink(fd); err == nil && strings.HasPrefix(target, d.Backing+"/") {
			holders++
			info, _ := os.ReadFile("/proc/self/fdinfo/" + filepath.Base(fd))
			t.Logf("archive holder: pid=%d %s -> %s; %s", os.Getpid(), fd, target, info)
		}
	}
	if holders != 1 {
		t.Errorf("want exactly one descriptor on the mount (archive reader), got %d", holders)
	}
	repaired := make(chan error, 1)
	go func() { repaired <- e.repair(d) }()
	var repairErr error
	early := false
	select {
	case repairErr = <-repaired:
		early = true
		t.Logf("repair while the archive holds the mount: %v", repairErr)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-archived; err != nil {
		t.Error(err)
	}
	if !early {
		repairErr = <-repaired
	}
	if early || repairErr != nil {
		t.Fatalf("repair must wait for archive close and then succeed: early=%v err=%v", early, repairErr)
	}
	t.Log("archive closed its sole reader; unmount, fsck and remount succeeded")
}
