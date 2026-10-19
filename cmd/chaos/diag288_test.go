//go:build chaosenv

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/chaos/env"
)

// TestDiag288RepairWhileArchiving forces the archive's open/read/close
// interval to overlap repair. A FIFO makes that normally tiny interval
// deterministic, without a large WAL, timing campaign, or tigerstore node.
// Its open descriptor pins ext4 in exactly the same way as a WAL read.
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
	pipe := filepath.Join(wal, "copy.wal")
	if err := syscall.Mkfifo(pipe, 0o600); err != nil {
		t.Fatal(err)
	}
	archived := make(chan error, 1)
	go func() { archived <- e.archiveOnce(t.TempDir(), map[string]time.Time{}) }()
	// A nonblocking writer connects only once archiveOnce has opened its
	// reader. Keep the writer open to hold os.ReadFile before EOF.
	var writer *os.File
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		writer, err = os.OpenFile(pipe, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.ENXIO) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	if writer == nil {
		t.Fatal("archive did not open its reader")
	}
	defer writer.Close()
	fds, _ := filepath.Glob("/proc/self/fd/*")
	for _, fd := range fds {
		if target, err := os.Readlink(fd); err == nil && strings.HasPrefix(target, d.Backing+"/") {
			info, _ := os.ReadFile("/proc/self/fdinfo/" + filepath.Base(fd))
			t.Logf("archive holder: pid=%d %s -> %s; %s", os.Getpid(), fd, target, info)
		}
	}
	repaired := make(chan error, 1)
	go func() { repaired <- e.repair(d) }()
	// A correct repair cannot finish until the archive closes its reader.
	var repairErr error
	early := false
	select {
	case repairErr = <-repaired:
		early = true
		t.Logf("repair while the archive holds the mount: %v", repairErr)
	case <-time.After(200 * time.Millisecond):
	}
	if err := writer.Close(); err != nil {
		t.Error(err)
	}
	if err := <-archived; err != nil {
		t.Error(err)
	}
	if !early {
		repairErr = <-repaired
	}
	if early || repairErr != nil {
		t.Fatalf("repair must wait for archive EOF and then succeed: early=%v err=%v", early, repairErr)
	}
	t.Log("archive closed its reader; unmount, fsck and remount succeeded")
}
