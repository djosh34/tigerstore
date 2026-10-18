package wal_test

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/djosh34/tigerstore/internal/mvcc"
	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
)

// TestDiag270ValueAfterRecoveryReuse exercises the real WAL's quarantine of
// next-2 left by a killed process, followed by two rotations, with the chaos
// archiver's concurrent reads. No crash or loss of unsynced data is needed.
func TestDiag270ValueAfterRecoveryReuse(t *testing.T) {
	bin := os.Getenv("DIAG270_LAZYFS")
	if bin == "" {
		t.Skip("set DIAG270_LAZYFS to the pinned LazyFS binary")
	}
	dir := t.TempDir()
	backing, mount := filepath.Join(dir, "backing"), filepath.Join(dir, "mount")
	for _, p := range []string{backing, mount} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	cfg := filepath.Join(dir, "config.toml")
	text := fmt.Sprintf("[faults]\nfifo_path=%q\n[cache]\napply_eviction=false\n[cache.simple]\ncustom_size=\"64mb\"\nblocks_per_page=1\n[filesystem]\nlog_all_operations=true\nlogfile=%q\n", filepath.Join(dir, "fifo"), filepath.Join(dir, "lazyfs.log"))
	if err := os.WriteFile(cfg, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(filepath.Join(dir, "out"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, mount, "--config-path", cfg, "-o", "modules=subdir", "-o", "subdir="+backing, "-f")
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		exec.Command("fusermount3", "-u", mount).Run()
		cmd.Process.Kill()
		cmd.Wait()
		out.Close()
		if t.Failed() {
			for _, name := range []string{"lazyfs.log", "out"} {
				b, _ := os.ReadFile(filepath.Join(dir, name))
				if len(b) > 24000 {
					b = b[len(b)-24000:]
				}
				t.Logf("%s:\n%s", name, b)
			}
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var st syscall.Statfs_t
		if syscall.Statfs(mount, &st) == nil && st.Type == 0x65735546 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mount timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for attempt := range 300 {
		path := filepath.Join(mount, fmt.Sprintf("wal-%d", attempt))
		disk, err := osfs.New(path)
		must(err)
		w, _, err := wal.Open(disk)
		must(err)
		w.Append(nil, []*raftpb.Entry{entry(1, 1, "initial")})
		must(w.Sync())
		must(wal.PrepareSegment(disk, 2)) // Left prepared before the kill.
		must(w.Close())
		w, _, err = wal.Open(disk)
		must(err)

		stop := make(chan struct{})
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					// archiveOnce reads and stats this name during rename.
					os.ReadFile(filepath.Join(path, "next-0000000000000002"))
					os.Stat(filepath.Join(path, "next-0000000000000002"))
				}
			}()
		}
		rotate := func() {
			must(wal.PrepareSegment(disk, w.Seq()+1))
			must(w.Seal())
			must(w.Segment().Sync())
			must(w.Rotate().Close())
			must(w.Place())
			must(w.SyncDir())
		}
		rotate() // next-2 -> spare-2; new next-2 -> segment 2.
		value := []byte("value in segment A")
		w.Append(nil, []*raftpb.Entry{entry(1, 2, string(value))})
		must(w.Sync())
		ref := mvcc.Ref{Index: 2, Len: uint32(len(value)), Sum: crc32.Checksum(value, crc32.MakeTable(crc32.Castagnoli))}
		rotate() // spare-2 -> next-3 -> segment 3.
		w.Append(nil, []*raftpb.Entry{entry(1, 3, "value in segment B")})
		must(w.Sync())
		close(stop)
		wg.Wait()
		seq, at, ok := w.ValuePlace(ref.Index, ref.Off, ref.Len)
		got, err := w.Value(ref.Index, ref.Off, ref.Len)
		must(err)
		var dst []byte
		err = (mvcc.Fill{Key: []byte("/chaos/hold"), Ref: ref, Dst: &dst}).Set(got)
		must(w.Close())
		if err != nil {
			a, _ := os.ReadFile(filepath.Join(path, wal.SegmentName(2)))
			b, _ := os.ReadFile(filepath.Join(path, wal.SegmentName(3)))
			t.Fatalf("attempt %d: %v; ValuePlace=(%d,%d,%v), value=%q; segment2==segment3: %v; headers=%x/%x", attempt, err, seq, at, ok, got, bytes.Equal(a, b), a[:24], b[:24])
		}
	}
}
