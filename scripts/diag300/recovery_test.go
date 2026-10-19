package diag300

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
	"go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sys/unix"
)

// TestLazyFSRecoveryValueReads compares a complete record read during recovery
// with a small value read after it, while startup prepares the next segment.
func TestLazyFSRecoveryValueReads(t *testing.T) {
	bin := os.Getenv("CHAOS_LAZYFS")
	if bin == "" {
		t.Fatal("CHAOS_LAZYFS is required")
	}
	root := t.TempDir()
	backing, mount := filepath.Join(root, "backing"), filepath.Join(root, "mount")
	for _, p := range []string{backing, mount} {
		if err := os.Mkdir(p, 0755); err != nil {
			t.Fatal(err)
		}
	}
	fifo, log := filepath.Join(root, "fifo"), filepath.Join(root, "lazyfs.log")
	config := fmt.Sprintf("[faults]\nfifo_path=%q\n[cache]\napply_eviction=false\n[cache.simple]\ncustom_size=\"64mb\"\nblocks_per_page=1\n[filesystem]\nlog_all_operations=true\nlogfile=%q\n", fifo, log)
	if err := os.WriteFile(filepath.Join(root, "config"), []byte(config), 0644); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(filepath.Join(root, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, mount, "--config-path", "config", "-o", "modules=subdir", "-o", "subdir="+backing, "-f")
	cmd.Dir, cmd.Stdout, cmd.Stderr = root, out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if b, e := exec.Command("fusermount3", "-uz", mount).CombinedOutput(); e != nil {
			t.Errorf("unmount: %s %v", b, e)
		}
		if e := cmd.Wait(); e != nil {
			t.Logf("LazyFS exit: %v", e)
		}
		if e := out.Close(); e != nil {
			t.Error(e)
		}
		for _, name := range []string{"lazyfs.log", "stdout"} {
			b, e := os.ReadFile(filepath.Join(root, name))
			if e != nil {
				t.Error(e)
				continue
			}
			if e := os.MkdirAll("../../results/diag300", 0755); e != nil {
				t.Error(e)
				continue
			}
			if e := os.WriteFile(filepath.Join("../../results/diag300", name), b, 0644); e != nil {
				t.Error(e)
			}
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for exec.Command("mountpoint", "-q", mount).Run() != nil {
		if time.Now().After(deadline) {
			t.Fatal("mount timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	fs, err := osfs.New(filepath.Join(mount, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	w, _, err := wal.Open(fs)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("v"), 29)
	for i := uint64(1); i <= 1200; i++ {
		w.Append(&wal.HardState{Term: 1, Commit: i}, []*raftpb.Entry{{Term: new(uint64(1)), Index: new(i), Data: data}})
		if err := w.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	// A normal run leaves this empty prepared file for the next start.
	if err := wal.PrepareSegment(fs, 2); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for turn := 0; turn < 24; turn++ {
		before, e := os.ReadFile(log)
		if e != nil {
			t.Fatal(e)
		}
		f, e := os.OpenFile(fifo, os.O_WRONLY, 0)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = f.WriteString("lazyfs::clear-cache\n"); e != nil {
			t.Fatal(e)
		}
		if e = f.Close(); e != nil {
			t.Fatal(e)
		}
		until := time.Now().Add(time.Second)
		for {
			b, e := os.ReadFile(log)
			if e != nil {
				t.Fatal(e)
			}
			if len(b) > len(before) && strings.Contains(string(b[len(before):]), "cache is cleared") {
				break
			}
			if time.Now().After(until) {
				t.Fatal("clear timeout")
			}
			time.Sleep(time.Millisecond)
		}
		w, rec, e := wal.Open(fs)
		if e != nil {
			t.Fatal(e)
		}
		if rec.Last != 1200 {
			t.Fatalf("turn %d: recovered %d entries, want 1200 after cache clear", turn, rec.Last)
		}
		// The real startup writes run.tmp and renames it before core replay.
		run, e := fs.Create("run.tmp")
		if e != nil {
			t.Fatal(e)
		}
		if _, e := run.Write([]byte{byte(turn + 1), 0, 0, 0, 0, 0, 0, 0}); e != nil {
			t.Fatal(e)
		}
		if e := run.Sync(); e != nil {
			t.Fatal(e)
		}
		if e := run.Close(); e != nil {
			t.Fatal(e)
		}
		if e := fs.Rename("run.tmp", "run"); e != nil {
			t.Fatal(e)
		}
		if e := fs.SyncDir(); e != nil {
			t.Fatal(e)
		}
		for i := uint64(1); i <= rec.Last; i++ {
			if _, e := w.Entry(i); e != nil {
				t.Fatal(e)
			}
		}
		for i := uint64(1); i <= rec.Last; i++ {
			value, e := w.Value(i, 23, 4)
			if e != nil || !bytes.Equal(value, data[23:27]) {
				t.Fatalf("turn %d index %d: %q %v", turn, i, value, e)
			}
		}
		// The node reads old watch values while startup prepares the next
		// segment through another path in the same directory.
		r, _, e := w.ValueReader(1, 23, 4)
		if e != nil {
			t.Fatal(e)
		}
		type place struct {
			index uint64
			at    int64
		}
		var places []place
		for i := uint64(1); i <= rec.Last; i++ {
			_, at, e := w.ValueReader(i, 23, 4)
			if e != nil {
				t.Fatal(e)
			}
			places = append(places, place{i, at})
		}
		// Alternate the normal warm path and forced cold reads. DONTNEED
		// drops only the kernel's clean pages, not LazyFS's unsynced data.
		if turn%2 == 1 {
			fd := r.(interface{ Fd() uintptr }).Fd()
			if e := unix.Fadvise(int(fd), 0, 0, unix.FADV_DONTNEED); e != nil {
				t.Fatal(e)
			}
		}
		beforeStat, e := os.Stat(filepath.Join(backing, "wal", wal.SegmentName(1)))
		if e != nil {
			t.Fatal(e)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		errors := make(chan error, 6)
		for worker := 0; worker < 6; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for _, p := range places {
					b, e := wal.ReadValue(r, p.at, p.index, 23, 4)
					if e != nil || !bytes.Equal(b, data[23:27]) {
						errors <- fmt.Errorf("entry %d file offset %d: %q %v", p.index, p.at, b, e)
						return
					}
				}
			}()
		}
		close(start)
		// This is the startup call whose unlink/rename differs between
		// #294 and main. The WAL itself has not rotated.
		prepareErr := wal.PrepareSegment(fs, w.Seq()+1)
		wg.Wait()
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		// Also read after the call, in case the concurrent readers ran
		// ahead of PrepareSegment's directory operations.
		for _, p := range places {
			b, e := wal.ReadValue(r, p.at, p.index, 23, 4)
			if e != nil || !bytes.Equal(b, data[23:27]) {
				t.Fatalf("after prepare, turn %d entry %d offset %d: %q %v", turn, p.index, p.at, b, e)
			}
		}
		afterStat, e := os.Stat(filepath.Join(backing, "wal", wal.SegmentName(1)))
		if e != nil {
			t.Fatal(e)
		}
		if !os.SameFile(beforeStat, afterStat) || beforeStat.Size() != afterStat.Size() {
			t.Errorf("segment changed: before=%v after=%v", beforeStat, afterStat)
		}
		close(errors)
		for e := range errors {
			t.Error(e)
		}
		// Leave an unsynced append as a killed process can. The next
		// clear must discard it without disturbing the recovered prefix.
		w.Append(nil, []*raftpb.Entry{{Term: new(uint64(1)), Index: new(rec.Last + 1), Data: data}})
		if e := w.Flush(); e != nil {
			t.Fatal(e)
		}
		if e := w.Close(); e != nil {
			t.Fatal(e)
		}
		t.Logf("turn %d cold=%v: recovered and read %d entries during PrepareSegment(2); backing inode and size unchanged", turn, turn%2 == 1, rec.Last)
	}
}
