package diag300

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djosh34/tigerstore/internal/osfs"
	"github.com/djosh34/tigerstore/internal/wal"
	"go.etcd.io/raft/v3/raftpb"
)

// TestLazyFSRecoveryValueReads compares a complete record read during recovery
// with a small value read after it. No process writes during the reads.
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
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for turn := 0; turn < 8; turn++ {
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
		if e := w.Close(); e != nil {
			t.Fatal(e)
		}
		t.Logf("turn %d: recovered and read %d entries", turn, rec.Last)
	}
}
