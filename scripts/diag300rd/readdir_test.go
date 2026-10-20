//go:build linux

package diag300rd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestLazyFSReadDirSize races READDIRPLUS attributes with pread on one fd.
// The target is immutable and synced. No reader calls stat or reopens it.
func TestLazyFSReadDirSize(t *testing.T) {
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
	config := fmt.Sprintf("[faults]\nfifo_path=%q\n[cache]\napply_eviction=false\n[cache.simple]\ncustom_size=\"64mb\"\nblocks_per_page=1\n[filesystem]\nlog_all_operations=false\nlogfile=%q\n", filepath.Join(root, "fifo"), filepath.Join(root, "lazyfs.log"))
	if err := os.WriteFile(filepath.Join(root, "config"), []byte(config), 0644); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(filepath.Join(root, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	cmd := exec.Command(bin, mount, "--config-path", "config", "-o", "modules=subdir", "-o", "subdir="+backing, "-f")
	cmd.Dir, cmd.Stdout, cmd.Stderr = root, out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		if b, e := exec.Command("fusermount3", "-uz", mount).CombinedOutput(); e != nil {
			t.Errorf("unmount: %s %v", b, e)
		}
		select {
		case e := <-done:
			if e != nil {
				t.Logf("LazyFS exit: %v", e)
			}
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("LazyFS did not exit after unmount")
		}
		if t.Failed() {
			b, _ := os.ReadFile(filepath.Join(root, "stdout"))
			t.Logf("LazyFS stdout: %s", b)
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for exec.Command("mountpoint", "-q", mount).Run() != nil {
		if time.Now().After(deadline) {
			t.Fatal("mount timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	dir := filepath.Join(mount, "wal")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"run", "next-2", "other"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("small"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	name := filepath.Join(dir, "0000000000000001.wal")
	w, err := os.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{0x5a}, 90*1024)
	_, writeErr := w.Write(data)
	syncErr, closeErr := w.Sync(), w.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		t.Fatalf("write=%v sync=%v close=%v", writeErr, syncErr, closeErr)
	}
	r, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	fd := int(r.Fd())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var reads, shorts, lists, mutations atomic.Uint64
	var first sync.Once
	var firstShort string
	errs := make(chan error, 9)
	start := make(chan struct{})
	launch := func(fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := fn(); err != nil {
				errs <- err
				cancel()
			}
		}()
	}
	for worker := 0; worker < 6; worker++ {
		launch(func() error {
			var b [3]byte
			offsets := [...]int64{83209, 83385, 4093, 32767, 65535, 90000}
			for i := 0; ctx.Err() == nil; i++ {
				at := offsets[i%len(offsets)]
				n, err := syscall.Pread(fd, b[:], at)
				reads.Add(1)
				if n < len(b) {
					shorts.Add(1)
					first.Do(func() { firstShort = fmt.Sprintf("offset=%d n=%d err=%v", at, n, err) })
				} else if !bytes.Equal(b[:], data[at:at+3]) {
					return fmt.Errorf("wrong bytes at %d: %x", at, b)
				}
				if err != nil {
					return fmt.Errorf("pread at %d: %w", at, err)
				}
			}
			return nil
		})
	}
	for worker := 0; worker < 2; worker++ {
		launch(func() error {
			for ctx.Err() == nil {
				// Do not call DirEntry.Info: that would refresh the target's size.
				if _, err := os.ReadDir(dir); err != nil {
					return err
				}
				lists.Add(1)
			}
			return nil
		})
	}
	launch(func() error {
		for ctx.Err() == nil {
			p := filepath.Join(dir, "churn")
			if err := os.WriteFile(p, []byte("x"), 0644); err != nil {
				return err
			}
			if err := os.Remove(p); err != nil {
				return err
			}
			mutations.Add(1)
		}
		return nil
	})
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	t.Logf("reads=%d short_reads=%d listings=%d create_remove_pairs=%d; first short: %s", reads.Load(), shorts.Load(), lists.Load(), mutations.Load(), firstShort)
	if shorts.Load() != 0 {
		t.Errorf("%d preads returned fewer than 3 bytes inside an immutable %d-byte file", shorts.Load(), len(data))
	}
	if reads.Load() == 0 || lists.Load() == 0 || mutations.Load() == 0 {
		t.Error("probe did not exercise every worker type")
	}
}
