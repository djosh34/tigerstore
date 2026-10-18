package diag267

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Exercise the recovery next->spare rename, recreate next, then reuse spare
// for a later segment, while the chaos archiver reads the same names.
func TestLazyFSRenameAliases(t *testing.T) {
	bin := os.Getenv("DIAG267_LAZYFS")
	if bin == "" {
		t.Skip("set DIAG267_LAZYFS")
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
			for _, n := range []string{"lazyfs.log", "out"} {
				b, _ := os.ReadFile(filepath.Join(dir, n))
				if len(b) > 24000 {
					b = b[len(b)-24000:]
				}
				t.Logf("%s:\n%s", n, b)
			}
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		b, _ := os.ReadFile(filepath.Join(dir, "lazyfs.log"))
		if bytes.Contains(b, []byte("running LazyFS")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mount timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	write := func(name string, b []byte) {
		f, err := os.OpenFile(filepath.Join(mount, name), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.WriteAt(b, 0); err != nil {
			t.Fatal(err)
		}
		if err = f.Sync(); err != nil {
			t.Fatal(err)
		}
		if err = f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	rename := func(a, b string) {
		if err := os.Rename(filepath.Join(mount, a), filepath.Join(mount, b)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 300; i++ {
		// Unique names isolate each attempt from the previous attempt's mappings.
		next, spare, a, b := fmt.Sprintf("next-%d", i), fmt.Sprintf("spare-%d", i), fmt.Sprintf("%d-a.wal", i), fmt.Sprintf("%d-b.wal", i)
		write(next, []byte("old prepared file"))
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					os.ReadFile(filepath.Join(mount, next))
					os.Stat(filepath.Join(mount, next))
				}
			}()
		}
		rename(next, spare)
		write(next, []byte("tigerwal segment A"))
		rename(next, a)
		rename(spare, b)
		write(b, []byte("tigerwal segment B"))
		close(stop)
		wg.Wait()
		av, ae := os.ReadFile(filepath.Join(mount, a))
		bv, be := os.ReadFile(filepath.Join(mount, b))
		if ae != nil || be != nil || !bytes.Equal(av, []byte("tigerwal segment A")) || !bytes.Equal(bv, []byte("tigerwal segment B")) {
			t.Fatalf("attempt %d: A=%q (%v), B=%q (%v)", i, av, ae, bv, be)
		}
	}
}
