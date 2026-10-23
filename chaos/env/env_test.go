//go:build chaosenv

package env

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// These tests need sudo -n.
func newDisk(t *testing.T) *Disk {
	t.Helper()
	d, err := NewDisk(t.TempDir(), fmt.Sprintf("chaosenv-%d", time.Now().UnixNano()%1e9), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d
}

// write writes s to path and syncs it.
func write(t *testing.T, path, s string) error {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, err = f.WriteString(s)
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

// TestDeviceFaults fails an fsync, slows one, stalls one, and fills the
// disk.
func TestDeviceFaults(t *testing.T) {
	d := newDisk(t)
	path := filepath.Join(d.Data, "f")
	if err := d.FailWrites(); err != nil {
		t.Fatal(err)
	}
	if err := write(t, path, "x"); err == nil {
		t.Fatal("an fsync succeeded while writes fail")
	}
	if err := d.Healthy(); err != nil {
		t.Fatal(err)
	}
	if err := d.Delay(0, 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := write(t, filepath.Join(d.Data, "g"), "y"); err != nil || time.Since(start) < 300*time.Millisecond {
		t.Fatalf("a delayed fsync took %v: %v", time.Since(start), err)
	}
	if err := d.Healthy(); err != nil {
		t.Fatal(err)
	}
	if err := d.Stall(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- write(t, filepath.Join(d.Data, "h"), "z") }()
	select {
	case err := <-done:
		t.Fatalf("an fsync finished during the stall: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	if err := d.Healthy(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("the stalled fsync after the heal: %v", err)
	}
	if _, err := d.Fill(); err != nil {
		t.Fatal(err)
	}
	if err := write(t, filepath.Join(d.Data, "big"), string(make([]byte, 1<<20))); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("a write to a full disk: %v", err)
	}
	if err := d.Unfill(); err != nil {
		t.Fatal(err)
	}
	if err := write(t, filepath.Join(d.Data, "big"), string(make([]byte, 1<<20))); err != nil {
		t.Fatal(err)
	}
}

// TestCgroupCPU throttles a busy process.
func TestCgroupCPU(t *testing.T) {
	c, err := NewCgroup(fmt.Sprintf("test-%d", time.Now().UnixNano()%1e9))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sh", "-c", "while :; do :; done")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := c.Add(cmd.Process.Pid); err != nil {
		t.Fatal(err)
	}
	if err := c.LimitCPU(10); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	st, err := c.Stats()
	if err != nil || st.Throttled == 0 {
		t.Fatalf("throttled %+v, %v", st, err)
	}
	if err := c.LimitCPU(0); err != nil {
		t.Fatal(err)
	}
}

// TestNetemLoss drops packets to one port and none to another.
func TestNetemLoss(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(io.Discard, c) }()
		}
	}()
	port := ln.Addr().(*net.TCPAddr).Port
	n, err := NewNetem(map[int]time.Duration{port: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := n.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := n.Impair(port, "loss 20%"); err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	for range 200 {
		if _, err := c.Write(make([]byte, 1000)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	_ = c.Close()
	st, err := n.Stats(port)
	if err != nil || st.Packets == 0 || st.Dropped == 0 {
		t.Fatalf("stats %+v, %v", st, err)
	}
	if _, err := n.Heal(port); err != nil {
		t.Fatal(err)
	}
}

// TestNetemHealClears puts each impairment on a port in turn, after loss,
// and heals it: the qdisc must then show the base delay alone. netem's
// change kept corruption and reordering, so a corrupting link stayed
// corrupt after its heal (#241).
func TestNetemHealClears(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	port := ln.Addr().(*net.TCPAddr).Port
	n, err := NewNetem(map[int]time.Duration{port: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := n.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, impairment := range []string{"loss 20%", "reorder 50% 50%", "corrupt 5%"} {
		if err := n.Impair(port, impairment); err != nil {
			t.Fatal(err)
		}
		// Heal fails while the qdisc still shows an impairment.
		if _, err := n.Heal(port); err != nil {
			t.Fatalf("heal after %s: %v", impairment, err)
		}
	}
}
