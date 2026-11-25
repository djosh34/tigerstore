package main

import (
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"
)

// With stacksEnv set, SIGUSR1 writes every goroutine's stack to stderr
// and the process goes on (#780).
func TestSIGUSR1WritesTheStacksWhenAskedFor(t *testing.T) {
	t.Setenv(stacksEnv, "1")
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	was := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = was }()
	stacksOnSignal()
	defer signal.Reset(syscall.SIGUSR1)
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 1)
	go func() {
		b := make([]byte, 1<<16)
		k, _ := io.ReadAtLeast(r, b, 64)
		got <- string(b[:k])
	}()
	select {
	case s := <-got:
		if !strings.Contains(s, "goroutine ") || !strings.Contains(s, "TestSIGUSR1WritesTheStacksWhenAskedFor") {
			t.Fatalf("stderr after SIGUSR1 has no stacks of this test's goroutine:\n%.500s", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no stacks on stderr within 5 s of SIGUSR1")
	}
	_ = w.Close()
}
