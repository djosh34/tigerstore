package main

import (
	"bufio"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// stacksChild is the process TestSIGUSR1WritesTheStacksWhenAskedFor
// starts: this test binary, which runs stacksOnSignal and waits.
const stacksChild = "TIGERSTORE_STACKS_TEST_CHILD"

func TestMain(m *testing.M) {
	if os.Getenv(stacksChild) == "1" {
		stacksOnSignal()
		time.Sleep(time.Minute)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// With stacksEnv set, a process says it handles SIGUSR1, and at SIGUSR1
// writes every goroutine's stack to stderr and goes on (#780). The
// handler runs in a child, so that its goroutine and signal handling end
// with it.
func TestSIGUSR1WritesTheStacksWhenAskedFor(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), stacksChild+"=1", stacksEnv+"=1")
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	lines := make(chan string, 1024)
	go func() {
		defer close(lines)
		s := bufio.NewScanner(stderr)
		s.Buffer(make([]byte, 1<<16), 1<<20)
		for s.Scan() {
			lines <- s.Text()
		}
	}()
	// until reads lines until one holds want, for 5 s at most.
	until := func(want string) {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case l, ok := <-lines:
				if !ok {
					t.Fatalf("stderr ended before %q", want)
				}
				if strings.Contains(l, want) {
					return
				}
			case <-deadline:
				t.Fatalf("no %q on stderr within 5 s", want)
			}
		}
	}
	until(stacksReady)
	if err := cmd.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	until("goroutine ")
	until("stacksOnSignal")
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("the process ended after SIGUSR1: %v", err)
	}
}
