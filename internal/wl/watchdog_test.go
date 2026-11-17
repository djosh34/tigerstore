//go:build !(sim || simio)

package wl

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// A turn that does not end is logged, once, from outside the loop
// (#720): here a "loop" goroutine on its own thread blocked in a read of
// a pipe. The line names the thread's state and system call and the
// goroutine's stack, down to the read; a loop waiting for events, or a
// turn reported already, logs nothing.
func TestTheWatchdogNamesWhereAStuckTurnStands(t *testing.T) {
	r, wr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = wr.Close() }()
	var w loopWatch
	ready := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		w.tid, w.goid = unix.Gettid(), goid()
		close(ready)
		var b [1]byte
		_, _ = unix.Read(int(r.Fd()), b[:]) // stands here until the pipe has a byte
	}()
	<-ready
	defer func() {
		_, _ = wr.Write([]byte{1})
		<-done
		_ = r.Close()
	}()
	var lines []string
	logf := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }

	w.wait()
	w.check(time.Now(), logf)
	if len(lines) != 0 {
		t.Fatalf("a loop waiting for events was logged: %q", lines)
	}
	w.begin(time.Now())
	w.check(time.Now(), logf)
	if len(lines) != 0 {
		t.Fatalf("a turn shorter than %v was logged: %q", watchAfter, lines)
	}
	// The goroutine reaches its read.
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(goroutineStack(w.goid), "unix.Read") {
		if time.Now().After(deadline) {
			t.Fatalf("the goroutine never reached its read: %s", goroutineStack(w.goid))
		}
		time.Sleep(time.Millisecond)
	}
	w.begin(time.Now().Add(-2 * watchAfter))
	w.check(time.Now(), logf)
	w.check(time.Now(), logf)
	if len(lines) != 1 {
		t.Fatalf("the stuck turn was logged %d times, want once: %q", len(lines), lines)
	}
	line := lines[0]
	for _, want := range []string{"stood in one turn for", fmt.Sprintf("thread %d state S", w.tid), "system call:", "the loop's goroutine: goroutine " + w.goid, "unix.Read"} {
		if !strings.Contains(line, want) {
			t.Errorf("the watchdog's line lacks %q:\n%s", want, line)
		}
	}
}
