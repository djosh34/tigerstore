package wl

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// The watchdog sees a turn that does not end (#720). The slow-turn line
// and the stall counters (stall.go) run on the loop, after a turn, so a
// loop that stands still logs nothing until it moves again, or ever, if
// the node is killed first: past-ram's leader logged nothing for its
// last 10 s. A goroutine of its own checks the turn the loop is in, and
// once a turn has run past watchAfter it logs, once per such turn, where
// the loop stands: its thread's state, the kernel's wait and system call
// for it, its kernel stack where the kernel shows it, the loop
// goroutine's Go stack, and the machine's pressure and dirty pages.

// watchAfter is how long a turn runs before the watchdog logs it: the
// election timeout, after which the followers stop waiting on a leader.
const watchAfter = electionTicks * tickEvery

// watchEvery is how often the watchdog looks.
const watchEvery = 250 * time.Millisecond

// loopWatch is what the loop tells its watchdog: when its turn began, 0
// while it waits for events, and its thread and goroutine.
type loopWatch struct {
	turnAt   atomic.Int64 // unix nanoseconds
	tid      int
	goid     string
	reported int64 // the turn reported already
}

// begin marks the loop's turn as begun at now; wait, as waiting for
// events.
func (w *loopWatch) begin(now time.Time) { w.turnAt.Store(now.UnixNano()) }
func (w *loopWatch) wait()               { w.turnAt.Store(0) }

// check logs a turn that has run past watchAfter, once.
func (w *loopWatch) check(now time.Time, logf func(string, ...any)) {
	at := w.turnAt.Load()
	if at == 0 || at == w.reported || now.Sub(time.Unix(0, at)) < watchAfter {
		return
	}
	w.reported = at
	logf("the loop has stood in one turn for %v: %s", now.Sub(time.Unix(0, at)).Round(time.Millisecond), w.where())
}

// where is where the loop's thread and goroutine stand, what can be read
// of it.
func (w *loopWatch) where() string {
	var b strings.Builder
	task := "/proc/thread-self"
	if w.tid != 0 {
		task = "/proc/self/task/" + strconv.Itoa(w.tid)
	}
	// stat: the thread's state (R running, S sleeping, D waiting on the
	// disk) and its user and system CPU in ticks.
	if s, err := procFile(task + "/stat"); err == nil {
		if _, rest, ok := bytes.CutLast(s, []byte(")")); ok {
			if f := bytes.Fields(rest); len(f) > 13 {
				fmt.Fprintf(&b, "thread %d state %s, CPU ticks user %s system %s", w.tid, f[0], f[11], f[12])
			}
		}
	}
	for _, f := range []struct{ name, what string }{{"wchan", "kernel wait"}, {"syscall", "system call"}, {"stack", "kernel stack"}} {
		if s, err := procFile(task + "/" + f.name); err == nil && len(bytes.TrimSpace(s)) > 0 {
			fmt.Fprintf(&b, "; %s: %s", f.what, strings.Join(strings.Fields(string(s)), " "))
		}
	}
	for _, p := range []string{"cpu", "io", "memory"} {
		if s, err := procFile("/proc/pressure/" + p); err == nil {
			fmt.Fprintf(&b, "; %s pressure: %s", p, strings.Join(strings.Fields(string(s)), " "))
		}
	}
	if s, err := procFile("/proc/meminfo"); err == nil {
		for line := range strings.Lines(string(s)) {
			if strings.HasPrefix(line, "Dirty:") || strings.HasPrefix(line, "Writeback:") {
				fmt.Fprintf(&b, "; %s", strings.Join(strings.Fields(line), " "))
			}
		}
	}
	if g := goroutineStack(w.goid); g != "" {
		fmt.Fprintf(&b, "; the loop's goroutine: %s", g)
	}
	return b.String()
}

// goid is the calling goroutine's id, as its stack names it.
func goid() string {
	buf := make([]byte, 64)
	buf = buf[:runtime.Stack(buf, false)]
	f := bytes.Fields(buf)
	if len(f) < 2 || string(f[0]) != "goroutine" {
		return ""
	}
	return string(f[1])
}

// goroutineStack is goroutine id's stack, on one line.
func goroutineStack(id string) string {
	if id == "" {
		return ""
	}
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for g := range bytes.SplitSeq(buf, []byte("\n\n")) {
		if bytes.HasPrefix(g, []byte("goroutine "+id+" ")) {
			return strings.Join(strings.Fields(string(g)), " ")
		}
	}
	return ""
}

// watch checks the loop's turn every watchEvery until stop closes.
func (w *loopWatch) watch(stop <-chan struct{}, logf func(string, ...any)) {
	t := time.NewTicker(watchEvery)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			w.check(now, logf)
		}
	}
}
