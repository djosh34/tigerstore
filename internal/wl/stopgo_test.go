package wl

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// A stopped node leaves no goroutine of its own behind: Run returns only
// once the work it started off the loop has ended (#819).
func TestAStoppedNodeLeavesNoGoroutine(t *testing.T) {
	before := goroutineIDs()
	loopOf := watchLoops(t)
	tc := newTestCluster(t)
	tc.client(t)
	// Work off the loop that is still going when the node is told to
	// stop, as a password's hash or a snapshot's copy may be.
	l := loopOf(tc.ids[0])
	l.offLoop(func() func() error {
		<-l.ctx.Done()
		time.Sleep(500 * time.Millisecond)
		return func() error { return nil }
	})
	tc.stop(t)
	if left := nodeGoroutines(before); len(left) > 0 {
		t.Fatalf("%d goroutines of the stopped nodes still run:\n\n%s", len(left), strings.Join(left, "\n\n"))
	}
}

// goroutineIDs are the goroutines that run now, by their first line.
func goroutineIDs() map[string]bool {
	ids := map[string]bool{}
	for _, g := range goroutines() {
		ids[strings.Fields(g)[1]] = true
	}
	return ids
}

// nodeGoroutines are the stacks of the goroutines that wl's own code
// started, not its tests, and that did not run before.
func nodeGoroutines(before map[string]bool) []string {
	var left []string
	for _, g := range goroutines() {
		if before[strings.Fields(g)[1]] {
			continue
		}
		_, created, ok := strings.Cut(g, "\ncreated by github.com/djosh34/tigerstore/internal/wl.")
		if !ok {
			continue
		}
		// The line after names the file that started it.
		if _, at, _ := strings.Cut(created, "\n"); !strings.Contains(at, "_test.go:") {
			left = append(left, g)
		}
	}
	return left
}

// goroutines are the stacks of every goroutine.
func goroutines() []string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Split(string(buf[:n]), "\n\n")
		}
		buf = make([]byte, 2*len(buf))
	}
}

// A client's snapshot copy whose loop stops before it takes the copy
// closes the copy's file: the loop drops what the copy posts (#865).
func TestAClientSnapshotCopyOfAStoppedLoopClosesItsFile(t *testing.T) {
	for _, posted := range []bool{false, true} {
		name := "stopped before the copy ended"
		if posted {
			name = "stopped with the copy posted"
		}
		t.Run(name, func(t *testing.T) {
			n := copyRingNode(t)
			_, _, h := snapshotClient(t, n)
			src := liveCopy(t, n)
			l := copyLoop(t, n)
			if !posted {
				l.stopPosts()
			}
			l.prepareClientSnap(h, src)
			l.goes.Wait()
			if posted {
				l.stopPosts()
			}
			if open := openFilesIn(t, n.ckptDir); len(open) > 0 {
				t.Fatalf("the copy's file is still open: %v", open)
			}
		})
	}
}

// openFilesIn are the client copies under dir that the process holds
// open, a removed one with " (deleted)" after its name.
func openFilesIn(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	var open []string
	for _, e := range ents {
		to, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err == nil && strings.HasPrefix(to, dir+string(filepath.Separator)) && strings.Contains(to, "client-") {
			open = append(open, to)
		}
	}
	return open
}
