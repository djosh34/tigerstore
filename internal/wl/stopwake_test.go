package wl

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestANodeStopsWhenCancelledRightAfterItLooked is #828: a node's context
// is cancelled right after its loop looked for a stop on a wake, and before
// it read the wake. The cancel's wake then came before that read, which
// took it with the post's, and the loop never looked for the stop again:
// Run did not return, and a test cluster's cleanup waited for good. The
// node must stop within 10 s.
func TestANodeStopsWhenCancelledRightAfterItLooked(t *testing.T) {
	loopOf := watchLoops(t)
	tc := newTestCluster(t)
	c := tc.client(t)
	waitMembers(t, c, 3)
	i := 2
	id, cancel, stopped := tc.ids[i], tc.cancels[i], tc.stopped[i]
	var armed atomic.Bool
	armed.Store(true)
	hook := func(l *loop) {
		if l.n.id == id && armed.CompareAndSwap(true, false) {
			cancel()
			// The cancel's wake is written by a goroutine of its own:
			// it comes now, before the loop goes on.
			time.Sleep(100 * time.Millisecond)
		}
	}
	testAfterStopCheck.Store(&hook)
	t.Cleanup(func() { testAfterStopCheck.Store(nil) })
	// A post wakes the loop; the hook cancels it right after it looked.
	go onLoop(loopOf(id), func() {})
	select {
	case err := <-stopped:
		if !errors.Is(err, errStopped) {
			t.Fatalf("node %d stopped with %v", i+1, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("node %d did not stop within 10 s of its cancel (armed %v)", i+1, armed.Load())
	}
	if armed.Load() {
		t.Fatal("the hook never ran: the test tested nothing")
	}
	// Stopped: the cluster's cleanup must not wait for it again.
	tc.cancels[i] = nil
}
