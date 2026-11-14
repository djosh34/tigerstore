package server

import (
	"testing"
	"time"
	"unsafe"
)

// TestDeadlinesEndCallsInOrder arms four calls' deadlines, drops one as
// its call ends, and ends the calls whose deadline passed: earliest
// first, and none that left or is still to come.
func TestDeadlinesEndCallsInOrder(t *testing.T) {
	c := framesConn(t)
	now := time.Now()
	var sts []*stream
	for i, d := range []int{3, 1, 2, 4} {
		st := testCall(c, uint32(2*i+3))
		c.armDeadline(st, now.Add(time.Duration(d)*time.Second))
		sts = append(sts, st)
	}
	c.disarmDeadline(sts[2])
	c.disarmDeadline(sts[2])
	c.expireDue(now.Add(3 * time.Second))
	for i, want := range []bool{true, true, false, false} {
		if got := sts[i].done.Load(); got != want {
			t.Errorf("call %d ended %v, want %v", i, got, want)
		}
	}
	if len(c.deadlines) != 1 || c.deadlines[0] != sts[3] || sts[3].due != 1 || sts[0].due != 0 || sts[2].due != 0 {
		t.Fatalf("%d deadlines left, the last call's place %d", len(c.deadlines), sts[3].due)
	}
}

// TestStreamSize keeps a stream within what a connection counts for each
// one it keeps for its next calls (connBytes).
func TestStreamSize(t *testing.T) {
	if n := unsafe.Sizeof(stream{}); n > streamBytes {
		t.Fatalf("a stream takes %d bytes, more than streamBytes %d", n, streamBytes)
	}
}

// TestStreamsAreReused ends a call the node ran and one at the node, and
// starts the next: it gets the first call's stream, zeroed, and the call
// still at the node keeps its own.
func TestStreamsAreReused(t *testing.T) {
	c := framesConn(t)
	done, atNode := testCall(c, 3), testCall(c, 5)
	c.atNode[atNode] = struct{}{}
	// A request that never decoded is not kept with the stream.
	done.msg, done.body = make([]byte, 1<<20), make([]byte, 1<<20)
	done.finish(nil)
	atNode.finish(nil)
	drain(c)
	if len(c.free) != 1 || c.free[0] != done || done.msg != nil || done.body != nil {
		t.Fatalf("%d streams kept for the next calls, want the ended call's without its request", len(c.free))
	}
	st := c.newStream(&reqHeaders{id: 7, method: "POST", path: "/etcdserverpb.KV/Range", contentType: "application/grpc"})
	if st != done || st.id != 7 || st.done.Load() || st.ended || st.out != nil || c.streams[7] != st || len(c.free) != 0 {
		t.Fatalf("the next call's stream: reused %v, id %d, done %v, ended %v", st == done, st.id, st.done.Load(), st.ended)
	}
	if http := c.newStream(&reqHeaders{id: 9, method: "GET", path: "/health"}); http == atNode {
		t.Fatal("a stream at the node was reused")
	}
}

// TestReleaseDropsDeadlines closes a connection with a call whose deadline
// is an hour away: once the loop lets go of the connection, neither its
// timer nor the deadline heap refers to the connection or the call.
func TestReleaseDropsDeadlines(t *testing.T) {
	c := framesConn(t)
	c.s.conns, c.s.gone = map[closer]struct{}{}, newConnMetrics()
	c.s.wg.Add(1)
	if !c.s.track(c) {
		t.Fatal("track refused the connection")
	}
	st := testCall(c, 3)
	c.armDeadline(st, time.Now().Add(time.Hour))
	c.close()
	c.release()
	if c.s.loop.Timeout(-1) >= 0 || len(c.deadlines) != 0 || st.due != 0 {
		t.Fatalf("after release: loop timeout %v, %d deadlines, call's place %d", c.s.loop.Timeout(-1), len(c.deadlines), st.due)
	}
}
