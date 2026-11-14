package server

import (
	"bytes"
	"testing"
)

// The tests overwrite every chunk before it is lent again, so a request
// read after its release shows up as wrong data: without the abandon for
// answers in pieces, TestAnswersHeldBackBySlowClient fails.
func init() { poisonBodies = true }

// TestArenaLendsChunks puts three requests into one chunk, which the
// connection holds until the last is released and which counts against
// its calls' bound meanwhile; then it goes back to the server, poisoned.
func TestArenaLendsChunks(t *testing.T) {
	c := framesConn(t)
	var sts []*stream
	for i := range 3 {
		st := &stream{c: c}
		st.msg, st.chunk = c.body(100)
		st.msg = append(st.msg, bytes.Repeat([]byte{byte(i + 1)}, 100)...)
		sts = append(sts, st)
	}
	ch := sts[0].chunk
	if ch == nil || sts[2].chunk != ch || ch.refs != 3 || c.held != bodyChunk {
		t.Fatalf("three requests in one chunk: %d refs, %d bytes held", ch.refs, c.held)
	}
	if got := ch.b[100:200]; !bytes.Equal(got, bytes.Repeat([]byte{2}, 100)) {
		t.Fatal("the second request is not where it was written")
	}
	c.releaseBody(sts[0])
	c.releaseBody(sts[0]) // a second release of the same call does nothing
	c.releaseBody(sts[1])
	if c.held != bodyChunk || c.s.bodyBufs.Kept() != 0 {
		t.Fatalf("one request left: %d bytes held, %d chunks back", c.held, c.s.bodyBufs.Kept())
	}
	c.releaseBody(sts[2])
	if c.held != 0 || c.s.bodyBufs.Kept() != 1 || c.arena.cur != nil {
		t.Fatalf("no request left: %d bytes held, %d chunks back, chunk kept %v", c.held, c.s.bodyBufs.Kept(), c.arena.cur != nil)
	}
	if !bytes.Equal(sts[2].msg, bytes.Repeat([]byte{1}, 100)) {
		t.Fatal("a chunk went back without being poisoned")
	}
}

// TestArenaAbandonsChunks keeps a chunk out of the arena once a call
// keeps its request past its reply, as an answer in pieces does: the node
// answering the call does not let go of the request, the chunk counts
// against the calls' bound until that call's stream lets go of it, never
// goes back to the server, and the next request goes into a new chunk.
func TestArenaAbandonsChunks(t *testing.T) {
	c := framesConn(t)
	a, b := &stream{c: c}, &stream{c: c}
	a.msg, a.chunk = c.body(10)
	b.msg, b.chunk = c.body(10)
	ch := a.chunk
	c.abandonBody(a)
	c.releaseReqLocked(a)
	c.releaseReqLocked(b)
	if a.chunk != ch || c.arena.cur != nil || c.held != bodyChunk {
		t.Fatalf("abandoned: request kept %v, chunk still current %v, %d bytes held", a.chunk == ch, c.arena.cur != nil, c.held)
	}
	n := &stream{c: c}
	n.msg, n.chunk = c.body(10)
	if n.chunk == ch || c.held != 2*bodyChunk {
		t.Fatal("a request went into an abandoned chunk")
	}
	c.releaseBody(a)
	if c.held != bodyChunk || c.s.bodyBufs.Kept() != 0 {
		t.Fatalf("the abandoned chunk's last call ended: %d bytes held, %d chunks back", c.held, c.s.bodyBufs.Kept())
	}
}

// TestArenaLeavesLargeRequests gives a request larger than a quarter of a
// chunk a slice of its own, and starts a new chunk for one that does not
// fit the rest of the current one.
func TestArenaLeavesLargeRequests(t *testing.T) {
	c := framesConn(t)
	if msg, ch := c.body(bodyChunk/4 + 1); ch != nil || cap(msg) != bodyChunk/4+1 || c.held != 0 {
		t.Fatalf("a large request: chunk %v, capacity %d, %d bytes held", ch != nil, cap(msg), c.held)
	}
	_, first := c.body(bodyChunk / 4)
	for range 3 {
		c.body(bodyChunk / 4)
	}
	if _, ch := c.body(1); ch == first || c.held != 2*bodyChunk {
		t.Fatalf("a request past a full chunk: same chunk %v, %d bytes held", ch == first, c.held)
	}
}
