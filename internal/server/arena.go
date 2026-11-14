package server

// The requests of the unary calls that the node runs go into a
// per-connection arena of chunks, which the server lends out, instead of a
// fresh slice per call. A chunk counts the calls whose requests it holds,
// and goes back to the server's chunks once none does. A call holds its
// request until the node has answered it (releaseReqLocked): a read that
// waits for ReadIndex, and a write the leader applies, use its bytes until
// then, through the aliases the decoders made.
const (
	// bodyChunk is the size of a chunk; a request larger than a quarter
	// of it gets a slice of its own, so a chunk wastes little at its end.
	bodyChunk = 64 << 10
	// keepBodyChunks is how many chunks the server keeps for the next
	// connection that needs one.
	keepBodyChunks = 64
)

// poisonBodies makes a chunk that goes back to the server's chunks be
// overwritten first, so that a request read after its call released it
// reads garbage, which the tests' checks then see. This package's tests
// set it (arena_test.go).
var poisonBodies = false

// chunk is one chunk of a connection's arena, and the number of calls
// whose requests it holds.
type chunk struct {
	b    []byte
	refs int
	// abandoned is set once a call that keeps its request past its first
	// reply holds the chunk: an answer in pieces, whose cursors at the node
	// read the request's keys for each piece. Such a chunk never goes back
	// to the server: it still counts against the calls' bound until its
	// last call let go of it, the answer in pieces at its end, and is then
	// left to the garbage collector.
	abandoned bool
}

// arena is a connection's chunks. cur is the chunk requests go into, if
// any; the others still hold requests the node has not answered. A chunk
// whose requests were all released goes back, cur too, so an idle
// connection holds none. Loop only; what it charges against maxHeld
// changes under c.mu.
type arena struct {
	cur *chunk
}

// body returns room for a request of n bytes, of length 0 and capacity n,
// and its chunk, nil for a request that gets a slice of its own. The
// stream keeps the chunk until it lets go of the request (releaseBody).
func (c *conn) body(n int) ([]byte, *chunk) {
	if n > bodyChunk/4 {
		return make([]byte, 0, n), nil
	}
	a := &c.arena
	if a.cur == nil || len(a.cur.b)+n > cap(a.cur.b) {
		// A full chunk is the connection's until its last request is
		// released. A chunk's bytes count against the calls' bound
		// (maxHeld) from when it is lent until it goes back.
		a.cur = &chunk{b: c.s.bodyBufs.Get()[:0]}
		c.mu.Lock()
		c.held += bodyChunk
		c.mu.Unlock()
	}
	ch := a.cur
	off := len(ch.b)
	ch.b = ch.b[:off+n]
	ch.refs++
	return ch.b[off : off : off+n], ch
}

// releaseBody lets go of the request of st, whose chunk goes back to the
// server once it holds no other request. c.mu is held.
func (c *conn) releaseBody(st *stream) {
	ch := st.chunk
	if ch == nil {
		return
	}
	st.chunk = nil
	if ch.refs--; ch.refs > 0 {
		return
	}
	if ch == c.arena.cur {
		c.arena.cur = nil
	}
	if !ch.abandoned {
		recycle(ch.b)
		c.s.bodyBufs.Put(ch.b[:cap(ch.b)])
	}
	c.held -= bodyChunk
	c.creditLocked()
}

// abandonBody keeps st's request, and its chunk out of the arena, until
// the stream ends: the node reads the request past its first reply. c.mu
// is held.
func (c *conn) abandonBody(st *stream) {
	st.keepBody = true
	if ch := st.chunk; ch != nil {
		ch.abandoned = true
		if ch == c.arena.cur {
			c.arena.cur = nil
		}
	}
}

// recycle overwrites the bytes of a chunk that is lent again, in tests.
func recycle(b []byte) {
	if poisonBodies {
		for i := range b {
			b[i] = 1
		}
	}
}
