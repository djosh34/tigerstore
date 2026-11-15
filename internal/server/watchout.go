package server

// The watch responses a stream sends on the node's loop are encoded into
// per-connection chunks that the server lends out, instead of a fresh
// slice per response (part 5 of docs/wire-loop.md). A chunk counts the
// responses that wait in it on their streams. A write pass that took a
// response whole counts it off once the pass wrote it (releaseTaken), and
// the chunk goes back to the server's chunks once none waits. A response
// dropped from its stream's queue, as when the stream ends, is never
// counted off: its chunk is left to the garbage collector.
const (
	// outChunkSize is the size of a chunk; a response larger than a
	// quarter of it gets a slice of its own.
	outChunkSize = 64 << 10
	// keepOutChunks is how many chunks the server keeps for the next
	// connection that needs one.
	keepOutChunks = 64
)

// outChunk is one chunk of a connection's watch output, and the number of
// responses in it that wait to be written.
type outChunk struct {
	b    []byte
	refs int
}

// watchRoom returns room for a watch response of n bytes, of length 0
// and capacity n, and its chunk, nil for one that gets a slice of its
// own. Loop only.
func (c *conn) watchRoom(n int) ([]byte, *outChunk) {
	if n > outChunkSize/4 {
		return make([]byte, 0, n), nil
	}
	if c.outCur == nil || len(c.outCur.b)+n > cap(c.outCur.b) {
		// A full chunk goes back once its last response was written.
		c.outCur = &outChunk{b: c.s.outBufs.Get()[:0]}
	}
	ch := c.outCur
	off := len(ch.b)
	ch.b = ch.b[:off+n]
	ch.refs++
	return ch.b[off : off : off+n], ch
}

// releaseTaken counts off the responses that the write pass took whole
// and has written, and hands back the chunks that hold no more. Loop
// only.
func (c *conn) releaseTaken() {
	for i, ch := range c.taken {
		c.taken[i] = nil
		if ch.refs--; ch.refs > 0 {
			continue
		}
		if ch == c.outCur {
			c.outCur = nil
		}
		recycle(ch.b)
		c.s.outBufs.Put(ch.b[:cap(ch.b)])
	}
	c.taken = c.taken[:0]
}
